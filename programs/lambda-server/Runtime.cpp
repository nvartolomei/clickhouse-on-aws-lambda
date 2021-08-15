//
// Created by Nicolae Vartolomei on 07/08/2021.
//

#include "Runtime.h"
#include <iostream>
#include <AggregateFunctions/registerAggregateFunctions.h>
#include <Databases/DatabaseMemory.h>
#include <Formats/FormatFactory.h>
#include <Formats/registerFormats.h>
#include <Functions/registerFunctions.h>
#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/WriteBufferFromFileDescriptorDiscardOnFailure.h>
#include <IO/WriteBufferFromOStream.h>
#include <Interpreters/executeQuery.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Processors/Transforms/MaterializingTransform.h>
#include <Storages/System/StorageSystemNumbers.h>
#include <Storages/System/StorageSystemOne.h>
#include <Storages/System/attachSystemTablesImpl.h>
#include <TableFunctions/registerTableFunctions.h>
#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/platform/Environment.h>
#include <aws/core/utils/logging/ConsoleLogSystem.h>
#include <aws/lambda/model/InvokeRequest.h>
#include <aws/lambda-runtime/runtime.h>
#include <aws/lambda/LambdaClient.h>
#include <Poco/ConsoleChannel.h>
#include <Poco/Base64Encoder.h>
#include <Poco/JSON/Parser.h>
#include <Poco/JSON/Object.h>
#include <Common/PipeFDs.h>
#include <common/DateLUT.h>
#include <common/ErrorHandlers.h>
#include <common/sleep.h>
#include <daemon/BaseDaemon.h>
#include <DataStreams/NativeBlockOutputStream.h>

std::function<std::shared_ptr<Aws::Utils::Logging::LogSystemInterface>()> GetConsoleLoggerFactory()
{
    return []
    {
        return Aws::MakeShared<Aws::Utils::Logging::ConsoleLogSystem>("console_logger",
                                                                      Aws::Utils::Logging::LogLevel::Warn);
    };
}


namespace DB
{
int Runtime::main(const std::vector<std::string> & args)
{
    initializeTerminationAndSignalProcessing();

    logger().root().setChannel(new Poco::ConsoleChannel);
    logger().root().setLevel(Poco::Message::PRIO_TRACE);
    logger().setLevel(Poco::Message::PRIO_TRACE);

    registerFunctions();
    registerAggregateFunctions();
    registerTableFunctions();
    registerFormats();

    auto shared_context = Context::createShared();
    global_context = Context::createGlobal(shared_context.get());

    global_context->makeGlobalContext();
    global_context->setApplicationType(Context::ApplicationType::SERVER);

    global_context->setProgressCallback([](const Progress &) {});

    DateLUT::instance();

    auto & database_catalog = DatabaseCatalog::instance();
    auto system_database = std::make_shared<DatabaseMemory>(DatabaseCatalog::SYSTEM_DATABASE, global_context);
    database_catalog.attachDatabase(DatabaseCatalog::SYSTEM_DATABASE, system_database);
    attach<StorageSystemOne>(*system_database, "one");
    attach<StorageSystemNumbers>(*system_database, "numbers", false);

    Aws::SDKOptions options;
    options.loggingOptions.logLevel = Aws::Utils::Logging::LogLevel::Warn;
    options.loggingOptions.logger_create_fn = GetConsoleLoggerFactory();
    InitAPI(options);
    Aws::Http::InitHttp();

    // Required for invoking distributed query processing.
    Aws::Client::ClientConfiguration aws_client_config;
    aws_client_config.region = Aws::Environment::GetEnv("AWS_REGION");

    global_context->setAwsLambdaFunctionName(Aws::Environment::GetEnv("AWS_LAMBDA_FUNCTION_NAME"));
    global_context->setAwsLambdaClient(std::make_shared<Aws::Lambda::LambdaClient>(aws_client_config));

    if (args.size() == 1 && args[0] == "local")
    {
        // Local test mode, reads payload from stdin.
        std::ostringstream os;
        os << std::cin.rdbuf();
        std::string input = os.str();

        std::cout << handleRequest(input);

        return 0;
    }

    {
        auto handler_fn = [&](aws::lambda_runtime::invocation_request const & req)
        {
            try
            {
                const auto result = handleRequest(req.payload);
                return aws::lambda_runtime::invocation_response::success(
                    result, "application/json");
            }
            catch (...)
            {
                tryLogCurrentException(&logger());

                std::string error_msg = "Failed to execute query: " + getCurrentExceptionMessage(false, false, false);
                return aws::lambda_runtime::invocation_response::failure(
                    error_msg, "Internal Server Error");
            }
        };
        aws::lambda_runtime::run_handler(handler_fn);
    }

    ShutdownAPI(options);

    return 0;
}

std::string Runtime::handleRequest(std::string const & input)
{
    LOG_TRACE(&logger(), "handleRequest");

    Strings tasks;
    QueryProcessingStage::Enum to_stage = QueryProcessingStage::Complete;
    String format = "JSON";

    Poco::JSON::Parser parser;
    Poco::Dynamic::Var input_json = parser.parse(input);
    Poco::JSON::Object::Ptr req_object = input_json.extract<Poco::JSON::Object::Ptr>();
    const auto & query = req_object->getValue<std::string>("query");

    ContextMutablePtr context = Context::createCopy(global_context);
    context->makeQueryContext();
    context->setCurrentQueryId("a-lambda-query");

    if (req_object->has("tasks"))
    {
        LOG_TRACE(&logger(), "Query with tasks: {}", fmt::join(tasks, ", "));

        for (const auto & t : *req_object->getArray("tasks"))
            tasks.push_back(t.extract<std::string>());

        context->getClientInfo().query_kind = ClientInfo::QueryKind::SECONDARY_QUERY;

        size_t task_ix = 0;
        context->setReadTaskCallback([&task_ix, &tasks]() -> String
                                     {
                                         if (task_ix < tasks.size())
                                             return tasks[task_ix++];
                                         else
                                             return {};
                                     });
    }

    if (req_object->has("to_stage"))
    {
        to_stage = QueryProcessingStage::Enum(req_object->getValue<UInt64>("to_stage"));
        LOG_TRACE(&logger(), "Execute to to_stage: {}", to_stage);
    }

    if (req_object->has("format"))
    {
        format = req_object->getValue<String>("format");
        LOG_TRACE(&logger(), "Execute with format: {}", format);
    }

    CurrentThread::QueryScope query_scope(context);

    std::stringstream result;
    std::ostream * result_stream = &result;
    result.exceptions(std::ios::failbit);
    std::optional<Poco::Base64Encoder> b64_encoder;

    // 100 binary data. Base64 encode to be safe-ish with JSON
    // and lambda runtime.
    if (to_stage == QueryProcessingStage::WithMergeableState)
    {
        b64_encoder.emplace(result);
        result_stream = &b64_encoder.value();
    }

    WriteBufferFromOStream out_buf(*result_stream);

    BlockIO streams = executeQuery(query, context, false, to_stage);
    auto & pipeline = streams.pipeline;

    if (!pipeline.isCompleted())
    {
        pipeline.addSimpleTransform(
            [](const Block & header) { return std::make_shared<MaterializingTransform>(header); });

        auto out = FormatFactory::instance().getOutputFormatParallelIfPossible(
            format, out_buf, pipeline.getHeader(), context, {}, {});

        out->setAutoFlush();

        /// Save previous progress callback if any. TODO Do it more conveniently.
        auto previous_progress_callback = context->getProgressCallback();

        /// NOTE Progress callback takes shared ownership of 'out'.
        pipeline.setProgressCallback([out, previous_progress_callback](const Progress & progress)
                                     {
                                         if (previous_progress_callback)
                                             previous_progress_callback(progress);
                                         out->onProgress(progress);
                                     });

        pipeline.setOutputFormat(std::move(out));
    }
    else
    {
        pipeline.setProgressCallback(context->getProgressCallback());
    }

    auto executor = pipeline.execute();
    executor->execute(pipeline.getNumThreads());

    Poco::JSON::Object response_payload;

    if (format == "JSON")
    {
        Poco::JSON::Parser out_parser;
        Poco::Dynamic::Var out_json = parser.parse(result.str());
        response_payload.set("data", out_json);
    }
    else
    {
        response_payload.set("data", result.str());
    }

    std::stringstream response_ostream;
    Poco::JSON::Stringifier stringifier;
    stringifier.stringify(
        response_payload, response_ostream, 2, 2);

    return response_ostream.str();
}

/** To use with std::set_terminate.
  * Collects slightly more info than __gnu_cxx::__verbose_terminate_handler,
  *  and send it to pipe. Other thread will read this info from pipe and asynchronously write it to log.
  * Look at libstdc++-v3/libsupc++/vterminate.cc for example.
  */
[[noreturn]] static void terminate_handler()
{
    static thread_local bool terminating = false;
    if (terminating)
        abort();

    terminating = true;

    std::string log_message;

    if (std::current_exception())
        log_message = "Terminate called for uncaught exception:\n" + DB::getCurrentExceptionMessage(true);
    else
        log_message = "Terminate called without an active exception";

    std::cerr << log_message << std::endl;

    /// POSIX.1 says that write(2)s of less than PIPE_BUF bytes must be atomic - man 7 pipe
    /// And the buffer should not be too small because our exception messages can be large.
    // static constexpr size_t buf_size = PIPE_BUF;
    //
    // if (log_message.size() > buf_size - 16)
    //     log_message.resize(buf_size - 16);
    //
    // char buf[buf_size];
    // DB::WriteBufferFromFileDescriptor out(signal_pipe.fds_rw[1], buf_size, buf);
    //
    // DB::writeBinary(static_cast<int>(SignalListener::StdTerminate), out);
    // DB::writeBinary(UInt32(getThreadId()), out);
    // DB::writeBinary(log_message, out);
    // out.next();

    abort();
}


static void blockSignals(const std::vector<int> & signals)
{
    sigset_t sig_set;

#if defined(OS_DARWIN)
    sigemptyset(&sig_set);
    for (auto signal : signals)
        sigaddset(&sig_set, signal);
#else
    if (sigemptyset(&sig_set))
        throw Poco::Exception("Cannot block signal.");

    for (auto signal : signals)
        if (sigaddset(&sig_set, signal))
            throw Poco::Exception("Cannot block signal.");
#endif

    if (pthread_sigmask(SIG_BLOCK, &sig_set, nullptr))
        throw Poco::Exception("Cannot block signal.");
};

using signal_function = void(int, siginfo_t *, void *);

static void addSignalHandler(const std::vector<int> & signals, signal_function handler,
                             std::vector<int> * out_handled_signals)
{
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_sigaction = handler;
    sa.sa_flags = SA_SIGINFO;

#if defined(OS_DARWIN)
    sigemptyset(&sa.sa_mask);
    for (auto signal : signals)
        sigaddset(&sa.sa_mask, signal);
#else
    if (sigemptyset(&sa.sa_mask))
        throw Poco::Exception("Cannot set signal handler.");

    for (auto signal : signals)
        if (sigaddset(&sa.sa_mask, signal))
            throw Poco::Exception("Cannot set signal handler.");
#endif

    for (auto signal : signals)
        if (sigaction(signal, &sa, nullptr))
            throw Poco::Exception("Cannot set signal handler.");

    if (out_handled_signals)
        std::copy(signals.begin(), signals.end(), std::back_inserter(*out_handled_signals));
};


static constexpr size_t max_query_id_size = 127;

static const size_t signal_pipe_buf_size =
    sizeof(int)
    + sizeof(siginfo_t)
    + sizeof(ucontext_t)
    + sizeof(StackTrace)
    + sizeof(UInt32)
    + max_query_id_size + 1    /// query_id + varint encoded length
    + sizeof(void *);

DB::PipeFDs signal_pipe;

static void call_default_signal_handler(int sig)
{
    signal(sig, SIG_DFL);
    raise(sig);
}

/** Handler for "fault" or diagnostic signals. Send data about fault to separate thread to write into log.
  */
static void signalHandler(int sig, siginfo_t * info, void * context)
{
    DENY_ALLOCATIONS_IN_SCOPE;
    auto saved_errno = errno;   /// We must restore previous value of errno in signal handler.

    char buf[signal_pipe_buf_size];
    DB::WriteBufferFromFileDescriptorDiscardOnFailure out(signal_pipe.fds_rw[1], signal_pipe_buf_size, buf);

    const ucontext_t signal_context = *reinterpret_cast<ucontext_t *>(context);
    const StackTrace stack_trace(signal_context);

    StringRef query_id = DB::CurrentThread::getQueryId();   /// This is signal safe.
    query_id.size = std::min(query_id.size, max_query_id_size);

    DB::writeBinary(sig, out);
    DB::writePODBinary(*info, out);
    DB::writePODBinary(signal_context, out);
    DB::writePODBinary(stack_trace, out);
    DB::writeBinary(UInt32(getThreadId()), out);
    DB::writeStringBinary(query_id, out);
    DB::writePODBinary(DB::current_thread, out);

    out.next();

    if (sig != SIGTSTP) /// This signal is used for debugging.
    {
        /// The time that is usually enough for separate thread to print info into log.
        sleepForSeconds(1);  /// FIXME: use some feedback from threads that process stacktrace
        call_default_signal_handler(sig);
    }

    errno = saved_errno;
}

class SignalListener : public Poco::Runnable
{
public:
    explicit SignalListener()
        : log(&Poco::Logger::get("BaseDaemon"))
    {
    }

private:
    Poco::Logger * log;
public:
    enum Signals : int
    {
        StdTerminate = -1,
        StopThread = -2,
    };

    void run() override
    {
        char buf[signal_pipe_buf_size];
        DB::ReadBufferFromFileDescriptor in(signal_pipe.fds_rw[0], signal_pipe_buf_size, buf);

        while (!in.eof())
        {
            int sig = 0;
            DB::readBinary(sig, in);

            if (sig == Signals::StopThread)
            {
                break;
            }
            else
            {
                siginfo_t info{};
                ucontext_t context{};
                StackTrace stack_trace(NoCapture{});
                UInt32 thread_num{};
                std::string query_id;
                DB::ThreadStatus * thread_ptr{};

                // if (sig != SanitizerTrap)
                // {
                DB::readPODBinary(info, in);
                DB::readPODBinary(context, in);
                // }

                DB::readPODBinary(stack_trace, in);
                DB::readBinary(thread_num, in);
                DB::readBinary(query_id, in);
                DB::readPODBinary(thread_ptr, in);

                /// This allows to receive more signals if failure happens inside onFault function.
                /// Example: segfault while symbolizing stack trace.
                std::thread(
                    [=, this] { onFault(sig, info, context, stack_trace, thread_num, query_id, thread_ptr); }).detach();
            }
        }
    }

    void onFault(
        int sig,
        const siginfo_t & info,
        const ucontext_t & context,
        const StackTrace & stack_trace,
        UInt32 thread_num,
        const std::string & query_id,
        DB::ThreadStatus * thread_ptr) const
    {
        DB::ThreadStatus thread_status;

        /// Send logs from this thread to client if possible.
        /// It will allow client to see failure messages directly.
        if (thread_ptr)
        {
            if (auto logs_queue = thread_ptr->getInternalTextLogsQueue())
                DB::CurrentThread::attachInternalTextLogsQueue(logs_queue, DB::LogsLevel::trace);
        }

        LOG_FATAL(log, "########################################");

        if (query_id.empty())
        {
            LOG_FATAL(log, "(from thread {}) (no query) Received signal {} ({})",
                      thread_num, strsignal(sig), sig);
        }
        else
        {
            LOG_FATAL(log, "(from thread {}) (query_id: {}) Received signal {} ({})",
                      thread_num, query_id, strsignal(sig), sig);
        }

        String error_message;

        // if (sig != SanitizerTrap)
        error_message = signalToErrorMessage(sig, info, context);
        // else
        //     error_message = "Sanitizer trap.";

        LOG_FATAL(log, error_message);

        if (stack_trace.getSize())
        {
            /// Write bare stack trace (addresses) just in case if we will fail to print symbolized stack trace.
            /// NOTE: This still require memory allocations and mutex lock inside logger.
            ///       BTW we can also print it to stderr using write syscalls.

            std::stringstream bare_stacktrace;
            bare_stacktrace << "Stack trace:";
            for (size_t i = stack_trace.getOffset(); i < stack_trace.getSize(); ++i)
                bare_stacktrace << ' ' << stack_trace.getFramePointers()[i];

            LOG_FATAL(log, bare_stacktrace.str());
        }

        /// Write symbolized stack trace line by line for better grep-ability.
        stack_trace.toStringEveryLine([&](const std::string & s) { LOG_FATAL(log, s); });

        /// When everything is done, we will try to send these error messages to client.
        if (thread_ptr)
            thread_ptr->onFatalError();
    }
};

void Runtime::initializeTerminationAndSignalProcessing()
{
    std::set_terminate(terminate_handler);

    blockSignals({SIGPIPE});
    addSignalHandler({SIGABRT, SIGSEGV, SIGILL}, signalHandler, nullptr);

    /// Set up Poco ErrorHandler for Poco Threads.
    static KillingErrorHandler killing_error_handler;
    Poco::ErrorHandler::set(&killing_error_handler);

    signal_pipe.setNonBlockingWrite();
    signal_pipe.tryIncreaseSize(1 << 20);

    signal_listener = std::make_unique<SignalListener>();
    signal_listener_thread.start(*signal_listener);
}


static void writeSignalIDtoSignalPipe(int sig)
{
    auto saved_errno = errno;   /// We must restore previous value of errno in signal handler.

    char buf[signal_pipe_buf_size];
    DB::WriteBufferFromFileDescriptor out(signal_pipe.fds_rw[1], signal_pipe_buf_size, buf);
    DB::writeBinary(sig, out);
    out.next();

    errno = saved_errno;
}

Runtime::~Runtime()
{
    writeSignalIDtoSignalPipe(SignalListener::StopThread);
    signal_listener_thread.join();
    /// Reset signals to SIG_DFL to avoid trying to write to the signal_pipe that will be closed after.
    for (int sig : {SIGABRT, SIGSEGV, SIGILL})
    {
        signal(sig, SIG_DFL);
    }
    signal_pipe.close();
}
}
