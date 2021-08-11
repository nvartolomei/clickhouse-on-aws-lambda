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
#include <aws/lambda-runtime/runtime.h>
#include <Poco/ConsoleChannel.h>
#include <Poco/JSON/Parser.h>
#include <common/DateLUT.h>

std::function<std::shared_ptr<Aws::Utils::Logging::LogSystemInterface>()> GetConsoleLoggerFactory()
{
    return [] { return Aws::MakeShared<Aws::Utils::Logging::ConsoleLogSystem>("console_logger", Aws::Utils::Logging::LogLevel::Trace); };
}


namespace DB
{
int Runtime::main(const std::vector<std::string> & args)
{
    initializeTerminationAndSignalProcessing();

    logger().root().setChannel(new Poco::ConsoleChannel);
    logger().root().setLevel(Poco::Message::PRIO_TRACE);

    registerFunctions();
    registerAggregateFunctions();
    registerTableFunctions();
    registerFormats();

    auto shared_context = Context::createShared();
    global_context = Context::createGlobal(shared_context.get());

    global_context->makeGlobalContext();
    global_context->setApplicationType(Context::ApplicationType::SERVER);

    global_context->setProgressCallback([](const Progress &) {});

    // Required for invoking distributed query processing.
    global_context->setAwsLambdaFunctionName(Aws::Environment::GetEnv("AWS_LAMBDA_FUNCTION_NAME"));

    DateLUT::instance();

    auto & database_catalog = DatabaseCatalog::instance();
    auto system_database = std::make_shared<DatabaseMemory>(DatabaseCatalog::SYSTEM_DATABASE, global_context);
    database_catalog.attachDatabase(DatabaseCatalog::SYSTEM_DATABASE, system_database);
    attach<StorageSystemOne>(*system_database, "one");
    attach<StorageSystemNumbers>(*system_database, "numbers", false);

    if (args.size() == 1 && args[0] == "local")
    {
        // Local test mode, reads payload from stdin.
        std::ostringstream os;
        os << std::cin.rdbuf();
        std::string input = os.str();

        std::cout << handleRequest(input);

        return 0;
    }

    Aws::SDKOptions options;
    options.loggingOptions.logLevel = Aws::Utils::Logging::LogLevel::Trace;
    options.loggingOptions.logger_create_fn = GetConsoleLoggerFactory();
    InitAPI(options);
    {
        auto handler_fn = [&](aws::lambda_runtime::invocation_request const & req) {
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
    Poco::JSON::Parser parser;
    Poco::Dynamic::Var input_json = parser.parse(input);
    Poco::JSON::Object::Ptr req_object = input_json.extract<Poco::JSON::Object::Ptr>();
    const auto & query = req_object->getValue<std::string>("query");

    ContextMutablePtr context = Context::createCopy(global_context);
    context->makeQueryContext();
    context->setCurrentQueryId("a-lambda-query");

    CurrentThread::QueryScope query_scope(context);

    std::stringstream result;
    result.exceptions(std::ios::failbit);
    WriteBufferFromOStream out_buf(result);

    BlockIO streams = executeQuery(query, context, false);
    auto & pipeline = streams.pipeline;

    if (!pipeline.isCompleted())
    {
        pipeline.addSimpleTransform([](const Block & header) { return std::make_shared<MaterializingTransform>(header); });

        auto out = FormatFactory::instance().getOutputFormatParallelIfPossible("JSON", out_buf, pipeline.getHeader(), context, {}, {});
        out->setAutoFlush();

        /// Save previous progress callback if any. TODO Do it more conveniently.
        auto previous_progress_callback = context->getProgressCallback();

        /// NOTE Progress callback takes shared ownership of 'out'.
        pipeline.setProgressCallback([out, previous_progress_callback](const Progress & progress) {
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

    return result.str();
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
void Runtime::initializeTerminationAndSignalProcessing()
{
    std::set_terminate(terminate_handler);
}
}
