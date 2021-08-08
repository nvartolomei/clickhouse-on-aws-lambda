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
#include <Poco/JSON/Parser.h>
#include <common/DateLUT.h>
#include <common/defines.h>

std::function<std::shared_ptr<Aws::Utils::Logging::LogSystemInterface>()> GetConsoleLoggerFactory()
{
    return [] { return Aws::MakeShared<Aws::Utils::Logging::ConsoleLogSystem>("console_logger", Aws::Utils::Logging::LogLevel::Trace); };
}

static char const TAG[] = "lambda";


namespace DB
{
int Runtime::main(const std::vector<std::string> & args)
{
    registerFunctions();
    registerAggregateFunctions();
    registerTableFunctions();
    registerFormats();

    auto shared_context = Context::createShared();
    global_context = Context::createGlobal(shared_context.get());

    global_context->makeGlobalContext();
    global_context->setApplicationType(Context::ApplicationType::SERVER);

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
        Aws::Client::ClientConfiguration config;
        config.region = Aws::Environment::GetEnv("AWS_REGION");
        config.caFile = "/etc/pki/tls/certs/ca-bundle.crt";

        auto credentialsProvider = Aws::MakeShared<Aws::Auth::EnvironmentAWSCredentialsProvider>(TAG);
        UNUSED(credentialsProvider);
        // S3::S3Client client(credentialsProvider, config);
        auto handler_fn = [&](aws::lambda_runtime::invocation_request const & req) {
            const auto result = handleRequest(req.payload);

            return aws::lambda_runtime::invocation_response::success(result, "application/json");
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

        pipeline.setOutputFormat(std::move(out));
    }

    auto executor = pipeline.execute();
    executor->execute(pipeline.getNumThreads());

    return result.str();
}
}
