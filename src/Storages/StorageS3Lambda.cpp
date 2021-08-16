#include <Storages/StorageS3Lambda.h>

#if !defined(ARCADIA_BUILD)
#include <Common/config.h>
#endif

#if USE_AWS_S3

#include <Common/Exception.h>
#include <Client/LambdaConnections.h>
#include <Core/QueryProcessingStage.h>
#include <Core/UUID.h>
#include <DataStreams/RemoteBlockInputStream.h>
#include <DataTypes/DataTypeString.h>
#include <Interpreters/Context.h>
#include <Interpreters/getHeaderForProcessingStage.h>
#include <Interpreters/SelectQueryOptions.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/getTableExpressions.h>
#include <DataStreams/AddingDefaultsBlockInputStream.h>
#include <Processors/Pipe.h>
#include <Processors/Sources/RemoteSource.h>
#include <Parsers/queryToString.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Storages/IStorage.h>
#include <Storages/SelectQueryInfo.h>
#include <common/logger_useful.h>

#include <aws/core/auth/AWSCredentials.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/ListObjectsV2Request.h>

#include <memory>
#include <string>
#include <cassert>

namespace DB
{

namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
}

StorageS3Lambda::StorageS3Lambda(
    const String & filename_,
    const String & access_key_id_,
    const String & secret_access_key_,
    const StorageID & table_id_,
    const String & format_name_,
    UInt64 max_connections_,
    UInt64 lambda_parallelism_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    ContextPtr context_,
    const String & compression_method_)
    : IStorage(table_id_)
    , log(&Poco::Logger::get("StorageS3Lambda"))
    , client_auth{S3::URI{Poco::URI{filename_}}, access_key_id_, secret_access_key_, max_connections_, {}, {}},
      filename(filename_), format_name(format_name_), lambda_parallelism(lambda_parallelism_),
      compression_method(compression_method_)
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    storage_metadata.setConstraints(constraints_);
    setInMemoryMetadata(storage_metadata);

    StorageS3::updateClientAndAuthSettings(context_, client_auth);
}

/// The code executes on initiator
Pipe StorageS3Lambda::read(
    const Names & column_names,
    const StorageMetadataPtr & metadata_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum processed_stage,
    size_t /*max_block_size*/,
    unsigned /*num_streams*/)
{
    LOG_TRACE(log, "Query processing stage: {}", QueryProcessingStage::toString(processed_stage));

    S3::URI s3_uri(Poco::URI{filename});
    StorageS3::updateClientAndAuthSettings(context, client_auth);

    auto iterator = std::make_shared<StorageS3Source::DisclosedGlobIterator>(*client_auth.client, client_auth.uri);

    /// Collect all files and then shard them across lambda instances.
    Strings files;
    for (String file = iterator->next(); !file.empty(); file = iterator->next())
    {
        files.emplace_back(file);
    }

    if (files.size() == 0)
        throw new Exception("No files to read", ErrorCodes::NOT_IMPLEMENTED);

    LOG_TRACE(log, "All files: {}", fmt::join(files, ", "));

    UInt64 actual_parallelism = std::min<UInt64>(files.size(), lambda_parallelism);
    UInt64 files_per_lambda = files.size() / actual_parallelism;
    UInt64 files_remainder = files.size() % actual_parallelism;

    std::vector<std::vector<std::string>> files_by_lambda;
    size_t offset = 0;
    for (size_t ix = 0; ix < actual_parallelism; ix++)
    {
        size_t sz = files_per_lambda + (files_remainder > ix);
        files_by_lambda.emplace_back(
            files.begin() + offset, files.begin() + offset + sz);
        offset += sz;
    }

    for (const auto & v : files_by_lambda)
        LOG_TRACE(log, "Per lambda: {}", fmt::join(v, ", "));

    /// Calculate the header. This is significant, because some columns could be thrown away in some cases like query with count(*)
    Block header =
        InterpreterSelectQuery(query_info.query, context,
                               SelectQueryOptions(processed_stage).analyze()).getSampleBlock();

    const Scalars & scalars = context->hasQueryContext() ? context->getQueryContext()->getScalars() : Scalars{};

    const bool add_agg_info = processed_stage == QueryProcessingStage::WithMergeableState;

    Pipes pipes;
    for (UInt64 lambda_ix = 0; lambda_ix < actual_parallelism; lambda_ix++)
    {
        const LambdaConnectionContext connection{
            .function_name = context->getAwsLambdaFunctionName(),
            .tasks = files_by_lambda[lambda_ix],
            .lambda_client = context->getAwsLambdaClient(),
        };

        auto remote_query_executor = std::make_shared<RemoteQueryExecutor>(
            connection, queryToString(query_info.query), header, context,
            scalars, Tables(), processed_stage);

        pipes.emplace_back(std::make_shared<RemoteSource>(remote_query_executor, add_agg_info, false));
    }

    metadata_snapshot->check(column_names, getVirtuals(), getStorageID());
    return Pipe::unitePipes(std::move(pipes));
}

QueryProcessingStage::Enum StorageS3Lambda::getQueryProcessingStage(
    ContextPtr context, QueryProcessingStage::Enum to_stage, const StorageMetadataPtr &, SelectQueryInfo &) const
{
    /// Initiator executes query on remote node.
    if (context->getClientInfo().query_kind == ClientInfo::QueryKind::INITIAL_QUERY)
        if (to_stage >= QueryProcessingStage::Enum::WithMergeableState)
            return QueryProcessingStage::Enum::WithMergeableState;

    /// Follower just reads the data.
    return QueryProcessingStage::Enum::FetchColumns;
}


NamesAndTypesList StorageS3Lambda::getVirtuals() const
{
    return NamesAndTypesList{
        {"_path", std::make_shared<DataTypeString>()},
        {"_file", std::make_shared<DataTypeString>()}
    };
}


}

#endif
