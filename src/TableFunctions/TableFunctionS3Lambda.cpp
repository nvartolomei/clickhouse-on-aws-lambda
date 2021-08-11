#include <Common/config.h>

#if USE_AWS_S3

#include <IO/S3Common.h>
#include <Storages/StorageS3.h>
#include <Storages/StorageS3Lambda.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/Context.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/TableFunctionS3Lambda.h>
#include <TableFunctions/parseColumnsListForTableFunction.h>
#include <Parsers/ASTLiteral.h>
#include "registerTableFunctions.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int LIMIT_EXCEEDED;
}

void TableFunctionS3Lambda::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    /// Parse args
    ASTs & args_func = ast_function->children;

    const auto message = fmt::format(
        "The signature of table function {} could be the following:\n" \
        " - url, format, structure\n" \
        " - url, format, structure, compression_method\n",
        getName());

    if (args_func.size() != 1)
        throw Exception("Table function '" + getName() + "' must have arguments.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    ASTs & args = args_func.at(0)->children;

    if (args.size() < 3 || args.size() > 4)
        throw Exception(message, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    for (auto & arg : args)
        arg = evaluateConstantExpressionOrIdentifierAsLiteral(arg, context);

    /// Size -> argument indexes
    static auto size_to_args = std::map<size_t, std::map<String, size_t>>
    {
        {3, {{"format", 1}, {"structure", 2}}},
        {4, {{"format", 1}, {"structure", 2}, {"compression_method", 3}}},
    };

    /// This argument is always the first
    filename = args[0]->as<ASTLiteral &>().value.safeGet<String>();

    auto & args_to_idx = size_to_args[args.size()];

    if (args_to_idx.contains("format"))
        format = args[args_to_idx["format"]]->as<ASTLiteral &>().value.safeGet<String>();

    if (args_to_idx.contains("structure"))
        structure = args[args_to_idx["structure"]]->as<ASTLiteral &>().value.safeGet<String>();

    if (args_to_idx.contains("compression_method"))
        compression_method = args[args_to_idx["compression_method"]]->as<ASTLiteral &>().value.safeGet<String>();
}

ColumnsDescription TableFunctionS3Lambda::getActualTableStructure(ContextPtr context) const
{
    return parseColumnsListFromString(structure, context);
}

StoragePtr TableFunctionS3Lambda::executeImpl(const ASTPtr & /*ast_function*/, ContextPtr context, const std::string & table_name, ColumnsDescription /*cached_columns*/) const
{
    Poco::URI uri (filename);
    S3::URI s3_uri (uri);
    UInt64 max_single_read_retries = context->getSettingsRef().s3_max_single_read_retries;
    UInt64 min_upload_part_size = context->getSettingsRef().s3_min_upload_part_size;
    UInt64 max_single_part_upload_size = context->getSettingsRef().s3_max_single_part_upload_size;
    UInt64 max_connections = context->getSettingsRef().s3_max_connections;
    UInt64 lambda_parallelism = context->getSettingsRef().s3_lambda_par;

    if (lambda_parallelism > 512)
        throw Exception("Can't use a parallelism higher than 512. Safety limit.", ErrorCodes::LIMIT_EXCEEDED);

    if (lambda_parallelism == 0)
        lambda_parallelism = 2;

    StoragePtr storage;

    if (lambda_parallelism == 1 || context->getClientInfo().query_kind == ClientInfo::QueryKind::SECONDARY_QUERY)
    {
         storage = StorageS3::create(
            s3_uri,
            access_key_id,
            secret_access_key,
            StorageID(getDatabaseName(), table_name),
            format,
            max_single_read_retries,
            min_upload_part_size,
            max_single_part_upload_size,
            max_connections,
            getActualTableStructure(context),
            ConstraintsDescription{},
            String{},
            context,
            compression_method);
    }
    else
    {
        storage = StorageS3Lambda::create(
            filename,
            access_key_id,
            secret_access_key,
            StorageID(getDatabaseName(), table_name),
            format,
            max_connections,
            lambda_parallelism,
            getActualTableStructure(context),
            ConstraintsDescription{},
            context,
            compression_method);
    }

    storage->startup();

    return storage;
}


void registerTableFunctionS3Lambda(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionS3Lambda>();
}

}

#endif
