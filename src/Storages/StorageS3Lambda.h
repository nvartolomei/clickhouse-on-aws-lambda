#pragma once

#if !defined(ARCADIA_BUILD)
#include <Common/config.h>
#endif

#if USE_AWS_S3

#include <memory>
#include <optional>

#include <common/shared_ptr_helper.h>

#include "Client/Connection.h"
#include <IO/S3Common.h>
#include <Storages/StorageS3.h>

namespace DB
{

class Context;

class StorageS3Lambda : public shared_ptr_helper<StorageS3Lambda>, public IStorage
{
    friend struct shared_ptr_helper<StorageS3Lambda>;
public:
    std::string getName() const override { return "S3Lambda"; }

    Pipe read(const Names &, const StorageMetadataPtr &, SelectQueryInfo &,
        ContextPtr, QueryProcessingStage::Enum, size_t /*max_block_size*/, unsigned /*num_streams*/) override;

    QueryProcessingStage::Enum
    getQueryProcessingStage(ContextPtr, QueryProcessingStage::Enum, const StorageMetadataPtr &, SelectQueryInfo &) const override;

    NamesAndTypesList getVirtuals() const override;

protected:
    StorageS3Lambda(
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
        const String & compression_method_);

private:
    StorageS3::ClientAuthentication client_auth;

    String filename;
    String cluster_name;
    String format_name;
    UInt64 lambda_parallelism;
    String compression_method;
};


}

#endif
