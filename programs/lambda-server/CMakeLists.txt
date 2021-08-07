set(CLICKHOUSE_LAMBDA_SERVER_SOURCES
    LambdaServer.cpp
)

set (CLICKHOUSE_LAMBDA_SERVER_LINK
    PRIVATE
        clickhouse_aggregate_functions
        clickhouse_common_config
        clickhouse_common_io
        clickhouse_common_zookeeper
        clickhouse_dictionaries
        clickhouse_functions
        clickhouse_parsers
        clickhouse_storages_system
        clickhouse_table_functions
        string_utils
        jemalloc

    PUBLIC
        daemon
)

clickhouse_program_add(lambda-server)
