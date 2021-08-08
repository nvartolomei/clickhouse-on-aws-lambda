set(CLICKHOUSE_LAMBDA_SERVER_SOURCES
    LambdaServer.cpp
    Runtime.cpp
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

#set (CLICKHOUSE_LAMBDA_SERVER_INCLUDE
#    PUBLIC
#        ${AWS_LAMBDA_INCLUDE_DIR}
#)
#
#message(STATUS "!!!")
#message(STATUS ${AWS_LAMBDA_INCLUDE_DIR})

clickhouse_program_add(lambda-server)

#target_link_libraries (clickhouse-lambda-server-lib PUBLIC ${AWS_LAMBDA_LIBRARY})
#target_include_directories (clickhouse-lambda-server-lib SYSTEM BEFORE PUBLIC $)
