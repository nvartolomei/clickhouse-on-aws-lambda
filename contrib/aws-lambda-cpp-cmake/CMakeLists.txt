SET(AWS_LAMBDA_LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/aws-lambda-cpp")


set(PROJECT_VERSION_MAJOR 1)
set(PROJECT_VERSION_MINOR 1)
set(PROJECT_VERSION_PATCH 1)
set(PROJECT_VERSION 1)

configure_file("${AWS_LAMBDA_LIBRARY_DIR}/src/version.cpp.in"
        "${CMAKE_CURRENT_BINARY_DIR}/src/version.cpp" @ONLY)

file(GLOB LAMBDA_SRC
    "${AWS_LAMBDA_LIBRARY_DIR}/src/*"
    "${CMAKE_CURRENT_BINARY_DIR}/src/*"
    "${CMAKE_CURRENT_BINARY_DIR}/src/version.cpp"
)

add_library(aws_lambda ${LAMBDA_SRC})

set(LAMBDA_INCLUDES
    "${CURL_INCLUDE_DIRS}"
    "${AWS_LAMBDA_LIBRARY_DIR}/include/"
)

target_link_libraries(aws_lambda PRIVATE ${CURL_LIBRARY})

target_include_directories(aws_lambda PUBLIC ${LAMBDA_INCLUDES})
