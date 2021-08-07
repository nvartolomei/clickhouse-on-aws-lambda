set(AWS_LAMBDA_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/aws/aws-lambda-cpp/include")
set(AWS_LAMBDA_LIBRARY aws_lambda)
set(USE_AWS_LAMBDA 1)

message (STATUS "Using aws_lambda=${USE_AWS_LAMBDA}: ${AWS_LAMBDA_INCLUDE_DIR} : ${AWS_LAMBDA_LIBRARY}")
