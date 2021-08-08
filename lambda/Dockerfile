FROM ubuntu:20.04

RUN echo 1
RUN apt-get update && apt-get install --yes ca-certificates && apt-get clean
COPY ./tmp/clickhouse /clickhouse

CMD ["/clickhouse", "lambda-server"]
