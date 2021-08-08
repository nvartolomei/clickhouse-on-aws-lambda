FROM debian:buster-slim

RUN apt-get update && \
        apt-get install --yes ca-certificates && \
        rm -rf /var/lib/apt/lists/*

# ADD https://github.com/aws/aws-lambda-runtime-interface-emulator/releases/latest/download/aws-lambda-rie /usr/bin/aws-lambda-rie

COPY ./aws-lambda-rie /usr/bin/aws-lambda-rie
RUN chmod +x /usr/bin/aws-lambda-rie

COPY ./entrypoint.sh /entrypoint.sh

RUN echo 1
COPY ./tmp/clickhouse /usr/bin/clickhouse

ENTRYPOINT ["/entrypoint.sh"]
