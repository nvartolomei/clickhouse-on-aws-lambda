//
// Created by Nicolae Vartolomei on 10/08/2021.
//

#pragma once

#include <Client/IConnections.h>
#include <Interpreters/ClientInfo.h>
#include <aws/lambda/LambdaClient.h>

namespace DB
{
struct LambdaConnectionContext
{
    const String function_name;
    const Strings tasks;
    const std::shared_ptr<Aws::Lambda::LambdaClient> lambda_client;
};

class LambdaConnections final : public IConnections
{
private:
    Poco::Logger *log;

    LambdaConnectionContext lambda_connection_context;

    mutable std::mutex state_mutex;
    bool query_sent = false;
    bool cancelled = false;
    bool active_query = false;
    bool table_structure_done = false;
    bool done = false;

    // query to run
    ConnectionTimeouts timeouts;
    String query;
    String query_id;
    UInt64 to_stage;
    ClientInfo client_info;

    // response
    Block block;
public:
    explicit LambdaConnections(const LambdaConnectionContext & settings);

    void sendScalarsData(Scalars & data) override;
    void sendExternalTablesData(std::vector<ExternalTablesData> & data) override;

    void sendQuery(
        const ConnectionTimeouts & timeouts,
        const String & query,
        const String & query_id,
        UInt64 to_stage,
        const ClientInfo & client_info,
        bool with_pending_data) override;

    void sendReadTaskResponse(const String & string) override;
    Packet receivePacket() override;
    Packet receivePacketUnlocked(AsyncCallback async_callback) override;
    void disconnect() override;
    void sendCancel() override;
    void sendIgnoredPartUUIDs(const std::vector<UUID> & uuids) override;
    Packet drain() override;
    std::string dumpAddresses() const override;
    size_t size() const override;
    bool hasActiveConnections() const override;
};
}
