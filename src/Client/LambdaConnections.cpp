//
// Created by Nicolae Vartolomei on 10/08/2021.
//

#include <Client/LambdaConnections.h>

namespace DB
{
namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
}

LambdaConnections::LambdaConnections(const LambdaConnectionContext & settings_)
    : log(&Poco::Logger::get("LambdaConnections")), lambda_connection_context(settings_)
{
}

void LambdaConnections::sendScalarsData(Scalars & data)
{
    LOG_TRACE(log, "Sending scalars ({}): {}", data.size(), fmt::join(data | boost::adaptors::map_keys, ", "));

    if (!data.empty())
        throw Exception("sending scalars not implemented", ErrorCodes::NOT_IMPLEMENTED);
}

void LambdaConnections::sendExternalTablesData(std::vector<ExternalTablesData> & data)
{
    const auto payloads = data.size();

    std::vector<String> tables;
    std::transform(data[0].cbegin(), data[0].cend(), std::back_inserter(tables),
                   [](const auto & table_data) { return table_data->table_name; });
    LOG_TRACE(log, "sendExternalTablesData, payloads: {}; first: {}", payloads, fmt::join(tables, ", "));

    if (payloads > 1 || data[0].size() > 1)
        throw Exception("sending external tables not implemented", ErrorCodes::NOT_IMPLEMENTED);
}

void LambdaConnections::sendQuery(
    const ConnectionTimeouts & timeouts_,
    const String & query_,
    const String & query_id_,
    UInt64 stage_,
    const ClientInfo & client_info_,
    bool with_pending_data)
{
    UNUSED(with_pending_data);

    timeouts = timeouts_;
    query = query_;
    query_id = query_id_;
    stage = stage_;
    client_info = client_info_;
}

void LambdaConnections::sendReadTaskResponse(const String & string)
{
    UNUSED(string);
    throw Exception("not implemented", ErrorCodes::NOT_IMPLEMENTED);
}

Packet LambdaConnections::receivePacket()
{
    LOG_TRACE(log, "receivePacket");

    if (cancelled)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "unexpected receivePacket call on a cancelled query/connection");

    std::scoped_lock lock(state_mutex);

    /// Empty packet, end of data.
    if (done)
    {
        Packet res;
        res.type = Protocol::Server::EndOfStream;
        return res;
    }
    else if (!table_structure_done)
    {
        table_structure_done = true;

        /// Return table structure.
        Packet res;
        res.type = Protocol::Server::Data;

        return res;
    }
    else
    {
        done = true;

        /// Return data, if any.
        Packet res;
        res.type = Protocol::Server::Data;

        return res;
    }
}

Packet LambdaConnections::receivePacketUnlocked(AsyncCallback async_callback)
{
    UNUSED(async_callback);
    throw Exception("receive unlocked not implemented", ErrorCodes::NOT_IMPLEMENTED);
}

void LambdaConnections::disconnect()
{
    std::scoped_lock lock(state_mutex);
    cancelled = true;
}

void LambdaConnections::sendCancel()
{
    std::scoped_lock lock(state_mutex);
    cancelled = true;
}

void LambdaConnections::sendIgnoredPartUUIDs(const std::vector<UUID> & uuids)
{
    UNUSED(uuids);
    throw Exception("not implemented", ErrorCodes::NOT_IMPLEMENTED);
}

Packet LambdaConnections::drain()
{
    auto p = Packet{};
    p.type = Protocol::Server::EndOfStream;
    return p;
}

std::string LambdaConnections::dumpAddresses() const
{
    return lambda_connection_context.function_name;
}

size_t LambdaConnections::size() const
{
    return 1;
}

bool LambdaConnections::hasActiveConnections() const
{
    std::scoped_lock lock(state_mutex);
    return active_query;
}
}
