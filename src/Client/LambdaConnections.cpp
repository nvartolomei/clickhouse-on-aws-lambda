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


LambdaConnections::LambdaConnections(const LambdaConnectionSettings & settings_) : settings(settings_)
{
}
void LambdaConnections::sendScalarsData(Scalars & data)
{
    UNUSED(data);
    throw Exception("not implemented", ErrorCodes::NOT_IMPLEMENTED);
}
void LambdaConnections::sendExternalTablesData(std::vector<ExternalTablesData> & data)
{
    UNUSED(data);
    throw Exception("not implemented", ErrorCodes::NOT_IMPLEMENTED);
}
void LambdaConnections::sendQuery(
    const ConnectionTimeouts & timeouts,
    const String & query,
    const String & query_id,
    UInt64 stage,
    const ClientInfo & client_info,
    bool with_pending_data)
{
    UNUSED(timeouts);
    UNUSED(query);
    UNUSED(query_id);
    UNUSED(stage);
    UNUSED(client_info);
    UNUSED(with_pending_data);
    throw Exception("not implemented", ErrorCodes::NOT_IMPLEMENTED);
}

void LambdaConnections::sendReadTaskResponse(const String & string)
{
    UNUSED(string);
    throw Exception("not implemented", ErrorCodes::NOT_IMPLEMENTED);
}

Packet LambdaConnections::receivePacket()
{
    throw Exception("not implemented", ErrorCodes::NOT_IMPLEMENTED);
}
Packet LambdaConnections::receivePacketUnlocked(AsyncCallback async_callback)
{
    UNUSED(async_callback);
    throw Exception("not implemented", ErrorCodes::NOT_IMPLEMENTED);
}
void LambdaConnections::disconnect()
{
    // throw Exception("not implemented", ErrorCodes::NOT_IMPLEMENTED);
}
void LambdaConnections::sendCancel()
{
    throw Exception("not implemented", ErrorCodes::NOT_IMPLEMENTED);
}
void LambdaConnections::sendIgnoredPartUUIDs(const std::vector<UUID> & uuids)
{
    UNUSED(uuids);
    throw Exception("not implemented", ErrorCodes::NOT_IMPLEMENTED);
}
Packet LambdaConnections::drain()
{
    throw Exception("not implemented", ErrorCodes::NOT_IMPLEMENTED);
}
std::string LambdaConnections::dumpAddresses() const
{
    throw Exception("not implemented", ErrorCodes::NOT_IMPLEMENTED);
}
size_t LambdaConnections::size() const
{
    throw Exception("not implemented", ErrorCodes::NOT_IMPLEMENTED);
}
bool LambdaConnections::hasActiveConnections() const
{
    throw Exception("not implemented", ErrorCodes::NOT_IMPLEMENTED);
}
}
