//
// Created by Nicolae Vartolomei on 07/08/2021.
//

#pragma once

#include <Interpreters/Context.h>
#include <Poco/Util/ServerApplication.h>

namespace DB
{
class Runtime : public Poco::Util::ServerApplication
{
    ContextMutablePtr global_context;

public:
    int main(const std::vector<std::string> &) override;
    std::string handleRequest(std::string const& input);

private:
    void initializeTerminationAndSignalProcessing();
};
}
