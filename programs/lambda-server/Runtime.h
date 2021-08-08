//
// Created by Nicolae Vartolomei on 07/08/2021.
//

#pragma once

#include <Interpreters/Context.h>

namespace DB
{
class Runtime
{
    ContextMutablePtr global_context;

public:
    int run(int argc, char ** argv);
    std::string handleRequest(std::string const& input);
};
}
