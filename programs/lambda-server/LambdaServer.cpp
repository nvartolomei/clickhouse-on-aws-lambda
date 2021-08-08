#include "Runtime.h"

#include <Common/Exception.h>
#include <iostream>

int mainEntryClickHouseLambdaServer(int argc, char ** argv)
{
    DB::Runtime runtime;
    try
    {
        return runtime.run(argc, argv);
    }
    catch (...)
    {
        std::cerr << DB::getCurrentExceptionMessage(true) << "\n";
        auto code = DB::getCurrentExceptionCode();
        return code ? code : 1;
    }
}
