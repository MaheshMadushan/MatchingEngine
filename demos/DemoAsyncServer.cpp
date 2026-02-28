// #include <utils/Common.h>
#include <gbase/net/GAsyncServer.hpp>
#include <sstream>
#include <iostream>
#include <gbase/logging/gLog.h>
#include <thread>
#include "message.h"

int main()
{
    //  define server object
    gbase::net::GAsyncServer asyncServer; // at 8080
    // asyncServer.init();
    // asyncServer.start();

    // will not reach unless ctrl+c
    return 0;
}