// #include <utils/Common.h>
#include <gbase/net/GSyncServer.hpp>
#include <sstream>
#include <iostream>
#include <gbase/logging/gLog.h>
#include "message.h"

int main()
{
    //  define server object
    gbase::net::GSyncServer server(9999);
    // gbase::net::GAsyncServer<> asyncServer;
    server.init();
    server.start();

    // will not reach unless ctrl+c
    return 0;
}