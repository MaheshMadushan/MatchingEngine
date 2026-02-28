#include <GServer.h>

namespace gbase::net
{
    template <GEventHandlingMode = SYNC>
    class GSyncServer : public GServer<SYNC>
    {
    public:
        GSyncServer(int port = 8080) : GServer(port) {};
        ~GSyncServer() = default;

        GSyncServer(GSyncServer const &) = delete;
        GSyncServer(GSyncServer &&) = delete;
        GSyncServer &operator=(GSyncServer const &) = delete;
        GSyncServer &operator=(GSyncServer &&) = delete;

        void start();
    };
}
