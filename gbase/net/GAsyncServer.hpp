#pragma once

#include <GServer.h>

namespace gbase::net
{
    template <GEventHandlingMode = ASYNC>
    class GAsyncServer : public GServer<ASYNC>
    {
    private:
        std::map<G_SOCKETFD, std::queue<ByteBuffer<std::byte>>> incomingMsgBuffer;
        std::map<G_SOCKETFD, std::queue<ByteBuffer<std::byte>>> outgoingMsgBuffer;

        G_EVENTFD eventNotifyingFileDiscriptor;

    public:
        GAsyncServer(int port = 8080) : GServer(port) {};
        ~GAsyncServer() = default;

        GAsyncServer(GAsyncServer const &) = delete;
        GAsyncServer(GAsyncServer &&) = delete;
        GAsyncServer &operator=(GAsyncServer const &) = delete;
        GAsyncServer &operator=(GAsyncServer &&) = delete;

        void start();

        // virtual void send(const G_SOCKETFD &client, const ByteBuffer<std::byte> &data);
    };

    
} // namespace GServer
