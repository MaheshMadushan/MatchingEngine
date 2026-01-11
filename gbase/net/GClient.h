#pragma once

#include <utils/Common.h>
#include <GSocket.h>
#include <net/Defs.h>
#include <logging/gLog.h>
#include <sys/eventfd.h>
#include <type_traits>
#include <memory>
#include <ByteBuffer.hpp>
#include <gProtocol.hpp>

namespace gbase::net
{
    using namespace gbase::net::l1;
    template <GEventHandlingMode E, typename T>
        requires std::is_base_of_v<gbase::ByteBuffer<std::byte>, T>
    class GClient
    {

    public:
        GClient() = default;
        virtual ~GClient() noexcept
        {
            close();
        };

        GClient(GClient const &) = delete;      // no copy
        GClient(GClient &&) = delete;           // no move
        GClient operator=(GClient &) = delete;  // no copy assignment
        GClient operator=(GClient &&) = delete; // no move assignment

        [[nodiscard]] static GEventHandlingMode getMode() noexcept
        {
            return m_serverMode;
        }

        void connect(const char *ip, int port)
        {
            if (ip == nullptr || port <= 0)
            {
                GLOG_ERROR("Invalid IP address or port");
                exit(1);
                return;
            }

            if (!clientSocket.create() || !clientSocket.connect(ip, port))
            {
                GLOG_ERROR("Client failed to connect {} {}", errno, strerror(errno));
                exit(1);
                return;
            }
            client_protocol.onClientConnect(clientSocket.getSocketFileDescriptor());
        }

        template <typename U = T>
        void send(T &&ss) noexcept
        {
            this->send(std::forward<U>(ss));
        }

        void close()
        {
            clientSocket.closeSelf();
        }

    protected:
        virtual void onResponse(std::string &&message) = 0;

        virtual void send(T &ss) noexcept = 0;
        virtual void send(const T &ss) noexcept = 0;
        virtual void send(T &&ss) noexcept = 0;

        static constexpr GEventHandlingMode m_serverMode{E};
        GSocket clientSocket;
        gbase::net::gProtocol::v1::server::Protocol client_protocol{};
    };

    template <typename T>
        requires std::is_base_of_v<ByteBuffer<std::byte>, T>
    class GSyncClient : public GClient<GEventHandlingMode::SYNC, T>
    {
    public:
        GSyncClient() : GClient<GEventHandlingMode::SYNC, T>() {};

        ~GSyncClient() override = default;

        GSyncClient(GSyncClient const &) = delete;      // no copy
        GSyncClient(GSyncClient &&) = delete;           // no move
        GSyncClient operator=(GSyncClient &) = delete;  // no copy assignment
        GSyncClient operator=(GSyncClient &&) = delete; // no move assignment

    protected:
        void onResponse(std::string &&message) override = 0;

        void send(T &ss) noexcept override
        {
            this->clientSocket.send(ss);
            // onResponse(this->clientSocket.receiveData());
        };

        void send(const T &ss) noexcept override
        {
            this->clientSocket.send(ss);
            // onResponse(this->clientSocket.receiveData());
        };

        void send(T &&ss) noexcept override
        {
            this->clientSocket.send(std::move(ss));
            // onResponse(this->clientSocket.receiveData());
            // ss.clear();
        };
    };

    template <typename T>
        requires std::is_base_of_v<gbase::ByteBuffer<std::byte>, T>
    class GAsyncClient : public GClient<GEventHandlingMode::ASYNC, T>
    {
    public:
        GAsyncClient() : GClient<GEventHandlingMode::ASYNC, T>() {};
        virtual ~GAsyncClient() = default;

        GAsyncClient(GAsyncClient const &) = delete;      // no copy
        GAsyncClient(GAsyncClient &&) = delete;           // no move
        GAsyncClient operator=(GAsyncClient &) = delete;  // no copy assignment
        GAsyncClient operator=(GAsyncClient &&) = delete; // no move assignment

        void start() noexcept
        {
            GLOG_DEBUG_L1("Starting async client event loop...");
            int maxfd = 0;
            eventfd_t holdingEvent = 0;
            eventNotifyingFileDiscriptor = eventfd(0, EFD_SEMAPHORE);
            std::string static_message = "Hi from server";
            while (true)
            {
                FD_ZERO(&writefds);
                FD_ZERO(&readfds);
                FD_SET(this->clientSocket.getSocketFileDescriptor(), &readfds);
                FD_SET(this->clientSocket.getSocketFileDescriptor(), &writefds);
                FD_SET(eventNotifyingFileDiscriptor, &readfds);
                maxfd = eventNotifyingFileDiscriptor;

                // wait until either socket has data ready to be recv()d (timeout 10.5 secs)
                tv.tv_sec = 10;
                tv.tv_usec = 500000;
                int rv = -1;
                rv = select(maxfd + 1, &readfds, holdingEvent == Event::MESSAGE_BUFFERRED ? &writefds : NULL, NULL, &tv);
                if (rv != -1)
                {
                    if (FD_ISSET(this->clientSocket.getSocketFileDescriptor(), &readfds) == true)
                    {
                        std::shared_ptr<ByteBuffer<std::byte>> p_byteBuffer{this->clientSocket.receive(this->clientSocket.getSocketFileDescriptor())};
                        GLOG_DEBUG_L1("read from client {}", this->clientSocket.getSocketFileDescriptor());
                        if (p_byteBuffer.get()->get_filled_size() == 0)
                        {
                            GLOG_DEBUG_L1("client {} closed connection", this->clientSocket.getSocketFileDescriptor());
                            this->clientSocket.closeSocket(this->clientSocket.getSocketFileDescriptor());
                            // m_clientSockets.erase(m_clientSockets.begin() + index - 1);
                            this->client_protocol.onClientDisconnect(this->clientSocket.getSocketFileDescriptor());
                            continue;
                        }
                        ByteBuffer<std::byte> recieved_bytes{this->client_protocol.recieve(this->clientSocket.getSocketFileDescriptor(), *p_byteBuffer)};
                        if (recieved_bytes.get_filled_size() > 0)
                            GLOG_DEBUG_L1("recieved data from server sent data - {}", gbase::byte_array_2_string(recieved_bytes));
                        // this way protocol is IPC method agnostic
                    }

                    if (FD_ISSET(this->clientSocket.getSocketFileDescriptor(), &writefds) == true)
                    {
                        // std::string static_message = "Hi from server";
                        // GLOG_INFO("send to client {}", static_message)
                        ByteBuffer<std::byte> static_message_bytes;
                        static_message.size() > 0 ? static_message_bytes.append(static_message.c_str(), static_message.size()) : static_message_bytes.release();
                        static_message.clear();
                        ByteBuffer<std::byte> bytes_to_send{this->client_protocol.send(this->clientSocket.getSocketFileDescriptor(), static_message_bytes)};
                        GLOG_DEBUG_L1("send to client {} - data {}", this->clientSocket.getSocketFileDescriptor(), gbase::byte_arra_as_string(bytes_to_send));
                        if (bytes_to_send.get_filled_size() > 0)
                            this->clientSocket.send(this->clientSocket.getSocketFileDescriptor(), bytes_to_send);
                        // this way protocol is IPC method agnostic
                    }
                    /*GLOG_DEBUG_L1("Select returned {}", rv);
                    if (FD_ISSET(eventNotifyingFileDiscriptor, &readfds) &&
                        Event::NONE == static_cast<Event>(holdingEvent))
                    {
                        eventfd_read(eventNotifyingFileDiscriptor, &holdingEvent);
                        GLOG_DEBUG_L1("event read :- {}", holdingEvent);
                    }

                    if (FD_ISSET(this->clientSocket.getSocketFileDescriptor(), &readfds) == true)
                    {
                        GLOG_DEBUG_L1("Data available to read...");
                        auto request = this->clientSocket.receiveData().c_str();
                        if (strlen(request) == 0)
                        {
                            this->clientSocket.closeSelf();
                            break;
                        }
                        // incomingMsgQueue.push(request);
                    }

                    if (FD_ISSET(this->clientSocket.getSocketFileDescriptor(), &writefds) == true &&
                        Event::MESSAGE_BUFFERRED == static_cast<Event>(holdingEvent))
                    {
                        GLOG_DEBUG_L1("Data available to write...");
                        // if (outgoingMsgQueue.empty())
                        // {
                        //     holdingEvent = static_cast<eventfd_t>(Event::NONE);
                        //     continue;
                        // }
                        // const char *data = nullptr;
                        // auto hurray = outgoingMsgQueue.pop(data);
                        // if (hurray)
                        // {
                        //     printf("%p\n", data);
                        //     GLOG_DEBUG_L1("sending data... {}", *data);
                        //     printf("%s\n", data);
                        //     this->clientSocket.sendData(data);
                        // }
                    }*/
                }
            }
        }

        template <typename U = T>
        void send(T &&ss) noexcept
        {
            GLOG_DEBUG_L1("Queueing message to send... forwarding");

            this->send(std::forward<U>(ss));
            eventfd_write(eventNotifyingFileDiscriptor, static_cast<int>(Event::MESSAGE_BUFFERRED));
        }

    protected:
        void onResponse([[maybe_unused]] std::string &&message) override {};

        void send(T &bb) noexcept override {
            // GLOG_DEBUG_L1("Queueing message to send... {}", ss.str());
            // this->clientSocket.sendData(bb);
            // std::string str = ss.str(); // make a copy to ensure data validity
            // outgoingMsgQueue.push(str.c_str());
        };

        void send(const T &bb) noexcept override {
            // GLOG_DEBUG_L1("Queueing message to send... {}", ss.str());

            // this->clientSocket.sendData(bb);

            // std::string str = ss.str(); // make a copy to ensure data validity
            // outgoingMsgQueue.push(str.c_str());
        };

        void send(T &&bb) noexcept override {
            // std::string str{ss.str()};
            // GLOG_DEBUG_L1("Queueing message to send temp - {}", str);
            // this->clientSocket.sendData(bb);
            // printf("%p\n", str.c_str());
            // char* msg = new char[str.size() + 1];
            // strncpy(msg, str.c_str(), str.size());
            // msg[str.size()] = '\0';
            // printf("%s\n", *msg);
            // outgoingMsgQueue.push(msg);
            // ss.clear();
        };

    private:
        // boost::lockfree::queue<gbase::ByteBuffer<std::byte>> incomingMsgQueue{1024};
        // boost::lockfree::queue<gbase::ByteBuffer<std::byte>> outgoingMsgQueue{1024};

        fd_set readfds;
        fd_set writefds;

        struct timeval tv;

        eventfd_t eventNotifyingFileDiscriptor{0};
    };

} // namespace gbase::net
