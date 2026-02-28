#include <GSyncServer.hpp>
#include <logging/gLog.h>

using namespace gbase::net::l1;
using namespace gbase::net;
template <>
void GSyncServer<>::start()
{
    GLOG_DEBUG_L1("Sync Server loop started");
    std::string static_message{""};
    while (true)
    {
        FD_ZERO(&writefds);
        FD_ZERO(&readfds);
        FD_SET(m_serverSocket.getSocketFileDescriptor(), &readfds);
        int maxfd = m_serverSocket.getSocketFileDescriptor();
        for (auto client_fd : m_clientSockets)
        {
            FD_SET(client_fd, &readfds);
            if (static_message.size() > 0 || protocol.shouldMonitorIPCChannelForWrite(client_fd) == true)
                FD_SET(client_fd, &writefds);
            if (client_fd > maxfd)
                maxfd = client_fd;
        }

        // wait until either socket has data ready to be recv()d (timeout 10.5 secs)
        tv.tv_sec = 10;
        tv.tv_usec = 500000;
        int rv = -1;
        rv = select(maxfd + 1, &readfds, &writefds, NULL, &tv);
        if (rv != -1)
        {
            if (FD_ISSET(m_serverSocket.getSocketFileDescriptor(), &readfds))
            {
                GLOG_DEBUG_L1("Client Connected")
                G_SOCKETFD client = m_serverSocket.accept();
                m_clientSockets.push_back(client);
                protocol.onConnect(client);
                continue;
            }

            int index = 0;
            for (auto client_fd : m_clientSockets)
            {
                gbase::net::gProtocol::State clientState = protocol.getState(client_fd);

                ++index;
                if (FD_ISSET(client_fd, &readfds) == true)
                {
                    std::shared_ptr<ByteBuffer<std::byte>> p_byteBuffer{m_serverSocket.receive(client_fd)};
                    if (p_byteBuffer.get()->get_filled_size() == 0)
                    {
                        GLOG_DEBUG_L1("client {} closed connection", client_fd);
                        m_serverSocket.closeSocket(client_fd);
                        m_clientSockets.erase(m_clientSockets.begin() + index - 1);
                        protocol.onDisconnect(client_fd);
                        continue;
                    }
                    GLOG_DEBUG_L1("recieved from client {} - data {}", client_fd, gbase::byte_arra_as_string(*p_byteBuffer));
                    ByteBuffer<std::byte> recieved_bytes{protocol.recieve(client_fd, *p_byteBuffer)};
                    if (recieved_bytes.get_filled_size() > 0)
                    {
                        static_message = "Data recieved dear clientele.";
                        GLOG_INFO("client data from client {} - {}", client_fd, gbase::byte_array_2_string(recieved_bytes));
                    }
                    // this way protocol is IPC method agnostic
                }

                if (FD_ISSET(client_fd, &writefds) == true)
                {
                    ByteBuffer<std::byte> static_message_bytes;
                    if (clientState == gbase::net::gProtocol::State::CONNECTED || clientState == gbase::net::gProtocol::State::IDLE)
                    {
                        static_message.size() > 0 ? static_message_bytes.append(static_message.c_str(), static_message.size()) : static_message_bytes.release();
                        static_message = "";
                    }
                    ByteBuffer<std::byte> bytes_to_send{protocol.send(client_fd, static_message_bytes)};

                    if (bytes_to_send.get_filled_size() > 0)
                    {
                        GLOG_DEBUG_L1("send to client {} - data {}", client_fd, gbase::byte_arra_as_string(bytes_to_send));
                        m_serverSocket.send(client_fd, bytes_to_send);
                    }

                    // this way protocol is IPC method agnostic
                }
            }
        }
    }
}