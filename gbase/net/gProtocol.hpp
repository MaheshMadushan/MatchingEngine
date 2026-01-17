#pragma once

#include <type_traits>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <ByteBuffer.hpp>
#include <flat_map> // C++23 header
#include <queue>
#include <bitset>

#include <logging/gLog.h>

constexpr uint8_t __G_PROTOCOL_MAJOR_VERSION__ = 1;
/*
    gProtocol - Defines how application handles message delivery to another process via network.
        Version 1
            supports application level message delivery via TCP/IP protocol.
            Defines how to identify start and end of message byte stream

            +----------------------------------------------------------+
            |               (Application Layer) gProtocol              |
            +----------------------------------------------------------+
            |                         TCP                              |
            +----------------------------------------------------------+
            |                    internet protocol                     |
            +----------------------------------------------------------+
            |                  communication network                   |
            +----------------------------------------------------------+

            Frame
                - header - 1 byte
                    - start of message  = 0x1
                    - message partition = 0x2
                - packet
                    - size of data - 16 bytes
                    - data - (size of data) bytes

                    0                   1
                    0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5
                    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
                    |     header    | proto version |
                    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
                    |         size of data          |
                    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
                    ~            data               ~
                    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

            protocol negotiation
                Client                          Server
                   |                               |
                   |------------startSession------>|
                   |<------------AckStart----------|
                   |                               |
                   |                               |
                   |----------StartDataTrans------>|
                   |<-------------Acked------------|
                   |-------------SendData--------->|
                   |-------------SendData--------->|
                   |-------------SendData--------->|
                   |-------------SendData--------->|
                   |-----------EndDataTrans------->|
                   |<-------------Acked------------|
                   |                               |
                   |<----------StartDataTrans------|
                   |--------------Acked----------->|
                   |<------------RecData-----------|
                   |<------------RecData-----------|
                   |<------------RecData-----------|
                   |<------------RecData-----------|
                   |<----------EndDataTrans--------|
                   |--------------Acked----------->|
                   |                               |
                   |                               |
                   |-----------EndSession--------->|
                   |<-------------Acked------------|


*/
namespace gbase::net::gProtocol
{

    static constexpr uint8_t START_SESSION = static_cast<uint8_t>(0x1);
    static constexpr uint8_t START_SESSION_ACK = static_cast<uint8_t>(0x2);
    static constexpr uint8_t END_SESSION = static_cast<uint8_t>(0x3);
    static constexpr uint8_t END_SESSION_ACK = static_cast<uint8_t>(0x4);
    static constexpr uint8_t START_DATA_TRANSMISSION = static_cast<uint8_t>(0x5);
    static constexpr uint8_t START_DATA_TRANSMISSION_ACK = static_cast<uint8_t>(0x6);
    static constexpr uint8_t END_DATA_TRANSMISSION = static_cast<uint8_t>(0x7);
    static constexpr uint8_t END_DATA_TRANSMISSION_ACK = static_cast<uint8_t>(0x8);
    static constexpr uint8_t DATA_ARRIVAL = static_cast<uint8_t>(0x9);
    static constexpr uint8_t DATA_RECEIVED_BY_CLIENT = static_cast<uint8_t>(0xA);

    enum State : int
    {
        APPLICATION_DATA_TRANSMISSION_COMPLETED,
        APPLICATION_DATA_TRANSMITTING,
        APPLICATION_DATA_RECEIVING,
        APPLICATION_DATA_RECEPTION_COMPLETED,
        START_APPLICATION_DATA_TRANSMISSION_ACK_WAITING,
        START_APPLICATION_DATA_TRANSMISSION_ACK_RECEIVED,
        END_APPLICATION_DATA_TRANSMISSION_ACK_WAITING,
        END_APPLICATION_DATA_TRANSMISSION_ACK_RECEIVED,

        CONNECTED,
        IDLE,

        UNDEFINED
    };

    enum TrasnmittingDataType : int
    {
        APPLICATION_DATA,
        PROTOCOL_DATA
    };

    class Protocol
    {

    private:
        struct DataWithMetaData
        {
            size_t __size_of_data__ = 0x0;
            TrasnmittingDataType _data_type_;
            gbase::ByteBuffer<std::byte> _data_;
        };

        using ClientId = int;
        using QueueOfData = std::queue<DataWithMetaData>;

        std::flat_map<ClientId, State> __client_states;
        std::flat_map<ClientId, QueueOfData> __data_waiting_to_sent;
        std::flat_map<ClientId, QueueOfData> __data_waiting_to_receive;

        uint16_t __header_and_proto_version__ = 0x0;
        uint16_t __size_of_data__ = 0x0;

    public:
        Protocol() = default;
        ~Protocol() = default;

        // for testing
        [[nodiscard]] auto getClientStates() const -> std::flat_map<ClientId, State>;
        [[nodiscard]] auto getDataWaitingToSent() const -> std::flat_map<ClientId, QueueOfData>;
        [[nodiscard]] auto getDataWaitingToRecieve() const -> std::flat_map<ClientId, QueueOfData>;

        [[nodiscard]] auto getState(ClientId client_id) const -> State;
        [[nodiscard]] auto isClientHasDataToSent(ClientId client_id) const -> bool;

        auto shouldMonitorIPCChannelForSend(ClientId client_id) -> bool;

        void onConnect(ClientId client_id);
        void onDisconnect(ClientId client_id);

        [[nodiscard]] auto send(ClientId client_id, gbase::ByteBuffer<std::byte> &data) -> gbase::ByteBuffer<std::byte>;
        [[nodiscard]] auto recieve(ClientId client_id, gbase::ByteBuffer<std::byte> &data) -> gbase::ByteBuffer<std::byte>;
    }; // namespace server

} // namespace gbase::net::gProtocol
