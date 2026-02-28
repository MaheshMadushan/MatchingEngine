#include <gProtocol.hpp>

using namespace gbase::net::gProtocol;

[[nodiscard]] auto Protocol::getClientStates() const -> std::flat_map<ClientId, State>
{
    return __client_states;
}

[[nodiscard]] auto Protocol::getDataWaitingToSent() const -> std::flat_map<ClientId, QueueOfData>
{
    return __data_waiting_to_sent;
}

[[nodiscard]] auto Protocol::getDataWaitingToRecieve() const -> std::flat_map<ClientId, QueueOfData>
{
    return __data_waiting_to_receive;
}

void Protocol::onConnect(Protocol::ClientId client_id)
{
    __client_states.emplace(client_id, State::CONNECTED);
}

void Protocol::onDisconnect(Protocol::ClientId client_id)
{
    __client_states.erase(client_id);
    __data_waiting_to_receive.erase(client_id);
    __data_waiting_to_sent.erase(client_id);
}

[[nodiscard]] auto Protocol::send(Protocol::ClientId client_id, gbase::ByteBuffer<std::byte> &data) -> gbase::ByteBuffer<std::byte>
{
    GLOG_DEBUG_L1("Server protocol send called client {}", client_id)
    __header_and_proto_version__ = 0x0;
    __size_of_data__ = 0x0;
    if (data.get_filled_size() > 0)
    {
        // application has data to sent
        GLOG_DEBUG_L1("application has data for client {}", client_id)
        __size_of_data__ = data.get_filled_size(); // fix warning
        __header_and_proto_version__ |= (uint16_t)START_DATA_TRANSMISSION << 8 | __G_PROTOCOL_MAJOR_VERSION__;

        gbase::ByteBuffer<std::byte> war_head;

        // send sot
        war_head.append(reinterpret_cast<const char *>(&__header_and_proto_version__), sizeof(uint16_t));
        war_head.append(reinterpret_cast<const char *>(&__size_of_data__), sizeof(uint16_t));

        if (const auto &q = __data_waiting_to_sent.find(client_id); q != __data_waiting_to_sent.end())
        {
            q->second.push({__size_of_data__, TrasnmittingDataType::PROTOCOL_DATA, std::move(war_head)});
            q->second.push({__size_of_data__, TrasnmittingDataType::APPLICATION_DATA, std::move(data)});
            GLOG_DEBUG_L1("queue size for client {} is {}", client_id, q->second.size())
        }
        else
        {
            QueueOfData new_q;
            new_q.push({__size_of_data__, TrasnmittingDataType::PROTOCOL_DATA, std::move(war_head)});
            new_q.push({__size_of_data__, TrasnmittingDataType::APPLICATION_DATA, std::move(data)});
            __data_waiting_to_sent.emplace(client_id, new_q);
        }
    }

    __header_and_proto_version__ = 0x0;
    __size_of_data__ = 0x0;
    gbase::ByteBuffer<std::byte> empty_data;
    if (const auto &itr = __client_states.find(client_id); itr != __client_states.end())
    {
        State client_state = itr->second;
        GLOG_DEBUG_L1("Server protocol client handle {} is in client state {}", client_id, static_cast<int>(client_state))
        switch (client_state)
        {
        case State::CONNECTED:
        case State::IDLE:
            if (const auto &itr = __data_waiting_to_sent.find(client_id); itr != __data_waiting_to_sent.end())
            {
                auto &q = itr->second;
                if (q.empty() == false && q.front()._data_type_ == TrasnmittingDataType::PROTOCOL_DATA)
                {
                    gbase::ByteBuffer<std::byte> pending_data{std::move(q.front()._data_)};
                    q.front()._data_.release();
                    q.pop();

                    uint16_t proto_header_and_version_to_send = 0x0;
                    pending_data.read<sizeof(uint16_t)>(reinterpret_cast<char *>(&proto_header_and_version_to_send));

                    uint8_t proto_header_to_send = proto_header_and_version_to_send >> 8;

                    
                    if (proto_header_to_send == END_DATA_TRANSMISSION_ACK)
                        __client_states[client_id] = State::IDLE;
                    if (proto_header_to_send == START_DATA_TRANSMISSION)
                        __client_states[client_id] = State::START_APPLICATION_DATA_TRANSMISSION_ACK_WAITING;
                    if (proto_header_to_send == END_DATA_TRANSMISSION)
                        __client_states[client_id] = State::END_APPLICATION_DATA_TRANSMISSION_ACK_WAITING;
                    if (proto_header_to_send == START_DATA_TRANSMISSION_ACK) // sends to client acking thatcliant starting data transmission
                        __client_states[client_id] = State::APPLICATION_DATA_RECEIVING;

                    // else - sending acks
                    return pending_data;
                }
            }
            break;
        case State::START_APPLICATION_DATA_TRANSMISSION_ACK_RECEIVED:
            if (const auto &itr = __data_waiting_to_sent.find(client_id); itr != __data_waiting_to_sent.end())
            {
                auto &pending_data = itr->second.front();
                if (pending_data._data_type_ == TrasnmittingDataType::APPLICATION_DATA)
                {
                    __header_and_proto_version__ |= (uint16_t)DATA_ARRIVAL << 8 | __G_PROTOCOL_MAJOR_VERSION__;
                    __size_of_data__ = pending_data._data_.get_filled_size();
                    gbase::ByteBuffer<std::byte> data_with_hdr;
                    data_with_hdr.append(reinterpret_cast<const char *>(&__header_and_proto_version__), sizeof(uint16_t));
                    data_with_hdr.append(reinterpret_cast<const char *>(&__size_of_data__), sizeof(uint16_t));
                    data_with_hdr.append(pending_data._data_.get().get(), pending_data._data_.get_filled_size());
                    pending_data._data_.release();
                    itr->second.pop();

                    __client_states[client_id] = State::APPLICATION_DATA_TRANSMITTING;
                    return data_with_hdr;
                }
                // else invalid protocol flow
            }
            break;
        case State::APPLICATION_DATA_TRANSMISSION_COMPLETED:
            __client_states[client_id] = State::END_APPLICATION_DATA_TRANSMISSION_ACK_WAITING;
            __header_and_proto_version__ |= (uint16_t)END_DATA_TRANSMISSION << 8 | __G_PROTOCOL_MAJOR_VERSION__;
            empty_data.append(reinterpret_cast<const char *>(&__header_and_proto_version__), sizeof(uint16_t));
            break;
        case State::APPLICATION_DATA_RECEPTION_COMPLETED:
            __client_states[client_id] = State::APPLICATION_DATA_RECEIVING;
            __header_and_proto_version__ |= (uint16_t)DATA_RECEIVED_BY_CLIENT << 8 | __G_PROTOCOL_MAJOR_VERSION__;
            empty_data.append(reinterpret_cast<const char *>(&__header_and_proto_version__), sizeof(uint16_t));
            break;
        case State::END_APPLICATION_DATA_TRANSMISSION_ACK_WAITING:   // concurrency control
        case State::APPLICATION_DATA_TRANSMITTING:                   // concurrency control
        case State::START_APPLICATION_DATA_TRANSMISSION_ACK_WAITING: // concurrency control
            break;

        default:
            break;
        }
    }
    else
    {
        GLOG_ERROR("No client found for id {}", client_id)
    }
    return empty_data;
};

[[nodiscard]] auto Protocol::recieve(Protocol::ClientId client_id, gbase::ByteBuffer<std::byte> &data) -> gbase::ByteBuffer<std::byte>
{
    // WARNING : TODO make sure it is a header (handle)
    data.read<sizeof(uint16_t)>(reinterpret_cast<char *>(&__header_and_proto_version__));
    gbase::ByteBuffer<std::byte> ack;

    uint8_t header = (uint8_t)(__header_and_proto_version__ >> 8);
    __header_and_proto_version__ = 0x0;
    __size_of_data__ = 0x0;
    gbase::ByteBuffer<std::byte> empty_data;
    switch (header)
    {
    // client side intiations
    case START_SESSION: // client starts
        GLOG_DEBUG_L1("Start of session")
        __header_and_proto_version__ |= (uint16_t)START_SESSION_ACK << 8 | __G_PROTOCOL_MAJOR_VERSION__;

        // send sot
        ack.append(reinterpret_cast<const char *>(&__header_and_proto_version__), sizeof(uint16_t));
        if (const auto &itr = __data_waiting_to_sent.find(client_id); itr != __data_waiting_to_sent.end())
        {
            itr->second.push({sizeof(uint16_t), TrasnmittingDataType::PROTOCOL_DATA, std::move(ack)});
        }
        else
        {
            QueueOfData q;
            q.push({sizeof(uint16_t), TrasnmittingDataType::PROTOCOL_DATA, std::move(ack)});
            __data_waiting_to_sent.emplace(client_id, q);
        }
        break;
    case START_DATA_TRANSMISSION: // client starts
        GLOG_DEBUG_L1("Start of data transmission")
        data.read<sizeof(uint16_t)>(reinterpret_cast<char *>(&__size_of_data__));

        if (const auto &itr = __data_waiting_to_receive.find(client_id); itr == __data_waiting_to_receive.end())
        {
            QueueOfData q;
            __data_waiting_to_receive.emplace(client_id, q);
        }

        __header_and_proto_version__ |= (uint16_t)START_DATA_TRANSMISSION_ACK << 8 | __G_PROTOCOL_MAJOR_VERSION__;

        // send sot
        ack.append(reinterpret_cast<const char *>(&__header_and_proto_version__), sizeof(uint16_t));
        ack.append(reinterpret_cast<const char *>(&__size_of_data__), sizeof(uint16_t));
        if (const auto &itr = __data_waiting_to_sent.find(client_id); itr != __data_waiting_to_sent.end())
        {
            itr->second.push({ack.get_filled_size(), TrasnmittingDataType::PROTOCOL_DATA, std::move(ack)});
        }
        else
        {
            QueueOfData q;
            q.push({ack.get_filled_size(), TrasnmittingDataType::PROTOCOL_DATA, std::move(ack)});
            __data_waiting_to_sent.emplace(client_id, q);
        }
        break;
    case DATA_ARRIVAL:
        GLOG_DEBUG_L1("Data arrived")
        if (const auto &itr = __client_states.find(client_id); itr != __client_states.end())
        {

            auto &client_state = itr->second;
            if (client_state == State::APPLICATION_DATA_RECEIVING)
            {
                if (const auto &itr = __data_waiting_to_receive.find(client_id); itr != __data_waiting_to_receive.end())
                {
                    data.read<sizeof(uint16_t)>(reinterpret_cast<char *>(&__size_of_data__));
                    GLOG_DEBUG_L1("Data size {}", __size_of_data__)

                    char *app_data = new char[__size_of_data__ + 1];

                    data.read(sizeof(uint16_t) << 1, __size_of_data__, app_data);
                    app_data[__size_of_data__] = '\0';

                    gbase::ByteBuffer<std::byte> received_data;
                    received_data.append(static_cast<const char *>(app_data), __size_of_data__);
                    itr->second.push({ack.get_filled_size(), TrasnmittingDataType::APPLICATION_DATA, std::move(received_data)});

                    delete[] app_data;
                }

                __client_states[client_id] = State::APPLICATION_DATA_RECEPTION_COMPLETED;
            }
        }
        break;
    case END_DATA_TRANSMISSION: // client ends
        GLOG_DEBUG_L1("End of trasnmission")
        __header_and_proto_version__ |= (uint16_t)END_DATA_TRANSMISSION_ACK << 8 | __G_PROTOCOL_MAJOR_VERSION__;

        // send sot
        ack.append(reinterpret_cast<const char *>(&__header_and_proto_version__), sizeof(uint16_t));
        if (const auto &itr = __data_waiting_to_sent.find(client_id); itr != __data_waiting_to_sent.end())
        {
            itr->second.push({ack.get_filled_size(), TrasnmittingDataType::PROTOCOL_DATA, std::move(ack)});
        }
        else
        {
            QueueOfData q;
            q.push({ack.get_filled_size(), TrasnmittingDataType::PROTOCOL_DATA, std::move(ack)});
            __data_waiting_to_sent.emplace(client_id, q);
        }
        __client_states[client_id] = State::IDLE;
        if (const auto &itr = __data_waiting_to_receive.find(client_id); itr != __data_waiting_to_receive.end())
        {
            auto &q = itr->second;
            gbase::ByteBuffer<std::byte> pending_data{std::move(q.front()._data_)};
            q.front()._data_.release();
            q.pop();

            return pending_data;
        }
        break;
    case END_SESSION: // client ends
        GLOG_DEBUG_L1("End of session")
        __header_and_proto_version__ |= (uint16_t)END_SESSION_ACK << 8 | __G_PROTOCOL_MAJOR_VERSION__;

        // send sot
        ack.append(reinterpret_cast<const char *>(&__header_and_proto_version__), sizeof(uint16_t));
        if (const auto &itr = __data_waiting_to_sent.find(client_id); itr != __data_waiting_to_sent.end())
        {
            itr->second.push({ack.get_filled_size(), TrasnmittingDataType::PROTOCOL_DATA, std::move(ack)});
        }
        else
        {
            QueueOfData q;
            q.push({ack.get_filled_size(), TrasnmittingDataType::PROTOCOL_DATA, std::move(ack)});
            __data_waiting_to_sent.emplace(client_id, q);
        }
        break;

    // // server side initiated actions acknowledgment handling
    case START_DATA_TRANSMISSION_ACK:
        GLOG_DEBUG_L1("Start of transmission acked by client")
        if (const auto &itr = __client_states.find(client_id); itr != __client_states.end())
        {
            auto &client_state = itr->second;
            if (client_state == State::START_APPLICATION_DATA_TRANSMISSION_ACK_WAITING)
            {
                __client_states[client_id] = State::START_APPLICATION_DATA_TRANSMISSION_ACK_RECEIVED;
            }
        }
        break;
    case DATA_RECEIVED_BY_CLIENT:
        GLOG_DEBUG_L1("Data received by client")
        if (const auto &itr = __client_states.find(client_id); itr != __client_states.end())
        {
            const auto &client_state = itr->second;
            if (client_state == State::APPLICATION_DATA_TRANSMITTING)
            {
                __client_states[client_id] = State::APPLICATION_DATA_TRANSMISSION_COMPLETED;
            }
        }
        break;
    case END_DATA_TRANSMISSION_ACK:
        GLOG_DEBUG_L1("End of trasnmission acked by client")
        if (const auto &itr = __client_states.find(client_id); itr != __client_states.end())
        {
            const auto &client_state = itr->second;
            if (client_state == State::END_APPLICATION_DATA_TRANSMISSION_ACK_WAITING)
            {
                __client_states[client_id] = State::IDLE;
            }
        }
        break;

    default:
        break;
    }
    return empty_data;
};

auto Protocol::getState(ClientId client_id) const -> State
{
    if (const auto &itr = __client_states.find(client_id); itr != __client_states.end())
    {
        return itr->second;
    }
    return State::UNDEFINED;
}

auto Protocol::isClientHasDataToSent(ClientId client_id) const -> bool
{
    if (const auto &itr = __data_waiting_to_sent.find(client_id); itr != __data_waiting_to_sent.end())
    {
        auto &q = itr->second;
        return q.empty() == false;
    }

    return false;
}

auto gbase::net::gProtocol::Protocol::shouldMonitorIPCChannelForWrite(ClientId client_id) -> bool
{
    bool ret = false;
    if (const auto &itr = __client_states.find(client_id); itr != __client_states.end())
    {
        State client_state = itr->second;
        switch (client_state)
        {
        case State::CONNECTED:
        case State::IDLE:
            ret = this->isClientHasDataToSent(client_id);
            break;
        case State::START_APPLICATION_DATA_TRANSMISSION_ACK_RECEIVED:
        case State::APPLICATION_DATA_TRANSMISSION_COMPLETED:
        case State::APPLICATION_DATA_RECEPTION_COMPLETED:
            ret = true;
            break;
        case State::END_APPLICATION_DATA_TRANSMISSION_ACK_WAITING:   // concurrency control
        case State::APPLICATION_DATA_TRANSMITTING:                   // concurrency control
        case State::START_APPLICATION_DATA_TRANSMISSION_ACK_WAITING: // concurrency control
            ret = false;
            break;

        default:
            break;
        }
    }
    GLOG_DEBUG_L1("shouldMonitorIPCChannelForWrite [client_id = {}]= {}", client_id, ret)
    return ret;
}
