/// @file   client/client.cpp
/// @brief  Implementación del cliente nativo bloqueante.
/// @ingroup client

#include "client/client.hpp"

#if defined(_WIN32)
#include <winsock2.h>  // ::send/::recv sobre SOCKET (Winsock); WSAStartup lo hace Socket::connect
#else
#include <sys/socket.h>
#endif

#include <cstddef>
#include <utility>

#include "client/consumer.hpp"
#include "client/producer.hpp"
#include "protocol/codec.hpp"

namespace nexus {

namespace {

// El handle nativo es `int` (fd POSIX) o `SOCKET` (Winsock); las llamadas y el tipo de retorno de
// ::send/::recv difieren por plataforma. Windows no tiene SIGPIPE (no hace falta MSG_NOSIGNAL).

/// Envía todos los bytes de @p data por @p fd (bloqueante); `false` si el par cerró o hubo error.
bool send_all(NativeHandle fd, ByteSpan data) {
    const auto* ptr = reinterpret_cast<const char*>(data.data());  // NOLINT(*-reinterpret-cast)
    std::size_t left = data.size();
    while (left > 0) {
#if defined(_WIN32)
        const int sent = ::send(static_cast<SOCKET>(fd), ptr, static_cast<int>(left), 0);
#else
        const ssize_t sent = ::send(fd, ptr, left, MSG_NOSIGNAL);
#endif
        if (sent <= 0) {
            return false;
        }
        ptr += sent;
        left -= static_cast<std::size_t>(sent);
    }
    return true;
}

/// Lee exactamente @p buf.size() bytes de @p fd (bloqueante); `false` si el par cerró antes.
bool recv_exact(NativeHandle fd, MutByteSpan buf) {
    auto* ptr = reinterpret_cast<char*>(buf.data());  // NOLINT(*-reinterpret-cast)
    std::size_t left = buf.size();
    while (left > 0) {
#if defined(_WIN32)
        const int got = ::recv(static_cast<SOCKET>(fd), ptr, static_cast<int>(left), 0);
#else
        const ssize_t got = ::recv(fd, ptr, left, 0);
#endif
        if (got <= 0) {
            return false;
        }
        ptr += got;
        left -= static_cast<std::size_t>(got);
    }
    return true;
}

/// Recibe una trama completa en @p rx (cabecera incluida): lee `length:u32` y luego `length` bytes.
expected<void> recv_frame_into(NativeHandle fd, Buffer& rx) {
    rx.clear();
    const MutByteSpan len_bytes = rx.extend(sizeof(std::uint32_t));
    if (!recv_exact(fd, len_bytes)) {
        return make_error(ErrorCode::IoError, "conexión cerrada al leer la longitud de la trama");
    }
    Decoder len_dec{ByteSpan{len_bytes.data(), len_bytes.size()}};
    const expected<std::uint32_t> length = len_dec.get_u32();
    if (!length) {
        return std::unexpected(length.error());
    }
    const MutByteSpan rest = rx.extend(*length);
    if (!recv_exact(fd, rest)) {
        return make_error(ErrorCode::IoError, "conexión cerrada al leer el cuerpo de la trama");
    }
    return {};
}

}  // namespace

expected<Client> Client::connect(const Endpoint& endpoint) {
    expected<Socket> sock = Socket::connect(endpoint.host, endpoint.port);
    if (!sock) {
        return std::unexpected(sock.error());
    }
    sock->set_nodelay(true);  // petición/respuesta pequeña: sin Nagle baja la latencia.
    return Client{std::move(*sock)};
}

template <class Resp, class Req>
expected<Resp> Client::round_trip(ApiKey key, std::uint16_t api_version, const Req& req) {
    Buffer body;
    Encoder body_enc{body};
    req.encode(body_enc);

    const std::uint32_t correlation_id = next_correlation_id_++;
    FrameHeader header;
    header.api_key = key;
    header.api_version = api_version;
    header.correlation_id = correlation_id;
    header.length = FrameHeader::length_for(body.size());

    Buffer frame;
    Encoder frame_enc{frame};
    header.encode(frame_enc);
    frame.append(body.as_span());
    if (!send_all(sock_.fd(), frame.as_span())) {
        return make_error(ErrorCode::IoError, "fallo al enviar la petición");
    }

    if (const expected<void> received = recv_frame_into(sock_.fd(), rx_); !received) {
        return std::unexpected(received.error());
    }
    Decoder dec{rx_.as_span()};
    const expected<FrameHeader> resp_header = FrameHeader::decode(dec);
    if (!resp_header) {
        return std::unexpected(resp_header.error());
    }
    if (resp_header->correlation_id != correlation_id) {
        return make_error(ErrorCode::Corrupt, "correlation_id de la respuesta no coincide");
    }
    return Resp::decode(dec);
}

expected<CreateTopicResponse> Client::create_topic(std::string name, std::int32_t partition_count,
                                                   std::int16_t replication_factor) {
    return round_trip<CreateTopicResponse>(
        ApiKey::CreateTopic, 0,
        CreateTopicRequest{.name = std::move(name),
                           .partition_count = partition_count,
                           .replication_factor = replication_factor});
}

expected<DeleteTopicResponse> Client::delete_topic(std::string name) {
    return round_trip<DeleteTopicResponse>(ApiKey::DeleteTopic, 0,
                                           DeleteTopicRequest{.name = std::move(name)});
}

expected<MetadataResponse> Client::metadata(std::vector<std::string> topics) {
    return round_trip<MetadataResponse>(ApiKey::Metadata, 0,
                                        MetadataRequest{.topics = std::move(topics)});
}

expected<ProduceResponse> Client::produce(const ProduceRequest& req) {
    return round_trip<ProduceResponse>(ApiKey::Produce, 0, req);
}

expected<FetchResponse> Client::fetch(const FetchRequest& req) {
    return round_trip<FetchResponse>(ApiKey::Fetch, 0, req);
}

expected<OffsetCommitResponse> Client::commit_offset(std::string group, std::string topic,
                                                     PartitionId partition, Offset offset,
                                                     std::string metadata) {
    return round_trip<OffsetCommitResponse>(ApiKey::OffsetCommit, 0,
                                            OffsetCommitRequest{.group = std::move(group),
                                                                .topic = std::move(topic),
                                                                .partition = partition,
                                                                .offset = offset,
                                                                .metadata = std::move(metadata)});
}

expected<OffsetFetchResponse> Client::fetch_offset(std::string group, std::string topic,
                                                   PartitionId partition) {
    return round_trip<OffsetFetchResponse>(
        ApiKey::OffsetFetch, 0,
        OffsetFetchRequest{
            .group = std::move(group), .topic = std::move(topic), .partition = partition});
}

Producer Client::producer() {
    return Producer{*this};
}

Consumer Client::consumer(std::string topic, PartitionId partition) {
    return Consumer{*this, std::move(topic), partition};
}

}  // namespace nexus
