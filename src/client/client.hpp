/// @file   client/client.hpp
/// @brief  Client: cliente nativo bloqueante del broker (Fase 1b, mono-nodo).
/// @ingroup client

#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "client/endpoint.hpp"
#include "common/bytes.hpp"
#include "common/error.hpp"
#include "common/types.hpp"
#include "io/socket.hpp"
#include "protocol/frame.hpp"
#include "protocol/messages.hpp"

namespace nexus {

class Producer;
class Consumer;

/// @brief Cliente nativo del broker: conexión TCP bloqueante + petición/respuesta enmarcada.
///   Afinidad: REACTOR-LOCAL (no es thread-safe; una conexión por `Client`).
/// @details Fase 1b, mono-nodo: habla el protocolo binario por **una** conexión bloqueante
///   (`Socket::connect` + framing manual). Expone las operaciones de bajo nivel (create/delete
///   topic, metadata, produce, fetch) y dos fachadas de alto nivel (`Producer`/`Consumer`). El
///   *smart-client* completo (multi-broker: `MetadataCache`/`ConnectionPool`, reintentos con
///   backoff, grupos de consumidores) llega con la distribución de Fase 2; aquí se ajusta el
///   desglose a un cliente síncrono de un solo nodo (anotado en la hoja de ruta).
/// @invariant Las vistas zero-copy de la respuesta (`FetchResponse::batches`) apuntan al búfer de
///   recepción interno y solo son válidas hasta la **siguiente** petición sobre este `Client`.
/// @note Movible (lo devuelve `connect`), pero no lo muevas mientras existan `Producer`/`Consumer`
///   creados desde él: guardan una referencia y quedarían colgando.
class Client {
public:
    /// @brief Conecta (bloqueante) a @p endpoint. @return el cliente o `IoError`/`InvalidArgument`.
    [[nodiscard]] static expected<Client> connect(const Endpoint& endpoint);

    ~Client() = default;
    Client(Client&&) noexcept = default;
    Client& operator=(Client&&) noexcept = default;
    Client(const Client&) = delete;
    Client& operator=(const Client&) = delete;

    /// Crea un topic con @p partition_count particiones (plano de control).
    [[nodiscard]] expected<CreateTopicResponse> create_topic(std::string name,
                                                             std::int32_t partition_count,
                                                             std::int16_t replication_factor = 1);
    /// Borra un topic por nombre.
    [[nodiscard]] expected<DeleteTopicResponse> delete_topic(std::string name);
    /// Pide metadatos del cluster; @p topics vacío = todos.
    [[nodiscard]] expected<MetadataResponse> metadata(std::vector<std::string> topics = {});
    /// Produce un batch ya codificado (bajo nivel; ver `Producer` para la fachada).
    [[nodiscard]] expected<ProduceResponse> produce(const ProduceRequest& req);
    /// Lee desde una partición (bajo nivel; `batches` válido hasta la próxima petición).
    [[nodiscard]] expected<FetchResponse> fetch(const FetchRequest& req);
    /// Confirma el offset consumido de @p group en @p topic/@p partition.
    [[nodiscard]] expected<OffsetCommitResponse> commit_offset(std::string group, std::string topic,
                                                               PartitionId partition, Offset offset,
                                                               std::string metadata = {});
    /// Consulta el offset confirmado de @p group en @p topic/@p partition (`-1` si no hay).
    [[nodiscard]] expected<OffsetFetchResponse> fetch_offset(std::string group, std::string topic,
                                                             PartitionId partition);

    /// Crea un `Producer` ligado a este cliente (debe sobrevivirlo).
    [[nodiscard]] Producer producer();
    /// Crea un `Consumer` de @p topic/@p partition ligado a este cliente (debe sobrevivirlo).
    [[nodiscard]] Consumer consumer(std::string topic, PartitionId partition = 0);

    [[nodiscard]] bool is_open() const noexcept { return sock_.is_open(); }
    void close() noexcept { sock_.close(); }

private:
    explicit Client(Socket sock) noexcept : sock_(std::move(sock)) {}

    /// Envía @p req como trama @p key y decodifica la respuesta `Resp` (espeja `correlation_id`).
    template <class Resp, class Req>
    [[nodiscard]] expected<Resp> round_trip(ApiKey key, std::uint16_t api_version, const Req& req);

    Socket sock_;
    std::uint32_t next_correlation_id_ = 0;
    /// Respalda las vistas zero-copy de la última respuesta (válidas hasta la siguiente petición).
    Buffer rx_;
};

}  // namespace nexus
