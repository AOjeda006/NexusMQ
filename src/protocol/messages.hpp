/// @file   protocol/messages.hpp
/// @brief  Mensajes request/response del protocolo (§7.2.1): ApiVersions, Metadata.
/// @ingroup protocol

#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "common/bytes.hpp"
#include "common/error.hpp"
#include "common/types.hpp"
#include "protocol/error_code.hpp"
#include "protocol/versioning.hpp"

namespace nexus {

class Encoder;
class Decoder;

/// @brief Negociación inicial: el cliente anuncia su versión máxima de protocolo.
struct ApiVersionsRequest {
    std::uint16_t client_version = 0;

    void encode(Encoder& enc) const;
    [[nodiscard]] static expected<ApiVersionsRequest> decode(Decoder& dec);
    bool operator==(const ApiVersionsRequest&) const = default;
};

/// @brief Respuesta: rangos de versión soportados por el servidor por `ApiKey`.
struct ApiVersionsResponse {
    std::vector<ApiVersionRange> ranges;

    void encode(Encoder& enc) const;
    [[nodiscard]] static expected<ApiVersionsResponse> decode(Decoder& dec);
    bool operator==(const ApiVersionsResponse&) const = default;
};

/// @brief Metadatos de un broker del cluster.
struct BrokerMeta {
    NodeId node_id = 0;
    std::string host;
    std::uint16_t port = 0;

    void encode(Encoder& enc) const;
    [[nodiscard]] static expected<BrokerMeta> decode(Decoder& dec);
    bool operator==(const BrokerMeta&) const = default;
};

/// @brief Metadatos de una partición: líder, réplicas y época de liderazgo.
struct PartitionMeta {
    PartitionId id = 0;
    NodeId leader_node_id = -1;
    std::vector<NodeId> replicas;
    Epoch leader_epoch = 0;

    void encode(Encoder& enc) const;
    [[nodiscard]] static expected<PartitionMeta> decode(Decoder& dec);
    bool operator==(const PartitionMeta&) const = default;
};

/// @brief Metadatos de un topic: nombre, error y sus particiones.
struct TopicMeta {
    std::string name;
    WireError error = WireError::None;
    std::vector<PartitionMeta> partitions;

    void encode(Encoder& enc) const;
    [[nodiscard]] static expected<TopicMeta> decode(Decoder& dec);
    bool operator==(const TopicMeta&) const = default;
};

/// @brief Solicitud de metadatos; `topics` vacío significa "todos".
struct MetadataRequest {
    std::vector<std::string> topics;

    void encode(Encoder& enc) const;
    [[nodiscard]] static expected<MetadataRequest> decode(Decoder& dec);
    bool operator==(const MetadataRequest&) const = default;
};

/// @brief Respuesta de metadatos: brokers del cluster y topics descritos.
struct MetadataResponse {
    std::vector<BrokerMeta> brokers;
    std::vector<TopicMeta> topics;

    void encode(Encoder& enc) const;
    [[nodiscard]] static expected<MetadataResponse> decode(Decoder& dec);
    bool operator==(const MetadataResponse&) const = default;
};

/// @brief Producción a una partición; `batch` son los bytes del RecordBatch (opacos aquí).
/// @details `batch` es una vista no propietaria: al decodificar apunta dentro del búfer de
///   entrada (debe vivir mientras se use). Sin `operator==` por defecto (compararía punteros).
struct ProduceRequest {
    std::string topic;
    PartitionId partition = 0;
    Acks acks = Acks::Leader;
    ByteSpan batch;

    void encode(Encoder& enc) const;
    [[nodiscard]] static expected<ProduceRequest> decode(Decoder& dec);
};

/// @brief Resultado de una producción: offset base asignado, error y throttling.
struct ProduceResponse {
    Offset base_offset = -1;
    WireError error_code = WireError::None;
    std::int32_t throttle_ms = 0;

    void encode(Encoder& enc) const;
    [[nodiscard]] static expected<ProduceResponse> decode(Decoder& dec);
    bool operator==(const ProduceResponse&) const = default;
};

/// @brief Lectura desde una partición a partir de `fetch_offset`.
struct FetchRequest {
    std::string topic;
    PartitionId partition = 0;
    Offset fetch_offset = 0;
    std::int32_t max_bytes = 0;
    std::int32_t min_bytes = 0;
    std::int32_t max_wait_ms = 0;

    void encode(Encoder& enc) const;
    [[nodiscard]] static expected<FetchRequest> decode(Decoder& dec);
    bool operator==(const FetchRequest&) const = default;
};

/// @brief Resultado de una lectura: batches (bytes opacos), high-watermark y error.
/// @details `batches` es una vista no propietaria al búfer de entrada (zero-copy). Sin
///   `operator==` por defecto (compararía punteros, no contenido).
struct FetchResponse {
    ByteSpan batches;
    Offset high_watermark = 0;
    Offset log_start_offset = 0;
    WireError error_code = WireError::None;

    void encode(Encoder& enc) const;
    [[nodiscard]] static expected<FetchResponse> decode(Decoder& dec);
};

}  // namespace nexus
