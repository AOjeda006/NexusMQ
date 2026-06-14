/// @file   protocol/messages.hpp
/// @brief  Mensajes request/response del protocolo (§7.2.1): ApiVersions, Metadata.
/// @ingroup protocol

#pragma once

#include <cstddef>
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

/// @brief Crea un topic con @p partition_count particiones y factor de réplica dado.
struct CreateTopicRequest {
    std::string name;
    std::int32_t partition_count = 1;
    std::int16_t replication_factor = 1;

    void encode(Encoder& enc) const;
    [[nodiscard]] static expected<CreateTopicRequest> decode(Decoder& dec);
    bool operator==(const CreateTopicRequest&) const = default;
};

struct CreateTopicResponse {
    WireError error_code = WireError::None;

    void encode(Encoder& enc) const;
    [[nodiscard]] static expected<CreateTopicResponse> decode(Decoder& dec);
    bool operator==(const CreateTopicResponse&) const = default;
};

/// @brief Borra un topic por nombre.
struct DeleteTopicRequest {
    std::string name;

    void encode(Encoder& enc) const;
    [[nodiscard]] static expected<DeleteTopicRequest> decode(Decoder& dec);
    bool operator==(const DeleteTopicRequest&) const = default;
};

struct DeleteTopicResponse {
    WireError error_code = WireError::None;

    void encode(Encoder& enc) const;
    [[nodiscard]] static expected<DeleteTopicResponse> decode(Decoder& dec);
    bool operator==(const DeleteTopicResponse&) const = default;
};

/// @brief Confirma el offset consumido de un grupo en una partición (con metadatos opcionales).
struct OffsetCommitRequest {
    std::string group;
    std::string topic;
    PartitionId partition = 0;
    Offset offset = 0;
    std::string metadata;

    void encode(Encoder& enc) const;
    [[nodiscard]] static expected<OffsetCommitRequest> decode(Decoder& dec);
    bool operator==(const OffsetCommitRequest&) const = default;
};

struct OffsetCommitResponse {
    WireError error_code = WireError::None;

    void encode(Encoder& enc) const;
    [[nodiscard]] static expected<OffsetCommitResponse> decode(Decoder& dec);
    bool operator==(const OffsetCommitResponse&) const = default;
};

/// @brief Consulta el offset confirmado de un grupo en una partición.
struct OffsetFetchRequest {
    std::string group;
    std::string topic;
    PartitionId partition = 0;

    void encode(Encoder& enc) const;
    [[nodiscard]] static expected<OffsetFetchRequest> decode(Decoder& dec);
    bool operator==(const OffsetFetchRequest&) const = default;
};

struct OffsetFetchResponse {
    Offset offset = -1;
    WireError error_code = WireError::None;

    void encode(Encoder& enc) const;
    [[nodiscard]] static expected<OffsetFetchResponse> decode(Decoder& dec);
    bool operator==(const OffsetFetchResponse&) const = default;
};

/// @brief Miembro de un grupo tal como lo ve el líder al repartir (id + suscripción opaca).
struct GroupMember {
    std::string member_id;
    std::vector<std::byte> subscription;

    void encode(Encoder& enc) const;
    [[nodiscard]] static expected<GroupMember> decode(Decoder& dec);
    bool operator==(const GroupMember&) const = default;
};

/// @brief Asignación que el líder fija para un miembro (bytes opacos del reparto de particiones).
struct GroupAssignment {
    std::string member_id;
    std::vector<std::byte> assignment;

    void encode(Encoder& enc) const;
    [[nodiscard]] static expected<GroupAssignment> decode(Decoder& dec);
    bool operator==(const GroupAssignment&) const = default;
};

/// @brief (Re)incorpora un consumidor a un grupo (abre o continúa el rebalanceo).
struct JoinGroupRequest {
    std::string group;
    std::string member_id;  ///< Vacío: alta de un consumidor nuevo (el broker asigna el id).
    std::int32_t session_timeout_ms = 0;
    std::vector<std::byte> subscription;  ///< Metadatos opacos del cliente (topics suscritos…).

    void encode(Encoder& enc) const;
    [[nodiscard]] static expected<JoinGroupRequest> decode(Decoder& dec);
    bool operator==(const JoinGroupRequest&) const = default;
};

/// @brief Resultado del `join`: identidad, generación y, solo para el líder, la lista de miembros.
struct JoinGroupResponse {
    WireError error_code = WireError::None;
    Generation generation = 0;
    std::string member_id;
    std::string leader_id;
    bool is_leader = false;
    std::vector<GroupMember> members;  ///< Poblado solo para el líder (la lista a repartir).

    void encode(Encoder& enc) const;
    [[nodiscard]] static expected<JoinGroupResponse> decode(Decoder& dec);
    bool operator==(const JoinGroupResponse&) const = default;
};

/// @brief Reparte (líder) o recoge (seguidor) las asignaciones de la generación.
struct SyncGroupRequest {
    std::string group;
    std::string member_id;
    Generation generation = 0;
    std::vector<GroupAssignment> assignments;  ///< Solo el líder lo rellena.

    void encode(Encoder& enc) const;
    [[nodiscard]] static expected<SyncGroupRequest> decode(Decoder& dec);
    bool operator==(const SyncGroupRequest&) const = default;
};

/// @brief Asignación de este miembro tras el `sync`.
struct SyncGroupResponse {
    WireError error_code = WireError::None;
    std::vector<std::byte> assignment;

    void encode(Encoder& enc) const;
    [[nodiscard]] static expected<SyncGroupResponse> decode(Decoder& dec);
    bool operator==(const SyncGroupResponse&) const = default;
};

/// @brief Latido de liveness de un miembro de un grupo.
struct HeartbeatRequest {
    std::string group;
    std::string member_id;
    Generation generation = 0;

    void encode(Encoder& enc) const;
    [[nodiscard]] static expected<HeartbeatRequest> decode(Decoder& dec);
    bool operator==(const HeartbeatRequest&) const = default;
};

/// @brief Resultado del latido (`None`, `RebalanceInProgress`, generación obsoleta…).
struct HeartbeatResponse {
    WireError error_code = WireError::None;

    void encode(Encoder& enc) const;
    [[nodiscard]] static expected<HeartbeatResponse> decode(Decoder& dec);
    bool operator==(const HeartbeatResponse&) const = default;
};

/// @brief Baja voluntaria de un miembro de un grupo.
struct LeaveGroupRequest {
    std::string group;
    std::string member_id;

    void encode(Encoder& enc) const;
    [[nodiscard]] static expected<LeaveGroupRequest> decode(Decoder& dec);
    bool operator==(const LeaveGroupRequest&) const = default;
};

struct LeaveGroupResponse {
    WireError error_code = WireError::None;

    void encode(Encoder& enc) const;
    [[nodiscard]] static expected<LeaveGroupResponse> decode(Decoder& dec);
    bool operator==(const LeaveGroupResponse&) const = default;
};

}  // namespace nexus
