/// @file   server/kafka_adapter.hpp
/// @brief  KafkaServerBroker: adaptador del puerto KafkaBroker sobre el broker real (F7f).
/// @ingroup server

#pragma once

#include <chrono>
#include <cstdint>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "broker/topic_manager.hpp"
#include "common/task.hpp"
#include "common/types.hpp"
#include "kafka/fetch.hpp"
#include "kafka/gateway.hpp"
#include "kafka/list_offsets.hpp"
#include "kafka/metadata.hpp"
#include "kafka/produce.hpp"
#include "reactor/partition_router.hpp"
#include "reactor/reactor.hpp"

namespace nexus {

class MetricsRegistry;

/// @brief Adaptador que implementa el puerto `kafka::KafkaBroker` sobre el broker real
///   (`TopicManager`/particiones), traduciendo entre el formato de Kafka y el log interno.
///   Afinidad: REACTOR-LOCAL (uno en el núcleo 0, que atiende las conexiones Kafka).
/// @details Cierra el cableado que F7a–F7e dejaron pendiente: el `KafkaGateway` decodifica el wire
///   de Kafka y delega aquí; este adaptador opera sobre las particiones. Reparte cada partición a
///   su **reactor dueño** (`partition % N`, ADR-0026) por paso de mensajes (`PartitionRouter`),
///   igual que el `RequestRouter` nativo, de modo que el estado de cada partición se toca solo
///   desde su núcleo (shared-nothing). El **RecordBatch v2 de Kafka se guarda como blob opaco**
///   dentro de un `RecordBatch` interno (envoltorio): produce cuenta sus records para avanzar los
///   offsets; fetch lo devuelve tal cual, reescribiendo el `baseOffset` al offset que asignó el log
///   (no cubierto por el CRC del batch) para que el consumidor vea offsets correctos.
/// @note Sin cablear al clúster (`bind_cluster` no llamado) opera **localmente** sobre el único
///   `TopicManager` (tests con `sync_wait`), igual que el `RequestRouter`.
class KafkaServerBroker : public kafka::KafkaBroker {
public:
    /// @param topics `TopicManager` que atiende (el del núcleo 0 en el servidor; el único en
    /// tests).
    /// @param node_id Identidad de este nodo (broker id anunciado en Metadata).
    /// @param host Host anunciado en Metadata (los clientes se reconectan aquí para produce/fetch).
    /// @param port Puerto Kafka anunciado en Metadata (el del *listener* Kafka, no el nativo).
    KafkaServerBroker(TopicManager& topics, NodeId node_id, std::string host,
                      std::uint16_t port) noexcept
        : topics_(topics), node_id_(node_id), host_(std::move(host)), port_(port) {}

    /// @brief Cablea el enrutado cross-core: produce/fetch se ejecutan en el reactor dueño de la
    ///   partición (`partition % N`).
    /// @param self Reactor donde corre el adaptador (atiende las conexiones Kafka, núcleo 0).
    /// @param partitions Router de particiones del nodo (no propietario; vive más que el
    /// adaptador).
    /// @param topics_by_core `TopicManager` de cada núcleo, indexado por `core_id`.
    /// @pre `self`, `partitions` y los punteros viven más que este adaptador.
    void bind_cluster(Reactor& self, PartitionRouter& partitions,
                      std::vector<TopicManager*> topics_by_core) noexcept {
        self_ = &self;
        partitions_ = &partitions;
        topics_by_core_ = std::move(topics_by_core);
    }

    /// @brief Cablea el registro de métricas (P5e): produce/fetch de Kafka se contabilizan en las
    ///   familias `nexus_broker_*` con `protocol="kafka"`, junto al plano nativo
    ///   (`protocol="native"`).
    /// @pre @p metrics vive más que este adaptador. `nullptr` implícito (sin cablear) = sin
    /// métricas.
    void set_metrics(MetricsRegistry& metrics) noexcept { metrics_ = &metrics; }

    task<kafka::MetadataResponse> metadata(const kafka::MetadataRequest& req) override;
    task<kafka::ProduceResponse> produce(const kafka::ProduceRequest& req) override;
    task<kafka::FetchResponse> fetch(const kafka::FetchRequest& req) override;
    task<kafka::ListOffsetsResponse> list_offsets(const kafka::ListOffsetsRequest& req) override;

private:
    /// `TopicManager` dueño de @p partition: el del núcleo `partition % N` (cableado) o `topics_`.
    [[nodiscard]] TopicManager& owner_manager(PartitionId partition) noexcept;

    /// Contabiliza una RPC Kafka en `nexus_broker_*{api,protocol="kafka"}` (P5e). No-op sin
    /// registro.
    void record_request(std::string_view api, std::uint64_t bytes, bool had_error,
                        std::chrono::steady_clock::time_point start) const;

    TopicManager& topics_;  // NOLINT(cppcoreguidelines-avoid-const-or-ref-data-members)
    NodeId node_id_;
    std::string host_;
    std::uint16_t port_;
    /// Registro de métricas (no propietario; cableado por `set_metrics`). `nullptr` = sin
    /// registrar.
    MetricsRegistry* metrics_ = nullptr;
    /// Enrutado cross-core (cableado por `bind_cluster`; `nullptr` = operación local, tests).
    Reactor* self_ = nullptr;
    PartitionRouter* partitions_ = nullptr;
    std::vector<TopicManager*> topics_by_core_;  ///< `TopicManager` por núcleo (indexado por core).
};

}  // namespace nexus
