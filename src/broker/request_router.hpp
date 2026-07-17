/// @file   broker/request_router.hpp
/// @brief  RequestRouter: despacha una petición decodificada del protocolo a la lógica del broker.
/// @ingroup broker

#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "broker/group_coordinator.hpp"
#include "broker/offset_manager.hpp"
#include "broker/topic_manager.hpp"
#include "common/bytes.hpp"
#include "common/error.hpp"
#include "common/task.hpp"
#include "protocol/error_code.hpp"
#include "protocol/frame.hpp"
#include "protocol/versioning.hpp"
#include "reactor/partition_router.hpp"

namespace nexus {

class Decoder;
class Encoder;
class MetricsRegistry;
class Counter;
class Histogram;

/// @brief Traduce una petición (cuerpo ya enmarcado) en su respuesta, sobre la lógica del broker.
///   Afinidad: REACTOR-LOCAL.
/// @details Es el puente protocolo↔dominio: decodifica el cuerpo según la `ApiKey`, llama al
///   `TopicManager`/`Partition` y **codifica la respuesta** en un búfer. No toca framing ni sockets
///   (eso es la `Connection` del servidor): así es testeable sin red. Los errores del núcleo se
///   traducen a `WireError` con `from_error` en el borde (ADR-0009).
/// @note `dispatch` es una **corrutina** (`task<expected<void>>`). Las operaciones de partición
///   (Produce/Fetch) se **enrutan al reactor dueño** (`PartitionRouter`/`call_on`, ADR-0025/0026)
///   cuando el router está cableado al clúster (`bind_cluster`); sin cablear (tests unitarios)
///   corren localmente sobre `topics_` y la corrutina se conduce con `sync_wait`. Con un solo
///   núcleo el dueño es el propio reactor y el fast-path de `call_on` evita el viaje por el buzón.
class RequestRouter {
public:
    RequestRouter(TopicManager& topics, NodeId node_id, std::string host,
                  std::uint16_t port) noexcept
        : topics_(topics), node_id_(node_id), host_(std::move(host)), port_(port) {}

    /// @brief Cablea el enrutado cross-core: a partir de aquí, Produce/Fetch se ejecutan en el
    ///   reactor dueño de la partición (`partition % N`).
    /// @param self Reactor donde corre este router (atiende las conexiones).
    /// @param partitions Router de particiones del nodo (no propietario; debe vivir más que este).
    /// @param topics_by_core `TopicManager` de cada núcleo, **indexado por `core_id`**; la
    /// operación
    ///   enrutada toca el del dueño. Para `N == 1` es `{ &topics }`.
    /// @param groups_by_core `GroupCoordinator` de cada núcleo (indexado por `core_id`); la
    /// operación
    ///   de grupo toca el del núcleo coordinador (`hash(group_id) % N`, ADR-0026).
    /// @param offsets_by_core `OffsetManager` de cada núcleo (indexado por `core_id`); el offset de
    ///   un grupo vive en su núcleo coordinador, junto a su membresía.
    /// @pre `self`, `partitions` y los punteros de los tres vectores viven más que este router; los
    ///   tres vectores tienen `partitions.core_count()` elementos.
    void bind_cluster(Reactor& self, PartitionRouter& partitions,
                      std::vector<TopicManager*> topics_by_core,
                      std::vector<GroupCoordinator*> groups_by_core,
                      std::vector<OffsetManager*> offsets_by_core) noexcept {
        self_ = &self;
        partitions_ = &partitions;
        topics_by_core_ = std::move(topics_by_core);
        groups_by_core_ = std::move(groups_by_core);
        offsets_by_core_ = std::move(offsets_by_core);
    }

    /// @brief Cablea el registro de métricas del plano de datos (las cuatro señales de oro:
    ///   tráfico/errores/latencia por tipo de petición; ADR-0017). Idempotente-no: llamar una vez,
    ///   antes de servir.
    /// @details Resuelve y **cachea** las series de Produce/Fetch (`counter`/`histogram` devuelven
    ///   referencias estables) para que el *hot path* solo incremente atómicos, sin buscar series
    ///   ni asignar memoria por petición (normativa de rendimiento/memoria). El registro vive más
    ///   que el router. Sin cablear, `dispatch` no registra nada (tests unitarios).
    void set_metrics(MetricsRegistry& metrics);

    /// @brief Despacha @p key (cuerpo en @p body) y escribe la respuesta codificada en @p out.
    /// @return Éxito (con @p out relleno) o un error si la `ApiKey` no se soporta o el cuerpo es
    ///   indecodificable de forma irrecuperable (la `Connection` cierra entonces la conexión).
    /// @note @p body y @p out deben seguir vivos hasta que la corrutina complete (lo están: viven
    ///   en el *frame* de la conexión que la `co_await`ea).
    [[nodiscard]] task<expected<void>> dispatch(ApiKey key, std::uint16_t api_version,
                                                Decoder& body, Buffer& out);

    /// Rangos de versión que soporta este servidor (para `ApiVersions` y la negociación).
    [[nodiscard]] static std::vector<ApiVersionRange> supported_versions();

private:
    /// @brief Crea @p name (con @p partition_count particiones) en **todos** los núcleos vía
    ///   `call_on` (fan-out de control, ADR-0026): cada reactor crea el topic en su propio
    ///   `TopicManager` —tocándolo solo desde su hilo— y abre únicamente las particiones que le
    ///   tocan; los metadatos se registran completos en cada núcleo.
    /// @return Éxito, o el primer error (con **rollback** de los núcleos ya creados: garantía
    ///   fuerte).
    /// @note Sin cablear al clúster (`partitions_ == nullptr`, tests unitarios) crea localmente
    ///   sobre el único `TopicManager`.
    [[nodiscard]] task<expected<void>> create_topic_cluster(const std::string& name,
                                                            std::int32_t partition_count);

    /// @brief Borra @p name de **todos** los núcleos vía `call_on` (fan-out de control, ADR-0026).
    /// @return El resultado del núcleo 0 (autoritativo); el borrado es idempotente en los demás.
    /// @note Sin cablear al clúster (`partitions_ == nullptr`, tests unitarios) borra localmente.
    [[nodiscard]] task<expected<void>> delete_topic_cluster(const std::string& name);

    /// @brief Despacha una operación de grupo/offset enrutándola al **núcleo coordinador** del
    /// grupo
    ///   (`hash(group_id) % N`, ADR-0026): decodifica el cuerpo, ejecuta la mutación en el shard de
    ///   ese núcleo por paso de mensajes (`call_on`) y codifica la respuesta en @p enc.
    /// @details Sin cablear al clúster (`partitions_ == nullptr`, tests unitarios) opera sobre el
    ///   coordinador/almacén local. El cuerpo decodificado se mueve al *frame* de la corrutina, así
    ///   que sobrevive al salto cross-core.
    /// @brief Despacha Produce/Fetch (plano de datos): decodifica, enruta al reactor dueño de la
    ///   partición (`partition % N`, ADR-0026) o ejecuta local (sin cablear), codifica la respuesta
    ///   y **registra las métricas** del plano de datos (tráfico/errores/bytes/latencia).
    /// @details Extraídos de `dispatch` para acotar su complejidad, igual que las operaciones de
    ///   grupo. `dispatch_fetch` mantiene vivo el `FetchOutcome` (vista zero-copy) hasta el encode.
    [[nodiscard]] task<expected<void>> dispatch_produce(Decoder& body, Encoder& enc);
    [[nodiscard]] task<expected<void>> dispatch_fetch(Decoder& body, Encoder& enc);

    [[nodiscard]] task<expected<void>> dispatch_offset_commit(Decoder& body, Encoder& enc);
    [[nodiscard]] task<expected<void>> dispatch_offset_fetch(Decoder& body, Encoder& enc);
    [[nodiscard]] task<expected<void>> dispatch_join_group(Decoder& body, Encoder& enc);
    [[nodiscard]] task<expected<void>> dispatch_sync_group(Decoder& body, Encoder& enc);
    [[nodiscard]] task<expected<void>> dispatch_heartbeat(Decoder& body, Encoder& enc);
    [[nodiscard]] task<expected<void>> dispatch_leave_group(Decoder& body, Encoder& enc);

    /// @brief Series cacheadas de un tipo de petición del plano de datos (las cuatro señales de
    /// oro).
    /// @details Punteros a series **estables** del `MetricsRegistry` (no propietarios); `nullptr`
    ///   hasta `set_metrics`. Cachearlas evita buscar series y asignar `Labels` por petición.
    struct DataPlaneMetrics {
        Counter* requests = nullptr;  ///< Tráfico: peticiones servidas.
        Counter* errors = nullptr;    ///< Errores: peticiones con `WireError != None`.
        Counter* bytes = nullptr;     ///< Volumen: bytes de payload (batch producido / servido).
        Counter* messages = nullptr;  ///< Volumen: nº de records del batch (distinto de requests).
        Histogram* duration = nullptr;  ///< Latencia de servicio (segundos).
    };

    /// @brief Resuelve y cachea las series `(name, {api})` de un tipo de petición en @p metrics.
    [[nodiscard]] static DataPlaneMetrics resolve_metrics(MetricsRegistry& metrics,
                                                          std::string_view api);

    /// @brief Registra una petición servida en @p series: tráfico, bytes, records, error (si lo
    ///   hubo) y latencia (`now() - start`). No-op si las métricas no están cableadas (`metrics_ ==
    ///   null`).
    /// @param records Número de records del batch producido/servido (un request agrupa N records).
    void record_request(const DataPlaneMetrics& series, WireError error, MonoTime start,
                        std::uint64_t bytes, std::uint64_t records) const;

    TopicManager& topics_;  // NOLINT(cppcoreguidelines-avoid-const-or-ref-data-members)
    NodeId node_id_;
    std::string host_;
    std::uint16_t port_;
    /// Enrutado cross-core (cableado por `bind_cluster`; `nullptr` = operación local, tests).
    Reactor* self_ = nullptr;
    PartitionRouter* partitions_ = nullptr;
    /// `TopicManager` por núcleo (indexado por `core_id`); la operación enrutada toca el del dueño.
    std::vector<TopicManager*> topics_by_core_;
    /// `GroupCoordinator` por núcleo (indexado por `core_id`); destino del enrutado de grupo.
    std::vector<GroupCoordinator*> groups_by_core_;
    /// `OffsetManager` por núcleo (indexado por `core_id`); destino del enrutado de offsets.
    std::vector<OffsetManager*> offsets_by_core_;
    /// Offsets confirmados (REACTOR-LOCAL); solo se usa **sin cablear** (tests): ver
    /// `OffsetManager`.
    OffsetManager offsets_;
    /// Membresía de grupos (REACTOR-LOCAL); solo se usa **sin cablear** (tests): ver
    /// `GroupCoordinator`.
    GroupCoordinator groups_;
    /// Registro de métricas (no propietario; cableado por `set_metrics`). `nullptr` = sin registrar
    /// (tests). Solo lo toca el reactor que sirve las conexiones (núcleo 0): sin contención.
    MetricsRegistry* metrics_ = nullptr;
    DataPlaneMetrics produce_metrics_;  ///< Series cacheadas de Produce (válidas si `metrics_`).
    DataPlaneMetrics fetch_metrics_;    ///< Series cacheadas de Fetch (válidas si `metrics_`).
};

}  // namespace nexus
