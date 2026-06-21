/// @file   broker/request_router.hpp
/// @brief  RequestRouter: despacha una petición decodificada del protocolo a la lógica del broker.
/// @ingroup broker

#pragma once

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "broker/group_coordinator.hpp"
#include "broker/offset_manager.hpp"
#include "broker/topic_manager.hpp"
#include "common/bytes.hpp"
#include "common/error.hpp"
#include "common/task.hpp"
#include "protocol/frame.hpp"
#include "protocol/versioning.hpp"
#include "reactor/partition_router.hpp"

namespace nexus {

class Decoder;

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
    /// @pre `self`, `partitions` y los punteros de `topics_by_core` viven más que este router.
    void bind_cluster(Reactor& self, PartitionRouter& partitions,
                      std::vector<TopicManager*> topics_by_core) noexcept {
        self_ = &self;
        partitions_ = &partitions;
        topics_by_core_ = std::move(topics_by_core);
    }

    /// @brief Despacha @p key (cuerpo en @p body) y escribe la respuesta codificada en @p out.
    /// @return Éxito (con @p out relleno) o un error si la `ApiKey` no se soporta o el cuerpo es
    ///   indecodificable de forma irrecuperable (la `Connection` cierra entonces la conexión).
    /// @note @p body y @p out deben seguir vivos hasta que la corrutina complete (lo están: viven
    ///   en el *frame* de la conexión que la `co_await`ea).
    [[nodiscard]] task<expected<void>> dispatch(ApiKey key, std::uint16_t api_version,
                                                Decoder& body, Buffer& out);

    /// Rangos de versión que soporta este servidor (para `ApiVersions` y la negociación).
    [[nodiscard]] static std::vector<ApiVersionRange> supported_versions();

    /// @brief Coordinador de grupos de este reactor (plano de control: lo lee el puerto de admin).
    /// @details REACTOR-LOCAL: el llamante debe estar en el hilo del reactor del router.
    [[nodiscard]] const GroupCoordinator& group_coordinator() const noexcept { return groups_; }

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

    TopicManager& topics_;  // NOLINT(cppcoreguidelines-avoid-const-or-ref-data-members)
    NodeId node_id_;
    std::string host_;
    std::uint16_t port_;
    /// Enrutado cross-core (cableado por `bind_cluster`; `nullptr` = operación local, tests).
    Reactor* self_ = nullptr;
    PartitionRouter* partitions_ = nullptr;
    /// `TopicManager` por núcleo (indexado por `core_id`); la operación enrutada toca el del dueño.
    std::vector<TopicManager*> topics_by_core_;
    /// Offsets confirmados por grupo (REACTOR-LOCAL: uno por router/reactor; ver `OffsetManager`).
    OffsetManager offsets_;
    /// Membresía de los grupos de consumidores (REACTOR-LOCAL; ver `GroupCoordinator`).
    GroupCoordinator groups_;
};

}  // namespace nexus
