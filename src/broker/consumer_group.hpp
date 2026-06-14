/// @file   broker/consumer_group.hpp
/// @brief  ConsumerGroup: FSM de membresía de un grupo de consumidores (rebalanceo generacional).
/// @ingroup broker

#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "common/error.hpp"
#include "common/types.hpp"

namespace nexus {

/// @brief Estado de la FSM de un grupo (protocolo *eager* estilo Kafka). Afinidad: INMUTABLE.
/// @details `Empty` (sin miembros) → `PreparingRebalance` (recogiendo `join` de los miembros) →
///   `CompletingRebalance` (todos reincorporados; sube la generación; espera el `sync` del líder) →
///   `Stable` (asignaciones repartidas; los miembros laten). `Dead` es terminal (grupo eliminado).
enum class GroupState : std::uint8_t {
    Empty,
    PreparingRebalance,
    CompletingRebalance,
    Stable,
    Dead,
};

/// @brief Resultado de un `heartbeat`: liveness aceptada o señal de re-incorporación.
enum class HeartbeatStatus : std::uint8_t {
    Ok,                  ///< Grupo estable: latido aceptado.
    RebalanceInProgress  ///< Hay un rebalanceo en curso: el miembro debe re-`join`.
};

/// @brief Miembro visible para el líder al repartir (id + metadatos de suscripción opacos).
struct MemberInfo {
    std::string member_id;
    std::vector<std::byte> subscription;
    bool operator==(const MemberInfo&) const = default;
};

/// @brief Asignación que el líder fija para un miembro (bytes opacos: el reparto de particiones).
struct MemberAssignment {
    std::string member_id;
    std::vector<std::byte> assignment;
    bool operator==(const MemberAssignment&) const = default;
};

/// @brief Resultado de un `join`: identidad, generación y, si es el líder, la lista a repartir.
struct JoinResult {
    std::string member_id;            ///< Id del miembro (el generado si vino vacío).
    Generation generation = 0;        ///< Generación vigente (sube al completar el rebalanceo).
    std::string leader_id;            ///< Miembro líder (el que reparte en `sync`).
    bool is_leader = false;           ///< ¿Este miembro es el líder de la generación?
    std::vector<MemberInfo> members;  ///< Solo para el líder en `CompletingRebalance`.
};

/// @brief Resultado de un `sync`: la asignación de este miembro (si ya la repartió el líder).
struct SyncResult {
    Generation generation = 0;
    bool assigned = false;              ///< false si el líder aún no ha repartido (reintentar).
    std::vector<std::byte> assignment;  ///< Asignación opaca de este miembro.
};

/// @brief Máquina de estados de la membresía de un grupo de consumidores (§7.6). Afinidad:
///   REACTOR-LOCAL (vive en el coordinador de su reactor; sin sincronización).
/// @details FSM **síncrona y sin E/S** (mismo principio que `RaftNode`, ADR-0015): el reloj se
///   inyecta como `MonoTime now` en cada entrada y la expiración de sesiones se evalúa en `tick`,
///   de modo que las pruebas son deterministas. Implementa el protocolo *eager*: un cambio de
///   membresía (nuevo `join`, `leave` o sesión expirada) abre un rebalanceo en el que **todos** los
///   miembros vuelven a incorporarse; el primero en hacerlo es el **líder**, que recibe la lista de
///   miembros y reparte las asignaciones en `sync`. La generación numera cada ronda y permite
///   descartar peticiones obsoletas. Las asignaciones son **bytes opacos** (el coordinador/cliente
///   decide su formato): el grupo solo transporta y enruta.
/// @invariant En `Stable` todos los miembros tienen su asignación de la `generation` vigente; la
///   `generation` es monótona no decreciente.
class ConsumerGroup {
public:
    explicit ConsumerGroup(std::string group_id) : group_id_(std::move(group_id)) {}

    /// @brief (Re)incorpora un miembro a la generación en curso (abre rebalanceo si estaba
    /// estable).
    /// @param member_id Id del miembro; **vacío** ⇒ se le asigna uno generado (alta de un
    /// consumidor
    ///   nuevo). @param subscription Metadatos opacos del cliente (p. ej. topics suscritos).
    /// @param session_timeout Plazo sin latido tras el cual `tick` lo expulsa.
    /// @return La identidad/estado del miembro; `Unsupported` si el grupo está `Dead`.
    [[nodiscard]] expected<JoinResult> join(MonoTime now, std::string member_id,
                                            std::vector<std::byte> subscription,
                                            std::chrono::milliseconds session_timeout);

    /// @brief Reparte (líder) o recoge (seguidor) las asignaciones de la generación.
    /// @details El **líder** entrega @p assignments (member_id→bytes); el grupo las guarda y pasa a
    ///   `Stable`. Cualquier miembro recibe su propia asignación. Si el líder aún no ha repartido,
    ///   un seguidor obtiene `assigned == false` (debe reintentar).
    /// @return `NotFound` si el miembro no existe; `InvalidArgument` si @p generation no es la
    ///   vigente (generación obsoleta); `Unsupported` si el grupo está `Dead`.
    [[nodiscard]] expected<SyncResult> sync(MonoTime now, std::string_view member_id,
                                            Generation generation,
                                            const std::vector<MemberAssignment>& assignments);

    /// @brief Renueva la liveness de un miembro (o le indica que hay un rebalanceo en curso).
    /// @return `Ok`/`RebalanceInProgress`; `NotFound` si no existe; `InvalidArgument` si la
    ///   generación es obsoleta estando el grupo estable; `Unsupported` si está `Dead`.
    [[nodiscard]] expected<HeartbeatStatus> heartbeat(MonoTime now, std::string_view member_id,
                                                      Generation generation);

    /// @brief Da de baja un miembro; abre rebalanceo (o vacía el grupo si era el último).
    /// @return `NotFound` si el miembro no existe; `Unsupported` si el grupo está `Dead`.
    expected<void> leave(std::string_view member_id);

    /// @brief Expulsa a los miembros sin latido dentro de su `session_timeout` y, si alguno cae,
    ///   abre un rebalanceo (o vacía el grupo). Llamar periódicamente desde el reactor.
    void tick(MonoTime now);

    [[nodiscard]] GroupState state() const noexcept { return state_; }
    [[nodiscard]] Generation generation() const noexcept { return generation_; }
    [[nodiscard]] const std::string& group_id() const noexcept { return group_id_; }
    [[nodiscard]] const std::string& leader_id() const noexcept { return leader_id_; }
    [[nodiscard]] std::size_t member_count() const noexcept { return members_.size(); }
    [[nodiscard]] bool contains(std::string_view member_id) const;
    /// Miembros actuales (id + suscripción), **ordenados por id** (reparto y pruebas
    /// deterministas).
    [[nodiscard]] std::vector<MemberInfo> members() const;

private:
    /// Estado por miembro (lo que el grupo necesita para enrutar y vigilar la liveness).
    struct Member {
        std::vector<std::byte> subscription;
        std::vector<std::byte> assignment;
        std::chrono::milliseconds session_timeout{0};
        MonoTime last_seen;
        bool joined_round = false;  ///< ¿Reincorporado a la ronda de rebalanceo en curso?
    };

    /// Abre un rebalanceo: a `PreparingRebalance`, reelige líder y exige que todos se reincorporen.
    void begin_rebalance();
    /// Si todos los miembros se han reincorporado, pasa a `CompletingRebalance` y sube la
    /// generación.
    void complete_if_ready();
    /// Genera un id de miembro único y reproducible (`<group>-<n>`).
    [[nodiscard]] std::string next_member_id();

    std::string group_id_;
    GroupState state_ = GroupState::Empty;
    Generation generation_ = 0;
    std::string leader_id_;
    std::unordered_map<std::string, Member> members_;
    std::uint64_t member_seq_ = 0;  ///< Contador para generar ids de miembro.
};

}  // namespace nexus
