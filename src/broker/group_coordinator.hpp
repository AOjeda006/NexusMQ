/// @file   broker/group_coordinator.hpp
/// @brief  GroupCoordinator: posee los grupos de consumidores de su reactor y enruta sus
/// peticiones.
/// @ingroup broker

#pragma once

#include <chrono>
#include <cstddef>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "broker/consumer_group.hpp"
#include "common/error.hpp"
#include "common/types.hpp"

namespace nexus {

/// @brief Coordinador de grupos de consumidores: un mapa `group_id → ConsumerGroup`. Afinidad:
///   REACTOR-LOCAL (vive en el `RequestRouter` de su reactor; sin sincronización).
/// @details Delega en el `ConsumerGroup` correspondiente: `join` **crea el grupo** si no existe
///   (alta del primer consumidor); `sync`/`heartbeat`/`leave` exigen que el grupo ya exista
///   (`NotFound` si no). Es la frontera de dominio que el `RequestRouter` traduce a/desde el wire.
///   La FSM no hace E/S (ADR-0015): el reloj se inyecta como `MonoTime now` y el portador llama a
///   `tick` para expirar sesiones caídas.
class GroupCoordinator {
public:
    /// @brief (Re)incorpora un miembro a @p group_id, creando el grupo si es la primera alta.
    [[nodiscard]] expected<JoinResult> join(MonoTime now, const std::string& group_id,
                                            std::string member_id,
                                            std::vector<std::byte> subscription,
                                            std::chrono::milliseconds session_timeout);

    /// @brief Reparte/recoge asignaciones de @p group_id. `NotFound` si el grupo no existe.
    [[nodiscard]] expected<SyncResult> sync(MonoTime now, std::string_view group_id,
                                            std::string_view member_id, Generation generation,
                                            const std::vector<MemberAssignment>& assignments);

    /// @brief Renueva la liveness de un miembro de @p group_id. `NotFound` si el grupo no existe.
    [[nodiscard]] expected<HeartbeatStatus> heartbeat(MonoTime now, std::string_view group_id,
                                                      std::string_view member_id,
                                                      Generation generation);

    /// @brief Da de baja un miembro de @p group_id. `NotFound` si el grupo o el miembro no existen.
    expected<void> leave(std::string_view group_id, std::string_view member_id);

    /// @brief Expira las sesiones caídas de todos los grupos (llamar periódicamente desde el
    /// reactor).
    void tick(MonoTime now);

    [[nodiscard]] std::size_t group_count() const noexcept { return groups_.size(); }
    /// Acceso al grupo (observabilidad/pruebas); `nullptr` si no existe.
    [[nodiscard]] ConsumerGroup* find(std::string_view group_id);

private:
    std::unordered_map<std::string, ConsumerGroup> groups_;
};

}  // namespace nexus
