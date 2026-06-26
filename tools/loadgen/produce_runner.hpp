/// @file   tools/loadgen/produce_runner.hpp
/// @brief  ProduceRunner: runner de carga que publica un payload fijo por el `Client` nativo.
/// @ingroup loadgen

#pragma once

#include <cstddef>
#include <memory>
#include <string>
#include <vector>

#include "client/client.hpp"
#include "client/endpoint.hpp"
#include "common/error.hpp"
#include "common/types.hpp"
#include "request_runner.hpp"

namespace nexus::loadgen {

/// @brief Runner de carga que publica un payload fijo en una (topic, partición) por una conexión
///   `Client` propia. Afinidad: REACTOR-LOCAL (una conexión por runner; no se comparte entre
///   hilos).
/// @details Cada conexión del generador posee un `ProduceRunner`: una conexión TCP bloqueante y un
///   payload preconstruido que reenvía en cada `run_once`. Así el bucle caliente no asigna memoria
///   por petición (el payload se reserva una vez) ni comparte estado entre hilos (shared-nothing).
class ProduceRunner : public RequestRunner {
public:
    /// @brief Construye sobre una conexión @p client ya abierta (lo usa `create`).
    ProduceRunner(Client client, std::string topic, PartitionId partition,
                  std::vector<std::byte> payload) noexcept;

    /// @brief Conecta a @p endpoint y prepara el runner para publicar en @p topic/@p partition.
    /// @param payload_size Bytes de payload de cada record (se reserva una vez).
    /// @return el runner o el error de conexión.
    [[nodiscard]] static expected<std::unique_ptr<ProduceRunner>> create(const Endpoint& endpoint,
                                                                         std::string topic,
                                                                         PartitionId partition,
                                                                         std::size_t payload_size);

    /// @copydoc RequestRunner::run_once
    [[nodiscard]] expected<void> run_once() override;

private:
    Client client_;
    std::string topic_;
    PartitionId partition_;
    std::vector<std::byte> payload_;
};

}  // namespace nexus::loadgen
