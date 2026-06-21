/// @file   reactor/reactor_pool.hpp
/// @brief  ReactorPool: crea N reactores (uno por núcleo), los fija por afinidad y los une.
/// @ingroup reactor

#pragma once

#include <functional>
#include <memory>
#include <thread>
#include <vector>

#include "common/types.hpp"
#include "io/proactor.hpp"
#include "reactor/reactor.hpp"

namespace nexus {

/// @brief Conjunto de reactores *thread-per-core*: uno por núcleo, *pinned*, unidos al apagar.
///   Afinidad: THREAD-SAFE.
/// @details Crea N `Reactor`, cablea sus peers para el paso de mensajes cross-core, lanza un hilo
///   por reactor fijado a su núcleo (`pthread_setaffinity_np`) y los une en `shutdown`. El
///   `Proactor` de cada reactor lo construye una **factoría** inyectada (DIP): producción crea
///   `IoUringBackend`; los tests, un doble. `start`/`shutdown` son plano de control (se llaman una
///   vez, no concurrentemente); tras `start`, los accesores son de solo lectura y seguros entre
///   hilos.
/// @invariant Tras `start(n)`, `size() == n` hasta `shutdown()`. El pool **posee** los reactores y
///   sus hilos; el destructor hace `shutdown` (RAII) si no se llamó.
/// @note No copiable ni movible: es el dueño raíz de los reactores y sus hilos.
class ReactorPool {
public:
    /// Factoría del `Proactor` de cada reactor (recibe su `core_id`). Inyectada (DIP, testable).
    using ProactorFactory = std::function<std::unique_ptr<Proactor>(int core_id)>;

    ReactorPool() = default;
    ~ReactorPool();

    ReactorPool(const ReactorPool&) = delete;
    ReactorPool& operator=(const ReactorPool&) = delete;
    ReactorPool(ReactorPool&&) = delete;
    ReactorPool& operator=(ReactorPool&&) = delete;

    /// @brief Crea @p num_reactors reactores (proactor de @p make_proactor), los cablea y arranca
    ///   un hilo por reactor fijado a su núcleo. Llamar una sola vez.
    void start(int num_reactors, const ProactorFactory& make_proactor);

    /// @brief Como `start`, pero **no** lanza hilo para el núcleo 0: lo corre el llamante (inline).
    /// @details Para un daemon cuyo hilo principal debe **correr** un reactor y atender señales:
    ///   tras esta llamada, los núcleos 1..N-1 corren en sus hilos y el llamante debe invocar
    ///   `reactor(0).run()` (bloquea). Así el apagado del núcleo 0 sigue siendo `reactor(0).stop()`
    ///   —escribe un `eventfd`, **async-signal-safe**— y no hace falta un mecanismo de espera extra
    ///   en el hilo principal. `shutdown()` para y une el resto. Llamar una sola vez.
    void start_main_inline(int num_reactors, const ProactorFactory& make_proactor);

    /// @brief Apagado ordenado: `stop()` a todos los reactores y `join` de sus hilos. Idempotente.
    void shutdown();

    /// Reactor asignado a la partición @p partition (reparto núcleo = partition % size()).
    [[nodiscard]] Reactor& reactor_for(PartitionId partition) const;

    /// Reactor del núcleo @p core_id.
    [[nodiscard]] Reactor& reactor(int core_id) const;

    /// Número de reactores en el pool.
    [[nodiscard]] int size() const noexcept { return static_cast<int>(reactors_.size()); }

private:
    /// Crea los @p num_reactors reactores (proactor de @p make_proactor) y cablea sus peers.
    void create_and_wire(int num_reactors, const ProactorFactory& make_proactor);
    /// Lanza un hilo para el reactor @p core (que corre su bucle), fijado a su núcleo.
    void launch(int core);

    std::vector<std::unique_ptr<Reactor>> reactors_;
    std::vector<std::thread> threads_;  // un hilo por reactor (jthread no está en libc++ 18)
};

}  // namespace nexus
