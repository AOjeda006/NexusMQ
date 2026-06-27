/// @file   reactor/reactor_pool.cpp
/// @brief  Implementación de ReactorPool (crear/cablear/fijar/unir N reactores).
/// @ingroup reactor

#include "reactor/reactor_pool.hpp"

#if defined(_WIN32)
#ifndef NOMINMAX
#define NOMINMAX  // evita los macros min/max de <windows.h>, que chocan con std::max
#endif
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>  // SetThreadAffinityMask, DWORD_PTR
#else
#include <pthread.h>
#include <sched.h>
#endif

#include <algorithm>
#include <cstddef>
#include <thread>
#include <utility>
#include <vector>

namespace nexus {

namespace {

// Fija @p thread al núcleo @p core (módulo nº de CPUs). Mejor esfuerzo: si el entorno restringe la
// afinidad (sandbox de CI…), se ignora el fallo —no es crítico para la corrección, solo locality—.
// El handle nativo y la API de afinidad son específicos de plataforma; el reparto (core % cpus) es
// común (ADR-0028): pthread_setaffinity_np/cpu_set_t en POSIX, SetThreadAffinityMask en Win32.
void pin_thread_to_core(std::thread& thread, int core) {
    const unsigned cpus = std::max(1U, std::thread::hardware_concurrency());
    const unsigned target = static_cast<unsigned>(core) % cpus;
#if defined(_WIN32)
    // La máscara de afinidad de Win32 cubre hasta 64 CPUs (un grupo de procesadores); en máquinas
    // mayores el pin es aproximado —igual que el «mejor esfuerzo» de POSIX—. El módulo evita un
    // desplazamiento >= 64 (comportamiento indefinido).
    constexpr unsigned kAffinityBits = sizeof(DWORD_PTR) * 8;
    const DWORD_PTR mask = static_cast<DWORD_PTR>(1) << (target % kAffinityBits);
    static_cast<void>(::SetThreadAffinityMask(thread.native_handle(), mask));
#else
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(target, &cpuset);
    static_cast<void>(pthread_setaffinity_np(thread.native_handle(), sizeof(cpuset), &cpuset));
#endif
}

}  // namespace

ReactorPool::~ReactorPool() {
    shutdown();
}

void ReactorPool::create_and_wire(int num_reactors, const ProactorFactory& make_proactor) {
    reactors_.reserve(static_cast<std::size_t>(num_reactors));
    for (int core = 0; core < num_reactors; ++core) {
        reactors_.push_back(std::make_unique<Reactor>(core, num_reactors, make_proactor(core)));
    }

    // Cablea los peers (indexados por core_id) en cada reactor: habilita el paso de mensajes.
    std::vector<Reactor*> peers;
    peers.reserve(reactors_.size());
    for (const std::unique_ptr<Reactor>& reactor : reactors_) {
        peers.push_back(reactor.get());
    }
    for (const std::unique_ptr<Reactor>& reactor : reactors_) {
        reactor->connect_peers(peers);
    }
    threads_.reserve(reactors_.size());
}

void ReactorPool::launch(int core) {
    Reactor* reactor = reactors_[static_cast<std::size_t>(core)].get();
    threads_.emplace_back([reactor] { reactor->run(); });
    pin_thread_to_core(threads_.back(), core);
}

void ReactorPool::start(int num_reactors, const ProactorFactory& make_proactor) {
    create_and_wire(num_reactors, make_proactor);
    for (int core = 0; core < num_reactors; ++core) {
        launch(core);
    }
}

void ReactorPool::start_main_inline(int num_reactors, const ProactorFactory& make_proactor) {
    create_and_wire(num_reactors, make_proactor);
    // El núcleo 0 lo corre el llamante (Server::run); aquí solo los workers 1..N-1.
    for (int core = 1; core < num_reactors; ++core) {
        launch(core);
    }
}

void ReactorPool::shutdown() {
    for (const std::unique_ptr<Reactor>& reactor : reactors_) {
        reactor->stop();
    }
    for (std::thread& thread : threads_) {
        if (thread.joinable()) {
            thread.join();
        }
    }
    threads_.clear();
    reactors_.clear();
}

Reactor& ReactorPool::reactor_for(PartitionId partition) const {
    const std::size_t index = static_cast<std::size_t>(partition) % reactors_.size();
    return *reactors_[index];
}

Reactor& ReactorPool::reactor(int core_id) const {
    return *reactors_[static_cast<std::size_t>(core_id)];
}

}  // namespace nexus
