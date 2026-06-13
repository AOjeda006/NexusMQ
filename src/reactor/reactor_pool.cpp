/// @file   reactor/reactor_pool.cpp
/// @brief  Implementación de ReactorPool (crear/cablear/fijar/unir N reactores).
/// @ingroup reactor

#include "reactor/reactor_pool.hpp"

#include <pthread.h>
#include <sched.h>

#include <algorithm>
#include <cstddef>
#include <thread>
#include <utility>
#include <vector>

namespace nexus {

namespace {

// Fija @p thread al núcleo @p core (módulo nº de CPUs). Mejor esfuerzo: si el entorno restringe la
// afinidad (sandbox de CI…), se ignora el fallo —no es crítico para la corrección, solo locality—.
void pin_thread_to_core(std::thread& thread, int core) {
    const unsigned cpus = std::max(1U, std::thread::hardware_concurrency());
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(static_cast<unsigned>(core) % cpus, &cpuset);
    static_cast<void>(pthread_setaffinity_np(thread.native_handle(), sizeof(cpuset), &cpuset));
}

}  // namespace

ReactorPool::~ReactorPool() {
    shutdown();
}

void ReactorPool::start(int num_reactors, const ProactorFactory& make_proactor) {
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

    // Lanza un hilo por reactor, fijado a su núcleo.
    threads_.reserve(reactors_.size());
    for (int core = 0; core < num_reactors; ++core) {
        Reactor* reactor = reactors_[static_cast<std::size_t>(core)].get();
        threads_.emplace_back([reactor] { reactor->run(); });
        pin_thread_to_core(threads_.back(), core);
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
