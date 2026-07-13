/// @file   broker/topic_cluster.cpp
/// @brief  Implementación del fan-out cross-core de crear/borrar topic (ADR-0026).
/// @ingroup broker

#include "broker/topic_cluster.hpp"

#include <cstddef>
#include <utility>

#include "broker/topic_manager.hpp"
#include "reactor/cross_core_call.hpp"
#include "reactor/partition_router.hpp"
#include "reactor/reactor.hpp"

namespace nexus {

task<expected<TopicMetadata>> create_topic_on_cluster(Reactor& self, PartitionRouter& partitions,
                                                      std::span<TopicManager* const> topics_by_core,
                                                      std::string name,
                                                      std::int32_t partition_count,
                                                      TopicConfig config) {
    const int cores = partitions.core_count();
    TopicMetadata authoritative;  // la del núcleo 0; las demás son idénticas salvo el instante.
    for (int core = 0; core < cores; ++core) {
        // `call_on` corre la creación en el hilo del reactor del núcleo: toca solo su
        // `TopicManager` (sin estado compartido entre hilos). El llamante sigue suspendido, así que
        // las capturas por referencia (`name`, `config`) viven durante el viaje.
        expected<TopicMetadata> meta = co_await call_on(
            self, partitions.reactor(core),
            [topics_by_core, core, &name, partition_count, &config] {
                return topics_by_core[static_cast<std::size_t>(core)]->create_topic(
                    name, partition_count, config);
            });
        if (!meta) {
            // Garantía fuerte: deshace los núcleos ya creados (orden inverso); el borrado es
            // inocuo.
            for (int done = core - 1; done >= 0; --done) {
                static_cast<void>(
                    co_await call_on(self, partitions.reactor(done), [topics_by_core, done, &name] {
                        return topics_by_core[static_cast<std::size_t>(done)]->delete_topic(name);
                    }));
            }
            co_return std::unexpected(meta.error());
        }
        if (core == 0) {
            authoritative = std::move(*meta);
        }
    }
    co_return authoritative;
}

task<expected<void>> delete_topic_on_cluster(Reactor& self, PartitionRouter& partitions,
                                             std::span<TopicManager* const> topics_by_core,
                                             std::string name) {
    const int cores = partitions.core_count();
    expected<void> result;
    for (int core = 0; core < cores; ++core) {
        const expected<void> deleted =
            co_await call_on(self, partitions.reactor(core), [topics_by_core, core, &name] {
                return topics_by_core[static_cast<std::size_t>(core)]->delete_topic(name);
            });
        // El núcleo 0 es autoritativo (todos registran el topic al crearlo); los demás se intentan
        // igual para dejar el clúster coherente.
        if (core == 0) {
            result = deleted;
        }
    }
    co_return result;
}

task<expected<TopicMetadata>> update_topic_config_on_cluster(
    Reactor& self, PartitionRouter& partitions, std::span<TopicManager* const> topics_by_core,
    std::string name, std::optional<std::int64_t> retention_ms,
    std::optional<std::int64_t> retention_bytes) {
    const int cores = partitions.core_count();
    expected<TopicMetadata> authoritative =
        make_error(ErrorCode::NotFound, "topic inexistente: " + name);
    for (int core = 0; core < cores; ++core) {
        // Publica la config en el hilo del reactor de cada núcleo (toca solo su `TopicManager`). El
        // update es idempotente y no reserva recursos: no hace falta rollback.
        expected<TopicMetadata> meta = co_await call_on(
            self, partitions.reactor(core),
            [topics_by_core, core, &name, retention_ms, retention_bytes] {
                return topics_by_core[static_cast<std::size_t>(core)]->update_config(
                    name, retention_ms, retention_bytes);
            });
        if (core == 0) {
            authoritative = std::move(meta);  // el núcleo 0 es autoritativo.
        }
    }
    co_return authoritative;
}

}  // namespace nexus
