/// @file   broker/topic_catalog.cpp
/// @brief  Implementación de TopicCatalog (managers por núcleo + creación replicada en arranque).
/// @ingroup broker

#include "broker/topic_catalog.hpp"

#include <utility>

namespace nexus {

TopicCatalog::TopicCatalog(const std::filesystem::path& data_dir, int num_cores) {
    const int cores = num_cores < 1 ? 1 : num_cores;
    managers_.reserve(static_cast<std::size_t>(cores));
    for (int core = 0; core < cores; ++core) {
        managers_.push_back(std::make_unique<TopicManager>(data_dir, cores, core));
    }
}

std::vector<TopicManager*> TopicCatalog::managers() const {
    std::vector<TopicManager*> result;
    result.reserve(managers_.size());
    for (const std::unique_ptr<TopicManager>& manager : managers_) {
        result.push_back(manager.get());
    }
    return result;
}

expected<TopicMetadata> TopicCatalog::create_topic(const std::string& name,
                                                   std::int32_t partition_count,
                                                   TopicConfig config) {
    TopicMetadata authoritative;  // la del núcleo 0; las demás son idénticas salvo el instante.
    for (std::size_t core = 0; core < managers_.size(); ++core) {
        expected<TopicMetadata> meta = managers_[core]->create_topic(name, partition_count, config);
        if (!meta) {
            // Garantía fuerte: deshace los núcleos ya creados (orden inverso); el borrado es
            // inocuo (idempotente), así que su resultado se ignora a propósito.
            for (std::size_t done = core; done-- > 0;) {
                [[maybe_unused]] const expected<void> rolled_back =
                    managers_[done]->delete_topic(name);
            }
            return std::unexpected(meta.error());
        }
        if (core == 0) {
            authoritative = std::move(*meta);
        }
    }
    return authoritative;
}

}  // namespace nexus
