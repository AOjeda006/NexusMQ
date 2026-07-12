/// @file   storage/local_storage_tier.cpp
/// @brief  Implementación de LocalStorageTier (directorio objeto local, ADR-0032).
/// @ingroup storage

#include "storage/local_storage_tier.hpp"

#include <algorithm>
#include <charconv>
#include <cstdint>
#include <filesystem>
#include <format>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

namespace nexus {
namespace {

/// Traduce un `std::error_code` de `<filesystem>` al modelo de error del núcleo (ADR-0009).
[[nodiscard]] Error fs_error(const std::error_code& ec, std::string_view op) {
    ErrorCode code = ErrorCode::IoError;
    if (ec == std::errc::no_space_on_device) {
        code = ErrorCode::OutOfSpace;
    } else if (ec == std::errc::no_such_file_or_directory) {
        code = ErrorCode::NotFound;
    }
    return Error{code, std::string{op} + ": " + ec.message()};
}

}  // namespace

LocalStorageTier::LocalStorageTier(std::filesystem::path object_dir)
    : object_dir_(std::move(object_dir)) {}

std::filesystem::path LocalStorageTier::path_of(const TierObjectKey& key) const {
    return object_dir_ / key.encode();
}

expected<void> LocalStorageTier::atomic_copy(const std::filesystem::path& from,
                                             const std::filesystem::path& to) const {
    std::error_code ec;
    std::filesystem::create_directories(to.parent_path(), ec);
    if (ec) {
        return std::unexpected(fs_error(ec, "create_directories"));
    }
    // Temporal hermano único: en el mismo directorio que el destino, para que `rename` sea atómico
    // (misma partición de FS). El contador atómico evita colisiones entre puts concurrentes.
    const auto seq = temp_seq_.fetch_add(1, std::memory_order_relaxed);
    const std::filesystem::path tmp =
        to.parent_path() / (to.filename().string() + ".tmp." + std::to_string(seq));

    std::filesystem::copy_file(from, tmp, std::filesystem::copy_options::overwrite_existing, ec);
    if (ec) {
        return std::unexpected(fs_error(ec, "copy_file"));
    }
    std::filesystem::rename(tmp, to, ec);
    if (ec) {
        std::error_code ignored;
        std::filesystem::remove(tmp, ignored);  // limpia el temporal; el error original manda.
        return std::unexpected(fs_error(ec, "rename"));
    }
    return {};
}

expected<void> LocalStorageTier::put_file(const TierObjectKey& key,
                                          const std::filesystem::path& source) {
    std::error_code ec;
    if (!std::filesystem::exists(source, ec) || ec) {
        return make_error(ErrorCode::NotFound, "put_file: origen inexistente: " + source.string());
    }
    return atomic_copy(source, path_of(key));
}

expected<void> LocalStorageTier::fetch_file(const TierObjectKey& key,
                                            const std::filesystem::path& dest) const {
    const std::filesystem::path src = path_of(key);
    std::error_code ec;
    if (!std::filesystem::exists(src, ec) || ec) {
        return make_error(ErrorCode::NotFound, "fetch_file: objeto inexistente: " + key.encode());
    }
    return atomic_copy(src, dest);
}

expected<bool> LocalStorageTier::contains(const TierObjectKey& key) const {
    std::error_code ec;
    const bool present = std::filesystem::exists(path_of(key), ec);
    if (ec) {
        return std::unexpected(fs_error(ec, "exists"));
    }
    return present;
}

expected<std::uint64_t> LocalStorageTier::object_size(const TierObjectKey& key) const {
    std::error_code ec;
    const auto size = std::filesystem::file_size(path_of(key), ec);
    if (ec) {
        return std::unexpected(fs_error(ec, "file_size"));
    }
    return static_cast<std::uint64_t>(size);
}

expected<void> LocalStorageTier::remove(const TierObjectKey& key) {
    std::error_code ec;
    std::filesystem::remove(path_of(key), ec);  // idempotente: `false` si no existía, sin error.
    if (ec) {
        return std::unexpected(fs_error(ec, "remove"));
    }
    return {};
}

expected<std::vector<Offset>> LocalStorageTier::list_segment_bases(std::string_view topic,
                                                                   std::int32_t partition) const {
    const std::filesystem::path prefix =
        object_dir_ / std::string{topic} / std::to_string(partition);
    std::vector<Offset> bases;

    std::error_code ec;
    if (!std::filesystem::exists(prefix, ec)) {
        return bases;  // prefijo inexistente: partición sin objetos (no es error).
    }
    std::filesystem::directory_iterator it(prefix, ec);
    if (ec) {
        return std::unexpected(fs_error(ec, "directory_iterator"));
    }
    for (const std::filesystem::directory_iterator end; it != end; it.increment(ec)) {
        if (ec) {
            return std::unexpected(fs_error(ec, "directory_iterator"));
        }
        if (it->path().extension() != ".log") {
            continue;  // solo los `.log` marcan un segmento; el `.index` lo acompaña.
        }
        const std::string stem = it->path().stem().string();
        Offset base = 0;
        const auto* first = stem.data();
        const auto* last = stem.data() + stem.size();
        const auto [ptr, parse_ec] = std::from_chars(first, last, base);
        if (parse_ec == std::errc{} && ptr == last && base >= 0) {
            bases.push_back(base);
        }
    }
    std::ranges::sort(bases);
    return bases;
}

}  // namespace nexus
