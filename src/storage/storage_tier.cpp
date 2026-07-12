/// @file   storage/storage_tier.cpp
/// @brief  Serialización de `TierObjectKey` (clave de objeto del tier, ADR-0032).
/// @ingroup storage

#include "storage/storage_tier.hpp"

#include <charconv>
#include <format>
#include <string>
#include <string_view>
#include <system_error>

namespace nexus {
namespace {

/// Extensión de fichero para cada pieza de segmento (sin el punto).
[[nodiscard]] std::string_view kind_ext(SegmentFileKind kind) noexcept {
    return kind == SegmentFileKind::Log ? "log" : "index";
}

/// Parsea @p text como un entero decimal no negativo; nullopt si no es un número exacto o es < 0.
template <class Int>
[[nodiscard]] expected<Int> parse_nonneg(std::string_view text) {
    Int value = 0;
    const auto* first = text.data();
    const auto* last = text.data() + text.size();
    const auto [ptr, ec] = std::from_chars(first, last, value);
    if (ec != std::errc{} || ptr != last || value < 0) {
        return make_error(ErrorCode::InvalidArgument,
                          "campo numérico inválido: " + std::string{text});
    }
    return value;
}

}  // namespace

std::string TierObjectKey::encode() const {
    return std::format("{}/{}/{:020d}.{}", topic, partition, base_offset, kind_ext(kind));
}

expected<TierObjectKey> TierObjectKey::decode(std::string_view key) {
    // Estructura: <topic>/<partition>/<base:020>.<ext>. Se parte por la derecha para no confundir
    // ningún carácter del topic con los separadores estructurales.
    const auto file_sep = key.rfind('/');
    if (file_sep == std::string_view::npos) {
        return make_error(ErrorCode::InvalidArgument, "clave de tier sin separadores");
    }
    const std::string_view file = key.substr(file_sep + 1);
    const std::string_view head = key.substr(0, file_sep);

    const auto part_sep = head.rfind('/');
    if (part_sep == std::string_view::npos) {
        return make_error(ErrorCode::InvalidArgument, "clave de tier sin partición");
    }
    const std::string_view topic = head.substr(0, part_sep);
    const std::string_view partition_str = head.substr(part_sep + 1);
    if (topic.empty()) {
        return make_error(ErrorCode::InvalidArgument, "clave de tier con topic vacío");
    }

    const auto dot = file.rfind('.');
    if (dot == std::string_view::npos) {
        return make_error(ErrorCode::InvalidArgument, "clave de tier sin extensión");
    }
    const std::string_view base_str = file.substr(0, dot);
    const std::string_view ext = file.substr(dot + 1);

    SegmentFileKind kind{};
    if (ext == "log") {
        kind = SegmentFileKind::Log;
    } else if (ext == "index") {
        kind = SegmentFileKind::Index;
    } else {
        return make_error(ErrorCode::InvalidArgument,
                          "extensión de tier desconocida: " + std::string{ext});
    }

    auto partition = parse_nonneg<std::int32_t>(partition_str);
    if (!partition) {
        return std::unexpected(partition.error());
    }
    auto base = parse_nonneg<Offset>(base_str);
    if (!base) {
        return std::unexpected(base.error());
    }
    return TierObjectKey{
        .topic = std::string{topic}, .partition = *partition, .base_offset = *base, .kind = kind};
}

}  // namespace nexus
