/// @file   kafka/messages.cpp
/// @brief  Implementación de cabeceras y ApiVersions del subconjunto Kafka (F7b).
/// @ingroup kafka

#include "kafka/messages.hpp"

namespace nexus::kafka {
namespace {

/// Versión mínima a partir de la cual cada API usa formato **flexible**; -1 = no soportada.
[[nodiscard]] std::int16_t flexible_since(ApiKey api_key) noexcept {
    switch (api_key) {
        case ApiKey::Produce:
            return 9;
        case ApiKey::Fetch:
            return 12;
        case ApiKey::ListOffsets:
            return 6;
        case ApiKey::Metadata:
            return 9;
        case ApiKey::ApiVersions:
            return 3;
    }
    return -1;
}

}  // namespace

bool is_flexible(ApiKey api_key, std::int16_t api_version) noexcept {
    const std::int16_t since = flexible_since(api_key);
    return since >= 0 && api_version >= since;
}

std::int16_t request_header_version(ApiKey api_key, std::int16_t api_version) noexcept {
    return is_flexible(api_key, api_version) ? std::int16_t{2} : std::int16_t{1};
}

std::int16_t response_header_version(ApiKey api_key, std::int16_t api_version) noexcept {
    // Caso especial: ApiVersions responde SIEMPRE con cabecera v0 (sin tagged fields).
    if (api_key == ApiKey::ApiVersions) {
        return 0;
    }
    return is_flexible(api_key, api_version) ? std::int16_t{1} : std::int16_t{0};
}

expected<RequestHeader> decode_request_header(Decoder& dec) {
    RequestHeader header;
    const expected<std::int16_t> api_key = dec.get_i16();
    if (!api_key) {
        return std::unexpected(api_key.error());
    }
    const expected<std::int16_t> api_version = dec.get_i16();
    if (!api_version) {
        return std::unexpected(api_version.error());
    }
    const expected<std::int32_t> correlation_id = dec.get_i32();
    if (!correlation_id) {
        return std::unexpected(correlation_id.error());
    }
    header.api_key = *api_key;
    header.api_version = *api_version;
    header.correlation_id = *correlation_id;

    const std::int16_t header_version =
        request_header_version(static_cast<ApiKey>(*api_key), *api_version);
    // header v1+ lleva client_id (NULLABLE_STRING clásico, también en la cabecera flexible v2).
    expected<std::optional<std::string>> client_id = dec.get_nullable_string();
    if (!client_id) {
        return std::unexpected(client_id.error());
    }
    header.client_id = std::move(*client_id);
    if (header_version >= 2) {
        const expected<void> tags = dec.skip_tagged_fields();
        if (!tags) {
            return std::unexpected(tags.error());
        }
    }
    return header;
}

void encode_response_header(Encoder& enc, std::int32_t correlation_id,
                            std::int16_t header_version) {
    enc.put_i32(correlation_id);
    if (header_version >= 1) {
        enc.put_empty_tagged_fields();
    }
}

std::vector<ApiVersionRange> supported_apis() {
    return {
        ApiVersionRange{.api_key = static_cast<std::int16_t>(ApiKey::Produce),
                        .min_version = 0,
                        .max_version = 9},
        ApiVersionRange{.api_key = static_cast<std::int16_t>(ApiKey::Fetch),
                        .min_version = 0,
                        .max_version = 12},
        ApiVersionRange{.api_key = static_cast<std::int16_t>(ApiKey::ListOffsets),
                        .min_version = 0,
                        .max_version = 7},
        ApiVersionRange{.api_key = static_cast<std::int16_t>(ApiKey::Metadata),
                        .min_version = 0,
                        .max_version = 9},
        ApiVersionRange{.api_key = static_cast<std::int16_t>(ApiKey::ApiVersions),
                        .min_version = 0,
                        .max_version = 3},
    };
}

void encode_api_versions_response(Encoder& enc, const ApiVersionsResponse& resp) {
    enc.put_i16(resp.error_code);
    enc.put_compact_array_len(static_cast<std::int32_t>(resp.api_keys.size()));
    for (const ApiVersionRange& api : resp.api_keys) {
        enc.put_i16(api.api_key);
        enc.put_i16(api.min_version);
        enc.put_i16(api.max_version);
        enc.put_empty_tagged_fields();  // tagged fields por elemento (versión flexible)
    }
    enc.put_i32(resp.throttle_time_ms);
    enc.put_empty_tagged_fields();  // tagged fields del cuerpo
}

ApiVersionsResponse make_api_versions_response() {
    return ApiVersionsResponse{
        .error_code = 0, .api_keys = supported_apis(), .throttle_time_ms = 0};
}

}  // namespace nexus::kafka
