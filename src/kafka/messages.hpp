/// @file   kafka/messages.hpp
/// @brief  Cabeceras y mensajes del subconjunto Kafka: framing, RequestHeader y ApiVersions (F7b).
/// @ingroup kafka

#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "common/bytes.hpp"
#include "common/error.hpp"
#include "kafka/codec.hpp"

namespace nexus::kafka {

/// Claves de API (subconjunto soportado, F7). Valores oficiales del protocolo de Kafka.
/// El tipo base es `int16_t` porque así viaja `api_key` en el wire (INT16), no por su rango.
/// NOLINTNEXTLINE(performance-enum-size): el tamaño espeja el campo del protocolo (INT16).
enum class ApiKey : std::int16_t {
    Produce = 0,
    Fetch = 1,
    Metadata = 3,
    ApiVersions = 18,
};

/// @brief Cabecera de **petición** de Kafka (sin el prefijo de tamaño). Afinidad: INMUTABLE.
/// @details Tras `Size:INT32` viene esta cabecera: `api_key:INT16 | api_version:INT16 |
///   correlation_id:INT32 | client_id:NULLABLE_STRING` (desde header v1) `| TAG_BUFFER` (header
///   v2). `client_id` es **NULLABLE_STRING clásico** incluso en la cabecera flexible v2 (solo
///   cambian los *tagged fields*).
struct RequestHeader {
    std::int16_t api_key = 0;
    std::int16_t api_version = 0;
    std::int32_t correlation_id = 0;
    std::optional<std::string> client_id;
};

/// @brief ¿Es **flexible** (compacta + *tagged fields*) la versión @p api_version de @p api_key?
/// @details Cada API gana formato flexible a partir de cierta versión: Produce v9+, Fetch v12+,
///   Metadata v9+, ApiVersions v3+. Para APIs no soportadas devuelve `false`.
[[nodiscard]] bool is_flexible(ApiKey api_key, std::int16_t api_version) noexcept;

/// @brief Versión de la **cabecera de petición** (1 = con `client_id`; 2 = flexible) para
///   @p api_key/@p api_version. Asumimos clientes modernos (siempre envían `client_id`).
[[nodiscard]] std::int16_t request_header_version(ApiKey api_key,
                                                  std::int16_t api_version) noexcept;

/// @brief Versión de la **cabecera de respuesta** (0 = solo correlation_id; 1 = flexible).
/// @details Caso especial famoso: `ApiVersions` responde **siempre** con cabecera **v0** aunque su
///   cuerpo sea flexible (para no romper a clientes que aún negocian la versión).
[[nodiscard]] std::int16_t response_header_version(ApiKey api_key,
                                                   std::int16_t api_version) noexcept;

/// @brief Decodifica una cabecera de petición desde @p dec (ya sin el prefijo `Size`).
/// @details Lee `api_key`/`api_version`/`correlation_id`, deduce la versión de cabecera y, según
///   ella, lee `client_id` y salta los *tagged fields*. Decodificador defensivo.
[[nodiscard]] expected<RequestHeader> decode_request_header(Decoder& dec);

/// @brief Escribe una cabecera de **respuesta** en @p enc: `correlation_id` y, si la versión de
///   cabecera es flexible (1), una sección de *tagged fields* vacía.
void encode_response_header(Encoder& enc, std::int32_t correlation_id, std::int16_t header_version);

/// Rango de versiones soportado por una API (para la respuesta de ApiVersions).
struct ApiVersionRange {
    std::int16_t api_key = 0;
    std::int16_t min_version = 0;
    std::int16_t max_version = 0;
};

/// Tabla de APIs soportadas y sus rangos de versión (lo que anunciamos en ApiVersions).
[[nodiscard]] std::vector<ApiVersionRange> supported_apis();

/// @brief Respuesta de **ApiVersions** (v3, flexible). Afinidad: INMUTABLE.
struct ApiVersionsResponse {
    std::int16_t error_code = 0;
    std::vector<ApiVersionRange> api_keys;
    std::int32_t throttle_time_ms = 0;
};

/// @brief Serializa el **cuerpo** de una respuesta ApiVersions v3 (flexible) en @p enc.
/// @details `error_code:INT16 | api_keys:COMPACT_ARRAY{api_key,min,max,TAG_BUFFER} |
///   throttle_time_ms:INT32 | TAG_BUFFER`.
void encode_api_versions_response(Encoder& enc, const ApiVersionsResponse& resp);

/// @brief Construye la respuesta ApiVersions anunciando `supported_apis()` (sin error).
[[nodiscard]] ApiVersionsResponse make_api_versions_response();

}  // namespace nexus::kafka
