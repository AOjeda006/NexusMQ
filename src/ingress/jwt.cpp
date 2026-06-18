/// @file   ingress/jwt.cpp
/// @brief  Implementación del verificador de JWT HS256 (RFC 7515/7519).
/// @ingroup ingress

#include "ingress/jwt.hpp"

#include <cstddef>
#include <utility>

#include "common/base64.hpp"
#include "common/bytes.hpp"
#include "common/sha256.hpp"
#include "ingress/json_value.hpp"

namespace nexus {

namespace {

/// Vista de bytes sobre una cadena (para alimentar HMAC/base64 sin copiar).
ByteSpan as_bytes(std::string_view text) noexcept {
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast): char↔byte, sin alternativa.
    return {reinterpret_cast<const std::byte*>(text.data()), text.size()};
}

/// Vista de cadena sobre bytes decodificados (para parsear JSON).
std::string_view as_text(const std::vector<std::byte>& data) noexcept {
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast): byte↔char, sin alternativa.
    return {reinterpret_cast<const char*>(data.data()), data.size()};
}

/// Comparación en tiempo constante (evita filtrar la firma por un *timing side-channel*).
bool constant_time_equal(ByteSpan lhs, ByteSpan rhs) noexcept {
    if (lhs.size() != rhs.size()) {
        return false;
    }
    unsigned diff = 0;
    for (std::size_t i = 0; i < lhs.size(); ++i) {
        diff |= std::to_integer<unsigned>(lhs[i]) ^ std::to_integer<unsigned>(rhs[i]);
    }
    return diff == 0;
}

std::unexpected<Error> reject(std::string_view why) {
    return make_error(ErrorCode::InvalidArgument, std::string{"JWT: "} + std::string{why});
}

/// Decodifica un segmento base64url y lo parsea como objeto JSON.
expected<JsonValue> decode_json_segment(std::string_view segment, std::string_view what) {
    const auto bytes = base64url_decode(segment);
    if (!bytes) {
        return reject(std::string{what} + " no es base64url");
    }
    auto json = parse_json(as_text(*bytes));
    if (!json) {
        return reject(std::string{what} + " no es JSON");
    }
    if (!json->is_object()) {
        return reject(std::string{what} + " no es un objeto JSON");
    }
    return json;
}

/// Valida los *claims* temporales (`exp`/`nbf`/`iat`) con el «ahora» inyectado.
expected<void> check_temporal(const JsonValue& payload, std::int64_t now,
                              const JwtOptions& options) {
    const JsonValue* exp = payload.find("exp");
    if (options.require_exp && (exp == nullptr || !exp->is_number())) {
        return reject("falta el claim 'exp'");
    }
    if (exp != nullptr && exp->is_number() && now > exp->as_int64() + options.leeway_seconds) {
        return reject("token expirado");
    }
    const JsonValue* nbf = payload.find("nbf");
    if (nbf != nullptr && nbf->is_number() && now + options.leeway_seconds < nbf->as_int64()) {
        return reject("token aún no válido (nbf)");
    }
    const JsonValue* iat = payload.find("iat");
    if (iat != nullptr && iat->is_number() && iat->as_int64() > now + options.leeway_seconds) {
        return reject("token emitido en el futuro (iat)");
    }
    return {};
}

/// Valida `iss` y `aud` contra la política configurada.
expected<void> check_issuer_audience(const JsonValue& payload, const JwtOptions& options) {
    if (!options.issuer.empty()) {
        const JsonValue* iss = payload.find("iss");
        if (iss == nullptr || !iss->is_string() || iss->as_string() != options.issuer) {
            return reject("emisor (iss) no autorizado");
        }
    }
    if (options.audience.empty()) {
        return {};
    }
    const JsonValue* aud = payload.find("aud");
    if (aud == nullptr) {
        return reject("falta la audiencia (aud)");
    }
    if (aud->is_string()) {
        return aud->as_string() == options.audience
                   ? expected<void>{}
                   : expected<void>{reject("audiencia (aud) no autorizada")};
    }
    if (aud->is_array()) {
        for (const JsonValue& item : aud->as_array()) {
            if (item.is_string() && item.as_string() == options.audience) {
                return {};
            }
        }
    }
    return reject("audiencia (aud) no autorizada");
}

/// Construye el `Principal` a partir de los *claims* `sub` y `roles`.
Principal build_principal(const JsonValue& payload) {
    Principal principal;
    const JsonValue* sub = payload.find("sub");
    if (sub != nullptr && sub->is_string()) {
        principal.subject = sub->as_string();
    }
    const JsonValue* roles = payload.find("roles");
    if (roles != nullptr && roles->is_array()) {
        for (const JsonValue& role : roles->as_array()) {
            if (role.is_string()) {
                principal.roles.push_back(role.as_string());
            }
        }
    }
    return principal;
}

}  // namespace

JwtVerifier::JwtVerifier(std::string secret, JwtOptions options)
    : secret_(std::move(secret)), options_(std::move(options)) {}

expected<Principal> JwtVerifier::verify(std::string_view token,
                                        std::int64_t now_unix_seconds) const {
    // 1) Trocear en exactamente tres segmentos: header.payload.signature.
    const std::size_t first_dot = token.find('.');
    if (first_dot == std::string_view::npos) {
        return reject("formato inválido (faltan '.')");
    }
    const std::size_t second_dot = token.find('.', first_dot + 1);
    if (second_dot == std::string_view::npos) {
        return reject("formato inválido (faltan '.')");
    }
    if (token.find('.', second_dot + 1) != std::string_view::npos) {
        return reject("formato inválido (sobran '.')");
    }
    const std::string_view header_b64 = token.substr(0, first_dot);
    const std::string_view payload_b64 = token.substr(first_dot + 1, second_dot - first_dot - 1);
    const std::string_view signature_b64 = token.substr(second_dot + 1);

    // 2) Cabecera: el algoritmo debe ser HS256 (rechaza 'none' y asimétricos).
    const auto header = decode_json_segment(header_b64, "cabecera");
    if (!header) {
        return std::unexpected{header.error()};
    }
    const JsonValue* alg = header->find("alg");
    if (alg == nullptr || !alg->is_string() || alg->as_string() != "HS256") {
        return reject("algoritmo no soportado (se exige HS256)");
    }

    // 3) Firma: HMAC-SHA256 sobre "header.payload"; comparación en tiempo constante.
    std::string signing_input;
    signing_input.reserve(header_b64.size() + 1 + payload_b64.size());
    signing_input.append(header_b64);
    signing_input.push_back('.');
    signing_input.append(payload_b64);
    const Sha256Digest mac = hmac_sha256(as_bytes(secret_), as_bytes(signing_input));
    const auto provided = base64url_decode(signature_b64);
    if (!provided) {
        return reject("firma no es base64url");
    }
    if (!constant_time_equal(mac, *provided)) {
        return reject("firma inválida");
    }

    // 4) Payload: validar claims temporales, emisor/audiencia y construir el Principal.
    const auto payload = decode_json_segment(payload_b64, "payload");
    if (!payload) {
        return std::unexpected{payload.error()};
    }
    if (auto temporal = check_temporal(*payload, now_unix_seconds, options_); !temporal) {
        return std::unexpected{temporal.error()};
    }
    if (auto authority = check_issuer_audience(*payload, options_); !authority) {
        return std::unexpected{authority.error()};
    }
    return build_principal(*payload);
}

}  // namespace nexus
