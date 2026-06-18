/// @file   ingress/jwt.hpp
/// @brief  JwtVerifier: verificación de JWT HS256 para la autenticación del REST admin (§7.6).
/// @ingroup ingress

#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "common/error.hpp"

namespace nexus {

/// @brief Identidad autenticada extraída de un JWT válido. Afinidad: REACTOR-LOCAL (valor).
/// @details Lo produce `JwtVerifier::verify` a partir de los *claims* del token. `subject` es el
///   *claim* `sub`; `roles` viene del *claim* `roles` (array de cadenas), si está presente.
struct Principal {
    std::string subject;             ///< `sub`: identidad del portador.
    std::vector<std::string> roles;  ///< `roles`: autorizaciones (vacío si no hay *claim*).
};

/// @brief Política de validación del `JwtVerifier`. Afinidad: INMUTABLE (configuración).
struct JwtOptions {
    /// Si no vacío, exige `iss` == issuer.
    std::string issuer;
    /// Si no vacío, exige que `aud` lo incluya (cadena o array).
    std::string audience;
    /// Tolerancia de desfase de reloj para `exp`/`nbf`/`iat`.
    std::int64_t leeway_seconds = 0;
    /// Si true, rechaza tokens sin `exp` (no caducan nunca).
    bool require_exp = true;
};

/// @brief Verifica JWT firmados con **HS256** (HMAC-SHA256), sin dependencias externas.
/// @details Afinidad: THREAD-SAFE (inmutable tras construir; `verify` no muta estado). Comprueba la
///   firma en tiempo constante, rechaza algoritmos distintos de HS256 (incluido `none`) y valida
///   los *claims* temporales con el instante **inyectado** (`now_unix_seconds`), para pruebas
///   deterministas. No usa el reloj del sistema: el borde REST le pasa la hora.
/// @invariant El secreto y las opciones no cambian tras construir.
class JwtVerifier {
public:
    explicit JwtVerifier(std::string secret, JwtOptions options = {});

    /// @brief Verifica @p token contra el secreto y la política, con @p now_unix_seconds como
    /// «ahora».
    /// @param now_unix_seconds Instante actual en segundos Unix (UTC); el llamante lo inyecta.
    /// @return el `Principal` autenticado, o `InvalidArgument` con la causa (firma, caducidad, …).
    [[nodiscard]] expected<Principal> verify(std::string_view token,
                                             std::int64_t now_unix_seconds) const;

private:
    std::string secret_;
    JwtOptions options_;
};

}  // namespace nexus
