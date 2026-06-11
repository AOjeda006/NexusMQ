/// @file error.hpp
/// @brief Modelo de errores del núcleo: expected<T> = std::expected<T, Error>.
/// @ingroup common

#pragma once

#include <cstdint>
#include <expected>
#include <string>
#include <string_view>
#include <utility>

namespace nexus {

/// @brief Categoría de error del núcleo.
/// @details Espeja los códigos de wire y añade errores internos. Se traduce al modelo externo
/// (binario/REST) en el borde, no se propaga crudo.
enum class ErrorCode : std::uint8_t {
    Corrupt,          ///< Datos corruptos (p. ej. CRC32C no cuadra).
    IoError,          ///< Fallo de E/S.
    OutOfSpace,       ///< Sin espacio en disco.
    InvalidArgument,  ///< Entrada malformada (validación en el borde).
    NotFound,         ///< Recurso inexistente.
    OutOfRange,       ///< Offset o índice fuera de rango.
    Unsupported,      ///< Operación o versión no soportada.
    Shutdown,         ///< El componente se está apagando.
};

/// @brief Error del núcleo: un código + mensaje de contexto. Afinidad: INMUTABLE
/// @details Inmutable tras construir. 'with_context' devuelve una **copia** enriquecida (no muta),
/// de modo que el error acumula contexto al propagarse.
class Error {
public:
    Error(ErrorCode code, std::string message) : code_(code), message_(std::move(message)) {}

    [[nodiscard]] ErrorCode code() const noexcept { return code_; }
    [[nodiscard]] std::string_view message() const noexcept { return message_; }

    /// Devuelve una copia con @p context antepuesto ("context: mensaje").
    [[nodiscard]] Error with_context(std::string_view context) const {
        return Error{code_, std::string{context} + ": " + message_};
    }

private:
    ErrorCode code_;
    std::string message_;
};

/// Resultado de una operación del núcleo: un valor T o un Error.
/// Nombre en minúscula a propósito: refleja `std::expected` (NOLINT de naming).
template <class T>
using expected = std::expected<T, Error>;  // NOLINT(readability-identifier-naming)

/// Azúcar para devolver un error desde una función que devuelve 'expected<T>'
[[nodiscard]] inline std::unexpected<Error> make_error(ErrorCode code, std::string message) {
    return std::unexpected<Error>(Error{code, std::move(message)});
}

}  // namespace nexus
