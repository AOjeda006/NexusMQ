/// @file   ingress/json_value.hpp
/// @brief  JsonValue + parse_json: lector JSON (acompaña al JsonWriter) para cuerpos del REST y
/// JWT.
/// @ingroup ingress

#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#include "common/error.hpp"

namespace nexus {

/// @brief Valor JSON inmutable (árbol). Afinidad: REACTOR-LOCAL (valor; copiable/movible).
/// @details Modelo de lectura que complementa al `JsonWriter` (escritura). Los números se guardan
///   como `double` (suficiente para enteros JWT hasta 2^53); los objetos conservan el **orden** de
///   inserción (vector de pares). Lo usan la verificación JWT (I10c) y los cuerpos JSON del REST.
/// @invariant El tipo activo del `variant` coincide con `type()`.
class JsonValue {
public:
    /// Etiqueta del tipo JSON contenido.
    enum class Type : std::uint8_t { Null, Bool, Number, String, Array, Object };

    using Array = std::vector<JsonValue>;
    using Member = std::pair<std::string, JsonValue>;
    using Object = std::vector<Member>;

    JsonValue() noexcept : value_(nullptr) {}  // null por defecto.
    explicit JsonValue(bool flag) noexcept : value_(flag) {}
    explicit JsonValue(double number) noexcept : value_(number) {}
    explicit JsonValue(std::string text) noexcept : value_(std::move(text)) {}
    explicit JsonValue(Array array) noexcept : value_(std::move(array)) {}
    explicit JsonValue(Object object) noexcept : value_(std::move(object)) {}

    [[nodiscard]] Type type() const noexcept { return static_cast<Type>(value_.index()); }
    [[nodiscard]] bool is_null() const noexcept { return type() == Type::Null; }
    [[nodiscard]] bool is_bool() const noexcept { return type() == Type::Bool; }
    [[nodiscard]] bool is_number() const noexcept { return type() == Type::Number; }
    [[nodiscard]] bool is_string() const noexcept { return type() == Type::String; }
    [[nodiscard]] bool is_array() const noexcept { return type() == Type::Array; }
    [[nodiscard]] bool is_object() const noexcept { return type() == Type::Object; }

    /// @pre `is_bool()`.
    [[nodiscard]] bool as_bool() const { return std::get<bool>(value_); }
    /// @pre `is_number()`.
    [[nodiscard]] double as_number() const { return std::get<double>(value_); }
    /// @pre `is_number()`. Trunca hacia cero (los timestamps JWT son enteros exactos en `double`).
    [[nodiscard]] std::int64_t as_int64() const { return static_cast<std::int64_t>(as_number()); }
    /// @pre `is_string()`.
    [[nodiscard]] const std::string& as_string() const { return std::get<std::string>(value_); }
    /// @pre `is_array()`.
    [[nodiscard]] const Array& as_array() const { return std::get<Array>(value_); }
    /// @pre `is_object()`.
    [[nodiscard]] const Object& as_object() const { return std::get<Object>(value_); }

    /// @brief Busca un miembro por clave en un objeto. @return puntero al valor o `nullptr`.
    /// @details Si no es objeto, devuelve `nullptr`. Ante claves duplicadas, gana la primera.
    [[nodiscard]] const JsonValue* find(std::string_view key) const;

private:
    std::variant<std::nullptr_t, bool, double, std::string, Array, Object> value_;
};

/// Profundidad máxima de anidamiento aceptada por el parser (defensa anti-DoS / *stack overflow*).
inline constexpr int kJsonMaxDepth = 64;

/// @brief Parsea @p text como un documento JSON completo (RFC 8259).
/// @details Defensivo (`expected`): valida la gramática, rechaza basura tras el valor raíz y limita
///   el anidamiento a `kJsonMaxDepth`. Soporta escapes (incluido `\uXXXX` con pares subrogados).
///   @return el árbol `JsonValue` o `InvalidArgument` con la posición del fallo.
[[nodiscard]] expected<JsonValue> parse_json(std::string_view text);

}  // namespace nexus
