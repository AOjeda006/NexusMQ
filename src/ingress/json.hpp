/// @file   ingress/json.hpp
/// @brief  JsonWriter: constructor incremental de JSON para los DTOs del REST admin (§7.6).
/// @ingroup ingress

#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace nexus {

/// @brief Escribe JSON de forma incremental con comas y escapado correctos. Afinidad:
/// REACTOR-LOCAL.
/// @details Construye objetos/arrays anidados sin asignar un árbol intermedio: el llamante emite la
///   estructura con `begin_object`/`key`/`value`/`end_object` (y equivalentes de array) y la clase
///   gestiona las **comas** entre elementos y el **escapado** de cadenas. Los DTOs del REST admin
///   no exponen las estructuras internas (§7.6); este escritor las serializa al contrato JSON.
/// @invariant Cada `begin_*` se cierra con su `end_*`; `key` va seguido de un `value`/`begin_*`.
class JsonWriter {
public:
    JsonWriter& begin_object();
    JsonWriter& end_object();
    JsonWriter& begin_array();
    JsonWriter& end_array();

    /// Escribe una clave de objeto (debe ir seguida de un valor o de un `begin_*`).
    JsonWriter& key(std::string_view name);

    JsonWriter& value(std::string_view text);
    JsonWriter& value(const char* text) { return value(std::string_view{text}); }
    JsonWriter& value(std::int64_t number);
    JsonWriter& value(int number) { return value(static_cast<std::int64_t>(number)); }
    JsonWriter& value(double number);
    JsonWriter& value(bool flag);
    JsonWriter& null_value();

    /// @name Atajos `clave: valor` dentro de un objeto.
    /// @{
    template <class T>
    JsonWriter& field(std::string_view name, T&& v) {
        key(name);
        return value(std::forward<T>(v));
    }
    /// @}

    /// Devuelve el JSON construido.
    [[nodiscard]] const std::string& str() const& noexcept { return out_; }
    /// Extrae el JSON construido (vacía el escritor).
    [[nodiscard]] std::string take() { return std::move(out_); }

private:
    void before_value();
    void before_key();

    std::string out_;
    std::vector<bool> first_;       ///< por contenedor abierto: ¿el próximo elemento es el 1.º?
    bool expecting_value_ = false;  ///< true tras `key` (el valor no añade coma propia).
};

}  // namespace nexus
