/// @file   bytes.hpp
/// @brief  Vistas y búfer propietario de bytes (vocabulario de E/S binaria).
/// @ingroup common

#pragma once

#include <cstddef>
#include <span>
#include <vector>

namespace nexus {

/// Vista de solo lectura sobre bytes contiguos (zero-copy). Afinidad: INMUTABLE.
using ByteSpan = std::span<const std::byte>;

/// Vista mutable sobre bytes contiguos (zero-copy).
using MutByteSpan = std::span<std::byte>;

/// @brief Búfer de bytes propietario y creciente (RAII).
/// @details Afinidad: REACTOR-LOCAL (no es thread-safe). Respaldado por un
///   `std::vector<std::byte>`: semántica de valor (copiable y movible) y
///   crecimiento geométrico amortizado. Sirve para construir tramas/batches sin
///   reservar en cada `append`.
/// @invariant size() <= capacity().
class Buffer {
public:
    Buffer() = default;

    /// Construye un búfer con capacidad inicial reservada.
    explicit Buffer(std::size_t capacity);

    /// Asegura capacidad para al menos @p capacity bytes (nunca reduce).
    void reserve(std::size_t capacity);

    /// Añade una copia de @p data al final; crece si hace falta.
    void append(ByteSpan data);

    /// @brief Hace crecer el búfer en @p n bytes y devuelve una vista **mutable** sobre esa cola,
    ///   para escribir E/S de entrada directamente (p. ej. `recv` sin copia intermedia).
    /// @details `size()` crece en @p n; los bytes nuevos quedan a cero. Si la E/S llena menos de
    ///   @p n, ajusta con `truncate`. La vista es válida hasta el próximo `extend`/`append`/`clear`
    ///   (pueden realocar). @return vista de los @p n bytes recién añadidos.
    [[nodiscard]] MutByteSpan extend(std::size_t n);

    /// Recorta el tamaño a @p new_size (debe ser ≤ `size()`); conserva la capacidad.
    void truncate(std::size_t new_size) noexcept;

    /// Vacía el contenido (size() = 0) conservando la capacidad reservada.
    void clear() noexcept;

    /// Vista de solo lectura de los bytes válidos [0, size()).
    [[nodiscard]] ByteSpan as_span() const noexcept;

    [[nodiscard]] std::size_t size() const noexcept { return data_.size(); }
    [[nodiscard]] std::size_t capacity() const noexcept { return data_.capacity(); }
    [[nodiscard]] bool empty() const noexcept { return data_.empty(); }

private:
    std::vector<std::byte> data_;
};

}  // namespace nexus
