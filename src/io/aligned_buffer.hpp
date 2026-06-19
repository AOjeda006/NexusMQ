/// @file   io/aligned_buffer.hpp
/// @brief  AlignedBuffer: búfer con alineación garantizada para E/S directa (O_DIRECT).
/// @ingroup io

#pragma once

#include <cstddef>

#include "common/bytes.hpp"
#include "common/error.hpp"

namespace nexus {

/// Alineación por defecto para E/S directa: el tamaño de página/bloque lógico habitual (4 KiB).
/// O_DIRECT exige que la dirección del búfer, el offset y la longitud sean múltiplos de la
/// alineación del dispositivo; 4096 cubre el caso común sin consultar `statvfs`.
inline constexpr std::size_t kDirectAlignment = 4096;

/// @brief Redondea @p value hacia arriba al múltiplo de @p alignment (potencia de dos).
[[nodiscard]] constexpr std::size_t align_up(std::size_t value, std::size_t alignment) noexcept {
    return (value + alignment - 1) & ~(alignment - 1);
}

/// @brief Búfer de bytes con dirección **alineada**, para E/S directa. Afinidad: REACTOR-LOCAL.
/// @details Confina la asignación cruda alineada (`operator new` con `align_val_t`) en un tipo RAII
///   **solo movible**, según la normativa (sin `new`/`delete` a la vista del resto del código). La
///   alineación debe ser potencia de dos; el tamaño se conserva tal cual se pide (para O_DIRECT el
///   llamante debe pedir un múltiplo de la alineación, p. ej. con `align_up`).
/// @invariant `data()` está alineado a `alignment()` mientras `size() > 0`.
class AlignedBuffer {
public:
    AlignedBuffer() = default;
    ~AlignedBuffer();
    AlignedBuffer(AlignedBuffer&& other) noexcept;
    AlignedBuffer& operator=(AlignedBuffer&& other) noexcept;
    AlignedBuffer(const AlignedBuffer&) = delete;
    AlignedBuffer& operator=(const AlignedBuffer&) = delete;

    /// @brief Reserva @p size bytes alineados a @p alignment (potencia de dos).
    /// @return El búfer, `InvalidArgument` si la alineación no es válida, o `OutOfSpace` si la
    ///   asignación falla.
    [[nodiscard]] static expected<AlignedBuffer> allocate(std::size_t size,
                                                          std::size_t alignment = kDirectAlignment);

    [[nodiscard]] MutByteSpan span() noexcept { return {data_, size_}; }
    [[nodiscard]] ByteSpan span() const noexcept { return {data_, size_}; }
    [[nodiscard]] std::byte* data() noexcept { return data_; }
    [[nodiscard]] const std::byte* data() const noexcept { return data_; }
    [[nodiscard]] std::size_t size() const noexcept { return size_; }
    [[nodiscard]] std::size_t alignment() const noexcept { return alignment_; }
    [[nodiscard]] bool empty() const noexcept { return size_ == 0; }

private:
    AlignedBuffer(std::byte* data, std::size_t size, std::size_t alignment) noexcept
        : data_(data), size_(size), alignment_(alignment) {}
    void release() noexcept;

    std::byte* data_ = nullptr;
    std::size_t size_ = 0;
    std::size_t alignment_ = 0;
};

}  // namespace nexus
