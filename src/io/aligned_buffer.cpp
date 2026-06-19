/// @file   io/aligned_buffer.cpp
/// @brief  Implementación de AlignedBuffer (asignación alineada confinada en RAII).
/// @ingroup io

#include "io/aligned_buffer.hpp"

#include <new>
#include <utility>

namespace nexus {
namespace {

/// ¿Es @p value una potencia de dos (y no cero)? Requisito de `operator new` alineado.
[[nodiscard]] bool is_power_of_two(std::size_t value) noexcept {
    return value != 0 && (value & (value - 1)) == 0;
}

}  // namespace

AlignedBuffer::~AlignedBuffer() {
    release();
}

AlignedBuffer::AlignedBuffer(AlignedBuffer&& other) noexcept
    : data_(std::exchange(other.data_, nullptr)),
      size_(std::exchange(other.size_, 0)),
      alignment_(std::exchange(other.alignment_, 0)) {}

AlignedBuffer& AlignedBuffer::operator=(AlignedBuffer&& other) noexcept {
    if (this != &other) {
        release();
        data_ = std::exchange(other.data_, nullptr);
        size_ = std::exchange(other.size_, 0);
        alignment_ = std::exchange(other.alignment_, 0);
    }
    return *this;
}

void AlignedBuffer::release() noexcept {
    if (data_ != nullptr) {
        ::operator delete(data_, std::align_val_t{alignment_});
        data_ = nullptr;
    }
    size_ = 0;
    alignment_ = 0;
}

expected<AlignedBuffer> AlignedBuffer::allocate(std::size_t size, std::size_t alignment) {
    if (!is_power_of_two(alignment)) {
        return make_error(ErrorCode::InvalidArgument, "alineación no es potencia de dos");
    }
    if (size == 0) {
        return AlignedBuffer{};
    }
    void* raw = ::operator new(size, std::align_val_t{alignment}, std::nothrow);
    if (raw == nullptr) {
        return make_error(ErrorCode::OutOfSpace, "asignación alineada fallida");
    }
    return AlignedBuffer{static_cast<std::byte*>(raw), size, alignment};
}

}  // namespace nexus
