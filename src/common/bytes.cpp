#include "common/bytes.hpp"

namespace nexus {

Buffer::Buffer(std::size_t capacity) {
    data_.reserve(capacity);
}

void Buffer::reserve(std::size_t capacity) {
    data_.reserve(capacity);
}

void Buffer::append(ByteSpan data) {
    data_.insert(data_.end(), data.begin(), data.end());
}

MutByteSpan Buffer::extend(std::size_t n) {
    const std::size_t old_size = data_.size();
    data_.resize(old_size + n);
    return MutByteSpan{data_.data() + old_size, n};
}

// Precondición: new_size <= size(). resize hacia abajo no realoca ni lanza (bytes triviales).
// NOLINTNEXTLINE(bugprone-exception-escape): por eso es noexcept pese a llamar a resize.
void Buffer::truncate(std::size_t new_size) noexcept {
    data_.resize(new_size);
}

void Buffer::clear() noexcept {
    data_.clear();
}

ByteSpan Buffer::as_span() const noexcept {
    return data_;
}

}  // namespace nexus
