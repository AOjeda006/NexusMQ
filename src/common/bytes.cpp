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

void Buffer::clear() noexcept {
    data_.clear();
}

ByteSpan Buffer::as_span() const noexcept {
    return data_;
}

}  // namespace nexus
