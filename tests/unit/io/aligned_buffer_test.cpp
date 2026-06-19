// Pruebas de AlignedBuffer: asignación alineada confinada en RAII (para E/S directa, F6).
#include "io/aligned_buffer.hpp"

#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <utility>

#include "common/error.hpp"

namespace {

using nexus::AlignedBuffer;

// Dirección de @p ptr como entero, para comprobar alineación.
std::uintptr_t addr(const std::byte* ptr) {
    return reinterpret_cast<std::uintptr_t>(ptr);  // NOLINT(*-reinterpret-cast)
}

TEST(AlignedBuffer, Allocate_DevuelvePunteroAlineadoYTamano) {
    const auto buf = AlignedBuffer::allocate(8192, 4096);
    ASSERT_TRUE(buf.has_value());
    EXPECT_EQ(buf->size(), 8192U);
    EXPECT_EQ(buf->alignment(), 4096U);
    EXPECT_FALSE(buf->empty());
    EXPECT_EQ(addr(buf->data()) % 4096U, 0U) << "la dirección debe estar alineada";
}

TEST(AlignedBuffer, Allocate_TamanoCero_EsBufferVacio) {
    const auto buf = AlignedBuffer::allocate(0, 4096);
    ASSERT_TRUE(buf.has_value());
    EXPECT_TRUE(buf->empty());
    EXPECT_EQ(buf->size(), 0U);
    EXPECT_EQ(buf->data(), nullptr);
}

TEST(AlignedBuffer, Allocate_AlineacionNoPotenciaDeDos_EsInvalidArgument) {
    const auto buf = AlignedBuffer::allocate(1024, 1000);
    ASSERT_FALSE(buf.has_value());
    EXPECT_EQ(buf.error().code(), nexus::ErrorCode::InvalidArgument);
}

TEST(AlignedBuffer, Move_DejaElOrigenVacio) {
    auto buf = AlignedBuffer::allocate(4096, 512);
    ASSERT_TRUE(buf.has_value());
    std::byte* const original = buf->data();

    AlignedBuffer moved = std::move(*buf);
    EXPECT_EQ(moved.data(), original);
    EXPECT_EQ(moved.size(), 4096U);
    EXPECT_TRUE(buf->empty());  // NOLINT(bugprone-use-after-move): se comprueba el estado movido.
    EXPECT_EQ(buf->data(), nullptr);
}

TEST(AlignedBuffer, Span_PermiteEscrituraYLectura) {
    auto buf = AlignedBuffer::allocate(512, 512);
    ASSERT_TRUE(buf.has_value());
    const nexus::MutByteSpan span = buf->span();
    ASSERT_EQ(span.size(), 512U);
    span[0] = std::byte{0xAB};
    span[511] = std::byte{0xCD};
    EXPECT_EQ(buf->span()[0], std::byte{0xAB});
    EXPECT_EQ(buf->span()[511], std::byte{0xCD});
}

TEST(AlignedBuffer, AlignUp_RedondeaAlMultiplo) {
    EXPECT_EQ(nexus::align_up(0, 4096), 0U);
    EXPECT_EQ(nexus::align_up(1, 4096), 4096U);
    EXPECT_EQ(nexus::align_up(4096, 4096), 4096U);
    EXPECT_EQ(nexus::align_up(4097, 4096), 8192U);
    EXPECT_EQ(nexus::align_up(100, 512), 512U);
}

}  // namespace
