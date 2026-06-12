#include "io/awaitable.hpp"

#include <gtest/gtest.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>

#include "common/error.hpp"
#include "reactor/task.hpp"
#include "support/fake_proactor.hpp"

namespace {

// Corrutina raíz que recoge el resultado de `work` en `out`. `work` se pasa por valor (vive en el
// frame) y `out` por referencia (lo declara el test, lo sobrevive): evita el cuelgue clásico de las
// lambdas-corrutina con capturas.
template <class T>
nexus::task<void> collect(nexus::task<T> work, std::optional<T>& out) {
    out = co_await std::move(work);
}

// Arranca `work`, deja que se suspenda en el awaitable (encola la op en el FakeProactor),
// la completa con `result` y devuelve el resultado producido por la corrutina.
template <class T>
T drive(nexus::FakeProactor& proactor, nexus::task<T> work, std::int32_t result) {
    std::optional<T> out;
    auto driver = collect(std::move(work), out);
    driver.handle().resume();           // corre hasta suspender en el awaitable
    EXPECT_EQ(proactor.pending(), 1U);  // la operación quedó encolada
    proactor.complete_front(result);    // dispara la completion → reanuda y completa
    EXPECT_TRUE(out.has_value());
    return std::move(*out);
}

nexus::task<nexus::expected<std::size_t>> do_read(nexus::FakeProactor& proactor,
                                                  nexus::MutByteSpan buffer) {
    co_return co_await nexus::async_read(proactor, 5, buffer, 100);
}

nexus::task<nexus::expected<void>> do_fsync(nexus::FakeProactor& proactor) {
    co_return co_await nexus::async_fsync(proactor, 9, true);
}

nexus::task<nexus::expected<int>> do_accept(nexus::FakeProactor& proactor) {
    co_return co_await nexus::async_accept(proactor, 3);
}

TEST(Awaitable, Read_ResultadoPositivo_DevuelveBytesLeidos) {
    nexus::FakeProactor proactor;
    std::array<std::byte, 16> buffer{};
    auto result = drive(proactor, do_read(proactor, buffer), 12);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result.value(), 12U);
}

TEST(Awaitable, Read_SuspendeAntesDeCompletar_Perezosa) {
    nexus::FakeProactor proactor;
    std::array<std::byte, 16> buffer{};
    std::optional<nexus::expected<std::size_t>> out;
    auto driver = collect(do_read(proactor, buffer), out);
    driver.handle().resume();
    EXPECT_FALSE(out.has_value());  // sigue suspendida esperando la completion
    EXPECT_EQ(proactor.pending(), 1U);
    proactor.complete_front(7);
    ASSERT_TRUE(out.has_value());
    EXPECT_EQ(out->value(), 7U);
}

TEST(Awaitable, Read_ResultadoNegativo_DevuelveIoError) {
    nexus::FakeProactor proactor;
    std::array<std::byte, 16> buffer{};
    auto result = drive(proactor, do_read(proactor, buffer), -5);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code(), nexus::ErrorCode::IoError);
}

TEST(Awaitable, Fsync_Exito_DevuelveVoid) {
    nexus::FakeProactor proactor;
    std::optional<nexus::expected<void>> out;
    auto driver = collect(do_fsync(proactor), out);
    driver.handle().resume();
    ASSERT_EQ(proactor.pending(), 1U);
    EXPECT_EQ(proactor.peek(0).kind, nexus::FakeProactor::OpKind::Fsync);
    EXPECT_TRUE(proactor.peek(0).datasync);  // se propagó datasync=true
    proactor.complete_front(0);
    ASSERT_TRUE(out.has_value());
    EXPECT_TRUE(out->has_value());
}

TEST(Awaitable, Accept_ResultadoPositivo_DevuelveDescriptor) {
    nexus::FakeProactor proactor;
    auto result = drive(proactor, do_accept(proactor), 42);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result.value(), 42);
}

}  // namespace
