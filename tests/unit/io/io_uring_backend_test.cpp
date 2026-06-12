// Smoke-test del backend io_uring. Solo se compila donde existe el uapi del kernel
// (NEXUS_HAVE_IOURING) y se OMITE en tiempo de ejecución si io_uring no está disponible
// (kernel viejo, seccomp del entorno de CI…). Hace E/S real sobre un fichero temporal.
#include <gtest/gtest.h>

#ifdef NEXUS_HAVE_IOURING

#include <fcntl.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <memory>
#include <optional>
#include <string>
#include <system_error>
#include <thread>

#include "io/io_uring_backend.hpp"

namespace {

// Intenta crear el backend; si io_uring no está disponible, devuelve nullptr (el test se omite).
std::unique_ptr<nexus::IoUringBackend> make_backend() {
    try {
        return std::make_unique<nexus::IoUringBackend>(32);
    } catch (const std::system_error&) {
        return nullptr;
    }
}

// Sondea la CQ (run_completions es no bloqueante) hasta drenar al menos una completion, con un
// tope de tiempo: si io_uring estuviera roto a medias, el test falla de forma visible, no se
// cuelga.
bool drain_one(nexus::IoUringBackend& backend) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (std::chrono::steady_clock::now() < deadline) {
        if (backend.run_completions(8) > 0) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
    return false;
}

// Fichero temporal con cierre RAII.
class TempFd {
public:
    TempFd() {
        std::array<char, 32> name{"/tmp/nexus_iouring_XXXXXX"};
        fd_ = ::mkstemp(name.data());
        if (fd_ >= 0) {
            ::unlink(name.data());  // se borra al cerrar (sigue accesible por el fd)
        }
    }
    ~TempFd() {
        if (fd_ >= 0) {
            ::close(fd_);
        }
    }
    TempFd(const TempFd&) = delete;
    TempFd& operator=(const TempFd&) = delete;
    TempFd(TempFd&&) = delete;
    TempFd& operator=(TempFd&&) = delete;
    [[nodiscard]] int get() const noexcept { return fd_; }

private:
    int fd_ = -1;
};

TEST(IoUringBackend, WriteRead_RoundTrip_DevuelveLoEscrito) {
    auto backend = make_backend();
    if (!backend) {
        GTEST_SKIP() << "io_uring no disponible en este entorno";
    }
    TempFd file;
    ASSERT_GE(file.get(), 0);

    const std::string payload = "NexusMQ io_uring";
    nexus::ByteSpan to_write{reinterpret_cast<const std::byte*>(payload.data()), payload.size()};

    std::optional<std::int32_t> write_result;
    backend->submit_write(file.get(), to_write, 0, [&](std::int32_t res) { write_result = res; });
    ASSERT_TRUE(drain_one(*backend));
    ASSERT_TRUE(write_result.has_value());
    ASSERT_EQ(*write_result, static_cast<std::int32_t>(payload.size()));

    std::array<std::byte, 32> read_buf{};
    std::optional<std::int32_t> read_result;
    backend->submit_read(file.get(), read_buf, 0, [&](std::int32_t res) { read_result = res; });
    ASSERT_TRUE(drain_one(*backend));
    ASSERT_TRUE(read_result.has_value());
    ASSERT_EQ(*read_result, static_cast<std::int32_t>(payload.size()));
    EXPECT_EQ(0, std::memcmp(read_buf.data(), payload.data(), payload.size()));
}

TEST(IoUringBackend, Fsync_Exito_DevuelveCero) {
    auto backend = make_backend();
    if (!backend) {
        GTEST_SKIP() << "io_uring no disponible en este entorno";
    }
    TempFd file;
    ASSERT_GE(file.get(), 0);

    std::optional<std::int32_t> result;
    backend->submit_fsync(file.get(), /*datasync=*/true, [&](std::int32_t res) { result = res; });
    ASSERT_TRUE(drain_one(*backend));
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(*result, 0);
}

TEST(IoUringBackend, Read_FdInvalido_DevuelveErrorNegativo) {
    auto backend = make_backend();
    if (!backend) {
        GTEST_SKIP() << "io_uring no disponible en este entorno";
    }
    std::array<std::byte, 8> buf{};
    std::optional<std::int32_t> result;
    backend->submit_read(-1, buf, 0, [&](std::int32_t res) { result = res; });
    ASSERT_TRUE(drain_one(*backend));
    ASSERT_TRUE(result.has_value());
    EXPECT_LT(*result, 0);  // EBADF u otro errno negado
}

TEST(IoUringBackend, Timer_Vence_DevuelveCero) {
    auto backend = make_backend();
    if (!backend) {
        GTEST_SKIP() << "io_uring no disponible en este entorno";
    }
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(5);
    std::optional<std::int32_t> result;
    backend->submit_timer(deadline, [&](std::int32_t res) { result = res; });
    ASSERT_TRUE(drain_one(*backend));
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(*result, 0);  // -ETIME se traduce a éxito
}

}  // namespace

#endif  // NEXUS_HAVE_IOURING
