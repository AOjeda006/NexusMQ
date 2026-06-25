// Pruebas del UpstreamPool (ADR-0027): resolución de dirección y acotación de la free-list (sin
// red) y dialado/reúso de conexiones por loopback con io_uring real.
#include "ingress/upstream_pool.hpp"

#include <gtest/gtest.h>

#include <unordered_map>
#include <utility>

#include "cluster/peer_directory.hpp"
#include "common/error.hpp"
#include "common/task.hpp"
#include "common/types.hpp"
#include "io/socket.hpp"
#include "support/fake_proactor.hpp"

namespace {

TEST(UpstreamPool, Idle_PoolNuevo_DevuelveCero) {
    const nexus::PeerDirectory dir;
    const nexus::UpstreamPool pool{dir};
    EXPECT_EQ(pool.idle(1), 0U);
}

TEST(UpstreamPool, Acquire_NodoDesconocido_DevuelveNotFound) {
    const nexus::PeerDirectory empty_dir;  // sin nodos registrados
    nexus::UpstreamPool pool{empty_dir};
    nexus::FakeProactor proactor;

    const nexus::expected<nexus::Socket> result = nexus::sync_wait(pool.acquire(proactor, 7));

    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code(), nexus::ErrorCode::NotFound);
}

TEST(UpstreamPool, Release_SuperaElTope_CierraElExcedente) {
    const nexus::PeerDirectory dir;
    nexus::UpstreamPool pool{dir, /*max_idle_per_node=*/1};

    pool.release(1, nexus::Socket{});
    pool.release(1, nexus::Socket{});  // excede el tope: se cierra en vez de guardarse

    EXPECT_EQ(pool.idle(1), 1U);
}

}  // namespace

#ifdef NEXUS_HAVE_IOURING

#include <chrono>
#include <functional>
#include <optional>
#include <system_error>
#include <vector>

#include "io/io_uring_backend.hpp"

namespace {

template <class T>
nexus::task<void> collect(nexus::task<T> work, std::optional<T>& out) {
    out = co_await std::move(work);
}

// Reanuda los conductores indicados y bombea el reactor hasta que @p done sea cierto (o venza).
bool pump_until(nexus::IoUringBackend& proactor, const std::vector<nexus::task<void>*>& resume_now,
                const std::function<bool()>& done) {
    for (auto* driver : resume_now) {
        driver->handle().resume();
    }
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (std::chrono::steady_clock::now() < deadline) {
        if (done()) {
            return true;
        }
        proactor.run_completions(16);
    }
    return false;
}

bool iouring_available() {
    try {
        (void)nexus::IoUringBackend{8};
        return true;
    } catch (const std::system_error&) {
        return false;
    }
}

TEST(UpstreamPoolE2E, Acquire_DialaConexionNuevaYLaReusaTrasRelease) {
    if (!iouring_available()) {
        GTEST_SKIP() << "io_uring no disponible en este entorno";
    }
    auto listener = nexus::Listener::bind("127.0.0.1", 0);
    ASSERT_TRUE(listener.has_value());

    std::unordered_map<nexus::NodeId, nexus::PeerAddress> addresses;
    addresses.emplace(nexus::NodeId{1},
                      nexus::PeerAddress{.host = "127.0.0.1", .port = listener->local_port()});
    const nexus::PeerDirectory dir{std::move(addresses)};

    nexus::IoUringBackend proactor{64};
    nexus::UpstreamPool pool{dir};

    // 1) Sin ociosas: acquire diala una conexión nueva; el listener la acepta.
    std::optional<nexus::expected<nexus::Socket>> dialed;
    std::optional<nexus::expected<nexus::Socket>> server_side;
    auto dial = collect(pool.acquire(proactor, 1), dialed);
    auto accept = collect(listener->async_accept(proactor), server_side);
    ASSERT_TRUE(
        pump_until(proactor, {&dial, &accept}, [&] { return dial.done() && accept.done(); }));
    ASSERT_TRUE(dialed.has_value() && dialed->has_value()) << dialed->error().message();
    ASSERT_TRUE(server_side.has_value() && server_side->has_value());
    const nexus::NativeHandle dialed_fd = dialed->value().fd();
    EXPECT_EQ(pool.idle(1), 0U);  // en préstamo exclusivo, no ociosa

    // 2) release devuelve la conexión a la free-list del nodo.
    pool.release(1, std::move(dialed->value()));
    EXPECT_EQ(pool.idle(1), 1U);

    // 3) acquire reúsa la MISMA conexión (mismo fd) sin dialar de nuevo (resuelve síncrono).
    const nexus::expected<nexus::Socket> reused = nexus::sync_wait(pool.acquire(proactor, 1));
    ASSERT_TRUE(reused.has_value());
    EXPECT_EQ(reused->fd(), dialed_fd);
    EXPECT_EQ(pool.idle(1), 0U);
}

}  // namespace

#endif  // NEXUS_HAVE_IOURING
