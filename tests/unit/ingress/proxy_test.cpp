// Pruebas del modo proxy: enrutado consistent-hash (sin red) y relevo petición/respuesta por
// loopback con io_uring real (un "líder" que hace eco de la trama).
#include "ingress/proxy.hpp"

#include <gtest/gtest.h>

#include <optional>
#include <string_view>

#include "common/types.hpp"
#include "ingress/load_balancer.hpp"

namespace {

TEST(Proxy, Route_AnilloVacio_DevuelveNullopt) {
    nexus::LoadBalancer balancer{nexus::BalanceStrategy::ConsistentHashing};
    nexus::Proxy proxy{balancer};
    EXPECT_FALSE(proxy.route("cualquier-clave").has_value());
}

TEST(Proxy, Route_MismaClave_MismoNodo) {
    nexus::LoadBalancer balancer{nexus::BalanceStrategy::ConsistentHashing};
    balancer.add_node(1);
    balancer.add_node(2);
    balancer.add_node(3);
    nexus::Proxy proxy{balancer};

    const std::optional<nexus::NodeId> first = proxy.route("orders/0");
    ASSERT_TRUE(first.has_value());
    // Determinista: la misma clave cae siempre en el mismo nodo.
    EXPECT_EQ(proxy.route("orders/0"), first);
    // El nodo elegido es uno de los del anillo.
    EXPECT_GE(*first, 1);
    EXPECT_LE(*first, 3);
}

}  // namespace

#ifdef NEXUS_HAVE_IOURING

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <string>
#include <system_error>
#include <vector>

#include "common/bytes.hpp"
#include "common/error.hpp"
#include "common/task.hpp"
#include "io/io_uring_backend.hpp"
#include "io/socket.hpp"
#include "protocol/frame.hpp"
#include "wire/frame_io.hpp"

namespace {

constexpr std::size_t kMaxFrame = 1UL << 20;

template <class T>
nexus::task<void> collect(nexus::task<T> work, std::optional<T>& out) {
    out = co_await std::move(work);
}

// Reanuda los conductores indicados y bombea el reactor hasta que @p done sea cierto (o venza).
bool pump_until(nexus::IoUringBackend& proactor, std::vector<nexus::task<void>*> resume_now,
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

std::optional<nexus::Socket> accept_one(nexus::IoUringBackend& proactor,
                                        const nexus::Listener& listener) {
    std::optional<nexus::expected<nexus::Socket>> accepted;
    auto driver = collect(listener.async_accept(proactor), accepted);
    if (!pump_until(proactor, {&driver}, [&] { return driver.done(); }) || !accepted.has_value() ||
        !accepted->has_value()) {
        return std::nullopt;
    }
    return std::move(accepted->value());
}

struct SocketPair {
    nexus::Socket accepted;
    nexus::Socket connected;
};

std::optional<SocketPair> loopback_pair(nexus::IoUringBackend& proactor,
                                        const nexus::Listener& listener) {
    auto connected = nexus::Socket::connect("127.0.0.1", listener.local_port());
    if (!connected) {
        return std::nullopt;
    }
    auto accepted = accept_one(proactor, listener);
    if (!accepted) {
        return std::nullopt;
    }
    return SocketPair{.accepted = std::move(*accepted), .connected = std::move(*connected)};
}

// "Líder": lee una trama de @p sock y la reescribe tal cual (eco).
nexus::task<nexus::expected<void>> echo_one_frame(nexus::Proactor& proactor, nexus::Socket& sock) {
    nexus::FrameReader reader{sock};
    nexus::FrameWriter writer{sock};
    const nexus::expected<nexus::Frame> frame = co_await reader.read_frame(proactor, kMaxFrame);
    if (!frame) {
        co_return std::unexpected(frame.error());
    }
    co_return co_await writer.write_frame(proactor, frame->header, frame->payload);
}

// Respuesta del cliente con el payload copiado (el del Frame vive en el búfer del lector).
struct Reply {
    nexus::FrameHeader header;
    std::string payload;
};

nexus::task<nexus::expected<Reply>> client_roundtrip(nexus::Proactor& proactor, nexus::Socket& sock,
                                                     nexus::FrameHeader header,
                                                     nexus::ByteSpan payload) {
    nexus::FrameWriter writer{sock};
    nexus::FrameReader reader{sock};
    if (const nexus::expected<void> sent = co_await writer.write_frame(proactor, header, payload);
        !sent) {
        co_return std::unexpected(sent.error());
    }
    const nexus::expected<nexus::Frame> response = co_await reader.read_frame(proactor, kMaxFrame);
    if (!response) {
        co_return std::unexpected(response.error());
    }
    co_return Reply{.header = response->header,
                    .payload = std::string{reinterpret_cast<const char*>(response->payload.data()),
                                           response->payload.size()}};
}

bool iouring_available() {
    try {
        (void)nexus::IoUringBackend{8};
        return true;
    } catch (const std::system_error&) {
        return false;
    }
}

TEST(ProxyE2E, RelevaPeticionRespuestaAlLider) {
    if (!iouring_available()) {
        GTEST_SKIP() << "io_uring no disponible en este entorno";
    }
    auto listener_client = nexus::Listener::bind("127.0.0.1", 0);
    auto listener_leader = nexus::Listener::bind("127.0.0.1", 0);
    ASSERT_TRUE(listener_client.has_value());
    ASSERT_TRUE(listener_leader.has_value());

    nexus::IoUringBackend proactor{64};
    auto client_side = loopback_pair(proactor, *listener_client);  // real_client ↔ proxy(cliente)
    auto leader_side = loopback_pair(proactor, *listener_leader);  // proxy(upstream) ↔ líder(eco)
    ASSERT_TRUE(client_side.has_value());
    ASSERT_TRUE(leader_side.has_value());

    nexus::Socket& real_client = client_side->connected;
    nexus::Socket& proxy_client = client_side->accepted;
    nexus::Socket& proxy_upstream = leader_side->connected;
    nexus::Socket& leader = leader_side->accepted;

    nexus::LoadBalancer balancer{nexus::BalanceStrategy::ConsistentHashing};
    nexus::Proxy proxy{balancer};

    const std::string payload = "carga-util-de-prueba";
    nexus::FrameHeader header;
    header.api_key = nexus::ApiKey::Produce;
    header.api_version = 1;
    header.correlation_id = 99;

    std::optional<nexus::expected<void>> echo_result;
    std::optional<nexus::expected<void>> forward_result;
    std::optional<nexus::expected<Reply>> reply;

    auto echo = collect(echo_one_frame(proactor, leader), echo_result);
    auto fwd = collect(proxy.forward(proactor, proxy_client, proxy_upstream), forward_result);
    auto cli = collect(
        client_roundtrip(
            proactor, real_client, header,
            nexus::ByteSpan{reinterpret_cast<const std::byte*>(payload.data()), payload.size()}),
        reply);

    // El relevo y el eco quedan vivos; bombeamos hasta que el cliente reciba su respuesta.
    ASSERT_TRUE(pump_until(proactor, {&echo, &fwd, &cli}, [&] { return cli.done(); }));
    ASSERT_TRUE(reply.has_value() && reply->has_value()) << reply->error().message();
    EXPECT_EQ(reply->value().header.correlation_id, 99U);
    EXPECT_EQ(reply->value().payload, payload);

    // Al cerrar el cliente, el relevo ve EOF y termina limpiamente.
    real_client.close();
    ASSERT_TRUE(pump_until(proactor, {}, [&] { return fwd.done(); }));
    ASSERT_TRUE(forward_result.has_value() && forward_result->has_value());
}

}  // namespace

#endif  // NEXUS_HAVE_IOURING
