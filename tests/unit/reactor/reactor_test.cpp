// Pruebas del Reactor con el doble FakeProactor (deterministas, paso a paso con poll_once). Se
// valida: spawn ejecuta la corrutina, submit_to entrega el trabajo cross-core, una corrutina con
// E/S asíncrona se reanuda en la completion, y stop() termina el bucle run() desde otro hilo.
#include "reactor/reactor.hpp"

#include <gtest/gtest.h>

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <thread>

#include "common/task.hpp"
#include "io/awaitable.hpp"
#include "support/fake_proactor.hpp"

namespace {

// Corrutina mínima: incrementa un contador y termina.
nexus::task<void> increment(int& counter) {
    ++counter;
    co_return;
}

// Corrutina con E/S: lee de @p fd y publica los bytes leídos (o -1 en error).
nexus::task<void> read_into(nexus::Proactor& proactor, int fd, nexus::MutByteSpan buffer,
                            int& out) {
    const auto result = co_await nexus::async_read(proactor, fd, buffer, 0);
    out = result.has_value() ? static_cast<int>(*result) : -1;
}

TEST(Reactor, Spawn_CorrutinaSimple_SeEjecutaEnPollOnce) {
    nexus::Reactor reactor(/*core_id=*/0, /*num_cores=*/1, std::make_unique<nexus::FakeProactor>());
    int counter = 0;
    reactor.spawn(increment(counter));
    EXPECT_EQ(counter, 0);  // perezosa: aún no ha corrido

    reactor.poll_once();
    EXPECT_EQ(counter, 1);
}

TEST(Reactor, SubmitTo_MismoNucleo_EntregaYEjecutaElTrabajo) {
    nexus::Reactor reactor(/*core_id=*/0, /*num_cores=*/1, std::make_unique<nexus::FakeProactor>());
    reactor.connect_peers({&reactor});  // peer 0 = él mismo (un solo núcleo)

    int counter = 0;
    reactor.submit_to(0, [&counter] { ++counter; });
    EXPECT_EQ(counter, 0);  // encolado, todavía no entregado

    reactor.poll_once();
    EXPECT_EQ(counter, 1);
}

TEST(Reactor, Spawn_CorrutinaConIo_SeReanudaTrasLaCompletion) {
    auto proactor = std::make_unique<nexus::FakeProactor>();
    nexus::FakeProactor* fake = proactor.get();
    nexus::Reactor reactor(/*core_id=*/0, /*num_cores=*/1, std::move(proactor));

    std::array<std::byte, 8> buffer{};
    int bytes_read = -99;
    reactor.spawn(read_into(reactor.proactor(), /*fd=*/5, buffer, bytes_read));

    reactor.poll_once();  // arranca la corrutina → submit_read (queda pendiente)
    EXPECT_EQ(fake->pending(), 1U);
    EXPECT_EQ(bytes_read, -99);  // sigue suspendida esperando la E/S

    fake->arm_front(4);   // la lectura completará devolviendo 4 bytes
    reactor.poll_once();  // wait_completions ejecuta la completion → reanuda la corrutina
    EXPECT_EQ(bytes_read, 4);
}

TEST(Reactor, Stop_DesdeOtroHilo_TerminaElBucleRun) {
    nexus::Reactor reactor(/*core_id=*/0, /*num_cores=*/1, std::make_unique<nexus::FakeProactor>());
    std::thread runner([&reactor] { reactor.run(); });

    std::this_thread::sleep_for(std::chrono::milliseconds(10));  // deja entrar al bucle
    reactor.stop();
    runner.join();  // si stop() no funcionara, esto colgaría (lo cazaría el timeout de ctest)
    SUCCEED();
}

}  // namespace
