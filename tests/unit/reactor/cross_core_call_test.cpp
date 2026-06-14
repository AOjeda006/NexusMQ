// Pruebas de call_on: petición/respuesta cross-core sobre dos reactores conducidos paso a paso con
// poll_once (deterministas, sin hilos). Se valida que el trabajo corre en el destino, que el
// resultado vuelve al origen y que no se resuelve sin drenar al destino.
#include "reactor/cross_core_call.hpp"

#include <gtest/gtest.h>

#include <memory>

#include "common/task.hpp"
#include "reactor/reactor.hpp"
#include "support/fake_proactor.hpp"

namespace {

// Pareja de reactores cableados (núcleos 0 y 1), cada uno con su FakeProactor.
struct ReactorPair {
    nexus::Reactor r0{0, 2, std::make_unique<nexus::FakeProactor>()};
    nexus::Reactor r1{1, 2, std::make_unique<nexus::FakeProactor>()};

    ReactorPair() {
        r0.connect_peers({&r0, &r1});
        r1.connect_peers({&r0, &r1});
    }

    // Avanza ambos reactores hasta que `done()` o se agoten los giros.
    template <class Pred>
    void pump_until(Pred done, int max_rounds = 16) {
        for (int i = 0; i < max_rounds && !done(); ++i) {
            r0.poll_once();
            r1.poll_once();
        }
    }
};

nexus::task<void> call_constant(nexus::Reactor& self, nexus::Reactor& target, int& out) {
    out = co_await nexus::call_on(self, target, [] { return 42; });
}

nexus::task<void> call_doubling(nexus::Reactor& self, nexus::Reactor& target, int base, int& out) {
    out = co_await nexus::call_on(self, target, [base] { return base * 2 + 1; });
}

TEST(CrossCoreCall, CallOn_EjecutaEnDestinoYReanudaConResultado) {
    ReactorPair pair;
    int out = -1;
    pair.r0.spawn(call_constant(pair.r0, pair.r1, out));
    pair.pump_until([&] { return out == 42; });
    EXPECT_EQ(out, 42);
}

TEST(CrossCoreCall, CallOn_TransportaElResultadoCalculado) {
    ReactorPair pair;
    int out = -1;
    pair.r0.spawn(call_doubling(pair.r0, pair.r1, 10, out));
    pair.pump_until([&] { return out == 21; });
    EXPECT_EQ(out, 21);
}

TEST(CrossCoreCall, CallOn_NoSeResuelveSinDrenarElDestino) {
    ReactorPair pair;
    int out = -1;
    pair.r0.spawn(call_constant(pair.r0, pair.r1, out));

    pair.r0.poll_once();  // arranca la corrutina: postea al destino y se suspende.
    EXPECT_EQ(out, -1);   // sin drenar r1 no hay resultado todavía.

    pair.pump_until([&] { return out == 42; });
    EXPECT_EQ(out, 42);
}

}  // namespace
