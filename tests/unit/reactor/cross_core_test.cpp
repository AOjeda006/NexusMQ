// Pruebas de CrossCoreMailbox: paso de mensajes entre reactores. Se valida la entrega FIFO por
// origen, la ejecución del trabajo, el wake del destino y, bajo ThreadSanitizer, varios
// productores concurrentes (uno por buzón) contra un único consumidor (contrato SPSC).
#include "reactor/cross_core.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <cstdint>
#include <numeric>
#include <thread>
#include <vector>

#include "support/fake_proactor.hpp"

namespace {

// Handler que solo ejecuta el trabajo del mensaje.
void run_work(nexus::Message& msg) {
    if (msg.work) {
        msg.work();
    }
}

TEST(CrossCoreMailbox, Post_UnMensaje_DrainLoEjecuta) {
    nexus::FakeProactor proactor;
    nexus::CrossCoreMailbox mailbox(/*num_cores=*/2, proactor);

    bool ran = false;
    mailbox.post(/*from_core=*/1, nexus::Message{.target_core = 0, .work = [&] { ran = true; }});

    EXPECT_EQ(mailbox.drain(run_work), 1);
    EXPECT_TRUE(ran);
}

TEST(CrossCoreMailbox, Drain_BuzonVacio_DevuelveCero) {
    nexus::FakeProactor proactor;
    nexus::CrossCoreMailbox mailbox(/*num_cores=*/4, proactor);
    EXPECT_EQ(mailbox.drain(run_work), 0);
}

TEST(CrossCoreMailbox, Post_VariosOrigenes_DrenaTodos) {
    nexus::FakeProactor proactor;
    nexus::CrossCoreMailbox mailbox(/*num_cores=*/3, proactor);

    int executed = 0;
    for (int core = 0; core < 3; ++core) {
        mailbox.post(core, nexus::Message{.target_core = 0, .work = [&] { ++executed; }});
    }

    EXPECT_EQ(mailbox.drain(run_work), 3);
    EXPECT_EQ(executed, 3);
}

TEST(CrossCoreMailbox, Post_MismoOrigen_DrenaEnOrdenFIFO) {
    nexus::FakeProactor proactor;
    nexus::CrossCoreMailbox mailbox(/*num_cores=*/2, proactor);

    std::vector<int> order;
    for (int i = 0; i < 5; ++i) {
        mailbox.post(/*from_core=*/1,
                     nexus::Message{.target_core = 0, .work = [&order, i] { order.push_back(i); }});
    }

    EXPECT_EQ(mailbox.drain(run_work), 5);
    EXPECT_EQ(order, (std::vector<int>{0, 1, 2, 3, 4}));
}

TEST(CrossCoreMailbox, Post_DespiertaAlDestino) {
    nexus::FakeProactor proactor;
    nexus::CrossCoreMailbox mailbox(/*num_cores=*/2, proactor);

    EXPECT_EQ(proactor.wakes(), 0);
    mailbox.post(/*from_core=*/0, nexus::Message{.target_core = 0, .work = [] {}});
    EXPECT_GE(proactor.wakes(), 1);  // post despierta al reactor destino
}

// Estrés concurrente (validado bajo ThreadSanitizer): N productores, cada uno en su propio buzón
// (productor único SPSC), contra un consumidor único que drena. Se comprueba conservación: cada
// mensaje incrementa exactamente una vez un acumulador del consumidor.
TEST(CrossCoreMailbox, Concurrente_ProductoresPorBuzon_ConsumidorUnico_NoPierdeMensajes) {
    constexpr int kProducers = 4;
    constexpr int kPerProducer = 500;
    constexpr int kTotal = kProducers * kPerProducer;

    nexus::FakeProactor proactor;
    nexus::CrossCoreMailbox mailbox(kProducers, proactor);

    std::atomic<bool> go{false};
    std::vector<std::jthread> producers;
    producers.reserve(kProducers);
    for (int core = 0; core < kProducers; ++core) {
        producers.emplace_back([&mailbox, &go, core] {
            while (!go.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            for (int i = 0; i < kPerProducer; ++i) {
                mailbox.post(core, nexus::Message{.target_core = 0, .work = [] {}});
            }
        });
    }

    go.store(true, std::memory_order_release);

    // Consumidor único: drena hasta recibir los kTotal mensajes (el handler corre solo aquí).
    long long received = 0;
    while (received < kTotal) {
        received += mailbox.drain([](nexus::Message& msg) {
            if (msg.work) {
                msg.work();
            }
        });
    }

    for (std::jthread& producer : producers) {
        producer.join();
    }
    EXPECT_EQ(received, kTotal);
}

}  // namespace
