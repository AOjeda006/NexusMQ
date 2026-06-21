// Pruebas de ReactorPool: arranque/apagado, mapeo partición→núcleo y entrega de trabajo cross-core
// a los reactores en sus hilos. Con FakeProactor (siempre) y, si hay io_uring, con el backend real
// (donde submit_to despierta a un reactor realmente bloqueado vía eventfd).
#include "reactor/reactor_pool.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <thread>

#include "support/fake_proactor.hpp"

#ifdef NEXUS_HAVE_IOURING
#include "io/io_uring_backend.hpp"
#endif

namespace {

// Factoría de proactores de test (un FakeProactor por reactor).
std::unique_ptr<nexus::Proactor> make_fake(int /*core_id*/) {
    return std::make_unique<nexus::FakeProactor>();
}

// Espera (con tope) a que @p value alcance @p target; devuelve si lo logró.
bool wait_until_eq(const std::atomic<int>& value, int target) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (std::chrono::steady_clock::now() < deadline) {
        if (value.load(std::memory_order_acquire) >= target) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
    return false;
}

TEST(ReactorPool, StartShutdown_CicloLimpio) {
    nexus::ReactorPool pool;
    pool.start(2, make_fake);
    EXPECT_EQ(pool.size(), 2);
    pool.shutdown();  // une los hilos; si stop/wake fallaran, colgaría (timeout de ctest)
    EXPECT_EQ(pool.size(), 0);
}

TEST(ReactorPool, ReactorFor_MapeaParticionANucleoPorModulo) {
    nexus::ReactorPool pool;
    pool.start(3, make_fake);
    EXPECT_EQ(&pool.reactor_for(0), &pool.reactor(0));
    EXPECT_EQ(&pool.reactor_for(1), &pool.reactor(1));
    EXPECT_EQ(&pool.reactor_for(4), &pool.reactor(1));  // 4 % 3 == 1
    EXPECT_EQ(pool.reactor(2).core_id(), 2);
    pool.shutdown();
}

TEST(ReactorPool, SubmitTo_EntregaTrabajoEnCadaReactor) {
    nexus::ReactorPool pool;
    pool.start(3, make_fake);

    std::atomic<int> counter{0};
    // Cada inyección la hace el hilo principal (único productor del buzón inbox[k] de su reactor).
    for (int core = 0; core < pool.size(); ++core) {
        pool.reactor(core).submit_to(
            core, [&counter] { counter.fetch_add(1, std::memory_order_relaxed); });
    }

    EXPECT_TRUE(wait_until_eq(counter, 3));
    pool.shutdown();
}

TEST(ReactorPool, StartMainInline_NoLanzaHiloDelNucleo0_ElLlamanteLoConduce) {
    nexus::ReactorPool pool;
    pool.start_main_inline(2, make_fake);
    EXPECT_EQ(pool.size(), 2);

    std::atomic<int> counter{0};
    // El worker (núcleo 1) corre en su hilo: el trabajo se ejecuta sin que nadie lo conduzca.
    pool.reactor(1).submit_to(1, [&counter] { counter.fetch_add(1, std::memory_order_relaxed); });
    EXPECT_TRUE(wait_until_eq(counter, 1));

    // El núcleo 0 NO tiene hilo: su trabajo solo avanza cuando el llamante lo conduce (poll_once).
    pool.reactor(0).submit_to(0, [&counter] { counter.fetch_add(1, std::memory_order_relaxed); });
    for (int i = 0; i < 100 && counter.load(std::memory_order_acquire) < 2; ++i) {
        pool.reactor(0).poll_once();
    }
    EXPECT_EQ(counter.load(std::memory_order_acquire), 2);

    pool.reactor(0).stop();
    pool.shutdown();
}

#ifdef NEXUS_HAVE_IOURING
TEST(ReactorPool, IoUring_SubmitTo_DespiertaAlReactorBloqueado) {
    // Comprueba disponibilidad real de io_uring; si no, se omite.
    try {
        (void)nexus::IoUringBackend{8};
    } catch (const std::system_error&) {
        GTEST_SKIP() << "io_uring no disponible en este entorno";
    }

    nexus::ReactorPool pool;
    pool.start(2, [](int) { return std::make_unique<nexus::IoUringBackend>(32); });

    // El reactor 0 está BLOQUEADO en wait_completions (io_uring_enter); submit_to escribe su
    // eventfd y lo despierta para que drene y ejecute el trabajo.
    std::atomic<int> counter{0};
    pool.reactor(0).submit_to(0, [&counter] { counter.fetch_add(1, std::memory_order_relaxed); });

    EXPECT_TRUE(wait_until_eq(counter, 1));
    pool.shutdown();
}
#endif  // NEXUS_HAVE_IOURING

}  // namespace
