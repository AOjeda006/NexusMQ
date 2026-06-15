// Pruebas de PartitionRouter: enrutado de operaciones de partición al núcleo dueño sobre dos
// reactores conducidos paso a paso (poll_once). Valida la regla de reparto (partición % núcleos),
// que el estado de cada partición vive SOLO en su dueño, y que el resultado vuelve al llamante.
#include "reactor/partition_router.hpp"

#include <gtest/gtest.h>

#include <array>
#include <cstddef>
#include <memory>
#include <unordered_map>
#include <vector>

#include "common/task.hpp"
#include "common/types.hpp"
#include "reactor/reactor.hpp"
#include "support/fake_proactor.hpp"

namespace {

// Estado de partición reactor-local: por núcleo, el próximo offset de cada partición que posee.
struct CoreState {
    std::unordered_map<nexus::PartitionId, int> next_offset;
};

// Pareja de reactores cableados (núcleos 0 y 1), cada uno con su FakeProactor.
struct ReactorPair {
    nexus::Reactor r0{0, 2, std::make_unique<nexus::FakeProactor>()};
    nexus::Reactor r1{1, 2, std::make_unique<nexus::FakeProactor>()};

    ReactorPair() {
        r0.connect_peers({&r0, &r1});
        r1.connect_peers({&r0, &r1});
    }

    template <class Pred>
    void pump_until(Pred done, int max_rounds = 128) {
        for (int i = 0; i < max_rounds && !done(); ++i) {
            r0.poll_once();
            r1.poll_once();
        }
    }
};

// "Produce" enrutado: asigna y devuelve el offset de @p p en el estado de su núcleo dueño.
// `parts` se pasa por valor a propósito: una corrutina perezosa que tomara una referencia a un
// temporal (la lista de inicialización del llamante) leería memoria liberada al ejecutarse.
nexus::task<void> produce_sequence(nexus::PartitionRouter& router, nexus::Reactor& self,
                                   std::array<CoreState, 2>& cores,
                                   std::vector<nexus::PartitionId> parts,
                                   std::vector<int>& offsets) {
    for (const nexus::PartitionId p : parts) {
        const int owner = router.owner_core(p);
        const int offset = co_await router.route(self, p, [&cores, owner, p] {
            return cores[static_cast<std::size_t>(owner)].next_offset[p]++;
        });
        offsets.push_back(offset);
    }
}

TEST(PartitionRouter, OwnerCore_RepartePorModuloDeNucleos) {
    ReactorPair pair;
    const nexus::PartitionRouter router{{&pair.r0, &pair.r1}};
    EXPECT_EQ(router.core_count(), 2);
    EXPECT_EQ(router.owner_core(0), 0);
    EXPECT_EQ(router.owner_core(1), 1);
    EXPECT_EQ(router.owner_core(2), 0);
    EXPECT_EQ(router.owner_core(3), 1);
}

TEST(PartitionRouter, Route_AsignaOffsetsPorParticionYDevuelveAlLlamante) {
    ReactorPair pair;
    nexus::PartitionRouter router{{&pair.r0, &pair.r1}};
    std::array<CoreState, 2> cores;
    std::vector<int> offsets;

    // Secuencia con repeticiones: p0 y p1 dos veces, p2 y p3 una vez.
    pair.r0.spawn(produce_sequence(router, pair.r0, cores, {0, 1, 2, 3, 0, 1}, offsets));
    pair.pump_until([&] { return offsets.size() == 6; });

    ASSERT_EQ(offsets.size(), 6U);
    // Offsets asignados por partición (cada una arranca en 0; la 2ª vez de p0/p1 da 1).
    EXPECT_EQ(offsets, (std::vector<int>{0, 0, 0, 0, 1, 1}));
}

TEST(PartitionRouter, Route_EstadoDeParticionViveSoloEnSuDueno) {
    ReactorPair pair;
    nexus::PartitionRouter router{{&pair.r0, &pair.r1}};
    std::array<CoreState, 2> cores;
    std::vector<int> offsets;

    pair.r0.spawn(produce_sequence(router, pair.r0, cores, {0, 1, 2, 3, 0, 1}, offsets));
    pair.pump_until([&] { return offsets.size() == 6; });

    // Núcleo 0 posee las particiones pares; el 1, las impares (sin solaparse).
    EXPECT_EQ(cores[0].next_offset[0], 2);  // p0 producida dos veces.
    EXPECT_EQ(cores[0].next_offset[2], 1);  // p2 una vez.
    EXPECT_FALSE(cores[0].next_offset.contains(1));
    EXPECT_FALSE(cores[0].next_offset.contains(3));

    EXPECT_EQ(cores[1].next_offset[1], 2);  // p1 producida dos veces.
    EXPECT_EQ(cores[1].next_offset[3], 1);  // p3 una vez.
    EXPECT_FALSE(cores[1].next_offset.contains(0));
    EXPECT_FALSE(cores[1].next_offset.contains(2));
}

}  // namespace
