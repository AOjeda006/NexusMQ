// Pruebas de ArenaAllocator: arena monótona reactor-local. Se valida la construcción de
// objetos en la arena, la independencia de direcciones, el recurso PMR y la liberación en
// bloque (reset) verificada con un upstream que cuenta bytes vivos.
#include "reactor/allocator.hpp"

#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <memory_resource>
#include <vector>

namespace {

// Upstream de prueba: cuenta llamadas de asignación y los bytes pendientes de liberar, para
// comprobar que `reset()` devuelve TODA la memoria que la arena pidió.
class CountingResource final : public std::pmr::memory_resource {
public:
    [[nodiscard]] int allocate_calls() const noexcept { return allocate_calls_; }
    [[nodiscard]] std::size_t outstanding() const noexcept { return outstanding_; }

private:
    void* do_allocate(std::size_t bytes, std::size_t alignment) override {
        ++allocate_calls_;
        outstanding_ += bytes;
        return std::pmr::new_delete_resource()->allocate(bytes, alignment);
    }
    void do_deallocate(void* pointer, std::size_t bytes, std::size_t alignment) override {
        outstanding_ -= bytes;
        std::pmr::new_delete_resource()->deallocate(pointer, bytes, alignment);
    }
    [[nodiscard]] bool do_is_equal(const std::pmr::memory_resource& other) const noexcept override {
        return this == &other;
    }

    int allocate_calls_ = 0;
    std::size_t outstanding_ = 0;
};

TEST(ArenaAllocator, Make_TipoTrivial_ConstruyeConElValorDado) {
    nexus::ArenaAllocator arena;
    int* value = arena.make<int>(42);
    ASSERT_NE(value, nullptr);
    EXPECT_EQ(*value, 42);
}

TEST(ArenaAllocator, Make_DosObjetos_DireccionesDistintas) {
    nexus::ArenaAllocator arena;
    int* first = arena.make<int>(1);
    int* second = arena.make<int>(2);
    EXPECT_NE(first, second);
    EXPECT_EQ(*first, 1);
    EXPECT_EQ(*second, 2);
}

TEST(ArenaAllocator, Make_TipoAlineado_RespetaLaAlineacion) {
    nexus::ArenaAllocator arena;
    struct alignas(64) Aligned {
        std::uint64_t value;
    };
    Aligned* object = arena.make<Aligned>(Aligned{.value = 7});
    EXPECT_EQ(reinterpret_cast<std::uintptr_t>(object) % alignof(Aligned), 0U);
    EXPECT_EQ(object->value, 7U);
}

TEST(ArenaAllocator, Resource_DevuelveRecursoUsablePorPmr) {
    nexus::ArenaAllocator arena;
    std::pmr::vector<int> numbers(arena.resource());
    for (int i = 0; i < 100; ++i) {
        numbers.push_back(i);
    }
    EXPECT_EQ(numbers.size(), 100U);
    EXPECT_EQ(numbers.front(), 0);
    EXPECT_EQ(numbers.back(), 99);
}

TEST(ArenaAllocator, Reset_TrasAsignar_DevuelveTodaLaMemoriaAlUpstream) {
    CountingResource upstream;
    {
        nexus::ArenaAllocator arena(/*initial_size=*/64, &upstream);
        // Fuerza varios bloques upstream asignando más que el bloque inicial.
        for (int i = 0; i < 256; ++i) {
            static_cast<void>(arena.make<std::uint64_t>(static_cast<std::uint64_t>(i)));
        }
        EXPECT_GE(upstream.allocate_calls(), 2);  // creció más allá del bloque inicial
        EXPECT_GT(upstream.outstanding(), 0U);

        arena.reset();
        EXPECT_EQ(upstream.outstanding(), 0U);  // reset libera todos los bloques

        // Tras el reset la arena sigue siendo usable.
        std::uint64_t* again = arena.make<std::uint64_t>(99);
        EXPECT_EQ(*again, 99U);
    }
    EXPECT_EQ(upstream.outstanding(), 0U);  // el destructor también libera
}

}  // namespace
