// Pruebas de RaftStateStore: persistencia durable del estado persistente de Raft (término y voto).
// La seguridad de Raft (§5) exige que term/voto sobrevivan a un reinicio; aquí se verifica el
// round-trip, el estado inicial de un fichero vacío, la persistencia entre reaperturas y la
// detección de corrupción (CRC32C) que protege la regla "un voto por término".
#include "consensus/raft_state_store.hpp"

#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <ios>
#include <optional>
#include <string>

#include "common/error.hpp"
#include "common/types.hpp"
#include "consensus/raft_state.hpp"

namespace {

class TempDir {
public:
    explicit TempDir(const char* tag)
        : path_(std::filesystem::temp_directory_path() /
                ("nexus_raftstate_" + std::string{tag} + "_" +
                 std::to_string(::testing::UnitTest::GetInstance()->random_seed()))) {
        std::filesystem::remove_all(path_);
        std::filesystem::create_directories(path_);
    }
    ~TempDir() { std::filesystem::remove_all(path_); }
    TempDir(const TempDir&) = delete;
    TempDir& operator=(const TempDir&) = delete;

    [[nodiscard]] std::string file_path() const { return (path_ / "raft-state").string(); }

private:
    std::filesystem::path path_;
};

TEST(RaftStateStore, Load_FicheroVacio_DevuelveEstadoInicial) {
    const TempDir dir{"empty"};
    auto store = nexus::RaftStateStore::open(dir.file_path());
    ASSERT_TRUE(store.has_value());

    const auto loaded = store->load();
    ASSERT_TRUE(loaded.has_value());
    EXPECT_EQ(loaded->current_term(), 0);
    EXPECT_FALSE(loaded->voted_for().has_value());
}

TEST(RaftStateStore, SaveLoad_ConTerminoYVoto_RoundTrip) {
    const TempDir dir{"roundtrip"};
    auto store = nexus::RaftStateStore::open(dir.file_path());
    ASSERT_TRUE(store.has_value());

    nexus::RaftPersistentState state;
    state.advance_term(7);
    state.record_vote(3);
    ASSERT_TRUE(store->save(state).has_value());

    const auto loaded = store->load();
    ASSERT_TRUE(loaded.has_value());
    EXPECT_EQ(loaded->current_term(), 7);
    ASSERT_TRUE(loaded->voted_for().has_value());
    EXPECT_EQ(*loaded->voted_for(), 3);
    EXPECT_EQ(*loaded, state);
}

TEST(RaftStateStore, SaveLoad_SinVoto_PreservaAusencia) {
    const TempDir dir{"novote"};
    auto store = nexus::RaftStateStore::open(dir.file_path());
    ASSERT_TRUE(store.has_value());

    nexus::RaftPersistentState state;
    state.advance_term(4);  // advance_term descarta el voto.
    ASSERT_TRUE(store->save(state).has_value());

    const auto loaded = store->load();
    ASSERT_TRUE(loaded.has_value());
    EXPECT_EQ(loaded->current_term(), 4);
    EXPECT_FALSE(loaded->voted_for().has_value());
}

TEST(RaftStateStore, Save_SobrescribeEstadoPrevio) {
    const TempDir dir{"overwrite"};
    auto store = nexus::RaftStateStore::open(dir.file_path());
    ASSERT_TRUE(store.has_value());

    nexus::RaftPersistentState first;
    first.advance_term(2);
    first.record_vote(1);
    ASSERT_TRUE(store->save(first).has_value());

    nexus::RaftPersistentState second;
    second.advance_term(9);
    second.record_vote(2);
    ASSERT_TRUE(store->save(second).has_value());

    const auto loaded = store->load();
    ASSERT_TRUE(loaded.has_value());
    EXPECT_EQ(loaded->current_term(), 9);
    ASSERT_TRUE(loaded->voted_for().has_value());
    EXPECT_EQ(*loaded->voted_for(), 2);
}

TEST(RaftStateStore, Load_TrasReabrir_RecuperaEstado) {
    const TempDir dir{"reopen"};
    {
        auto store = nexus::RaftStateStore::open(dir.file_path());
        ASSERT_TRUE(store.has_value());
        nexus::RaftPersistentState state;
        state.advance_term(11);
        state.record_vote(5);
        ASSERT_TRUE(store->save(state).has_value());
    }
    // Nuevo almacén sobre el mismo fichero (simula un reinicio del nodo).
    auto reopened = nexus::RaftStateStore::open(dir.file_path());
    ASSERT_TRUE(reopened.has_value());
    const auto loaded = reopened->load();
    ASSERT_TRUE(loaded.has_value());
    EXPECT_EQ(loaded->current_term(), 11);
    ASSERT_TRUE(loaded->voted_for().has_value());
    EXPECT_EQ(*loaded->voted_for(), 5);
}

TEST(RaftStateStore, Load_RegistroCorrupto_DevuelveCorrupt) {
    const TempDir dir{"corrupt"};
    {
        auto store = nexus::RaftStateStore::open(dir.file_path());
        ASSERT_TRUE(store.has_value());
        nexus::RaftPersistentState state;
        state.advance_term(6);
        state.record_vote(2);
        ASSERT_TRUE(store->save(state).has_value());
    }
    // Voltea un byte de la carga (el término): el CRC32C debe delatar la corrupción.
    {
        std::fstream file{dir.file_path(), std::ios::in | std::ios::out | std::ios::binary};
        ASSERT_TRUE(file.is_open());
        file.seekp(5);  // dentro del campo término (offset 4..11).
        const char flipped = static_cast<char>(0x7F);
        file.write(&flipped, 1);
    }
    auto store = nexus::RaftStateStore::open(dir.file_path());
    ASSERT_TRUE(store.has_value());
    const auto loaded = store->load();
    ASSERT_FALSE(loaded.has_value());
    EXPECT_EQ(loaded.error().code(), nexus::ErrorCode::Corrupt);
}

TEST(RaftStateStore, Load_RegistroTruncado_DevuelveCorrupt) {
    const TempDir dir{"truncated"};
    {
        auto store = nexus::RaftStateStore::open(dir.file_path());
        ASSERT_TRUE(store.has_value());
        nexus::RaftPersistentState state;
        state.advance_term(3);
        ASSERT_TRUE(store->save(state).has_value());
    }
    std::filesystem::resize_file(dir.file_path(), 5);  // menos de un registro completo (17 B).
    auto store = nexus::RaftStateStore::open(dir.file_path());
    ASSERT_TRUE(store.has_value());
    const auto loaded = store->load();
    ASSERT_FALSE(loaded.has_value());
    EXPECT_EQ(loaded.error().code(), nexus::ErrorCode::Corrupt);
}

}  // namespace
