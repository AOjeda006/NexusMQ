#include "storage/local_storage_tier.hpp"

#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include "common/error.hpp"
#include "storage/storage_tier.hpp"

namespace {

using nexus::ErrorCode;
using nexus::LocalStorageTier;
using nexus::SegmentFileKind;
using nexus::TierObjectKey;

// Directorio temporal único con limpieza RAII.
class TempDir {
public:
    explicit TempDir(const char* tag)
        : path_(std::filesystem::temp_directory_path() /
                ("nexus_tier_" + std::string{tag} + "_" +
                 std::to_string(::testing::UnitTest::GetInstance()->random_seed()))) {
        std::filesystem::remove_all(path_);
        std::filesystem::create_directories(path_);
    }
    ~TempDir() { std::filesystem::remove_all(path_); }
    TempDir(const TempDir&) = delete;
    TempDir& operator=(const TempDir&) = delete;

    [[nodiscard]] const std::filesystem::path& path() const noexcept { return path_; }

private:
    std::filesystem::path path_;
};

// Escribe @p bytes en @p path (binario). Falla el test si no puede.
void write_file(const std::filesystem::path& path, const std::vector<std::byte>& bytes) {
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    ASSERT_TRUE(out.is_open());
    out.write(reinterpret_cast<const char*>(bytes.data()),
              static_cast<std::streamsize>(bytes.size()));
    ASSERT_TRUE(out.good());
}

// Lee todo @p path como bytes.
std::vector<std::byte> read_file(const std::filesystem::path& path) {
    std::ifstream in(path, std::ios::binary);
    std::vector<char> raw((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    std::vector<std::byte> bytes(raw.size());
    for (std::size_t i = 0; i < raw.size(); ++i) {
        bytes[i] = static_cast<std::byte>(raw[i]);
    }
    return bytes;
}

// Contenido de prueba: incluye bytes nulos y todo el rango 0..255 (binario, no texto).
std::vector<std::byte> sample_bytes(std::size_t len = 300) {
    std::vector<std::byte> data(len);
    for (std::size_t i = 0; i < len; ++i) {
        data[i] = static_cast<std::byte>((i * 31 + 7) % 256);
    }
    return data;
}

const TierObjectKey kLogKey{"events", 3, 42, SegmentFileKind::Log};

TEST(LocalStorageTier, PutFile_ObjetoNuevo_SeSubeYContiene) {
    const TempDir root("put_nuevo");
    const auto object_dir = root.path() / "tier";
    const auto source = root.path() / "seg.log";
    write_file(source, sample_bytes());

    LocalStorageTier tier(object_dir);
    ASSERT_TRUE(tier.put_file(kLogKey, source).has_value());

    const auto present = tier.contains(kLogKey);
    ASSERT_TRUE(present.has_value());
    EXPECT_TRUE(*present);

    const auto size = tier.object_size(kLogKey);
    ASSERT_TRUE(size.has_value());
    EXPECT_EQ(*size, sample_bytes().size());
}

TEST(LocalStorageTier, PutFile_CreaLaJerarquiaDeLaClave) {
    const TempDir root("put_jerarquia");
    const auto object_dir = root.path() / "tier";
    const auto source = root.path() / "seg.log";
    write_file(source, sample_bytes(16));

    LocalStorageTier tier(object_dir);
    ASSERT_TRUE(tier.put_file(kLogKey, source).has_value());

    // El objeto vive exactamente en object_dir/<topic>/<partition>/<base:020>.log.
    EXPECT_TRUE(std::filesystem::exists(object_dir / "events" / "3" / "00000000000000000042.log"));
}

TEST(LocalStorageTier, PutFile_OrigenInexistente_DevuelveNotFound) {
    const TempDir root("put_sin_origen");
    LocalStorageTier tier(root.path() / "tier");
    const auto result = tier.put_file(kLogKey, root.path() / "no-existe.log");
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code(), ErrorCode::NotFound);
}

TEST(LocalStorageTier, PutFile_MismaClaveDosVeces_SobrescribeIdempotente) {
    const TempDir root("put_idem");
    const auto object_dir = root.path() / "tier";
    const auto source1 = root.path() / "v1.log";
    const auto source2 = root.path() / "v2.log";
    write_file(source1, sample_bytes(100));
    write_file(source2, sample_bytes(250));

    LocalStorageTier tier(object_dir);
    ASSERT_TRUE(tier.put_file(kLogKey, source1).has_value());
    ASSERT_TRUE(tier.put_file(kLogKey, source2).has_value());  // idempotente: reescribe.

    const auto size = tier.object_size(kLogKey);
    ASSERT_TRUE(size.has_value());
    EXPECT_EQ(*size, 250u);  // gana la última subida.
}

TEST(LocalStorageTier, FetchFile_ObjetoExistente_RestauraContenidoByteAByte) {
    const TempDir root("fetch_ok");
    const auto object_dir = root.path() / "tier";
    const auto source = root.path() / "seg.log";
    const auto restored = root.path() / "restaurado" / "seg.log";
    const auto payload = sample_bytes(512);
    write_file(source, payload);

    LocalStorageTier tier(object_dir);
    ASSERT_TRUE(tier.put_file(kLogKey, source).has_value());
    ASSERT_TRUE(tier.fetch_file(kLogKey, restored).has_value());

    EXPECT_EQ(read_file(restored), payload);  // idéntico al original (incluye nulos).
}

TEST(LocalStorageTier, FetchFile_ObjetoInexistente_DevuelveNotFound) {
    const TempDir root("fetch_ausente");
    LocalStorageTier tier(root.path() / "tier");
    const auto result = tier.fetch_file(kLogKey, root.path() / "dest.log");
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code(), ErrorCode::NotFound);
}

TEST(LocalStorageTier, Contains_ClaveAusente_DevuelveFalse) {
    const TempDir root("contains_no");
    LocalStorageTier tier(root.path() / "tier");
    const auto present = tier.contains(kLogKey);
    ASSERT_TRUE(present.has_value());
    EXPECT_FALSE(*present);
}

TEST(LocalStorageTier, ObjectSize_ClaveAusente_DevuelveNotFound) {
    const TempDir root("size_no");
    LocalStorageTier tier(root.path() / "tier");
    const auto size = tier.object_size(kLogKey);
    ASSERT_FALSE(size.has_value());
    EXPECT_EQ(size.error().code(), ErrorCode::NotFound);
}

TEST(LocalStorageTier, Remove_ObjetoExistente_LoBorra) {
    const TempDir root("remove_ok");
    const auto object_dir = root.path() / "tier";
    const auto source = root.path() / "seg.log";
    write_file(source, sample_bytes(64));

    LocalStorageTier tier(object_dir);
    ASSERT_TRUE(tier.put_file(kLogKey, source).has_value());
    ASSERT_TRUE(tier.remove(kLogKey).has_value());

    const auto present = tier.contains(kLogKey);
    ASSERT_TRUE(present.has_value());
    EXPECT_FALSE(*present);
}

TEST(LocalStorageTier, Remove_ClaveAusente_EsIdempotente) {
    const TempDir root("remove_idem");
    LocalStorageTier tier(root.path() / "tier");
    EXPECT_TRUE(tier.remove(kLogKey).has_value());  // no falla aunque no exista.
}

TEST(LocalStorageTier, ClavesDeParticionesDistintas_NoColisionan) {
    const TempDir root("no_colision");
    const auto object_dir = root.path() / "tier";
    const auto source = root.path() / "seg.log";
    write_file(source, sample_bytes(48));

    LocalStorageTier tier(object_dir);
    const TierObjectKey key_a{"events", 0, 0, SegmentFileKind::Log};
    const TierObjectKey key_b{"events", 1, 0, SegmentFileKind::Log};
    const TierObjectKey key_idx{"events", 0, 0, SegmentFileKind::Index};
    ASSERT_TRUE(tier.put_file(key_a, source).has_value());
    ASSERT_TRUE(tier.put_file(key_b, source).has_value());
    ASSERT_TRUE(tier.put_file(key_idx, source).has_value());

    for (const auto& key : {key_a, key_b, key_idx}) {
        const auto present = tier.contains(key);
        ASSERT_TRUE(present.has_value());
        EXPECT_TRUE(*present) << "clave: " << key.encode();
    }
    ASSERT_TRUE(tier.remove(key_a).has_value());
    // Borrar A no afecta a B ni al índice.
    EXPECT_TRUE(tier.contains(key_b).value());
    EXPECT_TRUE(tier.contains(key_idx).value());
}

}  // namespace
