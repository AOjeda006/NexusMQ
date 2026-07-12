#include "storage/storage_tier.hpp"

#include <gtest/gtest.h>

#include <cstdint>
#include <random>
#include <string>

#include "common/error.hpp"

namespace {

using nexus::ErrorCode;
using nexus::Offset;
using nexus::SegmentFileKind;
using nexus::TierObjectKey;

TEST(TierObjectKey, Encode_ClaveLog_ProduceRutaJerarquica) {
    const TierObjectKey key{"events", 3, 42, SegmentFileKind::Log};
    EXPECT_EQ(key.encode(), "events/3/00000000000000000042.log");
}

TEST(TierObjectKey, Encode_ClaveIndex_UsaExtensionIndex) {
    const TierObjectKey key{"events", 0, 0, SegmentFileKind::Index};
    EXPECT_EQ(key.encode(), "events/0/00000000000000000000.index");
}

TEST(TierObjectKey, Encode_OffsetGrande_SeRellenaA20Digitos) {
    const TierObjectKey key{"t", 1, 9'876'543'210, SegmentFileKind::Log};
    EXPECT_EQ(key.encode(), "t/1/00000000009876543210.log");
}

TEST(TierObjectKey, Decode_RutaValida_ReconstruyeLaClave) {
    const auto key = TierObjectKey::decode("events/3/00000000000000000042.log");
    ASSERT_TRUE(key.has_value());
    EXPECT_EQ(key->topic, "events");
    EXPECT_EQ(key->partition, 3);
    EXPECT_EQ(key->base_offset, 42);
    EXPECT_EQ(key->kind, SegmentFileKind::Log);
}

TEST(TierObjectKey, Decode_TopicConGuionYPunto_SeConserva) {
    const auto key = TierObjectKey::decode("my.topic-name/12/00000000000000000100.index");
    ASSERT_TRUE(key.has_value());
    EXPECT_EQ(key->topic, "my.topic-name");
    EXPECT_EQ(key->partition, 12);
    EXPECT_EQ(key->base_offset, 100);
    EXPECT_EQ(key->kind, SegmentFileKind::Index);
}

TEST(TierObjectKey, Decode_ExtensionDesconocida_DevuelveInvalidArgument) {
    const auto key = TierObjectKey::decode("events/0/00000000000000000000.dat");
    ASSERT_FALSE(key.has_value());
    EXPECT_EQ(key.error().code(), ErrorCode::InvalidArgument);
}

TEST(TierObjectKey, Decode_SinSeparadores_DevuelveInvalidArgument) {
    EXPECT_FALSE(TierObjectKey::decode("noseps.log").has_value());
    EXPECT_FALSE(TierObjectKey::decode("solo/unseparador.log").has_value());
}

TEST(TierObjectKey, Decode_TopicVacio_DevuelveInvalidArgument) {
    const auto key = TierObjectKey::decode("/3/00000000000000000042.log");
    ASSERT_FALSE(key.has_value());
    EXPECT_EQ(key.error().code(), ErrorCode::InvalidArgument);
}

TEST(TierObjectKey, Decode_ParticionNoNumerica_DevuelveInvalidArgument) {
    const auto key = TierObjectKey::decode("events/abc/00000000000000000042.log");
    ASSERT_FALSE(key.has_value());
    EXPECT_EQ(key.error().code(), ErrorCode::InvalidArgument);
}

TEST(TierObjectKey, Decode_ParticionNegativa_DevuelveInvalidArgument) {
    const auto key = TierObjectKey::decode("events/-1/00000000000000000042.log");
    ASSERT_FALSE(key.has_value());
    EXPECT_EQ(key.error().code(), ErrorCode::InvalidArgument);
}

TEST(TierObjectKey, Decode_SinExtension_DevuelveInvalidArgument) {
    const auto key = TierObjectKey::decode("events/3/00000000000000000042");
    ASSERT_FALSE(key.has_value());
    EXPECT_EQ(key.error().code(), ErrorCode::InvalidArgument);
}

// Property-based: encode∘decode es la identidad para claves arbitrarias válidas.
TEST(TierObjectKey, RoundTrip_ClavesAleatorias_PreservaLaClave) {
    std::mt19937_64 rng(0xB1B1B1B1);  // semilla fija: test determinista.
    std::uniform_int_distribution<std::int32_t> part_dist(0, 1'000'000);
    std::uniform_int_distribution<std::int64_t> off_dist(0, 1'000'000'000'000);
    std::uniform_int_distribution<int> kind_dist(0, 1);
    std::uniform_int_distribution<int> topic_len(1, 24);
    const std::string alphabet = "abcdefghijklmnopqrstuvwxyz0123456789._-";
    std::uniform_int_distribution<std::size_t> ch(0, alphabet.size() - 1);

    for (int i = 0; i < 500; ++i) {
        std::string topic;
        const int len = topic_len(rng);
        for (int c = 0; c < len; ++c) {
            topic.push_back(alphabet[ch(rng)]);
        }
        const TierObjectKey original{
            topic, part_dist(rng), static_cast<Offset>(off_dist(rng)),
            kind_dist(rng) == 0 ? SegmentFileKind::Log : SegmentFileKind::Index};

        const auto decoded = TierObjectKey::decode(original.encode());
        ASSERT_TRUE(decoded.has_value()) << "clave: " << original.encode();
        EXPECT_EQ(*decoded, original) << "clave: " << original.encode();
    }
}

}  // namespace
