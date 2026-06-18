// Parser JSON (RFC 8259): tipos primitivos, anidamiento, escapes (incl. \uXXXX y subrogados),
// rechazos de gramática y límite de profundidad.
#include "ingress/json_value.hpp"

#include <gtest/gtest.h>

#include <string>

namespace {

TEST(ParseJson, ObjetoPlano_TiposBasicos) {
    const auto value =
        nexus::parse_json(R"({"name":"orders","partitions":3,"leader":true,"x":null})");
    ASSERT_TRUE(value.has_value());
    ASSERT_TRUE(value->is_object());

    const nexus::JsonValue* name = value->find("name");
    ASSERT_NE(name, nullptr);
    ASSERT_TRUE(name->is_string());
    EXPECT_EQ(name->as_string(), "orders");

    const nexus::JsonValue* partitions = value->find("partitions");
    ASSERT_NE(partitions, nullptr);
    ASSERT_TRUE(partitions->is_number());
    EXPECT_EQ(partitions->as_int64(), 3);

    ASSERT_NE(value->find("leader"), nullptr);
    EXPECT_TRUE(value->find("leader")->as_bool());
    ASSERT_NE(value->find("x"), nullptr);
    EXPECT_TRUE(value->find("x")->is_null());
    EXPECT_EQ(value->find("ausente"), nullptr);
}

TEST(ParseJson, ArraysYAnidamiento) {
    const auto value = nexus::parse_json(R"({"ids":[0,1,2],"meta":{"k":"v"}})");
    ASSERT_TRUE(value.has_value());

    const nexus::JsonValue* ids = value->find("ids");
    ASSERT_NE(ids, nullptr);
    ASSERT_TRUE(ids->is_array());
    ASSERT_EQ(ids->as_array().size(), 3U);
    EXPECT_EQ(ids->as_array()[2].as_int64(), 2);

    const nexus::JsonValue* meta = value->find("meta");
    ASSERT_NE(meta, nullptr);
    ASSERT_NE(meta->find("k"), nullptr);
    EXPECT_EQ(meta->find("k")->as_string(), "v");
}

TEST(ParseJson, Numeros_FraccionYExponente) {
    const auto value = nexus::parse_json(R"([0,-5,3.14,1e3,-2.5E-2])");
    ASSERT_TRUE(value.has_value());
    const auto& array = value->as_array();
    EXPECT_DOUBLE_EQ(array[0].as_number(), 0.0);
    EXPECT_DOUBLE_EQ(array[1].as_number(), -5.0);
    EXPECT_DOUBLE_EQ(array[2].as_number(), 3.14);
    EXPECT_DOUBLE_EQ(array[3].as_number(), 1000.0);
    EXPECT_DOUBLE_EQ(array[4].as_number(), -0.025);
}

TEST(ParseJson, EscapesEnCadenas) {
    const auto value = nexus::parse_json(R"({"s":"a\"b\\c\n\t\/"})");
    ASSERT_TRUE(value.has_value());
    EXPECT_EQ(value->find("s")->as_string(), "a\"b\\c\n\t/");
}

TEST(ParseJson, UnicodeEscape_BMP) {
    // U+00E9 (é) escrito como é → UTF-8 0xC3 0xA9.
    const auto value = nexus::parse_json(R"({"s":"caf\u00e9"})");
    ASSERT_TRUE(value.has_value());
    EXPECT_EQ(value->find("s")->as_string(), "caf\xC3\xA9");
}

TEST(ParseJson, UnicodeEscape_ParSubrogado) {
    // U+1F600 (😀) escrito como par subrogado 😀 → UTF-8 0xF0 0x9F 0x98 0x80.
    const auto value = nexus::parse_json(R"({"s":"\ud83d\ude00"})");
    ASSERT_TRUE(value.has_value());
    EXPECT_EQ(value->find("s")->as_string(), "\xF0\x9F\x98\x80");
}

TEST(ParseJson, EspaciosAlrededor) {
    const auto value = nexus::parse_json("  \n\t {  \"a\" : 1 }  \n");
    ASSERT_TRUE(value.has_value());
    EXPECT_EQ(value->find("a")->as_int64(), 1);
}

TEST(ParseJson, RechazaGramaticaInvalida) {
    EXPECT_FALSE(nexus::parse_json("").has_value());
    EXPECT_FALSE(nexus::parse_json("{").has_value());
    EXPECT_FALSE(nexus::parse_json(R"({"a":1,})").has_value());       // coma colgante.
    EXPECT_FALSE(nexus::parse_json(R"({"a" 1})").has_value());        // falta ':'.
    EXPECT_FALSE(nexus::parse_json("[1,2").has_value());              // array sin cerrar.
    EXPECT_FALSE(nexus::parse_json("01").has_value());                // cero a la izquierda.
    EXPECT_FALSE(nexus::parse_json("nul").has_value());               // literal incompleto.
    EXPECT_FALSE(nexus::parse_json(R"({"a":1} extra)").has_value());  // basura tras el valor.
}

TEST(ParseJson, RechazaAnidamientoExcesivo) {
    const std::string deep = std::string(200, '[') + std::string(200, ']');
    const auto value = nexus::parse_json(deep);
    EXPECT_FALSE(value.has_value());
    EXPECT_EQ(value.error().code(), nexus::ErrorCode::InvalidArgument);
}

}  // namespace
