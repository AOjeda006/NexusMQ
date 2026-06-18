// JsonWriter (§7.6): construcción incremental con comas y escapado correctos. Verifica objetos,
// arrays anidados, tipos (string/int/double/bool/null) y el escapado de cadenas.
#include "ingress/json.hpp"

#include <gtest/gtest.h>

namespace {

TEST(JsonWriter, ObjetoPlano) {
    nexus::JsonWriter j;
    j.begin_object()
        .field("name", "orders")
        .field("partitions", 3)
        .field("leader", true)
        .end_object();
    EXPECT_EQ(j.str(), R"({"name":"orders","partitions":3,"leader":true})");
}

TEST(JsonWriter, ArrayDeEnteros) {
    nexus::JsonWriter j;
    j.begin_array().value(1).value(2).value(3).end_array();
    EXPECT_EQ(j.str(), "[1,2,3]");
}

TEST(JsonWriter, ObjetosAnidadosYArrays) {
    nexus::JsonWriter j;
    j.begin_object();
    j.key("topic").value("t");
    j.key("partitions").begin_array();
    j.begin_object().field("id", 0).field("leader", 1).end_object();
    j.begin_object().field("id", 1).field("leader", 2).end_object();
    j.end_array();
    j.end_object();
    EXPECT_EQ(j.str(), R"({"topic":"t","partitions":[{"id":0,"leader":1},{"id":1,"leader":2}]})");
}

TEST(JsonWriter, EscapaCadenas) {
    nexus::JsonWriter j;
    j.begin_object().field("k", "a\"b\\c\n").end_object();
    EXPECT_EQ(j.str(), R"({"k":"a\"b\\c\n"})");
}

TEST(JsonWriter, NullYArrayVacio) {
    nexus::JsonWriter j;
    j.begin_object().key("a").null_value().key("b").begin_array().end_array().end_object();
    EXPECT_EQ(j.str(), R"({"a":null,"b":[]})");
}

}  // namespace
