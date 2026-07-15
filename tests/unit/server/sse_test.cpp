// SSE (ADR-0038): formato de eventos `text/event-stream` — event:/data: por línea + línea en
// blanco. El framing es puro y determinista (el bucle de emisión no lo es y se prueba por e2e).
#include "server/sse.hpp"

#include <gtest/gtest.h>

#include <string>

namespace {

TEST(Sse, FormatEvent_ConNombreYPayloadDeUnaLinea) {
    const std::string frame = nexus::format_sse_event("metrics", R"({"a":1})");
    EXPECT_EQ(frame, "event: metrics\ndata: {\"a\":1}\n\n");
}

TEST(Sse, FormatEvent_SinNombre_OmiteLaLineaEvent) {
    const std::string frame = nexus::format_sse_event("", "hola");
    EXPECT_EQ(frame, "data: hola\n\n");
}

TEST(Sse, FormatEvent_PayloadMultilinea_UnaLineaDataPorLinea) {
    const std::string frame = nexus::format_sse_event("e", "l1\nl2");
    EXPECT_EQ(frame, "event: e\ndata: l1\ndata: l2\n\n");
}

TEST(Sse, FormatEvent_PayloadVacio_EmiteDataVacio) {
    const std::string frame = nexus::format_sse_event("e", "");
    EXPECT_EQ(frame, "event: e\ndata: \n\n");
}

TEST(Sse, FormatEvent_TerminaSiempreEnLineaEnBlanco) {
    const std::string frame = nexus::format_sse_event("metrics", R"({"x":2})");
    ASSERT_GE(frame.size(), 2U);
    EXPECT_EQ(frame.substr(frame.size() - 2), "\n\n");
}

}  // namespace
