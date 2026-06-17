// Logger estructurado JSON (§7.6, ADR-0017): reloj inyectado y salida determinista. Verifica el
// formato de línea (ts RFC3339, level, msg), el filtrado por nivel, los campos tipados, el escapado
// JSON, el contexto base en cada línea y el correlation id como campo.
#include "telemetry/logging.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <sstream>
#include <string>

namespace {

// Reloj fijo: epoch + `ms` milisegundos (UTC determinista).
nexus::Logger::ClockFn fixed_clock(std::int64_t ms) {
    return [ms] { return std::chrono::system_clock::time_point{std::chrono::milliseconds{ms}}; };
}

TEST(Logger, Log_EmiteLineaJsonConTimestampNivelYMensaje) {
    std::ostringstream out;
    nexus::Logger log(out, nexus::LogLevel::Info, fixed_clock(1234));
    log.info("hola mundo");
    EXPECT_EQ(out.str(),
              "{\"ts\":\"1970-01-01T00:00:01.234Z\",\"level\":\"info\",\"msg\":\"hola mundo\"}\n");
}

TEST(Logger, Log_NivelPorDebajoDelMinimo_NoEmite) {
    std::ostringstream out;
    nexus::Logger log(out, nexus::LogLevel::Warn, fixed_clock(0));
    log.info("no debe salir");
    log.debug("tampoco");
    EXPECT_TRUE(out.str().empty());
    log.error("esto si");
    EXPECT_NE(out.str().find("\"level\":\"error\""), std::string::npos);
}

TEST(Logger, Log_CamposTipados_SeSerializanComoJson) {
    std::ostringstream out;
    nexus::Logger log(out, nexus::LogLevel::Info, fixed_clock(0));
    const std::array<nexus::Field, 4> fields{
        nexus::field("topic", "orders"), nexus::field("partition", 3), nexus::field("leader", true),
        nexus::field("ratio", 0.5)};
    log.info("produce", fields);
    const std::string line = out.str();
    EXPECT_NE(line.find("\"topic\":\"orders\""), std::string::npos);
    EXPECT_NE(line.find("\"partition\":3"), std::string::npos);
    EXPECT_NE(line.find("\"leader\":true"), std::string::npos);
    EXPECT_NE(line.find("\"ratio\":0.5"), std::string::npos);
}

TEST(Logger, Log_EscapaComillasYControl) {
    std::ostringstream out;
    nexus::Logger log(out, nexus::LogLevel::Info, fixed_clock(0));
    log.info("dice \"hola\"\ty salta\n");
    const std::string line = out.str();
    EXPECT_NE(line.find("\\\"hola\\\""), std::string::npos);  // comillas escapadas.
    EXPECT_NE(line.find("salta\\n"), std::string::npos);      // salto de línea escapado.
    EXPECT_NE(line.find("\\t"), std::string::npos);           // tabulador escapado.
    // El registro es una sola línea: el único '\n' real es el terminador final.
    EXPECT_EQ(std::count(line.begin(), line.end(), '\n'), 1);
}

TEST(Logger, AddContext_SeEmiteEnCadaLinea) {
    std::ostringstream out;
    nexus::Logger log(out, nexus::LogLevel::Info, fixed_clock(0));
    log.add_context(nexus::field("node", 7));
    log.add_context(nexus::field("service", "nexusd"));
    log.info("uno");
    log.info("dos");
    const std::string text = out.str();
    EXPECT_EQ(std::count(text.begin(), text.end(), '\n'), 2);
    EXPECT_NE(text.find("\"node\":7"), std::string::npos);
    EXPECT_NE(text.find("\"service\":\"nexusd\""), std::string::npos);
}

TEST(Logger, CorrelationId_ComoCampoPorPeticion) {
    std::ostringstream out;
    nexus::Logger log(out, nexus::LogLevel::Info, fixed_clock(0));
    const std::array<nexus::Field, 1> fields{nexus::field("correlation_id", "abc-123")};
    log.info("request", fields);
    EXPECT_NE(out.str().find("\"correlation_id\":\"abc-123\""), std::string::npos);
}

}  // namespace
