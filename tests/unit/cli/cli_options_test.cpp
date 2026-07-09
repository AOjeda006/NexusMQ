// Parseo de opciones globales del CLI (--host/--port/--token), incluyendo `--flag=valor`, el corte
// en el primer no-flag y los errores de validación (puerto/flag desconocido).
#include <gtest/gtest.h>

#include <sstream>
#include <string>
#include <string_view>
#include <vector>

#include "cli/cli.hpp"
#include "common/error.hpp"

namespace {

std::vector<std::string_view> args_of(std::initializer_list<std::string_view> items) {
    return {items};
}

TEST(CliOptions, PorDefecto_LocalhostYPuerto8080) {
    const auto parsed = nexus::cli::parse_global_options(args_of({"topic", "list"}));
    ASSERT_TRUE(parsed.has_value());
    EXPECT_EQ(parsed->options.host, "127.0.0.1");
    EXPECT_EQ(parsed->options.port, 8080);
    ASSERT_EQ(parsed->rest.size(), 2U);
    EXPECT_EQ(parsed->rest[0], "topic");
    EXPECT_EQ(parsed->rest[1], "list");
}

TEST(CliOptions, FlagsConValorSeparado) {
    const auto parsed = nexus::cli::parse_global_options(
        args_of({"--host", "10.0.0.5", "--port", "9000", "--token", "abc", "topic", "list"}));
    ASSERT_TRUE(parsed.has_value());
    EXPECT_EQ(parsed->options.host, "10.0.0.5");
    EXPECT_EQ(parsed->options.port, 9000);
    EXPECT_EQ(parsed->options.token, "abc");
    ASSERT_EQ(parsed->rest.size(), 2U);
    EXPECT_EQ(parsed->rest[0], "topic");
}

TEST(CliOptions, FlagsConValorPegado) {
    const auto parsed =
        nexus::cli::parse_global_options(args_of({"--host=example", "--port=1234", "topic"}));
    ASSERT_TRUE(parsed.has_value());
    EXPECT_EQ(parsed->options.host, "example");
    EXPECT_EQ(parsed->options.port, 1234);
    ASSERT_EQ(parsed->rest.size(), 1U);
}

TEST(CliOptions, PuertoInvalido_DevuelveError) {
    const auto parsed = nexus::cli::parse_global_options(args_of({"--port", "0"}));
    ASSERT_FALSE(parsed.has_value());
    EXPECT_EQ(parsed.error().code(), nexus::ErrorCode::InvalidArgument);
}

TEST(CliOptions, PuertoFueraDeRango_DevuelveError) {
    const auto parsed = nexus::cli::parse_global_options(args_of({"--port", "70000"}));
    ASSERT_FALSE(parsed.has_value());
    EXPECT_EQ(parsed.error().code(), nexus::ErrorCode::InvalidArgument);
}

TEST(CliOptions, FlagDesconocido_DevuelveError) {
    const auto parsed = nexus::cli::parse_global_options(args_of({"--verbose"}));
    ASSERT_FALSE(parsed.has_value());
    EXPECT_EQ(parsed.error().code(), nexus::ErrorCode::InvalidArgument);
}

// --- P5d: --help/-h/help imprimen el uso y salen con código 0 (no "opción desconocida") ---

TEST(CliRun, HelpLargo_ImprimeUsoYSaleCero) {
    std::ostringstream out;
    std::ostringstream err;
    const int rc = nexus::cli::run_cli(args_of({"--help"}), out, err);
    EXPECT_EQ(rc, 0);
    EXPECT_NE(out.str().find("uso: nexus-cli"), std::string::npos);
    EXPECT_TRUE(err.str().empty()) << "no debe reportar error: " << err.str();
}

TEST(CliRun, HelpCortoYComando_ImprimenUsoYSalenCero) {
    for (const std::string_view flag : {std::string_view{"-h"}, std::string_view{"help"}}) {
        std::ostringstream out;
        std::ostringstream err;
        const std::vector<std::string_view> args{flag};
        EXPECT_EQ(nexus::cli::run_cli(args, out, err), 0) << "flag: " << flag;
        EXPECT_NE(out.str().find("uso: nexus-cli"), std::string::npos) << "flag: " << flag;
    }
}

TEST(CliRun, HelpTrasFlagsGlobales_SaleCero) {
    std::ostringstream out;
    std::ostringstream err;
    const int rc = nexus::cli::run_cli(args_of({"--host", "h", "--help"}), out, err);
    EXPECT_EQ(rc, 0);
    EXPECT_NE(out.str().find("uso: nexus-cli"), std::string::npos);
}

}  // namespace
