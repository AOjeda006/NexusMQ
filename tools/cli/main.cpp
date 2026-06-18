/// @file   cli/main.cpp
/// @brief  Punto de entrada de nexus-cli: traduce argv y despacha al motor del CLI.
/// @ingroup cli

#include <iostream>
#include <span>
#include <string_view>
#include <vector>

#include "cli/cli.hpp"

int main(int argc, char** argv) {
    std::vector<std::string_view> args;
    args.reserve(static_cast<std::size_t>(argc > 0 ? argc - 1 : 0));
    for (int i = 1; i < argc; ++i) {
        args.emplace_back(argv[i]);  // NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
    }
    return nexus::cli::run_cli(args, std::cout, std::cerr);
}
