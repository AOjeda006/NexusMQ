#!/usr/bin/env bash
# Verifica que la capa de E/S (nexus-io) y sus dependencias (nexus-common) COMPILAN y ENLAZAN para
# Windows usando el cross-compiler MinGW-w64 (headers Win32 reales), sin necesidad de MSVC ni una
# máquina Windows. NO ejecuta nada (no hay runtime Windows aquí): es la verificación de compilación
# que sostiene el port IOCP/Win32 (ADR-0022). El backend io_uring se excluye (es Linux-nativo).
#
# Uso:   tools/verify-windows-io.sh
# Requisitos: g++-mingw-w64-x86-64  (apt-get install -y g++-mingw-w64-x86-64)
set -euo pipefail

CXX="${MINGW_CXX:-x86_64-w64-mingw32-g++-posix}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
OUT="$(mktemp -d)"
trap 'rm -rf "$OUT"' EXIT

FLAGS=(-std=c++23 -Wall -Wextra -Wpedantic -Werror -I"$ROOT/src" -DNEXUSMQ_VERSION='"0.0.0-wincheck"')

echo "== Compilando common + io (Win32) con $CXX =="
mapfile -t SOURCES < <(
  ls "$ROOT"/src/common/*.cpp
  echo "$ROOT/src/io/file.cpp"
  echo "$ROOT/src/io/socket.cpp"
  echo "$ROOT/src/io/iocp_backend.cpp"
  echo "$ROOT/src/io/aligned_buffer.cpp"
  echo "$ROOT/src/io/block_reader.cpp"
)
OBJS=()
for src in "${SOURCES[@]}"; do
  obj="$OUT/$(basename "$src" .cpp).o"
  "$CXX" "${FLAGS[@]}" -c "$src" -o "$obj"
  OBJS+=("$obj")
done

echo "== Enlazando un driver que usa IocpBackend/File/Socket contra ws2_32/mswsock =="
cat >"$OUT/driver.cpp" <<'CPP'
#include "io/file.hpp"
#include "io/iocp_backend.hpp"
#include "io/socket.hpp"
int main() {
    nexus::IocpBackend backend(64);
    nexus::Proactor& proactor = backend;
    static_cast<void>(proactor.run_completions(1));
    const auto file = nexus::File::open("nul", nexus::File::Mode::ReadWrite);
    const auto sock = nexus::Socket::connect("127.0.0.1", 80);
    const auto listener = nexus::Listener::bind("", 0);
    return (file && sock && listener) ? 0 : 1;
}
CPP
"$CXX" "${FLAGS[@]}" -c "$OUT/driver.cpp" -o "$OUT/driver.o"
"$CXX" "${OBJS[@]}" "$OUT/driver.o" -o "$OUT/nexus-io-win.exe" -lws2_32 -lmswsock

echo "OK: nexus-io compila y enlaza para Windows ($(stat -c%s "$OUT/nexus-io-win.exe") bytes)."
