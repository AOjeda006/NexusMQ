/// @file   tools/wincheck/main.cpp
/// @brief  Arnés de verificación en RUNTIME de nexus-io en Windows (File + IOCP + timer).
/// @ingroup tools
///
/// @details Windows-only (F10, ADR-0022). Ejercita la capa de E/S **ejecutándola** de verdad, lo
///   que la build cruzada con MinGW no podía cubrir: (a) `File` con E/S bloqueante (round-trip,
///   size/sync/truncate, y E/S directa si el FS la admite); (b) `IocpBackend` + `Socket`/`Listener`
///   con un eco por loopback, bombeando las completions a mano (sin `ReactorPool`, que es POSIX);
///   (c) `submit_timer` con un deadline corto. No depende de ctest ni del reactor. Devuelve 0 si
///   todos los casos pasan; el número de fallos en caso contrario.

#include <array>
#include <chrono>
#include <coroutine>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <exception>
#include <filesystem>
#include <span>
#include <string>
#include <string_view>

#include "common/bytes.hpp"
#include "common/error.hpp"
#include "common/task.hpp"
#include "common/types.hpp"
#include "io/aligned_buffer.hpp"
#include "io/awaitable.hpp"
#include "io/file.hpp"
#include "io/iocp_backend.hpp"
#include "io/socket.hpp"

namespace {

using namespace std::chrono_literals;
using nexus::AlignedBuffer;
using nexus::ByteSpan;
using nexus::Error;
using nexus::expected;
using nexus::File;
using nexus::IocpBackend;
using nexus::Listener;
using nexus::MutByteSpan;
using nexus::Proactor;
using nexus::Socket;
using nexus::task;
using nexus::TimerAwaitable;

// --- Utilidades de reporte y conversión de bytes ---

/// Acumula PASA/FALLA/OMIT por caso y cuenta los fallos (código de salida del proceso).
class Reporter {
public:
    void pass(std::string_view name) { line("PASA", name, {}); }
    void fail(std::string_view name, std::string_view detail = {}) {
        ++failures_;
        line("FALLA", name, detail);
    }
    void skip(std::string_view name, std::string_view detail = {}) { line("OMIT", name, detail); }
    [[nodiscard]] int failures() const noexcept { return failures_; }

private:
    static void line(std::string_view tag, std::string_view name, std::string_view detail) {
        std::string msg = "[";
        msg += tag;
        msg += "] ";
        msg += name;
        if (!detail.empty()) {
            msg += " - ";
            msg += detail;
        }
        std::puts(msg.c_str());
    }
    int failures_ = 0;
};

/// Formatea un Error del núcleo para la salida (código + mensaje).
[[nodiscard]] std::string err_str(const Error& e) {
    return "err(" + std::to_string(static_cast<int>(e.code())) + "): " + std::string{e.message()};
}

/// Vista de bytes de solo lectura sobre una cadena (sin reinterpret_cast a la vista).
[[nodiscard]] ByteSpan to_bytes(std::string_view s) noexcept {
    return std::as_bytes(std::span<const char>{s.data(), s.size()});
}

/// Copia @p b a una std::string (para comparar/mostrar el contenido recibido).
[[nodiscard]] std::string from_bytes(ByteSpan b) {
    std::string s(b.size(), '\0');
    if (!b.empty()) {
        std::memcpy(s.data(), b.data(), b.size());
    }
    return s;
}

/// Ruta temporal única para los ficheros del arnés.
[[nodiscard]] std::string temp_path(std::string_view stem) {
    std::error_code ec;
    std::filesystem::path dir = std::filesystem::temp_directory_path(ec);
    if (ec) {
        dir = std::filesystem::path{"."};
    }
    const auto tick = std::chrono::steady_clock::now().time_since_epoch().count();
    return (dir / (std::string{stem} + "_" + std::to_string(tick) + ".tmp")).string();
}

// --- Driver de corrutinas: bombea las completions del backend hasta completar la raíz ---

/// @brief Arranca la task perezosa @p root y drena `wait_completions` hasta que termine o se agote
///   @p budget. @return true si la corrutina completó dentro del presupuesto.
[[nodiscard]] bool pump_until_done(IocpBackend& backend, std::coroutine_handle<> root,
                                   std::chrono::milliseconds budget) {
    root.resume();  // arranca la task (initial_suspend = suspend_always); corre a la 1ª suspensión
    const auto deadline = std::chrono::steady_clock::now() + budget;
    while (!root.done()) {
        if (std::chrono::steady_clock::now() >= deadline) {
            return false;
        }
        // Bloque corto y acotado: drena lo listo y deja vencer los temporizadores próximos.
        static_cast<void>(backend.wait_completions(64, std::chrono::steady_clock::now() + 20ms));
    }
    return true;
}

/// Borra @p path sin lanzar (la limpieza nunca debe abortar el arnés).
void remove_quietly(const std::string& path) {
    std::error_code ec;
    std::filesystem::remove(path, ec);
}

// --- Caso A: File (E/S bloqueante) ---

/// Ejecuta el round-trip de `File` y devuelve el mensaje de error (vacío = OK). El `File` se cierra
/// al volver de esta función (antes de borrar el fichero): en Windows no se puede borrar un fichero
/// abierto sin `FILE_SHARE_DELETE`.
[[nodiscard]] std::string run_file_ops(const std::string& path) {
    const std::string payload = "NexusMQ IOCP runtime check - File round trip 0123456789\n";

    expected<File> opened = File::open(path, File::Mode::ReadWrite);
    if (!opened) {
        return "File::open(ReadWrite): " + err_str(opened.error());
    }
    const File& f = *opened;

    if (expected<void> w = f.write_at(to_bytes(payload), 0); !w) {
        return "File::write_at: " + err_str(w.error());
    }
    if (expected<std::uint64_t> sz = f.size(); !sz || *sz != payload.size()) {
        return "File::size: " + (sz ? ("esperaba " + std::to_string(payload.size()) + ", obtuvo " +
                                       std::to_string(*sz))
                                    : err_str(sz.error()));
    }

    std::array<std::byte, 128> rbuf{};
    expected<std::size_t> n = f.read_at(MutByteSpan{rbuf.data(), payload.size()}, 0);
    if (!n || *n != payload.size() || from_bytes(ByteSpan{rbuf.data(), *n}) != payload) {
        return "File::read_at round-trip: " +
               (n ? ("leyó " + std::to_string(*n) + " bytes") : err_str(n.error()));
    }

    if (expected<void> s = f.sync(); !s) {
        return "File::sync: " + err_str(s.error());
    }

    constexpr std::uint64_t kCut = 10;
    if (expected<void> t = f.truncate(kCut); !t) {
        return "File::truncate: " + err_str(t.error());
    }
    if (expected<std::uint64_t> sz2 = f.size(); !sz2 || *sz2 != kCut) {
        return "File::size tras truncate: " +
               (sz2 ? ("esperaba 10, obtuvo " + std::to_string(*sz2)) : err_str(sz2.error()));
    }
    return {};
}

void test_file(Reporter& r) {
    const std::string path = temp_path("nexusmq_wincheck_file");
    const std::string err = run_file_ops(path);  // cierra el File al volver
    remove_quietly(path);
    if (err.empty()) {
        r.pass("File: open/write_at/read_at/size/sync/truncate (E/S bloqueante)");
    } else {
        r.fail("File", err);
    }
}

// --- Caso A': File con E/S directa (FILE_FLAG_NO_BUFFERING), si el FS la admite ---

/// Round-trip con E/S directa. Devuelve: "" = OK, "skip" = el FS no la admite, otro = error.
[[nodiscard]] std::string run_file_direct_ops(const std::string& path, std::size_t align) {
    expected<File> opened = File::open(path, File::Mode::ReadWriteDirect);
    if (!opened) {
        return "File::open(ReadWriteDirect): " + err_str(opened.error());
    }
    const File& f = *opened;
    if (!f.is_direct()) {
        return "skip";
    }

    expected<AlignedBuffer> wb = AlignedBuffer::allocate(align, align);
    expected<AlignedBuffer> rb = AlignedBuffer::allocate(align, align);
    if (!wb || !rb) {
        return "AlignedBuffer::allocate: " + (!wb ? err_str(wb.error()) : err_str(rb.error()));
    }

    MutByteSpan ws = wb->span();
    for (std::size_t i = 0; i < ws.size(); ++i) {
        ws[i] = static_cast<std::byte>(i & 0xFFU);
    }

    if (expected<void> w = f.write_at(wb->span(), 0); !w) {
        return "File::write_at (directa): " + err_str(w.error());
    }
    expected<std::size_t> n = f.read_at(rb->span(), 0);
    if (!n || *n != align || std::memcmp(rb->data(), wb->data(), align) != 0) {
        return "File::read_at (directa) round-trip: " +
               (n ? ("leyó " + std::to_string(*n) + " bytes") : err_str(n.error()));
    }
    return {};
}

void test_file_direct(Reporter& r) {
    const std::string path = temp_path("nexusmq_wincheck_direct");
    const std::size_t align = File::direct_alignment();  // 4096: búfer/offset/longitud alineados
    const std::string err = run_file_direct_ops(path, align);  // cierra el File al volver
    remove_quietly(path);
    if (err.empty()) {
        r.pass("File: E/S directa (NO_BUFFERING) round-trip alineado a " + std::to_string(align));
    } else if (err == "skip") {
        r.skip("File E/S directa: el FS no la admite (is_direct()==false)");
    } else {
        r.fail("File (directa)", err);
    }
}

// --- Caso B: IocpBackend + Socket/Listener, eco por loopback (connect/accept/recv/send) ---

/// Flujo de eco end-to-end como una sola corrutina secuencial: acepta, el cliente envía, el
/// servidor recibe y reenvía, el cliente recibe el eco; luego el servidor cierra y el cliente debe
/// ver EOF (cierre limpio). La conexión ya está establecida (connect síncrono), así que `AcceptEx`
/// recoge la conexión encolada en el backlog sin orden estricto respecto al send.
task<expected<std::string>> echo_flow(Proactor& proactor, const Listener& listener,
                                      const Socket& client, std::string request) {
    expected<Socket> accepted = co_await listener.async_accept(proactor);
    if (!accepted) {
        co_return std::unexpected(accepted.error());
    }
    Socket server = std::move(*accepted);

    if (expected<std::size_t> sent = co_await client.async_send(proactor, to_bytes(request));
        !sent) {
        co_return std::unexpected(sent.error());
    }

    std::array<std::byte, 512> sbuf{};
    expected<std::size_t> got =
        co_await server.async_recv(proactor, MutByteSpan{sbuf.data(), sbuf.size()});
    if (!got) {
        co_return std::unexpected(got.error());
    }

    if (expected<std::size_t> echoed =
            co_await server.async_send(proactor, ByteSpan{sbuf.data(), *got});
        !echoed) {
        co_return std::unexpected(echoed.error());
    }

    std::array<std::byte, 512> cbuf{};
    expected<std::size_t> reply =
        co_await client.async_recv(proactor, MutByteSpan{cbuf.data(), cbuf.size()});
    if (!reply) {
        co_return std::unexpected(reply.error());
    }
    std::string echo = from_bytes(ByteSpan{cbuf.data(), *reply});

    // Cierre limpio: el servidor cierra; un recv del cliente debe completar con 0 bytes (EOF).
    server.close();
    std::array<std::byte, 16> eofbuf{};
    expected<std::size_t> eof =
        co_await client.async_recv(proactor, MutByteSpan{eofbuf.data(), eofbuf.size()});
    if (!eof) {
        co_return std::unexpected(eof.error());
    }
    if (*eof != 0U) {
        co_return nexus::make_error(
            nexus::ErrorCode::IoError,
            "esperaba EOF (0 bytes) tras cierre del par, obtuvo " + std::to_string(*eof));
    }

    co_return echo;
}

void test_socket_echo(IocpBackend& backend, Reporter& r) {
    expected<Listener> listener = Listener::bind("127.0.0.1", 0);
    if (!listener) {
        r.fail("Listener::bind(127.0.0.1, 0)", err_str(listener.error()));
        return;
    }
    const std::uint16_t port = listener->local_port();
    if (port == 0) {
        r.fail("Listener::local_port", "devolvió 0 (sin puerto efímero)");
        return;
    }

    expected<Socket> client = Socket::connect("127.0.0.1", port);
    if (!client) {
        r.fail("Socket::connect", err_str(client.error()));
        return;
    }

    const std::string request = "ping-NexusMQ-IOCP-loopback";
    task<expected<std::string>> flow = echo_flow(backend, *listener, *client, request);
    if (!pump_until_done(backend, flow.handle(), 5s)) {
        static_cast<void>(
            flow.release());  // op en vuelo: no destruir el frame (evitar UAF al salir)
        r.fail("eco IOCP", "timeout (la corrutina no completó en 5s)");
        return;
    }

    expected<std::string> result = flow.handle().promise().result();
    if (!result) {
        r.fail("eco IOCP", err_str(result.error()));
        return;
    }
    if (*result != request) {
        r.fail("eco IOCP", "esperaba '" + request + "', obtuvo '" + *result + "'");
        return;
    }
    r.pass("Socket/Listener IOCP: eco end-to-end + cierre limpio (EOF)");
}

// --- Caso C: submit_timer ---

task<expected<void>> timer_flow(Proactor& proactor, std::chrono::milliseconds delay) {
    co_return co_await TimerAwaitable{proactor, std::chrono::steady_clock::now() + delay};
}

void test_timer(IocpBackend& backend, Reporter& r) {
    constexpr auto kDelay = 60ms;
    const auto start = std::chrono::steady_clock::now();
    task<expected<void>> flow = timer_flow(backend, kDelay);
    if (!pump_until_done(backend, flow.handle(), 5s)) {
        static_cast<void>(flow.release());
        r.fail("submit_timer", "timeout (el temporizador no venció en 5s)");
        return;
    }
    const auto elapsed = std::chrono::steady_clock::now() - start;
    if (expected<void> res = flow.handle().promise().result(); !res) {
        r.fail("submit_timer", err_str(res.error()));
        return;
    }
    if (elapsed < 40ms) {
        const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count();
        r.fail("submit_timer", "venció demasiado pronto (" + std::to_string(ms) + "ms < 40ms)");
        return;
    }
    r.pass("submit_timer: deadline vence y reanuda la corrutina");
}

}  // namespace

int main() {
    // Salida sin búfer: el arnés puede redirigirse a fichero/pipe; así cada caso se ve al instante
    // (y un eventual cuelgue/caída deja claro en qué caso ocurrió).
    static_cast<void>(std::setvbuf(stdout, nullptr, _IONBF, 0));
    std::puts("=== NexusMQ wincheck: verificacion de runtime de nexus-io (Windows / IOCP) ===");
    Reporter r;

    test_file(r);
    test_file_direct(r);

    try {
        IocpBackend backend{256};
        test_socket_echo(backend, r);
        test_timer(backend, r);
    } catch (const std::exception& e) {
        r.fail("IocpBackend (init/run)", e.what());
    }

    std::puts("");
    if (r.failures() == 0) {
        std::puts("RESULTADO: TODOS LOS CASOS PASARON");
        return 0;
    }
    std::puts(("RESULTADO: " + std::to_string(r.failures()) + " CASO(S) FALLARON").c_str());
    return r.failures();
}
