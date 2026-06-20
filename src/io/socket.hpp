/// @file   io/socket.hpp
/// @brief  Socket/Listener: envoltura RAII de sockets TCP con E/S asíncrona vía Proactor.
/// @ingroup io

#pragma once

#include <cstddef>
#include <cstdint>
#include <string_view>

#include "common/bytes.hpp"
#include "common/error.hpp"
#include "common/task.hpp"
#include "common/types.hpp"  // NativeHandle, kInvalidHandle
#include "io/proactor.hpp"

namespace nexus {

/// @brief Socket TCP conectado, con cierre RAII y E/S asíncrona por `Proactor`. REACTOR-LOCAL.
/// @details **Solo movible**: posee el descriptor y lo cierra al destruirse. La E/S es asíncrona
///   (io_uring `recv`/`send` tras el puerto `Proactor`): cada operación devuelve un `task` que se
///   reanuda en la completion. La conexión (accept/connect) es plano de control; el transporte de
///   datos, hot-path asíncrono.
/// @invariant `is_open()` ⇔ `fd_ >= 0`. El socket debe sobrevivir a sus operaciones en vuelo.
class Socket {
public:
    Socket() = default;
    /// Adopta @p fd (ya conectado), tomando su propiedad.
    explicit Socket(NativeHandle fd) noexcept : fd_(fd) {}

    /// @brief Conecta (bloqueante, plano de control) a @p host (IPv4 punteada) : @p port.
    /// @details La conexión es plano de control; el transporte de datos posterior es async. Pensado
    ///   para el cliente y los tests. @return el socket conectado o `IoError`/`InvalidArgument`.
    [[nodiscard]] static expected<Socket> connect(std::string_view host, std::uint16_t port);
    ~Socket();
    Socket(Socket&& other) noexcept;
    Socket& operator=(Socket&& other) noexcept;
    Socket(const Socket&) = delete;
    Socket& operator=(const Socket&) = delete;

    /// @brief Recibe en @p buffer; produce los bytes recibidos (`0` = el par cerró la conexión).
    task<expected<std::size_t>> async_recv(Proactor& proactor, MutByteSpan buffer) const;
    /// @brief Envía @p data en **una** operación; produce los bytes enviados (puede ser parcial).
    task<expected<std::size_t>> async_send(Proactor& proactor, ByteSpan data) const;

    /// Activa/desactiva `TCP_NODELAY` (algoritmo de Nagle). Mejor esfuerzo (ignora el fallo).
    void set_nodelay(bool enabled) const;
    void close() noexcept;
    [[nodiscard]] NativeHandle fd() const noexcept { return fd_; }
    [[nodiscard]] bool is_open() const noexcept { return fd_ != kInvalidHandle; }

private:
    NativeHandle fd_ = kInvalidHandle;
};

/// @brief Socket de escucha TCP con cierre RAII y `accept` asíncrono. Afinidad: REACTOR-LOCAL.
/// @details `bind` (plano de control) crea, enlaza y pone a escuchar el socket; `async_accept`
///   acepta conexiones de forma asíncrona vía `Proactor`, produciendo un `Socket` conectado.
/// @invariant `is_open()` ⇔ `fd_ >= 0`.
class Listener {
public:
    Listener() = default;
    ~Listener();
    Listener(Listener&& other) noexcept;
    Listener& operator=(Listener&& other) noexcept;
    Listener(const Listener&) = delete;
    Listener& operator=(const Listener&) = delete;

    /// @brief Crea y enlaza un socket de escucha en @p host (IPv4; vacío = todas) y @p port
    ///   (`0` = puerto efímero), con la cola de @p backlog. @return el listener o `IoError`.
    [[nodiscard]] static expected<Listener> bind(std::string_view host, std::uint16_t port,
                                                 int backlog = 1024);

    /// @brief Acepta una conexión entrante; produce el `Socket` conectado.
    task<expected<Socket>> async_accept(Proactor& proactor) const;

    /// Puerto local efectivamente enlazado (útil con puerto efímero); `0` si no está enlazado.
    [[nodiscard]] std::uint16_t local_port() const;
    void close() noexcept;
    [[nodiscard]] NativeHandle fd() const noexcept { return fd_; }
    [[nodiscard]] bool is_open() const noexcept { return fd_ != kInvalidHandle; }

private:
    explicit Listener(NativeHandle fd) noexcept : fd_(fd) {}
    NativeHandle fd_ = kInvalidHandle;
};

}  // namespace nexus
