/// @file   io/file.hpp
/// @brief  File: envoltura RAII sobre un descriptor de fichero (Fase 1: E/S bloqueante).
/// @ingroup io

#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

#include "common/bytes.hpp"
#include "common/error.hpp"
#include "io/aligned_buffer.hpp"  // kDirectAlignment

namespace nexus {

/// @brief Envoltura RAII sobre un descriptor de fichero POSIX. Afinidad: REACTOR-LOCAL.
/// @details **Solo movible**: posee el descriptor y lo cierra en el destructor (un
///   recurso del SO no debe fugarse). Fase 1: E/S **bloqueante** (`pread`/`pwrite`/
///   `fsync`); la variante asíncrona (io_uring) llega en 1b.
/// @invariant is_open() <=> el descriptor es válido.
class File {
public:
    /// Modo de apertura.
    enum class Mode : std::uint8_t {
        ReadOnly,         ///< Abre un fichero existente para lectura.
        ReadWrite,        ///< Abre o crea para lectura y escritura.
        ReadWriteDirect,  ///< Como `ReadWrite` pero con E/S directa (`O_DIRECT`), si el FS lo
                          ///< admite.
    };

    File() = default;
    ~File();
    File(File&& other) noexcept;
    File& operator=(File&& other) noexcept;
    File(const File&) = delete;
    File& operator=(const File&) = delete;

    /// @brief Abre @p path en el modo dado (crea el fichero si `ReadWrite`/`ReadWriteDirect`).
    /// @details Con `ReadWriteDirect` intenta `O_DIRECT`; si el sistema de ficheros no lo admite
    ///   (`EINVAL`), **recae** a E/S con búfer y deja `is_direct()` en `false` (sin error). En modo
    ///   directo, los búferes (`AlignedBuffer`), el offset y la longitud deben estar alineados a
    ///   `direct_alignment()`.
    /// @return El fichero abierto, o `IoError` con el `errno` traducido.
    [[nodiscard]] static expected<File> open(const std::string& path, Mode mode);

    /// ¿Está el fichero abierto en modo E/S directa (`O_DIRECT` efectivo)?
    [[nodiscard]] bool is_direct() const noexcept { return direct_; }

    /// Alineación requerida por la E/S directa (búfer/offset/longitud).
    [[nodiscard]] static constexpr std::size_t direct_alignment() noexcept;

    /// @brief Lee en @p dst desde @p offset; devuelve los bytes leídos (0 = EOF).
    [[nodiscard]] expected<std::size_t> read_at(MutByteSpan dst, std::uint64_t offset) const;

    /// @brief Escribe @p data en @p offset (escribe todo, reintentando parciales).
    [[nodiscard]] expected<void> write_at(ByteSpan data, std::uint64_t offset) const;

    /// @brief Fuerza la durabilidad a disco (`fsync`).
    [[nodiscard]] expected<void> sync() const;

    /// @brief Trunca (o extiende con ceros) el fichero a @p length bytes (`ftruncate`).
    [[nodiscard]] expected<void> truncate(std::uint64_t length) const;

    /// @brief Tamaño actual del fichero en bytes.
    [[nodiscard]] expected<std::uint64_t> size() const;

    [[nodiscard]] bool is_open() const noexcept { return fd_ >= 0; }

private:
    File(int fd, bool direct) noexcept : fd_(fd), direct_(direct) {}
    void close_fd() noexcept;

    int fd_ = -1;
    bool direct_ = false;  ///< `true` si se abrió con `O_DIRECT` efectivo.
};

constexpr std::size_t File::direct_alignment() noexcept {
    return kDirectAlignment;
}

}  // namespace nexus
