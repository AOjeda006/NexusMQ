// E/S de fichero **bloqueante** (Fase 1), con implementación por plataforma: POSIX
// (`pread`/`pwrite`/`fsync`) y Windows (Win32 `ReadFile`/`WriteFile`/`FlushFileBuffers`). El resto
// del árbol depende solo de la interfaz `File` (io/file.hpp), agnóstica de plataforma.

// O_DIRECT (E/S directa, F6) es una extensión GNU: hay que pedirla antes de <fcntl.h> (solo POSIX).
#if !defined(_WIN32) && !defined(_GNU_SOURCE)
#define _GNU_SOURCE
#endif

#include "io/file.hpp"

#include <system_error>
#include <utility>

#include "common/error.hpp"  // IWYU pragma: keep

#if defined(_WIN32)
// clang-format off
#include <windows.h>
// clang-format on
#else
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#endif

namespace nexus {

// --- Miembros especiales: portables (la liberación concreta vive en `close_fd`). ---

File::~File() {
    close_fd();
}

File::File(File&& other) noexcept
    : fd_(std::exchange(other.fd_, kInvalidHandle)), direct_(std::exchange(other.direct_, false)) {}

File& File::operator=(File&& other) noexcept {
    if (this != &other) {
        close_fd();
        fd_ = std::exchange(other.fd_, kInvalidHandle);
        direct_ = std::exchange(other.direct_, false);
    }
    return *this;
}

#if defined(_WIN32)

namespace {

// Traduce el último error de Win32 a un Error del núcleo, con la operación como contexto.
std::unexpected<Error> io_error(const char* op) {
    const auto code = static_cast<int>(::GetLastError());
    return make_error(ErrorCode::IoError,
                      std::string{op} + ": " + std::system_category().message(code));
}

// NOLINTNEXTLINE(*-reinterpret-cast): el NativeHandle alberga el HANDLE de Win32 (ADR-0021/0022).
HANDLE to_handle(NativeHandle fd) noexcept {
    return reinterpret_cast<HANDLE>(fd);
}

// Construye un OVERLAPPED con el offset de E/S posicional (equivalente a pread/pwrite).
OVERLAPPED overlapped_at(std::uint64_t offset) noexcept {
    OVERLAPPED ov = {};
    ov.Offset = static_cast<DWORD>(offset & 0xFFFFFFFFULL);
    ov.OffsetHigh = static_cast<DWORD>(offset >> 32U);
    return ov;
}

}  // namespace

void File::close_fd() noexcept {
    if (fd_ != kInvalidHandle) {
        ::CloseHandle(to_handle(fd_));
        fd_ = kInvalidHandle;
    }
    direct_ = false;
}

expected<File> File::open(const std::string& path, Mode mode) {
    const bool read_only = (mode == Mode::ReadOnly);
    const DWORD access = read_only ? GENERIC_READ : (GENERIC_READ | GENERIC_WRITE);
    // FILE_SHARE_DELETE da semántica POSIX: permite borrar/renombrar el fichero mientras sigue
    // abierto (en POSIX `unlink` de un fichero abierto quita la entrada y el inodo persiste hasta
    // el último `close`). Sin él, Windows rechaza `DeleteFile`/`std::filesystem::remove` con «el
    // proceso no puede acceder al archivo porque está siendo utilizado» mientras viva el handle. El
    // núcleo y los tests cuentan con esa semántica (rotación de segmentos, limpieza de temporales).
    // (ADR-0028)
    const DWORD share = FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE;
    const DWORD disposition =
        read_only ? OPEN_EXISTING : OPEN_ALWAYS;  // OPEN_ALWAYS = crea si falta
    DWORD flags = FILE_ATTRIBUTE_NORMAL;
    bool direct = false;
    if (mode == Mode::ReadWriteDirect) {
        // FILE_FLAG_NO_BUFFERING es el equivalente Win32 a O_DIRECT: exige alineación a sector en
        // búfer/offset/longitud (cubierta por AlignedBuffer/direct_alignment()).
        flags |= FILE_FLAG_NO_BUFFERING | FILE_FLAG_WRITE_THROUGH;
        direct = true;
    }

    const HANDLE handle =
        ::CreateFileA(path.c_str(), access, share, nullptr, disposition, flags, nullptr);
    if (handle == INVALID_HANDLE_VALUE) {
        return io_error("CreateFile");
    }
    // NOLINTNEXTLINE(*-reinterpret-cast): el HANDLE se guarda en el NativeHandle de ancho de
    // puntero.
    return File{reinterpret_cast<NativeHandle>(handle), direct};
}

expected<std::size_t> File::read_at(MutByteSpan dst, std::uint64_t offset) const {
    std::size_t total = 0;
    while (total < dst.size()) {
        OVERLAPPED ov = overlapped_at(offset + total);
        DWORD read = 0;
        const DWORD want = static_cast<DWORD>(dst.size() - total);
        if (::ReadFile(to_handle(fd_), dst.data() + total, want, &read, &ov) == FALSE) {
            if (::GetLastError() == ERROR_HANDLE_EOF) {
                break;  // EOF
            }
            return io_error("ReadFile");
        }
        if (read == 0) {
            break;  // EOF
        }
        total += static_cast<std::size_t>(read);
    }
    return total;
}

expected<void> File::write_at(ByteSpan data, std::uint64_t offset) const {
    std::size_t total = 0;
    while (total < data.size()) {
        OVERLAPPED ov = overlapped_at(offset + total);
        DWORD written = 0;
        const DWORD want = static_cast<DWORD>(data.size() - total);
        if (::WriteFile(to_handle(fd_), data.data() + total, want, &written, &ov) == FALSE) {
            return io_error("WriteFile");
        }
        total += static_cast<std::size_t>(written);
    }
    return {};
}

expected<void> File::sync() const {
    if (::FlushFileBuffers(to_handle(fd_)) == FALSE) {
        return io_error("FlushFileBuffers");
    }
    return {};
}

expected<void> File::truncate(std::uint64_t length) const {
    LARGE_INTEGER pos = {};
    pos.QuadPart = static_cast<LONGLONG>(length);
    if (::SetFilePointerEx(to_handle(fd_), pos, nullptr, FILE_BEGIN) == FALSE) {
        return io_error("SetFilePointerEx");
    }
    if (::SetEndOfFile(to_handle(fd_)) == FALSE) {
        return io_error("SetEndOfFile");
    }
    return {};
}

expected<std::uint64_t> File::size() const {
    LARGE_INTEGER result = {};
    if (::GetFileSizeEx(to_handle(fd_), &result) == FALSE) {
        return io_error("GetFileSizeEx");
    }
    return static_cast<std::uint64_t>(result.QuadPart);
}

#else  // POSIX

namespace {

constexpr int kCreateMode = 0644;  // rw-r--r-- al crear el fichero

// Traduce el errno actual a un Error del núcleo, con la operación como contexto.
std::unexpected<Error> io_error(const char* op) {
    return make_error(ErrorCode::IoError,
                      std::string{op} + ": " + std::generic_category().message(errno));
}

}  // namespace

void File::close_fd() noexcept {
    if (fd_ != kInvalidHandle) {
        ::close(fd_);
        fd_ = kInvalidHandle;
    }
    direct_ = false;
}

expected<File> File::open(const std::string& path, Mode mode) {
    if (mode == Mode::ReadOnly) {
        const int fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) {
            return io_error("open");
        }
        return File{fd, /*direct=*/false};
    }

    const int base_flags = O_RDWR | O_CREAT;
    if (mode == Mode::ReadWriteDirect) {
        // Intenta E/S directa; si el FS no la admite (EINVAL), recae a E/S con búfer (sin error).
        const int fd = ::open(path.c_str(), base_flags | O_DIRECT, kCreateMode);
        if (fd >= 0) {
            return File{fd, /*direct=*/true};
        }
        if (errno != EINVAL) {
            return io_error("open");
        }
    }

    const int fd = ::open(path.c_str(), base_flags, kCreateMode);
    if (fd < 0) {
        return io_error("open");
    }
    return File{fd, /*direct=*/false};
}

expected<std::size_t> File::read_at(MutByteSpan dst, std::uint64_t offset) const {
    std::size_t total = 0;
    while (total < dst.size()) {
        const ssize_t n = ::pread(fd_, dst.data() + total, dst.size() - total,
                                  static_cast<off_t>(offset + total));
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            return io_error("pread");
        }
        if (n == 0) {
            break;  // EOF
        }
        total += static_cast<std::size_t>(n);
    }
    return total;
}

expected<void> File::write_at(ByteSpan data, std::uint64_t offset) const {
    std::size_t total = 0;
    while (total < data.size()) {
        const ssize_t n = ::pwrite(fd_, data.data() + total, data.size() - total,
                                   static_cast<off_t>(offset + total));
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            return io_error("pwrite");
        }
        total += static_cast<std::size_t>(n);
    }
    return {};
}

expected<void> File::sync() const {
    while (::fsync(fd_) < 0) {
        if (errno == EINTR) {
            continue;
        }
        return io_error("fsync");
    }
    return {};
}

expected<void> File::truncate(std::uint64_t length) const {
    while (::ftruncate(fd_, static_cast<off_t>(length)) < 0) {
        if (errno == EINTR) {
            continue;
        }
        return io_error("ftruncate");
    }
    return {};
}

expected<std::uint64_t> File::size() const {
    struct stat st = {};
    if (::fstat(fd_, &st) < 0) {
        return io_error("fstat");
    }
    return static_cast<std::uint64_t>(st.st_size);
}

#endif  // _WIN32

}  // namespace nexus
