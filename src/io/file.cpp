#include "io/file.hpp"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <system_error>
#include <utility>

#include "common/error.hpp"  // IWYU pragma: keep

namespace nexus {
namespace {

constexpr int kCreateMode = 0644;  // rw-r--r-- al crear el fichero

// Traduce el errno actual a un Error del núcleo, con la operación como contexto.
std::unexpected<Error> io_error(const char* op) {
    return make_error(ErrorCode::IoError,
                      std::string{op} + ": " + std::generic_category().message(errno));
}

}  // namespace

File::~File() {
    close_fd();
}

File::File(File&& other) noexcept : fd_(std::exchange(other.fd_, -1)) {}

File& File::operator=(File&& other) noexcept {
    if (this != &other) {
        close_fd();
        fd_ = std::exchange(other.fd_, -1);
    }
    return *this;
}

void File::close_fd() noexcept {
    if (fd_ >= 0) {
        ::close(fd_);
        fd_ = -1;
    }
}

expected<File> File::open(const std::string& path, Mode mode) {
    const int flags = (mode == Mode::ReadOnly) ? O_RDONLY : (O_RDWR | O_CREAT);
    const int fd = ::open(path.c_str(), flags, kCreateMode);
    if (fd < 0) {
        return io_error("open");
    }
    return File{fd};
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

}  // namespace nexus
