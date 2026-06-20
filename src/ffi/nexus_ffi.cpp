/// @file   ffi/nexus_ffi.cpp
/// @brief  Implementación de la ABI C de NexusMQ (puente hacia el núcleo C++) — F9.
/// @ingroup ffi

#include "ffi/nexus_ffi.h"

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>

#include "common/bytes.hpp"
#include "common/crc32c.hpp"
#include "common/version.hpp"
#include "telemetry/tracing.hpp"

extern "C" {

const char* nexus_version() {
    // Copia estable terminada en NUL del string_view de versión (vive durante todo el proceso).
    static const std::string cached_version{nexus::version()};
    return cached_version.c_str();
}

std::uint32_t nexus_crc32c(const std::uint8_t* data, std::size_t len) {
    if (data == nullptr || len == 0) {
        return nexus::crc32c(nexus::ByteSpan{});
    }
    // NOLINTNEXTLINE(*-reinterpret-cast): puente C↔C++ sobre un búfer de bytes opaco.
    const auto* bytes = reinterpret_cast<const std::byte*>(data);
    return nexus::crc32c(nexus::ByteSpan{bytes, len});
}

int nexus_traceparent_format(std::uint64_t trace_hi, std::uint64_t trace_lo, std::uint64_t span_id,
                             std::uint8_t flags, char* out, std::size_t out_cap) {
    nexus::SpanContext ctx;
    ctx.trace_id = nexus::TraceId{.hi = trace_hi, .lo = trace_lo};
    ctx.span_id = nexus::SpanId{.value = span_id};
    ctx.flags = flags;
    const std::string header = nexus::format_traceparent(ctx);
    if (out == nullptr || out_cap < header.size() + 1) {
        return -1;
    }
    std::memcpy(out, header.data(), header.size());
    out[header.size()] = '\0';
    return 0;
}

int nexus_traceparent_parse(const char* header, std::uint64_t* trace_hi, std::uint64_t* trace_lo,
                            std::uint64_t* span_id, std::uint8_t* flags) {
    if (header == nullptr) {
        return -1;
    }
    const nexus::expected<nexus::SpanContext> ctx = nexus::parse_traceparent(header);
    if (!ctx) {
        return -1;
    }
    if (trace_hi != nullptr) {
        *trace_hi = ctx->trace_id.hi;
    }
    if (trace_lo != nullptr) {
        *trace_lo = ctx->trace_id.lo;
    }
    if (span_id != nullptr) {
        *span_id = ctx->span_id.value;
    }
    if (flags != nullptr) {
        *flags = ctx->flags;
    }
    return 0;
}

}  // extern "C"
