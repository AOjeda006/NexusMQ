/// @file   telemetry/logging.cpp
/// @brief  Implementación del logger estructurado JSON (§7.6).
/// @ingroup telemetry

#include "telemetry/logging.hpp"

#include <ctime>
#include <format>

namespace nexus {

namespace {

/// Escapa una cadena para JSON (comillas, barra invertida y caracteres de control).
void append_json_string(std::string& out, std::string_view value) {
    out += '"';
    for (const char c : value) {
        switch (c) {
            case '"':
                out += "\\\"";
                break;
            case '\\':
                out += "\\\\";
                break;
            case '\n':
                out += "\\n";
                break;
            case '\r':
                out += "\\r";
                break;
            case '\t':
                out += "\\t";
                break;
            default:
                if (static_cast<unsigned char>(c) < 0x20) {
                    out += std::format("\\u{:04x}", static_cast<unsigned>(c));
                } else {
                    out += c;
                }
        }
    }
    out += '"';
}

/// Serializa el valor de un campo al tipo JSON correspondiente.
void append_json_value(std::string& out,
                       const std::variant<std::string, std::int64_t, double, bool>& value) {
    if (const auto* s = std::get_if<std::string>(&value)) {
        append_json_string(out, *s);
    } else if (const auto* i = std::get_if<std::int64_t>(&value)) {
        out += std::to_string(*i);
    } else if (const auto* d = std::get_if<double>(&value)) {
        out += std::format("{}", *d);
    } else if (const auto* b = std::get_if<bool>(&value)) {
        out += *b ? "true" : "false";
    }
}

/// Formatea un instante en RFC 3339 UTC con milisegundos (`YYYY-MM-DDTHH:MM:SS.mmmZ`).
std::string format_rfc3339(std::chrono::system_clock::time_point tp) {
    using namespace std::chrono;
    const auto secs = time_point_cast<seconds>(tp);
    const auto millis = duration_cast<milliseconds>(tp - secs).count();
    const std::time_t tt = system_clock::to_time_t(secs);
    std::tm tm{};
    gmtime_r(&tt, &tm);
    return std::format("{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:03}Z", tm.tm_year + 1900,
                       tm.tm_mon + 1, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec,
                       static_cast<int>(millis));
}

}  // namespace

Logger::Logger(std::ostream& sink, LogLevel min_level, ClockFn clock)
    : sink_(sink),
      min_level_(min_level),
      clock_(clock ? std::move(clock) : [] { return std::chrono::system_clock::now(); }) {}

std::string_view Logger::level_name(LogLevel level) noexcept {
    switch (level) {
        case LogLevel::Trace:
            return "trace";
        case LogLevel::Debug:
            return "debug";
        case LogLevel::Info:
            return "info";
        case LogLevel::Warn:
            return "warn";
        case LogLevel::Error:
            return "error";
    }
    return "info";
}

void Logger::log(LogLevel level, std::string_view message, std::span<const Field> fields) {
    if (!enabled(level)) {
        return;
    }

    std::string line = R"({"ts":")";
    line += format_rfc3339(clock_());
    line += R"(","level":")";
    line += level_name(level);
    line += R"(","msg":)";
    append_json_string(line, message);

    const auto append_field = [&line](const Field& f) {
        line += ',';
        append_json_string(line, f.key);
        line += ':';
        append_json_value(line, f.value);
    };
    for (const Field& f : context_) {
        append_field(f);
    }
    for (const Field& f : fields) {
        append_field(f);
    }
    line += "}\n";

    const std::scoped_lock lock{mutex_};
    sink_ << line;
}

}  // namespace nexus
