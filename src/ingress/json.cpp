/// @file   ingress/json.cpp
/// @brief  Implementación de JsonWriter (escapado + comas).
/// @ingroup ingress

#include "ingress/json.hpp"

#include <format>

namespace nexus {

namespace {

void append_escaped(std::string& out, std::string_view text) {
    out += '"';
    for (const char c : text) {
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

}  // namespace

void JsonWriter::before_value() {
    if (expecting_value_) {
        expecting_value_ = false;
        return;  // el valor pertenece a una clave ya escrita (su coma se gestionó en `before_key`).
    }
    if (!first_.empty()) {
        if (!first_.back()) {
            out_ += ',';
        }
        first_.back() = false;
    }
}

void JsonWriter::before_key() {
    if (!first_.empty()) {
        if (!first_.back()) {
            out_ += ',';
        }
        first_.back() = false;
    }
}

JsonWriter& JsonWriter::begin_object() {
    before_value();
    out_ += '{';
    first_.push_back(true);
    return *this;
}

JsonWriter& JsonWriter::end_object() {
    out_ += '}';
    first_.pop_back();
    return *this;
}

JsonWriter& JsonWriter::begin_array() {
    before_value();
    out_ += '[';
    first_.push_back(true);
    return *this;
}

JsonWriter& JsonWriter::end_array() {
    out_ += ']';
    first_.pop_back();
    return *this;
}

JsonWriter& JsonWriter::key(std::string_view name) {
    before_key();
    append_escaped(out_, name);
    out_ += ':';
    expecting_value_ = true;
    return *this;
}

JsonWriter& JsonWriter::value(std::string_view text) {
    before_value();
    append_escaped(out_, text);
    return *this;
}

JsonWriter& JsonWriter::value(std::int64_t number) {
    before_value();
    out_ += std::to_string(number);
    return *this;
}

JsonWriter& JsonWriter::value(double number) {
    before_value();
    out_ += std::format("{}", number);
    return *this;
}

JsonWriter& JsonWriter::value(bool flag) {
    before_value();
    out_ += flag ? "true" : "false";
    return *this;
}

JsonWriter& JsonWriter::null_value() {
    before_value();
    out_ += "null";
    return *this;
}

}  // namespace nexus
