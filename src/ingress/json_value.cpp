/// @file   ingress/json_value.cpp
/// @brief  Implementación del lector JSON (parser recursivo descendente, RFC 8259).
/// @ingroup ingress

#include "ingress/json_value.hpp"

#include <cstdlib>
#include <optional>
#include <string>

namespace nexus {

const JsonValue* JsonValue::find(std::string_view key) const {
    if (!is_object()) {
        return nullptr;
    }
    for (const auto& [name, value] : as_object()) {
        if (name == key) {
            return &value;
        }
    }
    return nullptr;
}

namespace {

/// Parser recursivo descendente sobre un `string_view`. Afinidad: REACTOR-LOCAL (vive en la pila).
class Parser {
public:
    explicit Parser(std::string_view text) noexcept : text_(text) {}

    expected<JsonValue> parse_document() {
        skip_ws();
        auto value = parse_value(0);
        if (!value) {
            return value;
        }
        skip_ws();
        if (pos_ != text_.size()) {
            return fail("contenido sobrante tras el valor JSON");
        }
        return value;
    }

private:
    [[nodiscard]] std::unexpected<Error> fail(std::string_view what) const {
        return make_error(
            ErrorCode::InvalidArgument,
            "JSON inválido en byte " + std::to_string(pos_) + ": " + std::string{what});
    }

    [[nodiscard]] bool eof() const noexcept { return pos_ >= text_.size(); }
    [[nodiscard]] char peek() const noexcept { return text_[pos_]; }

    void skip_ws() noexcept {
        while (!eof()) {
            const char character = peek();
            if (character != ' ' && character != '\t' && character != '\n' && character != '\r') {
                break;
            }
            ++pos_;
        }
    }

    // NOLINTNEXTLINE(misc-no-recursion): descenso recursivo acotado por kJsonMaxDepth.
    expected<JsonValue> parse_value(int depth) {
        if (depth > kJsonMaxDepth) {
            return fail("anidamiento JSON demasiado profundo");
        }
        if (eof()) {
            return fail("se esperaba un valor");
        }
        switch (peek()) {
            case '{':
                return parse_object(depth);
            case '[':
                return parse_array(depth);
            case '"':
                return parse_string_value();
            case 't':
            case 'f':
                return parse_bool();
            case 'n':
                return parse_null();
            default:
                return parse_number();
        }
    }

    // NOLINTNEXTLINE(misc-no-recursion): descenso recursivo acotado por kJsonMaxDepth.
    expected<JsonValue> parse_object(int depth) {
        ++pos_;  // consume '{'.
        JsonValue::Object members;
        skip_ws();
        if (!eof() && peek() == '}') {
            ++pos_;
            return JsonValue{std::move(members)};
        }
        while (true) {
            skip_ws();
            if (eof() || peek() != '"') {
                return fail("se esperaba una clave de cadena");
            }
            auto key = parse_string_raw();
            if (!key) {
                return std::unexpected{key.error()};
            }
            skip_ws();
            if (eof() || peek() != ':') {
                return fail("se esperaba ':'");
            }
            ++pos_;
            skip_ws();
            auto value = parse_value(depth + 1);
            if (!value) {
                return value;
            }
            members.emplace_back(std::move(*key), std::move(*value));
            skip_ws();
            if (eof()) {
                return fail("objeto sin cerrar");
            }
            if (peek() == ',') {
                ++pos_;
                continue;
            }
            if (peek() == '}') {
                ++pos_;
                return JsonValue{std::move(members)};
            }
            return fail("se esperaba ',' o '}'");
        }
    }

    // NOLINTNEXTLINE(misc-no-recursion): descenso recursivo acotado por kJsonMaxDepth.
    expected<JsonValue> parse_array(int depth) {
        ++pos_;  // consume '['.
        JsonValue::Array items;
        skip_ws();
        if (!eof() && peek() == ']') {
            ++pos_;
            return JsonValue{std::move(items)};
        }
        while (true) {
            skip_ws();
            auto value = parse_value(depth + 1);
            if (!value) {
                return value;
            }
            items.push_back(std::move(*value));
            skip_ws();
            if (eof()) {
                return fail("array sin cerrar");
            }
            if (peek() == ',') {
                ++pos_;
                continue;
            }
            if (peek() == ']') {
                ++pos_;
                return JsonValue{std::move(items)};
            }
            return fail("se esperaba ',' o ']'");
        }
    }

    expected<JsonValue> parse_string_value() {
        auto text = parse_string_raw();
        if (!text) {
            return std::unexpected{text.error()};
        }
        return JsonValue{std::move(*text)};
    }

    expected<JsonValue> parse_bool() {
        if (text_.substr(pos_).starts_with("true")) {
            pos_ += 4;
            return JsonValue{true};
        }
        if (text_.substr(pos_).starts_with("false")) {
            pos_ += 5;
            return JsonValue{false};
        }
        return fail("literal booleano inválido");
    }

    expected<JsonValue> parse_null() {
        if (text_.substr(pos_).starts_with("null")) {
            pos_ += 4;
            return JsonValue{};
        }
        return fail("literal nulo inválido");
    }

    /// Valida la gramática de número JSON, extrae el token y lo convierte con `strtod`.
    expected<JsonValue> parse_number() {
        const std::size_t start = pos_;
        if (!eof() && peek() == '-') {
            ++pos_;
        }
        if (!consume_integer_part()) {
            return fail("número inválido");
        }
        consume_fraction();
        if (!consume_exponent()) {
            return fail("exponente inválido");
        }
        const std::string token{text_.substr(start, pos_ - start)};
        return JsonValue{std::strtod(token.c_str(), nullptr)};
    }

    bool consume_integer_part() noexcept {
        if (eof() || !is_digit(peek())) {
            return false;
        }
        if (peek() == '0') {
            ++pos_;  // un solo '0' (sin ceros a la izquierda).
            return true;
        }
        while (!eof() && is_digit(peek())) {
            ++pos_;
        }
        return true;
    }

    void consume_fraction() noexcept {
        if (eof() || peek() != '.') {
            return;
        }
        ++pos_;
        while (!eof() && is_digit(peek())) {
            ++pos_;
        }
    }

    bool consume_exponent() noexcept {
        if (eof() || (peek() != 'e' && peek() != 'E')) {
            return true;
        }
        ++pos_;
        if (!eof() && (peek() == '+' || peek() == '-')) {
            ++pos_;
        }
        if (eof() || !is_digit(peek())) {
            return false;
        }
        while (!eof() && is_digit(peek())) {
            ++pos_;
        }
        return true;
    }

    static bool is_digit(char character) noexcept { return character >= '0' && character <= '9'; }

    /// Parsea una cadena JSON (con la comilla de apertura en `pos_`) resolviendo los escapes.
    expected<std::string> parse_string_raw() {
        ++pos_;  // consume la comilla de apertura.
        std::string out;
        while (!eof()) {
            const char character = text_[pos_++];
            if (character == '"') {
                return out;
            }
            if (character == '\\') {
                if (!append_escape(out)) {
                    return std::unexpected{fail("escape inválido").error()};
                }
                continue;
            }
            out.push_back(character);
        }
        return std::unexpected{fail("cadena sin cerrar").error()};
    }

    /// Decodifica un escape (`pos_` justo tras la barra) y lo añade a @p out. false si es inválido.
    bool append_escape(std::string& out) {
        if (eof()) {
            return false;
        }
        const char escape = text_[pos_++];
        switch (escape) {
            case '"':
                out.push_back('"');
                return true;
            case '\\':
                out.push_back('\\');
                return true;
            case '/':
                out.push_back('/');
                return true;
            case 'b':
                out.push_back('\b');
                return true;
            case 'f':
                out.push_back('\f');
                return true;
            case 'n':
                out.push_back('\n');
                return true;
            case 'r':
                out.push_back('\r');
                return true;
            case 't':
                out.push_back('\t');
                return true;
            case 'u':
                return append_unicode_escape(out);
            default:
                return false;
        }
    }

    /// Decodifica `\uXXXX` (con pares subrogados) a UTF-8 y lo añade a @p out.
    bool append_unicode_escape(std::string& out) {
        auto first = read_hex4();
        if (!first) {
            return false;
        }
        std::uint32_t code_point = *first;
        if (code_point >= 0xD800 && code_point <= 0xDBFF) {  // alto subrogado: requiere pareja.
            if (!text_.substr(pos_).starts_with("\\u")) {
                return false;
            }
            pos_ += 2;
            auto second = read_hex4();
            if (!second || *second < 0xDC00 || *second > 0xDFFF) {
                return false;
            }
            code_point = 0x10000 + ((code_point - 0xD800) << 10) + (*second - 0xDC00);
        }
        append_utf8(out, code_point);
        return true;
    }

    /// Lee 4 dígitos hexadecimales desde `pos_`; los consume. nullopt si no son válidos.
    std::optional<std::uint32_t> read_hex4() noexcept {
        if (pos_ + 4 > text_.size()) {
            return std::nullopt;
        }
        std::uint32_t value = 0;
        for (int i = 0; i < 4; ++i) {
            const int digit = hex_digit(text_[pos_++]);
            if (digit < 0) {
                return std::nullopt;
            }
            value = (value << 4) | static_cast<std::uint32_t>(digit);
        }
        return value;
    }

    static int hex_digit(char character) noexcept {
        if (character >= '0' && character <= '9') {
            return character - '0';
        }
        if (character >= 'a' && character <= 'f') {
            return character - 'a' + 10;
        }
        if (character >= 'A' && character <= 'F') {
            return character - 'A' + 10;
        }
        return -1;
    }

    /// Codifica @p code_point en UTF-8 y lo añade a @p out.
    static void append_utf8(std::string& out, std::uint32_t code_point) {
        if (code_point <= 0x7F) {
            out.push_back(static_cast<char>(code_point));
        } else if (code_point <= 0x7FF) {
            out.push_back(static_cast<char>(0xC0 | (code_point >> 6)));
            out.push_back(static_cast<char>(0x80 | (code_point & 0x3F)));
        } else if (code_point <= 0xFFFF) {
            out.push_back(static_cast<char>(0xE0 | (code_point >> 12)));
            out.push_back(static_cast<char>(0x80 | ((code_point >> 6) & 0x3F)));
            out.push_back(static_cast<char>(0x80 | (code_point & 0x3F)));
        } else {
            out.push_back(static_cast<char>(0xF0 | (code_point >> 18)));
            out.push_back(static_cast<char>(0x80 | ((code_point >> 12) & 0x3F)));
            out.push_back(static_cast<char>(0x80 | ((code_point >> 6) & 0x3F)));
            out.push_back(static_cast<char>(0x80 | (code_point & 0x3F)));
        }
    }

    std::string_view text_;
    std::size_t pos_ = 0;
};

}  // namespace

expected<JsonValue> parse_json(std::string_view text) {
    Parser parser{text};
    return parser.parse_document();
}

}  // namespace nexus
