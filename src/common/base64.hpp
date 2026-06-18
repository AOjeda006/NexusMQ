/// @file   base64.hpp
/// @brief  Codec **base64url** sin relleno (RFC 4648 §5), base de la verificación JWT.
/// @ingroup common

#pragma once

#include <cstddef>
#include <string>
#include <string_view>
#include <vector>

#include "common/bytes.hpp"
#include "common/error.hpp"

namespace nexus {

/// @brief Codifica @p data en base64url **sin relleno** (`=`), con el alfabeto URL-safe (`-`/`_`).
/// @details Es la codificación que usan los JWT (RFC 7515): cada segmento (cabecera, payload,
/// firma)
///   va en base64url sin `=`. Afinidad: función pura.
[[nodiscard]] std::string base64url_encode(ByteSpan data);

/// @brief Decodifica @p text de base64url a bytes; tolera relleno `=` final opcional.
/// @details Rechaza caracteres fuera del alfabeto URL-safe y longitudes imposibles (`len % 4 ==
/// 1`).
///   No exige relleno (los JWT no lo llevan). @return los bytes decodificados o `InvalidArgument`.
[[nodiscard]] expected<std::vector<std::byte>> base64url_decode(std::string_view text);

}  // namespace nexus
