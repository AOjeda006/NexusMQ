/// @file   ingress/pagination.hpp
/// @brief  Paginación de colecciones del REST admin (`page`/`size`) (§7.6).
/// @ingroup ingress

#pragma once

#include <cstddef>
#include <string_view>

#include "common/error.hpp"

namespace nexus {

/// @brief Página solicitada (1-based). Afinidad: INMUTABLE.
struct Page {
    std::size_t number = 1;  ///< número de página, 1-based.
    std::size_t size = 20;   ///< elementos por página.

    /// Índice del primer elemento de la página (0-based).
    [[nodiscard]] std::size_t offset() const noexcept { return (number - 1) * size; }
};

/// @brief Límites de paginación. Afinidad: INMUTABLE.
struct PaginationLimits {
    std::size_t default_size = 20;
    std::size_t max_size = 100;
};

/// @brief Parsea `page`/`size` de la *query string* (§7.6: paginación obligatoria en colecciones).
/// @details Defensivo (`expected`): `page` por defecto 1 (debe ser `>= 1`); `size` por defecto
///   `default_size` y acotado a `[1, max_size]`. Rechaza valores no numéricos o fuera de rango con
///   `InvalidArgument`.
[[nodiscard]] expected<Page> parse_pagination(std::string_view query,
                                              const PaginationLimits& limits = {});

}  // namespace nexus
