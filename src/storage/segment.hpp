/// @file   storage/segment.hpp
/// @brief  Segment: un tramo del log append-only (.log + .index).
/// @ingroup storage

#pragma once

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <optional>

#include "common/error.hpp"
#include "common/record.hpp"
#include "common/types.hpp"
#include "io/file.hpp"
#include "storage/fetch_result.hpp"
#include "storage/index.hpp"
#include "storage/segment_crypto.hpp"

namespace nexus {

/// @brief Un tramo contiguo del log de una partición: fichero `.log` + su `.index`.
/// @details Afinidad: REACTOR-LOCAL (no es thread-safe). El `.log` es **append-only**:
///   los batches se escriben en orden por su offset y nunca se modifican. El `.index`
///   (SparseIndex) ancla ~cada N bytes un offset a su posición, para *seek* sin recorrer
///   todo el segmento. Un segmento Active recibe `append`; al `seal` pasa a Closed (se
///   persiste el índice y se fuerza durabilidad), y deja de admitir escrituras.
///
///   Los ficheros se nombran con el offset base a 20 dígitos (orden lexicográfico =
///   orden de offset): `00000000000000000000.log` / `.index`.
/// @invariant En estado Active, `size_bytes()` == bytes escritos en el `.log`.
class Segment {
public:
    /// Estado del segmento en su ciclo de vida (§5.9).
    enum class State : std::uint8_t {
        Active,  ///< Admite `append`; es el segmento al final del log.
        Closed,  ///< Sellado (índice persistido); solo lectura.
    };

    /// @brief Crea un segmento nuevo (ficheros `.log`/`.index` vacíos) en @p dir.
    /// @param base_offset Offset del primer record que albergará (ancla del índice).
    /// @param index_interval_bytes Densidad del índice disperso (bytes entre anclas).
    /// @param key Si no es `nullptr`, cifra el segmento (ADR-0031): deriva una DEK, escribe la
    ///   cabecera de cifrado al inicio del `.log` y sella cada batch con AES-256-GCM. Si es
    ///   `nullptr`, el segmento es en claro (comportamiento por defecto).
    [[nodiscard]] static expected<Segment> create(const std::filesystem::path& dir,
                                                  Offset base_offset,
                                                  std::size_t index_interval_bytes,
                                                  const EncryptionKey* key = nullptr);

    /// @brief Abre un segmento existente (no valida CRC: eso es `recover`).
    /// @param key Clave maestra para descifrar (si el `.log` está cifrado). Autodetecta cifrado vs.
    ///   claro por la cabecera: un `.log` cifrado sin @p key da `Unsupported`; uno en claro se abre
    ///   ignorando @p key (permite logs mixtos con degradación limpia).
    [[nodiscard]] static expected<Segment> open(const std::filesystem::path& dir,
                                                Offset base_offset,
                                                std::size_t index_interval_bytes,
                                                const EncryptionKey* key = nullptr);

    /// @brief Añade @p batch al final del `.log` e indexa su posición.
    /// @details No fuerza durabilidad por batch (la política de `fsync` es de M5); escribe
    ///   a la caché de página. El offset base del batch debe encajar al final del log.
    /// @return El último offset del batch escrito, o error de E/S / segmento sellado.
    [[nodiscard]] expected<Offset> append(const RecordBatch& batch);

    /// @brief Lee batches a partir de @p offset, hasta ~@p max_bytes (al menos uno si existe).
    /// @details *Seek* por el índice (`floor`) + barrido hacia delante hasta el batch que
    ///   contiene @p offset (§7.11 #3); copia batches **completos** mientras quepan en
    ///   @p max_bytes. No revalida el CRC (eso es de `recover`); para un batch *torn* al final
    ///   se detiene. Devuelve los bytes y el `next_offset` por el que continuar.
    /// @return `FetchResult` (puede ir vacío si @p offset supera el final), o error de E/S.
    [[nodiscard]] expected<FetchResult> read(Offset offset, std::size_t max_bytes) const;

    /// @brief Valida el `.log` y trunca una cola *torn* (§7.11 #2); reconstruye el índice.
    /// @details Recorre los batches desde el inicio validando longitud y CRC32C; se detiene en
    ///   el primer batch incompleto o corrupto (la cola escrita a medias por un *crash*) y
    ///   trunca el `.log` justo tras el último batch válido. Re-siembra el `.index` con los
    ///   batches válidos. Se llama al arrancar sobre el segmento activo.
    /// @return El último offset válido (o `base_offset() - 1` si el segmento queda vacío).
    [[nodiscard]] expected<Offset> recover();

    /// @brief Trunca el segmento eliminando los batches con offset >= @p target (resolución de
    ///   conflictos de Raft, §7.11 #5).
    /// @details @p target debe coincidir con la **frontera** de un batch (su `base_offset`) o con
    ///   `base_offset()` (vacía el segmento). Trunca el `.log` justo antes de ese batch,
    ///   reconstruye el índice con los restantes y deja el segmento **Active**. `InvalidArgument`
    ///   si @p target cae dentro de un batch (no es frontera).
    [[nodiscard]] expected<void> truncate_to(Offset target);

    /// @brief Fuerza la durabilidad: persiste el índice y hace `fsync` del `.log`.
    /// @details No cambia de estado (a diferencia de `seal`): el segmento sigue activo.
    [[nodiscard]] expected<void> sync();

    /// @brief Sella el segmento: lo sincroniza (`sync`) y pasa a Closed (solo lectura).
    [[nodiscard]] expected<void> seal();

    /// @brief ¿Ha alcanzado el segmento su tamaño máximo? (criterio de rotación, M4).
    [[nodiscard]] bool is_full(std::size_t segment_bytes) const noexcept {
        return size_bytes_ >= segment_bytes;
    }

    [[nodiscard]] Offset base_offset() const noexcept { return base_offset_; }
    [[nodiscard]] std::size_t size_bytes() const noexcept { return size_bytes_; }
    [[nodiscard]] State state() const noexcept { return state_; }

    [[nodiscard]] bool is_encrypted() const noexcept { return cipher_.has_value(); }

private:
    /// Cabecera de un bloque en disco leída sin descifrar (para recorrer el `.log`).
    struct BlockHeader {
        Offset base_offset = 0;         ///< Offset base del batch del bloque.
        std::int32_t record_count = 0;  ///< Número de records del batch.
        std::size_t on_disk_size =
            0;  ///< Bytes del bloque en disco (cabecera + cuerpo/ciphertext).

        [[nodiscard]] Offset last_offset() const noexcept { return base_offset + record_count - 1; }
    };

    Segment(Offset base_offset, File log, SparseIndex index, std::size_t size_bytes,
            std::optional<SegmentCipher> cipher, std::size_t data_start);

    /// Byte donde empieza el batch con `base_offset == target`; `InvalidArgument` si no es
    /// frontera.
    [[nodiscard]] expected<std::size_t> position_of(Offset target) const;
    /// Reconstruye el `.index` recorriendo los batches actuales del `.log`.
    [[nodiscard]] expected<void> rebuild_index();

    /// Tamaño de la cabecera on-disk de un bloque (cifrado o en claro).
    [[nodiscard]] std::size_t block_header_size() const noexcept;
    /// Lee la cabecera del bloque en @p position sin descifrar (para recorrer). `nullopt` si no hay
    /// cabecera completa/consistente (cola *torn* o fin); error solo ante fallo de E/S.
    [[nodiscard]] expected<std::optional<BlockHeader>> peek_block(std::size_t position) const;
    /// Lee el bloque en @p position, lo descifra si procede y anexa el **plaintext** del batch a
    /// @p out. `Corrupt` si la autenticación/tamaño falla (manipulación); error de E/S si falla.
    [[nodiscard]] expected<void> load_block(std::size_t position, std::size_t on_disk_size,
                                            Buffer& out) const;

    Offset base_offset_;      ///< Offset base del segmento.
    File log_;                ///< Fichero `.log` (RAII).
    SparseIndex index_;       ///< Índice disperso `.index` (RAII).
    std::size_t size_bytes_;  ///< Bytes del `.log` (incluye la cabecera de cifrado si la hay).
    std::optional<SegmentCipher> cipher_;  ///< Cifrador del segmento (vacío = en claro).
    std::size_t data_start_ =
        0;  ///< Primer byte de datos del `.log` (tras la cabecera de cifrado).
    State state_ = State::Active;
};

}  // namespace nexus
