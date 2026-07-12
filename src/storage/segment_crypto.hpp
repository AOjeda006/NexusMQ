/// @file   storage/segment_crypto.hpp
/// @brief  Cifrado en reposo del log: KEK, DEK por segmento y framing AEAD por bloque (ADR-0031).
/// @ingroup storage

#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <string_view>

#include "common/bytes.hpp"
#include "common/error.hpp"
#include "common/types.hpp"

namespace nexus {

/// Tamaño de la clave AES-256 (KEK y DEK), en bytes.
inline constexpr std::size_t kEncKeyBytes = 32;
/// Tamaño del nonce GCM (96 bits: el recomendado por NIST para AES-GCM).
inline constexpr std::size_t kEncNonceBytes = 12;
/// Tamaño del tag de autenticación GCM.
inline constexpr std::size_t kEncTagBytes = 16;
/// Tamaño de la salt aleatoria por segmento (alimenta el HKDF que deriva la DEK).
inline constexpr std::size_t kEncSaltBytes = 16;

/// Tamaño de la cabecera de cifrado al inicio del `.log` (magic + ids + salt).
inline constexpr std::size_t kEncSegmentHeaderSize = 32;
/// Tamaño de la cabecera de un bloque cifrado (metadatos autenticados + nonce + tag).
inline constexpr std::size_t kEncBlockHeaderSize =
    1 + 1 + 8 + 4 + 4 + kEncNonceBytes + kEncTagBytes;  // = 46

/// Magic de la cabecera de segmento cifrado ("NXSEG" + versión de formato). Distingue un `.log`
/// cifrado de uno en claro (autodetección al abrir; permite logs mixtos con degradación limpia).
inline constexpr std::array<std::byte, 8> kEncSegmentMagic = {
    std::byte{'N'}, std::byte{'X'}, std::byte{'S'}, std::byte{'E'},
    std::byte{'G'}, std::byte{'1'}, std::byte{0},   std::byte{0}};

/// @brief ¿Se compiló el broker con soporte de cifrado (OpenSSL)? Afinidad: pura.
/// @details Si es `false`, `EncryptionKey`/`SegmentCipher` devuelven `Unsupported`; los tests de
///   cifrado hacen `GTEST_SKIP`. Espeja el patrón opcional de TLS/compresión (degradación limpia).
[[nodiscard]] bool encryption_available() noexcept;

/// @brief Clave maestra de cifrado (KEK) del broker. Afinidad: INMUTABLE / THREAD-SAFE.
/// @details Inmutable tras construirse; solo deriva claves de segmento (DEK), nunca cifra datos
///   directamente. La KEK llega por config/entorno y **jamás** se persiste. Deriva una DEK distinta
///   por segmento vía HKDF-SHA256(KEK, salt), acotando el uso de cada clave a un único segmento
///   (aísla el radio de daño y mantiene el número de cifrados por clave muy por debajo del límite
///   de nonces aleatorios). Es copiable (semántica de valor sobre 32 bytes).
/// @invariant Contiene exactamente `kEncKeyBytes` bytes de material de clave.
class EncryptionKey {
public:
    /// @brief Construye la KEK desde @p hex (64 dígitos hexadecimales = 256 bits).
    /// @return La clave, `InvalidArgument` si la longitud o los dígitos no son válidos, o
    ///   `Unsupported` si el broker se compiló sin OpenSSL.
    [[nodiscard]] static expected<EncryptionKey> from_hex(std::string_view hex);

    /// @brief Construye la KEK desde @p key (exactamente `kEncKeyBytes` bytes crudos).
    [[nodiscard]] static expected<EncryptionKey> from_bytes(ByteSpan key);

    /// @brief Deriva la DEK de un segmento por HKDF-SHA256(KEK, @p salt, info fija).
    /// @return La DEK de 32 bytes, o `Unsupported`/`IoError` si el KDF falla.
    [[nodiscard]] expected<std::array<std::byte, kEncKeyBytes>> derive_segment_dek(
        ByteSpan salt) const;

private:
    explicit EncryptionKey(std::array<std::byte, kEncKeyBytes> key) noexcept : key_(key) {}

    std::array<std::byte, kEncKeyBytes> key_{};
};

/// @brief Vista de traversal de un bloque cifrado (metadatos en claro, autenticados). INMUTABLE.
/// @details `base_offset`/`record_count` viajan en claro en la cabecera del bloque (autenticados
///   por el tag GCM vía AAD) para recorrer el log **sin descifrar**; los offsets y tamaños no son
///   secretos, las claves/valores de los records sí (van en el ciphertext).
struct EncryptedBlockView {
    Offset base_offset = 0;            ///< Offset base del batch cifrado.
    std::int32_t record_count = 0;     ///< Número de records del batch.
    std::uint32_t ciphertext_len = 0;  ///< Bytes de ciphertext (= tamaño del batch en claro).

    /// Bytes totales del bloque en disco (cabecera de bloque + ciphertext).
    [[nodiscard]] std::size_t on_disk_size() const noexcept {
        return kEncBlockHeaderSize + ciphertext_len;
    }
    /// Mayor offset del batch: `base_offset + record_count - 1`.
    [[nodiscard]] Offset last_offset() const noexcept { return base_offset + record_count - 1; }
};

/// @brief Cifrador AEAD de un segmento: AES-256-GCM por bloque con la DEK del segmento.
/// REACTOR-LOCAL.
/// @details Cada `append` de un batch se cifra como **un bloque** independiente (granularidad por
///   bloque de escritura, nunca por record). El bloque en disco es autodescriptivo:
///   `version|flags|base_offset|record_count|ct_len|nonce|tag|ciphertext`. El **nonce es aleatorio
///   de 96 bits** por bloque (robusto ante truncado/re-append: cada escritura toma un nonce
///   fresco); con la DEK acotada a un segmento, el número de cifrados por clave queda muy por
///   debajo del límite de colisión de nonces aleatorios. La **AAD** cubre la cabecera del bloque
///   salvo el tag, de modo que manipular cualquier metadato en claro se detecta al descifrar.
/// @invariant Posee una DEK de `kEncKeyBytes` bytes; no comparte estado mutable (crea un contexto
///   EVP por operación), por lo que sus métodos son `const`.
class SegmentCipher {
public:
    /// @brief Crea el cifrador de un segmento derivando su DEK desde @p kek y @p salt.
    [[nodiscard]] static expected<SegmentCipher> create(const EncryptionKey& kek, ByteSpan salt);

    /// @brief Cifra @p plaintext (un batch codificado) y anexa el bloque completo a @p out.
    /// @param plaintext Bytes del `RecordBatch` en claro.
    /// @param base_offset Offset base del batch (metadato de traversal, autenticado en claro).
    /// @param record_count Número de records (metadato de traversal, autenticado en claro).
    /// @param out Búfer al que se anexa `cabecera_de_bloque || ciphertext`. Genera el nonce.
    /// @return Vacío en éxito; `IoError`/`Unsupported` si la criptografía falla.
    [[nodiscard]] expected<void> seal_block(ByteSpan plaintext, Offset base_offset,
                                            std::int32_t record_count, Buffer& out) const;

    /// @brief Lee la cabecera de @p frame sin descifrar (para recorrer el log).
    /// @return La vista, o `Corrupt` si la cabecera está truncada o es inconsistente.
    [[nodiscard]] static expected<EncryptedBlockView> peek_block(ByteSpan frame);

    /// @brief Descifra @p frame (cabecera + ciphertext) y anexa el plaintext a @p out.
    /// @details Verifica el tag GCM y la AAD: cualquier alteración del ciphertext o de los
    /// metadatos
    ///   en claro produce `Corrupt` (fallo autenticado), nunca datos corruptos silenciosos.
    /// @return Vacío en éxito; `Corrupt` si la autenticación falla; `Unsupported` sin OpenSSL.
    [[nodiscard]] expected<void> open_block(ByteSpan frame, Buffer& out) const;

private:
    explicit SegmentCipher(std::array<std::byte, kEncKeyBytes> dek) noexcept : dek_(dek) {}

    std::array<std::byte, kEncKeyBytes> dek_{};
};

/// @brief Escribe la cabecera de cifrado de un segmento (magic + ids + @p salt) en @p out.
/// @pre out.size() == kEncSegmentHeaderSize.
void encode_segment_header(ByteSpan salt, MutByteSpan out) noexcept;

/// @brief Comprueba si @p header (los primeros bytes del `.log`) es una cabecera de segmento
/// cifrado.
/// @details No falla: solo mira el magic. Permite autodetectar cifrado vs. claro al abrir.
[[nodiscard]] bool is_encrypted_segment_header(ByteSpan header) noexcept;

/// @brief Extrae la salt de una cabecera de segmento cifrado @p header.
/// @return La salt de `kEncSaltBytes` bytes, o `Corrupt` si el magic/versión no cuadran.
[[nodiscard]] expected<std::array<std::byte, kEncSaltBytes>> parse_segment_header(ByteSpan header);

/// @brief Genera @p out bytes criptográficamente aleatorios (CSPRNG de OpenSSL).
/// @return Vacío en éxito; `Unsupported` sin OpenSSL; `IoError` si el RNG falla.
[[nodiscard]] expected<void> random_bytes(MutByteSpan out);

}  // namespace nexus
