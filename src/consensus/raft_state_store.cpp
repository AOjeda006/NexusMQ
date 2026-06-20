/// @file   consensus/raft_state_store.cpp
/// @brief  Implementación de RaftStateStore (registro fijo con CRC32C + fsync).
/// @ingroup consensus

#include "consensus/raft_state_store.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <utility>

#include "common/crc32c.hpp"
#include "common/types.hpp"

namespace nexus {

namespace {

// Desplazamientos dentro del registro (ver el formato en la cabecera).
constexpr std::size_t kCrcOffset = 0;
constexpr std::size_t kTermOffset = 4;
constexpr std::size_t kHasVoteOffset = 12;
constexpr std::size_t kVotedForOffset = 13;
constexpr std::size_t kPayloadOffset =
    kTermOffset;  // el CRC cubre desde el término hasta el final.

}  // namespace

expected<RaftStateStore> RaftStateStore::open(const std::string& path) {
    auto file = File::open(path, File::Mode::ReadWrite);
    if (!file) {
        return std::unexpected(file.error());
    }
    return RaftStateStore(std::move(*file));
}

expected<RaftPersistentState> RaftStateStore::load() const {
    const auto file_size = file_.size();
    if (!file_size) {
        return std::unexpected(file_size.error());
    }
    if (*file_size == 0) {
        return RaftPersistentState{};  // fichero recién creado: estado inicial.
    }
    if (*file_size < kRecordSize) {
        return make_error(ErrorCode::Corrupt, "raft-state: registro truncado");
    }

    std::array<std::byte, kRecordSize> record{};
    const auto read = file_.read_at(MutByteSpan{record}, 0);
    if (!read) {
        return std::unexpected(read.error());
    }
    if (*read < kRecordSize) {
        return make_error(ErrorCode::Corrupt, "raft-state: lectura corta");
    }

    const ByteSpan view{record};
    const auto stored_crc = load_le<std::uint32_t>(view.subspan(kCrcOffset, 4));
    const auto computed_crc = crc32c(view.subspan(kPayloadOffset, kRecordSize - kPayloadOffset));
    if (stored_crc != computed_crc) {
        return make_error(ErrorCode::Corrupt, "raft-state: CRC32C no casa");
    }

    const auto term = load_le<std::int64_t>(view.subspan(kTermOffset, 8));
    const bool has_vote = std::to_integer<std::uint8_t>(record[kHasVoteOffset]) != 0;
    const auto voted_for_raw = load_le<std::int32_t>(view.subspan(kVotedForOffset, 4));
    std::optional<NodeId> voted_for;
    if (has_vote) {
        voted_for = voted_for_raw;
    }
    return RaftPersistentState::restore(term, voted_for);
}

expected<void> RaftStateStore::save(const RaftPersistentState& state) const {
    std::array<std::byte, kRecordSize> record{};
    const MutByteSpan out{record};

    store_le<std::int64_t>(state.current_term(), out.subspan(kTermOffset, 8));
    const std::optional<NodeId> voted = state.voted_for();
    record[kHasVoteOffset] = std::byte{static_cast<std::uint8_t>(voted ? 1 : 0)};
    store_le<std::int32_t>(voted ? *voted : 0, out.subspan(kVotedForOffset, 4));

    const auto crc = crc32c(ByteSpan{record}.subspan(kPayloadOffset, kRecordSize - kPayloadOffset));
    store_le<std::uint32_t>(crc, out.subspan(kCrcOffset, 4));

    if (const auto written = file_.write_at(ByteSpan{record}, 0); !written) {
        return std::unexpected(written.error());
    }
    return file_.sync();
}

}  // namespace nexus
