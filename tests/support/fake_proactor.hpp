/// @file   support/fake_proactor.hpp
/// @brief  FakeProactor: doble de test del puerto Proactor (completions deterministas).
/// @ingroup io

#pragma once

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <utility>

#include "common/bytes.hpp"
#include "common/types.hpp"
#include "io/proactor.hpp"

namespace nexus {

/// @brief Doble de test de `Proactor`: registra las operaciones y deja que el test decida
///   **cuándo** y con **qué resultado** se completan, sin tocar el SO. Afinidad: REACTOR-LOCAL.
/// @details Hace deterministas las pruebas de la E/S asíncrona (CLAUDE.md: inyectar red/reloj
///   virtuales). `submit_*` solo encola la operación; el test la dispara con `complete_front`
///   (síncrono) o la "arma" con `arm_front` y luego `run_completions` (como haría el reactor).
class FakeProactor final : public Proactor {
public:
    /// Tipo de operación encolada (para que el test inspeccione qué se pidió).
    enum class OpKind : std::uint8_t { Read, Write, Fsync, Accept, Recv, Send, Timer };

    /// Operación registrada y pendiente de completar.
    struct Op {
        OpKind kind = OpKind::Read;
        int fd = -1;
        MutByteSpan read_buffer{};
        ByteSpan write_buffer{};
        std::uint64_t offset = 0;
        bool datasync = false;
        MonoTime deadline{};
        Completion on_done{};
    };

    void submit_read(int fd, MutByteSpan buffer, std::uint64_t offset,
                     Completion on_done) override {
        pending_.push_back(Op{.kind = OpKind::Read,
                              .fd = fd,
                              .read_buffer = buffer,
                              .offset = offset,
                              .on_done = std::move(on_done)});
    }
    void submit_write(int fd, ByteSpan data, std::uint64_t offset, Completion on_done) override {
        pending_.push_back(Op{.kind = OpKind::Write,
                              .fd = fd,
                              .write_buffer = data,
                              .offset = offset,
                              .on_done = std::move(on_done)});
    }
    void submit_fsync(int fd, bool datasync, Completion on_done) override {
        pending_.push_back(Op{
            .kind = OpKind::Fsync, .fd = fd, .datasync = datasync, .on_done = std::move(on_done)});
    }
    void submit_accept(int listen_fd, Completion on_done) override {
        pending_.push_back(
            Op{.kind = OpKind::Accept, .fd = listen_fd, .on_done = std::move(on_done)});
    }
    void submit_recv(int fd, MutByteSpan buffer, Completion on_done) override {
        pending_.push_back(Op{
            .kind = OpKind::Recv, .fd = fd, .read_buffer = buffer, .on_done = std::move(on_done)});
    }
    void submit_send(int fd, ByteSpan data, Completion on_done) override {
        pending_.push_back(Op{
            .kind = OpKind::Send, .fd = fd, .write_buffer = data, .on_done = std::move(on_done)});
    }
    void submit_timer(MonoTime deadline, Completion on_done) override {
        pending_.push_back(
            Op{.kind = OpKind::Timer, .deadline = deadline, .on_done = std::move(on_done)});
    }

    /// Ejecuta hasta @p max completions previamente armadas (orden FIFO). @return cuántas.
    int run_completions(int max) override {
        int processed = 0;
        while (processed < max && !ready_.empty()) {
            auto entry = std::move(ready_.front());
            ready_.pop_front();
            entry.first.on_done(entry.second);
            ++processed;
        }
        return processed;
    }

    // `wake()` puede llegar desde otros hilos (lo invoca `CrossCoreMailbox::post`): átomo.
    // El doble NO bloquea: drena lo armado (los tests deciden las completions de forma síncrona).
    int wait_completions(int max, MonoTime /*deadline*/) override { return run_completions(max); }

    void wake() override { wakes_.fetch_add(1, std::memory_order_relaxed); }

    // --- API de control para los tests ---

    [[nodiscard]] std::size_t pending() const noexcept { return pending_.size(); }
    [[nodiscard]] const Op& peek(std::size_t index) const { return pending_.at(index); }
    [[nodiscard]] int wakes() const noexcept { return wakes_.load(std::memory_order_relaxed); }

    /// Completa **de inmediato** la operación pendiente más antigua con @p result.
    void complete_front(std::int32_t result) {
        Op op = std::move(pending_.front());
        pending_.pop_front();
        op.on_done(result);
    }

    /// Marca la operación pendiente más antigua como lista; se ejecuta en `run_completions`.
    void arm_front(std::int32_t result) {
        ready_.emplace_back(std::move(pending_.front()), result);
        pending_.pop_front();
    }

    /// @brief Entrega @p bytes a la op `recv`/`read` pendiente más antigua: copia hasta llenar su
    ///   búfer (como haría el kernel) y la marca lista con la cuenta entregada.
    /// @return bytes entregados (≤ tamaño del búfer de la operación).
    std::size_t deliver_recv_front(ByteSpan bytes) {
        Op& op = pending_.front();
        const std::size_t n = std::min(bytes.size(), op.read_buffer.size());
        std::copy_n(bytes.begin(), n, op.read_buffer.begin());
        arm_front(static_cast<std::int32_t>(n));
        return n;
    }

private:
    std::deque<Op> pending_;
    std::deque<std::pair<Op, std::int32_t>> ready_;
    std::atomic<int> wakes_{0};
};

}  // namespace nexus
