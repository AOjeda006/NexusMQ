/// @file   consensus/raft_node.cpp
/// @brief  Implementación de RaftNode (máquina de estados de Raft, ADR-0003/0015).
/// @ingroup consensus

#include "consensus/raft_node.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <span>
#include <utility>
#include <vector>

#include "common/bytes.hpp"
#include "common/record.hpp"

namespace nexus {

RaftNode::RaftNode(NodeId self, std::vector<NodeId> peers, RaftLog& log, RaftConfig config)
    : self_(self),
      peers_(std::move(peers)),
      log_(log),
      config_(config),
      rng_(static_cast<std::minstd_rand::result_type>(config.random_seed +
                                                      static_cast<std::uint64_t>(self))) {}

std::chrono::milliseconds RaftNode::random_election_timeout() {
    const auto lo = config_.election_timeout_min.count();
    const auto hi = config_.election_timeout_max.count();
    const auto span = hi > lo ? hi - lo : 0;
    const auto extra =
        span > 0 ? static_cast<std::int64_t>(rng_() % static_cast<std::uint64_t>(span + 1)) : 0;
    return std::chrono::milliseconds{lo + extra};
}

void RaftNode::reset_election_timer(MonoTime now) {
    election_deadline_ = now + random_election_timeout();
}

void RaftNode::reset_heartbeat_timer(MonoTime now) {
    heartbeat_deadline_ = now + config_.heartbeat_interval;
}

void RaftNode::become_follower(MonoTime now, Term term) {
    if (term > persistent_.current_term()) {
        persistent_.advance_term(term);  // término nuevo → resetea el voto.
    }
    role_ = RaftRole::Follower;
    volatile_.clear_leader_progress();
    votes_granted_.clear();
    transfer_target_.reset();
    reset_election_timer(now);
}

void RaftNode::start_pre_election(MonoTime now) {
    // Pre-vote (§9.6): sondea con el término *prospectivo* (current+1) sin subir el propio término
    // ni persistir un voto. Evita que un nodo aislado que se reincorpora disrumpa al líder vigente.
    role_ = RaftRole::PreCandidate;
    leader_id_.reset();
    votes_granted_.clear();
    votes_granted_.insert(self_);  // cuenta su propio pre-voto.
    reset_election_timer(now);
    if (has_majority(votes_granted_.size())) {
        become_candidate(now);  // cluster de un solo nodo: salta directo a la elección real.
        return;
    }
    broadcast_request_vote(true);
}

void RaftNode::become_candidate(MonoTime now) {
    persistent_.advance_term(persistent_.current_term() + 1);
    persistent_.record_vote(self_);  // se vota a sí mismo.
    role_ = RaftRole::Candidate;
    leader_id_.reset();
    votes_granted_.clear();
    votes_granted_.insert(self_);
    reset_election_timer(now);
    if (has_majority(votes_granted_.size())) {
        become_leader(now);  // cluster de un solo nodo: mayoría inmediata.
        return;
    }
    broadcast_request_vote(false);
}

void RaftNode::become_leader(MonoTime now) {
    role_ = RaftRole::Leader;
    leader_id_ = self_;
    leader_epoch_ += 1;
    volatile_.reset_leader_progress(peers_, log_.last_index());
    last_sent_.clear();
    transfer_target_.reset();
    reset_heartbeat_timer(now);
    replicate_all();  // afirma el liderazgo de inmediato (heartbeats).
}

void RaftNode::broadcast_request_vote(bool pre_vote) {
    // El pre-voto anuncia el término *prospectivo* (current+1) sin adoptarlo; el voto real usa el
    // término ya incrementado en `become_candidate`.
    const Term term = pre_vote ? persistent_.current_term() + 1 : persistent_.current_term();
    const RequestVoteArgs args{.term = term,
                               .candidate_id = self_,
                               .last_log_index = log_.last_index(),
                               .last_log_term = log_.last_term(),
                               .pre_vote = pre_vote};
    for (const NodeId peer : peers_) {
        send(peer, args);
    }
}

void RaftNode::replicate_to(NodeId peer) {
    const Index next = volatile_.next_index(peer);
    const Index prev_index = next - 1;
    const auto prev_term = log_.term_at(prev_index);
    auto entries = log_.entries_from(next, kMaxEntriesPerAppend);
    std::vector<RaftLogEntry> payload = entries ? std::move(*entries) : std::vector<RaftLogEntry>{};
    last_sent_[peer] = payload.empty() ? prev_index : payload.back().index;
    send(peer, AppendEntriesArgs{.term = persistent_.current_term(),
                                 .leader_id = self_,
                                 .prev_log_index = prev_index,
                                 .prev_log_term = prev_term ? *prev_term : Term{0},
                                 .entries = std::move(payload),
                                 .leader_commit = volatile_.commit_index(),
                                 .leader_epoch = leader_epoch_});
}

void RaftNode::replicate_all() {
    for (const NodeId peer : peers_) {
        replicate_to(peer);
    }
}

void RaftNode::advance_commit_index() {
    // Índice replicado en mayoría: ordena los match_index (incluido el propio = last_index) y toma
    // el de la posición de la mayoría. Solo se confirma si la entrada es del término actual (§5.4).
    std::vector<Index> matches;
    matches.reserve(cluster_size());
    matches.push_back(log_.last_index());
    for (const NodeId peer : peers_) {
        matches.push_back(volatile_.match_index(peer));
    }
    std::ranges::sort(matches, std::ranges::greater{});
    const Index candidate = matches[cluster_size() / 2];
    if (candidate <= volatile_.commit_index()) {
        return;
    }
    const auto term = log_.term_at(candidate);
    if (term && *term == persistent_.current_term()) {
        volatile_.set_commit_index(candidate);
    }
}

void RaftNode::maybe_transfer_to(NodeId peer) {
    if (!transfer_target_.has_value() || *transfer_target_ != peer) {
        return;
    }
    if (volatile_.match_index(peer) >= log_.last_index()) {
        send(peer, TimeoutNowArgs{.term = persistent_.current_term(), .leader_id = self_});
        transfer_target_.reset();  // el objetivo está al día: cede el liderazgo.
    } else {
        replicate_to(peer);  // sigue empujando hasta ponerlo al día.
    }
}

void RaftNode::tick(MonoTime now) {
    if (!started_) {
        started_ = true;
        reset_election_timer(now);  // arma sin disparar en el primer tick.
        return;
    }
    if (role_ == RaftRole::Leader) {
        if (now >= heartbeat_deadline_) {
            reset_heartbeat_timer(now);
            replicate_all();
        }
        return;
    }
    if (now >= election_deadline_) {
        start_pre_election(now);  // pre-vote antes de subir el término (§9.6).
    }
}

bool RaftNode::log_is_up_to_date(Index last_log_index, Term last_log_term) const {
    const Term my_last_term = log_.last_term();
    if (last_log_term != my_last_term) {
        return last_log_term > my_last_term;
    }
    return last_log_index >= log_.last_index();
}

RequestVoteReply RaftNode::make_pre_vote_reply(MonoTime now, const RequestVoteArgs& args) const {
    // Concede un pre-voto sin tocar estado: ni sube el término ni persiste voto ni rearma el timer.
    RequestVoteReply reply{.term = persistent_.current_term(), .vote_granted = false};
    if (args.term < persistent_.current_term()) {
        return reply;  // término prospectivo obsoleto.
    }
    if (role_ == RaftRole::Leader) {
        return reply;  // nos creemos líder: no respaldamos a un retador.
    }
    if (now < election_deadline_) {
        return reply;  // *lease*: hubo contacto reciente con el líder, no disrumpir.
    }
    if (log_is_up_to_date(args.last_log_index, args.last_log_term)) {
        reply.vote_granted = true;
    }
    return reply;
}

RequestVoteReply RaftNode::on_request_vote(MonoTime now, const RequestVoteArgs& args) {
    if (args.pre_vote) {
        return make_pre_vote_reply(now, args);
    }
    if (args.term > persistent_.current_term()) {
        become_follower(now, args.term);
    }
    RequestVoteReply reply{.term = persistent_.current_term(), .vote_granted = false};
    if (args.term < persistent_.current_term()) {
        return reply;  // candidato obsoleto.
    }
    // Lee el voto una sola vez: comprobar y desreferenciar la misma copia (no dos llamadas a
    // `voted_for()`, que devuelve por valor) evita bugprone-unchecked-optional-access.
    const std::optional<NodeId> voted = persistent_.voted_for();
    const bool can_vote = !voted.has_value() || *voted == args.candidate_id;
    if (can_vote && log_is_up_to_date(args.last_log_index, args.last_log_term)) {
        persistent_.record_vote(args.candidate_id);
        reset_election_timer(now);  // conceder un voto cuenta como contacto válido.
        reply.vote_granted = true;
    }
    reply.term = persistent_.current_term();
    return reply;
}

void RaftNode::on_request_vote_reply(MonoTime now, NodeId from, const RequestVoteReply& reply) {
    if (reply.term > persistent_.current_term()) {
        become_follower(now, reply.term);
        return;
    }
    if (role_ == RaftRole::PreCandidate) {
        if (reply.term == persistent_.current_term() && reply.vote_granted) {
            votes_granted_.insert(from);
            if (has_majority(votes_granted_.size())) {
                become_candidate(now);  // pre-votos en mayoría: arranca la elección real.
            }
        }
        return;
    }
    if (role_ != RaftRole::Candidate || reply.term != persistent_.current_term()) {
        return;  // respuesta obsoleta o ya no somos candidatos.
    }
    if (reply.vote_granted) {
        votes_granted_.insert(from);
        if (has_majority(votes_granted_.size())) {
            become_leader(now);
        }
    }
}

bool RaftNode::append_entries_from(const std::vector<RaftLogEntry>& entries) {
    std::size_t i = 0;
    for (; i < entries.size(); ++i) {
        const RaftLogEntry& entry = entries[i];
        if (entry.index > log_.last_index()) {
            break;  // territorio nuevo: anexa desde aquí.
        }
        const auto existing = log_.term_at(entry.index);
        if (existing && *existing == entry.term) {
            continue;  // ya presente y consistente.
        }
        if (!log_.truncate_from(entry.index)) {  // conflicto: borra desde aquí.
            return false;
        }
        break;
    }
    if (i < entries.size()) {
        const std::span<const RaftLogEntry> suffix{entries.data() + i, entries.size() - i};
        if (!log_.append(suffix)) {
            return false;
        }
    }
    return true;
}

AppendEntriesReply RaftNode::on_append_entries(MonoTime now, const AppendEntriesArgs& args) {
    AppendEntriesReply reply{
        .term = persistent_.current_term(), .success = false, .conflict_index = 0};
    if (args.term < persistent_.current_term()) {
        return reply;  // líder obsoleto.
    }
    // Líder válido (de este término o más nuevo): pasa a seguidor, reconoce al líder y rearma.
    if (args.term > persistent_.current_term() || role_ != RaftRole::Follower) {
        become_follower(now, args.term);
    }
    leader_id_ = args.leader_id;
    reset_election_timer(now);
    reply.term = persistent_.current_term();

    // Chequeo de consistencia del log (§7.11 #5).
    if (args.prev_log_index > log_.last_index()) {
        reply.conflict_index = log_.last_index() + 1;
        return reply;  // hueco: falta cola.
    }
    const auto prev_term = log_.term_at(args.prev_log_index);
    if (!prev_term) {
        reply.conflict_index = log_.last_index() + 1;
        return reply;
    }
    if (*prev_term != args.prev_log_term) {
        reply.conflict_index = args.prev_log_index;  // conflicto: reintentar desde aquí.
        return reply;
    }
    if (!append_entries_from(args.entries)) {
        return reply;  // error de E/S al anexar: respuesta negativa.
    }
    if (args.leader_commit > volatile_.commit_index()) {
        volatile_.set_commit_index(std::min(args.leader_commit, log_.last_index()));
    }
    reply.success = true;
    return reply;
}

void RaftNode::on_append_entries_reply(MonoTime now, NodeId from, const AppendEntriesReply& reply) {
    if (reply.term > persistent_.current_term()) {
        become_follower(now, reply.term);
        return;
    }
    if (role_ != RaftRole::Leader || reply.term != persistent_.current_term()) {
        return;  // respuesta obsoleta o ya no somos líder.
    }
    if (reply.success) {
        const Index matched = last_sent_[from];
        volatile_.set_match_index(from, std::max(volatile_.match_index(from), matched));
        volatile_.set_next_index(from, volatile_.match_index(from) + 1);
        advance_commit_index();
        maybe_transfer_to(from);  // si hay transferencia pendiente, avanza/dispara TimeoutNow.
        return;
    }
    // Fallo de consistencia: retrocede `next_index` (pista `conflict_index`) y reintenta.
    const Index hint =
        reply.conflict_index > 0 ? reply.conflict_index : volatile_.next_index(from) - 1;
    volatile_.set_next_index(from, std::max<Index>(1, hint));
    replicate_to(from);
}

expected<Index> RaftNode::propose(const RecordBatch& batch) {
    if (role_ != RaftRole::Leader) {
        return make_error(ErrorCode::Unsupported, "propose: el nodo no es líder");
    }
    Buffer encoded;
    batch.encode(encoded);
    const ByteSpan bytes = encoded.as_span();
    const RaftLogEntry entry{.term = persistent_.current_term(),
                             .index = log_.last_index() + 1,
                             .type = RaftEntryType::Data,
                             .payload = std::vector<std::byte>(bytes.begin(), bytes.end())};
    const std::vector<RaftLogEntry> one{entry};
    const auto appended = log_.append(one);
    if (!appended) {
        return std::unexpected(appended.error());
    }
    replicate_all();
    advance_commit_index();  // cluster de un nodo: confirma de inmediato.
    return *appended;
}

expected<void> RaftNode::transfer_leadership(NodeId target) {
    if (role_ != RaftRole::Leader) {
        return make_error(ErrorCode::Unsupported, "transfer_leadership: el nodo no es líder");
    }
    if (std::ranges::find(peers_, target) == peers_.end()) {
        return make_error(ErrorCode::InvalidArgument, "transfer_leadership: destino no es un peer");
    }
    transfer_target_ = target;
    maybe_transfer_to(target);  // al día → TimeoutNow; rezagado → replica y espera el ack.
    return {};
}

void RaftNode::on_timeout_now(MonoTime now, const TimeoutNowArgs& args) {
    if (args.term < persistent_.current_term()) {
        return;  // orden de un líder obsoleto.
    }
    if (args.term > persistent_.current_term()) {
        become_follower(now, args.term);  // adopta el término antes de postularse.
    }
    become_candidate(now);  // elección real inmediata: sin pre-vote ni espera de *lease* (§3.10).
}

std::vector<RaftMessage> RaftNode::take_messages() {
    return std::exchange(outbox_, {});
}

}  // namespace nexus
