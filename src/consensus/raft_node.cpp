/// @file   consensus/raft_node.cpp
/// @brief  Implementación de RaftNode (máquina de estados de Raft, ADR-0003/0015).
/// @ingroup consensus

#include "consensus/raft_node.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <span>
#include <utility>

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
    reset_election_timer(now);
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
    broadcast_request_vote();
}

void RaftNode::become_leader(MonoTime now) {
    role_ = RaftRole::Leader;
    leader_id_ = self_;
    leader_epoch_ += 1;
    volatile_.reset_leader_progress(peers_, log_.last_index());
    reset_heartbeat_timer(now);
    broadcast_heartbeat();  // afirma el liderazgo de inmediato.
}

void RaftNode::broadcast_request_vote() {
    const RequestVoteArgs args{.term = persistent_.current_term(),
                               .candidate_id = self_,
                               .last_log_index = log_.last_index(),
                               .last_log_term = log_.last_term(),
                               .pre_vote = false};
    for (const NodeId peer : peers_) {
        send(peer, args);
    }
}

void RaftNode::broadcast_heartbeat() {
    const AppendEntriesArgs args{.term = persistent_.current_term(),
                                 .leader_id = self_,
                                 .prev_log_index = log_.last_index(),
                                 .prev_log_term = log_.last_term(),
                                 .entries = {},
                                 .leader_commit = volatile_.commit_index(),
                                 .leader_epoch = leader_epoch_};
    for (const NodeId peer : peers_) {
        send(peer, args);
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
            broadcast_heartbeat();
        }
        return;
    }
    if (now >= election_deadline_) {
        become_candidate(now);
    }
}

bool RaftNode::log_is_up_to_date(Index last_log_index, Term last_log_term) const {
    const Term my_last_term = log_.last_term();
    if (last_log_term != my_last_term) {
        return last_log_term > my_last_term;
    }
    return last_log_index >= log_.last_index();
}

RequestVoteReply RaftNode::on_request_vote(MonoTime now, const RequestVoteArgs& args) {
    if (args.term > persistent_.current_term()) {
        become_follower(now, args.term);
    }
    RequestVoteReply reply{.term = persistent_.current_term(), .vote_granted = false};
    if (args.term < persistent_.current_term()) {
        return reply;  // candidato obsoleto.
    }
    const bool can_vote =
        !persistent_.voted_for().has_value() || *persistent_.voted_for() == args.candidate_id;
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
    }
    (void)from;  // El avance de match_index/commit_index por mayoría llega en C6.
}

std::vector<RaftMessage> RaftNode::take_messages() {
    return std::exchange(outbox_, {});
}

}  // namespace nexus
