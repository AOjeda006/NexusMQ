/// @file   ingress/circuit_breaker.cpp
/// @brief  Implementación de CircuitBreaker (Nygard, §6.4).
/// @ingroup ingress

#include "ingress/circuit_breaker.hpp"

#include <algorithm>

namespace nexus {

CircuitBreaker::CircuitBreaker(CircuitBreakerConfig config)
    : config_(config), window_(std::max<std::size_t>(1, config.window_size), false) {}

bool CircuitBreaker::allow(MonoTime now) {
    switch (state_) {
        case CircuitState::Closed:
            return true;
        case CircuitState::Open:
            if (now - opened_at_ >= config_.open_timeout) {
                enter_half_open();
                ++probes_issued_;  // la primera sonda tras transicionar.
                return true;
            }
            return false;
        case CircuitState::HalfOpen:
            if (probes_issued_ < config_.half_open_probes) {
                ++probes_issued_;
                return true;
            }
            return false;  // ya hay bastantes sondas en vuelo; espera a que resuelvan.
    }
    return false;
}

void CircuitBreaker::on_success() {
    switch (state_) {
        case CircuitState::Closed:
            record(/*failure=*/false);
            break;
        case CircuitState::HalfOpen:
            // Un fallo en sondeo ya reabre de inmediato (ver `on_failure`), así que llegar al cupo
            // de sondas resueltas implica que todas tuvieron éxito: el dependiente se recuperó.
            ++probe_successes_;
            if (probe_successes_ >= config_.half_open_probes) {
                close();
            }
            break;
        case CircuitState::Open:
            break;  // no debería ocurrir (no se admiten operaciones en Open).
    }
}

void CircuitBreaker::on_failure(MonoTime now) {
    switch (state_) {
        case CircuitState::Closed:
            record(/*failure=*/true);
            if (window_failure_ratio() >= config_.failure_ratio) {
                trip_open(now);
            }
            break;
        case CircuitState::HalfOpen:
            trip_open(now);  // un fallo durante el sondeo reabre de inmediato.
            break;
        case CircuitState::Open:
            break;
    }
}

void CircuitBreaker::trip_open(MonoTime now) {
    state_ = CircuitState::Open;
    opened_at_ = now;
}

void CircuitBreaker::enter_half_open() {
    state_ = CircuitState::HalfOpen;
    probes_issued_ = 0;
    probe_successes_ = 0;
}

void CircuitBreaker::close() {
    state_ = CircuitState::Closed;
    std::ranges::fill(window_, false);
    head_ = 0;
    count_ = 0;
    failures_ = 0;
}

void CircuitBreaker::record(bool failure) {
    if (count_ == window_.size() && window_[head_]) {
        --failures_;  // se sobrescribe una muestra que era fallo.
    }
    window_[head_] = failure;
    if (failure) {
        ++failures_;
    }
    head_ = (head_ + 1) % window_.size();
    count_ = std::min(count_ + 1, window_.size());
}

double CircuitBreaker::window_failure_ratio() const {
    if (count_ < config_.min_samples) {
        return 0.0;
    }
    return static_cast<double>(failures_) / static_cast<double>(count_);
}

}  // namespace nexus
