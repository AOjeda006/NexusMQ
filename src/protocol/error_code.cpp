#include "protocol/error_code.hpp"

namespace nexus {

bool is_retryable(WireError error) noexcept {
    switch (error) {
        // Transitorios: el líder cambió, no está disponible, timeout, falta quórum, throttling
        // o rebalanceo en curso. El cliente reintenta (con backoff) tras refrescar metadata.
        case WireError::NotLeaderForPartition:
        case WireError::LeaderNotAvailable:
        case WireError::RequestTimedOut:
        case WireError::NotEnoughReplicas:
        case WireError::Throttled:
        case WireError::RebalanceInProgress:
            return true;
        // Permanentes (o no-error): reintentar no ayuda.
        case WireError::None:
        case WireError::UnknownTopicOrPartition:
        case WireError::OffsetOutOfRange:
        case WireError::CorruptMessage:
        case WireError::MessageTooLarge:
        case WireError::OutOfOrderSequence:
        case WireError::DuplicateSequence:
        case WireError::UnsupportedVersion:
        case WireError::Unauthorized:
        case WireError::InvalidRequest:
            return false;
    }
    return false;
}

WireError from_error(const Error& error) noexcept {
    switch (error.code()) {
        case ErrorCode::Corrupt:
            return WireError::CorruptMessage;
        case ErrorCode::OutOfRange:
            return WireError::OffsetOutOfRange;
        case ErrorCode::NotFound:
            return WireError::UnknownTopicOrPartition;
        case ErrorCode::Unsupported:
            return WireError::UnsupportedVersion;
        case ErrorCode::InvalidArgument:
            return WireError::InvalidRequest;
        // Fallos del lado del servidor (almacenamiento, espacio, apagado): la partición no es
        // servible aquí ahora; el cliente reintenta en otro réplica/momento.
        case ErrorCode::IoError:
        case ErrorCode::OutOfSpace:
        case ErrorCode::Shutdown:
            return WireError::LeaderNotAvailable;
    }
    return WireError::InvalidRequest;
}

}  // namespace nexus
