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

Error to_error(WireError error) {
    switch (error) {
        case WireError::UnknownTopicOrPartition:
            return Error{ErrorCode::NotFound, "topic o partición desconocidos"};
        case WireError::OffsetOutOfRange:
            return Error{ErrorCode::OutOfRange, "offset fuera de rango"};
        case WireError::OutOfOrderSequence:
            return Error{ErrorCode::OutOfRange, "secuencia idempotente fuera de orden"};
        case WireError::DuplicateSequence:
            return Error{ErrorCode::OutOfRange, "secuencia idempotente duplicada"};
        case WireError::CorruptMessage:
            return Error{ErrorCode::Corrupt, "mensaje corrupto"};
        case WireError::MessageTooLarge:
            return Error{ErrorCode::InvalidArgument, "mensaje demasiado grande"};
        case WireError::UnsupportedVersion:
            return Error{ErrorCode::Unsupported, "versión no soportada"};
        case WireError::Unauthorized:
            return Error{ErrorCode::InvalidArgument, "no autorizado"};
        case WireError::InvalidRequest:
            return Error{ErrorCode::InvalidArgument, "petición inválida"};
        // Condiciones transitorias del servidor: el líder cambió/no está, timeout, falta quórum,
        // throttling o rebalanceo. Sin código de núcleo dedicado: se reportan como E/S transitoria.
        case WireError::NotLeaderForPartition:
            return Error{ErrorCode::IoError, "la partición no tiene líder aquí"};
        case WireError::LeaderNotAvailable:
            return Error{ErrorCode::IoError, "líder no disponible"};
        case WireError::RequestTimedOut:
            return Error{ErrorCode::IoError, "la petición expiró"};
        case WireError::NotEnoughReplicas:
            return Error{ErrorCode::IoError, "réplicas insuficientes"};
        case WireError::Throttled:
            return Error{ErrorCode::IoError, "limitado por el servidor (throttling)"};
        case WireError::RebalanceInProgress:
            return Error{ErrorCode::IoError, "rebalanceo en curso"};
        case WireError::None:
            return Error{ErrorCode::InvalidArgument, "sin error (None)"};
    }
    return Error{ErrorCode::InvalidArgument, "código de wire desconocido"};
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
