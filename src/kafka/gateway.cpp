/// @file   kafka/gateway.cpp
/// @brief  Implementación del dispatcher del subconjunto Kafka — F7e.
/// @ingroup kafka

#include "kafka/gateway.hpp"

#include <string>

#include "kafka/codec.hpp"
#include "kafka/messages.hpp"

namespace nexus::kafka {

expected<Buffer> KafkaGateway::handle_request(ByteSpan request) {
    Decoder dec{request};
    const expected<RequestHeader> header = decode_request_header(dec);
    if (!header) {
        return std::unexpected(header.error());
    }
    const auto api_key = static_cast<ApiKey>(header->api_key);
    const std::int16_t header_version = response_header_version(api_key, header->api_version);

    Buffer out;
    Encoder enc{out};
    encode_response_header(enc, header->correlation_id, header_version);

    switch (api_key) {
        case ApiKey::ApiVersions: {
            encode_api_versions_response(enc, make_api_versions_response());
            return out;
        }
        case ApiKey::Metadata: {
            const expected<MetadataRequest> req = decode_metadata_request(dec);
            if (!req) {
                return std::unexpected(req.error());
            }
            encode_metadata_response(enc, broker_.metadata(*req));
            return out;
        }
        case ApiKey::Produce: {
            const expected<ProduceRequest> req = decode_produce_request(dec);
            if (!req) {
                return std::unexpected(req.error());
            }
            encode_produce_response(enc, broker_.produce(*req));
            return out;
        }
        case ApiKey::Fetch: {
            const expected<FetchRequest> req = decode_fetch_request(dec);
            if (!req) {
                return std::unexpected(req.error());
            }
            encode_fetch_response(enc, broker_.fetch(*req));
            return out;
        }
    }
    return make_error(ErrorCode::Unsupported,
                      "api_key Kafka no soportada: " + std::to_string(header->api_key));
}

}  // namespace nexus::kafka
