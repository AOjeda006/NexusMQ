/// @file   kafka/gateway.cpp
/// @brief  Implementación del dispatcher del subconjunto Kafka — F7e.
/// @ingroup kafka

#include "kafka/gateway.hpp"

#include <string>

#include "kafka/codec.hpp"
#include "kafka/messages.hpp"

namespace nexus::kafka {

task<expected<Buffer>> KafkaGateway::handle_request(ByteSpan request) {
    Decoder dec{request};
    const expected<RequestHeader> header = decode_request_header(dec);
    if (!header) {
        co_return std::unexpected(header.error());
    }
    const auto api_key = static_cast<ApiKey>(header->api_key);
    const std::int16_t header_version = response_header_version(api_key, header->api_version);

    Buffer out;
    Encoder enc{out};
    encode_response_header(enc, header->correlation_id, header_version);

    switch (api_key) {
        case ApiKey::ApiVersions: {
            encode_api_versions_response(enc, make_api_versions_response());
            co_return out;
        }
        case ApiKey::Metadata: {
            const expected<MetadataRequest> req = decode_metadata_request(dec);
            if (!req) {
                co_return std::unexpected(req.error());
            }
            encode_metadata_response(enc, co_await broker_.metadata(*req));
            co_return out;
        }
        case ApiKey::Produce: {
            const expected<ProduceRequest> req = decode_produce_request(dec, header->api_version);
            if (!req) {
                co_return std::unexpected(req.error());
            }
            encode_produce_response(enc, co_await broker_.produce(*req), header->api_version);
            co_return out;
        }
        case ApiKey::Fetch: {
            const expected<FetchRequest> req = decode_fetch_request(dec, header->api_version);
            if (!req) {
                co_return std::unexpected(req.error());
            }
            encode_fetch_response(enc, co_await broker_.fetch(*req), header->api_version);
            co_return out;
        }
        case ApiKey::ListOffsets: {
            const expected<ListOffsetsRequest> req = decode_list_offsets_request(dec);
            if (!req) {
                co_return std::unexpected(req.error());
            }
            encode_list_offsets_response(enc, co_await broker_.list_offsets(*req));
            co_return out;
        }
    }
    co_return make_error(ErrorCode::Unsupported,
                         "api_key Kafka no soportada: " + std::to_string(header->api_key));
}

}  // namespace nexus::kafka
