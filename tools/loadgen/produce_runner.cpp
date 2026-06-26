/// @file   tools/loadgen/produce_runner.cpp
/// @brief  Implementación de ProduceRunner (ver produce_runner.hpp).
/// @ingroup loadgen

#include "produce_runner.hpp"

#include <expected>
#include <utility>

#include "client/producer.hpp"
#include "common/bytes.hpp"

namespace nexus::loadgen {

namespace {
/// Byte de relleno del payload sintético (irrelevante: solo cuenta su tamaño en el wire).
constexpr std::byte kFillByte{0x42};
}  // namespace

ProduceRunner::ProduceRunner(Client client, std::string topic, PartitionId partition,
                             std::vector<std::byte> payload) noexcept
    : client_(std::move(client)),
      topic_(std::move(topic)),
      partition_(partition),
      payload_(std::move(payload)) {}

expected<std::unique_ptr<ProduceRunner>> ProduceRunner::create(const Endpoint& endpoint,
                                                               std::string topic,
                                                               PartitionId partition,
                                                               std::size_t payload_size) {
    expected<Client> client = Client::connect(endpoint);
    if (!client) {
        return std::unexpected<Error>(client.error());
    }
    std::vector<std::byte> payload(payload_size, kFillByte);
    return std::make_unique<ProduceRunner>(std::move(*client), std::move(topic), partition,
                                           std::move(payload));
}

expected<void> ProduceRunner::run_once() {
    Producer producer{client_};
    const ByteSpan value{payload_.data(), payload_.size()};
    expected<Offset> sent = producer.send(topic_, partition_, value);
    if (!sent) {
        return std::unexpected<Error>(sent.error());
    }
    return {};
}

}  // namespace nexus::loadgen
