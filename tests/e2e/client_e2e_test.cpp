// E2E del cliente nativo: el `Client`/`Producer`/`Consumer` de nexus-client contra un `Server` real
// en su propio hilo (reactor io_uring). Cierra el lazo de alto nivel: publicar valores y volver a
// leerlos con sus offsets, además de crear topics y pedir metadata por el cliente.
#include <gtest/gtest.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <vector>

#include "client/client.hpp"
#include "client/consumer.hpp"
#include "client/endpoint.hpp"
#include "client/producer.hpp"
#include "common/bytes.hpp"
#include "common/error.hpp"
#include "common/types.hpp"
#include "protocol/error_code.hpp"
#include "protocol/messages.hpp"
#include "server/server.hpp"

namespace {

class TempDir {
public:
    explicit TempDir(const char* tag)
        : path_(std::filesystem::temp_directory_path() /
                ("nexus_client_e2e_" + std::string{tag} + "_" +
                 std::to_string(::testing::UnitTest::GetInstance()->random_seed()))) {
        std::filesystem::remove_all(path_);
    }
    ~TempDir() { std::filesystem::remove_all(path_); }
    TempDir(const TempDir&) = delete;
    TempDir& operator=(const TempDir&) = delete;

    [[nodiscard]] const std::filesystem::path& path() const noexcept { return path_; }

private:
    std::filesystem::path path_;
};

// Levanta un Server en su propio hilo; gestiona bind/run/stop/join con RAII. GTEST_SKIP si io_uring
// no está disponible (se comprueba antes de usar la fixture).
class ServerFixture {
public:
    explicit ServerFixture(const std::filesystem::path& data_dir) {
        nexus::Server::Config config;
        config.host = "127.0.0.1";
        config.port = 0;  // efímero
        config.data_dir = data_dir;
        config.advertised_host = "127.0.0.1";
        server_.emplace(std::move(config));
    }
    ~ServerFixture() {
        if (running_) {
            server_->stop();
            thread_.join();
        }
    }
    ServerFixture(const ServerFixture&) = delete;
    ServerFixture& operator=(const ServerFixture&) = delete;

    [[nodiscard]] bool start() {
        if (!server_->bind()) {
            return false;
        }
        port_ = server_->port();
        thread_ = std::thread{[this] { server_->run(); }};
        running_ = true;
        return port_ != 0;
    }

    [[nodiscard]] std::uint16_t port() const noexcept { return port_; }

private:
    std::optional<nexus::Server> server_;
    std::thread thread_;
    std::uint16_t port_ = 0;
    bool running_ = false;
};

// Convierte un literal en una vista de bytes (sin el terminador nulo).
nexus::ByteSpan bytes_of(std::string_view text) {
    return nexus::ByteSpan{
        reinterpret_cast<const std::byte*>(text.data()),  // NOLINT(*-reinterpret-cast)
        text.size()};
}

std::string_view as_text(const std::vector<std::byte>& bytes) {
    return std::string_view{reinterpret_cast<const char*>(bytes.data()),
                            bytes.size()};  // NOLINT(*-reinterpret-cast)
}

// Texto del valor de un record consumido (asume que el valor está presente, no es tombstone).
std::string_view value_text(const nexus::ConsumedRecord& rec) {
    return as_text(rec.value.value());
}

TEST(ClientE2E, ProducerConsumer_RoundTripConOffsets) {
    TempDir dir{"roundtrip"};
    std::optional<ServerFixture> fixture;
    try {
        fixture.emplace(dir.path());
    } catch (const std::system_error& ex) {
        GTEST_SKIP() << "io_uring no disponible en este entorno: " << ex.what();
    }
    ASSERT_TRUE(fixture->start());

    nexus::expected<nexus::Client> client_exp =
        nexus::Client::connect(nexus::Endpoint{.host = "127.0.0.1", .port = fixture->port()});
    ASSERT_TRUE(client_exp.has_value());
    nexus::Client& client = *client_exp;

    // Crear el topic por el propio cliente.
    const nexus::expected<nexus::CreateTopicResponse> created = client.create_topic("orders", 1);
    ASSERT_TRUE(created.has_value());
    EXPECT_EQ(created->error_code, nexus::WireError::None);

    // Publicar tres valores; offsets esperados 0, 1, 2.
    nexus::Producer producer = client.producer();
    const nexus::expected<nexus::Offset> off_a = producer.send("orders", 0, bytes_of("alpha"));
    ASSERT_TRUE(off_a.has_value());
    EXPECT_EQ(*off_a, 0);
    const nexus::expected<nexus::Offset> off_b = producer.send("orders", 0, bytes_of("beta"));
    ASSERT_TRUE(off_b.has_value());
    EXPECT_EQ(*off_b, 1);
    const nexus::expected<nexus::Offset> off_c = producer.send("orders", 0, bytes_of("gamma"));
    ASSERT_TRUE(off_c.has_value());
    EXPECT_EQ(*off_c, 2);

    // Consumir desde el principio: tres records con sus offsets y valores.
    nexus::Consumer consumer = client.consumer("orders", 0);
    const nexus::expected<std::vector<nexus::ConsumedRecord>> first = consumer.poll();
    ASSERT_TRUE(first.has_value());
    ASSERT_EQ(first->size(), 3U);
    EXPECT_EQ((*first)[0].offset, 0);
    EXPECT_EQ(value_text((*first)[0]), "alpha");
    EXPECT_EQ((*first)[1].offset, 1);
    EXPECT_EQ(value_text((*first)[1]), "beta");
    EXPECT_EQ((*first)[2].offset, 2);
    EXPECT_EQ(value_text((*first)[2]), "gamma");
    EXPECT_EQ(consumer.position(), 3);

    // Al día: el siguiente poll no devuelve nada.
    const nexus::expected<std::vector<nexus::ConsumedRecord>> second = consumer.poll();
    ASSERT_TRUE(second.has_value());
    EXPECT_TRUE(second->empty());

    client.close();
}

TEST(ClientE2E, SendBatch_AsignaOffsetsConsecutivos) {
    TempDir dir{"batch"};
    std::optional<ServerFixture> fixture;
    try {
        fixture.emplace(dir.path());
    } catch (const std::system_error& ex) {
        GTEST_SKIP() << "io_uring no disponible en este entorno: " << ex.what();
    }
    ASSERT_TRUE(fixture->start());

    nexus::expected<nexus::Client> client_exp =
        nexus::Client::connect(nexus::Endpoint{.host = "127.0.0.1", .port = fixture->port()});
    ASSERT_TRUE(client_exp.has_value());
    nexus::Client& client = *client_exp;
    ASSERT_TRUE(client.create_topic("t", 1).has_value());

    nexus::Producer producer = client.producer();
    const std::array<nexus::ByteSpan, 3> values{bytes_of("uno"), bytes_of("dos"), bytes_of("tres")};
    const nexus::expected<nexus::Offset> base = producer.send_batch("t", 0, values);
    ASSERT_TRUE(base.has_value());
    EXPECT_EQ(*base, 0);

    nexus::Consumer consumer = client.consumer("t", 0);
    const nexus::expected<std::vector<nexus::ConsumedRecord>> records = consumer.poll();
    ASSERT_TRUE(records.has_value());
    ASSERT_EQ(records->size(), 3U);
    EXPECT_EQ(value_text((*records)[0]), "uno");
    EXPECT_EQ(value_text((*records)[2]), "tres");
    EXPECT_EQ((*records)[2].offset, 2);
}

TEST(ClientE2E, SendKeyedYTombstone_RoundTripDeClaveYBorrado) {
    TempDir dir{"keyed"};
    std::optional<ServerFixture> fixture;
    try {
        fixture.emplace(dir.path());
    } catch (const std::system_error& ex) {
        GTEST_SKIP() << "io_uring no disponible en este entorno: " << ex.what();
    }
    ASSERT_TRUE(fixture->start());

    nexus::expected<nexus::Client> client_exp =
        nexus::Client::connect(nexus::Endpoint{.host = "127.0.0.1", .port = fixture->port()});
    ASSERT_TRUE(client_exp.has_value());
    nexus::Client& client = *client_exp;
    ASSERT_TRUE(client.create_topic("kv", 1).has_value());

    nexus::Producer producer = client.producer();
    ASSERT_TRUE(producer.send_keyed("kv", 0, bytes_of("user-1"), bytes_of("activo")).has_value());
    ASSERT_TRUE(producer.send_tombstone("kv", 0, bytes_of("user-1")).has_value());

    nexus::Consumer consumer = client.consumer("kv", 0);
    const nexus::expected<std::vector<nexus::ConsumedRecord>> records = consumer.poll();
    ASSERT_TRUE(records.has_value());
    ASSERT_EQ(records->size(), 2U);

    // Record con clave y valor.
    ASSERT_TRUE((*records)[0].key.has_value());
    EXPECT_EQ(as_text(*(*records)[0].key), "user-1");
    ASSERT_TRUE((*records)[0].value.has_value());
    EXPECT_EQ(value_text((*records)[0]), "activo");

    // Tombstone: misma clave, value nulo.
    ASSERT_TRUE((*records)[1].key.has_value());
    EXPECT_EQ(as_text(*(*records)[1].key), "user-1");
    EXPECT_FALSE((*records)[1].value.has_value());
}

TEST(ClientE2E, Metadata_ListaBrokerYTopic) {
    TempDir dir{"meta"};
    std::optional<ServerFixture> fixture;
    try {
        fixture.emplace(dir.path());
    } catch (const std::system_error& ex) {
        GTEST_SKIP() << "io_uring no disponible en este entorno: " << ex.what();
    }
    ASSERT_TRUE(fixture->start());

    nexus::expected<nexus::Client> client_exp =
        nexus::Client::connect(nexus::Endpoint{.host = "127.0.0.1", .port = fixture->port()});
    ASSERT_TRUE(client_exp.has_value());
    nexus::Client& client = *client_exp;
    ASSERT_TRUE(client.create_topic("eventos", 2).has_value());

    const nexus::expected<nexus::MetadataResponse> meta = client.metadata();
    ASSERT_TRUE(meta.has_value());
    ASSERT_EQ(meta->brokers.size(), 1U);
    EXPECT_EQ(meta->brokers[0].port, fixture->port());
    ASSERT_EQ(meta->topics.size(), 1U);
    EXPECT_EQ(meta->topics[0].name, "eventos");
    EXPECT_EQ(meta->topics[0].partitions.size(), 2U);
}

TEST(ClientE2E, CommitYFetchOffset_PersistenEnElBroker) {
    TempDir dir{"offset"};
    std::optional<ServerFixture> fixture;
    try {
        fixture.emplace(dir.path());
    } catch (const std::system_error& ex) {
        GTEST_SKIP() << "io_uring no disponible en este entorno: " << ex.what();
    }
    ASSERT_TRUE(fixture->start());

    nexus::expected<nexus::Client> client_exp =
        nexus::Client::connect(nexus::Endpoint{.host = "127.0.0.1", .port = fixture->port()});
    ASSERT_TRUE(client_exp.has_value());
    nexus::Client& client = *client_exp;
    ASSERT_TRUE(client.create_topic("t", 1).has_value());

    // Sin commit previo: el broker devuelve -1 (sin error).
    const nexus::expected<nexus::OffsetFetchResponse> before = client.fetch_offset("grp", "t", 0);
    ASSERT_TRUE(before.has_value());
    EXPECT_EQ(before->error_code, nexus::WireError::None);
    EXPECT_EQ(before->offset, -1);

    // Confirmar y volver a leer.
    const nexus::expected<nexus::OffsetCommitResponse> committed =
        client.commit_offset("grp", "t", 0, 5);
    ASSERT_TRUE(committed.has_value());
    EXPECT_EQ(committed->error_code, nexus::WireError::None);

    const nexus::expected<nexus::OffsetFetchResponse> after = client.fetch_offset("grp", "t", 0);
    ASSERT_TRUE(after.has_value());
    EXPECT_EQ(after->offset, 5);

    // Otra conexión ve el mismo commit (vive en el broker, no en el cliente).
    nexus::expected<nexus::Client> other_exp =
        nexus::Client::connect(nexus::Endpoint{.host = "127.0.0.1", .port = fixture->port()});
    ASSERT_TRUE(other_exp.has_value());
    const nexus::expected<nexus::OffsetFetchResponse> from_other =
        other_exp->fetch_offset("grp", "t", 0);
    ASSERT_TRUE(from_other.has_value());
    EXPECT_EQ(from_other->offset, 5);
}

TEST(ClientE2E, Produce_TopicInexistente_DevuelveError) {
    TempDir dir{"notopic"};
    std::optional<ServerFixture> fixture;
    try {
        fixture.emplace(dir.path());
    } catch (const std::system_error& ex) {
        GTEST_SKIP() << "io_uring no disponible en este entorno: " << ex.what();
    }
    ASSERT_TRUE(fixture->start());

    nexus::expected<nexus::Client> client_exp =
        nexus::Client::connect(nexus::Endpoint{.host = "127.0.0.1", .port = fixture->port()});
    ASSERT_TRUE(client_exp.has_value());
    nexus::Client& client = *client_exp;

    nexus::Producer producer = client.producer();
    const nexus::expected<nexus::Offset> result = producer.send("inexistente", 0, bytes_of("x"));
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code(), nexus::ErrorCode::NotFound);
}

}  // namespace
