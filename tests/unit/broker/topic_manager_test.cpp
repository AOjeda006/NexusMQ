// Pruebas de TopicManager: creación/borrado de topics y apertura de sus particiones.
#include "broker/topic_manager.hpp"

#include <gtest/gtest.h>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <string>
#include <vector>

#include "broker/partition.hpp"
#include "broker/partition_base.hpp"
#include "broker/replicated_partition.hpp"
#include "common/error.hpp"
#include "common/record.hpp"
#include "common/types.hpp"
#include "consensus/raft_carrier.hpp"
#include "consensus/raft_node.hpp"

namespace {

class TempDir {
public:
    explicit TempDir(const char* tag)
        : path_(std::filesystem::temp_directory_path() /
                ("nexus_tm_" + std::string{tag} + "_" +
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

// Batch no idempotente de un record con `payload_len` bytes (rueda segmentos con segment_bytes bajo).
nexus::RecordBatch make_batch(std::size_t payload_len) {
    nexus::RecordBatchHeader header;
    header.producer_id = -1;
    header.producer_epoch = 0;
    header.base_sequence = -1;
    header.record_count = 1;
    return nexus::RecordBatch{header, std::vector<std::byte>(payload_len, std::byte{0xCD})};
}

// TopicConfig con segmentos pequeños (rota pronto) y la retención dada.
nexus::TopicConfig retention_config(std::int64_t retention_bytes, std::int64_t retention_ms) {
    nexus::TopicConfig cfg;
    cfg.segment_bytes = 100;  // rota tras superar 100 bytes
    cfg.retention_bytes = retention_bytes;
    cfg.retention_ms = retention_ms;
    return cfg;
}

// Crea el topic, produce 6 batches en la partición 0 (rueda a >1 segmento) y la devuelve.
nexus::PartitionBase* topic_with_segments(nexus::TopicManager& manager, const nexus::TopicConfig& cfg) {
    EXPECT_TRUE(manager.create_topic("t", 1, cfg).has_value());
    nexus::PartitionBase* part = manager.get("t")->partition(0);
    EXPECT_NE(part, nullptr);
    for (int i = 0; i < 6; ++i) {
        EXPECT_TRUE(part->produce(make_batch(40)).has_value());
    }
    EXPECT_GT(part->log().segment_count(), 1U);
    return part;
}

TEST(TopicManager, EnforceRetentionAll_PorTamano_ReclamaSegmentosViejos) {
    TempDir dir{"ret_size"};
    nexus::TopicManager manager{dir.path()};
    nexus::PartitionBase* part = topic_with_segments(manager, retention_config(150, -1));
    const std::size_t segs_before = part->log().segment_count();

    manager.enforce_retention_all();  // la retención por tamaño no usa el reloj.

    EXPECT_LT(part->log().segment_count(), segs_before);  // reclamó los sellados más antiguos.
    EXPECT_GT(part->log().log_start_offset(), 0);
}

TEST(TopicManager, EnforceRetentionAll_PorTiempo_ReclamaConRelojInyectado) {
    TempDir dir{"ret_time"};
    nexus::TopicManager manager{dir.path()};
    nexus::PartitionBase* part = topic_with_segments(manager, retention_config(-1, 60000));  // 1 min

    // Reloj inyectado 1 hora en el futuro: los segmentos sellados superan 1 min de antigüedad.
    const auto future = std::filesystem::file_time_type::clock::now() + std::chrono::hours(1);
    manager.enforce_retention_all(future);

    EXPECT_GT(part->log().log_start_offset(), 0);  // reclamó los sellados viejos.
}

TEST(TopicManager, EnforceRetentionAll_DentroDePolitica_NoReclama) {
    TempDir dir{"ret_within"};
    nexus::TopicManager manager{dir.path()};
    nexus::PartitionBase* part =
        topic_with_segments(manager, retention_config(/*bytes=*/1'000'000, /*ms=*/60000));
    const std::size_t segs_before = part->log().segment_count();

    // Tamaño holgado y segmentos recientes (reloj actual): dentro de política, no reclama.
    manager.enforce_retention_all(std::filesystem::file_time_type::clock::now());

    EXPECT_EQ(part->log().segment_count(), segs_before);
    EXPECT_EQ(part->log().log_start_offset(), 0);
}

TEST(TopicManager, EnforceRetentionAll_SinPolitica_NoReclama) {
    TempDir dir{"ret_none"};
    nexus::TopicManager manager{dir.path()};
    nexus::PartitionBase* part = topic_with_segments(manager, retention_config(-1, -1));
    const std::size_t segs_before = part->log().segment_count();

    // Sin política aunque el reloj esté muy avanzado: nada se reclama.
    manager.enforce_retention_all(std::filesystem::file_time_type::clock::now() + std::chrono::hours(1));

    EXPECT_EQ(part->log().segment_count(), segs_before);
    EXPECT_EQ(part->log().log_start_offset(), 0);
}

TEST(TopicManager, CreateTopic_AbreParticionesYQuedaAccesible) {
    TempDir dir{"create"};
    nexus::TopicManager manager{dir.path()};

    const nexus::expected<nexus::TopicMetadata> meta = manager.create_topic("eventos", 3);
    ASSERT_TRUE(meta.has_value());
    EXPECT_EQ(meta->name, "eventos");
    EXPECT_EQ(meta->partition_count, 3);
    EXPECT_EQ(manager.topic_count(), 1U);

    nexus::Topic* topic = manager.get("eventos");
    ASSERT_NE(topic, nullptr);
    EXPECT_EQ(topic->partition_count(), 3U);
    EXPECT_NE(topic->partition(0), nullptr);
    EXPECT_NE(topic->partition(2), nullptr);
    EXPECT_EQ(topic->partition(3), nullptr);
}

TEST(TopicManager, CreateTopic_Duplicado_DevuelveInvalidArgument) {
    TempDir dir{"dup"};
    nexus::TopicManager manager{dir.path()};
    ASSERT_TRUE(manager.create_topic("t", 1).has_value());

    const nexus::expected<nexus::TopicMetadata> again = manager.create_topic("t", 1);
    ASSERT_FALSE(again.has_value());
    EXPECT_EQ(again.error().code(), nexus::ErrorCode::InvalidArgument);
}

TEST(TopicManager, CreateTopic_ConteoInvalido_DevuelveInvalidArgument) {
    TempDir dir{"badcount"};
    nexus::TopicManager manager{dir.path()};
    const nexus::expected<nexus::TopicMetadata> meta = manager.create_topic("t", 0);
    ASSERT_FALSE(meta.has_value());
    EXPECT_EQ(meta.error().code(), nexus::ErrorCode::InvalidArgument);
}

TEST(TopicManager, Get_TopicInexistente_DevuelveNullptr) {
    TempDir dir{"absent"};
    nexus::TopicManager manager{dir.path()};
    EXPECT_EQ(manager.get("nope"), nullptr);
}

TEST(TopicManager, DeleteTopic_QuitaDelRegistro) {
    TempDir dir{"del"};
    nexus::TopicManager manager{dir.path()};
    ASSERT_TRUE(manager.create_topic("t", 1).has_value());
    ASSERT_TRUE(manager.delete_topic("t").has_value());
    EXPECT_EQ(manager.get("t"), nullptr);
    EXPECT_EQ(manager.delete_topic("t").error().code(), nexus::ErrorCode::NotFound);
}

TEST(TopicManager, Describe_ListaTopicsConParticiones) {
    TempDir dir{"desc"};
    nexus::TopicManager manager{dir.path()};
    ASSERT_TRUE(manager.create_topic("t", 2).has_value());

    const std::vector<nexus::TopicMeta> metas = manager.describe(/*leader_node_id=*/7);
    ASSERT_EQ(metas.size(), 1U);
    EXPECT_EQ(metas[0].name, "t");
    ASSERT_EQ(metas[0].partitions.size(), 2U);
    EXPECT_EQ(metas[0].partitions[0].leader_node_id, 7);
}

TEST(TopicManager, ProduceTrasCrear_FuncionaSobreLaParticion) {
    TempDir dir{"produce"};
    nexus::TopicManager manager{dir.path()};
    ASSERT_TRUE(manager.create_topic("t", 1).has_value());

    nexus::PartitionBase* part = manager.get("t")->partition(0);
    ASSERT_NE(part, nullptr);
    nexus::RecordBatchHeader header;
    header.record_count = 3;
    const nexus::RecordBatch batch{header, std::vector<std::byte>(12, std::byte{0x5})};
    ASSERT_TRUE(part->produce(batch).has_value());
    EXPECT_EQ(part->high_watermark(), 3);
}

TEST(TopicManager, Sharding_AbreSoloLasParticionesDelNucleo) {
    TempDir dir{"shard"};
    // Núcleo 1 de 3: posee las particiones p con p % 3 == 1 → de [0,5): {1, 4}.
    nexus::TopicManager manager{dir.path(), /*num_cores=*/3, /*owner_core=*/1};
    ASSERT_TRUE(manager.create_topic("t", 5).has_value());

    nexus::Topic* topic = manager.get("t");
    ASSERT_NE(topic, nullptr);
    // Metadatos completos (las 5 particiones), pero solo 2 instaladas localmente.
    EXPECT_EQ(topic->meta().partition_count, 5);
    EXPECT_EQ(topic->partition_count(), 2U);
    EXPECT_NE(topic->partition(1), nullptr);
    EXPECT_NE(topic->partition(4), nullptr);
    EXPECT_EQ(topic->partition(0), nullptr);  // dueño = núcleo 0
    EXPECT_EQ(topic->partition(2), nullptr);  // dueño = núcleo 2
    EXPECT_EQ(topic->partition(3), nullptr);  // dueño = núcleo 0
}

TEST(TopicManager, OwnsPartition_SigueLaReglaModulo) {
    TempDir dir{"owns"};
    nexus::TopicManager manager{dir.path(), /*num_cores=*/4, /*owner_core=*/2};
    EXPECT_EQ(manager.num_cores(), 4);
    EXPECT_EQ(manager.owner_core(), 2);
    EXPECT_TRUE(manager.owns_partition(2));
    EXPECT_TRUE(manager.owns_partition(6));
    EXPECT_FALSE(manager.owns_partition(0));
    EXPECT_FALSE(manager.owns_partition(3));
}

TEST(TopicManager, Sharding_DescribeListaTodasLasParticionesAunqueNoSeanLocales) {
    TempDir dir{"shard_desc"};
    nexus::TopicManager manager{dir.path(), /*num_cores=*/2, /*owner_core=*/0};
    ASSERT_TRUE(manager.create_topic("t", 4).has_value());

    // Metadata anuncia las 4 particiones al cliente aunque este núcleo solo sirva {0, 2}.
    const std::vector<nexus::TopicMeta> metas = manager.describe(/*leader_node_id=*/1);
    ASSERT_EQ(metas.size(), 1U);
    EXPECT_EQ(metas[0].partitions.size(), 4U);
}

TEST(TopicManager, ArgumentosDeShardingInvalidos_SeAcotan) {
    TempDir dir{"clamp"};
    nexus::TopicManager manager{dir.path(), /*num_cores=*/0, /*owner_core=*/9};
    EXPECT_EQ(manager.num_cores(), 1);   // < 1 → 1
    EXPECT_EQ(manager.owner_core(), 0);  // fuera de rango → 0
    ASSERT_TRUE(manager.create_topic("t", 3).has_value());
    EXPECT_EQ(manager.get("t")->partition_count(), 3U);  // num_cores=1 → abre todas
}

// RaftConfig con timeouts cortos para que la elección del votante único sea rápida y determinista.
nexus::RaftConfig fast_raft() {
    using namespace std::chrono_literals;
    nexus::RaftConfig cfg;
    cfg.election_timeout_min = 50ms;
    cfg.election_timeout_max = 60ms;
    cfg.heartbeat_interval = 10ms;
    cfg.random_seed = 42;
    return cfg;
}

TEST(TopicManager, CreateTopicReplicado_CreaReplicatedPartitionConPortador) {
    using namespace std::chrono_literals;
    TempDir dir{"replicado"};
    nexus::TopicManager manager{dir.path(), /*num_cores=*/1, /*owner_core=*/0, /*node_id=*/1,
                                fast_raft()};

    const nexus::expected<nexus::TopicMetadata> meta =
        manager.create_topic("r", 1, {}, /*replication_factor=*/3);
    ASSERT_TRUE(meta.has_value());
    EXPECT_EQ(meta->replication_factor, 3);

    // Hay un portador para la única partición replicada de este núcleo.
    const std::vector<nexus::RaftCarrier*> carriers = manager.carriers();
    ASSERT_EQ(carriers.size(), 1U);
    nexus::PartitionBase* part = manager.get("r")->partition(0);
    ASSERT_NE(part, nullptr);
    EXPECT_FALSE(part->is_leader());  // aún no se ha conducido la FSM

    // Conduce la FSM con on_tick hasta que el votante único se elige líder.
    nexus::MonoTime now{};
    for (int i = 0; i < 200 && !part->is_leader(); ++i) {
        now += 10ms;
        carriers[0]->on_tick(now);
    }
    ASSERT_TRUE(part->is_leader());

    // Ya como líder, produce por la interfaz base y el high-watermark avanza (acks=quórum=1).
    nexus::RecordBatchHeader header;
    header.record_count = 3;
    const nexus::RecordBatch batch{header, std::vector<std::byte>(12, std::byte{0x7})};
    ASSERT_TRUE(part->produce(batch).has_value());
    EXPECT_EQ(part->high_watermark(), 3);
}

TEST(TopicManager, PoliticaDeCompactacion_SePropagaAlPortador) {
    using namespace std::chrono_literals;
    TempDir dir{"compaction"};
    // Umbral bajo (3) para disparar la compactación con pocas entradas: comprueba que la política
    // del manager (último argumento) llega hasta el `RaftCarrier`. (num_cores=1, owner_core=0,
    // node_id=1, sin voter_peers → votante único.)
    const nexus::CompactionPolicy policy{.applied_entries_threshold = 3};
    nexus::TopicManager manager{dir.path(), 1, 0, 1, fast_raft(), {}, policy};
    ASSERT_TRUE(manager.create_topic("r", 1, {}, /*replication_factor=*/3).has_value());

    auto* rep = dynamic_cast<nexus::ReplicatedPartition*>(manager.get("r")->partition(0));
    ASSERT_NE(rep, nullptr);
    const std::vector<nexus::RaftCarrier*> carriers = manager.carriers();
    ASSERT_EQ(carriers.size(), 1U);

    nexus::MonoTime now{};
    for (int i = 0; i < 200 && !rep->is_leader(); ++i) {
        now += 10ms;
        carriers[0]->on_tick(now);
    }
    ASSERT_TRUE(rep->is_leader());
    ASSERT_EQ(rep->raft_log().snapshot_index(), 0);

    nexus::RecordBatchHeader header;
    header.record_count = 1;
    const nexus::RecordBatch batch{header, std::vector<std::byte>(4, std::byte{0x7})};
    for (int i = 0; i < 3; ++i) {
        ASSERT_TRUE(rep->produce(batch).has_value());
    }
    now += 10ms;
    carriers[0]->on_tick(now);
    EXPECT_EQ(rep->raft_log().snapshot_index(), 3);  // el umbral de la política llegó al portador.
}

TEST(TopicManager, CreateTopicNoReplicado_NoTienePortadores) {
    TempDir dir{"no_rep"};
    nexus::TopicManager manager{dir.path()};
    ASSERT_TRUE(manager.create_topic("t", 2).has_value());  // replication_factor por defecto = 1
    EXPECT_TRUE(manager.carriers().empty());
    EXPECT_TRUE(manager.get("t")->partition(0)->is_leader());  // Partition: siempre líder
}

TEST(TopicManager, CarrierFor_DevuelveElPortadorDeLaReplicaONullptr) {
    TempDir dir{"carrier_for"};
    nexus::TopicManager manager{dir.path(), /*num_cores=*/1, /*owner_core=*/0, /*node_id=*/1,
                                fast_raft()};
    ASSERT_TRUE(manager.create_topic("r", 2, {}, /*replication_factor=*/3).has_value());

    nexus::RaftCarrier* carrier = manager.carrier_for("r", 1);
    ASSERT_NE(carrier, nullptr);
    EXPECT_EQ(carrier->topic(), "r");
    EXPECT_EQ(carrier->partition(), 1);

    EXPECT_EQ(manager.carrier_for("r", 99), nullptr);    // partición inexistente
    EXPECT_EQ(manager.carrier_for("otro", 0), nullptr);  // topic inexistente
}

TEST(TopicManager, CarrierFor_TopicNoReplicado_DevuelveNullptr) {
    TempDir dir{"carrier_for_norep"};
    nexus::TopicManager manager{dir.path()};
    ASSERT_TRUE(manager.create_topic("t", 1).has_value());
    EXPECT_EQ(manager.carrier_for("t", 0), nullptr);  // Partition (no replicada): sin portador
}

// --- P4: validación de nombre de topic centralizada en TopicManager (todas las superficies) ---

TEST(TopicManager, CreateTopic_NombreVacio_DevuelveInvalidArgumentSinCrearFicheros) {
    TempDir dir{"empty_name"};
    nexus::TopicManager manager{dir.path()};
    const nexus::expected<nexus::TopicMetadata> meta = manager.create_topic("", 1);
    ASSERT_FALSE(meta.has_value());
    EXPECT_EQ(meta.error().code(), nexus::ErrorCode::InvalidArgument);
    EXPECT_EQ(manager.topic_count(), 0U);
    // Regresión del hallazgo P4: un nombre vacío no debe materializar `data_dir/<partición>`
    // suelta.
    EXPECT_FALSE(std::filesystem::exists(dir.path() / "0"));
}

TEST(TopicManager, CreateTopic_NombreConSeparadoresOReservados_DevuelveInvalidArgument) {
    TempDir dir{"bad_name"};
    nexus::TopicManager manager{dir.path()};
    for (const char* bad : {"a/b", "a\\b", ".", "..", "con espacio", "tab\tulado"}) {
        const nexus::expected<nexus::TopicMetadata> meta = manager.create_topic(bad, 1);
        ASSERT_FALSE(meta.has_value()) << "aceptó un nombre inválido: " << bad;
        EXPECT_EQ(meta.error().code(), nexus::ErrorCode::InvalidArgument);
    }
    EXPECT_EQ(manager.topic_count(), 0U);
}

TEST(TopicManager, CreateTopic_NombreDemasiadoLargo_DevuelveInvalidArgument) {
    TempDir dir{"long_name"};
    nexus::TopicManager manager{dir.path()};
    const std::string too_long(nexus::TopicManager::kMaxTopicNameLength + 1, 'a');
    const nexus::expected<nexus::TopicMetadata> meta = manager.create_topic(too_long, 1);
    ASSERT_FALSE(meta.has_value());
    EXPECT_EQ(meta.error().code(), nexus::ErrorCode::InvalidArgument);
}

TEST(TopicManager, CreateTopic_NombreValidoConGuionPuntoYMayusculas_SeAcepta) {
    TempDir dir{"ok_name"};
    nexus::TopicManager manager{dir.path()};
    EXPECT_TRUE(manager.create_topic("orders.DLQ_v2-2024", 1).has_value());
}

TEST(TopicManager, ValidateTopicName_AplicaLasReglasDeNombrado) {
    using nexus::TopicManager;
    EXPECT_TRUE(TopicManager::validate_topic_name("ok").has_value());
    EXPECT_FALSE(TopicManager::validate_topic_name("").has_value());
    EXPECT_FALSE(TopicManager::validate_topic_name("a b").has_value());
    EXPECT_FALSE(TopicManager::validate_topic_name("a/b").has_value());
    EXPECT_TRUE(
        TopicManager::validate_topic_name(std::string(TopicManager::kMaxTopicNameLength, 'x'))
            .has_value());
    EXPECT_FALSE(
        TopicManager::validate_topic_name(std::string(TopicManager::kMaxTopicNameLength + 1, 'x'))
            .has_value());
}

// --- P3: delete_topic borra los datos en disco (sin fuga ni resurrección) ---

TEST(TopicManager, DeleteTopic_BorraLosFicherosEnDisco) {
    TempDir dir{"del_disk"};
    nexus::TopicManager manager{dir.path()};
    ASSERT_TRUE(manager.create_topic("t", 3).has_value());
    nexus::PartitionBase* part = manager.get("t")->partition(0);
    ASSERT_NE(part, nullptr);
    nexus::RecordBatchHeader header;
    header.record_count = 2;
    const nexus::RecordBatch batch{header, std::vector<std::byte>(8, std::byte{0x5})};
    ASSERT_TRUE(part->produce(batch).has_value());
    ASSERT_TRUE(std::filesystem::exists(dir.path() / "t"));

    ASSERT_TRUE(manager.delete_topic("t").has_value());
    EXPECT_EQ(manager.get("t"), nullptr);
    // Regresión del hallazgo P3: el directorio del topic y todas sus particiones desaparecen.
    EXPECT_FALSE(std::filesystem::exists(dir.path() / "t"));
}

TEST(TopicManager, DeleteTopic_NoResucita_ReDeclararArrancaVacio) {
    TempDir dir{"del_norez"};
    nexus::TopicManager manager{dir.path()};
    ASSERT_TRUE(manager.create_topic("t", 1).has_value());
    nexus::RecordBatchHeader header;
    header.record_count = 3;
    const nexus::RecordBatch batch{header, std::vector<std::byte>(12, std::byte{0x7})};
    ASSERT_TRUE(manager.get("t")->partition(0)->produce(batch).has_value());
    EXPECT_EQ(manager.get("t")->partition(0)->high_watermark(), 3);

    ASSERT_TRUE(manager.delete_topic("t").has_value());
    // Re-declarar el mismo nombre arranca vacío (sin resurrección de los datos "borrados").
    ASSERT_TRUE(manager.create_topic("t", 1).has_value());
    EXPECT_EQ(manager.get("t")->partition(0)->high_watermark(), 0);
}

}  // namespace
