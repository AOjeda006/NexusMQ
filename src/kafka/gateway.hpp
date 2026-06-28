/// @file   kafka/gateway.hpp
/// @brief  Dispatcher del subconjunto Kafka: enruta una petición a la API correspondiente — F7e.
/// @ingroup kafka

#pragma once

#include "common/bytes.hpp"
#include "common/error.hpp"
#include "common/task.hpp"
#include "kafka/fetch.hpp"
#include "kafka/list_offsets.hpp"
#include "kafka/metadata.hpp"
#include "kafka/produce.hpp"

namespace nexus::kafka {

/// @brief **Puerto** hacia el broker para las APIs de datos/metadatos de Kafka. Afinidad: depende
///   de la implementación (el adaptador real vive en el servidor; los tests inyectan un doble).
/// @details Separa el **protocolo** (este target, `nexus-kafka`, puro) del **broker**
/// (`nexus-broker`):
///   el `KafkaGateway` decodifica/serializa y delega la lógica en esta interfaz (inyección de
///   dependencias). Las respuestas llevan los `error_code` de Kafka en su propio cuerpo, así que
///   los métodos devuelven la respuesta por valor (no `expected`): un fallo de negocio es un código
///   de error en la respuesta, no un fallo de transporte.
/// @note Métodos **asíncronos** (`task<...>`): el adaptador real reparte cada partición a su
///   reactor dueño por paso de mensajes (`PartitionRouter`/`call_on`, ADR-0026), como el camino
///   nativo (`RequestRouter`). Un doble de test puede devolver el valor con una `task` que completa
///   inline (sin suspenderse). Los métodos no son `[[nodiscard]]` aquí porque la `task` perezosa ya
///   lo es al construirse.
class KafkaBroker {
public:
    KafkaBroker() = default;
    KafkaBroker(const KafkaBroker&) = delete;
    KafkaBroker& operator=(const KafkaBroker&) = delete;
    KafkaBroker(KafkaBroker&&) = delete;
    KafkaBroker& operator=(KafkaBroker&&) = delete;
    virtual ~KafkaBroker() = default;

    /// Describe el clúster y los topics pedidos (`nullopt` = todos) para una respuesta Metadata.
    virtual task<MetadataResponse> metadata(const MetadataRequest& req) = 0;
    /// Anexa los records de la petición a las particiones correspondientes.
    virtual task<ProduceResponse> produce(const ProduceRequest& req) = 0;
    /// Lee records desde las particiones/offsets pedidos.
    virtual task<FetchResponse> fetch(const FetchRequest& req) = 0;
    /// Resuelve el offset de inicio/fin de las particiones pedidas (para el arranque del
    /// consumidor).
    virtual task<ListOffsetsResponse> list_offsets(const ListOffsetsRequest& req) = 0;
};

/// @brief Enrutador de peticiones Kafka: cabecera → API → respuesta serializada. Afinidad:
///   REACTOR-LOCAL (uno por conexión/núcleo; no comparte estado).
/// @details `ApiVersions` se resuelve aquí (anuncia `supported_apis()`);
/// `Metadata`/`Produce`/`Fetch`
///   se delegan al `KafkaBroker` inyectado. No gestiona el *framing* (prefijo `Size:INT32`): opera
///   sobre el cuerpo del frame (cabecera + cuerpo) y devuelve el cuerpo de la respuesta; el prefijo
///   de tamaño lo añade el transporte.
class KafkaGateway {
public:
    explicit KafkaGateway(KafkaBroker& broker) noexcept : broker_(broker) {}

    /// @brief Procesa una petición (frame **sin** el prefijo `Size`) y devuelve la respuesta
    ///   serializada (también sin `Size`).
    /// @return La respuesta, o un error si la petición está malformada o la `api_key` no se
    /// soporta.
    /// @pre @p request debe seguir vivo hasta que la `task` complete (la petición se decodifica
    ///   sobre esa vista; el transporte la mantiene en el *frame* de la conexión).
    [[nodiscard]] task<expected<Buffer>> handle_request(ByteSpan request);

private:
    KafkaBroker& broker_;  // NOLINT(cppcoreguidelines-avoid-const-or-ref-data-members)
};

}  // namespace nexus::kafka
