# nexus-kafka — subconjunto compatible con Apache Kafka (F7): codec de wire big-endian y mensajes
# de las APIs ApiVersions/Metadata/Produce/Fetch, para hablar con clientes como kcat/librdkafka.
add_library(nexus-kafka
  codec.cpp)
add_library(nexus::kafka ALIAS nexus-kafka)

target_include_directories(nexus-kafka PUBLIC ${PROJECT_SOURCE_DIR}/src)
target_link_libraries(nexus-kafka PUBLIC nexus::common nexus_options)
