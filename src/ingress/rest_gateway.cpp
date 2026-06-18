/// @file   ingress/rest_gateway.cpp
/// @brief  Implementación del enrutado del REST admin sobre AdminService.
/// @ingroup ingress

#include "ingress/rest_gateway.hpp"

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/error.hpp"
#include "ingress/json.hpp"
#include "ingress/json_value.hpp"
#include "ingress/problem_detail.hpp"

namespace nexus {

namespace {

/// Construye una respuesta JSON con @p status y @p body.
HttpResponse json_response(int status, std::string body) {
    HttpResponse response;
    response.status = status;
    response.reason = std::string{http_reason(status)};
    response.set_header("Content-Type", "application/json");
    response.body = std::move(body);
    return response;
}

/// Respuesta RFC 7807 a partir de un `Error` del núcleo (mapea código→HTTP).
HttpResponse problem_response(const Error& error, std::string_view instance) {
    return problem_from_error(error, std::string{instance}).to_response();
}

/// Respuesta RFC 7807 con @p status/@p title/@p detail explícitos (auth, 404, 405…).
HttpResponse problem_explicit(int status, std::string detail, std::string_view instance) {
    ProblemDetail problem;
    problem.status = status;
    problem.title = std::string{http_reason(status)};
    problem.detail = std::move(detail);
    problem.instance = std::string{instance};
    return problem.to_response();
}

/// Serializa un `TopicSummary` como objeto JSON dentro de @p writer.
void write_topic_summary(JsonWriter& writer, const TopicSummary& topic) {
    writer.begin_object();
    writer.field("name", topic.name);
    writer.field("partitionCount", static_cast<std::int64_t>(topic.partition_count));
    writer.field("replicationFactor", static_cast<std::int64_t>(topic.replication_factor));
    writer.field("createdAtMs", topic.created_at_ms);
    writer.end_object();
}

/// Serializa una colección paginada `{page, size, items:[...]}` con @p emit por elemento.
template <class T, class Emit>
std::string paged_json(Page page, const std::vector<T>& items, Emit emit) {
    JsonWriter writer;
    writer.begin_object();
    writer.field("page", static_cast<std::int64_t>(page.number));
    writer.field("size", static_cast<std::int64_t>(page.size));
    writer.key("items").begin_array();
    for (const T& item : items) {
        emit(writer, item);
    }
    writer.end_array();
    writer.end_object();
    return writer.take();
}

/// Lee un campo numérico opcional del objeto JSON (lo deja igual si falta o no es número).
void read_optional_int(const JsonValue& object, std::string_view key, std::int64_t& out) {
    if (const JsonValue* value = object.find(key); value != nullptr && value->is_number()) {
        out = value->as_int64();
    }
}

}  // namespace

RestGateway::RestGateway(AdminService& admin, const JwtVerifier* verifier)
    : RestGateway(admin, verifier, Config{}) {}

RestGateway::RestGateway(AdminService& admin, const JwtVerifier* verifier, Config config)
    : admin_(admin), verifier_(verifier), config_(std::move(config)) {}

expected<Principal> RestGateway::authenticate(const HttpRequest& request,
                                              std::int64_t now_unix_seconds) const {
    if (verifier_ == nullptr) {
        return Principal{};  // autenticación desactivada.
    }
    const std::optional<std::string_view> authorization = request.header("Authorization");
    if (!authorization) {
        return make_error(ErrorCode::InvalidArgument, "falta la cabecera Authorization");
    }
    constexpr std::string_view kBearer = "Bearer ";
    const std::string_view value = *authorization;
    if (!value.starts_with(kBearer)) {
        return make_error(ErrorCode::InvalidArgument,
                          "se requiere 'Authorization: Bearer <token>'");
    }
    return verifier_->verify(value.substr(kBearer.size()), now_unix_seconds);
}

HttpResponse RestGateway::handle(const HttpRequest& request, std::int64_t now_unix_seconds) const {
    const std::string_view path = request.path();
    if (const auto principal = authenticate(request, now_unix_seconds); !principal) {
        return problem_explicit(401, std::string{principal.error().message()}, path);
    }
    if (!path.starts_with(config_.api_prefix)) {
        return problem_explicit(404, "recurso no encontrado", path);
    }
    const std::string_view resource = path.substr(config_.api_prefix.size());
    if (resource == "/topics" || resource.starts_with("/topics/")) {
        return route_topics(request, resource);
    }
    if (resource == "/groups") {
        return route_groups(request);
    }
    return problem_explicit(404, "recurso no encontrado", path);
}

HttpResponse RestGateway::route_topics(const HttpRequest& request,
                                       std::string_view resource) const {
    const std::string_view rest = resource.substr(std::string_view{"/topics"}.size());
    if (rest.empty()) {
        if (request.method == HttpMethod::Get) {
            return list_topics(request);
        }
        if (request.method == HttpMethod::Post) {
            return create_topic(request);
        }
        return problem_explicit(405, "método no permitido en /topics", request.path());
    }
    const std::string_view name = rest.substr(1);  // rest empieza por '/'.
    if (name.empty() || name.contains('/')) {
        return problem_explicit(404, "recurso no encontrado", request.path());
    }
    if (request.method == HttpMethod::Get) {
        return describe_topic(request, name);
    }
    if (request.method == HttpMethod::Delete) {
        return delete_topic(name);
    }
    return problem_explicit(405, "método no permitido en /topics/{name}", request.path());
}

HttpResponse RestGateway::list_topics(const HttpRequest& request) const {
    const auto page = parse_pagination(request.query(), config_.pagination);
    if (!page) {
        return problem_response(page.error(), request.path());
    }
    const std::vector<TopicSummary> topics = admin_.list_topics(*page);
    return json_response(200, paged_json(*page, topics, write_topic_summary));
}

HttpResponse RestGateway::create_topic(const HttpRequest& request) const {
    const auto json = parse_json(request.body);
    if (!json || !json->is_object()) {
        return problem_explicit(400, "el cuerpo debe ser un objeto JSON", request.path());
    }
    const JsonValue* name = json->find("name");
    if (name == nullptr || !name->is_string()) {
        return problem_explicit(400, "falta el campo 'name' (cadena)", request.path());
    }
    CreateTopicSpec spec;
    spec.name = name->as_string();
    std::int64_t partition_count = spec.partition_count;
    std::int64_t replication_factor = spec.replication_factor;
    read_optional_int(*json, "partitionCount", partition_count);
    read_optional_int(*json, "replicationFactor", replication_factor);
    read_optional_int(*json, "segmentBytes", spec.segment_bytes);
    read_optional_int(*json, "retentionMs", spec.retention_ms);
    read_optional_int(*json, "retentionBytes", spec.retention_bytes);
    spec.partition_count = static_cast<std::int32_t>(partition_count);
    spec.replication_factor = static_cast<std::int16_t>(replication_factor);

    const auto summary = admin_.create_topic(spec);
    if (!summary) {
        return problem_response(summary.error(), request.path());
    }
    JsonWriter writer;
    write_topic_summary(writer, *summary);
    HttpResponse response = json_response(201, writer.take());
    response.set_header("Location", config_.api_prefix + "/topics/" + summary->name);
    return response;
}

HttpResponse RestGateway::describe_topic(const HttpRequest& request, std::string_view name) const {
    const auto description = admin_.describe_topic(name);
    if (!description) {
        return problem_response(description.error(), request.path());
    }
    JsonWriter writer;
    writer.begin_object();
    writer.field("name", description->summary.name);
    writer.field("partitionCount", static_cast<std::int64_t>(description->summary.partition_count));
    writer.field("replicationFactor",
                 static_cast<std::int64_t>(description->summary.replication_factor));
    writer.field("createdAtMs", description->summary.created_at_ms);
    writer.key("partitions").begin_array();
    for (const PartitionInfo& partition : description->partitions) {
        writer.begin_object();
        writer.field("id", static_cast<std::int64_t>(partition.id));
        writer.field("leader", static_cast<std::int64_t>(partition.leader));
        writer.field("highWatermark", partition.high_watermark);
        writer.field("leaderEpoch", partition.leader_epoch);
        writer.end_object();
    }
    writer.end_array();
    writer.end_object();
    return json_response(200, writer.take());
}

HttpResponse RestGateway::delete_topic(std::string_view name) const {
    const auto result = admin_.delete_topic(name);
    if (!result) {
        return problem_response(result.error(),
                                std::string{config_.api_prefix} + "/topics/" + std::string{name});
    }
    HttpResponse response;
    response.status = 204;
    response.reason = std::string{http_reason(204)};
    return response;
}

HttpResponse RestGateway::route_groups(const HttpRequest& request) const {
    if (request.method != HttpMethod::Get) {
        return problem_explicit(405, "método no permitido en /groups", request.path());
    }
    const auto page = parse_pagination(request.query(), config_.pagination);
    if (!page) {
        return problem_response(page.error(), request.path());
    }
    const std::vector<GroupSummary> groups = admin_.list_groups(*page);
    return json_response(
        200, paged_json(*page, groups, [](JsonWriter& writer, const GroupSummary& group) {
            writer.begin_object();
            writer.field("groupId", group.group_id);
            writer.field("state", group.state);
            writer.field("generation", static_cast<std::int64_t>(group.generation));
            writer.field("memberCount", group.member_count);
            writer.end_object();
        }));
}

}  // namespace nexus
