# syntax=docker/dockerfile:1
#
# Imagen del broker NexusMQ. Build multi-stage → runtime distroless, no-root.
# Construir desde la RAÍZ del repo (el contexto es el árbol completo):
#
#     docker build -f deploy/Dockerfile -t nexusmq:dev .
#
# El kernel del host debe soportar io_uring (el broker usa el backend io_uring directo).

# ---- builder: toolchain completo, compila en Release ----------------------------------------------
FROM ubuntu:24.04 AS builder

# g++/cmake/ninja para compilar; libssl-dev habilita el plano TLS (find_package(OpenSSL)).
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      g++ cmake ninja-build libssl-dev ca-certificates \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY . .

# Release optimizado, sin tests/benchmarks (NEXUS_BUILD_TESTS=OFF evita bajar GoogleTest/Benchmark);
# -Werror sigue activo. Solo se compilan los dos binarios de despliegue: nexusd y nexus-cli.
RUN cmake -S . -B build -G Ninja \
      -DCMAKE_CXX_COMPILER=g++ \
      -DCMAKE_BUILD_TYPE=Release \
      -DNEXUS_BUILD_TESTS=OFF \
 && cmake --build build --target nexusd nexus-cli

# Directorio de datos vacío: se copia con la propiedad del uid no-root para que los volúmenes
# nuevos hereden permisos escribibles (distroless no trae shell para hacer chown en runtime).
RUN mkdir -p /nexus-empty

# ---- runtime: distroless, no-root ----------------------------------------------------------------
# distroless/cc trae glibc + libstdc++ + libssl (runtime), justo lo que enlazan los binarios.
FROM gcr.io/distroless/cc-debian12:nonroot AS runtime

LABEL org.opencontainers.image.title="NexusMQ" \
      org.opencontainers.image.description="Broker de mensajería distribuido (C++23, thread-per-core, Raft)" \
      org.opencontainers.image.source="https://github.com/AOjeda006/NexusMQ" \
      org.opencontainers.image.licenses="LicenseRef-PolyForm-Noncommercial-1.0.0"

COPY --from=builder /src/build/src/server/nexusd /usr/local/bin/nexusd
COPY --from=builder /src/build/tools/cli/nexus-cli /usr/local/bin/nexus-cli
COPY --from=builder --chown=65532:65532 /nexus-empty /var/lib/nexusmq

# 9092 = plano de datos (protocolo binario); 9644 = puerto de operación (REST admin/health/metrics).
EXPOSE 9092 9644

# El volumen de datos hereda la propiedad del uid no-root (65532) del directorio copiado arriba.
VOLUME ["/var/lib/nexusmq"]

# Distroless no trae shell ni curl: el healthcheck reutiliza el propio CLI, que sale !=0 si el nodo
# no está vivo y listo (consulta /healthz y /readyz del puerto de operación).
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
  CMD ["/usr/local/bin/nexus-cli", "--port", "9644", "diagnostics"]

ENTRYPOINT ["/usr/local/bin/nexusd"]
CMD ["--host", "0.0.0.0", "--port", "9092", "--admin-port", "9644", "--data-dir", "/var/lib/nexusmq"]
