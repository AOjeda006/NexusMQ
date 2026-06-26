#!/usr/bin/env bash
# Campaña de latencia end-to-end de NexusMQ (Bloque L / L2).
#
# Arranca un `nexusd` real, lanza el generador de carga open-loop `nexus-loadgen` contra él por la
# red (loopback) a varias tasas fijas y publica una tabla con p50/p99/p999/max (corregidos de
# coordinated omission) y el throughput logrado frente al objetivo (comprobación de saturación del
# método USE, ver fundamentos/rendimiento/). Reproducible: un binario optimizado, un solo árbol.
#
# Uso:
#   scripts/bench/latency_campaign.sh [BUILD_DIR]
# Variables de entorno (con valores por defecto):
#   PAYLOAD=256          tamaño del mensaje en bytes
#   CONNECTIONS=4        conexiones (= hilos) concurrentes del generador
#   DURATION=5           segundos objetivo por nivel de carga (fija op_count = tasa * DURATION)
#   RATES="..."          lista de tasas (req/s) a barrer; 0 = sin ritmo (throughput máximo)
set -euo pipefail

BUILD_DIR="${1:-build/linux-gcc-release}"
PAYLOAD="${PAYLOAD:-256}"
CONNECTIONS="${CONNECTIONS:-4}"
DURATION="${DURATION:-5}"
RATES="${RATES:-2000 5000 10000 20000 40000 0}"

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${repo_root}"

nexusd="${BUILD_DIR}/src/server/nexusd"
loadgen="${BUILD_DIR}/tools/loadgen/nexus-loadgen"
for bin in "${nexusd}" "${loadgen}"; do
  if [[ ! -x "${bin}" ]]; then
    echo "error: no existe ${bin}. Compila Release primero:" >&2
    echo "  cmake --preset linux-gcc-release && cmake --build --preset linux-gcc-release" >&2
    exit 1
  fi
done

# Puerto efímero libre (lo reserva el SO y lo cerramos; carrera improbable en un entorno de bench).
port="$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()')"
data_dir="$(mktemp -d "${TMPDIR:-/tmp}/nexus_bench_XXXXXX")"
topic="bench"
server_log="${data_dir}/nexusd.log"

server_pid=""
cleanup() {
  if [[ -n "${server_pid}" ]] && kill -0 "${server_pid}" 2>/dev/null; then
    kill "${server_pid}" 2>/dev/null || true
    wait "${server_pid}" 2>/dev/null || true
  fi
  rm -rf "${data_dir}"
}
trap cleanup EXIT

echo "# NexusMQ — campaña de latencia (L2)"
echo "broker=127.0.0.1:${port}  payload=${PAYLOAD}B  connections=${CONNECTIONS}  duration/level=${DURATION}s"
echo

"${nexusd}" --port "${port}" --data-dir "${data_dir}" --host 127.0.0.1 \
  --topic "${topic}:1" >"${server_log}" 2>&1 &
server_pid="$!"

# Espera a que el broker acepte conexiones (la E/S de red puede tardar en estar lista).
ready=0
for _ in $(seq 1 100); do
  if (exec 3<>"/dev/tcp/127.0.0.1/${port}") 2>/dev/null; then
    exec 3>&- 3<&- || true
    ready=1
    break
  fi
  sleep 0.1
done
if [[ "${ready}" -ne 1 ]]; then
  echo "error: el broker no quedó listo; log:" >&2
  cat "${server_log}" >&2
  exit 1
fi

# Extrae el valor numérico que sigue a una etiqueta "clave=" en la línea de resumen del generador.
field() { sed -n "s/.*$1=\\([0-9.]*\\).*/\\1/p" <<<"$2"; }

printf '| %-12s | %-10s | %-9s | %8s | %8s | %9s | %9s |\n' \
  'objetivo' 'logrado' 'errores' 'p50 µs' 'p99 µs' 'p999 µs' 'max µs'
printf '|%s|%s|%s|%s|%s|%s|%s|\n' \
  '--------------' '------------' '-----------' '----------' '----------' '-----------' '----------'

for rate in ${RATES}; do
  if [[ "${rate}" -eq 0 ]]; then
    count=200000
    warmup=20000
    label='máx (sin ritmo)'
  else
    count=$(( rate * DURATION ))
    warmup=$(( rate ))
    label="${rate} req/s"
  fi
  line="$(
    "${loadgen}" --host 127.0.0.1 --port "${port}" --topic "${topic}" \
      --payload "${PAYLOAD}" --connections "${CONNECTIONS}" \
      --rate "${rate}" --count "${count}" --warmup "${warmup}" | tail -n1
  )"
  achieved="$(field achieved "${line}")"
  errors="$(field errors "${line}")"
  p50="$(field p50 "${line}")"
  p99="$(field p99 "${line}")"
  p999="$(field p999 "${line}")"
  max="$(field max "${line}")"
  printf '| %-12s | %-10s | %-9s | %8s | %8s | %9s | %9s |\n' \
    "${label}" "${achieved} r/s" "${errors}" "${p50}" "${p99}" "${p999}" "${max}"
done

echo
echo "Notas: latencias end-to-end (envío→ack) corregidas de coordinated omission; medición a tasa"
echo "fija (open-loop). 'máx (sin ritmo)' satura el broker y mide el throughput tope, no la latencia"
echo "de cola bajo SLO. Entorno de un solo nodo sobre loopback (sin RTT de red real)."
