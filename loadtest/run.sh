#!/usr/bin/env bash
# Usage: loadtest/run.sh [symbols=50] [seconds=120] [warmup=90]
#   env: IMAGE (default market-data-gateway:latest)  RATE (0 = saturate)
#        MODE=subscribe|path  (path = pre-lanes builds, which encode streams in the URL)
#
# Replaces the local market-data-gateway container with a load-test one, measures a steady
# window after JIT warm-up, then prints throughput, CPU and memory. Writes to its own topic
# so the normal local topic is untouched. Restore the normal stack afterwards with:
#   docker compose up -d
set -euo pipefail
cd "$(dirname "$0")/.."

SYMBOLS=${1:-50}; SECONDS_=${2:-120}; WARMUP=${3:-90}
IMAGE=${IMAGE:-market-data-gateway:latest}; MODE=${MODE:-subscribe}; RATE=${RATE:-0}
TOPIC=loadtest-trades
PORT=${GATEWAY_PORT:-8080}

syms=(); for ((i = 0; i < SYMBOLS; i++)); do syms+=("$(printf 'L%03dUSDT' "$i")"); done
LOADTEST_SYMBOLS=$(IFS=,; echo "${syms[*]}")
if [[ $MODE == path ]]; then
  LOADTEST_URI="ws://fake-binance:9443/ws/$(printf '%s@trade/' "${syms[@]}" | tr 'A-Z' 'a-z')"
else
  LOADTEST_URI=ws://fake-binance:9443/ws
fi
export IMAGE RATE LOADTEST_SYMBOLS LOADTEST_URI

docker exec redpanda rpk topic delete "$TOPIC" >/dev/null 2>&1 || true
compose=(docker compose -f docker-compose.yml -f loadtest/docker-compose.loadtest.yml)
# Build only the generator. `up --build` would also rebuild the gateway from the working tree
# and tag it $IMAGE, silently turning a "before" run into an "after" run.
"${compose[@]}" build -q fake-binance
"${compose[@]}" up -d --no-build --no-deps --force-recreate fake-binance market-data-gateway 2>&1 | grep -v "^ *Container" || true

echo "image=$IMAGE mode=$MODE symbols=$SYMBOLS rate=${RATE:-max} window=${SECONDS_}s warmup=${WARMUP}s"
until curl -sf "localhost:$PORT/actuator/health/liveness" >/dev/null; do sleep 2; done

metric() { curl -s "localhost:$PORT/actuator/metrics/$1" \
  | python3 -c "import sys,json;print(int(json.load(sys.stdin)['measurements'][0]['value']))" 2>/dev/null || echo 0; }
cpu() { docker exec "$1" cat /sys/fs/cgroup/cpu.stat | awk '/^usage_usec/{print $2}'; }
snap() { echo "$(metric mdg.events.published) $(metric mdg.frames.throttled) $(metric mdg.lanes.backpressure.waits) $(cpu market-data-gateway) $(cpu redpanda) $(date +%s)"; }

sleep "$WARMUP"
read -r P0 T0 W0 CG0 CR0 S0 <<<"$(snap)"
peak=0
for ((t = 0; t < SECONDS_; t += 5)); do
  sleep 5
  m=$(docker stats --no-stream --format '{{.MemUsage}}' market-data-gateway | awk '{print $1}')
  mib=$(python3 -c "s='$m';u={'KiB':1/1024,'MiB':1,'GiB':1024};print(int(next(float(s[:-3])*v for k,v in u.items() if s.endswith(k))))")
  (( mib > peak )) && peak=$mib
done
read -r P1 T1 W1 CG1 CR1 S1 <<<"$(snap)"

python3 - "$P0" "$P1" "$T0" "$T1" "$W0" "$W1" "$CG0" "$CG1" "$CR0" "$CR1" "$S0" "$S1" "$peak" <<'PY'
import sys
p0,p1,t0,t1,w0,w1,cg0,cg1,cr0,cr1,s0,s1,peak=map(int,sys.argv[1:])
dt=s1-s0; ev=max(p1-p0,1)
print(f"  published          : {(p1-p0)/dt:,.0f} events/s  ({p1-p0:,} in {dt}s)")
print(f"  throttled (dropped): {t1-t0}")
print(f"  backpressure waits : {w1-w0:,}")
print(f"  CPU gateway        : {(cg1-cg0)/dt/1e4:.0f}% of one core, {(cg1-cg0)/ev:.0f} us/event")
print(f"  CPU redpanda       : {(cr1-cr0)/dt/1e4:.0f}% of one core, {(cr1-cr0)/ev:.0f} us/event")
print(f"  gateway memory peak: {peak} MiB (limit 400)")
PY
