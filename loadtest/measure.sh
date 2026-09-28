#!/bin/bash
# Usage: loadtest/measure.sh <seconds> [gateway_port]   (run after ~10 min of gateway uptime)
# Measures the running compose stack over a window: throughput, CPU per event (from the
# container cgroup counters), disk bytes per event, memory. Numbers feed the free-tier budget.
DUR=${1:-300}; PORT=${2:-8080}
metric(){ curl -s "localhost:$PORT/actuator/metrics/$1${2:+?tag=$2}" | python3 -c "import sys,json;print(int(json.load(sys.stdin)['measurements'][0]['value']))" 2>/dev/null || echo 0; }
cpu(){ docker exec "$1" cat /sys/fs/cgroup/cpu.stat 2>/dev/null | awk '/^usage_usec/{print $2}'; }
# Broker-reported partition bytes, not du: a fresh Redpanda pre-allocates and trims segment
# files, which makes du swing wildly (it read -4.7 KB/event on a fresh start).
disk(){ docker exec redpanda rpk cluster logdirs describe --topics normalized-market-data 2>/dev/null | awk 'NR>1{s+=$5} END{print s+0}'; }
uptime_s(){ docker inspect -f '{{.State.StartedAt}}' market-data-gateway | python3 -c "import sys,datetime;t=datetime.datetime.fromisoformat(sys.stdin.read().strip()[:26]+'+00:00');print(int((datetime.datetime.now(datetime.timezone.utc)-t).total_seconds()))"; }
echo "  gateway uptime at start: $(uptime_s)s (JIT warm-up inflates CPU in the first minutes)"
snap(){ echo "$(metric mdg.events.published) $(metric mdg.frames.received) $(metric mdg.events.deduplicated) $(cpu market-data-gateway) $(cpu redpanda) $(disk) $(date +%s)"; }
read P0 F0 D0 CG0 CR0 B0 T0 <<< "$(snap)"
sleep "$DUR"
read P1 F1 D1 CG1 CR1 B1 T1 <<< "$(snap)"
python3 - "$P0" "$P1" "$F0" "$F1" "$D0" "$D1" "$CG0" "$CG1" "$CR0" "$CR1" "$B0" "$B1" "$T0" "$T1" <<'PY'
import sys
p0,p1,f0,f1,d0,d1,cg0,cg1,cr0,cr1,b0,b1,t0,t1=map(int,sys.argv[1:])
dt=t1-t0; ev=max(p1-p0,1)
print(f"  window              : {dt}s")
print(f"  events published    : {p1-p0}  ({(p1-p0)/dt:.1f}/s)   frames: {(f1-f0)/dt:.1f}/s   deduplicated: {d1-d0}")
for name,a,b in [("gateway",cg0,cg1),("redpanda",cr0,cr1)]:
    us=b-a
    print(f"  CPU {name:<9}       : {us/dt/1e4:5.1f}% of one core   {us/ev:7.0f} us/event")
tot=(cg1-cg0)+(cr1-cr0)
print(f"  CPU stack total     : {tot/dt/1e4:5.1f}% of one core")
print(f"  topic growth        : {(b1-b0)/ev:.0f} bytes/event  -> {(b1-b0)/dt*86400/1e9:.2f} GB/day (before retention)")
PY
echo "  memory:"; docker stats --no-stream --format '    {{.Name}}\t{{.MemUsage}}\t{{.MemPerc}}' market-data-gateway redpanda redpanda-console 2>/dev/null
