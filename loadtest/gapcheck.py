#!/usr/bin/env python3
"""Reads normalized events (JSON lines) on stdin and reports, per venue symbol, how many trade
ids are missing from the sequence, how many arrived out of order, and how many duplicate
eventIds made it onto the topic. All three venues number trades sequentially per symbol, so a
missing id is a lost trade.

Unlike verify.py (synthetic load-test traffic), this runs against the LIVE topic:
  docker exec redpanda rpk topic consume normalized-market-data -o :end -f '%v\\n' \\
      | python3 loadtest/gapcheck.py [--since 2026-10-10T10:15:00Z]
Use `-o :end` (read up to the current end, then stop), not `start:end`.
--since judges only trades executed at or after that time, e.g. since the current build
was deployed; without it, every restart and laptop sleep in the retention window shows up.

Exit status is 1 if anything is missing, reordered or duplicated."""
import sys, json, collections
since = sys.argv[sys.argv.index("--since") + 1] if "--since" in sys.argv else ""
ids = collections.defaultdict(list); dup_events = 0; seen = set(); backfilled = 0; total = 0
for line in sys.stdin:
    try: v = json.loads(line)
    except Exception: continue
    if since and v["timestamp"] < since: continue
    total += 1
    if v["eventId"] in seen: dup_events += 1
    seen.add(v["eventId"])
    backfilled += 1 if v.get("backfilled") else 0
    ids[(v["exchange"], v["venueSymbol"])].append(int(v["tradeId"]))
print(f"  events read: {total}   duplicate eventIds on topic: {dup_events}   backfilled: {backfilled}")
bad = dup_events
for (ex, sym), xs in sorted(ids.items()):
    u = sorted(set(xs)); span = u[-1] - u[0] + 1; missing = span - len(u)
    gaps = [(a, b) for a, b in zip(u, u[1:]) if b - a > 1]
    biggest = max((b - a - 1 for a, b in gaps), default=0)
    # xs is in topic order (one partition per canonical symbol), so a smaller id after a larger one is a reorder.
    reorders = sum(1 for a, b in zip(xs, xs[1:]) if b < a)
    bad += missing + reorders
    print(f"  {ex:<9} {sym:<8} trades={len(u):<6} missing={missing:<5} gaps={len(gaps):<3} "
          f"largest-gap={biggest:<5} reorders={reorders}")
sys.exit(1 if bad else 0)
