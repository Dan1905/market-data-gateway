#!/usr/bin/env python3
"""Reads normalized events (JSON lines) on stdin and reports, per venue symbol, how many trade
ids are missing from the sequence and how many duplicate eventIds made it onto the topic.
All three venues number trades sequentially per symbol, so a missing id is a lost trade.

Unlike verify.py (synthetic load-test traffic), this runs against the LIVE topic:
  docker exec redpanda rpk topic consume normalized-market-data -o :end -f '%v\n' | python3 loadtest/gapcheck.py
Use `-o :end` (read up to the current end, then stop), not `start:end`."""
import sys, json, collections
ids = collections.defaultdict(list); dup_events = 0; seen = set(); backfilled = 0; total = 0
for line in sys.stdin:
    try: v = json.loads(line)
    except Exception: continue
    total += 1
    if v["eventId"] in seen: dup_events += 1
    seen.add(v["eventId"])
    backfilled += 1 if v.get("backfilled") else 0
    ids[(v["exchange"], v["venueSymbol"])].append(int(v["tradeId"]))
print(f"  events read: {total}   duplicate eventIds on topic: {dup_events}   backfilled: {backfilled}")
for (ex, sym), xs in sorted(ids.items()):
    u = sorted(set(xs)); span = u[-1] - u[0] + 1; missing = span - len(u)
    gaps = [(a, b) for a, b in zip(u, u[1:]) if b - a > 1]
    biggest = max((b - a - 1 for a, b in gaps), default=0)
    print(f"  {ex:<9} {sym:<8} trades={len(u):<6} missing={missing:<5} gaps={len(gaps):<3} largest-gap={biggest}")
