"""Checks what reached the topic against what fake_binance sent.

fake_binance numbers each symbol's trades 1, 2, 3, ... so for every venue symbol the topic
must contain exactly that run: no gap (loss), no repeat (duplicate), no inversion (reorder).
Reads one JSON event per line on stdin, e.g.

  docker exec redpanda rpk topic consume loadtest-trades -o :end -f '%v\\n' | python3 verify.py
"""
import json
import sys
from collections import defaultdict

last = {}
count = defaultdict(int)
problems = defaultdict(int)
examples = []

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    event = json.loads(line)
    symbol, tid = event["venueSymbol"], int(event["tradeId"])
    count[symbol] += 1
    prev = last.get(symbol, 0)
    if tid != prev + 1:
        kind = "gap" if tid > prev + 1 else ("duplicate" if tid == prev else "reorder")
        problems[kind] += 1
        if len(examples) < 5:
            examples.append(f"{symbol}: {prev} -> {tid} ({kind})")
    last[symbol] = max(prev, tid)

total = sum(count.values())
print(f"events={total:,} symbols={len(count)} "
      f"gaps={problems['gap']} duplicates={problems['duplicate']} reorders={problems['reorder']}")
for e in examples:
    print("  e.g.", e)
sys.exit(1 if any(problems.values()) else 0)
