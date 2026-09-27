"""A stand-in for Binance's public trade stream, for load-testing the gateway.

Speaks just enough of the real protocol: accepts SUBSCRIBE frames on /ws, acknowledges them
with {"result":null,"id":N}, then streams raw trade frames for every subscribed symbol with a
strictly consecutive trade id ("t") per symbol -- exactly the property gapcheck.py and the
gateway's own ordering guarantees are checked against.

  RATE=0      send as fast as the gateway will read (saturation: measures max throughput,
              since the socket's send buffer fills whenever the gateway applies backpressure)
  RATE=N      target N trades/second in total, round-robin across symbols

Every 5 s it prints the achieved send rate. GET-free by design: the gateway's own metrics and
the topic are the source of truth, this only reports what it offered.
"""
import asyncio
import json
import os
import time

import websockets

RATE = int(os.environ.get("RATE", "0"))
PORT = int(os.environ.get("PORT", "9443"))
BATCH = 200  # frames written per event-loop turn


def trade(symbol: str, trade_id: int, now_ms: int) -> str:
    # Field order and quoting match a real Binance trade frame.
    return (
        f'{{"e":"trade","E":{now_ms},"s":"{symbol}","t":{trade_id},'
        f'"p":"{100 + trade_id % 1000}.{trade_id % 100:02d}","q":"0.{trade_id % 997 + 1:03d}",'
        f'"T":{now_ms},"m":{"true" if trade_id % 2 else "false"},"M":true}}'
    )


async def stream(ws, symbols, stats):
    next_id = {s: 1 for s in symbols}
    i = 0
    started = time.monotonic()
    sent = 0
    while True:
        now_ms = int(time.time() * 1000)
        for _ in range(BATCH):
            symbol = symbols[i % len(symbols)]
            i += 1
            await ws.send(trade(symbol, next_id[symbol], now_ms))
            next_id[symbol] += 1
        sent += BATCH
        stats["sent"] += BATCH
        if RATE:
            # Sleep until the schedule says the next batch is due.
            due = started + sent / RATE
            delay = due - time.monotonic()
            if delay > 0:
                await asyncio.sleep(delay)
        else:
            await asyncio.sleep(0)


async def handler(ws):
    stats = {"sent": 0}
    symbols = []
    streamer = None
    reporter = asyncio.create_task(report(stats))
    # Older gateway builds encode streams in the path (/ws/btcusdt@trade/ethusdt@trade) and send
    # no SUBSCRIBE; accept that form too so before/after runs see identical traffic.
    path = getattr(ws, "path", None) or ws.request.path
    symbols = [part.split("@")[0].upper() for part in path.split("/") if part.endswith("@trade")]
    if symbols:
        print(f"path-subscribed: {len(symbols)} symbols, rate={'max' if not RATE else RATE}", flush=True)
        streamer = asyncio.create_task(stream(ws, list(symbols), stats))
    try:
        async for message in ws:
            request = json.loads(message)
            if request.get("method") == "SUBSCRIBE":
                symbols += [p.split("@")[0].upper() for p in request["params"]]
                await ws.send(json.dumps({"result": None, "id": request["id"]}))
                if streamer:
                    streamer.cancel()
                print(f"subscribed: {len(symbols)} symbols, rate={'max' if not RATE else RATE}", flush=True)
                streamer = asyncio.create_task(stream(ws, list(symbols), stats))
    except websockets.ConnectionClosed:
        pass
    finally:
        for task in (streamer, reporter):
            if task:
                task.cancel()
        print(f"connection closed after {stats['sent']} trades", flush=True)


async def report(stats):
    last, last_t = 0, time.monotonic()
    while True:
        await asyncio.sleep(5)
        now = time.monotonic()
        print(f"offered {(stats['sent'] - last) / (now - last_t):,.0f} trades/s (total {stats['sent']:,})", flush=True)
        last, last_t = stats["sent"], now


async def main():
    # max_size bounds inbound control frames; write_limit makes the send buffer small so the
    # gateway's backpressure reaches this generator quickly instead of hiding in userspace.
    async with websockets.serve(handler, "0.0.0.0", PORT, max_size=1 << 20, write_limit=64 * 1024,
                                ping_interval=20, compression=None):
        print(f"fake binance listening on :{PORT}/ws", flush=True)
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
