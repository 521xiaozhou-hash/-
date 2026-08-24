import asyncio
import json
import math
import os
import time
import types
import websockets

# Binance Alpha official WebSocket market-data endpoint.
# Live prices use individual <symbol>@bookTicker streams in a parallel pool.
# REST is intentionally NOT used as a live-price fallback.
ALPHA_WS = "wss://nbstream.binance.com/w3w/wsa/stream"
MAX_STREAMS_PER_CONNECTION = max(50, int(os.getenv("ALPHA_STREAMS_PER_CONNECTION", "200")))


def install_alpha_ws(engine):
    async def _alpha_worker(self, symbols, shard_index, signature):
        streams = [f"{s.lower()}@bookTicker" for s in symbols]
        try:
            async with websockets.connect(
                ALPHA_WS,
                ping_interval=20,
                ping_timeout=10,
                max_size=16 * 1024 * 1024,
                close_timeout=5,
            ) as ws:
                self.connections["Binance Alpha"] = "connected"
                await ws.send(json.dumps({"method": "SUBSCRIBE", "params": streams, "id": shard_index + 1}))
                while True:
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=15)
                    except asyncio.TimeoutError:
                        async with self.lock:
                            current = tuple(x.get("market_symbol") for x in self.alpha if x.get("market_symbol"))
                        if current != signature:
                            return
                        continue
                    try:
                        msg = json.loads(raw)
                    except Exception:
                        continue
                    data = msg.get("data") if isinstance(msg, dict) else None
                    if not isinstance(data, dict) or data.get("e") != "bookTicker":
                        continue
                    symbol = str(data.get("s") or "").upper()
                    try:
                        bid = float(data.get("b")) if data.get("b") not in (None, "") else None
                        ask = float(data.get("a")) if data.get("a") not in (None, "") else None
                    except Exception:
                        continue
                    if not symbol or bid is None or ask is None or bid <= 0 or ask <= 0 or bid > ask:
                        continue
                    ts = int(data.get("E") or data.get("T") or time.time() * 1000)
                    q = {
                        "bid": bid,
                        "ask": ask,
                        "bid_qty": float(data.get("B")) if data.get("B") not in (None, "") else None,
                        "ask_qty": float(data.get("A")) if data.get("A") not in (None, "") else None,
                        "last": (bid + ask) / 2,
                        "ts": ts,
                    }
                    async with self.lock:
                        self.alpha_quotes[symbol] = q
                        for row in self.alpha:
                            if row.get("market_symbol") == symbol:
                                row["price"] = q["last"]
                                row["ts"] = q["ts"]
                                break
                        self.alpha_diag["book_quotes"] = sum(
                            1 for row in self.alpha if self.alpha_quotes.get(row.get("market_symbol"))
                        )
        except Exception:
            self.connections["Binance Alpha"] = "reconnecting"
            raise

    async def alpha_realtime_pool(self):
        """Subscribe to every active Alpha market in parallel via realtime bookTicker.

        There is no REST polling for live prices. Symbols are sharded across multiple
        WebSocket connections; when Alpha's symbol list changes, the pool is rebuilt.
        """
        while True:
            try:
                async with self.lock:
                    symbols = sorted({
                        str(x.get("market_symbol") or "").upper()
                        for x in self.alpha
                        if x.get("market_symbol")
                    })
                if not symbols:
                    self.connections["Binance Alpha"] = "waiting-alpha"
                    await asyncio.sleep(1)
                    continue

                signature = tuple(symbols)
                shard_count = max(1, math.ceil(len(symbols) / MAX_STREAMS_PER_CONNECTION))
                shards = [
                    symbols[i * MAX_STREAMS_PER_CONNECTION:(i + 1) * MAX_STREAMS_PER_CONNECTION]
                    for i in range(shard_count)
                ]
                workers = [
                    asyncio.create_task(self._alpha_worker(shard, i, signature))
                    for i, shard in enumerate(shards)
                ]
                try:
                    await asyncio.gather(*workers)
                except Exception:
                    pass
                finally:
                    for task in workers:
                        if not task.done():
                            task.cancel()
                    await asyncio.gather(*workers, return_exceptions=True)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.connections["Binance Alpha"] = "reconnecting"
                await asyncio.sleep(1)

    engine.connections["Binance Alpha"] = "disconnected"
    engine.refresh_alpha_depth = types.MethodType(alpha_realtime_pool, engine)
    return engine
