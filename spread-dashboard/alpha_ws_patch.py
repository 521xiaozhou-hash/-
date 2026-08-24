import asyncio
import json
import time
import types
import websockets

ALPHA_WS = "wss://nbstream.binance.com/w3w/wsa/stream"


def install_alpha_ws(engine):
    async def alpha_depth_stream(self):
        """Use Binance Alpha's native bookTicker stream for live best bid/ask.
        This replaces the 5s REST fullDepth poll, which can lag the Binance UI.
        """
        while True:
            try:
                async with self.lock:
                    symbols = [str(x.get("market_symbol") or "").upper() for x in self.alpha if x.get("market_symbol")]
                symbols = sorted(set(symbols))
                if not symbols:
                    self.connections["Binance Alpha"] = "waiting-alpha"
                    await asyncio.sleep(1)
                    continue

                async with websockets.connect(
                    ALPHA_WS,
                    ping_interval=20,
                    ping_timeout=10,
                    max_size=16 * 1024 * 1024,
                ) as ws:
                    self.connections["Binance Alpha"] = "connected"
                    # Binance Alpha supports dynamic SUBSCRIBE messages.
                    # Keep each request reasonably sized for large Alpha lists.
                    for i in range(0, len(symbols), 100):
                        chunk = symbols[i:i + 100]
                        await ws.send(json.dumps({
                            "method": "SUBSCRIBE",
                            "params": [f"{s.lower()}@bookTicker" for s in chunk],
                            "id": i // 100 + 1,
                        }))

                    signature = tuple(symbols)
                    last_check = time.monotonic()
                    async for raw in ws:
                        now = time.monotonic()
                        if now - last_check >= 5:
                            async with self.lock:
                                current = tuple(sorted({str(x.get("market_symbol") or "").upper() for x in self.alpha if x.get("market_symbol")}))
                            if current != signature:
                                break
                            last_check = now

                        try:
                            msg = json.loads(raw)
                            d = msg.get("data") or {}
                            if not isinstance(d, dict) or d.get("e") != "bookTicker":
                                continue
                            symbol = str(d.get("s") or "").upper()
                            bid = d.get("b")
                            ask = d.get("a")
                            if not symbol or bid in (None, "") or ask in (None, ""):
                                continue
                            bid_f = float(bid)
                            ask_f = float(ask)
                            if bid_f <= 0 or ask_f <= 0 or bid_f > ask_f:
                                continue
                            q = {
                                "bid": bid_f,
                                "ask": ask_f,
                                "bid_qty": float(d.get("B")) if d.get("B") not in (None, "") else None,
                                "ask_qty": float(d.get("A")) if d.get("A") not in (None, "") else None,
                                "last": (bid_f + ask_f) / 2,
                                "ts": int(d.get("E") or d.get("T") or time.time() * 1000),
                            }
                            async with self.lock:
                                self.alpha_quotes[symbol] = q
                                for row in self.alpha:
                                    if row.get("market_symbol") == symbol:
                                        row["price"] = q["last"]
                                        row["ts"] = q["ts"]
                                        break
                        except Exception:
                            continue
            except Exception:
                self.connections["Binance Alpha"] = "reconnecting"
                await asyncio.sleep(2)

    engine.connections["Binance Alpha"] = "disconnected"
    engine.refresh_alpha_depth = types.MethodType(alpha_depth_stream, engine)
    return engine
