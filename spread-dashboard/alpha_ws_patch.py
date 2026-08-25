import asyncio
import json
import time
import types
import websockets

# Binance Alpha official WebSocket market-data endpoint.
# !bookTicker is the authoritative live source for best bid/ask.
# We deliberately do NOT keep periodically replacing live quotes with REST
# snapshots: a delayed snapshot can make the dashboard show a stale executable
# price. On reconnect we clear the old quotes so stale prices can never survive.
ALPHA_WS = "wss://nbstream.binance.com/w3w/wsa/stream"


def install_alpha_ws(engine):
    async def _alpha_all_bookticker(self):
        while True:
            try:
                async with self.lock:
                    symbols = tuple(sorted({
                        str(x.get("market_symbol") or "").upper()
                        for x in self.alpha
                        if x.get("market_symbol")
                    }))
                    # Do not expose an old quote while a new stream is being
                    # established. Accuracy is more important than briefly
                    # showing a stale number.
                    self.alpha_quotes = {}
                    self.alpha_diag.update({
                        "ws_subscribed": False,
                        "ws_updates": 0,
                        "ws_symbols": len(symbols),
                        "ws_seen_symbols": 0,
                        "ws_last_event_ms": 0,
                        "quote_source": "websocket_bookTicker",
                    })

                if not symbols:
                    self.connections["Binance Alpha"] = "waiting-alpha"
                    await asyncio.sleep(1)
                    continue

                async with websockets.connect(
                    ALPHA_WS,
                    ping_interval=20,
                    ping_timeout=10,
                    max_size=16 * 1024 * 1024,
                    close_timeout=5,
                ) as ws:
                    self.connections["Binance Alpha"] = "connected"
                    await ws.send(json.dumps({
                        "method": "SUBSCRIBE",
                        "params": ["!bookTicker"],
                        "id": "alpha-all-bookticker",
                    }))

                    subscribed = False
                    seen = set()
                    started = time.monotonic()
                    last_event = time.monotonic()

                    while True:
                        # Refresh the symbol set periodically. If Alpha listing
                        # changes, reconnect and start a clean stream state.
                        if time.monotonic() - started >= 60:
                            async with self.lock:
                                current = tuple(sorted({
                                    str(x.get("market_symbol") or "").upper()
                                    for x in self.alpha
                                    if x.get("market_symbol")
                                }))
                            if current != symbols:
                                break
                            started = time.monotonic()

                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=30)
                        except asyncio.TimeoutError:
                            # A global bookTicker stream should continuously
                            # deliver changes. If it is completely silent for a
                            # long period, reconnect instead of serving stale data.
                            if subscribed and time.monotonic() - last_event > 30:
                                raise RuntimeError("Alpha bookTicker silent for 30s")
                            continue

                        try:
                            msg = json.loads(raw)
                        except Exception:
                            continue

                        if msg.get("id") == "alpha-all-bookticker":
                            if msg.get("result") is None:
                                subscribed = True
                                async with self.lock:
                                    self.alpha_diag["ws_subscribed"] = True
                            else:
                                async with self.lock:
                                    self.alpha_diag["error"] = "SUBSCRIBE:" + str(msg.get("result"))[:160]
                            continue

                        data = msg.get("data") if isinstance(msg, dict) else None
                        if not isinstance(data, dict) or data.get("e") != "bookTicker":
                            continue

                        symbol = str(data.get("s") or "").upper()
                        if not symbol:
                            continue
                        try:
                            bid = float(data.get("b"))
                            ask = float(data.get("a"))
                            bid_qty = float(data.get("B"))
                            ask_qty = float(data.get("A"))
                            update_id = int(data.get("u") or 0)
                            ts = int(data.get("E") or data.get("T") or time.time() * 1000)
                        except Exception:
                            continue

                        if bid <= 0 or ask <= 0 or bid > ask:
                            continue

                        q = {
                            "bid": bid,
                            "ask": ask,
                            "bid_qty": bid_qty,
                            "ask_qty": ask_qty,
                            "last": (bid + ask) / 2,
                            "ts": ts,
                            "update_id": update_id,
                            "source": "websocket",
                        }

                        async with self.lock:
                            old = self.alpha_quotes.get(symbol)
                            old_id = int(old.get("update_id") or 0) if old else 0
                            old_ts = int(old.get("ts") or 0) if old else 0
                            if old and ((update_id and old_id and update_id < old_id) or (ts < old_ts)):
                                continue
                            self.alpha_quotes[symbol] = q
                            seen.add(symbol)
                            last_event = time.monotonic()
                            self.alpha_diag["ws_updates"] = int(self.alpha_diag.get("ws_updates") or 0) + 1
                            self.alpha_diag["ws_seen_symbols"] = len(seen)
                            self.alpha_diag["ws_last_event_ms"] = ts
                            self.alpha_diag["book_quotes"] = len(self.alpha_quotes)
                            for row in self.alpha:
                                if str(row.get("market_symbol") or "").upper() == symbol:
                                    row["price"] = q["last"]
                                    row["ts"] = ts
                                    break

            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.connections["Binance Alpha"] = "reconnecting"
                async with self.lock:
                    self.alpha_quotes = {}
                    self.alpha_diag["book_quotes"] = 0
                    self.alpha_diag["ws_subscribed"] = False
                    self.alpha_diag["error"] = f"WS:{type(e).__name__}:{str(e)[:120]}"
                await asyncio.sleep(1)

    engine.connections["Binance Alpha"] = "disconnected"
    engine._alpha_bootstrap = types.MethodType(lambda self, symbols: asyncio.sleep(0), engine)
    engine.refresh_alpha_depth = types.MethodType(_alpha_all_bookticker, engine)
    return engine
