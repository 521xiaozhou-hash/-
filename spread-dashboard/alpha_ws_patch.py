import asyncio
import json
import time
import types
from typing import Any

import httpx
import websockets

# Binance Alpha WebSocket combined-stream endpoint.
# Request-based subscriptions are documented on /stream/stream.
ALPHA_WS = "wss://nbstream.binance.com/w3w/wsa/stream/stream"
ALPHA_DEPTH_URL = "https://www.binance.com/bapi/defi/v1/public/alpha-trade/fullDepth"


def _norm_symbol(value: Any) -> str:
    return str(value or "").upper().replace("-", "").replace("_", "").replace("/", "")


def _book_message(raw):
    """Parse raw or combined Alpha bookTicker messages."""
    try:
        msg = json.loads(raw) if isinstance(raw, (str, bytes, bytearray)) else raw
    except Exception:
        return None
    if not isinstance(msg, dict):
        return None
    data = msg.get("data")
    if isinstance(data, dict) and data.get("e") == "bookTicker":
        msg = data
    elif msg.get("e") != "bookTicker":
        return None
    symbol = str(msg.get("s") or "").upper()
    if not symbol:
        return None
    try:
        bid = float(msg.get("b")); ask = float(msg.get("a"))
        bid_qty = float(msg.get("B")) if msg.get("B") not in (None, "") else None
        ask_qty = float(msg.get("A")) if msg.get("A") not in (None, "") else None
        update_id = int(msg.get("u") or 0)
        ts = int(msg.get("E") or msg.get("T") or time.time() * 1000)
    except Exception:
        return None
    if bid <= 0 or ask <= 0 or bid > ask:
        return None
    return symbol, {
        "bid": bid, "ask": ask, "bid_qty": bid_qty, "ask_qty": ask_qty,
        "last": (bid + ask) / 2, "ts": ts, "update_id": update_id,
        "source": "websocket",
    }


async def _rest_depth(engine, symbols):
    """Refresh Alpha best bid/ask from Binance REST as a safety net."""
    if not symbols:
        return 0
    headers = {"User-Agent": "Mozilla/5.0", "Accept": "application/json, text/plain, */*", "Referer": "https://www.binance.com/"}
    updated = 0
    try:
        async with httpx.AsyncClient(headers=headers, timeout=8, follow_redirects=True) as client:
            sem = asyncio.Semaphore(16)

            async def one(symbol):
                async with sem:
                    try:
                        r = await client.get(ALPHA_DEPTH_URL, params={"symbol": symbol, "limit": 5})
                        r.raise_for_status()
                        d = r.json(); x = d.get("data") if isinstance(d, dict) else None
                        if not isinstance(x, dict): return None
                        bids = x.get("bids") or []; asks = x.get("asks") or []
                        if not bids or not asks: return None
                        bid = float(bids[0][0]); ask = float(asks[0][0])
                        if bid <= 0 or ask <= 0 or bid > ask: return None
                        return symbol, {"bid": bid, "ask": ask, "last": (bid + ask) / 2, "ts": int(x.get("E") or time.time() * 1000), "source": "rest"}
                    except Exception:
                        return None

            results = await asyncio.gather(*(one(s) for s in symbols))
        async with engine.lock:
            current = {str(x.get("market_symbol") or "").upper(): x for x in engine.alpha}
            for item in results:
                if not item: continue
                symbol, q = item
                actual = current.get(symbol)
                if actual is None:
                    compact = _norm_symbol(symbol)
                    actual = next((x for x in engine.alpha if _norm_symbol(x.get("market_symbol")) == compact), None)
                key = str(actual.get("market_symbol") or symbol).upper() if actual else symbol
                old = engine.alpha_quotes.get(key)
                if old and old.get("source") == "websocket" and int(old.get("ts") or 0) >= int(q.get("ts") or 0):
                    continue
                engine.alpha_quotes[key] = q
                if actual:
                    actual["price"] = q["last"]; actual["ts"] = q["ts"]
                updated += 1
            engine.alpha_diag["rest_quotes"] = sum(1 for q in engine.alpha_quotes.values() if q.get("source") == "rest")
            engine.alpha_diag["book_quotes"] = len(engine.alpha_quotes)
            good = [item[1] for item in results if item and item[1]]
            if good:
                engine.alpha_diag["last_rest_update_ms"] = max(int(q.get("ts") or 0) for q in good)
    except Exception as e:
        async with engine.lock:
            engine.alpha_diag["rest_error"] = f"{type(e).__name__}:{str(e)[:120]}"
    return updated


def install_alpha_ws(engine):
    async def _alpha_market_stream(self):
        while True:
            try:
                async with self.lock:
                    symbols = tuple(sorted({str(x.get("market_symbol") or "").upper() for x in self.alpha if x.get("market_symbol")}))
                    self.alpha_diag.update({
                        "ws_subscribed": False,
                        "ws_updates": int(self.alpha_diag.get("ws_updates") or 0),
                        "ws_symbols": len(symbols),
                        "ws_seen_symbols": 0,
                        "ws_last_event_ms": int(self.alpha_diag.get("ws_last_event_ms") or 0),
                        "quote_source": "websocket+rest-fallback",
                        "rest_error": "",
                    })
                if not symbols:
                    self.connections["Binance Alpha"] = "waiting-alpha"
                    await asyncio.sleep(2)
                    continue

                # REST snapshot makes Alpha visible immediately while WS starts.
                await _rest_depth(self, symbols)

                async with websockets.connect(ALPHA_WS, ping_interval=20, ping_timeout=10, max_size=16 * 1024 * 1024, close_timeout=5) as ws:
                    self.connections["Binance Alpha"] = "connected"
                    request_id = int(time.time() * 1000) % 2147483647
                    params = [f"{s.lower()}@bookTicker" for s in symbols]
                    await ws.send(json.dumps({"method": "SUBSCRIBE", "params": params, "id": request_id}))
                    seen = set(); last_rest = time.monotonic(); started = time.monotonic()
                    while True:
                        if time.monotonic() - started >= 60:
                            async with self.lock:
                                current = tuple(sorted({str(x.get("market_symbol") or "").upper() for x in self.alpha if x.get("market_symbol")}))
                            if current != symbols: break
                            started = time.monotonic()
                        if time.monotonic() - last_rest >= 10:
                            await _rest_depth(self, symbols); last_rest = time.monotonic()
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=5)
                        except asyncio.TimeoutError:
                            continue
                        try: msg = json.loads(raw)
                        except Exception: continue
                        if isinstance(msg, dict) and msg.get("id") == request_id:
                            async with self.lock:
                                if msg.get("result") is None:
                                    self.alpha_diag["ws_subscribed"] = True; self.alpha_diag["error"] = ""
                                else:
                                    self.alpha_diag["error"] = "SUBSCRIBE:" + str(msg.get("result"))[:160]
                            continue
                        parsed = _book_message(raw)
                        if not parsed: continue
                        symbol, q = parsed; compact = _norm_symbol(symbol)
                        async with self.lock:
                            actual = next((x for x in self.alpha if _norm_symbol(x.get("market_symbol")) == compact), None)
                            if actual is None: continue
                            key = str(actual.get("market_symbol") or symbol).upper()
                            old = self.alpha_quotes.get(key)
                            old_id = int(old.get("update_id") or 0) if old else 0
                            old_ts = int(old.get("ts") or 0) if old else 0
                            if old and old.get("source") == "websocket" and ((q["update_id"] and old_id and q["update_id"] < old_id) or q["ts"] < old_ts): continue
                            self.alpha_quotes[key] = q; seen.add(key)
                            actual["price"] = q["last"]; actual["ts"] = q["ts"]
                            self.alpha_diag["ws_updates"] = int(self.alpha_diag.get("ws_updates") or 0) + 1
                            self.alpha_diag["ws_seen_symbols"] = len(seen)
                            self.alpha_diag["ws_last_event_ms"] = q["ts"]
                            self.alpha_diag["book_quotes"] = len(self.alpha_quotes)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.connections["Binance Alpha"] = "reconnecting"
                async with self.lock:
                    self.alpha_diag["ws_subscribed"] = False
                    self.alpha_diag["error"] = f"WS:{type(e).__name__}:{str(e)[:160]}"
                try:
                    async with self.lock:
                        fallback_symbols = [str(x.get("market_symbol") or "").upper() for x in self.alpha if x.get("market_symbol")]
                    await _rest_depth(self, fallback_symbols)
                except Exception:
                    pass
                await asyncio.sleep(2)

    engine.connections["Binance Alpha"] = "disconnected"
    engine._alpha_bootstrap = types.MethodType(lambda self, symbols: asyncio.sleep(0), engine)
    engine.refresh_alpha_depth = types.MethodType(_alpha_market_stream, engine)
    return engine
