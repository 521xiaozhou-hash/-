import asyncio
import json
import time
import types
import httpx
import websockets

ALPHA_WS = "wss://nbstream.binance.com/w3w/wsa/stream"
ALPHA_DEPTH_URL = "https://www.binance.com/bapi/defi/v1/public/alpha-trade/fullDepth"


def install_alpha_ws(engine):
    async def alpha_depth_stream(self):
        while True:
            try:
                async with websockets.connect(ALPHA_WS, ping_interval=20, ping_timeout=10, max_size=32 * 1024 * 1024) as ws:
                    self.connections["Binance Alpha"] = "connected"
                    await ws.send(json.dumps({"method": "SUBSCRIBE", "params": ["!bookTicker"], "id": 1}))
                    async for raw in ws:
                        try:
                            msg = json.loads(raw)
                            d = msg.get("data") or {}
                            if not isinstance(d, dict) or d.get("e") != "bookTicker":
                                continue
                            symbol = str(d.get("s") or "").upper()
                            bid = float(d.get("b")) if d.get("b") not in (None, "") else None
                            ask = float(d.get("a")) if d.get("a") not in (None, "") else None
                            if not symbol or bid is None or ask is None or bid <= 0 or ask <= 0 or bid > ask:
                                continue
                            q = {"bid": bid, "ask": ask, "bid_qty": float(d.get("B")) if d.get("B") not in (None, "") else None, "ask_qty": float(d.get("A")) if d.get("A") not in (None, "") else None, "last": (bid + ask) / 2, "ts": int(d.get("E") or d.get("T") or time.time() * 1000)}
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

    async def seed_missing_alpha(self):
        """Seed symbols that have not yet emitted an all-bookTicker update.
        The websocket remains the realtime source; REST is only a bounded fallback
        for missing/stale symbols, so inactive Alpha markets do not stay blank.
        """
        while True:
            try:
                now = int(time.time() * 1000)
                async with self.lock:
                    wanted = [r.get("market_symbol") for r in self.alpha if r.get("market_symbol")]
                    missing = [s for s in wanted if not self.alpha_quotes.get(s) or now - int(self.alpha_quotes[s].get("ts", 0)) > 15000]
                # Limit REST recovery to 80 symbols per cycle to protect the API.
                batch = missing[:80]
                if batch:
                    async with httpx.AsyncClient(timeout=6, headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json", "Referer": "https://www.binance.com/"}) as c:
                        sem = asyncio.Semaphore(16)
                        async def one(symbol):
                            async with sem:
                                try:
                                    r = await c.get(ALPHA_DEPTH_URL, params={"symbol": symbol, "limit": 5})
                                    r.raise_for_status()
                                    d = r.json(); x = d.get("data") or {}
                                    bids = x.get("bids") or []; asks = x.get("asks") or []
                                    bid = float(bids[0][0]) if bids else None
                                    ask = float(asks[0][0]) if asks else None
                                    if bid is None or ask is None or bid <= 0 or ask <= 0 or bid > ask:
                                        return symbol, None
                                    return symbol, {"bid": bid, "ask": ask, "last": (bid + ask) / 2, "ts": int(x.get("E") or now)}
                                except Exception:
                                    return symbol, None
                        results = await asyncio.gather(*(one(s) for s in batch))
                    async with self.lock:
                        for s, q in results:
                            if q:
                                self.alpha_quotes[s] = q
                                for row in self.alpha:
                                    if row.get("market_symbol") == s:
                                        row["price"] = q["last"]
                                        row["ts"] = q["ts"]
                                        break
                        self.alpha_diag["book_quotes"] = sum(1 for r in self.alpha if self.alpha_quotes.get(r.get("market_symbol")))
            except Exception:
                pass
            await asyncio.sleep(5)

    engine.connections["Binance Alpha"] = "disconnected"
    engine.refresh_alpha_depth = types.MethodType(alpha_depth_stream, engine)
    engine.seed_missing_alpha = types.MethodType(seed_missing_alpha, engine)
    return engine
