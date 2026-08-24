import asyncio
import json
import time
import types
import httpx
import websockets

# Binance Alpha official WebSocket market-data endpoint.
# !bookTicker is the live source of best bid/ask. REST fullDepth is used only
# once after a connection is established so symbols that have not emitted a
# bookTicker update yet still have an initial quote. It is never polled later.
ALPHA_WS = "wss://nbstream.binance.com/w3w/wsa/stream"
ALPHA_DEPTH_URL = "https://www.binance.com/bapi/defi/v1/public/alpha-trade/fullDepth"


def _best_bid_ask(data):
    """Return the actual best prices, independent of array ordering."""
    bids = data.get("bids") if isinstance(data, dict) else None
    asks = data.get("asks") if isinstance(data, dict) else None
    bid_levels = []
    ask_levels = []
    for level in bids or []:
        try:
            p = float(level[0])
            q = float(level[1]) if len(level) > 1 else None
            if p > 0:
                bid_levels.append((p, q))
        except Exception:
            continue
    for level in asks or []:
        try:
            p = float(level[0])
            q = float(level[1]) if len(level) > 1 else None
            if p > 0:
                ask_levels.append((p, q))
        except Exception:
            continue
    # Best bid is the highest bid; best ask is the lowest ask.
    bid = max(bid_levels, key=lambda x: x[0]) if bid_levels else None
    ask = min(ask_levels, key=lambda x: x[0]) if ask_levels else None
    if not bid or not ask or bid[0] > ask[0]:
        return None
    return bid[0], ask[0], bid[1], ask[1]


def install_alpha_ws(engine):
    async def _bootstrap(self, symbols):
        """Take one initial full-depth snapshot; live quotes come from WS only."""
        headers = {
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json, text/plain, */*",
            "Referer": "https://www.binance.com/",
        }
        async with httpx.AsyncClient(headers=headers, timeout=8, follow_redirects=True) as client:
            sem = asyncio.Semaphore(32)

            async def one(symbol):
                async with sem:
                    try:
                        r = await client.get(ALPHA_DEPTH_URL, params={"symbol": symbol, "limit": 20})
                        r.raise_for_status()
                        d = r.json()
                        x = d.get("data") if isinstance(d, dict) else None
                        best = _best_bid_ask(x)
                        if not best:
                            return None
                        bid, ask, bid_qty, ask_qty = best
                        return symbol, {
                            "bid": bid,
                            "ask": ask,
                            "bid_qty": bid_qty,
                            "ask_qty": ask_qty,
                            "last": (bid + ask) / 2,
                            "ts": int((x or {}).get("E") or time.time() * 1000),
                            "source": "bootstrap",
                        }
                    except Exception:
                        return None

            results = await asyncio.gather(*(one(s) for s in symbols))

        async with self.lock:
            for item in results:
                if not item:
                    continue
                symbol, q = item
                self.alpha_quotes[symbol] = q
                for row in self.alpha:
                    if row.get("market_symbol") == symbol:
                        row["price"] = q["last"]
                        row["ts"] = q["ts"]
                        break
            self.alpha_diag["book_quotes"] = sum(
                1 for row in self.alpha if self.alpha_quotes.get(row.get("market_symbol"))
            )
            self.alpha_diag["bootstrap_quotes"] = self.alpha_diag["book_quotes"]

    async def _alpha_all_bookticker(self):
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
                async with websockets.connect(
                    ALPHA_WS,
                    ping_interval=20,
                    ping_timeout=10,
                    max_size=16 * 1024 * 1024,
                    close_timeout=5,
                ) as ws:
                    self.connections["Binance Alpha"] = "connected"
                    self.alpha_diag["ws_subscribed"] = False
                    self.alpha_diag["ws_updates"] = 0

                    await ws.send(json.dumps({
                        "method": "SUBSCRIBE",
                        "params": ["!bookTicker"],
                        "id": "alpha-all-bookticker",
                    }))

                    # Initial snapshot only. After this point, do not replace live
                    # quotes with periodic REST responses.
                    await self._alpha_bootstrap(symbols)

                    while True:
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=30)
                        except asyncio.TimeoutError:
                            async with self.lock:
                                current = tuple(sorted({
                                    str(x.get("market_symbol") or "").upper()
                                    for x in self.alpha
                                    if x.get("market_symbol")
                                }))
                            if current != signature:
                                return
                            continue

                        try:
                            msg = json.loads(raw)
                        except Exception:
                            continue

                        if msg.get("id") == "alpha-all-bookticker":
                            ok = msg.get("result") is None
                            self.alpha_diag["ws_subscribed"] = ok
                            if not ok:
                                self.alpha_diag["error"] = str(msg.get("result"))
                            continue

                        data = msg.get("data") if isinstance(msg, dict) else None
                        if not isinstance(data, dict) or data.get("e") != "bookTicker":
                            continue

                        symbol = str(data.get("s") or "").upper()
                        try:
                            bid = float(data.get("b")) if data.get("b") not in (None, "") else None
                            ask = float(data.get("a")) if data.get("a") not in (None, "") else None
                            bid_qty = float(data.get("B")) if data.get("B") not in (None, "") else None
                            ask_qty = float(data.get("A")) if data.get("A") not in (None, "") else None
                        except Exception:
                            continue
                        if not symbol or bid is None or ask is None or bid <= 0 or ask <= 0 or bid > ask:
                            continue

                        ts = int(data.get("E") or data.get("T") or time.time() * 1000)
                        q = {
                            "bid": bid,
                            "ask": ask,
                            "bid_qty": bid_qty,
                            "ask_qty": ask_qty,
                            "last": (bid + ask) / 2,
                            "ts": ts,
                            "source": "websocket",
                        }
                        async with self.lock:
                            old = self.alpha_quotes.get(symbol)
                            # Never allow an older WS event to overwrite a newer quote.
                            if old and ts < int(old.get("ts") or 0):
                                continue
                            self.alpha_quotes[symbol] = q
                            for row in self.alpha:
                                if row.get("market_symbol") == symbol:
                                    row["price"] = q["last"]
                                    row["ts"] = q["ts"]
                                    break
                            self.alpha_diag["book_quotes"] = sum(
                                1 for row in self.alpha if self.alpha_quotes.get(row.get("market_symbol"))
                            )
                            self.alpha_diag["ws_updates"] = int(self.alpha_diag.get("ws_updates") or 0) + 1

            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.connections["Binance Alpha"] = "reconnecting"
                self.alpha_diag["error"] = f"WS:{type(e).__name__}"
                await asyncio.sleep(1)

    engine.connections["Binance Alpha"] = "disconnected"
    engine._alpha_bootstrap = types.MethodType(_bootstrap, engine)
    engine.refresh_alpha_depth = types.MethodType(_alpha_all_bookticker, engine)
    return engine
