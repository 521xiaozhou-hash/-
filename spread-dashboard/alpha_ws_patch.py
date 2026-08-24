import asyncio
import json
import os
import time
import types
import httpx
import websockets

# Binance Alpha official WebSocket market-data endpoint.
# Use the official ALL book-ticker stream so every Alpha market is covered
# by one real-time feed. REST is used ONLY as an initial snapshot bootstrap;
# it is never used as a periodic/fallback live-price source.
ALPHA_WS = "wss://nbstream.binance.com/w3w/wsa/stream"
ALPHA_DEPTH_URL = "https://www.binance.com/bapi/defi/v1/public/alpha-trade/fullDepth"


def install_alpha_ws(engine):
    async def _bootstrap(self, symbols):
        """Bootstrap the current top-of-book once after a WS connection starts.

        This prevents an illiquid symbol from staying blank merely because its
        bookTicker stream has not emitted a change yet. After bootstrap, only
        the WebSocket stream updates the quote; there is no periodic REST poll.
        """
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
                        r = await client.get(ALPHA_DEPTH_URL, params={"symbol": symbol, "limit": 5})
                        r.raise_for_status()
                        d = r.json()
                        x = d.get("data") if isinstance(d, dict) else None
                        bids = x.get("bids") if isinstance(x, dict) else None
                        asks = x.get("asks") if isinstance(x, dict) else None
                        bid = float(bids[0][0]) if bids else None
                        ask = float(asks[0][0]) if asks else None
                        if bid is None or ask is None or bid <= 0 or ask <= 0 or bid > ask:
                            return None
                        return symbol, {
                            "bid": bid,
                            "ask": ask,
                            "bid_qty": float(bids[0][1]) if bids and len(bids[0]) > 1 else None,
                            "ask_qty": float(asks[0][1]) if asks and len(asks[0]) > 1 else None,
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

                    # One official all-market stream. No per-symbol subscription fan-out.
                    await ws.send(json.dumps({
                        "method": "SUBSCRIBE",
                        "params": ["!bookTicker"],
                        "id": "alpha-all-bookticker",
                    }))

                    # Bootstrap only once. Live quotes thereafter come exclusively
                    # from WebSocket messages.
                    await self._alpha_bootstrap(symbols)

                    last_activity = time.monotonic()
                    while True:
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=30)
                        except asyncio.TimeoutError:
                            # Keep the socket alive and rebuild if Alpha's market list changed.
                            await ws.send(json.dumps({"method": "LIST_SUBSCRIPTION", "id": "alpha-list"}))
                            if tuple(sorted({
                                str(x.get("market_symbol") or "").upper()
                                for x in self.alpha
                                if x.get("market_symbol")
                            })) != signature:
                                return
                            continue

                        try:
                            msg = json.loads(raw)
                        except Exception:
                            continue

                        # Subscription acknowledgement / diagnostics.
                        if msg.get("id") == "alpha-all-bookticker":
                            if msg.get("result") is None:
                                self.alpha_diag["ws_subscribed"] = True
                            else:
                                self.alpha_diag["ws_subscribed"] = False
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
                            self.alpha_quotes[symbol] = q
                            for row in self.alpha:
                                if row.get("market_symbol") == symbol:
                                    row["price"] = q["last"]
                                    row["ts"] = q["ts"]
                                    break
                            self.alpha_diag["book_quotes"] = sum(
                                1 for row in self.alpha if self.alpha_quotes.get(row.get("market_symbol"))
                            )
                        last_activity = time.monotonic()

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
