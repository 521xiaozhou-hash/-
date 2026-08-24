import asyncio
import json
import os
import time
from typing import Any

import httpx
import websockets

ALPHA_LIST_URL = "https://www.binance.com/bapi/defi/v1/public/wallet-direct/buw/wallet/cex/alpha/all/token/list"
BSTOCK_LIST_URL = "https://www.binance.com/bapi/defi/v1/public/wallet-direct/buw/wallet/market/token/rwa/stock/detail/list/ai"
BSTOCK_PRICE_URL = "https://www.binance.com/bapi/defi/v2/public/wallet-direct/buw/wallet/market/token/rwa/dynamic/ai"
BYBIT_WS = "wss://stream.bybit.com/v5/public/spot"
GATE_WS = "wss://api.gateio.ws/ws/v4/"
OKX_WS = "wss://ws.okx.com:8443/ws/v5/public"
COINBASE_WS = "wss://ws-feed.exchange.coinbase.com"
ALPHA_REFRESH = int(os.getenv("ALPHA_REFRESH_SECONDS", "15"))
BSTOCK_REFRESH = int(os.getenv("BSTOCK_REFRESH_SECONDS", "5"))
HTTP_TIMEOUT = float(os.getenv("REQUEST_TIMEOUT", "6"))
ALPHA_SYMBOLS = {x.strip().upper() for x in os.getenv("ALPHA_SYMBOLS", "").split(",") if x.strip()}


def norm(s: str) -> str:
    return str(s).upper().replace("-", "").replace("_", "")


def spread_pct(a: Any, b: Any):
    try:
        a, b = float(a), float(b)
        return (a / b - 1.0) * 100 if b else None
    except Exception:
        return None


class MarketEngine:
    def __init__(self):
        self.lock = asyncio.Lock()
        self.alpha = []
        self.external = {x: {} for x in ("Bybit", "Gate", "OKX", "Coinbase")}
        self.bstocks = []
        self.connections = {x: "disconnected" for x in self.external}
        self.tasks = []

    async def http_json(self, client, url, params=None):
        try:
            r = await client.get(url, params=params, timeout=HTTP_TIMEOUT)
            r.raise_for_status()
            return r.json()
        except Exception:
            return None

    async def refresh_alpha(self):
        """Load the complete Binance Alpha token list.

        The official token-list endpoint contains both CEX-listed and Alpha-only
        tokens. We keep ALL Alpha tokens in the dashboard. CEX comparisons are
        populated only when Binance supplies cexCoinName for that Alpha token.
        """
        headers = {"User-Agent": "spread-dashboard/4.0", "Accept": "application/json"}
        async with httpx.AsyncClient(headers=headers) as c:
            while True:
                d = await self.http_json(c, ALPHA_LIST_URL)
                rows = []
                raw = d.get("data") if isinstance(d, dict) else None
                if isinstance(raw, dict):
                    raw = raw.get("data") or raw.get("tokens") or raw.get("list") or []
                if not isinstance(raw, list):
                    raw = []

                for x in raw:
                    if not isinstance(x, dict):
                        continue
                    aid = str(x.get("alphaId") or "").upper()
                    coin = str(x.get("cexCoinName") or "").upper().strip()
                    symbol = str(x.get("symbol") or x.get("name") or "").strip()
                    p = x.get("price")
                    if not aid or p in (None, ""):
                        continue
                    if ALPHA_SYMBOLS and aid not in ALPHA_SYMBOLS and coin not in ALPHA_SYMBOLS and norm(coin + "USDT") not in ALPHA_SYMBOLS:
                        continue
                    try:
                        price = float(p)
                    except Exception:
                        continue
                    rows.append({
                        "alpha_id": aid,
                        "coin": coin,
                        "symbol": symbol,
                        "price": price,
                        "ts": int(time.time() * 1000),
                    })

                # Preserve the previous good snapshot if Binance briefly returns
                # an error/empty response, rather than making the UI disappear.
                if rows:
                    async with self.lock:
                        self.alpha = rows
                await asyncio.sleep(ALPHA_REFRESH)

    async def refresh_bstocks(self):
        """Load ALL Binance tokenized-stock entries instead of a hard-coded 4-stock list."""
        headers = {"User-Agent": "spread-dashboard/4.0", "Accept": "application/json"}
        async with httpx.AsyncClient(headers=headers) as c:
            while True:
                listing = await self.http_json(c, BSTOCK_LIST_URL, {"type": 3})
                raw = listing.get("data") if isinstance(listing, dict) else None
                if isinstance(raw, dict):
                    raw = raw.get("data") or raw.get("list") or raw.get("tokens") or []
                if not isinstance(raw, list):
                    raw = []

                async def one_stock(x):
                    if not isinstance(x, dict):
                        return None
                    ticker = str(x.get("ticker") or x.get("baseAsset") or "").upper().strip()
                    addr = x.get("contractAddress")
                    chain = str(x.get("chainId") or "")
                    if not ticker or not addr:
                        return None
                    d = await self.http_json(c, BSTOCK_PRICE_URL, {"chainId": chain, "contractAddress": addr})
                    price = None
                    try:
                        token_info = (d or {}).get("data", {}).get("tokenInfo", {})
                        price = float(token_info.get("price"))
                    except Exception:
                        pass
                    return {
                        "ticker": ticker,
                        "symbol": x.get("symbol") or x.get("name") or ticker,
                        "price": price,
                        "chainId": chain,
                        "contractAddress": addr,
                        "ts": int(time.time() * 1000),
                    }

                # Fetch the whole list concurrently so one slow stock does not
                # delay every other stock.
                results = await asyncio.gather(*(one_stock(x) for x in raw), return_exceptions=True)
                rows = [x for x in results if isinstance(x, dict)]
                if rows:
                    rows.sort(key=lambda x: x.get("ticker", ""))
                    async with self.lock:
                        self.bstocks = rows
                await asyncio.sleep(BSTOCK_REFRESH)

    async def bybit(self):
        while True:
            try:
                async with websockets.connect(BYBIT_WS, ping_interval=20, ping_timeout=10, max_size=8 * 1024 * 1024) as ws:
                    self.connections["Bybit"] = "connected"
                    async with self.lock:
                        coins = sorted({x["coin"] for x in self.alpha if x.get("coin")})
                    args = [f"tickers.{c}USDT" for c in coins]
                    for i in range(0, len(args), 10):
                        if args[i:i + 10]:
                            await ws.send(json.dumps({"op": "subscribe", "args": args[i:i + 10]}))
                    async for raw in ws:
                        m = json.loads(raw)
                        if not str(m.get("topic", "")).startswith("tickers."):
                            continue
                        d = m.get("data") or {}
                        if isinstance(d, list):
                            d = d[0] if d else {}
                        sym = norm(d.get("symbol"))
                        if not sym:
                            continue
                        q = {"bid": float(d.get("bid1Price") or 0), "ask": float(d.get("ask1Price") or 0), "last": float(d.get("lastPrice") or 0), "ts": int(m.get("ts") or time.time() * 1000)}
                        async with self.lock:
                            self.external["Bybit"][sym] = q
            except Exception:
                self.connections["Bybit"] = "reconnecting"
                await asyncio.sleep(2)

    async def gate(self):
        while True:
            try:
                async with websockets.connect(GATE_WS, ping_interval=20, ping_timeout=10, max_size=8 * 1024 * 1024) as ws:
                    self.connections["Gate"] = "connected"
                    async with self.lock:
                        coins = sorted({x["coin"] for x in self.alpha if x.get("coin")})
                    for i in range(0, len(coins), 100):
                        pairs = [f"{c}_USDT" for c in coins[i:i + 100]]
                        if pairs:
                            await ws.send(json.dumps({"time": int(time.time()), "channel": "spot.book_ticker", "event": "subscribe", "payload": pairs}))
                    async for raw in ws:
                        m = json.loads(raw)
                        if m.get("channel") != "spot.book_ticker" or m.get("event") != "update":
                            continue
                        d = m.get("result") or {}
                        sym = norm(d.get("s") or d.get("currency_pair"))
                        if not sym:
                            continue
                        q = {"bid": float(d.get("b") or d.get("highest_bid") or 0), "ask": float(d.get("a") or d.get("lowest_ask") or 0), "last": float(d.get("b") or 0), "ts": int(d.get("t") or time.time() * 1000)}
                        async with self.lock:
                            self.external["Gate"][sym] = q
            except Exception:
                self.connections["Gate"] = "reconnecting"
                await asyncio.sleep(2)

    async def okx(self):
        while True:
            try:
                async with websockets.connect(OKX_WS, ping_interval=20, ping_timeout=10, max_size=8 * 1024 * 1024) as ws:
                    self.connections["OKX"] = "connected"
                    async with self.lock:
                        coins = sorted({x["coin"] for x in self.alpha if x.get("coin")})
                    args = [{"channel": "tickers", "instId": f"{c}-USDT"} for c in coins]
                    for i in range(0, len(args), 100):
                        if args[i:i + 100]:
                            await ws.send(json.dumps({"op": "subscribe", "args": args[i:i + 100]}))
                    async for raw in ws:
                        m = json.loads(raw)
                        for d in m.get("data") or []:
                            sym = norm(d.get("instId"))
                            if not sym:
                                continue
                            q = {"bid": float(d.get("bidPx") or 0), "ask": float(d.get("askPx") or 0), "last": float(d.get("last") or 0), "ts": int(d.get("ts") or time.time() * 1000)}
                            async with self.lock:
                                self.external["OKX"][sym] = q
            except Exception:
                self.connections["OKX"] = "reconnecting"
                await asyncio.sleep(2)

    async def coinbase(self):
        while True:
            try:
                async with websockets.connect(COINBASE_WS, ping_interval=20, ping_timeout=10, max_size=8 * 1024 * 1024) as ws:
                    self.connections["Coinbase"] = "connected"
                    async with self.lock:
                        coins = sorted({x["coin"] for x in self.alpha if x.get("coin")})
                    products = [f"{c}-USDT" for c in coins] + [f"{c}-USD" for c in coins]
                    if products:
                        await ws.send(json.dumps({"type": "subscribe", "product_ids": products, "channels": ["ticker", "heartbeat"]}))
                    async for raw in ws:
                        m = json.loads(raw)
                        if m.get("type") != "ticker":
                            continue
                        sym = norm(m.get("product_id"))
                        if not sym:
                            continue
                        q = {"bid": float(m.get("best_bid") or 0), "ask": float(m.get("best_ask") or 0), "last": float(m.get("price") or 0), "ts": int(time.time() * 1000)}
                        async with self.lock:
                            self.external["Coinbase"][sym] = q
            except Exception:
                self.connections["Coinbase"] = "reconnecting"
                await asyncio.sleep(2)

    async def snapshot(self):
        async with self.lock:
            alpha, ext, stocks, connections = list(self.alpha), {k: dict(v) for k, v in self.external.items()}, list(self.bstocks), dict(self.connections)

        rows = []
        for a in alpha:
            coin = a.get("coin", "")
            key = norm(coin + "USDT") if coin else ""
            venues = {name: (ext[name].get(key) if key else None) for name in ext}
            spreads = {name: spread_pct(a["price"], q.get("last") if q else None) for name, q in venues.items()}
            valid = [v for v in spreads.values() if isinstance(v, (int, float))]
            best = max(valid) if valid else None
            best_abs = max((abs(v) for v in valid), default=None)
            rows.append({
                "symbol": a["alpha_id"],
                "coin": coin or a.get("symbol") or a["alpha_id"],
                "alpha_name": a.get("symbol") or coin or a["alpha_id"],
                "alpha": a["price"],
                "alpha_ts": a["ts"],
                "venues": venues,
                "spreads": spreads,
                "best_spread": best,
                "best_abs_spread": best_abs,
            })

        # Default ranking: largest positive spread first. If no positive spread
        # exists, fall back to largest absolute spread, then Alpha ID.
        rows.sort(key=lambda r: (
            r["best_spread"] is not None and r["best_spread"] > 0,
            r["best_spread"] if r["best_spread"] is not None else float("-inf"),
            r["best_abs_spread"] if r["best_abs_spread"] is not None else float("-inf"),
            r["symbol"],
        ), reverse=True)

        return {"updated_at": int(time.time() * 1000), "alpha": rows, "bstocks": stocks, "connections": connections, "mode": "server-websocket", "sort": "best-positive-spread-desc"}

    async def start(self):
        self.tasks = [asyncio.create_task(self.refresh_alpha()), asyncio.create_task(self.refresh_bstocks())]
        # Give the Alpha list a moment to populate before opening exchange streams.
        await asyncio.sleep(2)
        self.tasks += [asyncio.create_task(self.bybit()), asyncio.create_task(self.gate()), asyncio.create_task(self.okx()), asyncio.create_task(self.coinbase())]


engine = MarketEngine()
