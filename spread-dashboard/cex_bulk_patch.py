import asyncio
import time
import types
import httpx

BYBIT_TICKERS_URL = "https://api.bybit.com/v5/market/tickers"
OKX_TICKERS_URL = "https://www.okx.com/api/v5/market/tickers"
GATE_TICKERS_URL = "https://api.gateio.ws/api/v4/spot/tickers"


def _base_aliases(row):
    vals = set()
    for k in ("cex_coin", "coin"):
        v = str(row.get(k) or "").upper().strip()
        if v:
            vals.add(v)
    ms = str(row.get("market_symbol") or "").upper()
    if ms.startswith("ALPHA_"):
        ms = ms[6:]
    if ms.endswith("USDT"):
        ms = ms[:-4]
    if ms:
        vals.add(ms)
    return vals


def _store_aliases(out, symbol, q, rows):
    out[symbol] = q
    base = symbol[:-4] if symbol.endswith("USDT") else symbol
    out[base] = q
    for row in rows:
        if _base_aliases(row) & {base}:
            coin = str(row.get("coin") or "").upper()
            cex = str(row.get("cex_coin") or "").upper()
            if coin: out[coin] = q
            if cex: out[cex] = q
            aid = str(row.get("alpha_id") or "").upper()
            if aid: out[aid] = q


def install_cex_bulk(engine):
    async def coins(self):
        async with self.lock:
            return list(self.alpha)

    async def bulk_loop(self, name, fetcher, interval=1.0):
        while True:
            try:
                rows = await fetcher()
                async with self.lock:
                    self.external[name] = rows
                    self.connections[name] = "connected"
            except Exception:
                self.connections[name] = "reconnecting"
            await asyncio.sleep(interval)

    async def bybit_fetch(self):
        alpha = await self._cex_coins(); wanted = {v for r in alpha for v in _base_aliases(r)}
        async with httpx.AsyncClient(timeout=6) as c:
            r = await c.get(BYBIT_TICKERS_URL, params={"category": "spot"}, headers={"Cache-Control": "no-cache"})
            r.raise_for_status(); payload = r.json()
        out = {}
        for d in (payload.get("result") or {}).get("list") or []:
            s = str(d.get("symbol") or "").upper()
            if not s.endswith("USDT"): continue
            base = s[:-4]
            if base not in wanted: continue
            try:
                bid, ask = float(d.get("bid1Price")), float(d.get("ask1Price"))
                if bid > 0 and ask > 0: _store_aliases(out, s, {"bid":bid,"ask":ask,"last":float(d.get("lastPrice") or (bid+ask)/2),"ts":int(payload.get("time") or time.time()*1000)}, alpha)
            except Exception: pass
        return out

    async def okx_fetch(self):
        alpha = await self._cex_coins(); wanted = {v for r in alpha for v in _base_aliases(r)}
        async with httpx.AsyncClient(timeout=6) as c:
            r = await c.get(OKX_TICKERS_URL, params={"instType":"SPOT"}, headers={"Cache-Control":"no-cache"})
            r.raise_for_status(); payload = r.json()
        out = {}
        for d in payload.get("data") or []:
            s = str(d.get("instId") or "").upper()
            if not s.endswith("-USDT"): continue
            base = s[:-5]
            if base not in wanted: continue
            try:
                bid, ask = float(d.get("bidPx")), float(d.get("askPx"))
                if bid > 0 and ask > 0:
                    _store_aliases(out, s.replace("-", ""), {"bid":bid,"ask":ask,"last":float(d.get("last") or (bid+ask)/2),"ts":int(d.get("ts") or time.time()*1000)}, alpha)
            except Exception: pass
        return out

    async def gate_fetch(self):
        alpha = await self._cex_coins(); wanted = {v for r in alpha for v in _base_aliases(r)}
        async with httpx.AsyncClient(timeout=6) as c:
            r = await c.get(GATE_TICKERS_URL, headers={"Cache-Control":"no-cache"})
            r.raise_for_status(); payload = r.json()
        out = {}
        for d in payload if isinstance(payload, list) else []:
            s = str(d.get("currency_pair") or "").upper()
            if not s.endswith("_USDT"): continue
            base = s[:-5]
            if base not in wanted: continue
            try:
                bid, ask = float(d.get("highest_bid")), float(d.get("lowest_ask"))
                if bid > 0 and ask > 0: _store_aliases(out, s.replace("_", ""), {"bid":bid,"ask":ask,"last":float(d.get("last") or (bid+ask)/2),"ts":int(time.time()*1000)}, alpha)
            except Exception: pass
        return out

    async def start(self):
        self.tasks = [asyncio.create_task(self.refresh_alpha()), asyncio.create_task(self.refresh_alpha_depth()), asyncio.create_task(self.seed_missing_alpha()), asyncio.create_task(self.refresh_bstocks())]
        for _ in range(80):
            await asyncio.sleep(0.25)
            async with self.lock:
                if self.alpha: break
        self.tasks += [asyncio.create_task(self._bybit_loop()), asyncio.create_task(self._gate_loop()), asyncio.create_task(self._okx_loop())]

    engine._cex_coins = types.MethodType(coins, engine)
    engine._bulk_loop = types.MethodType(bulk_loop, engine)
    engine._bybit_fetch = types.MethodType(bybit_fetch, engine)
    engine._okx_fetch = types.MethodType(okx_fetch, engine)
    engine._gate_fetch = types.MethodType(gate_fetch, engine)
    engine._bybit_loop = types.MethodType(lambda self: self._bulk_loop("Bybit", self._bybit_fetch, 1.0), engine)
    engine._okx_loop = types.MethodType(lambda self: self._bulk_loop("OKX", self._okx_fetch, 1.0), engine)
    engine._gate_loop = types.MethodType(lambda self: self._bulk_loop("Gate", self._gate_fetch, 1.0), engine)
    engine.external.pop("Coinbase", None)
    engine.connections.pop("Coinbase", None)
    engine.start = types.MethodType(start, engine)
    return engine
