import asyncio
import json
import time
import types
import httpx
import websockets

BYBIT_TICKERS_URL = "https://api.bybit.com/v5/market/tickers"
OKX_TICKERS_URL = "https://www.okx.com/api/v5/market/tickers"
GATE_TICKERS_URL = "https://api.gateio.ws/api/v4/spot/tickers"
COINBASE_WS = "wss://advanced-trade-ws.coinbase.com"
COINBASE_PRODUCTS_URL = "https://api.exchange.coinbase.com/products"


def install_cex_bulk(engine):
    async def coins(self):
        async with self.lock:
            return sorted({str(x.get("cex_coin") or "").upper() for x in self.alpha if x.get("cex_coin")})

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
        coins = await self._cex_coins()
        wanted = {f"{c}USDT" for c in coins}
        async with httpx.AsyncClient(timeout=6) as c:
            r = await c.get(BYBIT_TICKERS_URL, params={"category": "spot"}, headers={"Cache-Control": "no-cache"})
            r.raise_for_status(); payload = r.json()
        out = {}
        for d in (payload.get("result") or {}).get("list") or []:
            s = str(d.get("symbol") or "").upper()
            if s in wanted:
                try:
                    bid = float(d.get("bid1Price")); ask = float(d.get("ask1Price"))
                    if bid > 0 and ask > 0:
                        out[s] = {"bid": bid, "ask": ask, "last": float(d.get("lastPrice") or (bid + ask) / 2), "ts": int(payload.get("time") or time.time()*1000)}
                except Exception:
                    pass
        return out

    async def okx_fetch(self):
        coins = await self._cex_coins(); wanted = {f"{c}-USDT" for c in coins}
        async with httpx.AsyncClient(timeout=6) as c:
            r = await c.get(OKX_TICKERS_URL, params={"instType": "SPOT"}, headers={"Cache-Control": "no-cache"})
            r.raise_for_status(); payload = r.json()
        out = {}
        for d in payload.get("data") or []:
            s = str(d.get("instId") or "").upper()
            if s in wanted:
                try:
                    bid = float(d.get("bidPx")); ask = float(d.get("askPx"))
                    if bid > 0 and ask > 0:
                        out[s.replace("-", "")] = {"bid": bid, "ask": ask, "last": float(d.get("last") or (bid+ask)/2), "ts": int(d.get("ts") or time.time()*1000)}
                except Exception:
                    pass
        return out

    async def gate_fetch(self):
        coins = await self._cex_coins(); wanted = {f"{c}_USDT" for c in coins}
        async with httpx.AsyncClient(timeout=6) as c:
            r = await c.get(GATE_TICKERS_URL, headers={"Cache-Control": "no-cache"})
            r.raise_for_status(); payload = r.json()
        out = {}
        for d in payload if isinstance(payload, list) else []:
            s = str(d.get("currency_pair") or "").upper()
            if s in wanted:
                try:
                    bid = float(d.get("highest_bid")); ask = float(d.get("lowest_ask"))
                    if bid > 0 and ask > 0:
                        out[s.replace("_", "")] = {"bid": bid, "ask": ask, "last": float(d.get("last") or (bid+ask)/2), "ts": int(time.time()*1000)}
                except Exception:
                    pass
        return out

    async def _coinbase_worker(self, products):
        async with websockets.connect(COINBASE_WS, ping_interval=20, ping_timeout=10, max_size=16*1024*1024) as ws:
            await ws.send(json.dumps({"type":"subscribe","product_ids":products,"channel":"ticker"}))
            await ws.send(json.dumps({"type":"subscribe","channel":"heartbeats"}))
            async for raw in ws:
                try:
                    m = json.loads(raw)
                    if m.get("channel") != "ticker":
                        continue
                    for event in m.get("events") or []:
                        for d in event.get("tickers") or []:
                            pid = str(d.get("product_id") or "").upper()
                            bid = d.get("best_bid"); ask = d.get("best_ask")
                            if not pid or bid in (None, "") or ask in (None, ""):
                                continue
                            bid_f = float(bid); ask_f = float(ask)
                            if bid_f <= 0 or ask_f <= 0:
                                continue
                            async with self.lock:
                                self.external["Coinbase"][pid.replace("-", "")] = {"bid":bid_f,"ask":ask_f,"last":float(d.get("price") or (bid_f+ask_f)/2),"ts":int(time.time()*1000)}
                except Exception:
                    continue

    async def coinbase_loop(self):
        while True:
            workers = []
            try:
                coins = await self._cex_coins()
                products = sorted({f"{c}-USD" for c in coins})
                if not products:
                    self.connections["Coinbase"] = "waiting-alpha"
                    await asyncio.sleep(2); continue
                # Keep subscriptions manageable and reconnect when the Alpha universe changes.
                for i in range(0, len(products), 100):
                    workers.append(asyncio.create_task(self._coinbase_worker(products[i:i+100])))
                self.connections["Coinbase"] = "connected"
                signature = tuple(products)
                while True:
                    await asyncio.sleep(5)
                    current = sorted({f"{c}-USD" for c in await self._cex_coins()})
                    if tuple(current) != signature:
                        break
                for t in workers: t.cancel()
                await asyncio.gather(*workers, return_exceptions=True)
            except Exception:
                self.connections["Coinbase"] = "reconnecting"
                for t in workers: t.cancel()
                if workers: await asyncio.gather(*workers, return_exceptions=True)
                await asyncio.sleep(2)

    async def bybit_loop(self):
        await self._bulk_loop("Bybit", self._bybit_fetch, 1.0)
    async def okx_loop(self):
        await self._bulk_loop("OKX", self._okx_fetch, 1.0)
    async def gate_loop(self):
        await self._bulk_loop("Gate", self._gate_fetch, 1.0)

    async def start(self):
        self.tasks=[asyncio.create_task(self.refresh_alpha()),asyncio.create_task(self.refresh_alpha_depth()),asyncio.create_task(self.refresh_bstocks())]
        for _ in range(60):
            await asyncio.sleep(0.25)
            async with self.lock:
                if self.alpha: break
        self.tasks += [asyncio.create_task(self._bybit_loop()),asyncio.create_task(self._gate_loop()),asyncio.create_task(self._okx_loop()),asyncio.create_task(self._coinbase_loop())]

    engine._cex_coins = types.MethodType(coins, engine)
    engine._bulk_loop = types.MethodType(bulk_loop, engine)
    engine._bybit_fetch = types.MethodType(bybit_fetch, engine)
    engine._okx_fetch = types.MethodType(okx_fetch, engine)
    engine._gate_fetch = types.MethodType(gate_fetch, engine)
    engine._coinbase_worker = types.MethodType(_coinbase_worker, engine)
    engine._coinbase_loop = types.MethodType(coinbase_loop, engine)
    engine._bybit_loop = types.MethodType(bybit_loop, engine)
    engine._okx_loop = types.MethodType(okx_loop, engine)
    engine._gate_loop = types.MethodType(gate_loop, engine)
    engine.start = types.MethodType(start, engine)
    return engine
