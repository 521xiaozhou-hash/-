import asyncio
import json
import os
import time
from pathlib import Path
from typing import Any

import httpx
import websockets

ALPHA_LIST_URL = "https://www.binance.com/bapi/defi/v1/public/wallet-direct/buw/wallet/cex/alpha/all/token/list"
BSTOCK_LIST_URL = "https://www.binance.com/bapi/defi/v1/public/wallet-direct/buw/wallet/market/token/rwa/stock/detail/list/ai"
BSTOCK_PRICE_URL = "https://www.binance.com/bapi/defi/v2/public/wallet-direct/buw/wallet/market/token/rwa/dynamic/ai"
BINANCE_SPOT_BOOK = "https://api.binance.com/api/v3/ticker/bookTicker"
GATE_STOCK_SYMBOLS = "https://api.gateio.ws/api/v4/stock/symbols"
GATE_STOCK_BOOK = "https://api.gateio.ws/api/v4/stock/market/{symbol}/orderbook"
BYBIT_WS = "wss://stream.bybit.com/v5/public/spot"
GATE_WS = "wss://api.gateio.ws/ws/v4/"
OKX_WS = "wss://ws.okx.com:8443/ws/v5/public"
COINBASE_WS = "wss://ws-feed.exchange.coinbase.com"
ALPHA_REFRESH = int(os.getenv("ALPHA_REFRESH_SECONDS", "60"))
BSTOCK_REFRESH = int(os.getenv("BSTOCK_REFRESH_SECONDS", "30"))
HTTP_TIMEOUT = float(os.getenv("REQUEST_TIMEOUT", "6"))
ALPHA_SYMBOLS = {x.strip().upper() for x in os.getenv("ALPHA_SYMBOLS", "").split(",") if x.strip()}
BSTOCK_TICKERS = {x.strip().upper() for x in os.getenv("BSTOCK_TICKERS", "").split(",") if x.strip()}
BLACKLIST_FILE = Path(__file__).resolve().parent / os.getenv("BLACKLIST_FILE", "blacklist.json")

def norm(s: str) -> str:
    return str(s or "").upper().replace("-", "").replace("_", "").replace("/", "")

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
        self.binance_spot = {}
        self.gate_stocks = {}
        self.connections = {x: "disconnected" for x in self.external}
        self.tasks = []

    def blacklist(self):
        try:
            d = json.loads(BLACKLIST_FILE.read_text())
            return {str(x).upper() for x in d.get("symbols", [])}
        except Exception:
            return set()

    async def http_json(self, client, url, params=None):
        try:
            r = await client.get(url, params=params, timeout=HTTP_TIMEOUT)
            r.raise_for_status()
            return r.json()
        except Exception:
            return None

    async def refresh_alpha(self):
        headers = {"User-Agent": "spread-dashboard/4.0", "Accept": "application/json"}
        async with httpx.AsyncClient(headers=headers) as c:
            while True:
                d = await self.http_json(c, ALPHA_LIST_URL)
                blocked = self.blacklist(); rows = []
                for x in (d.get("data") or []) if isinstance(d, dict) else []:
                    if not isinstance(x, dict): continue
                    aid = str(x.get("alphaId") or x.get("symbol") or "").upper()
                    coin = str(x.get("cexCoinName") or x.get("symbol") or "").upper()
                    p = x.get("price") or x.get("lastPrice")
                    if not aid or not coin or p in (None, ""): continue
                    if aid in blocked or coin in blocked or norm(coin+"USDT") in blocked: continue
                    if ALPHA_SYMBOLS and aid not in ALPHA_SYMBOLS and coin not in ALPHA_SYMBOLS and norm(coin+"USDT") not in ALPHA_SYMBOLS: continue
                    try: price = float(p)
                    except Exception: continue
                    rows.append({"alpha_id": aid, "coin": coin, "price": price, "ts": int(time.time()*1000)})
                async with self.lock:
                    self.alpha = rows
                await asyncio.sleep(ALPHA_REFRESH)

    async def refresh_bstocks(self):
        headers = {"User-Agent": "spread-dashboard/4.0", "Accept": "application/json"}
        async with httpx.AsyncClient(headers=headers) as c:
            while True:
                listing = await self.http_json(c, BSTOCK_LIST_URL, {"type": 3})
                items = (listing.get("data") or []) if isinstance(listing, dict) else []
                blocked = self.blacklist()
                items = [x for x in items if isinstance(x, dict) and str(x.get("ticker") or "").upper() not in blocked]
                if BSTOCK_TICKERS: items = [x for x in items if str(x.get("ticker") or "").upper() in BSTOCK_TICKERS]
                async def one(x):
                    ticker = str(x.get("ticker") or "").upper(); addr = x.get("contractAddress"); chain = str(x.get("chainId") or "")
                    price = None
                    if addr:
                        d = await self.http_json(c, BSTOCK_PRICE_URL, {"chainId": chain, "contractAddress": addr})
                        try: price = float(d["data"]["tokenInfo"]["price"])
                        except Exception: pass
                    return {"ticker":ticker,"symbol":x.get("symbol") or ticker,"price":price,"chainId":chain,"ts":int(time.time()*1000)}
                rows = await asyncio.gather(*(one(x) for x in items))
                # Binance spot prices: one public batch request instead of one request per stock.
                spot = await self.http_json(c, BINANCE_SPOT_BOOK)
                spot_map = {}
                if isinstance(spot, list):
                    for q in spot:
                        s = str(q.get("symbol") or "").upper()
                        if s.endswith("USDT"):
                            spot_map[s[:-4]] = {"bid":float(q.get("bidPrice") or 0),"ask":float(q.get("askPrice") or 0),"last":(float(q.get("bidPrice") or 0)+float(q.get("askPrice") or 0))/2}
                async with self.lock:
                    self.bstocks = rows
                    self.binance_spot = spot_map
                await self.refresh_gate_stocks(c, [r["ticker"] for r in rows])
                await asyncio.sleep(BSTOCK_REFRESH)

    async def refresh_gate_stocks(self, c, tickers):
        out = {}
        # Gate's stock orderbook endpoint is public and rate-limited to 5 qps.
        # Limit concurrency so a full bStock list remains polite to the API.
        sem = asyncio.Semaphore(5)
        async def one(t):
            async with sem:
                d = await self.http_json(c, GATE_STOCK_BOOK.format(symbol=t), None)
                try:
                    data = d["data"]
                    bid = float(data["bids"][0]["p"]) if data.get("bids") else None
                    ask = float(data["asks"][0]["p"]) if data.get("asks") else None
                    return t, {"bid":bid,"ask":ask,"last":(bid+ask)/2 if bid and ask else (bid or ask),"ts":d.get("timestamp") or int(time.time()*1000)}
                except Exception:
                    return t, None
        results = await asyncio.gather(*(one(t) for t in tickers))
        for t, q in results:
            if q: out[t] = q
        async with self.lock: self.gate_stocks = out

    async def _subscribe(self, ws, args, chunk=100):
        for i in range(0, len(args), chunk):
            if args[i:i+chunk]: await ws.send(json.dumps({"op":"subscribe","args":args[i:i+chunk]}))

    async def bybit(self):
        while True:
            try:
                async with websockets.connect(BYBIT_WS, ping_interval=20, ping_timeout=10, max_size=8*1024*1024) as ws:
                    self.connections["Bybit"] = "connected"
                    async with self.lock: coins = [x["coin"] for x in self.alpha]
                    await self._subscribe(ws, [f"tickers.{c}USDT" for c in coins], 10)
                    async for raw in ws:
                        m=json.loads(raw); d=m.get("data") or {}; d=d[0] if isinstance(d,list) and d else d
                        if not str(m.get("topic","")).startswith("tickers."): continue
                        sym=norm(d.get("symbol"));
                        if sym:
                            q={"bid":float(d.get("bid1Price") or 0),"ask":float(d.get("ask1Price") or 0),"last":float(d.get("lastPrice") or 0),"ts":int(m.get("ts") or time.time()*1000)}
                            async with self.lock:self.external["Bybit"][sym]=q
            except Exception:
                self.connections["Bybit"]="reconnecting"; await asyncio.sleep(2)

    async def gate(self):
        while True:
            try:
                async with websockets.connect(GATE_WS,ping_interval=20,ping_timeout=10,max_size=8*1024*1024) as ws:
                    self.connections["Gate"]="connected"
                    async with self.lock: coins=[x["coin"] for x in self.alpha]
                    await self._subscribe(ws,[f"spot.book_ticker"],100) if False else None
                    for i in range(0,len(coins),100):
                        pairs=[f"{c}_USDT" for c in coins[i:i+100]]
                        if pairs: await ws.send(json.dumps({"time":int(time.time()),"channel":"spot.book_ticker","event":"subscribe","payload":pairs}))
                    async for raw in ws:
                        m=json.loads(raw)
                        if m.get("channel")!="spot.book_ticker" or m.get("event")!="update": continue
                        d=m.get("result") or {}; sym=norm(d.get("s") or d.get("currency_pair"))
                        if sym:
                            q={"bid":float(d.get("b") or d.get("highest_bid") or 0),"ask":float(d.get("a") or d.get("lowest_ask") or 0),"last":float(d.get("b") or 0),"ts":int(d.get("t") or time.time()*1000)}
                            async with self.lock:self.external["Gate"][sym]=q
            except Exception:
                self.connections["Gate"]="reconnecting"; await asyncio.sleep(2)

    async def okx(self):
        while True:
            try:
                async with websockets.connect(OKX_WS,ping_interval=20,ping_timeout=10,max_size=8*1024*1024) as ws:
                    self.connections["OKX"]="connected"
                    async with self.lock: coins=[x["coin"] for x in self.alpha]
                    await self._subscribe(ws,[{"channel":"tickers","instId":f"{c}-USDT"} for c in coins],100)
                    async for raw in ws:
                        m=json.loads(raw)
                        for d in m.get("data") or []:
                            sym=norm(d.get("instId"))
                            if sym:
                                q={"bid":float(d.get("bidPx") or 0),"ask":float(d.get("askPx") or 0),"last":float(d.get("last") or 0),"ts":int(d.get("ts") or time.time()*1000)}
                                async with self.lock:self.external["OKX"][sym]=q
            except Exception:
                self.connections["OKX"]="reconnecting"; await asyncio.sleep(2)

    async def coinbase(self):
        while True:
            try:
                async with websockets.connect(COINBASE_WS,ping_interval=20,ping_timeout=10,max_size=8*1024*1024) as ws:
                    self.connections["Coinbase"]="connected"
                    async with self.lock: coins=[x["coin"] for x in self.alpha]
                    products=[f"{c}-USDT" for c in coins]+[f"{c}-USD" for c in coins]
                    await ws.send(json.dumps({"type":"subscribe","product_ids":products,"channels":["ticker","heartbeat"]}))
                    async for raw in ws:
                        m=json.loads(raw)
                        if m.get("type")!="ticker": continue
                        sym=norm(m.get("product_id"))
                        if sym:
                            q={"bid":float(m.get("best_bid") or 0),"ask":float(m.get("best_ask") or 0),"last":float(m.get("price") or 0),"ts":int(time.time()*1000)}
                            async with self.lock:self.external["Coinbase"][sym]=q
            except Exception:
                self.connections["Coinbase"]="reconnecting"; await asyncio.sleep(2)

    async def snapshot(self):
        async with self.lock:
            alpha=list(self.alpha); ext={k:dict(v) for k,v in self.external.items()}; stocks=list(self.bstocks); spot=dict(self.binance_spot); gs=dict(self.gate_stocks); con=dict(self.connections)
        rows=[]
        for a in alpha:
            key=norm(a["coin"]+"USDT"); venues={n:ext[n].get(key) for n in ext}
            spreads={n:spread_pct(a["price"],q.get("last") if q else None) for n,q in venues.items()}
            rows.append({"symbol":a["alpha_id"],"coin":a["coin"],"alpha":a["price"],"alpha_ts":a["ts"],"venues":venues,"spreads":spreads,"max_spread":max([abs(x) for x in spreads.values() if x is not None],default=None)})
        rows.sort(key=lambda x: (x["max_spread"] is not None, x["max_spread"] or -1), reverse=True)
        stock_rows=[]
        for s in stocks:
            t=s["ticker"]; b=spot.get(t); g=gs.get(t); bp=s.get("price")
            stock_rows.append({**s,"binance_spot":b,"gate_stock":g,"bstock_vs_binance_spot":spread_pct(bp,b.get("last") if b else None),"bstock_vs_gate_stock":spread_pct(bp,g.get("last") if g else None)})
        stock_rows.sort(key=lambda x:max([abs(x.get("bstock_vs_binance_spot")) if x.get("bstock_vs_binance_spot") is not None else -1,abs(x.get("bstock_vs_gate_stock")) if x.get("bstock_vs_gate_stock") is not None else -1]),reverse=True)
        return {"updated_at":int(time.time()*1000),"alpha":rows,"bstocks":stock_rows,"connections":con,"mode":"server-websocket"}

    async def start(self):
        self.tasks=[asyncio.create_task(self.refresh_alpha()),asyncio.create_task(self.refresh_bstocks())]
        await asyncio.sleep(2)
        self.tasks += [asyncio.create_task(self.bybit()),asyncio.create_task(self.gate()),asyncio.create_task(self.okx()),asyncio.create_task(self.coinbase())]

engine=MarketEngine()
