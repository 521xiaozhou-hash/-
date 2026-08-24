import asyncio
import json
import os
import time
from pathlib import Path
from typing import Any

import httpx
import websockets

ALPHA_LIST_URL = "https://www.binance.com/bapi/defi/v1/public/wallet-direct/buw/wallet/cex/alpha/all/token/list"
ALPHA_DEPTH_URL = "https://www.binance.com/bapi/defi/v1/public/alpha-trade/fullDepth"
BSTOCK_LIST_URL = "https://www.binance.com/bapi/defi/v1/public/wallet-direct/buw/wallet/market/token/rwa/stock/detail/list/ai"
BSTOCK_PRICE_URL = "https://www.binance.com/bapi/defi/v2/public/wallet-direct/buw/wallet/market/token/rwa/dynamic/ai"
BINANCE_SPOT_BOOK = "https://api.binance.com/api/v3/ticker/bookTicker"
BINANCE_STOCK_QUOTE = "https://api.binance.com/sapi/v1/equity/market/quote"
GATE_STOCK_BOOK = "https://api.gateio.ws/api/v4/stock/market/{symbol}/orderbook"
BYBIT_WS = "wss://stream.bybit.com/v5/public/spot"
GATE_WS = "wss://api.gateio.ws/ws/v4/"
OKX_WS = "wss://ws.okx.com:8443/ws/v5/public"
COINBASE_WS = "wss://ws-feed.exchange.coinbase.com"

ALPHA_REFRESH = int(os.getenv("ALPHA_REFRESH_SECONDS", "60"))
ALPHA_DEPTH_REFRESH = int(os.getenv("ALPHA_DEPTH_REFRESH_SECONDS", "2"))
BSTOCK_REFRESH = int(os.getenv("BSTOCK_REFRESH_SECONDS", "15"))
HTTP_TIMEOUT = float(os.getenv("REQUEST_TIMEOUT", "6"))
BINANCE_STOCK_API_KEY = os.getenv("BINANCE_STOCK_API_KEY", "").strip()
ALPHA_SYMBOLS = {x.strip().upper() for x in os.getenv("ALPHA_SYMBOLS", "").split(",") if x.strip()}
BSTOCK_TICKERS = {x.strip().upper() for x in os.getenv("BSTOCK_TICKERS", "").split(",") if x.strip()}
BLACKLIST_FILE = Path(__file__).resolve().parent / os.getenv("BLACKLIST_FILE", "blacklist.json")


def norm(s: str) -> str:
    return str(s or "").upper().replace("-", "").replace("_", "").replace("/", "")


def pct(buy_price: Any, sell_price: Any):
    """Actual executable spread: buy at ask, sell at bid."""
    try:
        buy_price, sell_price = float(buy_price), float(sell_price)
        if buy_price <= 0 or sell_price <= 0:
            return None
        return (sell_price / buy_price - 1.0) * 100.0
    except Exception:
        return None


def quote(bid, ask, ts=None):
    try:
        bid = float(bid) if bid not in (None, "") else None
        ask = float(ask) if ask not in (None, "") else None
    except Exception:
        bid = ask = None
    return {"bid": bid, "ask": ask, "last": (bid + ask) / 2 if bid and ask else (bid or ask), "ts": ts or int(time.time() * 1000)}


def opportunity(a, b, aname, bname):
    """Return both executable directions between two markets."""
    if not a or not b:
        return {"buy_market": None, "sell_market": None, "spread": None, "buy_price": None, "sell_price": None}
    candidates = []
    # Buy A at A ask, sell B at B bid.
    s1 = pct(a.get("ask"), b.get("bid"))
    if s1 is not None:
        candidates.append({"buy_market": aname, "sell_market": bname, "spread": s1, "buy_price": a.get("ask"), "sell_price": b.get("bid")})
    # Buy B at B ask, sell A at A bid.
    s2 = pct(b.get("ask"), a.get("bid"))
    if s2 is not None:
        candidates.append({"buy_market": bname, "sell_market": aname, "spread": s2, "buy_price": b.get("ask"), "sell_price": a.get("bid")})
    return max(candidates, key=lambda x: x["spread"], default={"buy_market": None, "sell_market": None, "spread": None, "buy_price": None, "sell_price": None})


class MarketEngine:
    def __init__(self):
        self.lock = asyncio.Lock()
        self.alpha = []
        self.alpha_quotes = {}
        self.external = {x: {} for x in ("Bybit", "Gate", "OKX", "Coinbase")}
        self.bstocks = []
        self.bstock_quotes = {}
        self.binance_stocks = {}
        self.gate_stocks = {}
        self.connections = {x: "disconnected" for x in self.external}
        self.tasks = []

    def blacklist(self):
        try:
            d = json.loads(BLACKLIST_FILE.read_text())
            return {str(x).upper() for x in d.get("symbols", [])}
        except Exception:
            return set()

    async def http_json(self, client, url, params=None, headers=None):
        try:
            r = await client.get(url, params=params, headers=headers, timeout=HTTP_TIMEOUT)
            r.raise_for_status()
            return r.json()
        except Exception:
            return None

    async def refresh_alpha(self):
        headers = {"User-Agent": "spread-dashboard/5.0", "Accept": "application/json"}
        while True:
            try:
                async with httpx.AsyncClient(headers=headers) as c:
                    d = await self.http_json(c, ALPHA_LIST_URL)
                    blocked = self.blacklist(); rows = []
                    items = d.get("data") or [] if isinstance(d, dict) else []
                    for x in items:
                        if not isinstance(x, dict):
                            continue
                        aid = str(x.get("alphaId") or x.get("symbol") or "").upper()
                        coin = str(x.get("cexCoinName") or x.get("symbol") or "").upper()
                        if not aid or not coin:
                            continue
                        if aid in blocked or coin in blocked or norm(coin + "USDT") in blocked:
                            continue
                        if ALPHA_SYMBOLS and aid not in ALPHA_SYMBOLS and coin not in ALPHA_SYMBOLS and norm(coin + "USDT") not in ALPHA_SYMBOLS:
                            continue
                        rows.append({"alpha_id": aid, "coin": coin, "price": None, "ts": int(time.time() * 1000)})
                    async with self.lock:
                        self.alpha = rows
            except Exception:
                pass
            await asyncio.sleep(ALPHA_REFRESH)

    async def refresh_alpha_depth(self):
        """Use Binance Alpha fullDepth so spread uses executable Alpha bid/ask, not last price."""
        while True:
            try:
                async with self.lock:
                    symbols = [x["alpha_id"] for x in self.alpha]
                async with httpx.AsyncClient(headers={"User-Agent": "spread-dashboard/5.0"}) as c:
                    sem = asyncio.Semaphore(12)
                    async def one(symbol):
                        async with sem:
                            d = await self.http_json(c, ALPHA_DEPTH_URL, {"symbol": symbol, "limit": 5})
                            try:
                                x = d["data"]
                                bid = float(x["bids"][0][0]) if x.get("bids") else None
                                ask = float(x["asks"][0][0]) if x.get("asks") else None
                                return symbol, quote(bid, ask, x.get("E") or int(time.time()*1000))
                            except Exception:
                                return symbol, None
                    results = await asyncio.gather(*(one(s) for s in symbols))
                async with self.lock:
                    self.alpha_quotes = {s: q for s, q in results if q}
                    for x in self.alpha:
                        q = self.alpha_quotes.get(x["alpha_id"])
                        if q:
                            x["price"] = q["last"]
                            x["ts"] = q["ts"]
            except Exception:
                pass
            await asyncio.sleep(ALPHA_DEPTH_REFRESH)

    async def refresh_bstocks(self):
        headers = {"User-Agent": "spread-dashboard/5.0", "Accept": "application/json"}
        while True:
            try:
                async with httpx.AsyncClient(headers=headers) as c:
                    listing = await self.http_json(c, BSTOCK_LIST_URL, {"type": 3})
                    items = (listing.get("data") or []) if isinstance(listing, dict) else []
                    blocked = self.blacklist()
                    items = [x for x in items if isinstance(x, dict) and str(x.get("ticker") or "").upper() not in blocked]
                    if BSTOCK_TICKERS:
                        items = [x for x in items if str(x.get("ticker") or "").upper() in BSTOCK_TICKERS]

                    async def one(x):
                        ticker = str(x.get("ticker") or "").upper(); addr = x.get("contractAddress"); chain = str(x.get("chainId") or "")
                        price = None
                        if addr:
                            d = await self.http_json(c, BSTOCK_PRICE_URL, {"chainId": chain, "contractAddress": addr})
                            try: price = float(d["data"]["tokenInfo"]["price"])
                            except Exception: pass
                        return {"ticker": ticker, "symbol": x.get("symbol") or ticker, "price": price, "chainId": chain, "ts": int(time.time()*1000)}

                    rows = await asyncio.gather(*(one(x) for x in items))
                    # bStocks are traded on Binance Spot with symbols such as TSLABUSDT.
                    spot = await self.http_json(c, BINANCE_SPOT_BOOK)
                    spot_map = {}
                    if isinstance(spot, list):
                        for q in spot:
                            s = str(q.get("symbol") or "").upper()
                            if s.endswith("USDT"):
                                spot_map[s[:-4]] = quote(q.get("bidPrice"), q.get("askPrice"), q.get("time"))
                    async with self.lock:
                        self.bstocks = rows
                        self.bstock_quotes = {t: spot_map.get(t) for t in [r["ticker"] for r in rows]}
                    await self.refresh_binance_stocks(c, [r["ticker"].removesuffix("B") for r in rows])
                    await self.refresh_gate_stocks(c, [r["ticker"].removesuffix("B") for r in rows])
            except Exception:
                pass
            await asyncio.sleep(BSTOCK_REFRESH)

    async def refresh_binance_stocks(self, c, tickers):
        """Binance Stocks Trading quote. Requires a read-only Binance API key."""
        out = {}
        if not BINANCE_STOCK_API_KEY:
            async with self.lock: self.binance_stocks = {}
            return
        headers = {"X-MBX-APIKEY": BINANCE_STOCK_API_KEY}
        sem = asyncio.Semaphore(8)
        async def one(t):
            async with sem:
                d = await self.http_json(c, BINANCE_STOCK_QUOTE, {"symbol": t}, headers=headers)
                try:
                    return t, quote(d["bidPrice"], d["askPrice"])
                except Exception:
                    return t, None
        results = await asyncio.gather(*(one(t) for t in tickers))
        out = {t: q for t, q in results if q}
        async with self.lock: self.binance_stocks = out

    async def refresh_gate_stocks(self, c, tickers):
        out = {}; sem = asyncio.Semaphore(5)
        async def one(t):
            async with sem:
                d = await self.http_json(c, GATE_STOCK_BOOK.format(symbol=t))
                try:
                    data = d["data"]
                    bid = float(data["bids"][0]["p"]) if data.get("bids") else None
                    ask = float(data["asks"][0]["p"]) if data.get("asks") else None
                    return t, quote(bid, ask, d.get("timestamp"))
                except Exception:
                    return t, None
        results = await asyncio.gather(*(one(t) for t in tickers))
        async with self.lock: self.gate_stocks = {t: q for t, q in results if q}

    async def bybit(self):
        while True:
            try:
                async with websockets.connect(BYBIT_WS, ping_interval=20, ping_timeout=10, max_size=8*1024*1024) as ws:
                    self.connections["Bybit"] = "connected"
                    async with self.lock: coins = [x["coin"] for x in self.alpha]
                    for i in range(0, len(coins), 10):
                        await ws.send(json.dumps({"op":"subscribe","args":[f"tickers.{c}USDT" for c in coins[i:i+10]]}))
                    async for raw in ws:
                        m=json.loads(raw); d=m.get("data") or {}; d=d[0] if isinstance(d,list) and d else d
                        if not str(m.get("topic","")).startswith("tickers."): continue
                        sym=norm(d.get("symbol"))
                        if sym:
                            q=quote(d.get("bid1Price"),d.get("ask1Price"),m.get("ts"))
                            async with self.lock:self.external["Bybit"][sym]=q
            except Exception:
                self.connections["Bybit"]="reconnecting"; await asyncio.sleep(2)

    async def gate(self):
        while True:
            try:
                async with websockets.connect(GATE_WS,ping_interval=20,ping_timeout=10,max_size=8*1024*1024) as ws:
                    self.connections["Gate"]="connected"
                    async with self.lock: coins=[x["coin"] for x in self.alpha]
                    for i in range(0,len(coins),100):
                        await ws.send(json.dumps({"time":int(time.time()),"channel":"spot.book_ticker","event":"subscribe","payload":[f"{c}_USDT" for c in coins[i:i+100]]}))
                    async for raw in ws:
                        m=json.loads(raw)
                        if m.get("channel")!="spot.book_ticker" or m.get("event")!="update": continue
                        d=m.get("result") or {}; sym=norm(d.get("s") or d.get("currency_pair"))
                        if sym:
                            q=quote(d.get("b") or d.get("highest_bid"),d.get("a") or d.get("lowest_ask"),d.get("t"))
                            async with self.lock:self.external["Gate"][sym]=q
            except Exception:
                self.connections["Gate"]="reconnecting"; await asyncio.sleep(2)

    async def okx(self):
        while True:
            try:
                async with websockets.connect(OKX_WS,ping_interval=20,ping_timeout=10,max_size=8*1024*1024) as ws:
                    self.connections["OKX"]="connected"
                    async with self.lock: coins=[x["coin"] for x in self.alpha]
                    await ws.send(json.dumps({"op":"subscribe","args":[{"channel":"tickers","instId":f"{c}-USDT"} for c in coins]}))
                    async for raw in ws:
                        m=json.loads(raw)
                        for d in m.get("data") or []:
                            sym=norm(d.get("instId"))
                            if sym:
                                q=quote(d.get("bidPx"),d.get("askPx"),d.get("ts"))
                                async with self.lock:self.external["OKX"][sym]=q
            except Exception:
                self.connections["OKX"]="reconnecting"; await asyncio.sleep(2)

    async def coinbase(self):
        while True:
            try:
                async with websockets.connect(COINBASE_WS,ping_interval=20,ping_timeout=10,max_size=8*1024*1024) as ws:
                    self.connections["Coinbase"]="connected"
                    async with self.lock: coins=[x["coin"] for x in self.alpha]
                    await ws.send(json.dumps({"type":"subscribe","product_ids":[f"{c}-USDT" for c in coins]+[f"{c}-USD" for c in coins],"channels":["ticker","heartbeat"]}))
                    async for raw in ws:
                        m=json.loads(raw)
                        if m.get("type")!="ticker": continue
                        sym=norm(m.get("product_id"))
                        if sym:
                            q=quote(m.get("best_bid"),m.get("best_ask"),int(time.time()*1000))
                            async with self.lock:self.external["Coinbase"][sym]=q
            except Exception:
                self.connections["Coinbase"]="reconnecting"; await asyncio.sleep(2)

    async def snapshot(self):
        async with self.lock:
            alpha=list(self.alpha); aq=dict(self.alpha_quotes); ext={k:dict(v) for k,v in self.external.items()}; stocks=list(self.bstocks); bq=dict(self.bstock_quotes); bs=dict(self.binance_stocks); gs=dict(self.gate_stocks); con=dict(self.connections)
        rows=[]
        for a in alpha:
            key=norm(a["coin"]+"USDT"); alpha_q=aq.get(a["alpha_id"]); venues={n:ext[n].get(key) for n in ext}
            opp={n:opportunity(alpha_q,venues[n],"Binance Alpha",n) for n in venues}
            best=max(opp.values(),key=lambda x:x.get("spread") if x.get("spread") is not None else -1e99)
            rows.append({"symbol":a["alpha_id"],"coin":a["coin"],"alpha":alpha_q,"venues":venues,"opportunities":opp,"best_opportunity":best})
        rows.sort(key=lambda x:x["best_opportunity"].get("spread") if x["best_opportunity"].get("spread") is not None else -1e99,reverse=True)

        stock_rows=[]
        for s in stocks:
            t=s["ticker"]; b=bq.get(t); underlying=t.removesuffix("B"); bn=bs.get(underlying); g=gs.get(underlying)
            markets={"bStock":b,"Binance Stock":bn,"Gate Stock":g}
            pairs={}
            names=list(markets)
            for i in range(len(names)):
                for j in range(i+1,len(names)):
                    pairs[f"{names[i]} ↔ {names[j]}"]=opportunity(markets[names[i]],markets[names[j]],names[i],names[j])
            best=max(pairs.values(),key=lambda x:x.get("spread") if x.get("spread") is not None else -1e99,default={"spread":None})
            stock_rows.append({**s,"underlying":underlying,"markets":markets,"opportunities":pairs,"best_opportunity":best})
        stock_rows.sort(key=lambda x:x["best_opportunity"].get("spread") if x["best_opportunity"].get("spread") is not None else -1e99,reverse=True)
        return {"updated_at":int(time.time()*1000),"alpha":rows,"bstocks":stock_rows,"connections":con,"mode":"server-websocket-bidask","binance_stock_api_configured":bool(BINANCE_STOCK_API_KEY)}

    async def start(self):
        self.tasks=[asyncio.create_task(self.refresh_alpha()),asyncio.create_task(self.refresh_alpha_depth()),asyncio.create_task(self.refresh_bstocks())]
        await asyncio.sleep(2)
        self.tasks += [asyncio.create_task(self.bybit()),asyncio.create_task(self.gate()),asyncio.create_task(self.okx()),asyncio.create_task(self.coinbase())]

engine=MarketEngine()
