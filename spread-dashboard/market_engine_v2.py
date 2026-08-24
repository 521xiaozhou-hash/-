import asyncio
import json
import os
import time
from pathlib import Path
from typing import Any

import httpx
import websockets

ALPHA_LIST_URL = "https://www.binance.com/bapi/defi/v1/public/wallet-direct/buw/wallet/cex/alpha/all/token/list"
ALPHA_EXCHANGE_INFO_URL = "https://www.binance.com/bapi/defi/v1/public/alpha-trade/get-exchange-info"
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

ALPHA_REFRESH = int(os.getenv("ALPHA_REFRESH_SECONDS", "30"))
ALPHA_DEPTH_REFRESH = int(os.getenv("ALPHA_DEPTH_REFRESH_SECONDS", "5"))
BSTOCK_REFRESH = int(os.getenv("BSTOCK_REFRESH_SECONDS", "15"))
HTTP_TIMEOUT = float(os.getenv("REQUEST_TIMEOUT", "8"))
BINANCE_STOCK_API_KEY = os.getenv("BINANCE_STOCK_API_KEY", "").strip()
ALPHA_SYMBOLS = {x.strip().upper() for x in os.getenv("ALPHA_SYMBOLS", "").split(",") if x.strip()}
BSTOCK_TICKERS = {x.strip().upper() for x in os.getenv("BSTOCK_TICKERS", "").split(",") if x.strip()}
BLACKLIST_FILE = Path(__file__).resolve().parent / os.getenv("BLACKLIST_FILE", "blacklist.json")


def norm(s: str) -> str:
    return str(s or "").upper().replace("-", "").replace("_", "").replace("/", "")


def pct(buy_price: Any, sell_price: Any):
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
    return {"bid": bid, "ask": ask, "last": (bid + ask) / 2 if bid is not None and ask is not None else (bid or ask), "ts": ts or int(time.time() * 1000)}


def opportunity(a, b, aname, bname):
    empty = {"buy_market": None, "sell_market": None, "spread": None, "buy_price": None, "sell_price": None}
    if not a or not b:
        return empty
    candidates = []
    s1 = pct(a.get("ask"), b.get("bid"))
    if s1 is not None:
        candidates.append({"buy_market": aname, "sell_market": bname, "spread": s1, "buy_price": a.get("ask"), "sell_price": b.get("bid")})
    s2 = pct(b.get("ask"), a.get("bid"))
    if s2 is not None:
        candidates.append({"buy_market": bname, "sell_market": aname, "spread": s2, "buy_price": b.get("ask"), "sell_price": a.get("bid")})
    return max(candidates, key=lambda x: x["spread"], default=empty)


class MarketEngine:
    def __init__(self):
        self.lock = asyncio.Lock()
        self.alpha = []
        self.alpha_quotes = {}
        self.alpha_diag = {"token_list": 0, "exchange_info": 0, "active": 0, "error": ""}
        self.external = {x: {} for x in ("Bybit", "Gate", "OKX", "Coinbase")}
        self.bstocks = []
        self.bstock_quotes = {}
        self.binance_stocks = {}
        self.gate_stocks = {}
        self.connections = {x: "disconnected" for x in self.external}
        self.connections["Binance Alpha"] = "disconnected"
        self.tasks = []

    def blacklist(self):
        try:
            d = json.loads(BLACKLIST_FILE.read_text())
            return {str(x).upper() for x in d.get("symbols", [])}
        except Exception:
            return set()

    async def http_json(self, client, url, params=None, headers=None):
        try:
            r = await client.get(url, params=params, headers=headers, timeout=HTTP_TIMEOUT, follow_redirects=True)
            r.raise_for_status()
            return r.json()
        except Exception:
            return None

    @staticmethod
    def _alpha_items(payload):
        if isinstance(payload, list):
            return payload
        if not isinstance(payload, dict):
            return []
        data = payload.get("data")
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            for key in ("data", "tokens", "list", "rows", "items"):
                if isinstance(data.get(key), list):
                    return data[key]
        return []

    @staticmethod
    def _alpha_symbols(payload):
        if not isinstance(payload, dict):
            return []
        data = payload.get("data")
        if not isinstance(data, dict):
            return []
        symbols = data.get("symbols")
        return symbols if isinstance(symbols, list) else []

    async def refresh_alpha(self):
        headers = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/131 Safari/537.36", "Accept": "application/json, text/plain, */*", "Referer": "https://www.binance.com/"}
        while True:
            try:
                async with httpx.AsyncClient(headers=headers) as c:
                    token_payload, exchange_payload = await asyncio.gather(self.http_json(c, ALPHA_LIST_URL), self.http_json(c, ALPHA_EXCHANGE_INFO_URL))
                    token_items = self._alpha_items(token_payload)
                    exchange_items = self._alpha_symbols(exchange_payload)
                    active = {}
                    for item in exchange_items:
                        if not isinstance(item, dict):
                            continue
                        status = str(item.get("status") or "").upper()
                        if status and status not in {"TRADING", "ONLINE", "1"}:
                            continue
                        symbol = str(item.get("symbol") or "").upper()
                        base = str(item.get("baseAsset") or "").upper()
                        if not base and symbol:
                            for q in ("USDT", "USDC", "BNB"):
                                if symbol.endswith(q):
                                    base = symbol[:-len(q)]
                                    break
                        if base.startswith("ALPHA_") and symbol:
                            active[base] = symbol

                    meta = {str(x.get("alphaId") or "").upper(): x for x in token_items if isinstance(x, dict) and x.get("alphaId")}
                    blocked = self.blacklist()
                    rows = []
                    for aid, market_symbol in active.items():
                        x = meta.get(aid, {})
                        coin = str(x.get("cexCoinName") or x.get("symbol") or "").upper()
                        display_coin = coin or aid
                        if aid in blocked or display_coin in blocked or norm(display_coin + "USDT") in blocked:
                            continue
                        if ALPHA_SYMBOLS and aid not in ALPHA_SYMBOLS and display_coin not in ALPHA_SYMBOLS and norm(display_coin + "USDT") not in ALPHA_SYMBOLS:
                            continue
                        rows.append({"alpha_id": aid, "market_symbol": market_symbol, "coin": display_coin, "cex_coin": coin, "price": None, "ts": int(time.time() * 1000)})

                    if not rows and not exchange_items:
                        for x in token_items:
                            if not isinstance(x, dict) or x.get("offline") or x.get("offsell"):
                                continue
                            aid = str(x.get("alphaId") or "").upper()
                            coin = str(x.get("cexCoinName") or x.get("symbol") or "").upper()
                            if not aid or not coin or aid in blocked or coin in blocked:
                                continue
                            rows.append({"alpha_id": aid, "market_symbol": aid + "USDT", "coin": coin, "cex_coin": coin, "price": None, "ts": int(time.time() * 1000)})

                    rows.sort(key=lambda x: x["coin"])
                    async with self.lock:
                        self.alpha = rows
                        self.alpha_diag = {"token_list": len(token_items), "exchange_info": len(exchange_items), "active": len(active), "error": "" if rows or exchange_items else "Binance Alpha API 无有效返回"}
            except Exception as e:
                async with self.lock:
                    self.alpha_diag["error"] = type(e).__name__
            await asyncio.sleep(ALPHA_REFRESH)

    async def refresh_alpha_depth(self):
        while True:
            try:
                async with self.lock:
                    symbols = [x["market_symbol"] for x in self.alpha]
                if not symbols:
                    await asyncio.sleep(3)
                    continue
                async with httpx.AsyncClient(headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json", "Referer": "https://www.binance.com/"}) as c:
                    sem = asyncio.Semaphore(16)
                    async def one(symbol):
                        async with sem:
                            d = await self.http_json(c, ALPHA_DEPTH_URL, {"symbol": symbol, "limit": 5})
                            try:
                                x = d["data"]
                                bid = x.get("bids", [])[0][0] if x.get("bids") else None
                                ask = x.get("asks", [])[0][0] if x.get("asks") else None
                                return symbol, quote(bid, ask, x.get("E") or int(time.time() * 1000))
                            except Exception:
                                return symbol, None
                    results = await asyncio.gather(*(one(s) for s in symbols))
                async with self.lock:
                    for s, q in results:
                        if q:
                            self.alpha_quotes[s] = q
                    for x in self.alpha:
                        q = self.alpha_quotes.get(x["market_symbol"])
                        if q:
                            x["price"] = q["last"]
                            x["ts"] = q["ts"]
            except Exception:
                pass
            await asyncio.sleep(ALPHA_DEPTH_REFRESH)

    async def refresh_bstocks(self):
        headers = {"User-Agent": "Mozilla/5.0", "Accept": "application/json", "Referer": "https://www.binance.com/"}
        while True:
            try:
                async with httpx.AsyncClient(headers=headers) as c:
                    listing = await self.http_json(c, BSTOCK_LIST_URL, {"type": 3})
                    items = self._alpha_items(listing)
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
                        return {"ticker": ticker, "symbol": x.get("symbol") or ticker, "price": price, "chainId": chain, "ts": int(time.time() * 1000)}
                    rows = await asyncio.gather(*(one(x) for x in items))
                    spot = await self.http_json(c, BINANCE_SPOT_BOOK)
                    spot_map = {}
                    if isinstance(spot, list):
                        for q in spot:
                            s = str(q.get("symbol") or "").upper()
                            if s.endswith("USDT"):
                                spot_map[s[:-4]] = quote(q.get("bidPrice"), q.get("askPrice"), q.get("time"))
                    async with self.lock:
                        self.bstocks = rows
                        self.bstock_quotes = {r["ticker"]: spot_map.get(r["ticker"])}
                    await self.refresh_binance_stocks(c, [r["ticker"].removesuffix("B") for r in rows])
                    await self.refresh_gate_stocks(c, [r["ticker"].removesuffix("B") for r in rows])
            except Exception:
                pass
            await asyncio.sleep(BSTOCK_REFRESH)

    async def refresh_binance_stocks(self, c, tickers):
        if not BINANCE_STOCK_API_KEY:
            async with self.lock: self.binance_stocks = {}
            return
        headers = {"X-MBX-APIKEY": BINANCE_STOCK_API_KEY}; sem = asyncio.Semaphore(8)
        async def one(t):
            async with sem:
                d = await self.http_json(c, BINANCE_STOCK_QUOTE, {"symbol": t}, headers=headers)
                try: return t, quote(d["bidPrice"], d["askPrice"])
                except Exception: return t, None
        results = await asyncio.gather(*(one(t) for t in tickers))
        async with self.lock: self.binance_stocks = {t: q for t, q in results if q}

    async def refresh_gate_stocks(self, c, tickers):
        sem = asyncio.Semaphore(5)
        async def one(t):
            async with sem:
                d = await self.http_json(c, GATE_STOCK_BOOK.format(symbol=t))
                try:
                    data = d["data"]
                    bid = data.get("bids", [])[0].get("p") if data.get("bids") else None
                    ask = data.get("asks", [])[0].get("p") if data.get("asks") else None
                    return t, quote(bid, ask, d.get("timestamp"))
                except Exception: return t, None
        results = await asyncio.gather(*(one(t) for t in tickers))
        async with self.lock: self.gate_stocks = {t: q for t, q in results if q}

    async def alpha_coins(self):
        async with self.lock:
            return sorted({x["cex_coin"] for x in self.alpha if x.get("cex_coin")})

    async def ws_loop(self, name, uri, subscribe_builder, parser):
        while True:
            try:
                coins = await self.alpha_coins()
                if not coins:
                    self.connections[name] = "waiting-alpha"
                    await asyncio.sleep(3)
                    continue
                signature = tuple(coins)
                async with websockets.connect(uri, ping_interval=20, ping_timeout=10, max_size=8 * 1024 * 1024) as ws:
                    self.connections[name] = "connected"
                    for payload in subscribe_builder(coins):
                        await ws.send(json.dumps(payload))
                    started = time.monotonic()
                    async for raw in ws:
                        if time.monotonic() - started > 60:
                            if tuple(await self.alpha_coins()) != signature:
                                break
                            started = time.monotonic()
                        parsed = parser(raw)
                        if parsed:
                            sym, q = parsed
                            if sym:
                                async with self.lock: self.external[name][sym] = q
            except Exception:
                self.connections[name] = "reconnecting"
                await asyncio.sleep(2)

    async def bybit(self):
        def sub(coins): return [{"op":"subscribe","args":[f"tickers.{c}USDT" for c in coins[i:i+10]]} for i in range(0,len(coins),10)]
        def parse(raw):
            try:
                m=json.loads(raw); d=m.get("data") or {}; d=d[0] if isinstance(d,list) and d else d
                if not str(m.get("topic","" )).startswith("tickers."): return None
                return norm(d.get("symbol")), quote(d.get("bid1Price"),d.get("ask1Price"),m.get("ts"))
            except Exception: return None
        await self.ws_loop("Bybit", BYBIT_WS, sub, parse)

    async def gate(self):
        def sub(coins): return [{"time":int(time.time()),"channel":"spot.book_ticker","event":"subscribe","payload":[f"{c}_USDT" for c in coins[i:i+100]]} for i in range(0,len(coins),100)]
        def parse(raw):
            try:
                m=json.loads(raw)
                if m.get("channel")!="spot.book_ticker" or m.get("event")!="update": return None
                d=m.get("result") or {}; return norm(d.get("s") or d.get("currency_pair")), quote(d.get("b") or d.get("highest_bid"),d.get("a") or d.get("lowest_ask"),d.get("t"))
            except Exception: return None
        await self.ws_loop("Gate", GATE_WS, sub, parse)

    async def okx(self):
        def sub(coins): return [{"op":"subscribe","args":[{"channel":"tickers","instId":f"{c}-USDT"} for c in coins[i:i+100]]} for i in range(0,len(coins),100)]
        def parse(raw):
            try:
                m=json.loads(raw)
                for d in m.get("data") or []: return norm(d.get("instId")), quote(d.get("bidPx"),d.get("askPx"),d.get("ts"))
            except Exception: pass
            return None
        await self.ws_loop("OKX", OKX_WS, sub, parse)

    async def coinbase(self):
        def sub(coins): return [{"type":"subscribe","product_ids":[f"{c}-USDT" for c in coins]+[f"{c}-USD" for c in coins],"channels":["ticker","heartbeat"]}]
        def parse(raw):
            try:
                m=json.loads(raw)
                if m.get("type")!="ticker": return None
                return norm(m.get("product_id")), quote(m.get("best_bid"),m.get("best_ask"),int(time.time()*1000))
            except Exception: return None
        await self.ws_loop("Coinbase", COINBASE_WS, sub, parse)

    async def snapshot(self):
        async with self.lock:
            alpha=list(self.alpha); aq=dict(self.alpha_quotes); ext={k:dict(v) for k,v in self.external.items()}; stocks=list(self.bstocks); bq=dict(self.bstock_quotes); bs=dict(self.binance_stocks); gs=dict(self.gate_stocks); con=dict(self.connections); diag=dict(self.alpha_diag)
        rows=[]
        for a in alpha:
            key=norm(a.get("cex_coin") or "")+"USDT"; alpha_q=aq.get(a["market_symbol"]); venues={n:ext[n].get(key) for n in ext} if a.get("cex_coin") else {n:None for n in ext}
            opp={n:opportunity(alpha_q,venues[n],"Binance Alpha",n) for n in venues}; best=max(opp.values(),key=lambda x:x.get("spread") if x.get("spread") is not None else -1e99)
            rows.append({"symbol":a["alpha_id"],"coin":a["coin"],"alpha":alpha_q,"venues":venues,"opportunities":opp,"best_opportunity":best})
        rows.sort(key=lambda x:x["best_opportunity"].get("spread") if x["best_opportunity"].get("spread") is not None else -1e99,reverse=True)
        stock_rows=[]
        for s in stocks:
            t=s["ticker"]; b=bq.get(t); underlying=t.removesuffix("B"); bn=bs.get(underlying); g=gs.get(underlying); markets={"bStock":b,"Binance Stock":bn,"Gate Stock":g}; pairs={}; names=list(markets)
            for i in range(len(names)):
                for j in range(i+1,len(names)): pairs[f"{names[i]} ↔ {names[j]}"]=opportunity(markets[names[i]],markets[names[j]],names[i],names[j])
            best=max(pairs.values(),key=lambda x:x.get("spread") if x.get("spread") is not None else -1e99,default={"spread":None})
            stock_rows.append({**s,"underlying":underlying,"markets":markets,"opportunities":pairs,"best_opportunity":best})
        stock_rows.sort(key=lambda x:x["best_opportunity"].get("spread") if x["best_opportunity"].get("spread") is not None else -1e99,reverse=True)
        return {"updated_at":int(time.time()*1000),"alpha":rows,"bstocks":stock_rows,"connections":con,"mode":"server-websocket-bidask","binance_stock_api_configured":bool(BINANCE_STOCK_API_KEY),"alpha_diagnostics":diag}

    async def start(self):
        self.tasks=[asyncio.create_task(self.refresh_alpha()),asyncio.create_task(self.refresh_alpha_depth()),asyncio.create_task(self.refresh_bstocks())]
        for _ in range(20):
            await asyncio.sleep(0.5)
            async with self.lock:
                if self.alpha: break
        self.tasks += [asyncio.create_task(self.bybit()),asyncio.create_task(self.gate()),asyncio.create_task(self.okx()),asyncio.create_task(self.coinbase())]

engine=MarketEngine()
