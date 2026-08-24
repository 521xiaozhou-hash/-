import asyncio
import os
import time
from typing import Any

import httpx
from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.responses import HTMLResponse

load_dotenv()

HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", "8080"))
REFRESH_SECONDS = int(os.getenv("REFRESH_SECONDS", "10"))
TIMEOUT = float(os.getenv("REQUEST_TIMEOUT", "8"))
ALPHA_SYMBOLS = [x.strip().upper() for x in os.getenv("ALPHA_SYMBOLS", "").split(",") if x.strip()]
BSTOCK_SYMBOLS = [x.strip().upper() for x in os.getenv("BSTOCK_SYMBOLS", "AAPLUSDT,TSLAUSDT,NVDAUSDT,MSTRUSDT").split(",") if x.strip()]

app = FastAPI(title="Exchange Spread Dashboard")

async def get_json(client: httpx.AsyncClient, url: str, params: dict | None = None) -> Any:
    try:
        r = await client.get(url, params=params, timeout=TIMEOUT)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        return {"_error": str(e)}

async def binance_spot(client, symbol: str):
    d = await get_json(client, "https://api.binance.com/api/v3/ticker/bookTicker", {"symbol": symbol})
    if "_error" in d: return None
    try: return {"bid": float(d["bidPrice"]), "ask": float(d["askPrice"]), "last": (float(d["bidPrice"])+float(d["askPrice"])) / 2}
    except Exception: return None

async def bybit_spot(client, symbol: str):
    d = await get_json(client, "https://api.bybit.com/v5/market/tickers", {"category": "spot", "symbol": symbol})
    try:
        x = d["result"]["list"][0]
        return {"bid": float(x["bid1Price"]), "ask": float(x["ask1Price"]), "last": float(x["lastPrice"])}
    except Exception: return None

async def gate_spot(client, symbol: str):
    d = await get_json(client, "https://api.gateio.ws/api/v4/spot/tickers", {"currency_pair": symbol.replace("USDT", "_USDT")})
    try:
        x = d[0]
        return {"bid": float(x["highest_bid"]), "ask": float(x["lowest_ask"]), "last": float(x["last"])}
    except Exception: return None

async def okx_spot(client, symbol: str):
    d = await get_json(client, "https://www.okx.com/api/v5/market/ticker", {"instId": symbol.replace("USDT", "-USDT")})
    try:
        x = d["data"][0]
        return {"bid": float(x["bidPx"]), "ask": float(x["askPx"]), "last": float(x["last"])}
    except Exception: return None

async def coinbase_spot(client, symbol: str):
    product = symbol.replace("USDT", "-USDT")
    d = await get_json(client, f"https://api.exchange.coinbase.com/products/{product}/ticker")
    try:
        return {"bid": float(d["bid"]), "ask": float(d["ask"]), "last": float(d["price"])}
    except Exception: return None

async def alpha_prices(client):
    candidates = [
        "https://www.binance.com/bapi/defi/v1/public/alpha-trade/ticker",
        "https://www.binance.com/bapi/defi/v1/public/alpha-trade/get-price",
    ]
    for url in candidates:
        d = await get_json(client, url)
        if isinstance(d, dict) and not d.get("_error"):
            data = d.get("data")
            if isinstance(data, list):
                out = {}
                for x in data:
                    sym = str(x.get("symbol") or x.get("tokenSymbol") or "").upper()
                    p = x.get("price") or x.get("lastPrice")
                    if sym and p:
                        try: out[sym] = float(p)
                        except Exception: pass
                if out: return out
            if isinstance(data, dict):
                sym = str(data.get("symbol") or "").upper(); p = data.get("price") or data.get("lastPrice")
                if sym and p:
                    try: return {sym: float(p)}
                    except Exception: pass
    return {}

def spread_pct(a: float | None, b: float | None):
    if a is None or b is None or b == 0: return None
    return (a / b - 1.0) * 100.0

def normalize_symbol(s: str):
    s = s.upper().replace("-", "").replace("_", "")
    return s if s.endswith("USDT") else s + "USDT"

async def collect():
    async with httpx.AsyncClient(headers={"User-Agent": "spread-dashboard/1.0"}) as c:
        bstock = await asyncio.gather(*(binance_spot(c, s) for s in BSTOCK_SYMBOLS))
        alpha = await alpha_prices(c)
        if ALPHA_SYMBOLS:
            alpha = {k: v for k, v in alpha.items() if k in ALPHA_SYMBOLS or normalize_symbol(k) in ALPHA_SYMBOLS}
        symbols = sorted(alpha.keys())
        rows = []
        for sym in symbols:
            usdt = normalize_symbol(sym)
            prices = await asyncio.gather(bybit_spot(c, usdt), gate_spot(c, usdt), okx_spot(c, usdt), coinbase_spot(c, usdt))
            venues = dict(zip(["Bybit", "Gate", "OKX", "Coinbase"], prices))
            for venue, q in venues.items():
                rows.append({"symbol": sym, "venue": venue, "alpha": alpha[sym], "price": q["last"] if q else None,
                             "spread_pct": spread_pct(alpha[sym], q["last"] if q else None)})
        return {"updated_at": int(time.time()), "refresh_seconds": REFRESH_SECONDS, "bstocks": [
            {"symbol": s, "price": q["last"] if q else None} for s, q in zip(BSTOCK_SYMBOLS, bstock)
        ], "alpha": rows}

@app.get("/api/data")
async def api_data():
    # 页面点击“立即更新”时直接调用本机这个接口；不经过 GitHub、Webhook 或其它中间服务。
    return await collect()

@app.get("/", response_class=HTMLResponse)
async def index():
    return HTML

HTML = r'''<!doctype html>
<html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Crypto Spread Monitor</title><style>
:root{--bg:#080b12;--card:#111722;--line:#202938;--text:#eef2f7;--muted:#8e9aab;--green:#21d19a;--red:#ff6176;--accent:#f0b90b}*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--text);font:14px/1.5 -apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}main{max-width:1450px;margin:auto;padding:28px 22px}.top{display:flex;justify-content:space-between;align-items:end;margin-bottom:22px}.title{font-size:26px;font-weight:800}.sub{color:var(--muted);margin-top:4px}.status{font-size:12px;color:var(--muted)}.controls{display:flex;gap:8px;align-items:center;justify-content:flex-end;margin-top:10px}.btn,.select{border:1px solid var(--line);background:var(--card);color:var(--text);padding:9px 13px;border-radius:9px;cursor:pointer}.btn:hover{border-color:#667085}.btn:disabled{opacity:.55;cursor:wait}.grid{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:22px}.card{background:var(--card);border:1px solid var(--line);border-radius:14px;padding:15px}.label{color:var(--muted);font-size:12px}.value{font-size:20px;font-weight:750;margin-top:5px}.tabs{display:flex;gap:8px;margin:16px 0}.tab{border:1px solid var(--line);background:var(--card);color:var(--muted);padding:9px 14px;border-radius:10px;cursor:pointer}.tab.active{color:var(--text);border-color:#4a5568}.panel{background:var(--card);border:1px solid var(--line);border-radius:14px;overflow:auto}table{width:100%;border-collapse:collapse}th,td{text-align:right;padding:12px 14px;border-bottom:1px solid var(--line);white-space:nowrap}th:first-child,td:first-child{text-align:left}th{font-size:12px;color:var(--muted);font-weight:600;background:#0d121b;position:sticky;top:0}.green{color:var(--green)}.red{color:var(--red)}.neutral{color:var(--muted)}.note{color:var(--muted);font-size:12px;margin-top:10px}@media(max-width:900px){.grid{grid-template-columns:repeat(2,1fr)}.top{display:block}.controls{justify-content:flex-start}}
</style></head><body><main><div class="top"><div><div class="title">跨交易所价差监控</div><div class="sub">Binance bStocks ↔ 参考资产 · Binance Alpha ↔ Gate / Bybit / OKX / Coinbase</div></div><div><div class="status" id="status">等待更新…</div><div class="controls"><button class="btn" id="updateBtn" onclick="manualUpdate()">↻ 立即更新</button><label class="status">自动更新</label><select class="select" id="interval" onchange="changeInterval()"><option value="0">关闭</option><option value="5">5 秒</option><option value="10" selected>10 秒</option><option value="30">30 秒</option><option value="60">60 秒</option></select></div></div></div>
<div class="grid"><div class="card"><div class="label">Alpha 监控币种</div><div class="value" id="n">—</div></div><div class="card"><div class="label">最大绝对价差</div><div class="value" id="max">—</div></div><div class="card"><div class="label">正价差机会</div><div class="value" id="pos">—</div></div><div class="card"><div class="label">刷新周期</div><div class="value" id="refresh">—</div></div></div>
<div class="tabs"><button class="tab active" onclick="show('alpha',this)">Alpha 跨所</button><button class="tab" onclick="show('stocks',this)">bStocks</button></div><div class="panel"><table id="table"></table></div><div class="note">“立即更新”直接向当前服务器的 /api/data 请求最新行情，不需要连接 GitHub、Webhook 或其它更新服务。价差公式：Alpha 价格相对外部交易所价格的百分比。实际交易前请考虑手续费、滑点、资金费、充值提现及币种/交易对差异。</div></main>
<script>let data={};let mode='alpha';let timer=null;let loading=false;function fmt(x){return x==null?'—':Number(x).toLocaleString(undefined,{maximumFractionDigits:8})}function pct(x){if(x==null)return '—';return (x>=0?'+':'')+x.toFixed(3)+'%'}function show(m,el){mode=m;document.querySelectorAll('.tab').forEach(x=>x.classList.remove('active'));el.classList.add('active');render()}function render(){let t=document.getElementById('table');if(mode==='stocks'){t.innerHTML='<thead><tr><th>资产</th><th>Binance bStocks</th></tr></thead><tbody>'+data.bstocks.map(x=>`<tr><td>${x.symbol}</td><td>${fmt(x.price)}</td></tr>`).join('')+'</tbody>';return}let venues=['Bybit','Gate','OKX','Coinbase'];t.innerHTML='<thead><tr><th>Alpha</th><th>Alpha Price</th>'+venues.map(v=>`<th>${v}</th>`).join('')+'</tr></thead><tbody>'+[...new Set(data.alpha.map(x=>x.symbol))].map(s=>{let r=data.alpha.filter(x=>x.symbol===s);let a=r[0]?.alpha;return `<tr><td>${s}</td><td>${fmt(a)}</td>`+venues.map(v=>{let x=r.find(z=>z.venue===v);let c=x?.spread_pct>0?'green':x?.spread_pct<0?'red':'neutral';return `<td><span>${fmt(x?.price)}</span><br><span class="${c}">${pct(x?.spread_pct)}</span></td>`}).join('')+'</tr>'}).join('')+'</tbody>'}
async function load(){if(loading)return;loading=true;const btn=document.getElementById('updateBtn');btn.disabled=true;document.getElementById('status').textContent='正在更新行情…';try{let r=await fetch('/api/data?ts='+Date.now(),{cache:'no-store'});if(!r.ok)throw new Error('HTTP '+r.status);data=await r.json();document.getElementById('n').textContent=new Set(data.alpha.map(x=>x.symbol)).size;let vals=data.alpha.map(x=>Math.abs(x.spread_pct)).filter(Number.isFinite);document.getElementById('max').textContent=vals.length?Math.max(...vals).toFixed(3)+'%':'—';document.getElementById('pos').textContent=data.alpha.filter(x=>x.spread_pct>0).length;document.getElementById('refresh').textContent=data.refresh_seconds+'s';document.getElementById('status').textContent='最后更新 '+new Date(data.updated_at*1000).toLocaleTimeString();render()}catch(e){document.getElementById('status').textContent='更新失败：'+e.message}finally{loading=false;btn.disabled=false}}
function changeInterval(){if(timer){clearInterval(timer);timer=null}let sec=Number(document.getElementById('interval').value);if(sec>0)timer=setInterval(load,sec*1000)}async function manualUpdate(){await load()}load();changeInterval();</script></body></html>'''

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=HOST, port=PORT)
