import asyncio
import os
import secrets
import subprocess
import time
from pathlib import Path
from typing import Any

import httpx
from dotenv import load_dotenv
from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import HTMLResponse

load_dotenv()
HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", "8080"))
REQUEST_TIMEOUT = float(os.getenv("REQUEST_TIMEOUT", "6"))
ALPHA_SYMBOLS = {x.strip().upper() for x in os.getenv("ALPHA_SYMBOLS", "").split(",") if x.strip()}
BSTOCK_TICKERS = {x.strip().upper() for x in os.getenv("BSTOCK_TICKERS", "AAPL,TSLA,NVDA,MSTR").split(",") if x.strip()}
UPDATE_TOKEN = os.getenv("UPDATE_TOKEN", "")
ROOT = Path(__file__).resolve().parent
UPDATE_FLAG = ROOT / ".update-now"
VERSION_FILE = ROOT / "VERSION"

app = FastAPI(title="Exchange Spread Dashboard")

async def get_json(client: httpx.AsyncClient, url: str, params: dict | None = None) -> Any:
    try:
        r = await client.get(url, params=params, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        return r.json()
    except Exception:
        return None

def version():
    try: return VERSION_FILE.read_text().strip()
    except Exception: return "dev"

def pct(a, b):
    try:
        if a is None or b is None or float(b) == 0: return None
        return (float(a) / float(b) - 1) * 100
    except Exception: return None

def norm(s: str):
    return s.upper().replace("-", "").replace("_", "")

async def alpha_tokens(c):
    url = "https://www.binance.com/bapi/defi/v1/public/wallet-direct/buw/wallet/cex/alpha/all/token/list"
    d = await get_json(c, url)
    if not isinstance(d, dict): return []
    data = d.get("data") or []
    out = []
    for x in data:
        if not isinstance(x, dict): continue
        alpha_id = str(x.get("alphaId") or "").upper()
        coin = str(x.get("cexCoinName") or "").upper()
        price = x.get("price")
        if not alpha_id or not coin or price in (None, ""): continue
        if ALPHA_SYMBOLS and alpha_id not in ALPHA_SYMBOLS and norm(coin + "USDT") not in ALPHA_SYMBOLS and coin not in ALPHA_SYMBOLS: continue
        try: price = float(price)
        except Exception: continue
        out.append({"alpha_id": alpha_id, "coin": coin, "price": price, "listing_cex": bool(x.get("listingCex")), "alpha_price_time": int(time.time()*1000)})
    return out

async def bulk_bybit(c):
    d = await get_json(c, "https://api.bybit.com/v5/market/tickers", {"category":"spot"})
    out = {}
    try:
        for x in d["result"]["list"]:
            out[norm(x["symbol"])] = {"bid":float(x["bid1Price"]),"ask":float(x["ask1Price"]),"last":float(x["lastPrice"])}
    except Exception: pass
    return out

async def bulk_gate(c):
    d = await get_json(c, "https://api.gateio.ws/api/v4/spot/tickers")
    out = {}
    if isinstance(d, list):
        for x in d:
            try: out[norm(x["currency_pair"])] = {"bid":float(x["highest_bid"]),"ask":float(x["lowest_ask"]),"last":float(x["last"])}
            except Exception: pass
    return out

async def bulk_okx(c):
    d = await get_json(c, "https://www.okx.com/api/v5/market/tickers", {"instType":"SPOT"})
    out = {}
    try:
        for x in d["data"]:
            out[norm(x["instId"])] = {"bid":float(x["bidPx"]),"ask":float(x["askPx"]),"last":float(x["last"])}
    except Exception: pass
    return out

async def coinbase_products(c):
    d = await get_json(c, "https://api.exchange.coinbase.com/products")
    out = {}
    if isinstance(d, list):
        for x in d:
            if x.get("quote_currency") not in ("USDT", "USD", "USDC"): continue
            base = str(x.get("base_currency") or "").upper()
            if base: out[base] = x.get("id")
    return out

async def coinbase_tickers(c, products, coins):
    async def one(coin):
        pid = products.get(coin)
        if not pid: return coin, None
        d = await get_json(c, f"https://api.exchange.coinbase.com/products/{pid}/ticker")
        try: return coin, {"bid":float(d["bid"]),"ask":float(d["ask"]),"last":float(d["price"])}
        except Exception: return coin, None
    pairs = await asyncio.gather(*(one(x) for x in coins))
    return {k:v for k,v in pairs}

async def bstock_prices(c):
    # Binance RWA bStock list is type=3. Current price comes from the dynamic endpoint.
    listing = await get_json(c, "https://www.binance.com/bapi/defi/v1/public/wallet-direct/buw/wallet/market/token/rwa/stock/detail/list/ai", {"type":3})
    out = []
    if not isinstance(listing, dict): return out
    for x in listing.get("data") or []:
        ticker = str(x.get("ticker") or "").upper()
        chain = str(x.get("chainId") or "")
        addr = x.get("contractAddress")
        if not ticker or ticker not in BSTOCK_TICKERS or not addr: continue
        d = await get_json(c, "https://www.binance.com/bapi/defi/v2/public/wallet-direct/buw/wallet/market/token/rwa/dynamic/ai", {"chainId":chain,"contractAddress":addr})
        try: price=float(d["data"]["tokenInfo"]["price"])
        except Exception: price=None
        out.append({"ticker":ticker,"symbol":x.get("symbol"),"price":price,"chainId":chain})
    return out

async def collect():
    headers={"User-Agent":"spread-dashboard/2.0","Accept":"application/json"}
    async with httpx.AsyncClient(headers=headers) as c:
        alpha, bybit, gate, okx, products = await asyncio.gather(alpha_tokens(c), bulk_bybit(c), bulk_gate(c), bulk_okx(c), coinbase_products(c))
        coins = [x["coin"] for x in alpha]
        coinbase = await coinbase_tickers(c, products, coins)
        rows=[]
        for x in alpha:
            coin=x["coin"]; key=norm(coin+"USDT")
            venues={"Bybit":bybit.get(key),"Gate":gate.get(key),"OKX":okx.get(key),"Coinbase":coinbase.get(coin)}
            rows.append({"symbol":x["alpha_id"],"coin":coin,"alpha":x["price"],"venues":venues,
                         "spreads":{k:pct(x["price"],v.get("last") if v else None) for k,v in venues.items()}})
        return {"version":version(),"updated_at":int(time.time()*1000),"alpha":rows,"bstocks":await bstock_prices(c),"sources":{"alpha":"Binance Alpha token list","bstocks":"Binance RWA bStock","bybit":"Bybit Spot","gate":"Gate Spot","okx":"OKX Spot","coinbase":"Coinbase Exchange"}}

@app.get("/api/data")
async def api_data(): return await collect()

@app.get("/api/program")
async def program_status():
    remote=None
    try:
        r=subprocess.run(["git","ls-remote","origin","refs/heads/main"],cwd=ROOT,text=True,capture_output=True,timeout=5)
        if r.returncode==0 and r.stdout.strip(): remote=r.stdout.split()[0]
        local=subprocess.check_output(["git","rev-parse","HEAD"],cwd=ROOT,text=True,timeout=3).strip()
    except Exception: local=None
    return {"version":version(),"local":local,"remote":remote,"update_available":bool(remote and local and remote!=local),"update_pending":UPDATE_FLAG.exists()}

@app.post("/api/program/update")
async def program_update(x_update_token: str = Header(default="")):
    if not UPDATE_TOKEN or not secrets.compare_digest(x_update_token, UPDATE_TOKEN):
        raise HTTPException(status_code=403, detail="程序更新密钥错误")
    UPDATE_FLAG.write_text(str(int(time.time())))
    return {"ok":True,"message":"已发出更新指令，服务器会自动拉取 GitHub 并重启程序。"}

@app.get("/", response_class=HTMLResponse)
async def index(): return HTML

HTML=r'''<!doctype html><html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>跨交易所价差监控</title><style>
:root{--bg:#080b12;--card:#111722;--line:#202938;--text:#eef2f7;--muted:#8e9aab;--green:#21d19a;--red:#ff6176}*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--text);font:14px/1.5 -apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}main{max-width:1500px;margin:auto;padding:24px 22px}.top{display:flex;justify-content:space-between;gap:20px;align-items:flex-start}.title{font-size:26px;font-weight:800}.sub{color:var(--muted);margin-top:4px}.status{color:var(--muted);font-size:12px}.controls{display:flex;gap:7px;align-items:center;margin-top:8px;justify-content:flex-end}.btn,.select,.input{border:1px solid var(--line);background:var(--card);color:var(--text);padding:9px 12px;border-radius:9px}.btn{cursor:pointer}.btn:disabled{opacity:.5}.input{width:170px}.grid{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin:20px 0}.card{background:var(--card);border:1px solid var(--line);border-radius:14px;padding:15px}.label{color:var(--muted);font-size:12px}.value{font-size:20px;font-weight:750;margin-top:4px}.tabs{display:flex;gap:8px;margin:14px 0}.tab{border:1px solid var(--line);background:var(--card);color:var(--muted);padding:9px 14px;border-radius:10px;cursor:pointer}.tab.active{color:var(--text);border-color:#657084}.panel{background:var(--card);border:1px solid var(--line);border-radius:14px;overflow:auto}table{width:100%;border-collapse:collapse}th,td{text-align:right;padding:11px 13px;border-bottom:1px solid var(--line);white-space:nowrap}th:first-child,td:first-child{text-align:left}th{color:var(--muted);font-size:12px;background:#0d121b}.green{color:var(--green)}.red{color:var(--red)}.muted{color:var(--muted)}.note{color:var(--muted);font-size:12px;margin-top:10px}.modal{position:fixed;inset:0;background:#0009;display:none;align-items:center;justify-content:center}.box{background:#111722;border:1px solid #303a4b;border-radius:14px;padding:22px;width:min(440px,92vw)}.box h3{margin:0 0 10px}.box p{color:var(--muted)}@media(max-width:900px){.grid{grid-template-columns:repeat(2,1fr)}.top{display:block}.controls{justify-content:flex-start;flex-wrap:wrap}}
</style></head><body><main><div class="top"><div><div class="title">跨交易所价差监控</div><div class="sub">Binance bStocks ↔ 参考资产 · Binance Alpha ↔ Gate / Bybit / OKX / Coinbase</div></div><div><div class="status" id="status">连接中…</div><div class="controls"><button class="btn" onclick="load()">↻ 行情更新</button><span class="status">自动</span><select id="interval" class="select" onchange="setTimer()"><option value="1" selected>1 秒</option><option value="2">2 秒</option><option value="5">5 秒</option><option value="10">10 秒</option><option value="0">关闭</option></select><button class="btn" onclick="openUpdate()">⚙ 程序更新</button></div></div></div><div class="grid"><div class="card"><div class="label">Alpha 监控币种</div><div class="value" id="n">—</div></div><div class="card"><div class="label">最大绝对价差</div><div class="value" id="max">—</div></div><div class="card"><div class="label">正价差机会</div><div class="value" id="pos">—</div></div><div class="card"><div class="label">程序版本</div><div class="value" id="ver">—</div></div></div><div class="tabs"><button class="tab active" onclick="show('alpha',this)">Alpha 跨所</button><button class="tab" onclick="show('stocks',this)">bStocks</button></div><div class="panel"><table id="table"></table></div><div class="note">行情目标为 1 秒级刷新；外部交易所采用批量行情接口，Coinbase/Alpha 的数据受其公开接口更新频率影响。正负价差只是价格差，不等于扣除手续费后的可套利利润。</div></main><div class="modal" id="modal"><div class="box"><h3>程序更新</h3><p id="programText">检查中…</p><input class="input" id="token" placeholder="更新密钥" type="password"><div class="controls" style="justify-content:flex-start"><button class="btn" onclick="checkProgram()">检查 GitHub 新版本</button><button class="btn" onclick="doProgramUpdate()">立即更新</button><button class="btn" onclick="closeUpdate()">关闭</button></div></div></div>
<script>let data={},mode='alpha',timer=null,loading=false;const fmt=x=>x==null?'—':Number(x).toLocaleString(undefined,{maximumFractionDigits:10});const pct=x=>x==null?'—':(x>=0?'+':'')+Number(x).toFixed(3)+'%';function show(m,e){mode=m;document.querySelectorAll('.tab').forEach(x=>x.classList.remove('active'));e.classList.add('active');render()}function render(){let t=document.getElementById('table');if(mode==='stocks'){t.innerHTML='<thead><tr><th>股票</th><th>Binance bStock</th><th>链</th></tr></thead><tbody>'+data.bstocks.map(x=>`<tr><td>${x.ticker}</td><td>${fmt(x.price)}</td><td>${x.chainId}</td></tr>`).join('')+'</tbody>';return}let vs=['Bybit','Gate','OKX','Coinbase'];t.innerHTML='<thead><tr><th>Alpha / CEX</th><th>Alpha</th>'+vs.map(v=>`<th>${v}<br>价格 / 价差</th>`).join('')+'</tr></thead><tbody>'+data.alpha.map(x=>`<tr><td><b>${x.coin}</b><br><span class="muted">${x.symbol}</span></td><td>${fmt(x.alpha)}</td>`+vs.map(v=>{let q=x.venues[v],s=x.spreads[v],c=s>0?'green':s<0?'red':'muted';return `<td>${fmt(q?.last)}<br><span class="${c}">${pct(s)}</span></td>`}).join('')+'</tr>`).join('')+'</tbody>'}async function load(){if(loading)return;loading=true;document.getElementById('status').textContent='行情更新中…';try{let r=await fetch('/api/data?ts='+Date.now(),{cache:'no-store'});if(!r.ok)throw Error('HTTP '+r.status);data=await r.json();document.getElementById('n').textContent=data.alpha.length;let vals=data.alpha.flatMap(x=>Object.values(x.spreads)).map(Number).filter(Number.isFinite);document.getElementById('max').textContent=vals.length?Math.max(...vals.map(Math.abs)).toFixed(3)+'%':'—';document.getElementById('pos').textContent=vals.filter(x=>x>0).length;document.getElementById('ver').textContent=data.version;document.getElementById('status').textContent='最后更新 '+new Date(data.updated_at).toLocaleTimeString();render()}catch(e){document.getElementById('status').textContent='行情更新失败：'+e.message}finally{loading=false}}function setTimer(){if(timer)clearInterval(timer);let n=Number(document.getElementById('interval').value);if(n>0)timer=setInterval(load,n*1000)}async function openUpdate(){document.getElementById('modal').style.display='flex';await checkProgram()}function closeUpdate(){document.getElementById('modal').style.display='none'}async function checkProgram(){try{let r=await fetch('/api/program?ts='+Date.now(),{cache:'no-store'});let x=await r.json();document.getElementById('programText').textContent=x.update_available?'GitHub 有新版本，可以点击“立即更新”。':'当前已经是最新版本。当前版本：'+x.version}catch(e){document.getElementById('programText').textContent='检查失败：'+e.message}}async function doProgramUpdate(){let token=document.getElementById('token').value.trim();if(!token){alert('请输入更新密钥');return}let r=await fetch('/api/program/update',{method:'POST',headers:{'X-Update-Token':token}});let x=await r.json();if(!r.ok){alert(x.detail||'更新失败');return}document.getElementById('programText').textContent='已发送更新指令，服务器会自动拉取 GitHub、安装依赖并重启。网页可能短暂断开 1-3 秒。';setTimeout(checkProgram,5000)}load();setTimer();</script></body></html>'''

if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host=HOST, port=PORT)
