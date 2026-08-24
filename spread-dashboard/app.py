import json
import os
import secrets
import subprocess
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import HTMLResponse

from market_engine import engine, BLACKLIST_FILE

ROOT = Path(__file__).resolve().parent
UPDATE_FLAG = ROOT / ".update-now"
UPDATE_TOKEN = os.getenv("UPDATE_TOKEN", "")
VERSION_FILE = ROOT / "VERSION"

def version():
    try: return VERSION_FILE.read_text().strip()
    except Exception: return "dev"

def read_blacklist():
    try: return json.loads(BLACKLIST_FILE.read_text())
    except Exception: return {"symbols": [], "notes": {}}

def write_blacklist(symbols):
    symbols = sorted({str(x).strip().upper() for x in symbols if str(x).strip()})
    BLACKLIST_FILE.write_text(json.dumps({"symbols": symbols, "notes": {}}, ensure_ascii=False, indent=2))
    return symbols

@asynccontextmanager
async def lifespan(app):
    await engine.start()
    yield
    for task in engine.tasks: task.cancel()

app = FastAPI(title="Exchange Spread Dashboard", lifespan=lifespan)

@app.get("/api/data")
async def api_data():
    d = await engine.snapshot(); d["version"] = version(); return d

@app.get("/api/blacklist")
async def get_blacklist():
    return read_blacklist()

@app.post("/api/blacklist")
async def set_blacklist(payload: dict):
    symbols = payload.get("symbols", [])
    if not isinstance(symbols, list): raise HTTPException(status_code=400, detail="symbols 必须是数组")
    return {"ok": True, "symbols": write_blacklist(symbols)}

@app.get("/api/program")
async def program_status():
    remote = local = None
    try:
        r = subprocess.run(["git","ls-remote","origin","refs/heads/main"],cwd=ROOT,text=True,capture_output=True,timeout=5)
        if r.returncode == 0 and r.stdout.strip(): remote=r.stdout.split()[0]
        local=subprocess.check_output(["git","rev-parse","HEAD"],cwd=ROOT,text=True,timeout=3).strip()
    except Exception: pass
    return {"version":version(),"local":local,"remote":remote,"update_available":bool(remote and local and remote!=local),"update_pending":UPDATE_FLAG.exists()}

@app.post("/api/program/update")
async def program_update(x_update_token: str = Header(default="")):
    if not UPDATE_TOKEN or not secrets.compare_digest(x_update_token, UPDATE_TOKEN): raise HTTPException(status_code=403, detail="程序更新密钥错误")
    UPDATE_FLAG.write_text("1"); return {"ok":True,"message":"更新指令已发送，服务器将自动拉取 GitHub 并重启。"}

@app.get("/", response_class=HTMLResponse)
async def index(): return HTML

HTML = r'''<!doctype html><html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>跨交易所价差监控</title><style>
:root{--bg:#080b12;--card:#111722;--line:#202938;--text:#eef2f7;--muted:#8e9aab;--green:#21d19a;--red:#ff6176;--yellow:#f0b90b}*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--text);font:14px/1.5 -apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}main{max-width:1600px;margin:auto;padding:24px 22px}.top{display:flex;justify-content:space-between;gap:20px;align-items:flex-start}.title{font-size:26px;font-weight:800}.sub{color:var(--muted);margin-top:4px}.status{color:var(--muted);font-size:12px}.controls{display:flex;gap:7px;align-items:center;margin-top:8px;justify-content:flex-end;flex-wrap:wrap}.btn,.select,.input,.textarea{border:1px solid var(--line);background:var(--card);color:var(--text);padding:9px 12px;border-radius:9px}.btn{cursor:pointer}.grid{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin:20px 0}.card{background:var(--card);border:1px solid var(--line);border-radius:14px;padding:15px}.label{color:var(--muted);font-size:12px}.value{font-size:20px;font-weight:750;margin-top:4px}.tabs{display:flex;gap:8px;margin:14px 0}.tab{border:1px solid var(--line);background:var(--card);color:var(--muted);padding:9px 14px;border-radius:10px;cursor:pointer}.tab.active{color:var(--text);border-color:#657084}.panel{background:var(--card);border:1px solid var(--line);border-radius:14px;overflow:auto}table{width:100%;border-collapse:collapse}th,td{text-align:right;padding:10px 12px;border-bottom:1px solid var(--line);white-space:nowrap}th:first-child,td:first-child{text-align:left}th{color:var(--muted);font-size:12px;background:#0d121b;position:sticky;top:0}.green{color:var(--green)}.red{color:var(--red)}.muted{color:var(--muted)}.online{color:var(--green)}.offline{color:var(--red)}.note{color:var(--muted);font-size:12px;margin-top:10px}.modal{position:fixed;inset:0;background:#000b;display:none;align-items:center;justify-content:center;z-index:5}.box{background:#111722;border:1px solid #303a4b;border-radius:14px;padding:22px;width:min(560px,92vw)}.box h3{margin:0 0 10px}.box p{color:var(--muted)}.row{display:flex;gap:8px;align-items:center;margin:10px 0}.input,.textarea{width:100%}.textarea{min-height:170px;font-family:monospace}.small{font-size:12px;color:var(--muted)}@media(max-width:900px){.grid{grid-template-columns:repeat(2,1fr)}.top{display:block}.controls{justify-content:flex-start}}
</style></head><body><main><div class="top"><div><div class="title">跨交易所价差监控</div><div class="sub">行情由你的 Linux 服务器直接连接交易所；GitHub 只负责程序版本更新</div></div><div><div class="status" id="status">服务器行情引擎启动中…</div><div class="controls"><button class="btn" onclick="load()">↻ 立即更新</button><span class="status">自动</span><select id="interval" class="select" onchange="setTimer()"><option value="1" selected>1 秒</option><option value="2">2 秒</option><option value="5">5 秒</option><option value="10">10 秒</option><option value="0">关闭</option></select><button class="btn" onclick="openBlacklist()">🚫 黑名单</button><button class="btn" onclick="openUpdate()">⚙ 程序更新</button></div></div></div><div class="grid"><div class="card"><div class="label">Alpha 监控币种</div><div class="value" id="n">—</div></div><div class="card"><div class="label">最大绝对价差</div><div class="value" id="max">—</div></div><div class="card"><div class="label">正价差机会</div><div class="value" id="pos">—</div></div><div class="card"><div class="label">程序版本</div><div class="value" id="ver">—</div></div></div><div class="tabs"><button class="tab active" onclick="show('alpha',this)">Alpha 跨所</button><button class="tab" onclick="show('stocks',this)">bStocks / 股票价差</button></div><div class="panel"><table id="table"></table></div><div class="note">价差是价格参考值；真正套利应进一步使用可成交的 best bid/ask、手续费、汇率、滑点和交易状态判断。Gate 股票为真实股票业务，与 Gate 的代币化股票是不同产品。Gate 官方股票 API 提供公开 symbol 与 orderbook 数据。</div></main><div class="modal" id="modal"><div class="box"><h3>程序更新</h3><p id="programText">正在检查 GitHub 版本…</p><div class="row"><input class="input" id="token" placeholder="输入服务器更新密钥" type="password"></div><div class="controls" style="justify-content:flex-start"><button class="btn" onclick="checkProgram()">检查新版本</button><button class="btn" onclick="doProgramUpdate()">立即更新</button><button class="btn" onclick="closeModal()">关闭</button></div></div></div><div class="modal" id="blackmodal"><div class="box"><h3>币种黑名单</h3><p>每行输入一个币种或 Alpha ID，例如 SXT、KAT、ZKC、AAPL。加入后不会显示在 Alpha 或 bStocks 相关列表。</p><textarea class="textarea" id="blacklist"></textarea><div class="controls" style="justify-content:flex-start"><button class="btn" onclick="saveBlacklist()">保存黑名单</button><button class="btn" onclick="closeBlacklist()">关闭</button></div><div class="small">黑名单保存在服务器本地，不会被 GitHub 更新覆盖。</div></div></div>
<script>let data={},mode='alpha',timer=null,loading=false;const fmt=x=>x==null?'—':Number(x).toLocaleString(undefined,{maximumFractionDigits:10});const pct=x=>x==null?'—':(x>=0?'+':'')+Number(x).toFixed(3)+'%';function show(m,e){mode=m;document.querySelectorAll('.tab').forEach(x=>x.classList.remove('active'));e.classList.add('active');render()}function render(){let t=document.getElementById('table');if(mode==='stocks'){t.innerHTML='<thead><tr><th>股票</th><th>Binance bStock</th><th>Binance现货</th><th>bStock/现货</th><th>Gate股票</th><th>bStock/Gate股票</th></tr></thead><tbody>'+data.bstocks.map(x=>`<tr><td><b>${x.ticker}</b><br><span class="muted">${x.symbol||''}</span></td><td>${fmt(x.price)}</td><td>${fmt(x.binance_spot?.last)}</td><td class="${x.bstock_vs_binance_spot>0?'green':x.bstock_vs_binance_spot<0?'red':'muted'}">${pct(x.bstock_vs_binance_spot)}</td><td>${fmt(x.gate_stock?.last)}</td><td class="${x.bstock_vs_gate_stock>0?'green':x.bstock_vs_gate_stock<0?'red':'muted'}">${pct(x.bstock_vs_gate_stock)}</td></tr>`).join('')+'</tbody>';return}let vs=['Bybit','Gate','OKX','Coinbase'];t.innerHTML='<thead><tr><th>Alpha / CEX</th><th>Alpha</th>'+vs.map(v=>`<th>${v}<br>价格 / 价差</th>`).join('')+'<th>最大绝对价差</th></tr></thead><tbody>'+data.alpha.map(x=>`<tr><td><b>${x.coin}</b><br><span class="muted">${x.symbol}</span></td><td>${fmt(x.alpha)}</td>`+vs.map(v=>{let q=x.venues[v],s=x.spreads[v],c=s>0?'green':s<0?'red':'muted';return `<td>${fmt(q?.last)}<br><span class="${c}">${pct(s)}</span></td>`}).join('')+`<td class="${x.max_spread>0?'green':'muted'}">${pct(x.max_spread)}</td></tr>`).join('')+'</tbody>'}function connectionText(c){return Object.entries(c||{}).map(([k,v])=>`${k}:${v}`).join(' · ')}async function load(){if(loading)return;loading=true;try{let r=await fetch('/api/data',{cache:'no-store'});data=await r.json();let vals=data.alpha.flatMap(x=>Object.values(x.spreads||{})).filter(Number.isFinite);document.getElementById('n').textContent=data.alpha.length;document.getElementById('max').textContent=vals.length?Math.max(...vals.map(Math.abs)).toFixed(3)+'%':'—';document.getElementById('pos').textContent=data.alpha.reduce((n,x)=>n+Object.values(x.spreads||{}).filter(v=>v>0).length,0);document.getElementById('ver').textContent=data.version||'—';document.getElementById('status').textContent='服务器行情 · '+new Date(data.updated_at).toLocaleTimeString()+' · '+connectionText(data.connections);render()}catch(e){document.getElementById('status').textContent='服务器行情接口暂时不可用'}finally{loading=false}}function setTimer(){if(timer)clearInterval(timer);let n=Number(document.getElementById('interval').value);if(n)timer=setInterval(load,n*1000)}function closeModal(){document.getElementById('modal').style.display='none'}async function openUpdate(){document.getElementById('modal').style.display='flex';await checkProgram()}async function checkProgram(){let p=document.getElementById('programText');p.textContent='检查 GitHub 版本中…';try{let r=await fetch('/api/program',{cache:'no-store'}),x=await r.json();p.textContent=x.update_available?`发现新版本。当前 ${x.version}，GitHub commit ${x.remote?.slice(0,8)}。`:`已是最新版本：${x.version}。`}catch(e){p.textContent='检查失败，请确认服务器联网。'}}async function doProgramUpdate(){let token=document.getElementById('token').value.trim();if(!token)return alert('请输入更新密钥');let r=await fetch('/api/program/update',{method:'POST',headers:{'X-Update-Token':token}});let x=await r.json();if(!r.ok)return alert(x.detail||'更新失败');document.getElementById('programText').textContent='更新指令已发送，服务器会自动重启；请等待约 10-30 秒后刷新网页。'}async function openBlacklist(){let r=await fetch('/api/blacklist',{cache:'no-store'}),x=await r.json();document.getElementById('blacklist').value=(x.symbols||[]).join('\n');document.getElementById('blackmodal').style.display='flex'}function closeBlacklist(){document.getElementById('blackmodal').style.display='none'}async function saveBlacklist(){let symbols=document.getElementById('blacklist').value.split(/[\n,\s]+/).map(x=>x.trim().toUpperCase()).filter(Boolean);let r=await fetch('/api/blacklist',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({symbols})});let x=await r.json();if(!r.ok)return alert(x.detail||'保存失败');closeBlacklist();await load()}load();setTimer()</script></body></html>'''

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=os.getenv("HOST", "0.0.0.0"), port=int(os.getenv("PORT", "8080")))
