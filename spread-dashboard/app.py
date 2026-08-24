import json, os, secrets, subprocess
from contextlib import asynccontextmanager
from pathlib import Path
from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import HTMLResponse
from market_engine_v2 import engine, BLACKLIST_FILE
from alpha_ws_patch import install_alpha_ws
from cex_bulk_patch import install_cex_bulk

ROOT = Path(__file__).resolve().parent
UPDATE_FLAG = ROOT / ".update-now"
UPDATE_TOKEN = os.getenv("UPDATE_TOKEN", "")
VERSION_FILE = ROOT / "VERSION"
install_alpha_ws(engine)
install_cex_bulk(engine)

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
    d = await engine.snapshot()
    d["alpha_count"] = len(d.get("alpha", []))
    d["alpha_diagnostics"] = dict(getattr(engine, "alpha_diag", {}))
    return d | {"version": version()}

@app.get("/api/blacklist")
async def get_blacklist(): return read_blacklist()

@app.post("/api/blacklist")
async def set_blacklist(payload: dict):
    symbols = payload.get("symbols", [])
    if not isinstance(symbols, list): raise HTTPException(400, "symbols 必须是数组")
    return {"ok": True, "symbols": write_blacklist(symbols)}

@app.get("/api/program")
async def program_status():
    remote = local = None
    try:
        r = subprocess.run(["git", "ls-remote", "origin", "refs/heads/main"], cwd=ROOT, text=True, capture_output=True, timeout=5)
        if r.returncode == 0 and r.stdout.strip(): remote = r.stdout.split()[0]
        local = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True, timeout=3).strip()
    except Exception: pass
    return {"version": version(), "local": local, "remote": remote, "update_available": bool(remote and local and remote != local), "update_pending": UPDATE_FLAG.exists()}

@app.post("/api/program/update")
async def program_update(x_update_token: str = Header(default="")):
    if not UPDATE_TOKEN or not secrets.compare_digest(x_update_token, UPDATE_TOKEN): raise HTTPException(403, "程序更新密钥错误")
    UPDATE_FLAG.write_text("1"); return {"ok": True, "message": "更新指令已发送，服务器将自动拉取 GitHub 并重启。"}

HTML = r'''<!doctype html><html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>实时跨市场价差监控</title><style>
:root{--bg:#080b12;--card:#111722;--line:#202938;--text:#eef2f7;--muted:#8e9aab;--green:#21d19a;--red:#ff6176}*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--text);font:14px/1.5 -apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}main{max-width:1750px;margin:auto;padding:24px}.top{display:flex;justify-content:space-between;gap:20px}.title{font-size:26px;font-weight:800}.sub,.status,.muted,.note{color:var(--muted)}.status,.note{font-size:12px}.controls{display:flex;gap:7px;align-items:center;margin-top:8px;justify-content:flex-end;flex-wrap:wrap}.btn,.select,.input,.textarea{border:1px solid var(--line);background:var(--card);color:var(--text);padding:9px 12px;border-radius:9px}.btn{cursor:pointer}.grid{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin:20px 0}.card{background:var(--card);border:1px solid var(--line);border-radius:14px;padding:15px}.label{color:var(--muted);font-size:12px}.value{font-size:20px;font-weight:750;margin-top:4px}.tabs{display:flex;gap:8px;margin:14px 0}.tab{border:1px solid var(--line);background:var(--card);color:var(--muted);padding:9px 14px;border-radius:10px;cursor:pointer}.tab.active{color:var(--text);border-color:#657084}.panel{background:var(--card);border:1px solid var(--line);border-radius:14px;overflow:auto}table{width:100%;border-collapse:collapse}th,td{text-align:right;padding:10px 12px;border-bottom:1px solid var(--line);white-space:nowrap}th:first-child,td:first-child{text-align:left}th{color:var(--muted);font-size:12px;background:#0d121b;position:sticky;top:0}.green{color:var(--green);font-weight:700}.red{color:var(--red)}.modal{position:fixed;inset:0;background:#000b;display:none;align-items:center;justify-content:center;z-index:5}.box{background:#111722;border:1px solid #303a4b;border-radius:14px;padding:22px;width:min(620px,92vw)}.input,.textarea{width:100%}.textarea{min-height:170px;font-family:monospace}.row{display:flex;gap:8px;margin:10px 0}@media(max-width:900px){.grid{grid-template-columns:repeat(2,1fr)}.top{display:block}}
</style></head><body><main><div class="top"><div><div class="title">实时跨市场价差监控</div><div class="sub">Alpha ↔ Bybit / Gate / OKX；严格按 Ask 买入 → Bid 卖出计算；行情由 Linux 服务器直接连接交易所</div></div><div><div class="status" id="status">行情引擎启动中…</div><div class="controls"><button class="btn" onclick="load()">↻ 立即更新</button><select id="interval" class="select" onchange="setTimer()"><option value="1" selected>1 秒</option><option value="2">2 秒</option><option value="5">5 秒</option><option value="10">10 秒</option><option value="30">30 秒</option></select><button class="btn" onclick="openBlacklist()">🚫 黑名单</button><button class="btn" onclick="openUpdate()">⚙ 程序更新</button></div></div></div><div class="grid"><div class="card"><div class="label">Alpha 币种</div><div class="value" id="n">—</div></div><div class="card"><div class="label">最大可执行价差</div><div class="value" id="max">—</div></div><div class="card"><div class="label">正向机会</div><div class="value" id="pos">—</div></div><div class="card"><div class="label">程序版本</div><div class="value" id="ver">—</div></div></div><div class="tabs"><button class="tab active" onclick="show('alpha',this)">Alpha ↔ CEX</button><button class="tab" onclick="show('stocks',this)">bStock ↔ Binance股票 ↔ Gate股票</button></div><div class="panel"><table id="table"></table></div><div class="note" id="note">Alpha 正在建立实时盘口…</div></main><div class="modal" id="modal"><div class="box"><h3>程序更新</h3><p id="programText">检查中…</p><div class="row"><input class="input" id="token" placeholder="输入服务器更新密钥" type="password"></div><div class="controls"><button class="btn" onclick="checkProgram()">检查新版本</button><button class="btn" onclick="doProgramUpdate()">立即更新</button><button class="btn" onclick="closeModal()">关闭</button></div></div></div><div class="modal" id="blackmodal"><div class="box"><h3>币种黑名单</h3><p>每行一个，例如 SXT、KAT、ZKC。保存后服务器立即过滤。</p><textarea class="textarea" id="blacklist"></textarea><div class="controls"><button class="btn" onclick="saveBlacklist()">保存</button><button class="btn" onclick="closeBlacklist()">关闭</button></div></div></div><script>
let data={},mode='alpha',timer=null,busy=false;const fmt=x=>x==null?'—':Number(x).toLocaleString(undefined,{maximumFractionDigits:10});const pct=x=>x==null?'—':(x>=0?'+':'')+Number(x).toFixed(3)+'%';const cls=x=>x==null?'':x>0?'green':'red';
function show(m,e){mode=m;document.querySelectorAll('.tab').forEach(x=>x.classList.remove('active'));e.classList.add('active');render()}
function spread(buy,sell){if(!Number.isFinite(Number(buy))||!Number.isFinite(Number(sell))||Number(buy)<=0||Number(sell)<=0)return null;return(Number(sell)/Number(buy)-1)*100}
function directional(x){let ab=x.alpha,first=[],second=[];for(let[name,q]of Object.entries(x.venues||{})){let s1=spread(ab?.ask,q?.bid),s2=spread(q?.ask,ab?.bid);if(s1!=null)first.push({s:s1,n:name});if(s2!=null)second.push({s:s2,n:name})}first.sort((a,b)=>b.s-a.s);second.sort((a,b)=>b.s-a.s);return{a:first[0]||null,b:second[0]||null}}
function op2(x){let d=directional(x);let l=d.a?`<span class="${cls(d.a.s)}">${pct(d.a.s)}</span><br><span class="muted" style="font-size:11px">Alpha买 → ${d.a.n}卖</span>`:'—';let r=d.b?`<span class="${cls(d.b.s)}">${pct(d.b.s)}</span><br><span class="muted" style="font-size:11px">${d.b.n}买 → Alpha卖</span>`:'—';return`${l}<span class="muted"> / </span>${r}`}
function stockOp(x){let m=x.markets||{},names=Object.keys(m),left=[],right=[];for(let i=0;i<names.length;i++)for(let j=0;j<names.length;j++)if(i!==j){let s=spread(m[names[i]]?.ask,m[names[j]]?.bid);if(s!=null){if(names[i]==='bStock')left.push({s,n:names[j]});if(names[j]==='bStock')right.push({s,n:names[i]})}}left.sort((a,b)=>b.s-a.s);right.sort((a,b)=>b.s-a.s);let l=left[0]?`<span class="${cls(left[0].s)}">${pct(left[0].s)}</span><br><span class="muted" style="font-size:11px">bStock买 → ${left[0].n}卖</span>`:'—';let r=right[0]?`<span class="${cls(right[0].s)}">${pct(right[0].s)}</span><br><span class="muted" style="font-size:11px">${right[0].n}买 → bStock卖</span>`:'—';return`${l}<span class="muted"> / </span>${r}`}
function render(){let t=document.getElementById('table');if(mode==='stocks'){t.innerHTML='<thead><tr><th>股票</th><th>bStock<br>Bid / Ask</th><th>Binance 股票<br>Bid / Ask</th><th>Gate 股票<br>Bid / Ask</th><th>最佳可执行价差<br>bStock买→卖出 / 买入→bStock卖</th></tr></thead><tbody>'+(data.bstocks||[]).map(x=>{let b=x.markets?.bStock,bn=x.markets?.['Binance Stock'],g=x.markets?.['Gate Stock'];return`<tr><td><b>${x.underlying}</b><br><span class="muted">${x.ticker}</span></td><td>${fmt(b?.bid)} / ${fmt(b?.ask)}</td><td>${bn?fmt(bn.bid)+' / '+fmt(bn.ask):'未配置'}</td><td>${fmt(g?.bid)} / ${fmt(g?.ask)}</td><td>${stockOp(x)}</td></tr>`}).join('')+'</tbody>';return}t.innerHTML='<thead><tr><th>Alpha</th><th>Alpha<br>Bid / Ask</th><th>Bybit<br>Bid / Ask</th><th>Gate<br>Bid / Ask</th><th>OKX<br>Bid / Ask</th><th>最佳可执行价差<br>Alpha买→其他卖 / 其他买→Alpha卖</th></tr></thead><tbody>'+(data.alpha||[]).map(x=>`<tr><td><b>${x.coin}</b><br><span class="muted">${x.symbol}</span></td><td>${fmt(x.alpha?.bid)} / ${fmt(x.alpha?.ask)}</td>`+['Bybit','Gate','OKX'].map(v=>{let q=x.venues?.[v];return`<td>${fmt(q?.bid)} / ${fmt(q?.ask)}</td>`}).join('')+`<td>${op2(x)}</td></tr>`).join('')+'</tbody>'}
function conn(c){return Object.entries(c||{}).filter(([k])=>k!=='Coinbase').map(([k,v])=>`${k}:${v}`).join(' · ')}
async function load(){if(busy)return;busy=true;try{let r=await fetch('/api/data',{cache:'no-store'});data=await r.json();let vals=[];(data.alpha||[]).forEach(x=>{let d=directional(x);if(d.a)vals.push(d.a.s);if(d.b)vals.push(d.b.s)});(data.bstocks||[]).forEach(x=>{let m=x.markets||{},names=Object.keys(m);for(let i=0;i<names.length;i++)for(let j=0;j<names.length;j++)if(i!==j){let s=spread(m[names[i]]?.ask,m[names[j]]?.bid);if(s!=null)vals.push(s)}});document.getElementById('n').textContent=data.alpha_count??(data.alpha||[]).length;document.getElementById('max').textContent=vals.length?Math.max(...vals).toFixed(3)+'%':'—';document.getElementById('pos').textContent=vals.filter(x=>x>0).length;document.getElementById('ver').textContent=data.version||'—';let ad=data.alpha_diagnostics||{};document.getElementById('status').textContent='服务器行情 · '+new Date(data.updated_at).toLocaleTimeString()+' · '+conn(data.connections);document.getElementById('note').textContent=`Alpha：Token ${ad.token_list||0} · ExchangeInfo ${ad.exchange_info||0} · 当前市场 ${ad.active||0} · 有Bid/Ask ${ad.book_quotes||0} · 页面 ${data.alpha_count||0} · ${ad.error||'正常'}`;render()}catch(e){document.getElementById('status').textContent='服务器行情接口不可用'}finally{busy=false}}
function setTimer(){if(timer)clearInterval(timer);timer=setInterval(load,Number(document.getElementById('interval').value)*1000)}function closeModal(){document.getElementById('modal').style.display='none'}async function openUpdate(){document.getElementById('modal').style.display='flex';await checkProgram()}async function checkProgram(){let p=document.getElementById('programText');try{let x=await(await fetch('/api/program',{cache:'no-store'})).json();p.textContent=x.update_available?`发现新版本：当前 ${x.version}，GitHub ${x.remote?.slice(0,8)}`:`当前已是最新版本：${x.version}`}catch{p.textContent='检查失败'}}async function doProgramUpdate(){let token=document.getElementById('token').value.trim();if(!token)return alert('请输入更新密钥');let r=await fetch('/api/program/update',{method:'POST',headers:{'X-Update-Token':token}}),x=await r.json();if(!r.ok)return alert(x.detail||'更新失败');document.getElementById('programText').textContent='更新指令已发送，请等待 10-30 秒后刷新。'}async function openBlacklist(){let x=await(await fetch('/api/blacklist')).json();document.getElementById('blacklist').value=(x.symbols||[]).join('\n');document.getElementById('blackmodal').style.display='flex'}function closeBlacklist(){document.getElementById('blackmodal').style.display='none'}async function saveBlacklist(){let symbols=document.getElementById('blacklist').value.split(/[\n,\s]+/).map(x=>x.trim().toUpperCase()).filter(Boolean);await fetch('/api/blacklist',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({symbols})});closeBlacklist();load()}load();setTimer();</script></body></html>'''

@app.get("/", response_class=HTMLResponse)
async def index(): return HTML

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=os.getenv("HOST", "0.0.0.0"), port=int(os.getenv("PORT", "8080")))
