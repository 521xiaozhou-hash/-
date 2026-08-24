"""Runtime compatibility patch for Binance Alpha discovery.

Loaded automatically by Python before app.py.  The Binance Alpha public APIs
have changed response details over time, so this keeps discovery resilient:
Token List is the fallback source, while Exchange Info is used when available
as the authoritative set of currently tradable symbols.
"""
import asyncio
import time

import httpx


def _items(payload):
    if not isinstance(payload, dict):
        return []
    data = payload.get("data")
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for k in ("symbols", "tokens", "list", "rows", "data"):
            v = data.get(k)
            if isinstance(v, list):
                return v
    return []


async def _patched_refresh_alpha(self):
    headers = {
        "User-Agent": "binance-alpha/1.0.0 (Linux; x86_64)",
        "Accept-Encoding": "identity",
        "Accept": "application/json, text/plain, */*",
        "Referer": "https://www.binance.com/",
    }
    token_url = "https://www.binance.com/bapi/defi/v1/public/wallet-direct/buw/wallet/cex/alpha/all/token/list"
    exchange_url = "https://www.binance.com/bapi/defi/v1/public/alpha-trade/get-exchange-info"

    while True:
        try:
            async with httpx.AsyncClient(headers=headers, follow_redirects=True, timeout=12) as c:
                token_r, exchange_r = await asyncio.gather(
                    c.get(token_url), c.get(exchange_url), return_exceptions=True
                )

                token_payload = None
                exchange_payload = None
                errors = []
                if isinstance(token_r, Exception):
                    errors.append("token:" + type(token_r).__name__)
                elif token_r.status_code == 200:
                    try:
                        token_payload = token_r.json()
                    except Exception:
                        errors.append("token:invalid-json")
                else:
                    errors.append(f"token:http-{token_r.status_code}")

                if isinstance(exchange_r, Exception):
                    errors.append("exchange:" + type(exchange_r).__name__)
                elif exchange_r.status_code == 200:
                    try:
                        exchange_payload = exchange_r.json()
                    except Exception:
                        errors.append("exchange:invalid-json")
                else:
                    errors.append(f"exchange:http-{exchange_r.status_code}")

                token_items = _items(token_payload)
                exchange_items = _items(exchange_payload)

                # Exchange Info has the exact current market symbols, e.g.
                # ALPHA_105USDT.  Build a map by ALPHA_xxx base asset.
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

                blocked = self.blacklist()
                meta = {
                    str(x.get("alphaId") or "").upper(): x
                    for x in token_items
                    if isinstance(x, dict) and x.get("alphaId")
                }

                rows = []
                # Preferred: intersection with current Exchange Info.
                if active:
                    candidates = [(aid, symbol, meta.get(aid, {})) for aid, symbol in active.items()]
                else:
                    # Critical fallback: Token List is an official list of Alpha
                    # tokens and is better than showing zero when Exchange Info
                    # is temporarily unavailable/blocked from the server.
                    candidates = []
                    for aid, x in meta.items():
                        if x.get("offline") is True or x.get("offsell") is True or x.get("cexOffDisplay") is True:
                            continue
                        candidates.append((aid, aid + "USDT", x))

                for aid, market_symbol, x in candidates:
                    if not isinstance(x, dict):
                        x = {}
                    # When Exchange Info is unavailable, retain only tokens that
                    # are not explicitly offline/offsell/hidden.
                    if not active and (x.get("offline") is True or x.get("offsell") is True or x.get("cexOffDisplay") is True):
                        continue
                    coin = str(x.get("cexCoinName") or "").upper()
                    display_coin = coin or str(x.get("symbol") or aid).upper()
                    if aid in blocked or display_coin in blocked or display_coin + "USDT" in blocked:
                        continue
                    if getattr(__import__("market_engine_v2"), "ALPHA_SYMBOLS", set()):
                        allow = __import__("market_engine_v2").ALPHA_SYMBOLS
                        if aid not in allow and display_coin not in allow and display_coin + "USDT" not in allow:
                            continue
                    rows.append({
                        "alpha_id": aid,
                        "market_symbol": market_symbol,
                        "coin": display_coin,
                        "cex_coin": coin,
                        "price": None,
                        "ts": int(time.time() * 1000),
                    })

                rows.sort(key=lambda x: x["coin"])
                async with self.lock:
                    self.alpha = rows
                    self.alpha_diag = {
                        "token_list": len(token_items),
                        "exchange_info": len(exchange_items),
                        "active": len(active),
                        "displayed": len(rows),
                        "source": "exchange_info" if active else "token_list_fallback",
                        "error": ";".join(errors),
                    }
        except Exception as e:
            async with self.lock:
                self.alpha_diag = {
                    **getattr(self, "alpha_diag", {}),
                    "error": type(e).__name__ + ":" + str(e)[:160],
                }
        await asyncio.sleep(int(__import__("market_engine_v2").ALPHA_REFRESH))


try:
    import market_engine_v2
    market_engine_v2.MarketEngine.refresh_alpha = _patched_refresh_alpha
except Exception:
    pass
