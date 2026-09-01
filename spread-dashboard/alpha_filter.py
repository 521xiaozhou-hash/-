import time
import httpx

ALPHA_EXCHANGE_INFO_URL = "https://www.binance.com/bapi/defi/v1/public/alpha-trade/get-exchange-info"
_CACHE = set()
_CACHE_AT = 0.0
CACHE_SECONDS = 30


def _extract_symbols(payload):
    if isinstance(payload, list):
        return payload
    if not isinstance(payload, dict):
        return []
    data = payload.get("data")
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for key in ("symbols", "list", "rows", "items", "data"):
            value = data.get(key)
            if isinstance(value, list):
                return value
    return []


def _base_from_symbol(symbol):
    symbol = str(symbol or "").upper()
    if symbol.startswith("ALPHA_"):
        for suffix in ("USDT", "USDC", "BNB"):
            if symbol.endswith(suffix):
                return symbol[:-len(suffix)]
        return symbol
    return ""


def fetch_active_alpha_ids(timeout=8.0):
    """Return active Alpha IDs/market bases, keeping the last good cache on errors."""
    global _CACHE, _CACHE_AT
    now = time.time()
    if _CACHE and now - _CACHE_AT < CACHE_SECONDS:
        return set(_CACHE)
    try:
        r = httpx.get(
            ALPHA_EXCHANGE_INFO_URL,
            headers={
                "User-Agent": "Mozilla/5.0",
                "Accept": "application/json, text/plain, */*",
                "Referer": "https://www.binance.com/",
            },
            timeout=timeout,
            follow_redirects=True,
        )
        r.raise_for_status()
        symbols = _extract_symbols(r.json())
        active = set()
        for item in symbols:
            if not isinstance(item, dict):
                continue
            symbol = str(item.get("symbol") or "").upper()
            base = str(item.get("baseAsset") or "").upper()
            status = str(item.get("status") or "").upper()
            if status and status not in {"TRADING", "ONLINE", "1"}:
                continue
            candidate = base if base.startswith("ALPHA_") else symbol
            candidate = _base_from_symbol(candidate) if candidate.startswith("ALPHA_") else candidate
            if candidate:
                active.add(candidate)
        if active:
            _CACHE = active
            _CACHE_AT = now
        return set(_CACHE) if not active else set(active)
    except Exception:
        return set(_CACHE)
