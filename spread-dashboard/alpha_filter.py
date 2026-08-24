import time
import httpx

ALPHA_EXCHANGE_INFO_URL = "https://www.binance.com/bapi/defi/v1/public/alpha-trade/get-exchange-info"
_CACHE = set()
_CACHE_AT = 0.0
CACHE_SECONDS = 30


def _extract_symbols(payload):
    if not isinstance(payload, dict):
        return []
    data = payload.get("data")
    if isinstance(data, dict):
        symbols = data.get("symbols")
        if isinstance(symbols, list):
            return symbols
    if isinstance(data, list):
        return data
    return []


def fetch_active_alpha_ids(timeout=8.0):
    """Return Alpha IDs that currently exist in Alpha exchange-info.

    Binance's token-list endpoint contains token metadata, including historical
    and offline entries. The Alpha exchange-info endpoint describes actual
    Alpha trading symbols, so it is used as the authoritative active-market
    filter. The result is cached briefly to avoid a request on every dashboard
    refresh.
    """
    global _CACHE, _CACHE_AT
    now = time.time()
    if _CACHE and now - _CACHE_AT < CACHE_SECONDS:
        return set(_CACHE)
    try:
        r = httpx.get(
            ALPHA_EXCHANGE_INFO_URL,
            headers={"User-Agent": "spread-dashboard/5.1", "Accept": "application/json"},
            timeout=timeout,
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
            for candidate in (symbol, base):
                if candidate.startswith("ALPHA_"):
                    for suffix in ("USDT", "USDC", "BNB"):
                        if candidate.endswith(suffix):
                            candidate = candidate[:-len(suffix)]
                            break
                    active.add(candidate)
        if active:
            _CACHE = active
            _CACHE_AT = now
        return set(active)
    except Exception:
        return set(_CACHE)
