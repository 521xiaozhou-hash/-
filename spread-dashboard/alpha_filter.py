import httpx

ALPHA_EXCHANGE_INFO_URL = "https://www.binance.com/bapi/defi/v1/public/alpha-trade/get-exchange-info"


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
    """Return the Alpha IDs that are actually present in Alpha exchange info.

    The token-list endpoint can contain historical/offline tokens. Exchange info is
    the authoritative market-symbol list for tokens that currently have an Alpha
    trading pair, so the dashboard uses its intersection with the token list.
    """
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
                    active.add(candidate.split("USDT")[0].split("USDC")[0].split("BNB")[0])
        return active
    except Exception:
        return set()
