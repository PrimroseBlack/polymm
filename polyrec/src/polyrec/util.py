from __future__ import annotations

import asyncio
import datetime as dt
import time

import httpx

UTC = dt.timezone.utc


def parse_iso(s: str) -> int:
    """ISO-8601 timestamp (with Z or offset) -> unix seconds."""
    return int(dt.datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp())


def iso(ts: float) -> str:
    return dt.datetime.fromtimestamp(ts, UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def day_of(ts: float) -> str:
    return dt.datetime.fromtimestamp(ts, UTC).strftime("%Y-%m-%d")


def day_start(day: str) -> int:
    return int(dt.datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=UTC).timestamp())


def day_range(start: str, end: str) -> list[str]:
    """Inclusive list of YYYY-MM-DD days."""
    out, t, t1 = [], day_start(start), day_start(end)
    while t <= t1:
        out.append(day_of(t))
        t += 86400
    return out


class _Retry(Exception):
    def __init__(self, wait: float | None, msg: str):
        super().__init__(msg)
        self.wait = wait


def _check(r: httpx.Response):
    if r.status_code == 429 or r.status_code >= 500:
        ra = r.headers.get("retry-after", "")
        wait = float(ra) if ra.replace(".", "", 1).isdigit() else None
        raise _Retry(wait, f"HTTP {r.status_code} from {r.request.url.host}")
    r.raise_for_status()
    return r


async def aget(client: httpx.AsyncClient, url: str, params=None, *, retries: int = 5) -> httpx.Response:
    """GET with retry on 429 / 5xx / network errors."""
    delay = 0.5
    for attempt in range(retries):
        try:
            return _check(await client.get(url, params=params))
        except (_Retry, httpx.TransportError) as e:
            if attempt == retries - 1:
                raise
            await asyncio.sleep(min(getattr(e, "wait", None) or delay, 30))
            delay *= 2
    raise AssertionError("unreachable")


def get(client: httpx.Client, url: str, params=None, *, retries: int = 6) -> httpx.Response:
    """Blocking twin of aget."""
    delay = 0.5
    for attempt in range(retries):
        try:
            return _check(client.get(url, params=params))
        except (_Retry, httpx.TransportError) as e:
            if attempt == retries - 1:
                raise
            time.sleep(min(getattr(e, "wait", None) or delay, 30))
            delay *= 2
    raise AssertionError("unreachable")
