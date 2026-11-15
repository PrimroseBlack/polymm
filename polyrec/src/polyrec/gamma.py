"""Market discovery through Polymarket's Gamma API."""
from __future__ import annotations

import json
from dataclasses import dataclass

import httpx

from .config import DURATIONS
from .util import aget, iso, parse_iso


@dataclass(frozen=True)
class Market:
    condition_id: str
    event_id: str
    market_id: str
    slug: str
    coin: str
    duration: str
    start_ts: int
    end_ts: int
    token_up: str
    token_down: str

    @property
    def tokens(self) -> tuple[str, str]:
        return (self.token_up, self.token_down)


def parse_market(event: dict, coin: str, duration: str) -> Market | None:
    """Build a Market from a Gamma event; None if it is not a usable Up/Down market."""
    try:
        m = event["markets"][0]
        outcomes = [o.lower() for o in json.loads(m["outcomes"])]
        tokens = json.loads(m["clobTokenIds"])
        by = dict(zip(outcomes, tokens))
        end_ts = parse_iso(m["endDate"])
        start = m.get("eventStartTime") or event.get("startTime")
        start_ts = parse_iso(start) if start else end_ts - DURATIONS[duration]
        return Market(
            condition_id=m["conditionId"],
            event_id=str(event["id"]),
            market_id=str(m["id"]),
            slug=event["slug"],
            coin=coin,
            duration=duration,
            start_ts=start_ts,
            end_ts=end_ts,
            token_up=by["up"],
            token_down=by["down"],
        )
    except (KeyError, IndexError, ValueError, TypeError):
        return None


def official_outcome(event: dict) -> str | None:
    """'Up' / 'Down' once the market is closed and resolved, else None."""
    try:
        m = event["markets"][0]
        if not m.get("closed"):
            return None
        outcomes = json.loads(m["outcomes"])
        prices = [float(p) for p in json.loads(m["outcomePrices"])]
    except (KeyError, IndexError, ValueError, TypeError):
        return None
    winners = [o for o, p in zip(outcomes, prices) if p >= 0.99]
    return winners[0] if len(winners) == 1 else None


class Gamma:
    def __init__(self, client: httpx.AsyncClient, base_url: str):
        self.client = client
        self.base = base_url.rstrip("/")

    async def series_id(self, slug: str) -> str | None:
        r = await aget(self.client, f"{self.base}/series", {"slug": slug})
        data = r.json()
        return str(data[0]["id"]) if data else None

    async def upcoming(self, series_id: str, now: float, limit: int = 3) -> list[dict]:
        """The current window and the next few, soonest first."""
        params = {
            "series_id": series_id,
            "closed": "false",
            "end_date_min": iso(now),
            "order": "endDate",
            "ascending": "true",
            "limit": limit,
        }
        return (await aget(self.client, f"{self.base}/events", params)).json()

    async def event_by_slug(self, slug: str) -> dict | None:
        data = (await aget(self.client, f"{self.base}/events", {"slug": slug})).json()
        return data[0] if data else None
