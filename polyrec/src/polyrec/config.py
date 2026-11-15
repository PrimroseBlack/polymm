from __future__ import annotations

import logging
import os
import tomllib
from dataclasses import dataclass, field, fields
from pathlib import Path

log = logging.getLogger(__name__)
CRED_VARS = ("POLYMARKET_API_KEY", "POLYMARKET_API_SECRET", "POLYMARKET_API_PASSPHRASE")

COINS = ("btc", "eth", "sol", "xrp", "doge", "hype", "bnb")
DURATIONS = {"5m": 300, "15m": 900, "1h": 3600, "4h": 14400}

# Gamma series slugs follow "<coin>-up-or-down-<5m|15m|hourly|4h>" with one exception.
_SERIES_OVERRIDES = {("sol", "1h"): "solana-up-or-down-hourly"}


def series_slug(coin: str, duration: str) -> str:
    if (coin, duration) in _SERIES_OVERRIDES:
        return _SERIES_OVERRIDES[(coin, duration)]
    return f"{coin}-up-or-down-{'hourly' if duration == '1h' else duration}"


@dataclass
class AuxFeed:
    """An extra websocket recorded verbatim (e.g. an exchange reference-price stream)."""

    name: str
    url: str
    subscribe: list = field(default_factory=list)  # JSON messages sent after connecting
    stale_seconds: float = 60.0


# Reference prices. NOT verified from the build sandbox (it cannot open websockets);
# `polyrec smoke` checks them on the host. Remove or edit in polyrec.toml as needed.
_BINANCE_SPOT = "btcusdt ethusdt solusdt xrpusdt dogeusdt bnbusdt".split()
DEFAULT_AUX = [
    AuxFeed(
        name="binance_spot",
        url="wss://data-stream.binance.vision/stream?streams=" + "/".join(f"{s}@kline_1s" for s in _BINANCE_SPOT),
    ),
    AuxFeed(name="binance_futures", url="wss://fstream.binance.com/stream?streams=hypeusdt@markPrice@1s"),
]


@dataclass
class Config:
    data_dir: Path = Path("data")
    coins: tuple = COINS
    durations: tuple = tuple(DURATIONS)
    # Subscribe this long before a window opens and keep listening this long after it ends.
    lead_seconds: int = 120
    linger_seconds: int = 20
    discovery_interval: float = 30.0
    # REST order-book snapshots: a slow cross-check normally, fast while the websocket is down.
    rest_checkpoint_interval: float = 30.0
    rest_fallback_interval: float = 1.0
    rest_fallback_after: float = 5.0  # seconds without websocket data before falling back
    ws_stale_seconds: float = 30.0  # reconnect after this long without data
    depth_levels: int = 10  # order-book levels per side kept in the 1 Hz table
    raw_retention_days: int = 14  # raw logs older than this are pruned once derived
    ws_url: str = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    gamma_url: str = "https://gamma-api.polymarket.com"
    clob_url: str = "https://clob.polymarket.com"
    data_api_url: str = "https://data-api.polymarket.com"
    # Chainlink prices (the settlement feed). Recorded when API credentials are available.
    chainlink: bool = True
    polybolt_url: str = "wss://ws-live-v2.polymarket.com/ws"
    env_file: str = "/etc/polyrec.env"  # KEY=VALUE file holding the credentials
    aux_feeds: list = field(default_factory=lambda: list(DEFAULT_AUX))

    @property
    def series(self) -> list[tuple[str, str]]:
        return [(c, d) for c in self.coins for d in self.durations]


def load_config(path: str | Path | None = None) -> Config:
    """Load polyrec.toml (explicit path, or ./polyrec.toml if present); defaults otherwise."""
    if path is None:
        path = Path("polyrec.toml")
        if not path.exists():
            return Config()
    with open(path, "rb") as f:
        raw = tomllib.load(f)
    known = {f.name for f in fields(Config)}
    unknown = set(raw) - known
    if unknown:
        raise ValueError(f"unknown config keys in {path}: {sorted(unknown)}")
    if "data_dir" in raw:
        raw["data_dir"] = Path(raw["data_dir"])
    for k in ("coins", "durations"):
        if k in raw:
            raw[k] = tuple(raw[k])
    if "aux_feeds" in raw:
        raw["aux_feeds"] = [AuxFeed(**a) for a in raw["aux_feeds"]]
    cfg = Config(**raw)
    bad = [d for d in cfg.durations if d not in DURATIONS]
    if bad:
        raise ValueError(f"unknown durations {bad}; choose from {list(DURATIONS)}")
    return cfg


def load_env_file(path: str | Path) -> bool:
    """Read KEY=VALUE lines into the environment (existing variables win). True if the file was read."""
    try:
        text = Path(path).read_text()
    except FileNotFoundError:
        return False
    except PermissionError:
        log.warning("cannot read %s (permission denied): credentials in it are not available", path)
        return False
    for line in text.splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, _, value = line.partition("=")
            os.environ.setdefault(key.strip(), value.strip().strip("'\""))
    return True


def clob_creds():
    """CLOB API credentials from the environment, or None unless all three are set."""
    from .chainlink import ClobCreds

    values = [os.environ.get(v, "").strip() for v in CRED_VARS]
    return ClobCreds(*values) if all(values) else None
