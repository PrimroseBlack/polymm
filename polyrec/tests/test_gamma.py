import json
from pathlib import Path

from polyrec.config import COINS, DURATIONS, Config, load_config, series_slug
from polyrec.gamma import official_outcome, parse_market

FIX = Path(__file__).parent / "fixtures"


def load(name):
    return json.loads((FIX / name).read_text())


def test_parse_5m_market():
    ev = load("event_btc_5m_open.json")
    m = parse_market(ev, "btc", "5m")
    assert m.slug == ev["slug"] and m.slug.startswith("btc-updown-5m-")
    assert m.end_ts - m.start_ts == 300
    assert m.start_ts == int(m.slug.rsplit("-", 1)[1])
    tokens = json.loads(ev["markets"][0]["clobTokenIds"])
    assert (m.token_up, m.token_down) == (tokens[0], tokens[1])
    assert official_outcome(ev) is None  # still open


def test_parse_hourly_market_uses_event_start_time():
    m = parse_market(load("event_eth_1h_open.json"), "eth", "1h")
    assert m.end_ts - m.start_ts == 3600 and m.start_ts % 3600 == 0


def test_outcome_order_is_read_from_labels():
    ev = load("event_btc_5m_open.json")
    mk = ev["markets"][0]
    up, down = json.loads(mk["clobTokenIds"])
    mk["outcomes"] = '["Down", "Up"]'
    m = parse_market(ev, "btc", "5m")
    assert (m.token_up, m.token_down) == (down, up)


def test_official_outcome():
    ev = load("event_btc_5m_open.json")
    mk = ev["markets"][0]
    mk["closed"], mk["outcomePrices"] = True, '["0", "1"]'
    assert official_outcome(ev) == "Down"
    mk["outcomePrices"] = '["0.5", "0.5"]'
    assert official_outcome(ev) is None


def test_unusable_event_is_rejected():
    assert parse_market({"id": 1, "slug": "x", "markets": []}, "btc", "5m") is None


def test_series_slugs():
    assert series_slug("btc", "5m") == "btc-up-or-down-5m"
    assert series_slug("btc", "1h") == "btc-up-or-down-hourly"
    assert series_slug("sol", "1h") == "solana-up-or-down-hourly"
    assert len(Config().series) == len(COINS) * len(DURATIONS) == 28


def test_load_config(tmp_path):
    p = tmp_path / "polyrec.toml"
    p.write_text('data_dir = "/x"\ncoins = ["btc"]\nlead_seconds = 60\n[[aux_feeds]]\nname = "a"\nurl = "wss://a"\n')
    cfg = load_config(p)
    assert str(cfg.data_dir) == "/x" and cfg.coins == ("btc",) and cfg.lead_seconds == 60
    assert [a.name for a in cfg.aux_feeds] == ["a"]
    p.write_text("nonsense = 1\n")
    try:
        load_config(p)
    except ValueError as e:
        assert "nonsense" in str(e)
    else:
        raise AssertionError("unknown key accepted")
