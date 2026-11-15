import time

import orjson

from polyrec import rawlog
from polyrec.rawlog import HOUR_NS, RawLog, embed, hour_path, iter_raw, raw_files


def test_embed_keeps_payload_verbatim():
    line = embed(5, b'"c":2,', '[{"a": 1.50}]')
    assert line == b'{"r":5,"c":2,"m":[{"a": 1.50}]}\n'
    assert orjson.loads(embed(5, b"", '{\n "a": 1\n}')) == {"r": 5, "m": {"a": 1}}
    assert orjson.loads(embed(5, b"", "hello")) == {"r": 5, "txt": "hello"}


def test_rotation_compression_and_reading(tmp_path):
    h = 500_000  # arbitrary hour number
    lg = RawLog(tmp_path, "ws")
    lg.record(h * HOUR_NS + 1, x=1)
    lg.record(h * HOUR_NS + 2, x=2)
    lg.record((h + 1) * HOUR_NS + 3, x=3)  # rotates; the first file is compressed in the background
    lg.close()
    rawlog._pool.submit(lambda: None).result()  # wait for the compressor
    first = hour_path(tmp_path, "ws", h)
    assert not first.exists() and first.with_name(first.name + ".zst").exists()
    assert hour_path(tmp_path, "ws", h + 1).exists()
    assert len(raw_files(tmp_path, "ws", h * HOUR_NS, (h + 1) * HOUR_NS + 10)) == 2
    got = [r["x"] for r in iter_raw(tmp_path, "ws", h * HOUR_NS + 2, (h + 2) * HOUR_NS)]
    assert got == [2, 3]


def test_truncated_last_line_is_skipped(tmp_path):
    lg = RawLog(tmp_path, "ws")
    ns = time.time_ns()
    lg.record(ns, x=1)
    lg.write(b'{"r":%d,"x":' % (ns + 1), ns + 1)  # a crash mid-write
    lg.close()
    assert [r["x"] for r in iter_raw(tmp_path, "ws", ns - 1, ns + 10)] == [1]


def test_reopen_appends_to_same_hour(tmp_path):
    ns = time.time_ns()
    for i in range(2):
        lg = RawLog(tmp_path, "ws")
        lg.record(ns + i, x=i)
        lg.close()
    assert [r["x"] for r in iter_raw(tmp_path, "ws", ns, ns + 10)] == [0, 1]
