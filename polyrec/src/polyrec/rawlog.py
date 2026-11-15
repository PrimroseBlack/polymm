"""Append-only raw logs: one JSON object per line, one file per stream per UTC hour.

The file for the current hour is plain .jsonl so a crash loses at most the last
unflushed second; finished hours are compressed to .jsonl.zst in the background.
"""
from __future__ import annotations

import io
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Iterator

import orjson
import zstandard

log = logging.getLogger(__name__)
HOUR_NS = 3_600_000_000_000
_pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="compress")


def hour_path(root: Path, stream: str, hour: int) -> Path:
    t = time.gmtime(hour * 3600)
    return (
        root / "raw" / stream / time.strftime("%Y-%m-%d", t) / f"{stream}-{time.strftime('%Y%m%dT%H', t)}.jsonl"
    )


def compress_file(path: Path) -> None:
    dst = path.with_name(path.name + ".zst")
    tmp = dst.with_name(dst.name + ".tmp")
    try:
        with open(path, "rb") as src, open(tmp, "wb") as out:
            zstandard.ZstdCompressor(level=10).copy_stream(src, out)
        tmp.replace(dst)
        path.unlink()
    except Exception:
        log.exception("compressing %s failed", path)
        tmp.unlink(missing_ok=True)


def compress_leftovers(root: Path) -> None:
    """Compress finished-hour files left uncompressed by a crash or restart."""
    current = time.time_ns() // HOUR_NS
    for p in sorted((root / "raw").glob("*/*/*.jsonl")):
        stream = p.parent.parent.name
        if p != hour_path(root, stream, current):
            _pool.submit(compress_file, p)


def embed(ns: int, extra: bytes, raw: str | bytes) -> bytes:
    """Wrap a raw JSON payload as {"r":ns,<extra>"m":<payload>} without re-parsing it."""
    if isinstance(raw, str):
        raw = raw.encode()
    raw = raw.strip()
    if raw[:1] in (b"{", b"[") and b"\n" not in raw:
        return b'{"r":%d,%s"m":%s}\n' % (ns, extra, raw)
    try:  # multi-line JSON: normalise to one line
        return b'{"r":%d,%s"m":%s}\n' % (ns, extra, orjson.dumps(orjson.loads(raw)))
    except orjson.JSONDecodeError:  # not JSON: keep as text
        return b'{"r":%d,%s"txt":%s}\n' % (ns, extra, orjson.dumps(raw.decode(errors="replace")))


class RawLog:
    def __init__(self, root: Path, stream: str):
        self.root, self.stream = Path(root), stream
        self._hour = -1
        self._f: io.BufferedWriter | None = None
        self._path: Path | None = None
        self.lines = 0

    def write(self, line: bytes, ns: int) -> None:
        hour = ns // HOUR_NS
        if hour != self._hour:
            if hour < self._hour:  # clock stepped back across an hour: stay in the open file
                hour = self._hour
            else:
                self._rotate(hour)
        self._f.write(line)
        self.lines += 1

    def record(self, ns: int, **fields) -> None:
        self.write(orjson.dumps({"r": ns, **fields}) + b"\n", ns)

    def _rotate(self, hour: int) -> None:
        old = self._path
        if self._f:
            self._f.close()
        self._path = hour_path(self.root, self.stream, hour)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._f = open(self._path, "ab", buffering=1 << 16)
        self._hour = hour
        if old and old != self._path:
            _pool.submit(compress_file, old)

    def flush(self) -> None:
        if self._f:
            self._f.flush()

    def close(self) -> None:
        if self._f:
            self._f.close()
            self._f = None


def raw_files(root: Path, stream: str, start_ns: int, end_ns: int) -> list[Path]:
    out = []
    for hour in range(start_ns // HOUR_NS, end_ns // HOUR_NS + 1):
        p = hour_path(Path(root), stream, hour)
        z = p.with_name(p.name + ".zst")
        if z.exists():
            out.append(z)
        elif p.exists():
            out.append(p)
    return out


def iter_file(path: Path) -> Iterator[dict]:
    with open(path, "rb") as f:
        src = zstandard.ZstdDecompressor().stream_reader(f) if path.suffix == ".zst" else f
        for line in io.BufferedReader(src, 1 << 20):
            try:
                yield orjson.loads(line)
            except orjson.JSONDecodeError:  # truncated last line after a crash
                continue


def iter_raw(root: Path, stream: str, start_ns: int, end_ns: int) -> Iterator[dict]:
    """Records of a stream with start_ns <= r < end_ns, in file order."""
    for path in raw_files(root, stream, start_ns, end_ns):
        for rec in iter_file(path):
            if start_ns <= rec.get("r", 0) < end_ns:
                yield rec
