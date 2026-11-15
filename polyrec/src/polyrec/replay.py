"""Rebuild order books from the derived l2 table, for event-level backtests."""
from __future__ import annotations

from typing import Iterable, Iterator

from .book import Book

DELTA, LEVEL, RESET, DESYNC = 0, 1, 2, 3


def apply_l2(book: Book, kind: int, side: int, price, size, r: int = 0) -> None:
    """Apply one l2 row. After DESYNC the book is unknown (book.src is None) until the next RESET."""
    if kind == RESET:
        book.snapshot((), (), "l2", r)
    elif kind == DESYNC:
        book.invalidate()
    elif book.src is not None:
        book.apply(side == 0, price, size, r)


def replay(rows: Iterable[dict]) -> Iterator[tuple[dict, Book]]:
    """Yield (row, book) after each l2 row of ONE token (filter by condition_id and outcome first)."""
    book = Book()
    for row in rows:
        apply_l2(book, row["kind"], row["side"], row["price"], row["size"], row["r"])
        yield row, book
