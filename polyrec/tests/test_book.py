import math

from polyrec.book import Book


def lv(*pairs):
    return [{"price": str(p), "size": str(s)} for p, s in pairs]


def test_snapshot_best_top_depth():
    b = Book()
    b.snapshot(lv((0.48, 50), (0.49, 100), (0.40, 10)), lv((0.51, 80), (0.60, 5)), "ws", 1)
    assert b.best() == (0.49, 100.0, 0.51, 80.0)
    assert b.top(2) == ([0.49, 0.48], [100.0, 50.0], [0.51, 0.6], [80.0, 5.0])
    bid_depth, ask_depth = b.depth(0.05)
    assert bid_depth == 0.49 * 100 + 0.48 * 50  # 0.40 is more than 5c away
    assert math.isclose(ask_depth, 0.51 * 80)


def test_apply_sets_and_removes_levels():
    b = Book()
    b.snapshot(lv((0.49, 100)), lv((0.51, 80)), "ws", 1)
    b.apply(True, "0.50", "30", 2)
    assert b.best()[:2] == (0.5, 30.0)
    b.apply(True, "0.50", "0", 3)
    assert b.best()[:2] == (0.49, 100.0)
    b.apply(False, "0.51", "0", 4)
    assert math.isnan(b.best()[2]) and b.depth()[1] == 0.0


def test_sub_cent_ticks_do_not_collide():
    b = Book()
    b.snapshot(lv((0.501, 1), (0.502, 2), (0.5, 3)), [], "ws", 1)
    assert b.top(3)[0] == [0.502, 0.501, 0.5]


def test_empty_and_zero_size_levels():
    b = Book()
    b.snapshot(lv((0.5, 0)), [], "ws", 1)
    assert all(math.isnan(x) for x in b.best())
