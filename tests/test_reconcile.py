import shutil
from collections import Counter

import pytest

import reconcile
from reconcile import RAW_REFINED, REFINED, REFINED_CURATED, SOURCE_RAW

# message, aggressor_id, passive_id, price, quantity: one fill, then a sweep of two
TRADES = [
    ("1", "11", "10", "100000", "5"),
    ("2", "12", "10", "100001", "7"),
    ("2", "12", "9", "100002", "3"),
]


def with_quantity(row, quantity):
    return row[:4] + (quantity,)


# The flow orders at messages 1 and 2, which are the aggressors in TRADES
ORDER_AT_MESSAGE = {1: 11, 2: 12}


# A batch every layer carried faithfully, unless a test overrides one layer. Alone in its
# session unless key_counts says otherwise.
def trades_batch(raw=None, rejected=(), refined=None, curated=None, order_ids=None,
                 order_at_message=ORDER_AT_MESSAGE, key_counts=None):
    raw = list(TRADES) if raw is None else raw
    if refined is None:
        refined = [reconcile.as_refined("trades", row) for row in raw if row not in rejected]
    volume = sum(row[4] for row in refined)
    if curated is None:
        curated = {"trade_count": len(refined), "volume": volume, "lifecycle_filled": 2 * volume}
    return reconcile.Batch(
        "session_01/trades_TEST.csv", "trades", "2026-10-19", "TEST",
        source=list(TRADES), raw=raw, rejected=list(rejected), refined=refined, curated=curated,
        order_ids={9, 10, 11, 12} if order_ids is None else order_ids,
        order_at_message=order_at_message,
        key_counts=(Counter(reconcile.row_key("trades", row) for row in refined)
                    if key_counts is None else key_counts),
    )


def failures(batch, checks=reconcile.CHECKS):
    return {(r.check, r.hop) for r in reconcile.run_checks([batch], checks) if not r.passed}


def test_a_faithfully_carried_batch_passes_every_check():
    assert failures(trades_batch()) == set()


def test_row_count_catches_a_row_lost_before_raw():
    batch = trades_batch(raw=TRADES[:2])
    assert failures(batch, [reconcile.check_row_count]) == {("row_count", SOURCE_RAW)}


def test_row_count_catches_a_row_refine_lost_without_rejecting_it():
    refined = [reconcile.as_refined("trades", row) for row in TRADES[:2]]
    assert failures(trades_batch(refined=refined), [reconcile.check_row_count]) == {
        ("row_count", RAW_REFINED)
    }


def test_traded_quantity_catches_an_altered_quantity():
    raw = [with_quantity(TRADES[0], "6")] + TRADES[1:]
    assert failures(trades_batch(raw=raw), [reconcile.check_traded_quantity]) == {
        ("traded_quantity", SOURCE_RAW)
    }


def test_traded_quantity_catches_curated_volume_that_disagrees_with_refined():
    batch = trades_batch(curated={"trade_count": 3, "volume": 14, "lifecycle_filled": 30})
    assert failures(batch, [reconcile.check_traded_quantity]) == {
        ("traded_quantity", REFINED_CURATED)
    }


# Offsetting errors: one quantity up 2, another down 2. Counts and totals still agree, so
# only the row-level comparison sees it. This is why totals alone cannot be trusted.
def test_offsetting_errors_pass_every_total_and_fail_only_matching_rows():
    raw = [with_quantity(TRADES[0], "7"), with_quantity(TRADES[1], "5"), TRADES[2]]
    results = reconcile.run_checks([trades_batch(raw=raw)])
    assert {(r.check, r.hop) for r in results if not r.passed} == {("matching_rows", SOURCE_RAW)}
    [break_found] = [r for r in results if not r.passed]
    assert break_found.details == (
        "message 1, passive_id 10: quantity 5 in source, 7 in raw",
        "message 2, passive_id 10: quantity 7 in source, 5 in raw",
    )


def test_rejected_rows_catches_an_unparseable_value_that_refine_set_aside():
    unparseable = with_quantity(TRADES[0], "1.5")
    batch = trades_batch(raw=[unparseable] + TRADES[1:], rejected=[unparseable])
    assert failures(batch, [reconcile.check_rejected_rows]) == {("rejected_rows", RAW_REFINED)}


def test_positive_quantity_catches_zero_and_negative_quantities():
    for quantity in ("0", "-3"):
        raw = [with_quantity(TRADES[0], quantity)] + TRADES[1:]
        assert failures(trades_batch(raw=raw), [reconcile.check_positive_quantity]) == {
            ("positive_quantity", REFINED)
        }


def test_order_ids_exist_catches_a_trade_against_an_order_never_added():
    batch = trades_batch(order_ids={10, 11, 12})
    assert failures(batch, [reconcile.check_order_ids]) == {("order_ids_exist", REFINED)}


def test_aggressor_at_message_catches_a_trade_whose_message_holds_another_order():
    batch = trades_batch(order_at_message={1: 11, 2: 9})
    results = reconcile.run_checks([batch], [reconcile.check_aggressor_at_message])
    assert [(r.check, r.found, r.details) for r in results] == [(
        "aggressor_at_message", 2, (
            "message 2, passive_id 9: aggressor 12, flow has order 9",
            "message 2, passive_id 10: aggressor 12, flow has order 9",
        ),
    )]


def test_natural_key_unique_catches_a_trade_also_in_another_batch():
    batch = trades_batch(key_counts=Counter({(1, 10): 2, (2, 10): 1, (2, 9): 1}))
    results = reconcile.run_checks([batch], [reconcile.check_natural_key_unique])
    assert [(r.check, r.found, r.details) for r in results] == [
        ("natural_key_unique", 1, ("message 1, passive_id 10: 2 rows in the session",))
    ]


def test_lifecycle_fills_catches_fills_that_do_not_match_twice_the_volume():
    batch = trades_batch(curated={"trade_count": 3, "volume": 15, "lifecycle_filled": 29})
    assert failures(batch, [reconcile.check_lifecycle_fills]) == {
        ("lifecycle_fills", REFINED_CURATED)
    }


# Bad data the engine itself emitted: source and lake agree, so no source-to-output check can
# see it and only a validity check will
def emitted_by_engine(rows, **overrides):
    batch = trades_batch(raw=rows, **overrides)
    batch.source = list(rows)
    return batch


UNPARSEABLE = with_quantity(TRADES[0], "1.5")
ONLY_CATCHER = {
    "matching_rows": trades_batch(
        raw=[with_quantity(TRADES[0], "7"), with_quantity(TRADES[1], "5"), TRADES[2]]
    ),
    "positive_quantity": emitted_by_engine([with_quantity(TRADES[0], "0")] + TRADES[1:]),
    "rejected_rows": emitted_by_engine([UNPARSEABLE] + TRADES[1:], rejected=[UNPARSEABLE]),
    "order_ids_exist": trades_batch(order_ids={10, 11, 12}),
    "aggressor_at_message": trades_batch(order_at_message={1: 11, 2: 9}),
    # A line the engine wrote twice: matching_rows compares by key, so it cannot see the copy
    "natural_key_unique": emitted_by_engine(TRADES + [TRADES[0]]),
    "traded_quantity": trades_batch(curated={"trade_count": 3, "volume": 14,
                                             "lifecycle_filled": 30}),
    "row_count": trades_batch(curated={"trade_count": 2, "volume": 15, "lifecycle_filled": 30}),
    "lifecycle_fills": trades_batch(curated={"trade_count": 3, "volume": 15,
                                             "lifecycle_filled": 29}),
}


# Each check is load-bearing: for each one there is a failure that no other check sees
@pytest.mark.parametrize("check", list(ONLY_CATCHER))
def test_each_check_is_the_only_one_that_catches_some_failure(check):
    results = reconcile.run_checks([ONLY_CATCHER[check]])
    assert {r.check for r in results if not r.passed} == {check}


def test_the_clean_lake_passes_every_check(spark, lake_dir, curated_dir):
    results = reconcile.run_checks(reconcile.load_batches(spark, lake_dir))
    assert len(results) > 0
    assert [r for r in results if not r.passed] == []


def test_the_planted_break_is_caught_where_it_entered(spark, raw_dir, curated_dir, tmp_path):
    broken_lake = tmp_path / "lake_broken"
    shutil.copytree(raw_dir, broken_lake / "raw")
    reconcile.damage_batch(spark, broken_lake / "raw")
    reconcile.refine_and_curate(spark, broken_lake)

    [batch_dir] = (broken_lake / "raw" / "trades").glob(reconcile.BROKEN_BATCH_PATTERN)
    assert (batch_dir / "_SUCCESS").exists()

    results = reconcile.run_checks(reconcile.load_batches(spark, broken_lake))
    broken = "session_02/trades_MSFT.csv"
    # 3 fills of message 93 dropped (quantity 44) and one quantity of 38 set to 0
    assert {(r.check, r.hop, r.batch, r.expected, r.found) for r in results if not r.passed} == {
        ("row_count", SOURCE_RAW, broken, 1187, 1184),
        ("traded_quantity", SOURCE_RAW, broken, 30505, 30505 - 44 - 38),
        ("matching_rows", SOURCE_RAW, broken, 0, 4),
        ("positive_quantity", REFINED, broken, 0, 1),
    }
    [rows] = [r for r in results if r.check == "matching_rows" and not r.passed]
    assert rows.details == (
        "message 30, passive_id 7: quantity 38 in source, 0 in raw",
        "message 93, passive_id 6: in source, missing from raw",
        "message 93, passive_id 23: in source, missing from raw",
        "message 93, passive_id 48: in source, missing from raw",
    )
