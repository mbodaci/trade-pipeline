from datetime import datetime, timedelta

from pyspark.sql import Window
from pyspark.sql import functions as F

import curate

SESSION_OPEN = datetime(2026, 10, 19, 9, 30)


def at(message):
    return SESSION_OPEN + timedelta(seconds=message)


# F.lit(datetime) makes a zoned timestamp, which shifts by the session zone when compared with
# an NTZ column. Parsing the wall-clock string as NTZ keeps the comparison zone-free.
def ntz(moment):
    return F.to_timestamp_ntz(F.lit(moment.isoformat(sep=" ")))


def event(message, request=None, filled=None, side=None, price=None, quantity=None,
          order_type=None):
    return {"message": message, "event_ts": at(message), "request": request, "side": side,
            "price": price, "quantity": quantity, "order_type": order_type, "filled": filled}


# (state, price, remaining, filled, from_message, valid_to) of each version
def states(versions):
    return [(v[2], v[3], v[4], v[5], v[6], v[8]) for v in versions]


# Order 22, AAPL 2026-10-19, traced by hand in the CSVs
def test_order_22_rests_fills_and_ignores_the_late_cancel():
    versions = curate.order_versions([
        event(36, "add", side="sell", price=99998, quantity=15, order_type="limit"),
        event(46, filled=15),
        event(62, "cancel"),
    ])
    assert states(versions) == [
        ("placed", 99998, 15, 0, 36, at(46)),
        ("filled", 99998, 0, 15, 46, None),
    ]


def test_market_sweep_is_one_filled_version():
    versions = curate.order_versions([
        event(46, "add", filled=73, side="buy", price=0, quantity=73, order_type="market"),
    ])
    assert states(versions) == [("filled", None, 0, 73, 46, None)]


def test_market_remainder_is_discarded_as_cancelled():
    versions = curate.order_versions([
        event(10, "add", filled=20, side="buy", price=0, quantity=50, order_type="market"),
    ])
    assert states(versions) == [("cancelled", None, 0, 20, 10, None)]


def test_partial_fills_at_different_messages_each_make_a_version():
    versions = curate.order_versions([
        event(5, "add", side="buy", price=99990, quantity=30, order_type="limit"),
        event(9, filled=10),
        event(14, filled=5),
        event(20, "cancel"),
    ])
    assert states(versions) == [
        ("placed", 99990, 30, 0, 5, at(9)),
        ("partially_filled", 99990, 20, 10, 9, at(14)),
        ("partially_filled", 99990, 15, 15, 14, at(20)),
        ("cancelled", 99990, 0, 15, 20, None),
    ]


def test_modify_reprices_a_live_order_and_is_ignored_once_it_is_gone():
    versions = curate.order_versions([
        event(1, "add", side="sell", price=100010, quantity=40, order_type="limit"),
        event(3, "modify", price=100010, quantity=40),
        event(4, "modify", price=100005, quantity=25),
        event(6, filled=25),
        event(8, "modify", price=100001, quantity=10),
    ])
    # Message 3 changes nothing, so no version; message 8 targets a filled order, so it is rejected
    assert states(versions) == [
        ("placed", 100010, 40, 0, 1, at(4)),
        ("placed", 100005, 25, 0, 4, at(6)),
        ("filled", 100005, 0, 25, 6, None),
    ]


def test_modify_that_trades_on_re_entry_is_one_version():
    versions = curate.order_versions([
        event(1, "add", side="buy", price=99990, quantity=40, order_type="limit"),
        event(7, "modify", filled=12, price=100002, quantity=30),
    ])
    assert states(versions) == [
        ("placed", 99990, 40, 0, 1, at(7)),
        ("partially_filled", 100002, 18, 12, 7, None),
    ]


def test_lifecycle_of_orders_22_and_28_from_the_real_data(lifecycle):
    rows = (
        lifecycle.filter("trade_date = '2026-10-19' AND instrument = 'AAPL'")
        .filter(F.col("order_id").isin(22, 28))
        .orderBy("order_id", "valid_from")
        .select("order_id", "state", "remaining_quantity", "filled_quantity", "valid_from",
                "valid_to")
        .collect()
    )
    # Order 28's three fills share message 46, so they make one version, not three
    assert [tuple(row) for row in rows] == [
        (22, "placed", 15, 0, at(36), at(46)),
        (22, "filled", 0, 15, at(46), None),
        (28, "filled", 0, 73, at(46), None),
    ]


def test_as_of_query_returns_the_version_valid_at_that_moment(lifecycle):
    order_22 = lifecycle.filter(
        "trade_date = '2026-10-19' AND instrument = 'AAPL' AND order_id = 22"
    )

    def as_of(moment):
        valid = (F.col("valid_from") <= ntz(moment)) & (
            F.col("valid_to").isNull() | (ntz(moment) < F.col("valid_to"))
        )
        return [row["state"] for row in order_22.filter(valid).collect()]

    assert as_of(at(30)) == []
    assert as_of(at(40)) == ["placed"]
    assert as_of(at(46)) == ["filled"]
    assert as_of(at(4000)) == ["filled"]


def test_every_order_has_exactly_one_open_version_and_it_is_the_last(lifecycle):
    per_order = lifecycle.groupBy(*curate.ORDER_KEY).agg(
        F.sum(F.col("valid_to").isNull().cast("int")).alias("open_versions"),
        F.max("valid_from").alias("last_from"),
        F.max(F.when(F.col("valid_to").isNull(), F.col("valid_from"))).alias("open_from"),
    )
    assert per_order.count() > 0
    assert per_order.filter("open_versions != 1 OR open_from != last_from").count() == 0


def test_versions_are_contiguous_and_never_empty(lifecycle):
    by_order = Window.partitionBy(*curate.ORDER_KEY).orderBy("valid_from")
    checked = lifecycle.withColumn("next_from", F.lead("valid_from").over(by_order))
    broken = checked.filter(
        ~F.col("valid_to").eqNullSafe(F.col("next_from"))
        | (F.col("valid_to") <= F.col("valid_from"))
    )
    assert broken.count() == 0


def test_lifecycle_fills_account_for_every_trade_twice(lifecycle, trades_refined):
    final = lifecycle.filter(F.col("valid_to").isNull())
    filled = final.agg(F.sum("filled_quantity")).first()[0]
    traded = trades_refined.agg(F.sum("quantity")).first()[0]
    # Each trade fills one aggressor and one passive order by the same quantity
    assert filled == 2 * traded


# Checked by hand with awk over trades_AAPL.csv: messages 0-299 are 09:30:00 to 09:34:59
def test_first_interval_of_aapl_on_2026_10_19(traded_volume):
    first = traded_volume.filter(
        (F.col("trade_date") == "2026-10-19")
        & (F.col("instrument") == "AAPL")
        & (F.col("interval_start") == ntz(at(0)))
    )
    assert [
        (row["interval_end"], row["trade_count"], row["volume"], row["notional"])
        for row in first.collect()
    ] == [(at(300), 56, 1461, 146098057)]


def test_interval_totals_sum_to_each_session(traded_volume, trades_refined):
    from_intervals = traded_volume.groupBy(*curate.PARTITION_COLUMNS).agg(
        F.sum("trade_count").alias("trades"),
        F.sum("volume").alias("volume"),
        F.sum("notional").alias("notional"),
    )
    direct = trades_refined.groupBy(*curate.PARTITION_COLUMNS).agg(
        F.count("*").alias("trades"),
        F.sum("quantity").alias("volume"),
        F.sum(F.col("price") * F.col("quantity")).alias("notional"),
    )
    assert direct.count() == 9
    assert sorted(from_intervals.collect()) == sorted(direct.collect())
    # Hand check with awk: AAPL 2026-10-19 session VWAP is 2949798493 / 29498
    aapl = from_intervals.filter("trade_date = '2026-10-19' AND instrument = 'AAPL'").first()
    assert (aapl["notional"], aapl["volume"]) == (2949798493, 29498)
