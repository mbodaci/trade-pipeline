from datetime import datetime

import pytest
from pyspark.sql import functions as F

import refine

KEY = ["session_date", "instrument", "message"]


@pytest.fixture(scope="module")
def trades_with_causing_order(flow_raw, trades_raw):
    causing_order = flow_raw.select(
        *KEY, F.col("id").alias("flow_id"), F.col("event_ts").alias("flow_ts")
    )
    return trades_raw.join(causing_order, KEY, "left")


def formatted_ts(rows):
    formatted = rows.select(F.date_format("event_ts", "yyyy-MM-dd HH:mm:ss").alias("ts"))
    return {row["ts"] for row in formatted.collect()}


# Checked by hand against the CSVs: flow line 48 (header is line 1) is message 46
def test_aapl_message_46_is_the_market_buy_that_swept_three_asks(flow_raw, trades_raw):
    at_message_46 = (
        (F.col("session_date") == "2026-10-19")
        & (F.col("instrument") == "AAPL")
        & (F.col("message") == 46)
    )
    orders = flow_raw.filter(at_message_46)
    fills = trades_raw.filter(at_message_46)

    order_fields = ["type", "id", "side", "price", "quantity", "order_type", "owner"]
    assert [tuple(row) for row in orders.select(order_fields).collect()] == [
        ("add", "28", "buy", "0", "73", "market", "6")
    ]
    fill_fields = ["aggressor_id", "passive_id", "price", "quantity"]
    assert sorted(tuple(row) for row in fills.select(fill_fields).collect()) == sorted([
        ("28", "22", "99998", "15"),
        ("28", "25", "99998", "29"),
        ("28", "13", "99999", "29"),
    ])
    assert formatted_ts(orders) == formatted_ts(fills) == {"2026-10-19 09:30:46"}


def test_every_trade_has_a_flow_row_at_its_message(trades_with_causing_order):
    assert trades_with_causing_order.count() > 0
    assert trades_with_causing_order.filter(F.col("flow_id").isNull()).count() == 0


def test_aggressor_is_the_order_at_that_message(trades_with_causing_order):
    mismatched = trades_with_causing_order.filter(F.col("flow_id") != F.col("aggressor_id"))
    assert mismatched.count() == 0


def test_passive_order_was_added_before_the_trade(flow_raw, trades_raw):
    adds = flow_raw.filter(F.col("type") == "add").select(
        "session_date", "instrument", F.col("id").alias("passive_id"),
        F.col("message").alias("add_message"),
    )
    checked = trades_raw.join(adds, ["session_date", "instrument", "passive_id"], "left")
    never_added_or_later = checked.filter(
        F.col("add_message").isNull() | (F.col("add_message") >= F.col("message"))
    )
    assert never_added_or_later.count() == 0


def test_event_ts_carries_no_time_zone(flow_raw, trades_raw):
    assert flow_raw.schema["event_ts"].dataType.simpleString() == "timestamp_ntz"
    assert trades_raw.schema["event_ts"].dataType.simpleString() == "timestamp_ntz"


def test_trade_and_flow_timestamps_agree(trades_with_causing_order):
    disagreeing = trades_with_causing_order.filter(F.col("flow_ts") != F.col("event_ts"))
    assert disagreeing.count() == 0


def test_unparseable_integers_are_flagged_not_cast(spark):
    values = ["42", "-5", None, "1.5", "abc", " 42", "9223372036854775808"]
    rows = spark.createDataFrame([(value,) for value in values], "quantity string")
    flagged = rows.select("quantity", refine.unparseable(["quantity"]).alias("failed")).collect()
    # "1.5" matters most: a bare cast would quietly turn it into 1
    assert {row["quantity"]: row["failed"] for row in flagged} == {
        "42": False, "-5": False, None: False,
        "1.5": True, "abc": True, " 42": True, "9223372036854775808": True,
    }


def test_only_the_newest_batch_per_source_file_is_kept(spark):
    rows = spark.createDataFrame(
        [
            ("session_01/trades_AAPL.csv", "wrong-date", datetime(2026, 10, 21, 8, 0)),
            ("session_01/trades_AAPL.csv", "corrected", datetime(2026, 10, 21, 9, 0)),
            ("session_01/trades_GOOG.csv", "only", datetime(2026, 10, 21, 8, 0)),
        ],
        "source_file string, batch_id string, ingested_at timestamp",
    )
    kept = {row["batch_id"] for row in refine.latest_batches(rows).collect()}
    assert kept == {"corrected", "only"}


def test_refine_keeps_every_raw_row_of_clean_data(spark, refined_dir, trades_raw, flow_raw):
    for entity, raw in [("trades", trades_raw), ("flow", flow_raw)]:
        refined = spark.read.parquet(str(refined_dir / entity))
        trade_date = F.to_date("session_date").alias("trade_date")
        raw_counts = raw.groupBy(trade_date, "instrument").count()
        refined_counts = refined.groupBy("trade_date", "instrument").count()
        assert raw_counts.count() > 0
        assert sorted(raw_counts.collect()) == sorted(refined_counts.collect())
        assert spark.read.parquet(str(refined_dir / "rejects" / entity)).count() == 0


def test_refined_is_one_file_per_trade_date_and_instrument(refined_dir):
    for entity in ("trades", "flow"):
        files = sorted((refined_dir / entity).rglob("*.parquet"))
        partitions = [file.parent.relative_to(refined_dir / entity).parts for file in files]
        assert len(partitions) == len(set(partitions)) == 9
        assert all(date.startswith("trade_date=") and instrument.startswith("instrument=")
                   for date, instrument in partitions)


def test_venue_is_null_before_day_3_and_set_on_it(spark, refined_dir):
    trades = spark.read.parquet(str(refined_dir / "trades"))
    venues = trades.groupBy("trade_date").agg(F.collect_set("venue").alias("venues")).collect()
    assert {str(row["trade_date"]): sorted(row["venues"]) for row in venues} == {
        "2026-10-19": [], "2026-10-20": [], "2026-10-21": ["SIM"],
    }
