import pytest
from pyspark.sql import functions as F

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

