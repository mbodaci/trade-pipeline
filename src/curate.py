from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampNTZType,
)

ROOT = Path(__file__).resolve().parent.parent
REFINED_DIR = ROOT / "lake" / "refined"
CURATED_DIR = ROOT / "lake" / "curated"

INTERVAL = "5 minutes"
PARTITION_COLUMNS = ["trade_date", "instrument"]
ORDER_KEY = ["trade_date", "instrument", "order_id"]

PLACED = "placed"
PARTIALLY_FILLED = "partially_filled"
FILLED = "filled"
CANCELLED = "cancelled"
LIVE_STATES = (PLACED, PARTIALLY_FILLED)

LIFECYCLE_SCHEMA = StructType([
    StructField("trade_date", DateType()),
    StructField("instrument", StringType()),
    StructField("order_id", LongType()),
    StructField("side", StringType()),
    StructField("order_type", StringType()),
    StructField("state", StringType()),
    StructField("price", LongType()),
    StructField("remaining_quantity", LongType()),
    StructField("filled_quantity", LongType()),
    StructField("from_message", LongType()),
    StructField("valid_from", TimestampNTZType()),
    StructField("valid_to", TimestampNTZType()),
])


# Notional and volume, not VWAP: VWAP is not additive, so the VWAP of a longer period has to be
# sum(notional) / sum(volume) over its intervals, never an average of interval VWAPs.
# window() keeps NTZ bounds; intervals with no trades produce no row.
def traded_volume(trades):
    return (
        trades.groupBy(*PARTITION_COLUMNS, F.window("event_ts", INTERVAL).alias("interval"))
        .agg(
            F.count("*").alias("trade_count"),
            F.sum("quantity").alias("volume"),
            F.sum(F.col("price") * F.col("quantity")).alias("notional"),
        )
        .select(
            *PARTITION_COLUMNS,
            F.col("interval.start").alias("interval_start"),
            F.col("interval.end").alias("interval_end"),
            "trade_count",
            "volume",
            "notional",
        )
    )


# One row per order per message: the order's own flow message, if it has one there, and the
# quantity it traded at that message. A fill counts for both its aggressor and its passive order.
def order_events(flow, trades):
    fill_sides = [
        trades.select(
            *PARTITION_COLUMNS,
            F.col(side).alias("order_id"),
            "message",
            F.col("event_ts").alias("fill_ts"),
            "quantity",
        )
        for side in ("aggressor_id", "passive_id")
    ]
    fills = (
        fill_sides[0]
        .unionByName(fill_sides[1])
        .groupBy(*ORDER_KEY, "message", "fill_ts")
        .agg(F.sum("quantity").alias("filled"))
    )
    requests = flow.select(
        *PARTITION_COLUMNS,
        F.col("id").alias("order_id"),
        "message",
        "event_ts",
        F.col("type").alias("request"),
        "side",
        "price",
        "quantity",
        "order_type",
    )
    return (
        requests.join(fills, ORDER_KEY + ["message"], "full")
        .withColumn("event_ts", F.coalesce("event_ts", "fill_ts"))
        .drop("fill_ts")
    )


def next_state(order_type, remaining, filled):
    if remaining == 0:
        return FILLED
    # The engine discards whatever a market order could not fill; it never rests
    if order_type == "market":
        return CANCELLED
    return PARTIALLY_FILLED if filled > 0 else PLACED


# Replays one order's messages, sorted by message, and returns its versions in schema order.
# The engine rejects a cancel or modify of an order that is no longer resting and emits nothing,
# so a request takes effect only if the order is still live by its own fills. An order removed by
# self-trade prevention leaves no trace in either output, so here it stays live: a known gap.
def order_versions(events):
    versions = []
    side = order_type = price = remaining = state = None
    filled = 0
    for event in events:
        request = event["request"]
        live = state in LIVE_STATES
        if request == "add":
            side, order_type, remaining = event["side"], event["order_type"], event["quantity"]
            price = event["price"] if order_type == "limit" else None
        elif state is None:
            continue
        elif request in ("cancel", "modify") and not live:
            continue
        elif request == "modify" and event["quantity"] == 0:
            continue
        elif request == "modify":
            price, remaining = event["price"], event["quantity"]

        if request == "cancel":
            state, remaining = CANCELLED, 0
        else:
            fills = event["filled"] or 0
            remaining -= fills
            filled += fills
            state = next_state(order_type, remaining, filled)
            if state == CANCELLED:
                remaining = 0

        version = (side, order_type, state, price, remaining, filled, event["message"],
                   event["event_ts"])
        # A new version only when state, price or remaining quantity changes
        if not versions or versions[-1][2:5] != version[2:5]:
            versions.append(version)

    # Half-open [valid_from, valid_to): each version closes when the next opens; the last stays open
    starts = [version[-1] for version in versions]
    return [version + (valid_to,) for version, valid_to in zip(versions, starts[1:] + [None])]


def order_lifecycle(spark, flow, trades):
    # Spark's Python workers import this module to unpickle order_versions; when curate is
    # imported by another script or by the tests, it is not on their path unless shipped
    spark.sparkContext.addPyFile(__file__)
    event_fields = ["message", "event_ts", "request", "side", "price", "quantity", "order_type",
                    "filled"]
    # sort_array orders structs by their first field, message, so each order's events are in order
    per_order = order_events(flow, trades).groupBy(*ORDER_KEY).agg(
        F.sort_array(F.collect_list(F.struct(*event_fields))).alias("events")
    )
    versions = per_order.rdd.flatMap(
        lambda order: [
            (order.trade_date, order.instrument, order.order_id, *version)
            for version in order_versions(order.events)
        ]
    )
    return spark.createDataFrame(versions, LIFECYCLE_SCHEMA)


def write_partitioned(rows, path):
    (
        rows.repartition(*PARTITION_COLUMNS)
        .write.mode("overwrite")
        .partitionBy(*PARTITION_COLUMNS)
        .parquet(str(path))
    )


def curate_lake(spark, refined_dir=REFINED_DIR, curated_dir=CURATED_DIR):
    flow = spark.read.parquet(str(refined_dir / "flow"))
    trades = spark.read.parquet(str(refined_dir / "trades"))

    write_partitioned(traded_volume(trades), curated_dir / "traded_volume")
    write_partitioned(order_lifecycle(spark, flow, trades), curated_dir / "order_lifecycle")

    for name in ("traded_volume", "order_lifecycle"):
        print(f"{name}: {spark.read.parquet(str(curated_dir / name)).count()} rows")


def main():
    spark = SparkSession.builder.master("local[*]").appName("curate").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    curate_lake(spark)

    spark.stop()


if __name__ == "__main__":
    main()
