from datetime import time
from pathlib import Path

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = ROOT / "lake" / "raw"
REFINED_DIR = ROOT / "lake" / "refined"

# Raw columns that must hold integers. Prices are integer ticks in the engine, never decimals.
# Flow's message is absent: ingest generates it as a number, so it never needs parsing.
INTEGER_COLUMNS = {
    "trades": ["message", "aggressor_id", "passive_id", "price", "quantity"],
    "flow": ["id", "price", "quantity", "owner"],
}
INTEGER_PATTERN = r"^-?[0-9]+$"
PARTITION_COLUMNS = ["trade_date", "instrument"]

# The engine has no clock: message n of a session is stamped SESSION_OPEN + n seconds
SESSION_OPEN = time(9, 30)
SECONDS_PER_MESSAGE = 1


def read_raw(spark, raw_dir, entity):
    # mergeSchema: batches from before the venue column existed are read with venue as null
    return spark.read.option("mergeSchema", True).parquet(str(raw_dir / entity))


# A corrected manifest date or a regenerated file creates a new batch for the same source file.
# Raw is immutable, so the old batch stays there; refine keeps only the newest. ingested_at is
# evaluated once per write, so every row of a batch shares it.
def latest_batches(raw):
    per_source_file = Window.partitionBy("source_file")
    return (
        raw.withColumn("newest_ingest", F.max("ingested_at").over(per_source_file))
        .filter(F.col("ingested_at") == F.col("newest_ingest"))
        .drop("newest_ingest")
    )


# A cast alone is not a parse check: it turns "1.5" into 1 without complaint. A present value is
# unparseable if it is not an integer as written, or too large for a long (the cast gives null).
# Empty fields, such as a cancel's price, are already null in raw and are not failures.
def unparseable(columns):
    failed = F.lit(False)
    for name in columns:
        bad = ~F.col(name).rlike(INTEGER_PATTERN) | F.col(name).cast("long").isNull()
        failed = failed | (F.col(name).isNotNull() & bad)
    return failed


def typed(rows, columns):
    casts = [
        F.col(name).cast("long").alias(name) if name in columns else name for name in rows.columns
    ]
    return rows.select(*casts)


# Derived here, not in raw: if the rule changes, the next refine run corrects every row.
# TIMESTAMP_NTZ because the engine has no clock and no zone, so a zoned timestamp would invent one.
def event_ts():
    session_open = F.to_timestamp_ntz(F.concat_ws(" ", "session_date", F.lit(str(SESSION_OPEN))))
    offset = F.make_interval(secs=F.col("message") * SECONDS_PER_MESSAGE)
    return session_open + offset


def refine_entity(spark, entity, raw_dir=RAW_DIR, refined_dir=REFINED_DIR):
    raw = read_raw(spark, raw_dir, entity)
    latest = latest_batches(raw)
    columns = INTEGER_COLUMNS[entity]
    failed = unparseable(columns)

    # Rejects keep the raw strings: the evidence of what arrived is the point of keeping them
    rejects = latest.filter(failed)
    refined = (
        typed(latest.filter(~failed), columns)
        .withColumn("event_ts", event_ts())
        .withColumn("trade_date", F.to_date("session_date"))
        .drop("session_date")
    )

    # Rebuilt from raw on every run, so a full overwrite is idempotent.
    # repartition by the partition columns: one file per partition, not one per task.
    (
        refined.repartition(*PARTITION_COLUMNS)
        .write.mode("overwrite")
        .partitionBy(*PARTITION_COLUMNS)
        .parquet(str(refined_dir / entity))
    )
    rejects.coalesce(1).write.mode("overwrite").parquet(str(refined_dir / "rejects" / entity))

    written = spark.read.parquet(str(refined_dir / entity)).count()
    rejected = spark.read.parquet(str(refined_dir / "rejects" / entity)).count()
    superseded = raw.count() - latest.count()
    print(
        f"{entity}: {written} refined, {rejected} rejected, "
        f"{superseded} rows in superseded batches skipped"
    )


def main():
    spark = SparkSession.builder.master("local[*]").appName("refine").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    for entity in INTEGER_COLUMNS:
        refine_entity(spark, entity)

    spark.stop()


if __name__ == "__main__":
    main()
