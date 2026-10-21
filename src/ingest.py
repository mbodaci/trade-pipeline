import csv
import hashlib
import re
from datetime import date, time
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"
MANIFEST = DATA_DIR / "manifest.csv"
RAW_DIR = ROOT / "lake" / "raw"

# The engine has no clock: message n of a session is stamped SESSION_OPEN + n seconds
SESSION_OPEN = time(9, 30)
SECONDS_PER_MESSAGE = 1

# The engine is single-instrument, so the instrument is only recoverable from the filename
SOURCE_NAME = re.compile(r"^(trades|flow)_([A-Z]+)\.csv$")


def parse_source_name(filename):
    match = SOURCE_NAME.match(filename)
    return match.groups() if match else None


def read_manifest():
    with open(MANIFEST, newline="") as f:
        entries = list(csv.DictReader(f))
    # Validate every entry before writing anything: a bad row should stop the run, not half of it
    for entry in entries:
        date.fromisoformat(entry["session_date"])
        if parse_source_name(Path(entry["source_file"]).name) is None:
            raise ValueError(f"{entry['source_file']}: expected <trades|flow>_<TICKER>.csv")
    return entries


def batch_id_for(path, session_date):
    # Content hash: re-running on an unchanged file maps to the same batch, so it is not ingested
    # twice. The date is in the id so a corrected manifest date makes a new batch, not a skip.
    digest = hashlib.sha256(path.read_bytes()).hexdigest()[:12]
    return f"{path.stem}-{session_date}-{digest}"


def read_trades(spark, path):
    # No type inference: a malformed value must reach raw as written, not silently become null
    return spark.read.option("header", True).option("inferSchema", False).csv(str(path))


# The flow has no sequence column: a trade's message number is the flow row's position in the
# file, which Parquet does not preserve, so it has to be captured here or it is lost
def read_flow(spark, path):
    lines = spark.sparkContext.textFile(str(path))
    header = lines.first()
    # zipWithIndex follows file order across partitions, unlike monotonically_increasing_id.
    # The header is line 0, so line n is message n - 1, 0-based like the trade log.
    numbered = (
        lines.zipWithIndex()
        .filter(lambda pair: pair[1] > 0)
        .map(lambda pair: (pair[0], pair[1] - 1))
        .toDF(["line", "message"])
    )
    schema = ", ".join(f"`{name}` STRING" for name in header.split(","))
    return numbered.select("message", F.from_csv("line", schema).alias("row")).select(
        "message", "row.*"
    )


# TIMESTAMP_NTZ: the engine has no clock and no zone, so a zoned timestamp would invent one
def event_ts_for(session_date):
    session_open = F.to_timestamp_ntz(F.lit(f"{session_date} {SESSION_OPEN}"))
    offset = F.make_interval(secs=F.col("message").cast("long") * SECONDS_PER_MESSAGE)
    return session_open + offset


def ingest_file(spark, source_file, session_date):
    path = DATA_DIR / source_file
    entity, instrument = parse_source_name(path.name)
    batch_id = batch_id_for(path, session_date)
    target = RAW_DIR / entity / f"batch_id={batch_id}"

    # _SUCCESS is written last by Spark's commit, so its absence means no batch or a partial one
    if (target / "_SUCCESS").exists():
        print(f"skip {source_file}: batch {batch_id} already ingested")
        return

    read_source = read_flow if entity == "flow" else read_trades
    source_rows = (
        read_source(spark, path)
        .withColumn("instrument", F.lit(instrument))
        .withColumn("session_date", F.lit(session_date))
        .withColumn("event_ts", event_ts_for(session_date))
        .withColumn("source_file", F.lit(source_file))
        .withColumn("ingested_at", F.current_timestamp())
    )

    # Overwrite only ever replaces a partial batch: committed batches were skipped above
    source_rows.coalesce(1).write.mode("overwrite").parquet(str(target))
    # Counted from what landed, and cheap: a Parquet count reads only the file footer
    written = spark.read.parquet(str(target)).count()
    print(f"ingested {source_file}: {written} rows as batch {batch_id}")


def main():
    entries = read_manifest()

    listed = {DATA_DIR / entry["source_file"] for entry in entries}
    for path in sorted(set(DATA_DIR.rglob("*.csv")) - listed - {MANIFEST}):
        print(f"skip {path.relative_to(DATA_DIR)}: not in manifest, so it has no session date")

    spark = SparkSession.builder.master("local[*]").appName("ingest").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    for entry in entries:
        ingest_file(spark, entry["source_file"], entry["session_date"])

    spark.stop()


if __name__ == "__main__":
    main()
