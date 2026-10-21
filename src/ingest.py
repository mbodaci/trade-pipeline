import csv
import hashlib
import re
from datetime import date
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"
MANIFEST = DATA_DIR / "manifest.csv"
RAW_DIR = ROOT / "lake" / "raw"

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


def ingest_file(spark, source_file, session_date):
    path = DATA_DIR / source_file
    entity, instrument = parse_source_name(path.name)
    batch_id = batch_id_for(path, session_date)
    target = RAW_DIR / entity / f"batch_id={batch_id}"

    # _SUCCESS is written last by Spark's commit, so its absence means no batch or a partial one
    if (target / "_SUCCESS").exists():
        print(f"skip {source_file}: batch {batch_id} already ingested")
        return

    # No type inference: a malformed value must reach raw as written, not silently become null
    source_rows = (
        spark.read.option("header", True)
        .option("inferSchema", False)
        .csv(str(path))
        .withColumn("instrument", F.lit(instrument))
        .withColumn("session_date", F.lit(session_date))
        .withColumn("source_file", F.lit(source_file))
        .withColumn("ingested_at", F.current_timestamp())
    )

    # Overwrite only ever replaces a partial batch: committed batches were skipped above
    source_rows.coalesce(1).write.mode("overwrite").parquet(str(target))
    print(f"ingested {source_file}: {source_rows.count()} rows -> {target.relative_to(ROOT)}")


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
