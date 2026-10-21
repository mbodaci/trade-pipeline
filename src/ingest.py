import hashlib
import re
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"
RAW_DIR = ROOT / "lake" / "raw"

# The engine is single-instrument, so the instrument is only recoverable from the filename
SOURCE_NAME = re.compile(r"^(trades|flow)_([A-Z]+)\.csv$")


def parse_source_name(filename):
    match = SOURCE_NAME.match(filename)
    return match.groups() if match else None


def batch_id_for(path):
    # Content hash: re-running on an unchanged file maps to the same batch, so it is not ingested twice
    digest = hashlib.sha256(path.read_bytes()).hexdigest()[:12]
    return f"{path.stem}-{digest}"


def ingest_file(spark, path, entity, instrument):
    batch_id = batch_id_for(path)
    target = RAW_DIR / entity / f"batch_id={batch_id}"

    # _SUCCESS is written last by Spark's commit, so its absence means no batch or a partial one
    if (target / "_SUCCESS").exists():
        print(f"skip {path.name}: batch {batch_id} already ingested")
        return

    # No type inference: a malformed value must reach raw as written, not silently become null
    source_rows = (
        spark.read.option("header", True)
        .option("inferSchema", False)
        .csv(str(path))
        .withColumn("instrument", F.lit(instrument))
        .withColumn("source_file", F.lit(path.name))
        .withColumn("ingested_at", F.current_timestamp())
    )

    # Overwrite only ever replaces a partial batch: committed batches were skipped above
    source_rows.coalesce(1).write.mode("overwrite").parquet(str(target))
    print(f"ingested {path.name}: {source_rows.count()} rows -> {target.relative_to(ROOT)}")


def main():
    spark = SparkSession.builder.master("local[*]").appName("ingest").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    for path in sorted(DATA_DIR.glob("*.csv")):
        parsed = parse_source_name(path.name)
        if parsed is None:
            print(f"skip {path.name}: filename does not name an entity and instrument")
            continue
        entity, instrument = parsed
        ingest_file(spark, path, entity, instrument)

    spark.stop()


if __name__ == "__main__":
    main()
