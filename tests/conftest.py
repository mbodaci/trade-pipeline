import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

import ingest


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("tests")
        # Deliberately not UTC: event_ts is zone-free, so no test may depend on the session zone
        .config("spark.sql.session.timeZone", "America/New_York")
        # The default of 200 shuffle partitions makes every join on this small data slow
        .config("spark.sql.shuffle.partitions", 4)
        .getOrCreate()
    )
    yield session
    session.stop()


# Ingest every manifest entry once per test run, into a temporary lake, never into lake/
@pytest.fixture(scope="session")
def raw_dir(spark, tmp_path_factory):
    raw_dir = tmp_path_factory.mktemp("raw")
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(ingest, "RAW_DIR", raw_dir)
        for entry in ingest.read_manifest():
            ingest.ingest_file(spark, entry["source_file"], entry["session_date"])
    return raw_dir


@pytest.fixture(scope="session")
def flow_raw(spark, raw_dir):
    return spark.read.parquet(str(raw_dir / "flow"))


@pytest.fixture(scope="session")
def trades_raw(spark, raw_dir):
    # message is a string in raw trades but a number in raw flow, which ingest generates
    return (
        spark.read.option("mergeSchema", True)
        .parquet(str(raw_dir / "trades"))
        .withColumn("message", F.col("message").cast("long"))
    )
