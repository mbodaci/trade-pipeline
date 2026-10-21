import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

import curate
import ingest
import refine


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


# One temporary lake per test run, laid out like lake/, so the tests never touch lake/ itself
@pytest.fixture(scope="session")
def lake_dir(tmp_path_factory):
    return tmp_path_factory.mktemp("lake")


@pytest.fixture(scope="session")
def raw_dir(spark, lake_dir):
    raw_dir = lake_dir / "raw"
    for entry in ingest.read_manifest():
        ingest.ingest_file(spark, entry["source_file"], entry["session_date"], raw_dir)
    return raw_dir


@pytest.fixture(scope="session")
def refined_dir(spark, lake_dir, raw_dir):
    refined_dir = lake_dir / "refined"
    for entity in refine.INTEGER_COLUMNS:
        refine.refine_entity(spark, entity, raw_dir, refined_dir)
    return refined_dir


@pytest.fixture(scope="session")
def curated_dir(spark, lake_dir, refined_dir):
    curated_dir = lake_dir / "curated"
    curate.curate_lake(spark, refined_dir, curated_dir)
    return curated_dir


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


@pytest.fixture(scope="session")
def flow_refined(spark, refined_dir):
    return spark.read.parquet(str(refined_dir / "flow"))


@pytest.fixture(scope="session")
def trades_refined(spark, refined_dir):
    return spark.read.parquet(str(refined_dir / "trades"))


@pytest.fixture(scope="session")
def lifecycle(spark, curated_dir):
    return spark.read.parquet(str(curated_dir / "order_lifecycle"))


@pytest.fixture(scope="session")
def traded_volume(spark, curated_dir):
    return spark.read.parquet(str(curated_dir / "traded_volume"))
