import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("tests")
        # Same zone as the pipeline, so formatted timestamps match what ingest wrote
        .config("spark.sql.session.timeZone", "UTC")
        # The default of 200 shuffle partitions makes every join on this small data slow
        .config("spark.sql.shuffle.partitions", 4)
        .getOrCreate()
    )
    yield session
    session.stop()
