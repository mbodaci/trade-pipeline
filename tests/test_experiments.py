import random
import shutil

import pytest
from pyspark.sql import functions as F

import ingest
import reconcile
from reconcile import RAW_REFINED, REFINED, REFINED_CURATED

# The experiments behind README sections 4 and 7. Each edits a copy of data/, never data/ itself,
# and asserts the figures the README quotes, so the README cannot drift from what the code does.


@pytest.fixture
def data_copy(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    shutil.copytree(ingest.DATA_DIR, data_dir)
    monkeypatch.setattr(ingest, "DATA_DIR", data_dir)
    monkeypatch.setattr(ingest, "MANIFEST", data_dir / "manifest.csv")
    return data_dir


def rewrite(path, edit):
    path.write_text("\n".join(edit(path.read_text().splitlines())) + "\n")


def with_column_changed(lines, name, change):
    position = lines[0].split(",").index(name)

    def changed(line):
        fields = line.split(",")
        fields[position] = change(fields[position])
        return ",".join(fields)

    return lines[:1] + [changed(line) for line in lines[1:]]


# The clean lake's raw layer plus the edited file as a newer batch. Refine keeps only the newest
# batch per source file, so this matches a rebuild from scratch without re-ingesting 18 files.
def run_experiment(spark, raw_dir, tmp_path, source_file, session_date):
    entity, _ = ingest.parse_source_name(source_file.split("/")[-1])
    batch_id = ingest.batch_id_for(ingest.DATA_DIR / source_file, session_date)
    # A new batch id proves the edit changed the file: an unchanged file would be skipped
    assert not (raw_dir / entity / f"batch_id={batch_id}").exists()

    lake = tmp_path / "lake"
    shutil.copytree(raw_dir, lake / "raw")
    ingest.ingest_file(spark, source_file, session_date, lake / "raw")
    reconcile.refine_and_curate(spark, lake)
    return lake


def reconciled(spark, lake):
    return reconcile.run_checks(reconcile.load_batches(spark, lake))


def failed(results):
    return {(r.check, r.hop, r.batch, r.expected, r.found) for r in results if not r.passed}


def details(results, check):
    [result] = [r for r in results if r.check == check and not r.passed]
    return result.details


def session(frame, trade_date, instrument):
    return frame.filter((F.col("trade_date") == trade_date) & (F.col("instrument") == instrument))


def read(spark, lake, path):
    return spark.read.parquet(str(lake / path))


# Characterisation test: it pins current known-bad behaviour so a change to it is noticed, and
# does not endorse it. The production fix, a header contract checked at ingest, is in README
# section 4, "Renaming a column".
def test_renamed_column_lands_as_nulls_and_stops_the_reconciliation(
    spark, raw_dir, data_copy, tmp_path
):
    source = "session_03/trades_AAPL.csv"
    rewrite(data_copy / source, lambda lines: [lines[0].replace("quantity", "qty")] + lines[1:])
    lake = run_experiment(spark, raw_dir, tmp_path, source, "2026-10-21")

    trades = session(read(spark, lake, "refined/trades"), "2026-10-21", "AAPL")
    assert trades.count() == 1183
    assert trades.filter(F.col("quantity").isNotNull()).count() == 0
    assert trades.filter(F.col("qty").isNull()).count() == 0
    assert read(spark, lake, "refined/rejects/trades").count() == 0

    volume = session(read(spark, lake, "curated/traded_volume"), "2026-10-21", "AAPL")
    totals = volume.agg(F.sum("trade_count"), F.count("volume")).first()
    assert tuple(totals) == (1183, 0)

    with pytest.raises(KeyError, match="quantity"):
        reconcile.load_batches(spark, lake)


def test_decimal_prices_reject_the_whole_batch(spark, raw_dir, data_copy, tmp_path):
    source = "session_03/trades_GOOG.csv"

    def as_dollars(ticks):
        return f"{int(ticks) // 100}.{int(ticks) % 100:02d}"

    rewrite(data_copy / source, lambda lines: with_column_changed(lines, "price", as_dollars))
    lake = run_experiment(spark, raw_dir, tmp_path, source, "2026-10-21")

    assert session(read(spark, lake, "refined/trades"), "2026-10-21", "GOOG").count() == 0
    assert session(read(spark, lake, "curated/traded_volume"), "2026-10-21", "GOOG").count() == 0
    # The refined-to-curated checks pass for this batch because both sides are empty
    assert failed(reconciled(spark, lake)) == {("rejected_rows", RAW_REFINED, source, 0, 1178)}


# Source and lake agree and every value parses, so a source-to-output reconciliation is blind
def test_prices_scaled_by_100_pass_every_check(spark, raw_dir, data_copy, tmp_path):
    source = "session_03/trades_MSFT.csv"
    rewrite(data_copy / source,
            lambda lines: with_column_changed(lines, "price", lambda ticks: str(int(ticks) * 100)))
    lake = run_experiment(spark, raw_dir, tmp_path, source, "2026-10-21")
    assert failed(reconciled(spark, lake)) == set()


def test_mergeschema_cannot_read_a_column_retyped_between_batches(spark, tmp_path):
    table = tmp_path / "trades"
    spark.createDataFrame([(100002,)], "price long").write.parquet(str(table / "batch=1"))
    spark.createDataFrame([(1000.02,)], "price double").write.parquet(str(table / "batch=2"))
    with pytest.raises(Exception, match="merge"):
        spark.read.option("mergeSchema", True).parquet(str(table)).collect()


def test_swapped_trade_messages_are_caught_by_aggressor_and_lifecycle_checks(
    spark, raw_dir, data_copy, tmp_path
):
    source = "session_01/trades_AAPL.csv"
    swapped = {"1,2,1,99987,16": "3,2,1,99987,16", "3,4,3,100002,63": "1,4,3,100002,63"}
    rewrite(data_copy / source, lambda lines: [swapped.get(line, line) for line in lines])
    lake = run_experiment(spark, raw_dir, tmp_path, source, "2026-10-19")
    results = reconciled(spark, lake)
    assert failed(results) == {
        ("aggressor_at_message", REFINED, source, 0, 2),
        ("lifecycle_fills", REFINED_CURATED, source, 58996, 58854),
    }
    assert details(results, "aggressor_at_message") == (
        "message 1, passive_id 3: aggressor 4, flow has order 2",
        "message 3, passive_id 1: aggressor 2, flow has order 4",
    )


# The flow's message number is the row's position, so swapping rows swaps their messages, and
# source and raw still agree because both number rows the same way
def test_swapped_flow_rows_are_caught_by_aggressor_and_lifecycle_checks(
    spark, raw_dir, data_copy, tmp_path
):
    source = "session_02/flow_AAPL.csv"

    def swap_messages_11_and_17(lines):
        # Line 0 is the header, so message n is line n + 1
        lines[12], lines[18] = lines[18], lines[12]
        return lines

    rewrite(data_copy / source, swap_messages_11_and_17)
    lake = run_experiment(spark, raw_dir, tmp_path, source, "2026-10-20")
    results = reconciled(spark, lake)
    trades = "session_02/trades_AAPL.csv"
    assert failed(results) == {
        ("aggressor_at_message", REFINED, trades, 0, 2),
        ("lifecycle_fills", REFINED_CURATED, trades, 59660, 59646),
    }
    assert details(results, "aggressor_at_message") == (
        "message 11, passive_id 1: aggressor 9, flow has order 14",
        "message 17, passive_id 13: aggressor 14, flow has order 9",
    )


# Same rows, different bytes: the content hash differs and so does the path, so refine keeps both
def test_crlf_redelivery_doubles_the_session(spark, raw_dir, data_copy, tmp_path):
    original = "session_01/trades_GOOG.csv"
    redelivered = "session_01_redelivered/trades_GOOG.csv"
    (data_copy / redelivered).parent.mkdir()
    (data_copy / redelivered).write_bytes(
        (data_copy / original).read_bytes().replace(b"\n", b"\r\n")
    )
    with open(data_copy / "manifest.csv", "a") as f:
        f.write(f"{redelivered},2026-10-19\n")
    lake = run_experiment(spark, raw_dir, tmp_path, redelivered, "2026-10-19")

    assert session(read(spark, lake, "refined/trades"), "2026-10-19", "GOOG").count() == 2494
    # natural_key_unique names the duplicate. The refined-to-curated checks fail too, but only
    # because curated totals are per session while each batch expects only its own rows.
    assert failed(reconciled(spark, lake)) == {
        (check, hop, batch, expected, found)
        for batch in (original, redelivered)
        for check, hop, expected, found in [
            ("natural_key_unique", REFINED, 0, 1247),
            ("row_count", REFINED_CURATED, 1247, 2494),
            ("traded_quantity", REFINED_CURATED, 32573, 65146),
            ("lifecycle_fills", REFINED_CURATED, 65146, 130292),
        ]
    }


def same_rows(first, second):
    return first.exceptAll(second).count() == 0 and second.exceptAll(first).count() == 0


# Everything derived from the trades is ordered by message, never by arrival
def test_shuffled_rows_change_nothing(
    spark, raw_dir, lifecycle, traded_volume, data_copy, tmp_path
):
    source = "session_01/trades_AAPL.csv"

    def shuffled(lines):
        rows = lines[1:]
        random.Random(0).shuffle(rows)
        return lines[:1] + rows

    rewrite(data_copy / source, shuffled)
    lake = run_experiment(spark, raw_dir, tmp_path, source, "2026-10-19")

    results = reconciled(spark, lake)
    assert len(results) == 189 and failed(results) == set()
    for name, clean, count in [("order_lifecycle", lifecycle, 5577),
                               ("traded_volume", traded_volume, 17)]:
        rebuilt = session(read(spark, lake, f"curated/{name}"), "2026-10-19", "AAPL")
        assert rebuilt.count() == count
        assert same_rows(rebuilt, session(clean, "2026-10-19", "AAPL"))
