import argparse
import csv
import re
import shutil
import sys
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

import curate
import ingest
import refine

ROOT = Path(__file__).resolve().parent.parent
LAKE_DIR = ROOT / "lake"
BROKEN_LAKE_DIR = ROOT / "lake_broken"
RESULTS_DIR = ROOT / "results"

# Flow rows carry their message number first: the source has it only as the row's position
FIELDS = {
    "trades": ["message", "aggressor_id", "passive_id", "price", "quantity"],
    "flow": ["message", "type", "id", "side", "price", "quantity", "order_type", "owner"],
}
# Kept separate from refine's list on purpose: the checker should not borrow the rules it checks
INTEGER_FIELDS = {
    "trades": {"message", "aggressor_id", "passive_id", "price", "quantity"},
    "flow": {"id", "price", "quantity", "owner"},
}
INTEGER_PATTERN = re.compile(r"^-?[0-9]+$")
LONG_RANGE = range(-(2**63), 2**63)

SOURCE_RAW = "source -> raw"
RAW_REFINED = "raw -> refined"
REFINED_CURATED = "refined -> curated"
REFINED = "refined"

EMPTY_CURATED = {"trade_count": 0, "volume": 0, "lifecycle_filled": 0}

# The planted break: a partial write that lost one whole sweep, plus one quantity set to zero
BROKEN_SOURCE_FILE = "session_02/trades_MSFT.csv"
BROKEN_BATCH_PATTERN = "batch_id=trades_MSFT-2026-10-20-*"
DROPPED_MESSAGE = "93"
ZEROED_TRADE = ("30", "7")


@dataclass(frozen=True)
class Result:
    check: str
    hop: str
    batch: str
    expected: int
    found: int
    details: tuple = ()

    @property
    def passed(self):
        return self.expected == self.found


# One source file as it appears in every layer. Rows are tuples in FIELDS order: strings in
# source and raw, integers where refine parses them in refined. The last three fields describe
# the batch's session as a whole, which some checks need and no single batch holds.
@dataclass
class Batch:
    source_file: str
    entity: str
    trade_date: str
    instrument: str
    source: list
    raw: list
    rejected: list
    refined: list
    curated: dict = field(default_factory=dict)
    order_ids: set = field(default_factory=set)
    order_at_message: dict = field(default_factory=dict)
    key_counts: Counter = field(default_factory=Counter)


def parse_int(value):
    if value is None or not INTEGER_PATTERN.match(value):
        return None
    number = int(value)
    return number if number in LONG_RANGE else None


def row_key(entity, row):
    return row[0] if entity == "flow" else (row[0], row[2])


def describe_key(entity, key):
    return f"message {key}" if entity == "flow" else f"message {key[0]}, passive_id {key[1]}"


# Numeric order where a key part is a number, so message 93 sorts before message 100
def sort_key(key):
    parts = key if isinstance(key, tuple) else (key,)
    return tuple((0, int(part), "") if parse_int(str(part)) is not None else (1, 0, str(part))
                 for part in parts)


# Source and raw hold strings, refined holds integers; unparseable values are left out
def total_quantity(entity, rows):
    position = FIELDS[entity].index("quantity")
    numbers = [parse_int(row[position]) if isinstance(row[position], str) else row[position]
               for row in rows]
    return sum(number for number in numbers if number is not None)


# What refine should make of a raw row: integer fields parsed, text left alone
def as_refined(entity, row):
    return tuple(
        parse_int(value) if name in INTEGER_FIELDS[entity] and value is not None else value
        for name, value in zip(FIELDS[entity], row)
    )


# Raw rows that refine should have kept, i.e. every raw row not in the rejects
def accepted_raw(batch):
    rejected_keys = {row_key(batch.entity, row) for row in batch.rejected}
    return [row for row in batch.raw if row_key(batch.entity, row) not in rejected_keys]


# Row-level comparison by key: what is missing, what is extra, and which fields differ
def differences(entity, expected_rows, found_rows, expected_layer, found_layer):
    expected = {row_key(entity, row): row for row in expected_rows}
    found = {row_key(entity, row): row for row in found_rows}
    lines = []
    for key in sorted(expected.keys() | found.keys(), key=sort_key):
        where = describe_key(entity, key)
        if key not in found:
            lines.append(f"{where}: in {expected_layer}, missing from {found_layer}")
        elif key not in expected:
            lines.append(f"{where}: in {found_layer}, not in {expected_layer}")
        elif expected[key] != found[key]:
            changed = [
                f"{name} {old} in {expected_layer}, {new} in {found_layer}"
                for name, old, new in zip(FIELDS[entity], expected[key], found[key])
                if old != new
            ]
            lines.append(f"{where}: " + "; ".join(changed))
    return lines


def check_row_count(batch):
    results = [
        Result("row_count", SOURCE_RAW, batch.source_file, len(batch.source), len(batch.raw)),
        # Every raw row must leave refine as either a refined row or a reject, never vanish
        Result("row_count", RAW_REFINED, batch.source_file, len(batch.raw),
               len(batch.refined) + len(batch.rejected)),
    ]
    if batch.entity == "trades":
        results.append(Result("row_count", REFINED_CURATED, batch.source_file,
                              len(batch.refined), batch.curated["trade_count"]))
    return results


def check_traded_quantity(batch):
    if batch.entity != "trades":
        return []
    refined_total = total_quantity("trades", batch.refined)
    return [
        Result("traded_quantity", SOURCE_RAW, batch.source_file,
               total_quantity("trades", batch.source), total_quantity("trades", batch.raw)),
        Result("traded_quantity", RAW_REFINED, batch.source_file,
               total_quantity("trades", accepted_raw(batch)), refined_total),
        Result("traded_quantity", REFINED_CURATED, batch.source_file,
               refined_total, batch.curated["volume"]),
    ]


# Totals detect cheaply but can be fooled by offsetting errors; matching rows by key cannot
def check_matching_rows(batch):
    source_raw = differences(batch.entity, batch.source, batch.raw, "source", "raw")
    expected_refined = [as_refined(batch.entity, row) for row in accepted_raw(batch)]
    raw_refined = differences(batch.entity, expected_refined, batch.refined, "raw", "refined")
    return [
        Result("matching_rows", SOURCE_RAW, batch.source_file, 0, len(source_raw),
               tuple(source_raw)),
        Result("matching_rows", RAW_REFINED, batch.source_file, 0, len(raw_refined),
               tuple(raw_refined)),
    ]


def check_rejected_rows(batch):
    return [Result("rejected_rows", RAW_REFINED, batch.source_file, 0, len(batch.rejected))]


def check_positive_quantity(batch):
    position = FIELDS[batch.entity].index("quantity")
    bad = [row for row in batch.refined if row[position] is not None and row[position] <= 0]
    details = tuple(
        f"{describe_key(batch.entity, row_key(batch.entity, row))}: quantity {row[position]}"
        for row in bad
    )
    return [Result("positive_quantity", REFINED, batch.source_file, 0, len(bad), details)]


def check_order_ids(batch):
    if batch.entity != "trades":
        return []
    orphans = [row for row in batch.refined
               if row[1] not in batch.order_ids or row[2] not in batch.order_ids]
    details = tuple(
        f"{describe_key('trades', row_key('trades', row))}: aggressor {row[1]}, passive {row[2]}"
        for row in orphans
    )
    return [Result("order_ids_exist", REFINED, batch.source_file, 0, len(orphans), details)]


# The flow row at a trade's message is the order that caused it, so it must be the aggressor.
# The only check on the flow's message numbers, which source and raw agree on by construction.
def check_aggressor_at_message(batch):
    if batch.entity != "trades":
        return []
    mismatched = sorted(
        (row for row in batch.refined if batch.order_at_message.get(row[0]) != row[1]),
        key=lambda row: sort_key(row_key("trades", row)),
    )
    details = tuple(
        f"{describe_key('trades', row_key('trades', row))}: aggressor {row[1]}, "
        + (f"flow has order {batch.order_at_message[row[0]]}"
           if row[0] in batch.order_at_message else "no flow row")
        for row in mismatched
    )
    return [Result("aggressor_at_message", REFINED, batch.source_file, 0, len(mismatched),
                   details)]


# Counted across the whole session, so a row delivered again in another file is seen, and so is
# a duplicate within one file, which matching_rows cannot see because it compares rows by key
def check_natural_key_unique(batch):
    duplicated = [row for row in batch.refined
                  if batch.key_counts[row_key(batch.entity, row)] > 1]
    details = tuple(
        f"{describe_key(batch.entity, key)}: {batch.key_counts[key]} rows in the session"
        for key in sorted({row_key(batch.entity, row) for row in duplicated}, key=sort_key)
    )
    return [Result("natural_key_unique", REFINED, batch.source_file, 0, len(duplicated), details)]


# Each trade fills one aggressor and one passive order, so final fills are twice the volume
def check_lifecycle_fills(batch):
    if batch.entity != "trades":
        return []
    return [Result("lifecycle_fills", REFINED_CURATED, batch.source_file,
                   2 * total_quantity("trades", batch.refined), batch.curated["lifecycle_filled"])]


CHECKS = [
    check_row_count,
    check_traded_quantity,
    check_matching_rows,
    check_rejected_rows,
    check_positive_quantity,
    check_order_ids,
    check_aggressor_at_message,
    check_natural_key_unique,
    check_lifecycle_fills,
]


def run_checks(batches, checks=CHECKS):
    return [result for batch in batches for check in checks for result in check(batch)]


# Read with the csv module, not Spark: a reconciliation that reads through the path it is
# checking shares that path's bugs. Empty fields are compared as None, as raw stores them.
def read_source(source_file, entity):
    with open(ingest.DATA_DIR / source_file, newline="") as f:
        reader = csv.DictReader(f)
        if entity == "flow":
            return [(index, *(row[name] or None for name in FIELDS["flow"][1:]))
                    for index, row in enumerate(reader)]
        return [tuple(row[name] or None for name in FIELDS["trades"]) for row in reader]


def rows_by_source_file(frame, columns):
    grouped = {}
    for row in frame.select("source_file", *columns).collect():
        grouped.setdefault(row["source_file"], []).append(tuple(row[name] for name in columns))
    return grouped


# The newest batch per source file, chosen here rather than by refine's own function
def latest_raw_rows(frame, columns):
    rows = frame.select("source_file", "ingested_at", *columns).collect()
    newest = {}
    for row in rows:
        newest[row["source_file"]] = max(newest.get(row["source_file"], row["ingested_at"]),
                                         row["ingested_at"])
    grouped = {}
    for row in rows:
        if row["ingested_at"] == newest[row["source_file"]]:
            grouped.setdefault(row["source_file"], []).append(tuple(row[c] for c in columns))
    return grouped


def curated_totals(spark, curated_dir):
    volume = (
        spark.read.parquet(str(curated_dir / "traded_volume"))
        .groupBy("trade_date", "instrument")
        .agg(F.sum("trade_count").alias("trade_count"), F.sum("volume").alias("volume"))
        .collect()
    )
    filled = (
        spark.read.parquet(str(curated_dir / "order_lifecycle"))
        .filter(F.col("valid_to").isNull())
        .groupBy("trade_date", "instrument")
        .agg(F.sum("filled_quantity").alias("lifecycle_filled"))
        .collect()
    )
    # A session missing from curated keeps zeros, so its checks fail instead of being skipped
    totals = {}
    for row in volume + filled:
        key = (str(row["trade_date"]), row["instrument"])
        totals.setdefault(key, dict(EMPTY_CURATED))
        totals[key].update({name: row[name] for name in row.asDict() if name in EMPTY_CURATED})
    return totals


def load_batches(spark, lake_dir):
    raw, rejected, refined = {}, {}, {}
    for entity, columns in FIELDS.items():
        raw_frame = spark.read.option("mergeSchema", True).parquet(str(lake_dir / "raw" / entity))
        raw[entity] = latest_raw_rows(raw_frame, columns)
        rejects_frame = spark.read.parquet(str(lake_dir / "refined" / "rejects" / entity))
        rejected[entity] = rows_by_source_file(rejects_frame, columns)
        refined_frame = spark.read.parquet(str(lake_dir / "refined" / entity))
        refined[entity] = rows_by_source_file(refined_frame, columns)
    curated = curated_totals(spark, lake_dir / "curated")

    batches = []
    for entry in ingest.read_manifest():
        source_file, trade_date = entry["source_file"], entry["session_date"]
        entity, instrument = ingest.parse_source_name(Path(source_file).name)
        batches.append(Batch(
            source_file, entity, trade_date, instrument,
            source=read_source(source_file, entity),
            raw=raw[entity].get(source_file, []),
            rejected=rejected[entity].get(source_file, []),
            refined=refined[entity].get(source_file, []),
            curated=curated.get((trade_date, instrument), dict(EMPTY_CURATED)),
        ))

    # A trade's order ids must be orders added in the same session's flow
    flows = {(batch.trade_date, batch.instrument): batch.refined
             for batch in batches if batch.entity == "flow"}
    key_counts = {}
    for batch in batches:
        session = (batch.trade_date, batch.instrument)
        key_counts.setdefault((batch.entity, *session), Counter()).update(
            row_key(batch.entity, row) for row in batch.refined
        )
        if batch.entity == "trades":
            flow = flows.get(session, [])
            batch.order_ids = {row[2] for row in flow if row[1] == "add"}
            batch.order_at_message = {row[0]: row[2] for row in flow}
    for batch in batches:
        batch.key_counts = key_counts[(batch.entity, batch.trade_date, batch.instrument)]
    return batches


# Rewrites the batch's data files but leaves its _SUCCESS marker in place, so to ingest the
# batch still looks committed. _SUCCESS proves a write finished, not that what it wrote is right.
def damage_batch(spark, raw_dir):
    [batch_dir] = (raw_dir / "trades").glob(BROKEN_BATCH_PATTERN)
    message, passive_id = ZEROED_TRADE
    zeroed = (F.col("message") == message) & (F.col("passive_id") == passive_id)
    damaged = (
        spark.read.parquet(str(batch_dir))
        .filter(F.col("message") != DROPPED_MESSAGE)
        .withColumn("quantity", F.when(zeroed, F.lit("0")).otherwise(F.col("quantity")))
    )
    # Spark cannot overwrite files it is still reading, so write elsewhere and swap the data files
    staging = raw_dir.parent / "_damage_staging"
    damaged.coalesce(1).write.mode("overwrite").parquet(str(staging))
    for old in batch_dir.glob("*part-*"):
        old.unlink()
    for new in staging.glob("*part-*"):
        new.rename(batch_dir / new.name)
    shutil.rmtree(staging)


def refine_and_curate(spark, lake_dir):
    for entity in refine.INTEGER_COLUMNS:
        refine.refine_entity(spark, entity, lake_dir / "raw", lake_dir / "refined")
    curate.curate_lake(spark, lake_dir / "refined", lake_dir / "curated")


def build_broken_lake(spark, lake_dir):
    # Generated from nothing on every run, so the broken lake is the same each time
    shutil.rmtree(lake_dir, ignore_errors=True)
    for entry in ingest.read_manifest():
        ingest.ingest_file(spark, entry["source_file"], entry["session_date"], lake_dir / "raw")
    damage_batch(spark, lake_dir / "raw")
    refine_and_curate(spark, lake_dir)


PLANTED_BREAK = f"""The break was planted by `python src/reconcile.py --plant-break`, which builds
`lake_broken/` from the same source files and then damages one raw batch,
`{BROKEN_SOURCE_FILE}` for 2026-10-20, before refine and curate run:

- the three fills of message {DROPPED_MESSAGE} are dropped (a partial write losing one sweep);
- the quantity of the trade at message {ZEROED_TRADE[0]}, passive_id {ZEROED_TRADE[1]}, is set to 0;
- the batch's `_SUCCESS` marker is left in place, so the batch still looks committed.

Everything below is what the checks found, without being told what was planted."""


def report(results, title, lake_name, preamble=""):
    failed = [result for result in results if not result.passed]
    lines = [f"# {title}", "",
             f"Lake: `{lake_name}`. Source: the CSV files in `data/`, read with Python's `csv`",
             "module, not Spark. One row per batch per hop, so a break shows where it entered.", ""]
    if preamble:
        lines += [preamble, ""]

    lines += ["## Summary", "", "| Check | Hop | Batches | Failed |", "|---|---|---|---|"]
    groups = {}
    for result in results:
        groups.setdefault((result.check, result.hop), []).append(result)
    checks = list(dict.fromkeys(result.check for result in results))
    hops = [SOURCE_RAW, RAW_REFINED, REFINED_CURATED, REFINED]
    for check, hop in sorted(groups, key=lambda key: (checks.index(key[0]), hops.index(key[1]))):
        group = groups[(check, hop)]
        lines.append(f"| {check} | {hop} | {len(group)} | "
                     f"{sum(not r.passed for r in group)} |")

    lines += ["", "## Breaks", ""]
    if not failed:
        lines += ["No breaks.", ""]
    for result in failed:
        lines += [f"### {result.check}, {result.hop}: `{result.batch}`", "",
                  f"Expected {result.expected}, found {result.found}.", ""]
        lines += [f"- {detail}" for detail in result.details]
        if result.details:
            lines.append("")

    lines += ["## Every check", "", "| Check | Hop | Batch | Expected | Found | Result |",
              "|---|---|---|---|---|---|"]
    lines += [f"| {r.check} | {r.hop} | `{r.batch}` | {r.expected} | {r.found} | "
              f"{'pass' if r.passed else 'FAIL'} |" for r in results]
    return "\n".join(lines) + "\n"


def write_report(text, name):
    RESULTS_DIR.mkdir(exist_ok=True)
    # newline="\n": byte-identical on every platform, so a re-run can be diffed against Git
    with open(RESULTS_DIR / name, "w", newline="\n") as f:
        f.write(text)


def main():
    parser = argparse.ArgumentParser(description="Reconcile the lake against the source CSVs.")
    parser.add_argument("--plant-break", action="store_true",
                        help="build lake_broken/ with one damaged raw batch and reconcile that")
    args = parser.parse_args()

    spark = SparkSession.builder.master("local[*]").appName("reconcile").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    if args.plant_break:
        build_broken_lake(spark, BROKEN_LAKE_DIR)
        results = run_checks(load_batches(spark, BROKEN_LAKE_DIR))
        text = report(results, "Reconciliation: planted break", "lake_broken/", PLANTED_BREAK)
        write_report(text, "reconciliation_break.md")
    else:
        results = run_checks(load_batches(spark, LAKE_DIR))
        write_report(report(results, "Reconciliation: clean run", "lake/"),
                     "reconciliation_clean.md")
    spark.stop()

    failures = sum(not result.passed for result in results)
    print(f"{len(results)} checks, {failures} failed")
    # A clean run must have no failures; a planted-break run must have at least one
    sys.exit(int(failures == 0) if args.plant_break else int(failures > 0))


if __name__ == "__main__":
    main()
