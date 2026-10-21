# Trade Pipeline

A local PySpark pipeline over the trade log and order flow produced by my C++ order book
engine ([orderbook](https://github.com/mbodaci/orderbook)). One system generates the data;
this one lands it, types it, and checks it.

## Running it

Spark runs under WSL2 (Ubuntu 22.04) with Python 3.10 and Java 17. Native Windows lacks
Hadoop's `winutils.exe` and `hadoop.dll`, which Spark needs to list and write local files.

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pytest -v
python src/ingest.py
python src/refine.py
```

## 1. What it does, and where the data comes from

The engine is single-instrument and has no clock, so each run produces two CSV files for one
instrument and one session:

- `flow_<TICKER>.csv`: the order messages fed to the engine,
  `type,id,side,price,quantity,order_type,owner`. 5,000 messages per session.
- `trades_<TICKER>.csv`: the trades it produced, `message,aggressor_id,passive_id,price,quantity`.
  `message` is the 0-based index of the flow message that caused the trade.

`data/` holds three sessions of AAPL, MSFT and GOOG, each reproducible from a recorded seed:

| Session | Date | AAPL | MSFT | GOOG |
|---|---|---|---|---|
| `session_01` | 2026-10-19 | 1 | 2 | 3 |
| `session_02` | 2026-10-20 | 11 | 12 | 13 |
| `session_03` | 2026-10-21 | 21 | 22 | 23 |

Two things the engine does not emit, which the pipeline derives:

- **Instrument** comes from the filename. It is provenance, captured at ingest.
- **Time** comes from `message`: message *n* of a session is stamped 09:30:00 + *n* seconds,
  derived in refine. A session of 5,000 messages runs from 09:30:00 to 10:53:19.

The **session date** is metadata, not data, so it lives in `data/manifest.csv`, which maps every
source file to its date. The assignment is versioned in Git, and a full multi-day load is one
command. A file not in the manifest is reported and skipped, because it has no date.

## 2. The layers, and why they are split that way

| Layer | Holds | Written |
|---|---|---|
| raw | the source as written, plus what cannot be recovered later | once per source file, never rewritten |
| refined | typed, validated rows with derived columns | rebuilt from raw on every run |
| curated | aggregates for use | rebuilt from refined |

### Raw holds only what cannot be recovered later

Raw keeps the source text as written (every column a string), the flow's message number, and
provenance: instrument, session date, source file, batch id, ingest time. Anything that can be
derived from those belongs in refine.

The principle came out of a bug, not a whiteboard. `event_ts` was first derived at ingest. When
I changed its type from a zoned timestamp to `TIMESTAMP_NTZ` (below), the ingest code changed
but the source files did not, so every batch id stayed the same. On an existing lake the next
ingest would have skipped all 18 batches as already ingested and kept the old timestamps:
silently stale. A derived column in immutable storage cannot be corrected when its derivation
changes. Moving the derivation to refine fixes that, because refine is rebuilt on every run.

The flow's message number stays in raw for the opposite reason. The flow has no sequence
column: a trade's `message` is the flow row's position in the file. Parquet does not preserve
row order, so the position has to be captured while the file is being read, or it is lost.
Ingest numbers lines with `zipWithIndex`, which follows file order across partitions (checked
with the file forced into 8 partitions). `monotonically_increasing_id` looks similar but is not
contiguous across partitions, so it cannot serve as a message number.

### Batch ids, idempotency, and why line endings are a correctness concern

Each source file becomes one raw batch, `batch_id=<file>-<session date>-<sha256 prefix>`. The
hash is of the file's bytes, so re-running ingest on an unchanged file finds the committed batch
(Spark's `_SUCCESS` marker) and skips it. A half-written batch has no marker and is rewritten.
The date is part of the id so that correcting it in the manifest creates a new batch instead of
being skipped.

Because the id hashes bytes, line endings are part of the data's identity. With
`core.autocrlf` on, a Windows checkout would give the CSVs CRLF endings and a Linux checkout LF,
so the same data would get different batch ids on different machines. `.gitattributes` forces
LF everywhere. This is a correctness fix, not style, and it mattered in practice: flow files
written by Python's `csv` module, which always writes `\r\n`, sat on disk with CRLF while Git
stored them as LF, so ingest was hashing bytes no fresh clone would have.

### Refine: newest batch, and parsing that cannot be fooled

Raw is immutable, so a corrected date or a regenerated file leaves the old batch in place.
Refine keeps only the batch with the newest `ingested_at` per source file.

A successful integer conversion is not proof of parsing. Spark 3.5 (with ANSI mode off, the
default) casts the string `"1.5"` to the integer `1` without complaint. A check that only looks
for null after the cast would pass a corrupted quantity. Refine instead treats a present value
as unparseable if it does not match `^-?[0-9]+$` or overflows a long. Unparseable rows go to
`lake/refined/rejects/<entity>/` with their raw strings kept as evidence, not dropped.

### Time without a time zone

`event_ts` is `TIMESTAMP_NTZ`. The engine has no clock and no zone, so a zoned timestamp would
invent information the data does not have: the same instant would display as 09:30 in UTC and
05:30 in New York. I tested the bucketing functions the curated layer needs in PySpark 3.5.9:

- `window(event_ts, "5 minutes")` works on NTZ, returns NTZ bounds, and gives identical buckets
  under UTC, America/New_York and Asia/Tokyo session zones.
- `date_trunc` works but returns a zoned `TIMESTAMP`, so it quietly reintroduces the zone. Cast
  back with `.cast("timestamp_ntz")` it is again identical across zones. Curated uses `window`,
  or `date_trunc` with that cast.
- NTZ survives a Parquet round trip as `TimestampNTZType`.

The tests run with the session zone set to America/New_York on purpose: they pass only if
nothing depends on the zone.

## 3. Partitioning: the choice and the trade-off

Refined tables are partitioned `trade_date=/instrument=`, one file per partition (rows are
repartitioned by the partition columns before the write, so each partition gets one file rather
than one per task).

At this data size that is over-partitioned. Each session-instrument partition holds roughly
1,100 to 1,250 trades, and every file is small. Past a few hundred instruments I would move to
partitioning by date only, sorted by instrument within each file, and rely on Parquet's per-file
min/max statistics to skip data for instrument filters: far fewer files, at the cost of coarser
skipping.

Date-then-instrument versus instrument-then-date is not about query pruning: Spark prunes by
partition column regardless of directory order. It is about what a reload or delete touches.
Reprocessing or expiring one day is one directory under date-first, and one directory per
instrument under instrument-first.

## 4. Schema evolution: what is safe, what is not

Session 03's trade logs carry an extra nullable column, `venue`, emitted by the engine's
`--venue` flag. Raw reads columns by header name, so the new column is kept, and refine reads
all batches with `mergeSchema`, so sessions 01 and 02 read it as null.

The venue is constant per file because it labels the engine run, not each trade. It demonstrates
a nullable column arriving in a later batch; it is not multi-venue matching.

The rest of this section, on renames and type changes, is written with the schema evolution
stage.

## 5. Temporal modelling: how history is kept

Not yet written.

## 6. Reconciliation: the checks, and the break it caught

Not yet written.

## 7. Late and out-of-order data: what breaks, and what I would change

The session date is assigned per batch from the manifest, not from the ingest date. Using the
ingest date would conflate event time with processing time: a session replayed or delivered a
day late would land in the wrong partition. That is the bug this section is about.

The rest of this section is written with the curated layer.

## 8. Limitations

- Venue is a per-run label, not a property of each trade.
- Each source file holds exactly one instrument and one session.
- Local Spark only: no cluster, no orchestration, no streaming, no cloud storage.
