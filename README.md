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
python src/curate.py
python src/reconcile.py                 # writes results/reconciliation_clean.md
python src/reconcile.py --plant-break   # writes results/reconciliation_break.md
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
| raw | source as written, plus what cannot be recovered later | once per file, never rewritten |
| refined | typed, validated rows with derived columns | rebuilt from raw on every run |
| curated | traded volume per interval, order lifecycle | rebuilt from refined |

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
nothing depends on the zone. That setting caught a real trap. `F.lit(datetime(...))` in
PySpark makes a zoned timestamp, and comparing it with an NTZ column shifts it by the session
zone, so an as-of query for 09:30:40 silently asked about a different moment. Literals compared
with NTZ columns are built with `to_timestamp_ntz` from the wall-clock string instead.

### Curated: volume and VWAP

`traded_volume` holds trade count, volume and notional (the sum of price × quantity, exact in
integer ticks) per instrument per 5-minute interval, bucketed with `window`. Intervals with no
trades produce no row rather than a zero row.

It stores notional and volume, not VWAP. VWAP is not additive across intervals: the VWAP of an
hour is not the average of its twelve 5-minute VWAPs, so storing only the ratio makes every
longer-period rollup wrong. The two totals sum correctly over any period, and VWAP is
`sum(notional) / sum(volume)` over whatever intervals a query selects. A test checks that the
interval totals re-aggregate to each whole session.

## 3. Partitioning: the choice and the trade-off

Refined and curated tables are partitioned `trade_date=/instrument=`, one file per partition
(rows are repartitioned by the partition columns before the write, so each partition gets one
file rather than one per task).

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

`order_lifecycle` keeps every order's history as versions, one row per version, each with
stored validity bounds. An as-of query asks which version was valid at a moment *t*. Computing
bounds at query time with `lead()` was rejected: the spec asks for records that carry their
bounds, and bounds computed per query do not exist in the data.

Each version holds the order's state (`placed`, `partially_filled`, `filled`, `cancelled`), its
price, remaining and filled quantity, and `[valid_from, valid_to)`. Order 22 (AAPL, 2026-10-19)
rests at message 36, is filled by a market order at message 46, and is the target of a cancel at
message 62:

| state | remaining | filled | valid_from | valid_to |
|---|---|---|---|---|
| placed | 15 | 0 | 09:30:36 | 09:30:46 |
| filled | 0 | 15 | 09:30:46 | null |

The cancel produces nothing: the order was already filled, so the engine rejected it.

### The rules

- **Half-open bounds.** A version closes when the order's next version opens, so two adjacent
  versions never both match the same moment.
- **Terminal states get a final row, open-ended.** "Filled at 10:00" returns a row, so filled is
  distinguishable from never existed.
- **Orders still resting at session end stay open.** The engine has no session close, and
  closing them with an invented "expired" state would fabricate a transition. Order ids restart
  each session, so the key is `(trade_date, instrument, order_id)`.
- **Open intervals are null, not `9999-12-31`.** The sentinel would make queries simpler but
  stores a date that is not true. The accepted cost is that every as-of query is written
  `valid_from <= t AND (valid_to IS NULL OR t < valid_to)`.
- **One version per order per message** that changes its state, price or remaining quantity.
  When a market order sweeps three resting orders, its three fills share one message and make one
  version, not three. This is correct because time resolution equals message resolution: one
  message is one second, so versions inside a message would have zero-length intervals that no
  query can return. With sub-message timestamps, one version per fill would be right.
- **A cancel or modify takes effect only if the order is still resting** by its own fills. The
  engine rejects the rest and emits nothing, so the rule is inferred from what the outputs show.

The state machine is a plain Python function over one order's sorted messages, because a modify
resets the remaining quantity rather than subtracting from it, and whether a request applies
depends on the state at that moment. Spark groups each order's events; the function replays them.

### What the outputs cannot show

The lifecycle is inferred from the flow and the trades, and neither records what the engine
refused or removed. Self-trade prevention discards an incoming order's remainder without a trace
in either file. Replaying all nine flows through the order book's independent reference matcher
measured the gap: 266 limit orders (1.2% of 22,725 limit adds) were removed that way, 223 on
arrival and 43 when a modify re-entered them. The lifecycle shows them still resting. Downstream,
130 cancels and 64 modifies sent to those orders, which the engine rejected, are treated as
applied.

Everywhere else the inference is exact. Comparing each of the 27,130 orders' version history
with the reference book's state after every message: 26,864 are identical, and all 266 that
differ are self-trade-prevention orders. None differ for any other reason.

The production fix is for the engine to publish order events (accepted, rested, cancelled,
rejected, removed by self-trade prevention), so the lifecycle reads facts rather than inferring
state from side effects. It was scoped out of this project.

## 6. Reconciliation: the checks, and the break it caught

`reconcile.py` compares every source file with what each layer made of it, one result per batch
per hop (source to raw, raw to refined, refined to curated), so a break shows where it entered.

The source side is read with Python's `csv` module, not Spark. A reconciliation that reads
through the path it is checking shares that path's bugs: if Spark's CSV reader or the ingest
dropped a row, a check that also read the CSV through Spark would agree with it.

| Check | Compares |
|---|---|
| `row_count` | source vs raw; raw vs refined plus rejects; refined vs curated trade count |
| `traded_quantity` | total traded quantity at every hop |
| `matching_rows` | every row by key, `(message, passive_id)` for trades, `message` for flow |
| `rejected_rows` | rows refine could not parse, which must be zero |
| `positive_quantity` | no zero or negative quantities in refined |
| `order_ids_exist` | each trade's aggressor and passive order was added in that session's flow |
| `lifecycle_fills` | final fills in the order lifecycle are twice the traded volume |

### Totals detect, rows explain

Counts and sums are cheap, and in production they are often all an upstream system provides,
but offsetting errors defeat them: one quantity inflated by 2 and another deflated by 2 leaves
both the row count and the total unchanged. Comparing rows by key catches that, and also says
exactly which rows are missing, extra or different. A test plants that offsetting error and
asserts that every total passes and only the row-level check fails.

### The planted break

A break has to enter between source and output to be a reconciliation break. Corrupting a
source file would not show one: the pipeline would carry the corrupted file faithfully, source
and lake would agree, and only the validity checks could notice.

So `python src/reconcile.py --plant-break` builds a separate lake, `lake_broken/`, from the same
source files, and damages one raw batch after ingest and before refine. Nothing corrupted is
committed to `data/`, and the pipeline stages contain no test hooks. Tampering with stored data
is less common in practice than a transform bug, but it stands in for a partial write or a bad
reprocess, and what matters is that the damage lands between source and output.

The damage, to `session_02/trades_MSFT.csv` (2026-10-20):

- the three fills of message 93 are dropped: a market sell for 44 that swept three bids;
- the quantity of the trade at message 30, passive_id 7, is set from 38 to 0;
- the batch's `_SUCCESS` marker is left in place. To ingest, the batch still looks committed:
  `_SUCCESS` proves a write finished, not that what it wrote is right.

What the checks found, without being told what was planted
([full report](results/reconciliation_break.md)):

| Check | Hop | Expected | Found |
|---|---|---|---|
| `row_count` | source to raw | 1,187 | 1,184 |
| `traded_quantity` | source to raw | 30,505 | 30,423 |
| `matching_rows` | source to raw | 0 | 4: message 93's three fills missing, quantity 38 now 0 |
| `positive_quantity` | refined | 0 | 1 |

Every hop after raw passes, because refine and curate carried the damage faithfully. That is
what places the break in the raw batch rather than in a transform. The other 158 checks pass.

### Reproducing it

Both reports in `results/` contain no timestamps or run-specific values, so a re-run produces
byte-identical files. CI runs the whole pipeline, both reconciliations, and
`git diff --exit-code results/`: if a fresh run does not reproduce the committed reports, the
build fails. A test also plants the same break on every push and asserts the exact four
failures and the four row-level differences.

### Are the checks load-bearing?

I disabled each check in turn and re-ran the planted break. No single disabled check lets it
through: the dropped rows are seen by `row_count`, `traded_quantity` and `matching_rows`, and the
zeroed quantity by `traded_quantity`, `matching_rows` and `positive_quantity`. The smallest set of
disabled checks that lets the whole break pass undetected is those four together.

The planted break therefore cannot show that each check matters on its own. What can is a
failure that only one check catches, and there is one for every check:

| Check | Failure no other check catches |
|---|---|
| `matching_rows` | offsetting errors: one quantity +2, another -2 |
| `positive_quantity` | a zero quantity emitted by the engine itself, so source and lake agree |
| `rejected_rows` | an unparseable value emitted by the engine itself |
| `order_ids_exist` | a trade against an order that was never added |
| `traded_quantity` | curated volume disagreeing with refined |
| `row_count` | curated trade count disagreeing with refined |
| `lifecycle_fills` | lifecycle fills not equal to twice the traded volume |

Each row is a test that asserts exactly one check fails.

## 7. Late and out-of-order data: what breaks, and what I would change

The session date is assigned per batch from the manifest, not from the ingest date. Using the
ingest date would conflate event time with processing time: a session replayed or delivered a
day late would land in the wrong partition. That is the bug this section is about.

The rest of this section is not yet written.

## 8. Limitations

- The order lifecycle cannot see self-trade prevention: 266 limit orders appear to rest after
  the engine removed them (section 5).
- Reconciliation collects each batch's rows into Python to compare them. That is fine at a few
  thousand rows per batch; at scale the totals would run first in Spark and the row-level
  comparison would be a keyed join.
- Venue is a per-run label, not a property of each trade.
- Each source file holds exactly one instrument and one session.
- Local Spark only: no cluster, no orchestration, no streaming, no cloud storage.
