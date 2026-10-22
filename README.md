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

Adding a nullable column is safe here because nothing downstream depends on it. Renaming and
retyping are not. I ran both against a copy of `data/` and pushed it through the whole
pipeline and the reconciliation; the figures below come from those runs. Every experiment in
this section and in section 7 is a test in `tests/test_experiments.py` that asserts these
figures, so the README cannot drift from what the code does.

### Renaming a column

Experiment: session 03's AAPL trades file with `quantity` renamed to `qty`.

- **Ingest accepts it silently.** Raw reads columns by header name and does not check the
  header against an expected list, so the batch lands with a `qty` column and no `quantity`.
- **Refine produces nulls, not rejects.** Reading all batches with `mergeSchema` gives both
  columns: older batches have `qty` null, the renamed batch has `quantity` null. Refine treats a
  null as an empty field, not a parse failure (a cancel's price is legitimately empty), so all
  1,183 trades reached refined with a null quantity and 0 rejects.
- **Curated is wrong without an error.** That session's `traded_volume` has a null volume
  beside a trade count of 1,183.
- **Reconciliation stops rather than reports.** It reads the source file by column name and
  raised `KeyError: 'quantity'`. That halts the run loudly, but only after the lake has been
  written, and it produces no report saying which batch broke.

How I would handle it in production (not built here):

- Check each file's header against a contract at ingest, and quarantine a file that is missing
  a required column instead of landing it.
- Give refine nullability rules per column. A trade's quantity is required; a flow row's price
  may be null only for a cancel or a market order. Today a required column that is entirely
  null passes.
- Map renames explicitly, keyed by a schema version recorded per file in the manifest, and apply
  the mapping in refine. Raw keeps the file as written, and because refine is rebuilt from raw on
  every run, adding a mapping corrects every affected batch on the next run.

### Changing a column's type

Experiment: session 03's GOOG trades with prices written as decimal dollars (`1000.02`) instead
of integer ticks (`100002`).

- **Raw absorbs it,** because every raw column is a string. That matters: a raw layer with
  inferred types would not even be readable. A Parquet table with `price` as a long in one batch
  and a double in another made Spark's `mergeSchema` read fail outright (tested separately).
- **Refine rejects the whole batch.** `1000.02` fails the integer check, so all 1,178 trades went
  to `rejects/` and that session-instrument vanished from refined and curated.
- **Reconciliation catches it,** but with only one check: `rejected_rows` failed with 1,178. The
  refined-to-curated checks for that batch pass, because both sides are empty.

A type change that still parses is worse. Experiment: session 03's MSFT prices multiplied by 100,
still integers. Every check passed. Source and lake agree, and every value parses, so a
source-to-output reconciliation has nothing to compare against; only knowing what a price should
look like would catch it.

How I would handle it in production (not built here):

- Version the parser: record the schema version per file in the manifest and have refine convert
  each version to the canonical type, for example decimal dollars to integer ticks, failing if
  the conversion is not exact. Raw already holds the batch, so no re-ingest is needed.
- Make the producer declare units in the contract, and add plausibility checks a reconciliation
  cannot provide, such as prices within a band of the previous session's.

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
| `aggressor_at_message` | each trade's aggressor is the flow's order at that trade's message |
| `natural_key_unique` | each natural key appears once in refined across every batch of a session |
| `lifecycle_fills` | final fills in the order lifecycle are twice the traded volume |

`aggressor_at_message` is the only check on the flow's message numbers. Those are row positions,
so source and raw agree on them by construction. `natural_key_unique` is the only check that
looks across batches: every other check compares one source file with what became of it.

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
what places the break in the raw batch rather than in a transform. The other 185 checks pass.

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
| `aggressor_at_message` | a trade whose message holds a different order in the flow |
| `natural_key_unique` | a line the engine wrote twice, which `matching_rows` sees as one row |
| `traded_quantity` | curated volume disagreeing with refined |
| `row_count` | curated trade count disagreeing with refined |
| `lifecycle_fills` | lifecycle fills not equal to twice the traded volume |

Each row is a test that asserts exactly one check fails.

## 7. Late and out-of-order data: what breaks, and what I would change

The session date is assigned per batch from the manifest, not from the ingest date. Using the
ingest date would conflate event time with processing time: a session replayed or delivered a
day late would land in the wrong partition. That is the bug this section is about.

### The concrete case: two trades with their sequence numbers swapped

Experiment: in session 01's AAPL trades, the trades at messages 1 and 3 had their message numbers
exchanged (`1,2,1,99987,16` became `3,2,1,99987,16`, and `3,4,3,100002,63` became
`1,4,3,100002,63`), then the copy went through the whole pipeline.

What broke:

- **Event time.** `event_ts` is derived from the message number, so each trade is stamped with
  the other's time.
- **Validity bounds.** A fill opens a new version of its order at the fill's message, so the
  versions now open at the wrong moments, in the wrong order. One trade now comes before its own
  orders were added. Measured: the lifecycle lost 142 units of fill quantity against the trades.
  Traced through the code, not measured: `order_versions` skips any event for an order it has
  not seen added, so 126 of those units are fills that now precede their orders' adds, leaving
  those orders open with quantity they no longer have; the other 16 are a fill landing on a
  market order that had already finished, which opens no new version.
- **Running aggregates.** Anything accumulated in message order, such as an order's remaining
  quantity, is computed in the wrong order. The 5-minute volume happened to be unaffected,
  because both trades stayed in the same interval; a swap across an interval boundary would
  move volume between intervals (reasoning, not tested).

What caught it: `aggressor_at_message`, which names both trades (the trade now at message 1 has
aggressor 4, but the flow has order 2 there; the one at message 3 has aggressor 2, but the flow
has order 4), and `lifecycle_fills`, expected 58,996 and found 58,854. Row counts, totals and
row-level matching all passed, because the source file itself carried the swap, so source and
lake agree. Before `aggressor_at_message` moved from the test suite into `reconcile.py`,
`lifecycle_fills` was the only check that failed, and it reports a total, not which rows.

The flow is more fragile than the trades. Its sequence number is not a column but the row's
position, so swapping two flow rows (session 02, AAPL, messages 11 and 17) swaps their message
numbers too. Source and raw still agree, because both number rows by position. Both messages
caused a trade, so `aggressor_at_message` fails for both (aggressors 9 and 14, each now at the
other's message), and `lifecycle_fills` found 59,646 against an expected 59,660. A swap of two
flow rows that caused no trade would pass `aggressor_at_message`, which can check only the
flow rows that caused a trade (reasoning, not tested).

### What I would change, against what the pipeline already does

**Order by sequence number within each key.** Already done for anything derived from the
trades. The lifecycle sorts each order's events by message, not by arrival, and volume is an
order-independent sum. Tested: with every row of a trades file shuffled, that session's 5,577
lifecycle versions and 17 volume intervals were identical to the clean run, and all 189 checks
passed. Arrival order within a file does not matter. What it cannot fix is a wrong sequence
number, as above, and the flow has no sequence number of its own. The fix there is upstream:
the engine should write the message number as a column, which would let a reconciliation check
every flow row, not only those that caused a trade.

**Hold late arrivals until a watermark passes.** Not done, and not needed in the current design
(reasoning, not tested): every run rebuilds refined and curated from all of raw, so a late file
for an old session is simply included next time, in the right partition, because its date comes
from the manifest. The cost is that nothing is ever final: a curated figure can change after it
was published, without notice, and the full rebuild does not scale. An incremental design would
need a rule for when a session is complete, and the engine emits no end-of-session marker. A
late flow file would also break the positional message numbering, which restarts at 0 in every
file.

**Make the write idempotent so a replay does not double count.** Half done. Ingest is
idempotent by content hash: re-running it on unchanged files skipped all 18 batches (tested).
That protects against an exact replay. It does not protect against the same data delivered with
different bytes. Experiment: session 01's GOOG trades delivered again with CRLF line endings in a
new folder, listed in the manifest with the same date. The hash differed, so it became a second
batch; refine keeps the newest batch per source file, and the paths differ, so both were kept.
Refined and curated doubled, from 1,247 trades to 2,494.

`natural_key_unique` names it: both batches fail, each with all 1,247 of its trades also present
in the other. The refined-to-curated checks fail for both files too, but only by accident:
curated totals are per session and instrument, while each batch expects only its own rows.
Before `natural_key_unique` existed, those accidental failures were the only signal. Every
source-to-raw and raw-to-refined check passed, because each file on its own was carried
faithfully.

The check detects the duplicate; it does not remove it. The fix I would make, not built here,
is to deduplicate in refine on the natural key, `(trade_date, instrument, message, passive_id)`
for trades, with an explicit rule for conflicting duplicates.

Refine and curate overwrite their outputs in full, so re-running them cannot double count:
two builds from scratch produced byte-identical reconciliation reports.

## 8. Limitations

- The order lifecycle cannot see self-trade prevention: 266 limit orders appear to rest after
  the engine removed them (section 5).
- Reconciliation collects each batch's rows into Python to compare them. That is fine at a few
  thousand rows per batch; at scale the totals would run first in Spark and the row-level
  comparison would be a keyed join.
- A unit change that still parses, such as prices scaled by 100, passes every check (section 4).
- The flow's sequence number is its row position, so swapped or late flow rows get the wrong
  message numbers. `aggressor_at_message` catches this only for flow rows that caused a trade
  (section 7).
- A redelivery with different bytes is double counted. `natural_key_unique` reports it, but
  nothing deduplicates on the natural key (section 7).
- Venue is a per-run label, not a property of each trade.
- Each source file holds exactly one instrument and one session.
- Local Spark only: no cluster, no orchestration, no streaming, no cloud storage.
