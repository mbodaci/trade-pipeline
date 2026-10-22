# Interview guide

This guide is for defending the project out loud. It walks each stage as the code is written,
then each design decision with its alternatives, the question it invites, and an answer
grounded in what the code actually does.

- **Where the numbers come from.** Row counts, check counts and the planted break are copied
  from the committed reports, [reconciliation_clean.md](../results/reconciliation_clean.md) and
  [reconciliation_break.md](../results/reconciliation_break.md). Experiment figures are asserted
  in [test_experiments.py](../tests/test_experiments.py). The lifecycle comparison and the
  self-trade-prevention figures are from README section 5. Test and CI times are from runs on
  2026-10-22. Anything else is labelled as reasoning.
- **What is not here.** File sizes, Spark job timings per stage, and any performance figure at
  larger scale were never measured, so they are left out. Section 7 lists what not to claim.

## 1. The 60-second summary

A local PySpark pipeline over the output of my own C++ order book engine. One system generates
the data; this one lands it, types it, models its history, and proves it carried it faithfully.

- **The data:** three sessions of three instruments. Each engine run writes the order flow it
  was fed, 5,000 messages, and the trades it produced. The engine has no clock and no instrument
  field, so the pipeline derives both: the instrument from the filename at ingest, and time from
  the message number in refine.
- **The layers:** raw keeps every file as written, as strings, under a batch id that is a hash
  of the file's bytes, so re-running is idempotent. Refined parses and validates, keeps the
  rejects, and partitions by date and instrument. Curated holds traded volume per 5-minute
  interval and an order lifecycle where every version of every order carries its own validity
  bounds, so you can ask what an order looked like at any past moment.
- **The point of it:** reconciliation. Nine checks compare each source file with every layer,
  per batch and per hop, reading the source with Python's `csv` module rather than through Spark.
  On the clean lake all 189 pass. On a lake where I damaged one raw batch, 4 fail. Every hop
  after raw passes, which places the break in the raw batch, and the row-level report names
  the exact missing and altered rows.
- **What breaks:** I ran the schema changes and ordering failures from the spec against copies of
  the data. Each experiment is now a test asserting what broke and which check caught it, and
  the README writes up what the pipeline cannot catch.

## 2. Numbers worth knowing

**Input and layers.** Every figure here is from the committed clean report, where source, raw,
refined and curated agree for every batch.

| Session | Date | AAPL trades | GOOG trades | MSFT trades | Flow rows per instrument |
|---|---|---|---|---|---|
| `session_01` | 2026-10-19 | 1,149 | 1,247 | 1,149 | 5,000 |
| `session_02` | 2026-10-20 | 1,156 | 1,187 | 1,187 | 5,000 |
| `session_03` | 2026-10-21 | 1,183 | 1,178 | 1,109 | 5,000 |

- **Totals:** 18 source files and 18 raw batches. 10,545 trades and 45,000 flow rows in refined,
  with 0 rejects.
- **Traded quantity per batch:** ranges from 29,181 (session 03 GOOG) to 32,573 (session 01
  GOOG). Every batch has its own figure in the clean report.
- **Curated:**
  - `traded_volume`: 153 rows, 17 five-minute intervals per session-instrument.
  - `order_lifecycle`: 50,122 versions across 27,130 orders. Per session-instrument the
    versions range from 5,517 (session 03 MSFT) to 5,625 (session 01 GOOG); session 01 AAPL
    has 5,577.
- **Hand-checked values in the tests:**
  - AAPL on 2026-10-19, first interval: 56 trades, volume 1,461, notional 146,098,057.
  - AAPL on 2026-10-19, whole session: notional 2,949,798,493 over volume 29,498.
  - Message 46 of that session: the market buy that swept three asks, stamped 09:30:46.

**Reconciliation.**

- **Check count:** 9 checks, giving 189 results on the clean lake. Trades batches get 14 each
  (126 in total) and flow batches get 7 each (63). All 189 pass.
- **The planted break:** 4 of 189 fail, all on `session_02/trades_MSFT.csv` and all but one at
  the source-to-raw hop:
  - `row_count`: 1,187 expected, 1,184 found. Message 93's three fills were dropped.
  - `traded_quantity`: 30,505 expected, 30,423 found. That is 44 dropped plus 38 zeroed.
  - `matching_rows`: 4 differences, naming the three missing fills and the zeroed quantity.
  - `positive_quantity` (refined): 1.

  The other 185 pass.

**The lifecycle's known gap** (README section 5, measured by replaying all nine flows through
the order book's reference matcher):

- 266 limit orders were removed by self-trade prevention: 1.2% of 22,725 limit adds. 223 were
  removed on arrival and 43 when a modify re-entered them. The lifecycle shows them still
  resting.
- 130 cancels and 64 modifies sent to those orders, which the engine rejected, are treated as
  applied.
- Of 27,130 orders, 26,864 have version histories identical to the reference. All 266 that
  differ are self-trade-prevention orders.

**Experiment figures** (asserted in `test_experiments.py`):

| Experiment | Result |
|---|---|
| Rename `quantity` to `qty` (session 03 AAPL) | 1,183 trades in refined with null quantity, 0 rejects, curated volume null, reconcile raises `KeyError` |
| Decimal prices (session 03 GOOG) | 1,178 rejects; only `rejected_rows` fails |
| Prices x100 (session 03 MSFT) | every check passes |
| Swapped trade messages 1 and 3 (session 01 AAPL) | `aggressor_at_message` 2 rows; `lifecycle_fills` 58,996 expected, 58,854 found |
| Swapped flow rows 11 and 17 (session 02 AAPL) | `aggressor_at_message` 2 rows; `lifecycle_fills` 59,660 expected, 59,646 found |
| CRLF redelivery (session 01 GOOG) | refined doubles from 1,247 to 2,494; `natural_key_unique` 1,247 per batch, plus 3 accidental refined-to-curated failures per batch |
| Shuffled rows (session 01 AAPL) | 5,577 versions and 17 intervals identical to clean; 189 checks pass |
| `mergeSchema` over long and double | the read fails |

**Tests and CI.**

- **Tests:** 57 in total. `test_transforms.py` 13, `test_curate.py` 13, `test_reconcile.py` 23,
  `test_experiments.py` 8.
- **Local suite time** (WSL2): 126 s, 134 s and 253 s across three runs on 2026-10-22. It varies
  a lot, so quote it as "two to four minutes on my laptop".
  - The shared lake fixture took 44 to 52 s of each run.
  - The 8 experiment tests took 8.5 to 12.7 s each, most of them about 9 s.
- **CI** (ubuntu-24.04), two runs on 2026-10-22:

  | Commit | Test step | Pipeline and both reconciliations | Whole run |
  |---|---|---|---|
  | `dbf85dd` | 101 s | 77 s | 3 min 25 s |
  | `8ea05d2` | 132 s | 97 s | 4 min 10 s |

  Before the experiment tests existed (`e0390dd`), the Test step took 38 s.

## 3. How each stage works

### 3.1 Ingest: [ingest.py](../src/ingest.py)

- **Manifest-driven.** `data/manifest.csv` maps each source file to its session date.
  - **Validation first:** `read_manifest` validates every entry (an ISO date, a filename
    matching `<trades|flow>_<TICKER>.csv`) before anything is written, so a bad row stops the
    run instead of half of it.
  - **Unlisted files:** a CSV on disk but not in the manifest is reported and skipped, because
    it has no date.
- **Batch id.** `<file stem>-<session date>-<first 12 hex chars of sha256(file bytes)>`.
  - **Idempotency:** if the target directory already has Spark's `_SUCCESS` marker, the file
    is skipped. A directory without it is a partial write and is overwritten.
  - **Why the date is in the id:** so a corrected manifest date makes a new batch rather than a
    skip.
- **Trades:** read with a header and `inferSchema` off, so every column lands as a string.
- **Flow:** read as text lines, numbered with `zipWithIndex`, and the header dropped.
  - **Numbering:** line *n* becomes message *n - 1*, 0-based like the trade log.
  - **Parsing:** each line is split with `from_csv`, using a schema built from the header with
    every column `STRING`.
  - **Type:** flow's `message` is a generated number, while trades' `message` is a string from
    the file.
- **Provenance columns:**
  - `instrument`, from the filename;
  - `session_date`, from the manifest;
  - `source_file`;
  - `ingested_at`, from `current_timestamp()`, evaluated once per write so every row of a batch
    shares it.
- **Write:** `coalesce(1)` to one file per batch. The count printed afterwards is read back
  from what landed.

### 3.2 Refine: [refine.py](../src/refine.py)

- **Read:** all raw batches, with `mergeSchema`. The batches before session 03 have no `venue`
  column, so it reads as null for them.
- **Newest batch per source file:** a window over `source_file` keeps the rows whose
  `ingested_at` equals the maximum. Raw is immutable, so a regenerated file or a corrected date
  leaves the old batch in place, and this is where it is superseded.
- **Parse check:** a present value is unparseable if it does not match `^-?[0-9]+$` or the
  cast to long gives null (overflow).
  - **Nulls:** a null is not a failure, because a cancel's price is legitimately empty.
  - **Columns checked:** trades check all five columns. Flow checks `id`, `price`, `quantity`
    and `owner`, not `message`, which ingest generated.
- **Rejects** keep the raw strings and go to `refined/rejects/<entity>/`. Accepted rows are cast
  to long.
- **`event_ts`** is `to_timestamp_ntz(session_date + " 09:30:00")` plus `message` seconds, as
  `TIMESTAMP_NTZ`. `trade_date` is the date, and `session_date` is dropped.
- **Write:** `repartition` by `trade_date, instrument`, then `partitionBy` the same, with
  overwrite. That gives one file per partition, not one per task.

### 3.3 Curate: [curate.py](../src/curate.py)

- **`traded_volume`:** grouped by date, instrument and `window(event_ts, "5 minutes")`.
  - **Columns:** trade count, volume (sum of quantity) and notional (sum of price x quantity,
    exact in integer ticks), with the interval's start and end.
  - **Empty intervals:** intervals with no trades produce no row.
- **`order_events`:** one row per order per message.
  - **Fills:** each trade is split into an aggressor side and a passive side, unioned, and
    summed per order per message, so a sweep's fills on one order collapse into one figure.
  - **Join:** full-joined with the flow requests on `(trade_date, instrument, order_id,
    message)`, so a request and the fills it caused sit in one event.
- **`order_versions`:** a plain Python state machine over one order's events, sorted by message.
  - **Add:** sets side, type, price and quantity. A market order has no price.
  - **Events before an add are skipped**, for an order never seen added.
  - **Cancel or modify** applies only while the order is live by its own fills. A modify to
    quantity 0 is ignored. A modify resets price and quantity rather than subtracting.
  - **Fills** reduce `remaining` and add to `filled`. A market order with quantity left is
    `cancelled`, because the engine discards its remainder.
  - **Versions:** a new one only when state, price or remaining quantity changes.
  - **Bounds:** `valid_to` is the next version's `valid_from`; the last is `None`.
- **`order_lifecycle`:**
  - **Grouping:** `collect_list` of event structs per order, sorted by `sort_array`. That works
    because `message` is the struct's first field.
  - **Replay:** `rdd.flatMap` runs `order_versions` per order.
  - **Schema:** the result is built with an explicit schema, `valid_from` and `valid_to` as
    `TimestampNTZType`.
  - **Shipping the module:** `addPyFile(__file__)` ships the module to the Python workers, which
    need it to unpickle the function when curate is imported by the tests or reconcile.

### 3.4 Reconcile: [reconcile.py](../src/reconcile.py)

- **Independent source read.** Source files are read with Python's `csv` module, with empty
  fields as `None`, as raw stores them.
  - **Flow numbering:** flow rows are numbered by `enumerate`, independently of `zipWithIndex`.
  - **Own rules:** the checker has its own integer pattern, its own list of integer columns
    (kept separate from refine's on purpose) and its own newest-batch selection.
- **`Batch`:** one source file as it appears in every layer: source rows, raw rows (newest
  batch), rejects, refined rows, curated totals for its session, plus three per-session
  lookups.
  - **The lookups:** the order ids added in the flow, the flow's order at each message, and
    natural-key counts across all batches of the session.
  - **Where they're built:** `load_batches` builds them, which keeps every check a function of
    one batch.
- **Nine checks**, each returning `Result(check, hop, batch, expected, found, details)`:
  - `row_count`: source vs raw; raw vs refined plus rejects (a row must not vanish); refined vs
    curated trade count.
  - `traded_quantity`: totals at every hop. The raw-to-refined comparison excludes rejected
    rows.
  - `matching_rows`: rows compared by key (`(message, passive_id)` for trades, `message` for
    flow), listing missing, extra and changed fields.
  - `rejected_rows`: must be zero.
  - `positive_quantity`: no zero or negative quantities in refined.
  - `order_ids_exist`: each trade's aggressor and passive were added in that session's flow.
  - `aggressor_at_message`: the flow's order at a trade's message is its aggressor.
  - `natural_key_unique`: each key appears once per session in refined, across batches.
  - `lifecycle_fills`: the final versions' filled quantity is twice the traded volume.
- **Report:** a summary per check and hop, then each break with its row-level details, then
  every result.
  - **Byte-for-byte reproducibility:** the report has no timestamps and is written with
    `newline="\n"`, so a re-run reproduces it byte for byte.
  - **Exit code:** a clean run exits 1 if anything fails; a planted-break run exits 1 if
    nothing fails.
- **Planted break (`--plant-break`):** builds `lake_broken/` from nothing, ingests all 18 files,
  then damages `session_02/trades_MSFT.csv`'s raw batch:
  - drops message 93 (three fills, quantity 44);
  - sets the quantity of the trade at message 30, passive_id 7, from 38 to 0;
  - leaves `_SUCCESS` in place.

  The damaged batch is written to a staging directory and the part files are swapped in,
  because Spark cannot overwrite files it is still reading. Refine and curate then run on the
  broken lake, and reconcile checks it.

## 4. Design decisions, alternatives, and the questions they invite

### 4.1 Parquet, partitioned by `trade_date` then `instrument`

- **Alternatives:**
  - instrument first;
  - date only, sorted by instrument;
  - no partitioning;
  - CSV or JSON in the refined layer;
  - a table format such as Delta or Iceberg.
- **Likely question:** "Why that partition order, and isn't it too fine for this data?"
- **Answer:**
  - **It is over-partitioned at this size.** Each partition holds about 1,100 to 1,250 trades.
    Past a few hundred instruments I would partition by date only, sort by instrument within
    each file, and rely on Parquet's per-file min/max statistics for instrument filters: far
    fewer files, coarser skipping (reasoning, not measured).
  - **Order is not about pruning.** Spark prunes on any partition column regardless of
    directory order. It is about what a reload or delete touches: reprocessing or expiring one
    day is one directory under date-first, and one per instrument under instrument-first.
- **Why Parquet** (mostly reasoning):
  - **It carries the schema with the data.** `TIMESTAMP_NTZ` survives a round trip, which was
    tested.
  - **Columnar storage and per-file statistics** suit scans of a few columns.
  - **The spec requires it.**
- **Where Parquet bit me:** it is also where both type experiments ran into trouble.
  `mergeSchema` cannot read a column stored as long in one batch and double in another (tested),
  which is one reason raw is all strings.
- **Delta or Iceberg:**
  - **What it would add:** atomic commits instead of trusting `_SUCCESS`, time travel, and
    enforced schemas (reasoning).
  - **Why not:** I stayed with plain Parquet because the scope is local Spark with no extra
    infrastructure.
  - **The weakness to name:** the `_SUCCESS` marker proves a write finished, not that it is
    right, and the planted break exploits exactly that.

### 4.2 Raw holds only what cannot be recovered later

- **Alternatives:** type and derive at ingest; keep raw as a copy of the files.
- **Likely question:** "Why not parse once at ingest and save a layer?"
- **Answer:** it came out of a bug.
  - **The bug:** `event_ts` was first derived at ingest. When I changed its type from zoned to
    `TIMESTAMP_NTZ`, the code changed but the files did not, so every batch id stayed the same.
    On an existing lake the next ingest would have skipped all 18 batches and kept the old
    timestamps: silently stale.
  - **The general rule:** a derived column in immutable, content-addressed storage cannot be
    corrected when its derivation changes. Refine is rebuilt from raw on every run, so a changed
    rule corrects every row.
  - **What raw keeps:** the source text as written, the flow's message number, and provenance.
- **Follow-up: "So raw is just a copy of the CSVs?"**
  - **Not quite.** It adds provenance (instrument, date, file, batch id, ingest time) and the
    message number, which the files do not carry in any column.
  - **Why strings:** all strings means a column that changes type between batches can still be
    read. With inferred types, the decimal-price batch would have made the whole table
    unreadable (tested).

### 4.3 Why `message` stays in raw when `event_ts` does not

- **Likely question:** "You just said derived columns don't belong in raw. Isn't the flow's
  message number derived?"
- **Answer:** it is derived from something that does not survive. The flow has no sequence
  column; a trade's `message` is the flow row's position in the file. Parquet does not preserve
  row order, so once the file is gone the position cannot be recovered from raw. `event_ts` can
  be recomputed from `message` and the session date at any time; `message` cannot be recomputed
  from anything raw holds. That is the whole test: can it be recovered later?

### 4.4 `zipWithIndex`, not `monotonically_increasing_id`

- **Likely question:** "Why drop to the RDD API for a row number?"
- **Answer:**
  - **`monotonically_increasing_id` is unique and increasing but not contiguous.** It puts the
    partition id in the upper bits, so the second partition starts at 2^33. It cannot serve as
    a message number.
  - **`zipWithIndex` follows file order across partitions**, which I checked with the file
    forced into 8 partitions.
  - **The cost** is an extra Spark job to count each partition first. At 5,000 lines that does
    not matter.
- **Follow-up: "What about `row_number()` over a window?"** It needs an ordering column, and
  there is no column to order by. File position is exactly what is missing.

### 4.5 `TIMESTAMP_NTZ` rather than a zoned timestamp

- **Likely question:** "Why not store UTC like everyone else?"
- **Answer:**
  - **No zone to store.** The engine has no clock and no zone. A zoned timestamp would invent
    one: the same instant displays as 09:30 in UTC and 05:30 in New York.
  - **What NTZ says:** "09:30:46 on this session's date", which is all the data supports.
- **Two traps found along the way:**
  - **`date_trunc` returns a zoned `TIMESTAMP`**, quietly reintroducing the zone. Curated uses
    `window`, which keeps NTZ bounds and gave identical buckets under UTC, New York and Tokyo
    session zones. If `date_trunc` were needed, cast the result back with
    `.cast("timestamp_ntz")`.
  - **`F.lit(datetime(...))` is zoned.** Compared with an NTZ column, it shifts by the session
    zone, so an as-of query for 09:30:40 silently asked about another moment. Literals are built
    with `to_timestamp_ntz` from the wall-clock string.
  - **How it surfaced:** the tests run with the session zone set to America/New_York on purpose,
    which is what surfaced it.
- **When I would decide differently:** if the engine stamped real wall-clock times in a known
  zone, store a zoned timestamp normalised to UTC.

### 4.6 A pattern check, not convert-and-check-for-null

- **Likely question:** "Why a regex when a cast already gives null on bad input?"
- **Answer:** because it does not.
  - **The cast accepts bad input:** with ANSI mode off (Spark 3.5's default), casting the string
    `"1.5"` to long gives `1`, with no null and no error. A cast-then-null check would pass a
    corrupted quantity.
  - **What counts as unparseable:** a present value that does not match `^-?[0-9]+$` or
    overflows a long. A test pins `"1.5"`, `"abc"`, `" 42"` and `"9223372036854775808"` as
    failures, and null as not a failure.
- **Follow-up: "Why not turn ANSI mode on?"** (reasoning; I did not try it)
  - **It would raise on the first bad value** and fail the whole job, rather than routing one
    row to rejects.
  - **The check should not depend on a session setting.**

### 4.7 Rejects kept, not dropped

- **Likely question:** "Why keep bad rows at all?"
- **Answer:**
  - **They are the evidence of what arrived**, kept with their raw strings.
  - **They close the count:** reconciliation checks raw against refined plus rejects, so a row
    can be rejected but never vanish.
  - **They make a parse failure visible**, through `rejected_rows`. In the decimal-price
    experiment that was the only check that fired: the refined-to-curated checks passed on two
    empty sides. With rejects dropped instead, raw-to-refined `row_count` would still fail, but
    nothing would hold the rows to inspect (reasoning).

### 4.8 One version per message, not per fill

- **Likely question:** "A market order that sweeps three levels has three fills. Why one
  version?"
- **Answer:**
  - **Time resolution equals message resolution:** one message is one second, and every fill
    of a sweep has the same timestamp.
  - **Per-fill versions would be useless.** Three versions inside one message would have
    zero-length intervals, `[t, t)`, which no as-of query can ever return.
  - **The test:** order 28's three fills at message 46 make one `filled` version.
- **When I would decide differently:** with sub-message timestamps, one version per fill is
  right.

### 4.9 Stored validity bounds, not `lead()` at query time

- **Likely question:** "`valid_to` is just the next row's `valid_from`. Why store it?"
- **Answer:**
  - **The spec asks for records that carry their bounds**, and bounds computed per query do not
    exist in the data.
  - **The as-of query becomes a simple filter** rather than a window function over each order's
    history in every query (reasoning; not benchmarked).
  - **They can be tested as data.** The bounds are checked directly: every order has exactly
    one open version and it is the last, and versions are contiguous and never empty.
- **The cost:** a late event for an order means recomputing that order's versions. Here curated
  is rebuilt in full anyway.

### 4.10 Null, not a far-future sentinel, for open intervals

- **Likely question:** "Why not `9999-12-31` so queries can use `BETWEEN`?"
- **Answer:**
  - **A sentinel stores a date that is not true**, and the value leaks into any aggregate or
    export that forgets about it.
  - **The accepted cost:** every as-of query is written
    `valid_from <= t AND (valid_to IS NULL OR t < valid_to)`.
  - **Bounds are half-open**, so two adjacent versions never both match a moment.
  - **Resting orders stay open.** Orders still resting at session end stay open, because the
    engine has no session close, and an invented "expired" state would fabricate a transition.

### 4.11 Notional and volume, not VWAP

- **Likely question:** "The spec asks for VWAP. Where is it?"
- **Answer:**
  - **VWAP is not additive.** The VWAP of an hour is not the average of its twelve 5-minute
    VWAPs.
  - **Store the parts instead.** Notional and volume do sum correctly over any period, so VWAP
    is `sum(notional) / sum(volume)` over whichever intervals the query selects.
  - **Storing only the ratio** makes every longer-period rollup wrong.
  - **The test:** interval totals must re-aggregate to each whole session, and the session
    figure for AAPL on 2026-10-19 was checked by hand with awk.

### 4.12 The source read with Python's `csv`, not Spark

- **Likely question:** "You have Spark. Why read the source twice, and in Python?"
- **Answer:**
  - **A reconciliation that reads through the path it is checking shares that path's bugs.** If
    Spark's CSV reader or ingest dropped a row, a check that also read the source through Spark
    would agree with it.
  - **The same reasoning applies throughout the checker:** its own integer rules, its own
    newest-batch selection, and flow numbering by `enumerate` rather than `zipWithIndex`.
- **The honest limit:** it does share the manifest and filename parsing with ingest
  (`ingest.read_manifest`), so a wrong date in the manifest would be carried consistently and
  not caught.

### 4.13 `matching_rows` on every batch, not only when a total fails

- **Likely question:** "Row-level comparison is expensive. Why not run it only to explain a
  failed total?"
- **Answer:**
  - **Offsetting errors defeat every total.** One quantity up by 2 and another down by 2 leave
    the count and the sum unchanged.
  - **The test:** a test plants exactly that and asserts every total passes and only
    `matching_rows` fails.
  - **Conditional checking misses it.** If the row check ran only after a total failed, it
    would never run in that case.
- **At scale:** the totals would run first in Spark, and the row comparison would be a keyed
  full outer join rather than Python dictionaries (reasoning).

### 4.14 Why corrupting the source cannot demonstrate a break

- **Likely question:** "Why build a separate broken lake instead of just editing a CSV?"
- **Answer:**
  - **A source-versus-output break has to enter between source and output.** Corrupt the source
    file and the pipeline carries the corruption faithfully: source and lake agree, and every
    comparison passes.
  - **Only validity checks would notice**, such as `positive_quantity` for a zero quantity.
  - **The experiments showed it repeatedly:** swapped trade messages, swapped flow rows and
    prices x100 all pass every source-to-output comparison.

### 4.15 Why the break tampers with a raw batch, not a fault in refine

- **Likely question:** "Isn't editing stored Parquet unrealistic? Transform bugs are more
  common."
- **Answer:**
  - **No test hooks in production code.** Injecting a fault into refine would mean a flag or
    branch in the pipeline code that exists only for the demo.
  - **It stands in for real failures.** Damaging the raw batch after ingest stands in for a
    partial write or a bad reprocess, and it lands between source and output, which is what
    reconciliation is for.
  - **`_SUCCESS` stays, deliberately.** The marker is left in place to show that the marker
    proves a write finished, not that it is right.
  - **Transform bugs are covered too**, by unit tests: for example, refine losing a row without
    rejecting it, and curated volume disagreeing with refined.
- **What the report shows:** every hop after raw passes, which is what places the break in the
  raw batch rather than in a transform.

### 4.16 Why dedup was left out of refine

- **Likely question:** "You detect the CRLF redelivery. Why not fix it?"
- **Answer:**
  - **Dedup needs a rule I do not have.** For identical duplicates it is trivial. For two rows
    with the same key and different values, something must decide which wins: newest ingest,
    or quarantine both. That is a business rule, and inventing one would hide exactly the
    conflicts reconciliation should surface.
  - **Reconciliation's job is detection.** `natural_key_unique` now names every duplicate row,
    and the README names dedup as the production fix.
  - **When I would build it:** with a stated rule for conflicting duplicates, I would
    deduplicate in refine on `(trade_date, instrument, message, passive_id)` and keep the
    uniqueness check as a guard.

## 5. Testing and verification questions

- **"What do the 57 tests cover?"**
  - **`test_transforms.py` (13), ingest and refine on the real data:**
    - message 46 traced by hand;
    - every trade has a flow row at its message whose order is its aggressor;
    - parse rules;
    - newest batch;
    - NTZ;
    - one file per partition.
  - **`test_curate.py` (13), the state machine and the real lifecycle:**
    - the state machine on hand-built events;
    - orders 22 and 28 from the real data;
    - as-of queries;
    - interval and session totals checked by hand with awk.
  - **`test_reconcile.py` (23):**
    - each check on a hand-built batch;
    - one failure per check that no other check catches;
    - the clean lake passing;
    - the planted break caught with exact figures and row details.
  - **`test_experiments.py` (8):** the README's experiments.
- **"How do you know the README's experiment figures are real?"**
  - **Each experiment is a test** that edits a copy of `data/`, ingests the edited file as a
    newer batch on top of the clean raw layer, rebuilds refined and curated, reconciles, and
    asserts the README's figures. If the code changes what an experiment does, the test fails
    and the README must change with it.
  - **This caught real drift.** When I added the two new checks, exactly four experiment
    tests failed: both swaps and the CRLF redelivery, because a new check now caught them, and
    the shuffle, because the check count went from 162 to 189. The README's "only
    `lifecycle_fills` caught it" lines had to change with them.
  - **A guard:** each test first asserts the edit produced a new batch id, so an edit that did
    not change the file cannot pass silently.
- **"Why is there a test asserting a `KeyError`?"**
  - **It is a characterisation test.** It pins current known-bad behaviour: a renamed column
    lands as nulls, and reconciliation stops with `KeyError: 'quantity'` instead of reporting
    which batch broke.
  - **It does not endorse the behaviour.** The test exists so that the behaviour cannot change
    unnoticed.
  - **The fix is in README section 4:** a header contract checked at ingest.
- **"How do you know each check matters?"**
  - **Mutation testing on the planted break was not enough.** I disabled each check in turn and
    re-ran the planted break, and none of them let it through on its own. The break is caught
    redundantly: the dropped rows by three checks, the zeroed quantity by three. The smallest
    set of disabled checks that hides it entirely is the four that fire, together.
  - **So the planted break cannot show any single check is load-bearing.** Instead, each check
    has a failure that only it catches, and a parametrised test asserts that exactly that one
    check fails. Examples:
    - offsetting errors for `matching_rows`;
    - an engine-emitted duplicate line for `natural_key_unique`;
    - a trade whose message holds another order for `aggressor_at_message`.
- **"What does the shuffle test show?"**
  - **It is the negative control.** It shuffles every row of a trades file and asserts nothing
    changes: 5,577 versions and 17 intervals identical to the clean run, and all 189 checks
    passing.
  - **It validates the harness itself.** It shows the experiment harness, a superseding batch
    on a copied lake, introduces no differences of its own, so the failures in the other
    experiments come from the edits.
  - **It also proves a design claim:** everything derived from trades is ordered by message,
    never by arrival.
- **"How do you know the reports are reproducible?"**
  - **CI rebuilds and diffs them.** CI runs the full pipeline and both reconciliations on a
    fresh ubuntu-24.04 runner, then `git diff --exit-code results/`. The committed reports were
    generated on my WSL2 Ubuntu 22.04 machine, so a pass means a different machine reproduced
    them byte for byte. It passed on the 189-row reports.
  - **What makes that possible:**
    - no timestamps in the reports;
    - `newline="\n"`;
    - LF forced by `.gitattributes`, because batch ids hash file bytes;
    - row details sorted by key.
  - **The sorting was a real fix:** `aggressor_at_message` details first came out in Parquet
    row order, which is not guaranteed.

## 6. Weak spots to be ready for

These are true of the current code. Raise them yourself before an interviewer finds them. For
each: the reasoning, the condition under which I would decide differently, and the likely
follow-up.

**The 266 self-trade-prevention orders.**

> The lifecycle is inferred from two outputs, the flow and the trades, and neither records what
> the engine removed. Self-trade prevention discards an incoming order's remainder without a
> trace, so 266 limit orders, 1.2% of limit adds, look like they are still resting. I measured
> that against an independent reference matcher rather than guessing, and every one of the
> orders whose history differs from the reference is one of those 266. Everything else matches
> exactly. The fix is upstream: the engine should publish order events — accepted, rested,
> cancelled, rejected, removed — so the lifecycle reads facts instead of inferring them. I
> scoped that out deliberately.

- **Follow-up: "Could you infer them?"** Partly. The pipeline could replay the book itself, but
  then it would be a second matching engine, and the lifecycle would be only as right as that
  re-implementation. Reading events is the correct fix.

**A unit change that still parses passes every check.**

> Prices scaled by 100 are still integers, and source and lake agree, so a source-to-output
> reconciliation has nothing to compare against. That is a limit of reconciliation itself, not
> a missing check: it proves the pipeline carried the data faithfully, not that the data is
> plausible. Catching it needs knowledge of what a price should look like: the producer
> declaring units in a schema contract, and a plausibility band against the previous session.

- **Follow-up: "Why didn't you add the plausibility check?"** It needs history across sessions
  and a tolerance I would be inventing, and three sessions is not enough to set a band honestly.

**The flow's sequence number is its row position.**

> The flow file has no message column, so a row's message number is its position. Swap two rows
> and their message numbers swap, and source and raw still agree, because both number by
> position. `aggressor_at_message` catches it when either swapped row caused a trade, and in
> the experiment both did. A swap of two rows that caused no trade would pass it (reasoning, not
> tested). The real fix is for the engine to write the message number as a column.

- **Follow-up: "Would `lifecycle_fills` catch the no-trade swap?"** Not necessarily: it checks
  a total, and two adds swapped with no fills can leave it unchanged (reasoning, not tested).

**A redelivery with different bytes is double counted.**

> Ingest is idempotent by content hash, which protects against an exact replay. The same data
> with CRLF line endings, in a new folder, is a new hash and a new path, so refine keeps both and
> the session doubles: 1,247 trades became 2,494. `natural_key_unique` now names every
> duplicated row, so it is detected. It is not removed, because dedup needs a rule for
> conflicting duplicates that I do not have.

- **Follow-up: "Why not hash normalised content instead of bytes?"**
  - **Normalising line endings would catch this case,** but byte identity is the honest
    definition of "the same file".
  - **The general problem is semantic duplicates**, which only the natural key catches.

**Rows are collected into Python.**

> Reconciliation collects each batch's rows to the driver and compares them in dictionaries. At
> about 10,500 trades and 45,000 flow rows across three layers that is fine. At scale it would
> not be: the totals would run as Spark aggregations first, and the row-level comparison would
> be a keyed full outer join, writing differences out rather than collecting them. I kept it
> in Python because it is easier to show it is independent of the Spark path it checks.

- **Follow-up: "What's the threshold?"** I have not measured one. Say so, rather than guess.

**No watermark.**

> Every run rebuilds refined and curated from all of raw, so a late file for an old session is
> included next time, in the right partition, because its date comes from the manifest. Nothing
> needs to wait. The cost is that nothing is ever final: a published curated figure can change
> without notice, and a full rebuild does not scale. An incremental design would need a rule for
> when a session is complete, and the engine emits no end-of-session marker.

- **Follow-up: "So how would you set the watermark?"** On the session, not on processing time:
  a session closes when its end marker arrives or after a stated lateness bound, and a later
  arrival is routed to a correction process rather than silently changing a closed figure
  (reasoning).

**Venue is constant per file.**

> The venue column demonstrates a nullable column arriving in a later batch. It labels the engine
> run, not each trade, so it is not multi-venue matching, and I say so in the README.

- **Follow-up: "Does anything use it?"** No, which is why adding it was safe. That is the point
  of the experiment.

**The experiment tests rebuild the whole lake each time.**

> They share the session's raw layer, but each one rebuilds refined and curated for all nine
> sessions and reconciles all 18 batches, about 9 seconds each. That took CI's test step from
> 38 s to 101 s in the first run. It buys realism: each experiment runs the real stages
> unchanged. If the suite grew, I would restrict each experiment's manifest to the affected
> session, which should cut the work by most of that (reasoning, not measured).

- **Follow-up: "Why not one shared broken lake for all experiments?"** Because they would
  interfere: two edits in one lake could mask or cause each other's failures.

**Smaller items, if asked:**

- **Order of `positive_quantity` and `order_ids_exist` details.** These details are in refined
  row order, not sorted. Each has at most one detail in the committed reports, so the byte-for-
  byte diff is unaffected. With several failures, the order could differ between machines
  (reasoning, not observed).
- **The manifest is a shared dependency.** Reconcile uses the same manifest as ingest, so a
  wrong session date would be carried consistently (see 4.12).

## 7. Things not to claim

- **That it is production-grade.**
  - It is local Spark on 18 small files, with no cluster, orchestration, streaming or table
    format.
  - Batch commit relies on Spark's `_SUCCESS` marker.
- **Any performance figure at scale.** The partitioning advice past a few hundred instruments,
  the cost of `lead()`, and where Python collection stops being viable are all reasoning. No
  Spark job was timed per stage and no file sizes were measured.
- **That suite or CI times are benchmarks.** They varied from 126 s to 253 s locally and from
  3 min 25 s to 4 min 10 s in CI for the same code.
- **That the lifecycle is exact.** It is exact except for the 266 self-trade-prevention orders,
  and the 130 cancels and 64 modifies sent to them.
- **That reconciliation catches everything.**
  - A unit change that still parses passes.
  - A flow swap with no trades at either message is expected to pass.
  - A renamed column stops the run instead of producing a report.
- **That ingest is idempotent end to end.** It is idempotent for an exact replay. A redelivery
  with different bytes is detected, not deduplicated.
- **That reconciliation is fully independent of the pipeline.** It shares the manifest and
  filename parsing with ingest.
- **That `aggressor_at_message` verifies the flow's numbering.** It verifies it only at messages
  that caused a trade.
- **That the experiments were run at scale or on real exchange data.** The data is from my own
  engine, three sessions of three instruments.
