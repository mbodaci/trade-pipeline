# CLAUDE.md — Trade Pipeline (Project 2)

## What this is
A PySpark pipeline over the trade log produced by my C++ order book engine
(Project 1). Built as a portfolio project for data engineering roles, with the
Goldman Sachs Lakehouse and AI Data Platform posting as the target: Spark,
Parquet, partitioning, schema evolution, temporal data modelling, and data
quality and reconciliation.

## Hard rule
I must be able to explain every line aloud without notes. Do not write code I
cannot defend. Prefer small guided pieces over large paste-in blocks.

## The defining risk
This must not read as a Spark tutorial. What makes it an engineering project is
the reconciliation catching a discrepancy I planted, and a written account of
what breaks in production. If a section could appear unchanged in a Spark
"getting started" guide, it is not pulling its weight.

## Scope is fixed by the spec
Local Spark only. No cluster, no orchestration tool (no Airflow, Dagster,
Prefect), no streaming, no cloud storage. Depth on the six ideas in the spec
beats breadth across the stack. Push back if I am scope-creeping.

## Input data
`data/` holds real trade logs from my own engine, one CSV per instrument.
Columns: `message, aggressor_id, passive_id, price, quantity`.

Two things the engine does not emit, which the pipeline must derive:
- **Instrument** — the engine is single-instrument; instrument comes from the
  source filename. This is provenance, so it is captured at ingest.
- **Time** — `message` is a logical sequence number, not wall-clock time. The
  event timestamp (`TIMESTAMP_NTZ`) is derived from it in refine, not ingest.

Both derivations must be documented in the README. This adaptation is part of
the engineering, not something to hide.

## Layer boundary: what goes in raw
Raw holds only what cannot be recovered later: the source text as written, the
flow's message number, and provenance (instrument, session date, source file,
batch id, ingest time). Anything derivable from those belongs in refine.

Why: raw is immutable and its batch id is a content hash of the source file. A
derived column in raw cannot be corrected when its derivation changes, because
the unchanged file maps to the same batch and is skipped. Refine is rebuilt
from raw on every run, so a changed rule corrects every row.

The flow's `message` stays in raw for exactly this reason: it is the row's
position in the source file, and Parquet does not preserve row order, so once
the file is gone it cannot be recovered.

## Environment and commands
Runs in WSL2 (Ubuntu 22.04, Python 3.10, Java 17). Native Windows lacks the
Hadoop binaries Spark needs for local files.
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Run tests:
```bash
pytest -v
```

Run the pipeline stages:
```bash
python src/ingest.py
python src/refine.py
python src/curate.py
python src/reconcile.py                 # clean run, writes results/reconciliation_clean.md
python src/reconcile.py --plant-break   # builds lake_broken/, writes reconciliation_break.md
```

## Code style

### Comments
Sparse and purposeful. Follow these rules:
- **Section markers** for logical blocks: `# Derive event timestamp from sequence number`
- **Why-comments** when the reason is not obvious from the code:
  `# repartition before write: avoids one small file per input partition`
- **No comments** on self-explanatory lines.
- **No docstrings on trivial functions.** Function and parameter names should carry it.
- **No per-line narration.** If five consecutive lines each have a comment, something is wrong.
- **No commented-out code** in committed files. Use Git history.

### Naming
- Functions and variables: `snake_case`
- Constants: `UPPER_SNAKE`
- DataFrames: name them for what they hold (`trades_raw`, `trades_refined`), not `df`, `df2`

### Formatting
- 4-space indentation
- Max line length: 100 characters

## Other rules
- No emojis anywhere: not in the README, comments, commit messages, or output.
- Never invent numbers. Any figure in the README must come from a run on my own
  machine. No made-up row counts, timings, or file sizes.
- Lay out alternatives and trade-offs before deciding for me, especially on
  partitioning scheme, schema evolution strategy, and how validity bounds are
  modelled.

## Repo layout
```
trade-pipeline/
  README.md
  CLAUDE.md
  requirements.txt
  src/          ingest.py, refine.py, curate.py, reconcile.py
  tests/        test_transforms.py, test_curate.py, test_reconcile.py, test_experiments.py
  data/         sample input from the order book engine
  results/      reconciliation report, including the caught break
  .github/workflows/ci.yml
```

## README sections (from the spec)
1. What it does, and where the data comes from.
2. The layers, and why they are split that way.
3. Partitioning: the choice and the trade-off.
4. Schema evolution: what is safe, what is not.
5. Temporal modelling: how history is kept.
6. Reconciliation: the checks, and the break it caught.
7. Late and out-of-order data: what breaks, and what I would change.
8. Limitations.

## Current state (handoff, updated 2026-10-22)
Read this first when resuming. Update it at the end of every session.

### Built, committed and pushed
Latest feature commit: `dbf85dd` (README for the new checks), after `6d187eb` (experiment tests
and the two new checks). CI green on it in 3 min 25 s (Test 101 s, pipeline and both
reconciliations 77 s), and the 189-row reports reproduced byte for byte. The commit that updated
this section comes after it.
- `src/ingest.py`: manifest-driven raw layer, all columns as strings, content-hash batch ids,
  flow message number from row position (`zipWithIndex`).
- `src/refine.py`: newest batch per source file, regex parse check, rejects kept,
  `event_ts` derived, partitioned `trade_date=/instrument=`.
- `src/curate.py`: `traded_volume` (notional and volume per 5-minute window) and
  `order_lifecycle` (one row per version, `[valid_from, valid_to)`).
- `src/reconcile.py`: 9 checks per batch per hop, 189 results on the clean lake, planted break
  via `--plant-break` (4 failures), both reports committed in `results/`. The two added on
  2026-10-22: `aggressor_at_message` (the only check on the flow's positional message numbers)
  and `natural_key_unique` (the only cross-batch check, counted per session in `load_batches`).
- Tests: 57 in `tests/`. Full suite about 130 s locally; the eight experiment tests take about
  9 s each (copy clean raw, ingest the edited file as a newer batch, refine, curate, reconcile).
- `tests/test_experiments.py`: the seven README experiments plus the `mergeSchema` long/double
  read, each asserting the README's figures. The rename test is a characterisation test of
  known-bad behaviour (it expects the `KeyError`).
- CI (`.github/workflows/ci.yml`, ubuntu-24.04): pytest, then the full pipeline and both
  reconciliations, then `git diff --exit-code results/` so the committed reports must reproduce
  byte for byte.
- README: all eight spec sections written; every figure came from a run on this machine.

### Environment facts
- WSL2 Ubuntu 22.04 only. Native Windows Spark hangs: the Oracle `javapath` java shim points at
  a removed JRE 1.8, and there is no `hadoop.dll`/`winutils.exe`. Do not create a Windows venv.
- OpenJDK 17 and Python 3.10 inside WSL. Venv at `.venv/` in the repo (a Linux venv), PySpark
  3.5.9, pytest 9.1.1.
- From Git Bash, run WSL commands through a script file:
  `MSYS_NO_PATHCONV=1 wsl.exe -d Ubuntu-22.04 -- bash /mnt/c/.../script.sh`. Inline quoting
  through PowerShell or `bash -c` breaks.
- `/etc/hosts` has `127.0.1.1 M1001A`. If the JVM dies with `UnknownHostException`, that line
  is missing.
- `lake/` and `lake_broken/` are rebuilt by the pipeline and gitignored, never committed.
- `.gitattributes` forces LF: batch ids hash file bytes, so line endings are part of identity.

### Half-done or deliberately deferred
- Dedup on the natural key in refine: left out by decision. `natural_key_unique` detects a
  redelivery; the README names dedup as the production fix.
- The scripts that measured the 266 self-trade-prevention orders and compared the lifecycle
  with the order book's reference matcher (`orderbook/tools/analyse.py`) were also scratchpad
  only. The method is in README section 5.
- Scoped out by decision: an engine order-event feed, and an explicit message column in the
  flow. Both are the production fixes named in the README.

### Settled design rules (do not relitigate)
- Layer boundary: raw holds only what cannot be recovered later (source text, the flow message
  number, provenance). Anything derivable goes in refine, because raw is immutable and
  content-hashed, so a derived column there could never be corrected.
- `event_ts` is `TIMESTAMP_NTZ`: the engine has no clock and no zone. `F.lit(datetime)` is
  zoned and shifts against NTZ columns, so build NTZ literals with `to_timestamp_ntz` from a
  string. `date_trunc` returns a zoned type, so use `window` or cast back. Tests run with the
  session zone set to America/New_York on purpose.
- Parsing: a regex check, never cast-then-null, because a cast turns `"1.5"` into 1.
- Partitioning: `trade_date=/instrument=` for refined and curated. Over-partitioned at this
  size; past a few hundred instruments move to date-only sorted by instrument. Date-first vs
  instrument-first is about what a reload or delete touches, not pruning.
- Lifecycle: one version per order per message (time resolution equals message resolution),
  open intervals null not a sentinel, terminal states get an open final row, orders resting at
  session end stay open.
- The 266 self-trade-prevention limit orders (1.2% of limit adds) are a documented gap, not a
  bug to fix: the outputs cannot show them.
- `matching_rows` runs on every batch as a check, not only to explain a failed total, because
  offsetting errors defeat every total.
- Experiments are tests, not a script or a third report. Any README figure from an experiment
  must be asserted in `tests/test_experiments.py`.
- The planted break damages one raw batch in `lake_broken/` (message 93 dropped, one quantity
  zeroed, `_SUCCESS` left in place). Nothing corrupted lives in `data/`.
