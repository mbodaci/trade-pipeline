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
  source filename.
- **Time** — `message` is a logical sequence number, not wall-clock time. The
  event timestamp is derived from it at ingest.

Both derivations happen in the ingest layer and must be documented in the README.
This adaptation is part of the engineering, not something to hide.

## Environment and commands
```bash
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
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
python src/reconcile.py
```

## Code style

### Comments
Sparse and purposeful. Follow these rules:
- **Section markers** for logical blocks: `# Derive event timestamp from sequence number`
- **Why-comments** when the reason is not obvious from the code: `# repartition before write: avoids one small file per input partition`
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
  tests/        test_transforms.py, test_reconcile.py
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
