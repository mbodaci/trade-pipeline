# Project 2 spec: PySpark pipeline over the order book's trade log

**Purpose:** a portfolio project for data engineering roles, above all the Goldman Sachs Lakehouse and AI Data Platform role, whose posting names Spark, Parquet, partitioning, schema evolution, temporal data modelling, and data quality and reconciliation. It closes the exact gaps the CV currently leaves open.

**Hard stop:** one to two weeks. Start it after Project 1 produces trade output.

**Risk to avoid:** this must not read as a Spark tutorial. What makes it an engineering project is the reconciliation catching a discrepancy you planted, and a written account of what breaks in production.

---

## Input

The trade and event log from Project 1. Using your own engine's output is what ties the portfolio together: one system generates the data, the next one processes it.

## Pipeline

**Raw layer.** Ingest the CSV or JSON log as it is. Keep it immutable.

**Refined layer.** Parse, type and clean it. Write Parquet partitioned by date and instrument. Explain in the README why you partitioned that way, and what goes wrong with too many small partitions.

**Curated layer.** Aggregates ready for use: traded volume per instrument per interval, VWAP, and the order lifecycle (placed, partially filled, filled, cancelled).

## The four things that make it serious

**1. Schema evolution.** Add a column in a later batch (a venue or participant field) and show the pipeline reading old and new files together without breaking. Say in the README which changes are safe (adding a nullable column) and which are not (renaming or retyping), and how you would handle the unsafe ones.

**2. Temporal modelling.** Keep the history of change rather than overwriting. Give each record validity bounds, so you can ask what the state looked like at a past moment. This is what the Goldman posting means by "historical state and change over time".

**3. Reconciliation that catches something.** Build checks that compare source and output: row counts, total traded quantity, no trade without a matching order id, and no negative or zero quantities. Then deliberately corrupt one batch, by dropping rows or altering a quantity, and show the check catching it and reporting the break. A check that has never failed proves nothing.

**4. Late and out-of-order data, written up concretely.** Not a generic paragraph. Name the case: two trades for the same instrument arrive with their sequence numbers swapped. Say what breaks (the validity bounds get applied in the wrong order, and any running aggregate is wrong) and what you would change (order by sequence number within each key, hold late arrivals until a watermark passes, and make the write idempotent so a replay does not double count).

## Repo layout

```
trade-pipeline/
  README.md
  src/          ingest.py, refine.py, curate.py, reconcile.py
  tests/        test_transforms.py, test_reconcile.py
  data/         sample input (small)
  results/      reconciliation report, including the caught break
  .github/workflows/ci.yml
```

## README sections

1. What it does, and where the data comes from.
2. The layers, and why they are split that way.
3. Partitioning: the choice and the trade-off.
4. Schema evolution: what is safe, what is not.
5. Temporal modelling: how history is kept.
6. Reconciliation: the checks, and the break it caught.
7. Late and out-of-order data: what breaks, and what I would change.
8. Limitations.

## Keep it small

Local Spark is fine. No cluster, no orchestration tool, no streaming. The posting says candidates are not expected to have deep expertise in every tool from day one, so depth on these six ideas beats breadth across the stack.
