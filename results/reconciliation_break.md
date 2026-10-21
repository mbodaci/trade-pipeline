# Reconciliation: planted break

Lake: `lake_broken/`. Source: the CSV files in `data/`, read with Python's `csv`
module, not Spark. One row per batch per hop, so a break shows where it entered.

The break was planted by `python src/reconcile.py --plant-break`, which builds
`lake_broken/` from the same source files and then damages one raw batch,
`session_02/trades_MSFT.csv` for 2026-10-20, before refine and curate run:

- the three fills of message 93 are dropped (a partial write losing one sweep);
- the quantity of the trade at message 30, passive_id 7, is set to 0;
- the batch's `_SUCCESS` marker is left in place, so the batch still looks committed.

Everything below is what the checks found, without being told what was planted.

## Summary

| Check | Hop | Batches | Failed |
|---|---|---|---|
| row_count | source -> raw | 18 | 1 |
| row_count | raw -> refined | 18 | 0 |
| row_count | refined -> curated | 9 | 0 |
| matching_rows | source -> raw | 18 | 1 |
| matching_rows | raw -> refined | 18 | 0 |
| rejected_rows | raw -> refined | 18 | 0 |
| positive_quantity | refined | 18 | 1 |
| traded_quantity | source -> raw | 9 | 1 |
| traded_quantity | raw -> refined | 9 | 0 |
| traded_quantity | refined -> curated | 9 | 0 |
| order_ids_exist | refined | 9 | 0 |
| lifecycle_fills | refined -> curated | 9 | 0 |

## Breaks

### row_count, source -> raw: `session_02/trades_MSFT.csv`

Expected 1187, found 1184.

### traded_quantity, source -> raw: `session_02/trades_MSFT.csv`

Expected 30505, found 30423.

### matching_rows, source -> raw: `session_02/trades_MSFT.csv`

Expected 0, found 4.

- message 30, passive_id 7: quantity 38 in source, 0 in raw
- message 93, passive_id 6: in source, missing from raw
- message 93, passive_id 23: in source, missing from raw
- message 93, passive_id 48: in source, missing from raw

### positive_quantity, refined: `session_02/trades_MSFT.csv`

Expected 0, found 1.

- message 30, passive_id 7: quantity 0

## Every check

| Check | Hop | Batch | Expected | Found | Result |
|---|---|---|---|---|---|
| row_count | source -> raw | `session_01/flow_AAPL.csv` | 5000 | 5000 | pass |
| row_count | raw -> refined | `session_01/flow_AAPL.csv` | 5000 | 5000 | pass |
| matching_rows | source -> raw | `session_01/flow_AAPL.csv` | 0 | 0 | pass |
| matching_rows | raw -> refined | `session_01/flow_AAPL.csv` | 0 | 0 | pass |
| rejected_rows | raw -> refined | `session_01/flow_AAPL.csv` | 0 | 0 | pass |
| positive_quantity | refined | `session_01/flow_AAPL.csv` | 0 | 0 | pass |
| row_count | source -> raw | `session_01/flow_GOOG.csv` | 5000 | 5000 | pass |
| row_count | raw -> refined | `session_01/flow_GOOG.csv` | 5000 | 5000 | pass |
| matching_rows | source -> raw | `session_01/flow_GOOG.csv` | 0 | 0 | pass |
| matching_rows | raw -> refined | `session_01/flow_GOOG.csv` | 0 | 0 | pass |
| rejected_rows | raw -> refined | `session_01/flow_GOOG.csv` | 0 | 0 | pass |
| positive_quantity | refined | `session_01/flow_GOOG.csv` | 0 | 0 | pass |
| row_count | source -> raw | `session_01/flow_MSFT.csv` | 5000 | 5000 | pass |
| row_count | raw -> refined | `session_01/flow_MSFT.csv` | 5000 | 5000 | pass |
| matching_rows | source -> raw | `session_01/flow_MSFT.csv` | 0 | 0 | pass |
| matching_rows | raw -> refined | `session_01/flow_MSFT.csv` | 0 | 0 | pass |
| rejected_rows | raw -> refined | `session_01/flow_MSFT.csv` | 0 | 0 | pass |
| positive_quantity | refined | `session_01/flow_MSFT.csv` | 0 | 0 | pass |
| row_count | source -> raw | `session_01/trades_AAPL.csv` | 1149 | 1149 | pass |
| row_count | raw -> refined | `session_01/trades_AAPL.csv` | 1149 | 1149 | pass |
| row_count | refined -> curated | `session_01/trades_AAPL.csv` | 1149 | 1149 | pass |
| traded_quantity | source -> raw | `session_01/trades_AAPL.csv` | 29498 | 29498 | pass |
| traded_quantity | raw -> refined | `session_01/trades_AAPL.csv` | 29498 | 29498 | pass |
| traded_quantity | refined -> curated | `session_01/trades_AAPL.csv` | 29498 | 29498 | pass |
| matching_rows | source -> raw | `session_01/trades_AAPL.csv` | 0 | 0 | pass |
| matching_rows | raw -> refined | `session_01/trades_AAPL.csv` | 0 | 0 | pass |
| rejected_rows | raw -> refined | `session_01/trades_AAPL.csv` | 0 | 0 | pass |
| positive_quantity | refined | `session_01/trades_AAPL.csv` | 0 | 0 | pass |
| order_ids_exist | refined | `session_01/trades_AAPL.csv` | 0 | 0 | pass |
| lifecycle_fills | refined -> curated | `session_01/trades_AAPL.csv` | 58996 | 58996 | pass |
| row_count | source -> raw | `session_01/trades_GOOG.csv` | 1247 | 1247 | pass |
| row_count | raw -> refined | `session_01/trades_GOOG.csv` | 1247 | 1247 | pass |
| row_count | refined -> curated | `session_01/trades_GOOG.csv` | 1247 | 1247 | pass |
| traded_quantity | source -> raw | `session_01/trades_GOOG.csv` | 32573 | 32573 | pass |
| traded_quantity | raw -> refined | `session_01/trades_GOOG.csv` | 32573 | 32573 | pass |
| traded_quantity | refined -> curated | `session_01/trades_GOOG.csv` | 32573 | 32573 | pass |
| matching_rows | source -> raw | `session_01/trades_GOOG.csv` | 0 | 0 | pass |
| matching_rows | raw -> refined | `session_01/trades_GOOG.csv` | 0 | 0 | pass |
| rejected_rows | raw -> refined | `session_01/trades_GOOG.csv` | 0 | 0 | pass |
| positive_quantity | refined | `session_01/trades_GOOG.csv` | 0 | 0 | pass |
| order_ids_exist | refined | `session_01/trades_GOOG.csv` | 0 | 0 | pass |
| lifecycle_fills | refined -> curated | `session_01/trades_GOOG.csv` | 65146 | 65146 | pass |
| row_count | source -> raw | `session_01/trades_MSFT.csv` | 1149 | 1149 | pass |
| row_count | raw -> refined | `session_01/trades_MSFT.csv` | 1149 | 1149 | pass |
| row_count | refined -> curated | `session_01/trades_MSFT.csv` | 1149 | 1149 | pass |
| traded_quantity | source -> raw | `session_01/trades_MSFT.csv` | 29457 | 29457 | pass |
| traded_quantity | raw -> refined | `session_01/trades_MSFT.csv` | 29457 | 29457 | pass |
| traded_quantity | refined -> curated | `session_01/trades_MSFT.csv` | 29457 | 29457 | pass |
| matching_rows | source -> raw | `session_01/trades_MSFT.csv` | 0 | 0 | pass |
| matching_rows | raw -> refined | `session_01/trades_MSFT.csv` | 0 | 0 | pass |
| rejected_rows | raw -> refined | `session_01/trades_MSFT.csv` | 0 | 0 | pass |
| positive_quantity | refined | `session_01/trades_MSFT.csv` | 0 | 0 | pass |
| order_ids_exist | refined | `session_01/trades_MSFT.csv` | 0 | 0 | pass |
| lifecycle_fills | refined -> curated | `session_01/trades_MSFT.csv` | 58914 | 58914 | pass |
| row_count | source -> raw | `session_02/flow_AAPL.csv` | 5000 | 5000 | pass |
| row_count | raw -> refined | `session_02/flow_AAPL.csv` | 5000 | 5000 | pass |
| matching_rows | source -> raw | `session_02/flow_AAPL.csv` | 0 | 0 | pass |
| matching_rows | raw -> refined | `session_02/flow_AAPL.csv` | 0 | 0 | pass |
| rejected_rows | raw -> refined | `session_02/flow_AAPL.csv` | 0 | 0 | pass |
| positive_quantity | refined | `session_02/flow_AAPL.csv` | 0 | 0 | pass |
| row_count | source -> raw | `session_02/flow_GOOG.csv` | 5000 | 5000 | pass |
| row_count | raw -> refined | `session_02/flow_GOOG.csv` | 5000 | 5000 | pass |
| matching_rows | source -> raw | `session_02/flow_GOOG.csv` | 0 | 0 | pass |
| matching_rows | raw -> refined | `session_02/flow_GOOG.csv` | 0 | 0 | pass |
| rejected_rows | raw -> refined | `session_02/flow_GOOG.csv` | 0 | 0 | pass |
| positive_quantity | refined | `session_02/flow_GOOG.csv` | 0 | 0 | pass |
| row_count | source -> raw | `session_02/flow_MSFT.csv` | 5000 | 5000 | pass |
| row_count | raw -> refined | `session_02/flow_MSFT.csv` | 5000 | 5000 | pass |
| matching_rows | source -> raw | `session_02/flow_MSFT.csv` | 0 | 0 | pass |
| matching_rows | raw -> refined | `session_02/flow_MSFT.csv` | 0 | 0 | pass |
| rejected_rows | raw -> refined | `session_02/flow_MSFT.csv` | 0 | 0 | pass |
| positive_quantity | refined | `session_02/flow_MSFT.csv` | 0 | 0 | pass |
| row_count | source -> raw | `session_02/trades_AAPL.csv` | 1156 | 1156 | pass |
| row_count | raw -> refined | `session_02/trades_AAPL.csv` | 1156 | 1156 | pass |
| row_count | refined -> curated | `session_02/trades_AAPL.csv` | 1156 | 1156 | pass |
| traded_quantity | source -> raw | `session_02/trades_AAPL.csv` | 29830 | 29830 | pass |
| traded_quantity | raw -> refined | `session_02/trades_AAPL.csv` | 29830 | 29830 | pass |
| traded_quantity | refined -> curated | `session_02/trades_AAPL.csv` | 29830 | 29830 | pass |
| matching_rows | source -> raw | `session_02/trades_AAPL.csv` | 0 | 0 | pass |
| matching_rows | raw -> refined | `session_02/trades_AAPL.csv` | 0 | 0 | pass |
| rejected_rows | raw -> refined | `session_02/trades_AAPL.csv` | 0 | 0 | pass |
| positive_quantity | refined | `session_02/trades_AAPL.csv` | 0 | 0 | pass |
| order_ids_exist | refined | `session_02/trades_AAPL.csv` | 0 | 0 | pass |
| lifecycle_fills | refined -> curated | `session_02/trades_AAPL.csv` | 59660 | 59660 | pass |
| row_count | source -> raw | `session_02/trades_GOOG.csv` | 1187 | 1187 | pass |
| row_count | raw -> refined | `session_02/trades_GOOG.csv` | 1187 | 1187 | pass |
| row_count | refined -> curated | `session_02/trades_GOOG.csv` | 1187 | 1187 | pass |
| traded_quantity | source -> raw | `session_02/trades_GOOG.csv` | 32026 | 32026 | pass |
| traded_quantity | raw -> refined | `session_02/trades_GOOG.csv` | 32026 | 32026 | pass |
| traded_quantity | refined -> curated | `session_02/trades_GOOG.csv` | 32026 | 32026 | pass |
| matching_rows | source -> raw | `session_02/trades_GOOG.csv` | 0 | 0 | pass |
| matching_rows | raw -> refined | `session_02/trades_GOOG.csv` | 0 | 0 | pass |
| rejected_rows | raw -> refined | `session_02/trades_GOOG.csv` | 0 | 0 | pass |
| positive_quantity | refined | `session_02/trades_GOOG.csv` | 0 | 0 | pass |
| order_ids_exist | refined | `session_02/trades_GOOG.csv` | 0 | 0 | pass |
| lifecycle_fills | refined -> curated | `session_02/trades_GOOG.csv` | 64052 | 64052 | pass |
| row_count | source -> raw | `session_02/trades_MSFT.csv` | 1187 | 1184 | FAIL |
| row_count | raw -> refined | `session_02/trades_MSFT.csv` | 1184 | 1184 | pass |
| row_count | refined -> curated | `session_02/trades_MSFT.csv` | 1184 | 1184 | pass |
| traded_quantity | source -> raw | `session_02/trades_MSFT.csv` | 30505 | 30423 | FAIL |
| traded_quantity | raw -> refined | `session_02/trades_MSFT.csv` | 30423 | 30423 | pass |
| traded_quantity | refined -> curated | `session_02/trades_MSFT.csv` | 30423 | 30423 | pass |
| matching_rows | source -> raw | `session_02/trades_MSFT.csv` | 0 | 4 | FAIL |
| matching_rows | raw -> refined | `session_02/trades_MSFT.csv` | 0 | 0 | pass |
| rejected_rows | raw -> refined | `session_02/trades_MSFT.csv` | 0 | 0 | pass |
| positive_quantity | refined | `session_02/trades_MSFT.csv` | 0 | 1 | FAIL |
| order_ids_exist | refined | `session_02/trades_MSFT.csv` | 0 | 0 | pass |
| lifecycle_fills | refined -> curated | `session_02/trades_MSFT.csv` | 60846 | 60846 | pass |
| row_count | source -> raw | `session_03/flow_AAPL.csv` | 5000 | 5000 | pass |
| row_count | raw -> refined | `session_03/flow_AAPL.csv` | 5000 | 5000 | pass |
| matching_rows | source -> raw | `session_03/flow_AAPL.csv` | 0 | 0 | pass |
| matching_rows | raw -> refined | `session_03/flow_AAPL.csv` | 0 | 0 | pass |
| rejected_rows | raw -> refined | `session_03/flow_AAPL.csv` | 0 | 0 | pass |
| positive_quantity | refined | `session_03/flow_AAPL.csv` | 0 | 0 | pass |
| row_count | source -> raw | `session_03/flow_GOOG.csv` | 5000 | 5000 | pass |
| row_count | raw -> refined | `session_03/flow_GOOG.csv` | 5000 | 5000 | pass |
| matching_rows | source -> raw | `session_03/flow_GOOG.csv` | 0 | 0 | pass |
| matching_rows | raw -> refined | `session_03/flow_GOOG.csv` | 0 | 0 | pass |
| rejected_rows | raw -> refined | `session_03/flow_GOOG.csv` | 0 | 0 | pass |
| positive_quantity | refined | `session_03/flow_GOOG.csv` | 0 | 0 | pass |
| row_count | source -> raw | `session_03/flow_MSFT.csv` | 5000 | 5000 | pass |
| row_count | raw -> refined | `session_03/flow_MSFT.csv` | 5000 | 5000 | pass |
| matching_rows | source -> raw | `session_03/flow_MSFT.csv` | 0 | 0 | pass |
| matching_rows | raw -> refined | `session_03/flow_MSFT.csv` | 0 | 0 | pass |
| rejected_rows | raw -> refined | `session_03/flow_MSFT.csv` | 0 | 0 | pass |
| positive_quantity | refined | `session_03/flow_MSFT.csv` | 0 | 0 | pass |
| row_count | source -> raw | `session_03/trades_AAPL.csv` | 1183 | 1183 | pass |
| row_count | raw -> refined | `session_03/trades_AAPL.csv` | 1183 | 1183 | pass |
| row_count | refined -> curated | `session_03/trades_AAPL.csv` | 1183 | 1183 | pass |
| traded_quantity | source -> raw | `session_03/trades_AAPL.csv` | 31266 | 31266 | pass |
| traded_quantity | raw -> refined | `session_03/trades_AAPL.csv` | 31266 | 31266 | pass |
| traded_quantity | refined -> curated | `session_03/trades_AAPL.csv` | 31266 | 31266 | pass |
| matching_rows | source -> raw | `session_03/trades_AAPL.csv` | 0 | 0 | pass |
| matching_rows | raw -> refined | `session_03/trades_AAPL.csv` | 0 | 0 | pass |
| rejected_rows | raw -> refined | `session_03/trades_AAPL.csv` | 0 | 0 | pass |
| positive_quantity | refined | `session_03/trades_AAPL.csv` | 0 | 0 | pass |
| order_ids_exist | refined | `session_03/trades_AAPL.csv` | 0 | 0 | pass |
| lifecycle_fills | refined -> curated | `session_03/trades_AAPL.csv` | 62532 | 62532 | pass |
| row_count | source -> raw | `session_03/trades_GOOG.csv` | 1178 | 1178 | pass |
| row_count | raw -> refined | `session_03/trades_GOOG.csv` | 1178 | 1178 | pass |
| row_count | refined -> curated | `session_03/trades_GOOG.csv` | 1178 | 1178 | pass |
| traded_quantity | source -> raw | `session_03/trades_GOOG.csv` | 29181 | 29181 | pass |
| traded_quantity | raw -> refined | `session_03/trades_GOOG.csv` | 29181 | 29181 | pass |
| traded_quantity | refined -> curated | `session_03/trades_GOOG.csv` | 29181 | 29181 | pass |
| matching_rows | source -> raw | `session_03/trades_GOOG.csv` | 0 | 0 | pass |
| matching_rows | raw -> refined | `session_03/trades_GOOG.csv` | 0 | 0 | pass |
| rejected_rows | raw -> refined | `session_03/trades_GOOG.csv` | 0 | 0 | pass |
| positive_quantity | refined | `session_03/trades_GOOG.csv` | 0 | 0 | pass |
| order_ids_exist | refined | `session_03/trades_GOOG.csv` | 0 | 0 | pass |
| lifecycle_fills | refined -> curated | `session_03/trades_GOOG.csv` | 58362 | 58362 | pass |
| row_count | source -> raw | `session_03/trades_MSFT.csv` | 1109 | 1109 | pass |
| row_count | raw -> refined | `session_03/trades_MSFT.csv` | 1109 | 1109 | pass |
| row_count | refined -> curated | `session_03/trades_MSFT.csv` | 1109 | 1109 | pass |
| traded_quantity | source -> raw | `session_03/trades_MSFT.csv` | 29271 | 29271 | pass |
| traded_quantity | raw -> refined | `session_03/trades_MSFT.csv` | 29271 | 29271 | pass |
| traded_quantity | refined -> curated | `session_03/trades_MSFT.csv` | 29271 | 29271 | pass |
| matching_rows | source -> raw | `session_03/trades_MSFT.csv` | 0 | 0 | pass |
| matching_rows | raw -> refined | `session_03/trades_MSFT.csv` | 0 | 0 | pass |
| rejected_rows | raw -> refined | `session_03/trades_MSFT.csv` | 0 | 0 | pass |
| positive_quantity | refined | `session_03/trades_MSFT.csv` | 0 | 0 | pass |
| order_ids_exist | refined | `session_03/trades_MSFT.csv` | 0 | 0 | pass |
| lifecycle_fills | refined -> curated | `session_03/trades_MSFT.csv` | 58542 | 58542 | pass |
