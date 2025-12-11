# 📘 Parquet Audit Toolkit (C++23, Apache Arrow/Parquet)

A high-performance C++23 toolkit for auditing Binance spot market Parquet datasets (top, depth, trade).
The utilities rely on Apache Arrow + Parquet, operate at scale, and detect dozens of real-world anomalies in tick-level datasets.

The toolkit includes:

✔ parquet_audit_new.cpp — universal auditor for top/depth/trade files 

✔ parquet_audit_readme_new

✔ parquet_depth_audit.cpp — in-depth validator for order book delta Parquet files

✔ parquet_trade_spot_audit.cpp — trade-level anomaly detector

✔ parquet_top_spot_audit.cpp — top-of-book anomaly detector

✔ parquet_reader.cpp + parquet_reader_lib.* — an interactive Parquet inspector

✔ parquet2csv.cpp — converter of Parquet → CSV (for debugging / manual inspection)

The toolkit is designed for HFT / crypto market data pipelines, where correctness of parquet snapshots is critical.

### 🚀 Key Features
✔ Fast C++23 implementation (GNU++23/Clang++23)

✔ High-performance scanning of huge Parquet datasets (millions of rows)

✔ Detailed structural and semantic anomaly detection

✔ Unified audit report in plain text or NDJSON

✔ Automatic detection of file type (top / trade / depth)

✔ Support for both:

flattened arrays with offsets;

flattened arrays without offsets (flagged as informational limitation)

### 📂 Project Structure
```text
/
├── parquet_audit_new.cpp          # Main multi-format auditor  (top/depth/trade)
├── parquet_reader.cpp             # Pretty-print column values for debugging
├── parquet_reader_lib.cpp/.h      # Shared Parquet-reading utilities
├── parquet2csv.cpp                # Parquet → CSV converter
├── parquet_top_spot_audit.cpp     # Top-of-book anomaly detector
├── parquet_depth_audit.cpp        # Depth-book (delta) anomaly detector
├── parquet_trade_spot_audit.cpp   # Trade-file anomaly detector
└── README.md                      # (this file)
```

### 🛠️ Build Instructions
Dependencies

```
Apache Arrow / Parquet

zstd

C++23 compiler (g++ ≥ 12 or clang++ ≥ 15)

Example build commands
g++ -std=gnu++23 -O3 parquet_audit_new.cpp -lparquet -larrow -lzstd -o parquet_audit_new
g++ -std=gnu++23 -O3 parquet_depth_audit.cpp -lparquet -larrow -lzstd -o parquet_depth_audit
g++ -std=gnu++23 -O3 parquet_trade_spot_audit.cpp -lparquet -larrow -lzstd -o parquet_trade_spot_audit
g++ -std=gnu++23 -O3 parquet_top_spot_audit.cpp -lparquet -larrow -lzstd -o parquet_top_spot_audit
```

### 📊 1. Universal Auditor — parquet_audit_new.cpp

Multi-format analyzer for:

```top_spot

trade_spot

depth_spot
```
Usage
./parquet_audit_new /path/to/*.parquet --out=report.txt

Detects dozens of anomalies, including:
A. Time anomalies
```
non_monotonic_ts — timestamps go backwards

huge gaps in timestamps

corrupted row-groups
```
B. ID anomalies
```
lastId < firstId

id_overlap_count (duplicate deltas)

id_gap_count (missing segments)
```
C. Structural mismatches
```
flattened arrays without offsets

offset mismatches

missing columns

mismatched element counts

zero counts in px/qty arrays

arrays declared but empty
```
D. Market data logic
```
crossed_book_count — best bid ≥ best ask

price_change_10x_count — price jumps more than ×10

price_not_div1000_count — price not in correct tick size

qty_not_div1e8_count — quantity not in minimal unit

extreme qty deviations (>1e6× from avg)

Output example
Auditing: bn_top_spot_DFUSDT_2025_09_07.parquet
No problematic files found.


or for depth:

Auditing: bn_depth_spot_DFUSDT_2025_09_08.parquet
Wrote audit report to: parquet_audit_report_depth.txt (problematic files: 309)
```
### 📊 2. Trade Spot Auditor — parquet_trade_spot_audit.cpp
Purpose

Detect anomalies in Binance spot trade parquet files.

Flags anomalous when:
```
empty files (rows_scanned == 0)

metadata mismatch (rows_scanned != meta_rows)

duplicate trade IDs

null values in ts/px/qty/tradeId

timestamp monotonicity issues

statistical outliers:

rows_ratio

price mean

qty mean

ts gaps

z-score > 3 deviations
```
Produces NDJSON:

./parquet_trade_spot_audit dir/ anomalies.ndjson

### 📊 3. Depth Spot Auditor — parquet_depth_audit.cpp

Specialized for order book depth delta Parquet files.

Detects:
```
missing/null ts, firstId, lastId, eventTime

lastId < firstId

ID overlaps or gaps

inconsistencies in bid/ask arrays

too many zeros in px/qty

per-row array mismatch

crossed book (bid ≥ ask)

abnormal price/qty changes

improperly flattened arrays

extremely small/large files (likely incomplete)
```
Example:
./parquet_depth_audit /path/to/*.parquet anomalies_depth.ndjson

### 📊 4. Top Spot Auditor — parquet_top_spot_audit.cpp

Analyzes top-of-book snapshots.

Flags:
```
null bid/ask price/qty

timestamp monotonicity issues

crossed book

large gaps between snapshots

duplicate snapshots

10% zeros in px/qty

statistical outliers for px_avg, qty_avg, row count

incomplete files (meta_rows < threshold)
```
### 🧪 5. Parquet Reader — parquet_reader.cpp

A simple inspector tool that prints:
```
schema

row groups

column contents

per-row values
```
Useful for debugging anomalies found by auditors.

### 🔄 6. Parquet → CSV converter — parquet2csv.cpp

Not part of the audit pipeline.
Used to convert Parquet into human-readable CSV for debugging.

### 🧠 Interpretation of Anomalies
Critical anomalies (file considered “problematic”):
```
timestamps going backwards

missing mandatory fields

structural inconsistencies in arrays

crossed book

corrupted row-group (unexpected end-of-stream)

mismatched metadata

duplicate or missing IDs

statistical outliers (z-score > 3)

Informational (do not fail file unless --include-info)

flattened arrays without offsets

very small files

anomalies in global aggregated stats
```
### ⚠️ Error Handling

Errors that do not appear in NDJSON (because file could not be read at all):

ERROR: open/read failed … Unexpected end of stream: Read 0 values, expected N


Meaning:
```
Parquet file is physically corrupted

interrupted download

broken footer

damaged row-group
```
These filenames should be captured separately in failed_files.txt.

### 🧩 Supported Parquet Schemas

Toolkit supports:
```
flattened arrays

arrays with offset columns

nested list encoding (Arrow style)

scalar top-level columns

mixed-schema depth files (bid/ask arrays, eventTime, IDs)
```
### 📈 Example Audit Flow
```
Universal multi-type audit
./parquet_audit_new dir/*.parquet --out=report.txt

Depth-focused audit (NDJSON)
./parquet_depth_audit dir/*.parquet anomalies_depth.ndjson

Trade audit
./parquet_trade_spot_audit dir/*.parquet anomalies_trade.ndjson

Top-of-book snapshots
./parquet_top_spot_audit dir/*.parquet anomalies_top.ndjson

Debug single file
./parquet_reader file.parquet
```
### 🏁 Summary

This toolkit provides:

✔ High-performance C++23 Parquet scanning

✔ Depth, top, and trade-specific anomaly detectors

✔ Multi-layer consistency checks

✔ Statistical outlier detection

✔ Clean text or NDJSON reports

✔ Practical tools for building a robust market-data pipeline

It is suitable for:

✔ HFT/quant research

✔ validating downloaded Binance datasets

✔ building ETL pipelines

✔ academic research on order-book microstructure
