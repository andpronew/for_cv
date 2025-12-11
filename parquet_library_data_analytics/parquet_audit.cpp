// parquet_audit.cpp
// Build:
//   g++ -std=gnu++23 -O3 parquet_audit.cpp -lparquet -larrow -lzstd -o parquet_audit
//
// Usage:
//   ./parquet_audit file.parquet
//
// Output: one JSON object to stdout (one line) with summary metrics.

#include <parquet/api/reader.h>

#include <iostream>
#include <iomanip>
#include <string>
#include <vector>
#include <unordered_set>
#include <limits>
#include <cstdint>
#include <sstream>
#include <cmath>

using namespace std;

static int find_col_idx(const parquet::SchemaDescriptor* schema, const string& name) {
  for (int i = 0; i < schema->num_columns(); ++i)
    if (schema->Column(i)->path()->ToDotString() == name) return i;
  return -1;
}

int main(int argc, char** argv) {
  if (argc != 2) {
    cerr << "Usage: " << argv[0] << " file.parquet\n";
    return 2;
  }
  string path = argv[1];

  // Open parquet file
  unique_ptr<parquet::ParquetFileReader> reader;
  try {
    reader = parquet::ParquetFileReader::OpenFile(path, /*memory_map=*/true);
  } catch (const exception& e) {
    cerr << "{\"file\":\"" << path << "\",\"error\":\"open_failed\",\"msg\":\"" << e.what() << "\"}\n";
    return 0;
  }

  auto md = reader->metadata();
  auto schema = md->schema();
  int num_row_groups = md->num_row_groups();
  int ncols = schema->num_columns();
  int64_t total_rows_meta = md->num_rows();

  // find commonly interesting columns
  int idx_ts = find_col_idx(schema, "ts");
  int idx_px = find_col_idx(schema, "px");
  int idx_qty = find_col_idx(schema, "qty");
  int idx_tradeId = find_col_idx(schema, "tradeId");

  // Metrics to compute
  uint64_t total_rows = 0;
  int64_t ts_min = numeric_limits<int64_t>::max();
  int64_t ts_max = numeric_limits<int64_t>::min();
  uint64_t null_ts = 0;
  int64_t prev_ts = 0;
  bool have_prev_ts = false;
  uint64_t max_gap_ns = 0;
  uint64_t gaps_gt_1s = 0;
  uint64_t gaps_gt_100ms = 0;

  // px/qty stats
  bool have_px = (idx_px >= 0);
  bool have_qty = (idx_qty >= 0);
  long double sum_px = 0.0L; uint64_t cnt_px = 0;
  int64_t px_min = numeric_limits<int64_t>::max(), px_max = numeric_limits<int64_t>::min();
  long double sum_qty = 0.0L; uint64_t cnt_qty = 0;
  int64_t qty_min = numeric_limits<int64_t>::max(), qty_max = numeric_limits<int64_t>::min();
  uint64_t zero_px = 0, zero_qty = 0;

  // tradeId duplicates within file
  bool have_tradeId = (idx_tradeId >= 0);
  unordered_set<uint64_t> tradeid_seen;
  uint64_t dup_tradeid = 0;
  uint64_t tradeid_min = numeric_limits<uint64_t>::max(), tradeid_max = 0;

  // null counts for any column
  vector<uint64_t> null_counts(ncols, 0);

  // iterate row groups
  for (int rg = 0; rg < num_row_groups; ++rg) {
    auto rg_reader = reader->RowGroup(rg);
    int64_t rows = rg_reader->metadata()->num_rows();
    if (rows <= 0) continue;

    // Prepare storage for columns we need: read as int64 or bool
    vector<int64_t> buf_ts;
    vector<int64_t> buf_px;
    vector<int64_t> buf_qty;
    vector<int64_t> buf_tradeId;

    // read selected columns if exist
    auto read_i64 = [&](int col_idx, vector<int64_t>& outbuf) {
      auto col = rg_reader->Column(col_idx);
      auto* r = static_cast<parquet::Int64Reader*>(col.get());
      outbuf.resize(rows);
      int64_t done = 0;
      while (done < rows) {
        int64_t values_read = 0;
        int64_t levels = r->ReadBatch(rows - done, nullptr, nullptr, outbuf.data() + done, &values_read);
        if (levels == 0 && values_read == 0) break;
        done += values_read;
      }
      if (done != rows) outbuf.resize(done);
    };

    if (idx_ts >= 0) read_i64(idx_ts, buf_ts);
    if (idx_px >= 0) read_i64(idx_px, buf_px);
    if (idx_qty >= 0) read_i64(idx_qty, buf_qty);
    if (idx_tradeId >= 0) read_i64(idx_tradeId, buf_tradeId);

    // Adjust row count to minimal available across selected columns (safety)
    int64_t nrows_actual = rows;
    if (idx_ts >= 0) nrows_actual = min<int64_t>(nrows_actual, (int64_t)buf_ts.size());
    if (idx_px >= 0) nrows_actual = min<int64_t>(nrows_actual, (int64_t)buf_px.size());
    if (idx_qty >= 0) nrows_actual = min<int64_t>(nrows_actual, (int64_t)buf_qty.size());
    if (idx_tradeId >= 0) nrows_actual = min<int64_t>(nrows_actual, (int64_t)buf_tradeId.size());

    // Process rows
    for (int64_t i = 0; i < nrows_actual; ++i) {
      ++total_rows;

      // ts
      if (idx_ts >= 0) {
        int64_t t = buf_ts[i];
        // assume non-null because schema often required; if value looks like sentinel, still process
        ts_min = min(ts_min, t);
        ts_max = max(ts_max, t);

        if (have_prev_ts) {
          uint64_t gap = (t >= prev_ts) ? (uint64_t)(t - prev_ts) : 0;
          if (gap > max_gap_ns) max_gap_ns = gap;
          if (gap >= 1000000000ULL) ++gaps_gt_1s;
          if (gap >= 100000000ULL) ++gaps_gt_100ms;
        }
        prev_ts = t;
        have_prev_ts = true;
      } else {
        ++null_ts;
      }

      // px
      if (idx_px >= 0) {
        int64_t v = buf_px[i];
        ++cnt_px;
        sum_px += (long double)v;
        if (v < px_min) px_min = v;
        if (v > px_max) px_max = v;
        if (v == 0) ++zero_px;
      }
      // qty
      if (idx_qty >= 0) {
        int64_t v = buf_qty[i];
        ++cnt_qty;
        sum_qty += (long double)v;
        if (v < qty_min) qty_min = v;
        if (v > qty_max) qty_max = v;
        if (v == 0) ++zero_qty;
      }
      // tradeId
      if (idx_tradeId >= 0) {
        uint64_t tid = static_cast<uint64_t>(buf_tradeId[i]);
        if (tradeid_seen.find(tid) != tradeid_seen.end()) ++dup_tradeid;
        else tradeid_seen.insert(tid);
        if (tid < tradeid_min) tradeid_min = tid;
        if (tid > tradeid_max) tradeid_max = tid;
      }
    }

    // If some columns had fewer elements than declared rows, count missing as nulls
    if (idx_ts >= 0 && (int64_t)buf_ts.size() < rows) null_counts[idx_ts] += (rows - buf_ts.size());
    if (idx_px >= 0 && (int64_t)buf_px.size() < rows) null_counts[idx_px] += (rows - buf_px.size());
    if (idx_qty >= 0 && (int64_t)buf_qty.size() < rows) null_counts[idx_qty] += (rows - buf_qty.size());
    if (idx_tradeId >= 0 && (int64_t)buf_tradeId.size() < rows) null_counts[idx_tradeId] += (rows - buf_tradeId.size());
  } // rg

  // Prepare output JSON
  ostringstream o;
  o << "{";
  o << "\"file\":\"" << path << "\"";
  o << ",\"meta_rows\":" << total_rows_meta;
  o << ",\"rows_scanned\":" << total_rows;
  o << ",\"row_groups\":" << num_row_groups;
  if (idx_ts >= 0) {
    o << ",\"ts_min\":" << (ts_min==numeric_limits<int64_t>::max() ? 0 : ts_min);
    o << ",\"ts_max\":" << (ts_max==numeric_limits<int64_t>::min() ? 0 : ts_max);
    o << ",\"max_gap_ns\":" << max_gap_ns;
    o << ",\"gaps_gt_1s\":" << gaps_gt_1s;
    o << ",\"gaps_gt_100ms\":" << gaps_gt_100ms;
  } else {
    o << ",\"ts_present\":false";
  }
  if (have_px) {
    long double avg_px = cnt_px ? (sum_px / (long double)cnt_px) : 0.0L;
    o << ",\"px_min\":" << (px_min==numeric_limits<int64_t>::max() ? 0 : px_min);
    o << ",\"px_max\":" << (px_max==numeric_limits<int64_t>::min() ? 0 : px_max);
    o << ",\"px_avg\":" << fixed << setprecision(6) << (double)avg_px;
    o << ",\"px_zero_count\":" << zero_px;
  } else {
    o << ",\"px_present\":false";
  }
  if (have_qty) {
    long double avg_qty = cnt_qty ? (sum_qty / (long double)cnt_qty) : 0.0L;
    o << ",\"qty_min\":" << (qty_min==numeric_limits<int64_t>::max() ? 0 : qty_min);
    o << ",\"qty_max\":" << (qty_max==numeric_limits<int64_t>::min() ? 0 : qty_max);
    o << ",\"qty_avg\":" << fixed << setprecision(6) << (double)avg_qty;
    o << ",\"qty_zero_count\":" << zero_qty;
  } else {
    o << ",\"qty_present\":false";
  }

  if (have_tradeId) {
    o << ",\"tradeId_min\":" << (tradeid_min==numeric_limits<uint64_t>::max()?0:tradeid_min);
    o << ",\"tradeId_max\":" << tradeid_max;
    o << ",\"dup_tradeid\":" << dup_tradeid;
  } else {
    o << ",\"tradeId_present\":false";
  }

  // null counts (only for columns we care)
  o << ",\"null_counts\":{";
  bool first_nc = true;
  if (idx_ts >= 0) { if (!first_nc) o<<","; o << "\"ts\":"<<null_counts[idx_ts]; first_nc=false; }
  if (idx_px >= 0) { if (!first_nc) o<<","; o << "\"px\":"<<null_counts[idx_px]; first_nc=false; }
  if (idx_qty >= 0) { if (!first_nc) o<<","; o << "\"qty\":"<<null_counts[idx_qty]; first_nc=false; }
  if (idx_tradeId >= 0) { if (!first_nc) o<<","; o << "\"tradeId\":"<<null_counts[idx_tradeId]; first_nc=false; }
  o << "}";

  o << "}\n";

  cout << o.str();
  return 0;
}

