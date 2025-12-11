// parquet_trade_spot_audit.cpp
// Scan top_spot parquet files and detect anomalies.
// Build:
//   g++ -std=gnu++23 -O3 parquet_trade_spot_audit.cpp -lparquet -larrow -lzstd -o parquet_trade_spot_audit
//
// Usage:
//   ./parquet_trade_spot_audit /path/to/parquets output.ndjson [--all]
//
// Produces NDJSON; by default writes only files that have anomalies. Use --all to emit all files.

#include <parquet/api/reader.h>

#include <filesystem>
#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <limits>
#include <cstdint>
#include <cmath>
#include <iomanip>

using namespace std;
namespace fs = std::filesystem;

static int find_col_idx(const parquet::SchemaDescriptor* schema, const string& name) {
    for (int i = 0; i < schema->num_columns(); ++i)
        if (schema->Column(i)->path()->ToDotString() == name) return i;
    return -1;
}

static void read_i64_column(parquet::RowGroupReader& rg, int col_idx, vector<int64_t>& out) {
    const int64_t rows = rg.metadata()->num_rows();
    out.clear();
    out.resize(rows);
    auto col = rg.Column(col_idx);
    auto* r = static_cast<parquet::Int64Reader*>(col.get());
    int64_t done = 0;
    while (done < rows) {
        int64_t values_read = 0;
        int64_t levels = r->ReadBatch(rows - done, nullptr, nullptr, out.data() + done, &values_read);
        if (levels == 0 && values_read == 0) break;
        done += values_read;
    }
    if (done != rows) out.resize(done);
}

// Simple Welford accumulator for mean/stddev if needed later
struct Welford {
    long double mean = 0.0L, m2 = 0.0L;
    uint64_t n = 0;
    void add(long double x) {
        ++n;
        long double delta = x - mean;
        mean += delta / (long double)n;
        long double delta2 = x - mean;
        m2 += delta * delta2;
    }
    long double variance() const { return (n > 1) ? (m2 / (long double)(n - 1)) : 0.0L; }
    long double stddev() const { return sqrt((double)variance()); }
};

struct FileMetric {
    string path;
    int64_t meta_rows = 0;
    int64_t rows_scanned = 0;
    int row_groups = 0;

    bool has_ts=false;
    int64_t ts_min = numeric_limits<int64_t>::max();
    int64_t ts_max = numeric_limits<int64_t>::min();
    uint64_t max_gap_ns = 0;
    uint64_t gaps_gt_100ms = 0;
    uint64_t gaps_gt_1s = 0;
    uint64_t non_monotonic_ts = 0;

    bool has_bid_px=false, has_bid_qty=false, has_ask_px=false, has_ask_qty=false, has_valu=false;
    int64_t bid_px_min = numeric_limits<int64_t>::max(), bid_px_max = numeric_limits<int64_t>::min();
    long double bid_px_avg = 0.0L; uint64_t bid_px_zero = 0; uint64_t bid_px_count = 0;
    int64_t ask_px_min = numeric_limits<int64_t>::max(), ask_px_max = numeric_limits<int64_t>::min();
    long double ask_px_avg = 0.0L; uint64_t ask_px_zero = 0; uint64_t ask_px_count = 0;
    int64_t bid_qty_min = numeric_limits<int64_t>::max(), bid_qty_max = numeric_limits<int64_t>::min();
    long double bid_qty_avg = 0.0L; uint64_t bid_qty_zero = 0; uint64_t bid_qty_count = 0;
    int64_t ask_qty_min = numeric_limits<int64_t>::max(), ask_qty_max = numeric_limits<int64_t>::min();
    long double ask_qty_avg = 0.0L; uint64_t ask_qty_zero = 0; uint64_t ask_qty_count = 0;

    uint64_t null_ts=0, null_bid_px=0, null_bid_qty=0, null_ask_px=0, null_ask_qty=0, null_valu=0;

    uint64_t duplicate_snapshot_count = 0; // consecutive identical snapshots
    uint64_t cross_book_count = 0; // bid_px > ask_px
    uint64_t repeated_ts_count = 0; // same ts repeated (not necessarily bad)
    long double valu_avg = 0.0L; uint64_t valu_count = 0; int64_t valu_min = numeric_limits<int64_t>::max(), valu_max = numeric_limits<int64_t>::min();
};

static bool analyze_top_file(const string& path, FileMetric& out, string* err_out=nullptr) {
    out = FileMetric();
    out.path = path;
    try {
        unique_ptr<parquet::ParquetFileReader> reader = parquet::ParquetFileReader::OpenFile(path, /*memory_map=*/true);
        auto md = reader->metadata();
        auto schema = md->schema();
        out.meta_rows = md->num_rows();
        out.row_groups = md->num_row_groups();

        // find columns (accept common names)
        int idx_ts = find_col_idx(schema, "ts");
        int idx_bid_px = find_col_idx(schema, "bid_px"); if (idx_bid_px < 0) idx_bid_px = find_col_idx(schema, "bidprice"); if (idx_bid_px < 0) idx_bid_px = find_col_idx(schema, "bid.price");
        int idx_bid_qty = find_col_idx(schema, "bid_qty"); if (idx_bid_qty < 0) idx_bid_qty = find_col_idx(schema, "bidqty");
        int idx_ask_px = find_col_idx(schema, "ask_px"); if (idx_ask_px < 0) idx_ask_px = find_col_idx(schema, "askprice"); if (idx_ask_px < 0) idx_ask_px = find_col_idx(schema, "ask.price");
        int idx_ask_qty = find_col_idx(schema, "ask_qty"); if (idx_ask_qty < 0) idx_ask_qty = find_col_idx(schema, "askqty");
        int idx_valu = find_col_idx(schema, "valu"); if (idx_valu < 0) idx_valu = find_col_idx(schema, "value");

        out.has_ts = (idx_ts >= 0);
        out.has_bid_px = (idx_bid_px >= 0);
        out.has_bid_qty = (idx_bid_qty >= 0);
        out.has_ask_px = (idx_ask_px >= 0);
        out.has_ask_qty = (idx_ask_qty >= 0);
        out.has_valu = (idx_valu >= 0);

        Welford bid_px_w, ask_px_w, bid_qty_w, ask_qty_w, valu_w;

        // to detect duplicate consecutive snapshots: remember previous tuple
        int64_t prev_ts = 0;
        int64_t prev_bid_px=LLONG_MIN, prev_ask_px=LLONG_MIN, prev_bid_qty=LLONG_MIN, prev_ask_qty=LLONG_MIN;
        bool have_prev = false;

        for (int rg=0; rg<out.row_groups; ++rg) {
            auto rg_reader = reader->RowGroup(rg);
            int64_t rows = rg_reader->metadata()->num_rows();
            if (rows <= 0) continue;

            vector<int64_t> v_ts, v_bid_px, v_bid_qty, v_ask_px, v_ask_qty, v_valu;
            if (out.has_ts) read_i64_column(*rg_reader, idx_ts, v_ts);
            if (out.has_bid_px) read_i64_column(*rg_reader, idx_bid_px, v_bid_px);
            if (out.has_bid_qty) read_i64_column(*rg_reader, idx_bid_qty, v_bid_qty);
            if (out.has_ask_px) read_i64_column(*rg_reader, idx_ask_px, v_ask_px);
            if (out.has_ask_qty) read_i64_column(*rg_reader, idx_ask_qty, v_ask_qty);
            if (out.has_valu) read_i64_column(*rg_reader, idx_valu, v_valu);

            int64_t nrows = rows;
            if (out.has_ts) nrows = min<int64_t>(nrows, (int64_t)v_ts.size());
            if (out.has_bid_px) nrows = min<int64_t>(nrows, (int64_t)v_bid_px.size());
            if (out.has_bid_qty) nrows = min<int64_t>(nrows, (int64_t)v_bid_qty.size());
            if (out.has_ask_px) nrows = min<int64_t>(nrows, (int64_t)v_ask_px.size());
            if (out.has_ask_qty) nrows = min<int64_t>(nrows, (int64_t)v_ask_qty.size());
            if (out.has_valu) nrows = min<int64_t>(nrows, (int64_t)v_valu.size());

            for (int64_t i=0;i<nrows;++i) {
                ++out.rows_scanned;

                // ts
                if (out.has_ts) {
                    int64_t t = v_ts[i];
                    if (t < out.ts_min) out.ts_min = t;
                    if (t > out.ts_max) out.ts_max = t;
                    if (have_prev) {
                        if (t < prev_ts) ++out.non_monotonic_ts;
                        uint64_t gap = (t >= prev_ts) ? (uint64_t)(t - prev_ts) : 0;
                        if (gap > out.max_gap_ns) out.max_gap_ns = gap;
                        if (gap >= 100000000ULL) ++out.gaps_gt_100ms;
                        if (gap >= 1000000000ULL) ++out.gaps_gt_1s;
                        if (t == prev_ts) ++out.repeated_ts_count;
                    }
                    prev_ts = t;
                } else {
                    ++out.null_ts;
                }

                // bid/ask px/qty
                bool cur_has_bid_px = out.has_bid_px && (i < (int64_t)v_bid_px.size());
                bool cur_has_ask_px = out.has_ask_px && (i < (int64_t)v_ask_px.size());
                bool cur_has_bid_qty = out.has_bid_qty && (i < (int64_t)v_bid_qty.size());
                bool cur_has_ask_qty = out.has_ask_qty && (i < (int64_t)v_ask_qty.size());
                if (!cur_has_bid_px) ++out.null_bid_px;
                if (!cur_has_bid_qty) ++out.null_bid_qty;
                if (!cur_has_ask_px) ++out.null_ask_px;
                if (!cur_has_ask_qty) ++out.null_ask_qty;

                int64_t cur_bid_px = cur_has_bid_px ? v_bid_px[i] : 0;
                int64_t cur_ask_px = cur_has_ask_px ? v_ask_px[i] : 0;
                int64_t cur_bid_qty = cur_has_bid_qty ? v_bid_qty[i] : 0;
                int64_t cur_ask_qty = cur_has_ask_qty ? v_ask_qty[i] : 0;

                if (cur_has_bid_px) {
                    bid_px_w.add((long double)cur_bid_px);
                    if (cur_bid_px < out.bid_px_min) out.bid_px_min = cur_bid_px;
                    if (cur_bid_px > out.bid_px_max) out.bid_px_max = cur_bid_px;
                    if (cur_bid_px == 0) ++out.bid_px_zero;
                    ++out.bid_px_count;
                }
                if (cur_has_ask_px) {
                    ask_px_w.add((long double)cur_ask_px);
                    if (cur_ask_px < out.ask_px_min) out.ask_px_min = cur_ask_px;
                    if (cur_ask_px > out.ask_px_max) out.ask_px_max = cur_ask_px;
                    if (cur_ask_px == 0) ++out.ask_px_zero;
                    ++out.ask_px_count;
                }
                if (cur_has_bid_qty) {
                    bid_qty_w.add((long double)cur_bid_qty);
                    if (cur_bid_qty < out.bid_qty_min) out.bid_qty_min = cur_bid_qty;
                    if (cur_bid_qty > out.bid_qty_max) out.bid_qty_max = cur_bid_qty;
                    if (cur_bid_qty == 0) ++out.bid_qty_zero;
                    ++out.bid_qty_count;
                }
                if (cur_has_ask_qty) {
                    ask_qty_w.add((long double)cur_ask_qty);
                    if (cur_ask_qty < out.ask_qty_min) out.ask_qty_min = cur_ask_qty;
                    if (cur_ask_qty > out.ask_qty_max) out.ask_qty_max = cur_ask_qty;
                    if (cur_ask_qty == 0) ++out.ask_qty_zero;
                    ++out.ask_qty_count;
                }

                if (out.has_valu && i < (int64_t)v_valu.size()) {
                    int64_t v = v_valu[i];
                    valu_w.add((long double)v);
                    if (v < out.valu_min) out.valu_min = v;
                    if (v > out.valu_max) out.valu_max = v;
                    ++out.valu_count;
                } else {
                    if (!out.has_valu) ++out.null_valu;
                }

                // cross book
                if (cur_has_bid_px && cur_has_ask_px) {
                    if (cur_bid_px > cur_ask_px) ++out.cross_book_count;
                }

                // duplicate snapshot detection (consecutive identical snapshots)
                if (have_prev &&
                    cur_has_bid_px && cur_has_ask_px && cur_has_bid_qty && cur_has_ask_qty &&
                    cur_bid_px == prev_bid_px && cur_ask_px == prev_ask_px &&
                    cur_bid_qty == prev_bid_qty && cur_ask_qty == prev_ask_qty) {
                    ++out.duplicate_snapshot_count;
                }

                // set previous per-row values
                if (cur_has_bid_px) prev_bid_px = cur_bid_px; else prev_bid_px = LLONG_MIN;
                if (cur_has_ask_px) prev_ask_px = cur_ask_px; else prev_ask_px = LLONG_MIN;
                if (cur_has_bid_qty) prev_bid_qty = cur_bid_qty; else prev_bid_qty = LLONG_MIN;
                if (cur_has_ask_qty) prev_ask_qty = cur_ask_qty; else prev_ask_qty = LLONG_MIN;
                have_prev = true;
            } // rows in RG
        } // row groups

        out.bid_px_avg = (bid_px_w.n > 0) ? bid_px_w.mean : 0.0L;
        out.ask_px_avg = (ask_px_w.n > 0) ? ask_px_w.mean : 0.0L;
        out.bid_qty_avg = (bid_qty_w.n > 0) ? bid_qty_w.mean : 0.0L;
        out.ask_qty_avg = (ask_qty_w.n > 0) ? ask_qty_w.mean : 0.0L;
        out.valu_avg = (valu_w.n > 0) ? valu_w.mean : 0.0L;

        return true;
    } catch (const exception& e) {
        if (err_out) *err_out = e.what();
        return false;
    }
}

// escape json-string
static string esc(const string &s) {
    string r; r.reserve(s.size()*2);
    for (char c : s) {
        if (c == '\\') r += "\\\\";
        else if (c == '"') r += "\\\"";
        else if (c == '\n') r += "\\n";
        else if (c == '\r') r += "\\r";
        else r.push_back(c);
    }
    return r;
}

int main(int argc, char** argv) {
    if (argc < 3) {
        cerr << "Usage: " << argv[0] << " /path/to/parquets output.ndjson [--all]\n";
        return 1;
    }

    // Print anomaly rules
    cerr << "Anomaly rules applied (file reported if any match):\n";
    cerr << "  1) rows_scanned == 0\n";
    cerr << "  2) rows_scanned != meta_rows\n";
    cerr << "  3) null_counts > 0 for ts/bid_px/bid_qty/ask_px/ask_qty/valu\n";
    cerr << "  4) non_monotonic_ts > 0\n";
    cerr << "  5) gaps > 100ms or >1s (gaps_gt_100ms/gaps_gt_1s)\n";
    cerr << "  6) cross_book_count > 0 (bid_px > ask_px)\n";
    cerr << "  7) high fraction of px==0 or qty==0 (>10%)\n";
    cerr << "  8) duplicate consecutive snapshots (duplicate_snapshot_count > 0)\n";
    cerr << "  9) meta_rows < 100 (small file)\n";
    cerr << " 10) statistical outliers across files (z-score > 3) for rows_ratio/px_avg/qty_avg/max_gap (computed after scan)\n\n";

    string dir = argv[1];
    string out_path = argv[2];
    bool write_all = false;
    if (argc >= 4 && string(argv[3]) == "--all") write_all = true;

    vector<string> files;
    for (auto &p : fs::directory_iterator(dir)) {
        if (!p.is_regular_file()) continue;
        if (p.path().extension() == ".parquet") files.push_back(p.path().string());
    }
    if (files.empty()) { cerr << "No .parquet files found in " << dir << "\n"; return 1; }

    vector<FileMetric> metrics;
    metrics.reserve(files.size());

    ofstream fout(out_path);
    if (!fout.is_open()) { cerr << "Failed to open output " << out_path << "\n"; return 1; }

    // First pass: analyze all files; write error entries for files that failed to read
    cerr << "Scanning " << files.size() << " files...\n";
    for (size_t i=0;i<files.size();++i) {
        const string &f = files[i];
        cerr << "[" << (i+1) << "/" << files.size() << "] " << f << " ... " << flush;
        FileMetric fm;
        string err;
        bool ok = analyze_top_file(f, fm, &err);
        if (!ok) {
            cerr << "ERROR: " << err << "\n";
            // write JSON for failed file (so it is included in NDJSON)
            ostringstream j;
            j << "{\"file\":\"" << esc(f) << "\",\"error\":\"" << esc(err) << "\",\"anomalies\":[\"open_read_failed\"]}\n";
            fout << j.str();
            continue;
        }
        cerr << "ok (rows=" << fm.rows_scanned << ")\n";
        metrics.push_back(std::move(fm));
    }

    // compute global statistics (for z-score outliers)
    Welford w_rows_ratio, w_px_avg, w_qty_avg, w_max_gap;
    for (auto &m : metrics) {
        long double ratio = (m.meta_rows>0) ? ((long double)m.rows_scanned / (long double)m.meta_rows) : 0.0L;
        w_rows_ratio.add(ratio);
        if (m.bid_px_count>0) w_px_avg.add((long double)m.bid_px_avg);
        if (m.bid_qty_count>0) w_qty_avg.add((long double)m.bid_qty_avg);
        w_max_gap.add((long double)m.max_gap_ns);
    }
    auto zscore = [&](const Welford &w, long double v)->long double {
        long double sd = w.stddev();
        if (sd <= 0.0L) return 0.0L;
        return fabsl((v - w.mean) / sd);
    };

    const long double Z_THRESH = 3.0L;
    const double ZERO_FRAC_THRESH_PERCENT = 10.0; // 10%
    const int MIN_META_ROWS = 100;

    // Second pass: decide anomalies and write JSON for each (only anomalous unless --all)
    for (auto &m : metrics) {
        vector<string> anomalies;
        if (m.rows_scanned == 0) anomalies.push_back("rows_scanned == 0");
        if (m.meta_rows != m.rows_scanned) anomalies.push_back("rows_scanned != meta_rows");
        if (m.null_ts>0 || m.null_bid_px>0 || m.null_bid_qty>0 || m.null_ask_px>0 || m.null_ask_qty>0 || m.null_valu>0)
            anomalies.push_back("null_counts > 0");

        if (m.non_monotonic_ts > 0) anomalies.push_back("non_monotonic_ts > 0");
        if (m.gaps_gt_100ms > 0) anomalies.push_back("gaps_gt_100ms > 0");
        if (m.gaps_gt_1s > 0) anomalies.push_back("gaps_gt_1s > 0");
        if (m.cross_book_count > 0) anomalies.push_back("cross_book_count > 0 (bid_px > ask_px)");
        if (m.duplicate_snapshot_count > 0) anomalies.push_back("duplicate_snapshot_count > 0");

        auto pct = [](uint64_t zero, uint64_t total)->double {
            if (total==0) return 0.0;
            return 100.0 * (double)zero / (double)total;
        };
        if (m.bid_px_count>0 && pct(m.bid_px_zero, m.bid_px_count) > ZERO_FRAC_THRESH_PERCENT) anomalies.push_back("high_fraction_bid_px_zero");
        if (m.ask_px_count>0 && pct(m.ask_px_zero, m.ask_px_count) > ZERO_FRAC_THRESH_PERCENT) anomalies.push_back("high_fraction_ask_px_zero");
        if (m.bid_qty_count>0 && pct(m.bid_qty_zero, m.bid_qty_count) > ZERO_FRAC_THRESH_PERCENT) anomalies.push_back("high_fraction_bid_qty_zero");
        if (m.ask_qty_count>0 && pct(m.ask_qty_zero, m.ask_qty_count) > ZERO_FRAC_THRESH_PERCENT) anomalies.push_back("high_fraction_ask_qty_zero");

        if (m.meta_rows > 0 && m.meta_rows < MIN_META_ROWS) anomalies.push_back("meta_rows < 100 (small file)");

        // statistical outliers (z-score)
        long double z_rows = (m.meta_rows>0) ? zscore(w_rows_ratio, ((long double)m.rows_scanned / (long double)m.meta_rows)) : 0.0L;
        if (z_rows > Z_THRESH) anomalies.push_back("rows_ratio statistical_outlier");

        if (m.bid_px_count>0) {
            long double z_px = zscore(w_px_avg, (long double)m.bid_px_avg);
            if (z_px > Z_THRESH) anomalies.push_back("px_avg statistical_outlier");
        }
        if (m.bid_qty_count>0) {
            long double z_qty = zscore(w_qty_avg, (long double)m.bid_qty_avg);
            if (z_qty > Z_THRESH) anomalies.push_back("qty_avg statistical_outlier");
        }
        long double z_gap = zscore(w_max_gap, (long double)m.max_gap_ns);
        if (z_gap > Z_THRESH) anomalies.push_back("max_gap_ns statistical_outlier");

        if (anomalies.empty() && !write_all) continue;

        // compose JSON
        ostringstream o;
        o << "{";
        o << "\"file\":\"" << esc(m.path) << "\"";
        o << ",\"meta_rows\":" << m.meta_rows;
        o << ",\"rows_scanned\":" << m.rows_scanned;
        o << ",\"row_groups\":" << m.row_groups;
        if (m.has_ts) {
            o << ",\"ts_min\":" << m.ts_min << ",\"ts_max\":" << m.ts_max << ",\"max_gap_ns\":" << m.max_gap_ns;
            o << ",\"gaps_gt_100ms\":" << m.gaps_gt_100ms << ",\"gaps_gt_1s\":" << m.gaps_gt_1s << ",\"non_monotonic_ts\":" << m.non_monotonic_ts;
            o << ",\"repeated_ts\":" << m.repeated_ts_count;
        } else o << ",\"ts_present\":false";

        if (m.has_bid_px) {
            o << ",\"bid_px_min\":" << m.bid_px_min << ",\"bid_px_max\":" << m.bid_px_max << ",\"bid_px_avg\":" << fixed << setprecision(6) << (double)m.bid_px_avg << ",\"bid_px_zero\":" << m.bid_px_zero << ",\"bid_px_count\":" << m.bid_px_count;
        } else o << ",\"bid_px_present\":false";
        if (m.has_ask_px) {
            o << ",\"ask_px_min\":" << m.ask_px_min << ",\"ask_px_max\":" << m.ask_px_max << ",\"ask_px_avg\":" << fixed << setprecision(6) << (double)m.ask_px_avg << ",\"ask_px_zero\":" << m.ask_px_zero << ",\"ask_px_count\":" << m.ask_px_count;
        } else o << ",\"ask_px_present\":false";

        if (m.has_bid_qty) {
            o << ",\"bid_qty_min\":" << m.bid_qty_min << ",\"bid_qty_max\":" << m.bid_qty_max << ",\"bid_qty_avg\":" << fixed << setprecision(6) << (double)m.bid_qty_avg << ",\"bid_qty_zero\":" << m.bid_qty_zero << ",\"bid_qty_count\":" << m.bid_qty_count;
        } else o << ",\"bid_qty_present\":false";
        if (m.has_ask_qty) {
            o << ",\"ask_qty_min\":" << m.ask_qty_min << ",\"ask_qty_max\":" << m.ask_qty_max << ",\"ask_qty_avg\":" << fixed << setprecision(6) << (double)m.ask_qty_avg << ",\"ask_qty_zero\":" << m.ask_qty_zero << ",\"ask_qty_count\":" << m.ask_qty_count;
        } else o << ",\"ask_qty_present\":false";

        o << ",\"duplicate_snapshot_count\":" << m.duplicate_snapshot_count;
        o << ",\"cross_book_count\":" << m.cross_book_count;

        o << ",\"null_counts\":{";
        bool first_nc = true;
        if (m.has_ts) { o << "\"ts\":" << m.null_ts; first_nc=false; }
        if (m.has_bid_px) { if (!first_nc) o << ","; o << "\"bid_px\":" << m.null_bid_px; first_nc=false; }
        if (m.has_bid_qty) { if (!first_nc) o << ","; o << "\"bid_qty\":" << m.null_bid_qty; first_nc=false; }
        if (m.has_ask_px) { if (!first_nc) o << ","; o << "\"ask_px\":" << m.null_ask_px; first_nc=false; }
        if (m.has_ask_qty) { if (!first_nc) o << ","; o << "\"ask_qty\":" << m.null_ask_qty; first_nc=false; }
        if (m.has_valu) { if (!first_nc) o << ","; o << "\"valu\":" << m.null_valu; first_nc=false; }
        o << "}";

        o << ",\"anomalies\":[";
        for (size_t i=0;i<anomalies.size();++i) {
            if (i) o << ",";
            o << "\"" << esc(anomalies[i]) << "\"";
        }
        o << "]}";
        o << "\n";
        fout << o.str();
    }

    fout.close();
    cerr << "Scan complete. Results: " << out_path << (write_all ? " (all files)" : " (only anomalous files)") << "\n";
    return 0;
}
