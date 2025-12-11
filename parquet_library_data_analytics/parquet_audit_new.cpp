// parquet_audit.cpp
// Lightweight auditor for parquet top/trade/depth files.
// Produces a single text report listing detected issues per-file.
//
// Build:
//   g++ -std=gnu++23 -O3 parquet_audit.cpp -lparquet -larrow -lzstd -o parquet_audit

#include <parquet/api/reader.h>

#include <cerrno>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <map>
#include <optional>
#include <regex>
#include <set>
#include <sstream>
#include <string>
#include <vector>

using namespace std;
namespace fs = std::filesystem;

// ---------------- small helpers ----------------

static string basename_of(const string& p)
{
    return fs::path(p).filename().string();
}

static string to_lower(string s)
{
    for (char &c : s) c = static_cast<char>(tolower((unsigned char)c));
    return s;
}

static bool file_exists(const string& p)
{
    return fs::exists(p) && fs::is_regular_file(p);
}

// read int64 column into vector<int64_t>
static void read_i64_column(parquet::RowGroupReader& rg, int col_idx, vector<int64_t>& out)
{
    int64_t rows = rg.metadata()->num_rows();
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
    if (done != rows) {
        // short read: resize to actual read size
        out.resize(done);
    }
}

// read int32 column into vector<int32_t>
static void read_i32_column(parquet::RowGroupReader& rg, int col_idx, vector<int32_t>& out)
{
    int64_t rows = rg.metadata()->num_rows();
    out.clear();
    out.resize(rows);
    auto col = rg.Column(col_idx);
    auto* r = static_cast<parquet::Int32Reader*>(col.get());
    int64_t done = 0;
    while (done < rows) {
        int64_t values_read = 0;
        int64_t levels = r->ReadBatch(rows - done, nullptr, nullptr, out.data() + done, &values_read);
        if (levels == 0 && values_read == 0) break;
        done += values_read;
    }
    if (done != rows) {
        out.resize(done);
    }
}

// find column index by exact dot-path name
static int find_col_idx(const parquet::SchemaDescriptor* schema, const string& name)
{
    for (int i = 0; i < schema->num_columns(); ++i) {
        if (schema->Column(i)->path()->ToDotString() == name) return i;
    }
    return -1;
}

// candidate offset names
static vector<string> offset_name_variants(const string& base)
{
    return {
        base + "_off",
        base + "_offs",
        base + "_offset",
        base + "_offsets",
        base + "s_off",
        base + "_off_idx"
    };
}

// ---------------- auditing structure ----------------

struct FileReport
{
    string file_path;
    string type; // top|trade|depth + maybe _spot/_fut
    uint64_t rows_scanned = 0;
    uint64_t non_monotonic_ts = 0;
    uint64_t lastid_lt_firstid = 0;
    uint64_t id_overlap_count = 0;
    uint64_t id_gap_count = 0;
    uint64_t bid_px_count = 0;
    uint64_t ask_px_count = 0;
    uint64_t bid_qty_count = 0;
    uint64_t ask_qty_count = 0;
    uint64_t has_bid_px_but_zero_count = 0;
    uint64_t has_ask_px_but_zero_count = 0;
    uint64_t bid_qty_zero = 0;
    uint64_t ask_qty_zero = 0;
    uint64_t bid_px_zero = 0;
    uint64_t ask_px_zero = 0;
    uint64_t crossed_book_count = 0; // best_bid >= best_ask
    uint64_t price_change_10x_count = 0;
    uint64_t qty_extreme_deviation_count = 0;
    uint64_t price_not_div1000_count = 0;
    uint64_t qty_not_div1e8_count = 0;

    bool flattened_without_offsets = false;
    bool per_row_offsets_mismatch = false;

    // additional counters
    uint64_t total_price_samples = 0;
    long double sum_qty = 0.0L;
    uint64_t qty_samples = 0;
};

// Helper deciding whether report should be considered "problematic".
// - By default informational flags (flattened_without_offsets, per_row_offsets_mismatch) are ignored.
// - If include_info==true, treat informational flags as problems too.
static bool is_problematic(const FileReport& r, bool include_info)
{
    if (r.non_monotonic_ts > 0) return true;
    if (r.lastid_lt_firstid > 0) return true;
    if (r.id_overlap_count > 0) return true;
    if (r.id_gap_count > 0) return true;
    if (r.has_bid_px_but_zero_count > 0) return true;
    if (r.has_ask_px_but_zero_count > 0) return true;
    if (r.bid_qty_zero > 0) return true;
    if (r.ask_qty_zero > 0) return true;
    if (r.bid_px_zero > 0) return true;
    if (r.ask_px_zero > 0) return true;
    if (r.crossed_book_count > 0) return true;
    if (r.price_change_10x_count > 0) return true;
    if (r.price_not_div1000_count > 0) return true;
    if (r.qty_not_div1e8_count > 0) return true;
    if (include_info) {
        if (r.flattened_without_offsets) return true;
        if (r.per_row_offsets_mismatch) return true;
    }
    return false;
}

// ---------------- scan single parquet file ----------------

static FileReport audit_parquet_file(const string& path)
{
    FileReport rep;
    rep.file_path = path;

    // determine type by filename
    string fn = basename_of(path);
    string lfn = to_lower(fn);
    if (lfn.find("top") != string::npos) rep.type = "top";
    else if (lfn.find("trade") != string::npos) rep.type = "trade";
    else if (lfn.find("depth") != string::npos) rep.type = "depth";
    else rep.type = "unknown";

    unique_ptr<parquet::ParquetFileReader> reader;
    try {
        reader = parquet::ParquetFileReader::OpenFile(path, /*memory_map=*/true);
    } catch (const exception& e) {
        cerr << "ERROR: cannot open " << path << " : " << e.what() << "\n";
        return rep;
    }

    auto md = reader->metadata();
    auto schema = md->schema();

    // find canonical columns if present
    int ts_i = find_col_idx(schema, "ts");
    int firstid_i = find_col_idx(schema, "firstId");
    if (firstid_i < 0) firstid_i = find_col_idx(schema, "firstid");
    int lastid_i = find_col_idx(schema, "lastId");
    if (lastid_i < 0) lastid_i = find_col_idx(schema, "lastid");

    int ask_px_i = find_col_idx(schema, "ask_px");
    if (ask_px_i < 0) ask_px_i = find_col_idx(schema, "askpx");
    int ask_qty_i = find_col_idx(schema, "ask_qty");
    if (ask_qty_i < 0) ask_qty_i = find_col_idx(schema, "askqty");
    int bid_px_i = find_col_idx(schema, "bid_px");
    if (bid_px_i < 0) bid_px_i = find_col_idx(schema, "bidpx");
    int bid_qty_i = find_col_idx(schema, "bid_qty");
    if (bid_qty_i < 0) bid_qty_i = find_col_idx(schema, "bidqty");

    // try offsets
    int ask_off_i = -1;
    int bid_off_i = -1;
    for (auto &cand : offset_name_variants("ask")) {
        int idx = find_col_idx(schema, cand);
        if (idx >= 0) { ask_off_i = idx; break; }
    }
    for (auto &cand : offset_name_variants("bid")) {
        int idx = find_col_idx(schema, cand);
        if (idx >= 0) { bid_off_i = idx; break; }
    }

    // If element arrays exist but no offsets -> informational flag
    bool ask_elem_exists = (ask_px_i >= 0 || ask_qty_i >= 0);
    bool bid_elem_exists = (bid_px_i >= 0 || bid_qty_i >= 0);
    if ((ask_elem_exists && ask_off_i < 0) || (bid_elem_exists && bid_off_i < 0)) {
        rep.flattened_without_offsets = true;
    }

    // We'll scan row-group by row-group and maintain previous state for id/ts
    int64_t prev_ts = numeric_limits<int64_t>::min();
    optional<int64_t> prev_lastid = nullopt;
    optional<int64_t> prev_price_sample = nullopt; // for 10x change detection (take ask_px if present, else bid_px, else px)
    vector<int64_t> v_ts, v_firstid, v_lastid;
    vector<int64_t> v_ask_px, v_ask_qty, v_bid_px, v_bid_qty;
    vector<int32_t> v_ask_off, v_bid_off;

    uint64_t global_rows = 0;

    for (int rg = 0; rg < md->num_row_groups(); ++rg) {
        auto rg_reader = reader->RowGroup(rg);
        int64_t rows_in_rg = rg_reader->metadata()->num_rows();

        // read scalar per-row columns if exist
        v_ts.clear(); v_firstid.clear(); v_lastid.clear();
        if (ts_i >= 0) read_i64_column(*rg_reader, ts_i, v_ts);
        if (firstid_i >= 0) read_i64_column(*rg_reader, firstid_i, v_firstid);
        if (lastid_i >= 0) read_i64_column(*rg_reader, lastid_i, v_lastid);

        // read element arrays (flattened) if present
        v_ask_px.clear(); v_ask_qty.clear(); v_bid_px.clear(); v_bid_qty.clear();
        if (ask_px_i >= 0) read_i64_column(*rg_reader, ask_px_i, v_ask_px);
        if (ask_qty_i >= 0) read_i64_column(*rg_reader, ask_qty_i, v_ask_qty);
        if (bid_px_i >= 0) read_i64_column(*rg_reader, bid_px_i, v_bid_px);
        if (bid_qty_i >= 0) read_i64_column(*rg_reader, bid_qty_i, v_bid_qty);

        // read offsets if present (per-row offsets, int32)
        v_ask_off.clear(); v_bid_off.clear();
        if (ask_off_i >= 0) read_i32_column(*rg_reader, ask_off_i, v_ask_off);
        if (bid_off_i >= 0) read_i32_column(*rg_reader, bid_off_i, v_bid_off);

        // --- basic counts for arrays ---
        rep.ask_px_count += v_ask_px.size();
        rep.ask_qty_count += v_ask_qty.size();
        rep.bid_px_count += v_bid_px.size();
        rep.bid_qty_count += v_bid_qty.size();

        // If offsets exist, verify offsets array length equals rows+1 for this RG (common pattern)
        if (!v_ask_off.empty()) {
            if (static_cast<int64_t>(v_ask_off.size()) < rows_in_rg + 1) {
                rep.per_row_offsets_mismatch = true;
            }
        }
        if (!v_bid_off.empty()) {
            if (static_cast<int64_t>(v_bid_off.size()) < rows_in_rg + 1) {
                rep.per_row_offsets_mismatch = true;
            }
        }

        // iterate per-row to do per-row checks (use minimal data from flattened arrays via offsets when possible)
        for (int64_t i = 0; i < rows_in_rg; ++i) {
            ++global_rows;
            rep.rows_scanned = global_rows;

            int64_t ts = (ts_i >= 0 && static_cast<size_t>(i) < v_ts.size()) ? v_ts[i] : 0;

            // non-monotonic ts
            if (prev_ts != numeric_limits<int64_t>::min() && ts < prev_ts) {
                ++rep.non_monotonic_ts;
            }
            prev_ts = ts;

            // ids
            bool have_first = (firstid_i >= 0 && static_cast<size_t>(i) < v_firstid.size());
            bool have_last = (lastid_i >= 0 && static_cast<size_t>(i) < v_lastid.size());
            int64_t firstId = have_first ? v_firstid[i] : 0;
            int64_t lastId = have_last ? v_lastid[i] : 0;

            if (have_first && have_last) {
                if (lastId < firstId) ++rep.lastid_lt_firstid;
                if (prev_lastid.has_value()) {
                    if (firstId <= *prev_lastid) ++rep.id_overlap_count;
                    else if (firstId > (*prev_lastid + 1)) ++rep.id_gap_count;
                }
                prev_lastid = lastId;
            }

            // book-level checks: best ask/bid (first elements) using offsets if present
            optional<int64_t> first_ask_px = nullopt;
            optional<int64_t> first_bid_px = nullopt;
            optional<int64_t> first_ask_qty = nullopt;
            optional<int64_t> first_bid_qty = nullopt;

            if (!v_ask_off.empty() && (ask_px_i >= 0 || ask_qty_i >= 0)) {
                if (static_cast<size_t>(i) + 1 < v_ask_off.size()) {
                    int start = v_ask_off[i];
                    int end   = v_ask_off[i + 1];
                    if (start < end) {
                        if (ask_px_i >= 0 && static_cast<size_t>(start) < v_ask_px.size())
                            first_ask_px = v_ask_px[start];
                        if (ask_qty_i >= 0 && static_cast<size_t>(start) < v_ask_qty.size())
                            first_ask_qty = v_ask_qty[start];
                    } else {
                        if (ask_px_i >= 0) ++rep.has_ask_px_but_zero_count;
                        if (ask_qty_i >= 0) ++rep.has_ask_px_but_zero_count;
                    }
                }
            }

            if (!v_bid_off.empty() && (bid_px_i >= 0 || bid_qty_i >= 0)) {
                if (static_cast<size_t>(i) + 1 < v_bid_off.size()) {
                    int start = v_bid_off[i];
                    int end   = v_bid_off[i + 1];
                    if (start < end) {
                        if (bid_px_i >= 0 && static_cast<size_t>(start) < v_bid_px.size())
                            first_bid_px = v_bid_px[start];
                        if (bid_qty_i >= 0 && static_cast<size_t>(start) < v_bid_qty.size())
                            first_bid_qty = v_bid_qty[start];
                    } else {
                        if (bid_px_i >= 0) ++rep.has_bid_px_but_zero_count;
                        if (bid_qty_i >= 0) ++rep.has_bid_px_but_zero_count;
                    }
                }
            }

            // crossed book
            if (first_bid_px.has_value() && first_ask_px.has_value()) {
                if (*first_bid_px >= *first_ask_px) ++rep.crossed_book_count;
            }

            // rep price sample
            optional<int64_t> rep_px = first_ask_px.has_value() ? first_ask_px : first_bid_px;
            if (!rep_px.has_value()) {
                if (ask_px_i >= 0 && static_cast<size_t>(i) < v_ask_px.size()) rep_px = v_ask_px[i];
                else if (bid_px_i >= 0 && static_cast<size_t>(i) < v_bid_px.size()) rep_px = v_bid_px[i];
            }
            if (rep_px.has_value()) {
                ++rep.total_price_samples;
                if (rep_px.value() == 0) ++rep.bid_px_zero; // conservative
                if (prev_price_sample.has_value()) {
                    long double prevv = static_cast<long double>(*prev_price_sample);
                    long double curv = static_cast<long double>(*rep_px);
                    if (prevv > 0.0L && curv > 0.0L) {
                        long double ratio = (curv > prevv) ? (curv / prevv) : (prevv / curv);
                        if (ratio > 10.0L) ++rep.price_change_10x_count;
                    }
                }
                prev_price_sample = rep_px;
            }

            // qty stats
            if (first_bid_qty.has_value()) {
                rep.sum_qty += static_cast<long double>(*first_bid_qty);
                ++rep.qty_samples;
            } else if (first_ask_qty.has_value()) {
                rep.sum_qty += static_cast<long double>(*first_ask_qty);
                ++rep.qty_samples;
            }

            // divisibility checks
            if (first_bid_px.has_value()) {
                if ((*first_bid_px % 1000) != 0) ++rep.price_not_div1000_count;
            }
            if (first_ask_px.has_value()) {
                if ((*first_ask_px % 1000) != 0) ++rep.price_not_div1000_count;
            }
            if (first_bid_qty.has_value()) {
                if ((*first_bid_qty % 100000000LL) != 0) ++rep.qty_not_div1e8_count;
            }
            if (first_ask_qty.has_value()) {
                if ((*first_ask_qty % 100000000LL) != 0) ++rep.qty_not_div1e8_count;
            }
        } // per-row loop

        // element-level zero counts
        for (int64_t v : v_bid_qty) if (v == 0) ++rep.bid_qty_zero;
        for (int64_t v : v_ask_qty) if (v == 0) ++rep.ask_qty_zero;
        for (int64_t v : v_bid_px) if (v == 0) ++rep.bid_px_zero;
        for (int64_t v : v_ask_px) if (v == 0) ++rep.ask_px_zero;
    } // row-groups

    // if file declares bid/ask arrays but element counts are zero -> suspicious
    if ((bid_px_i >= 0 || bid_qty_i >= 0) && rep.bid_px_count == 0) rep.has_bid_px_but_zero_count = rep.rows_scanned;
    if ((ask_px_i >= 0 || ask_qty_i >= 0) && rep.ask_px_count == 0) rep.has_ask_px_but_zero_count = rep.rows_scanned;

    return rep;
}

// ---------------- write filtered report ----------------

static void write_report_filtered(const string& outpath, const vector<FileReport>& reports, bool include_info)
{
    ofstream f(outpath, ios::trunc);
    if (!f) {
        cerr << "ERROR: cannot open report file for write: " << outpath << "\n";
        return;
    }

    f << "Parquet audit report\n";
    f << "====================\n\n";

    size_t problems = 0;
    for (const auto& r : reports) {
        if (!is_problematic(r, include_info)) continue;
        ++problems;
        f << "File: " << r.file_path << "\n";
        f << "Type: " << r.type << "\n";
        f << "Rows scanned: " << r.rows_scanned << "\n";
        f << "\nWarnings / Errors summary:\n";
        f << "  non_monotonic_ts: " << r.non_monotonic_ts << "\n";
        f << "  lastId < firstId: " << r.lastid_lt_firstid << "\n";
        f << "  id_overlap_count: " << r.id_overlap_count << "\n";
        f << "  id_gap_count: " << r.id_gap_count << "\n";
        f << "  ask_px_count (elements): " << r.ask_px_count << "\n";
        f << "  bid_px_count (elements): " << r.bid_px_count << "\n";
        f << "  ask_qty_count (elements): " << r.ask_qty_count << "\n";
        f << "  bid_qty_count (elements): " << r.bid_qty_count << "\n";
        f << "  has_bid_px_but_zero_count: " << r.has_bid_px_but_zero_count << "\n";
        f << "  has_ask_px_but_zero_count: " << r.has_ask_px_but_zero_count << "\n";
        f << "  bid_qty_zero (elements): " << r.bid_qty_zero << "\n";
        f << "  ask_qty_zero (elements): " << r.ask_qty_zero << "\n";
        f << "  bid_px_zero (elements): " << r.bid_px_zero << "\n";
        f << "  ask_px_zero (elements): " << r.ask_px_zero << "\n";
        f << "  crossed_book_count (per-row best_bid>=best_ask): " << r.crossed_book_count << "\n";
        f << "  price_change_10x_count: " << r.price_change_10x_count << "\n";
        f << "  price_not_div1000_count: " << r.price_not_div1000_count << "\n";
        f << "  qty_not_div1e8_count: " << r.qty_not_div1e8_count << "\n";
        f << "  flattened_arrays_without_offsets (informational): " << (r.flattened_without_offsets ? "yes" : "no") << "\n";
        f << "  per_row_offsets_mismatch (informational): " << (r.per_row_offsets_mismatch ? "yes" : "no") << "\n";

        f << "\nDetailed notes:\n";
        if (r.non_monotonic_ts > 0) f << "  -> Non-monotonic timestamps found: timestamps decreased.\n";
        if (r.lastid_lt_firstid > 0) f << "  -> lastId < firstId inside some rows.\n";
        if (r.id_overlap_count > 0) f << "  -> ID range overlaps detected (firstId <= prev.lastId).\n";
        if (r.id_gap_count > 0) f << "  -> ID gaps detected (missing delta segments between rows).\n";
        if (r.bid_px_zero > 0 || r.ask_px_zero > 0) f << "  -> Some price elements are zero (likely invalid).\n";
        if (r.bid_qty_zero > 0 || r.ask_qty_zero > 0) f << "  -> Some quantity elements are zero (suspicious).\n";
        if (r.crossed_book_count > 0) f << "  -> Crossed book rows (best_bid >= best_ask).\n";
        if (r.price_change_10x_count > 0) f << "  -> Price changed more than 10x between adjacent samples.\n";
        if (r.flattened_without_offsets) f << "  -> Arrays present but no offsets found: cannot map elements to rows precisely.\n";
        if (r.per_row_offsets_mismatch) f << "  -> Offsets array length does not match rows+1 (per-RG) in at least one RG.\n";

        f << "\n----\n\n";
    }

    if (problems == 0) {
        f << "No problematic files found.\n";
        cout << "No problematic files found (report written to " << outpath << ").\n";
    } else {
        cout << "Wrote audit report to: " << outpath << " (problematic files: " << problems << ")\n";
    }
    f << "\nEnd of report\n";
    f.close();
}

// ---------------- main ----------------

int main(int argc, char** argv)
{
    if (argc < 2) {
        cerr << "Usage: " << argv[0] << " <parquet-file-1> [<parquet-file-2> ...] [--out=report.txt] [--include-info]\n";
        cerr << "If a directory is passed, use shell expansion: e.g. /path/to/dir/*.parquet or find ... | xargs\n";
        return 1;
    }

    vector<string> files;
    string outpath = "parquet_audit_report.txt";
    bool include_info = false;

    for (int i = 1; i < argc; ++i) {
        string a = argv[i];
        if (a.rfind("--out=", 0) == 0) {
            outpath = a.substr(6);
        } else if (a == "--include-info") {
            include_info = true;
        } else {
            files.push_back(a);
        }
    }

    if (files.empty()) {
        cerr << "No parquet input files provided\n";
        return 1;
    }

    vector<FileReport> reports;
    for (const string& f : files) {
        // If argument is a directory, skip (user should expand), but allow explicit files only
        if (!file_exists(f)) {
            cerr << "Skipping missing file: " << f << "\n";
            continue;
}
        cerr << "Auditing: " << f << "\n";
        try {
            FileReport r = audit_parquet_file(f);
            reports.push_back(std::move(r));
        } catch (const exception& e) {
            cerr << "ERROR auditing " << f << " : " << e.what() << "\n";
        }
    }

    write_report_filtered(outpath, reports, include_info);
    return 0;
}
