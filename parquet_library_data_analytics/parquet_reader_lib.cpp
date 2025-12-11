// parquet_reader_lib.cpp
// Implementation of the reader library (private Parquet/Arrow deps here)

#include "parquet_reader_lib.h"

#include <parquet/api/reader.h>
#include <parquet/schema.h>

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <ctime>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <memory>
#include <optional>
#include <stdexcept>
#include <sstream>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#if defined(__linux__)
  #include <fcntl.h>
  #include <sys/stat.h>
  #include <unistd.h>
#endif

using namespace std;
namespace fs = std::filesystem;

// ======== Global debug & prefetch toggles ========
static bool g_debug = false;
void ShardedDB::set_debug(bool enabled) { g_debug = enabled; }

static bool g_prefetch = false;
void ShardedDB::set_prefetch(bool enabled) { g_prefetch = enabled; }

// Small Linux prefetch helper (no-op elsewhere)
static inline void prefetch_path(const std::string& path) {
#if defined(__linux__)
  if (!g_prefetch) return;
  int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
  if (fd < 0) return;
  struct stat st{};
  if (::fstat(fd, &st) == 0 && st.st_size > 0) {
    posix_fadvise(fd, 0, st.st_size, POSIX_FADV_SEQUENTIAL);
    posix_fadvise(fd, 0, st.st_size, POSIX_FADV_WILLNEED);
    readahead(fd, 0, static_cast<size_t>(st.st_size));
  }
  ::close(fd);
#else
  (void)path;
#endif
}

// ======== Internal low-level batched cursor (used by nested depth path) ========

struct Entry
{
  int16_t def = 0;
  int16_t rep = 0;
  bool has_value = false;
  int64_t value = 0;
  bool valid = false;
};

struct Int64Cursor
{
  shared_ptr<parquet::ColumnReader> holder;
  parquet::Int64Reader* r = nullptr;
  int16_t max_def = 0;
  int16_t max_rep = 0;
  bool eof = false;

  bool has_pending = false;
  Entry pending;

  static constexpr int64_t BATCH = 65536;
  vector<int16_t> defbuf;
  vector<int16_t> repbuf;
  vector<int64_t> valbuf;
  int64_t levels_in_buf = 0;
  int64_t level_idx = 0;
  int64_t values_in_buf = 0;
  int64_t value_idx = 0;

  Int64Cursor() = default;

  Int64Cursor(shared_ptr<parquet::ColumnReader> col, const parquet::ColumnDescriptor* descr)
  :
    holder(move(col))
  {
    r = dynamic_cast<parquet::Int64Reader*>(holder.get());
    if (!r)
    {
      throw runtime_error("Column is not INT64");
    }

    max_def = descr->max_definition_level();
    max_rep = descr->max_repetition_level();

    if (max_def) defbuf.resize(BATCH);
    if (max_rep) repbuf.resize(BATCH);
    valbuf.resize(BATCH);
  }

  bool refill()
  {
    if (eof) return false;

    int64_t values_read = 0;
    levels_in_buf = r->ReadBatch(
        BATCH,
        max_def ? defbuf.data() : nullptr,
        max_rep ? repbuf.data() : nullptr,
        valbuf.data(),
        &values_read);

    if (levels_in_buf == 0)
    {
      eof = true;
      return false;
    }

    level_idx = 0;
    value_idx = 0;
    values_in_buf = values_read;
    return true;
  }

  bool ensure_pending()
  {
    if (eof && !has_pending) return false;
    if (has_pending) return true;

    if (level_idx >= levels_in_buf)
    {
      if (!refill()) return false;
    }

    Entry e{};
    e.valid = true;

    e.def = max_def ? defbuf[level_idx] : 0;
    e.rep = max_rep ? repbuf[level_idx] : 0;

    if (e.def == max_def)
    {
      if (value_idx >= values_in_buf) throw runtime_error("value_idx overflow");
      e.has_value = true;
      e.value = valbuf[value_idx++];
    }
    else
    {
      e.has_value = false;
      e.value = 0;
    }

    ++level_idx;

    pending = e;
    has_pending = true;
    return true;
  }

  Entry take()
  {
    if (!ensure_pending()) return Entry{};
    has_pending = false;
    return pending;
  }

  const Entry* peek()
  {
    if (!ensure_pending()) return nullptr;
    return &pending;
  }
};

static int find_col_idx(const parquet::SchemaDescriptor* schema, const string& name)
{
  for (int i = 0; i < schema->num_columns(); ++i)
  {
    if (schema->Column(i)->path()->ToDotString() == name)
    {
      return i;
    }
  }
  return -1;
}

// Read LIST from a single leaf. Returns count appended.
static uint32_t append_list_from_leaf_for_row(
    Int64Cursor& leaf,
    vector<int64_t>* out_vals)
{
  const Entry* p = leaf.peek();
  if (!p) return 0;

  bool empty_start = (!p->has_value && p->rep == 0);
  if (empty_start)
  {
    (void)leaf.take();
    return 0;
  }

  uint32_t count = 0;
  while (true)
  {
    Entry e = leaf.take();
    if (out_vals) out_vals->push_back(e.value);
    ++count;

    const Entry* n = leaf.peek();
    if (!n || n->rep == 0) break;
  }
  return count;
}

// Read LIST pairs (px, qty) per row; returns number of pairs appended.
static uint32_t append_list_pairs_for_row_typed(
    Int64Cursor& px, Int64Cursor& qty,
    vector<int64_t>* out_px,
    vector<int64_t>* out_qty)
{
  const Entry* p_peek = px.peek();
  const Entry* q_peek = qty.peek();

  if (!p_peek || !q_peek) return 0;

  bool empty_start = (!p_peek->has_value && p_peek->rep == 0) &&
                     (!q_peek->has_value && q_peek->rep == 0);

  if (empty_start)
  {
    (void)px.take();
    (void)qty.take();
    return 0;
  }

  uint32_t count = 0;

  while (true)
  {
    Entry epx = px.take();
    Entry eqy = qty.take();

    if (out_px)  out_px->push_back(epx.value);
    if (out_qty) out_qty->push_back(eqy.value);
    ++count;

    const Entry* npx = px.peek();
    const Entry* nqy = qty.peek();

    if (!npx || !nqy) break;
    if (npx->rep == 0) break;
    if (npx->rep != nqy->rep) break;
  }

  return count;
}

// ======== Date helpers & file mapping (chronological order) ========

struct YMD { int year; int month; int day; };

static YMD ymd_utc_from_ns(int64_t ns)
{
  time_t s = static_cast<time_t>(ns / 1'000'000'000LL);
  tm tm{};
  gmtime_r(&s, &tm);
  return {tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday};
}

static int64_t floor_day_ns(int64_t ns)
{
  int64_t sec = ns / 1'000'000'000LL;
  int64_t day_sec = sec - (sec % 86400LL);
  return day_sec * 1'000'000'000LL;
}

static int64_t ymd_utc_start_ns(int Y, int M, int D)
{
  tm tm{};
  tm.tm_year = Y - 1900;
  tm.tm_mon  = M - 1;
  tm.tm_mday = D;
  tm.tm_hour = 0; tm.tm_min = 0; tm.tm_sec = 0;
  time_t s = timegm(&tm); // GNU extension: UTC mktime
  if (s == (time_t)-1) return 0;
  return static_cast<int64_t>(s) * 1'000'000'000LL;
}

static string iso_from_ns(int64_t ns)
{
  time_t s = static_cast<time_t>(ns / 1'000'000'000LL);
  tm tm{}; gmtime_r(&s, &tm);
  ostringstream o;
  o << setfill('0')
    << setw(4) << (tm.tm_year + 1900) << '-'
    << setw(2) << (tm.tm_mon + 1) << '-'
    << setw(2) << tm.tm_mday << 'T'
    << setw(2) << tm.tm_hour << ':'
    << setw(2) << tm.tm_min  << ':'
    << setw(2) << tm.tm_sec  << 'Z';
  return o.str();
}

struct Candidate
{
  string  path;
  int64_t file_start_ns = 0;
  int64_t file_end_ns   = 0;
};

static string to_lower(string s) { for (auto& c:s) c=(char)tolower((unsigned char)c); return s; }

// Normalize/accept market input
static optional<string> norm_market(optional<string> m)
{
  if (!m) return nullopt;
  string s = to_lower(*m);
  if (s=="futures") s="fut";
  if (s=="future")  s="fut";
  if (s=="spot" || s=="fut") return s;
  return nullopt;
}

static void debug_try_path(const string& p, int64_t s_ns, int64_t e_ns)
{
  if (!g_debug) return;
  bool ex = fs::exists(p);
  cerr << "[debug] try: " << p
       << " [" << iso_from_ns(s_ns) << " .. " << iso_from_ns(e_ns)
       << "] " << (ex ? "EXISTS" : "missing") << "\n";
}

// STRICT layout only:
//   <root>/<kind>_<market>/<SYMB>/<Y>/<M>/bn_<kind>_<market>_<SYMB>_<Y>_<M>_<D>.parquet
// Non-padded month/day (e.g., 2025/9/3)
// kind: "top" | "trade" | "depth"
static vector<Candidate> candidate_files_strict(const string& root,
                                                const string& symb,
                                                const string& base_type,
                                                optional<string> market,
                                                int64_t start_ns,
                                                int64_t end_ns,
                                                const optional<string>& sampling)
{
  vector<Candidate> out;
  if (start_ns >= end_ns) return out;

  // sampling not used in strict layout; warn in debug to avoid confusion
  if (g_debug && sampling.has_value()) {
    cerr << "[debug] note: --sampling is ignored for strict layout discovery\n";
  }

  vector<string> markets;
  if (market) {
    auto nm = norm_market(market);
    if (!nm) throw runtime_error("market must be 'fut' or 'spot'");
    markets.push_back(*nm);
  } else {
    markets = {"fut","spot"}; // backward-compatible overloads search both
  }

  const int64_t day_ns = 86'400'000'000'000LL;
  int64_t cur = floor_day_ns(start_ns);
  const int64_t end_floor = floor_day_ns(end_ns - 1);

  for (const string& mkt : markets)
  {
    while (cur <= end_floor)
    {
      auto ymd = ymd_utc_from_ns(cur);
      const int64_t file_start = ymd_utc_start_ns(ymd.year, ymd.month, ymd.day);
      const int64_t file_end   = file_start + day_ns;

      // <root>/<kind>_<market>/<SYMB>/<Y>/<M>/bn_<kind>_<market>_<SYMB>_<Y>_<M>_<D>.parquet
      ostringstream dir;
      dir << root << '/'
          << base_type << '_' << mkt << '/'
          << symb << '/'
          << ymd.year << '/'
          << ymd.month << '/'; // non-padded month

      ostringstream file;
      file << "bn_" << base_type << '_' << mkt << '_' << symb << '_'
           << ymd.year << '_' << ymd.month << '_' << ymd.day << ".parquet";

      const string path = dir.str() + file.str();

      debug_try_path(path, file_start, file_end);
      if (fs::exists(path)) {
        out.push_back(Candidate{path, file_start, file_end});
      }

      cur += day_ns;
    }

    // reset day cursor for next market
    cur = floor_day_ns(start_ns);
  }

  if (g_debug) {
    cerr << "[debug] candidates(" << base_type << "): " << out.size() << "\n";
    for (const auto& c : out) {
      cerr << "  - " << c.path << " [" << iso_from_ns(c.file_start_ns)
           << " .. " << iso_from_ns(c.file_end_ns) << ")\n";
    }
  }

  return out;
}

// ======== RowGroup -> column vectors (decode once per RG) ========

struct FileStreamerTopCols
{
  unique_ptr<parquet::ParquetFileReader> reader;
  shared_ptr<parquet::FileMetaData> md;
  const parquet::SchemaDescriptor* schema = nullptr;
  int rg_idx = 0;

  explicit FileStreamerTopCols(string path)
  {
    //cerr << path << endl; // print file when processing
    reader = parquet::ParquetFileReader::OpenFile(path, /*memory_map=*/false);
    md     = reader->metadata();
    schema = md->schema();
  }

  static void read_required_i64_column(
      parquet::RowGroupReader& rg, int col_idx, vector<int64_t>& out)
  {
    const int64_t rows = rg.metadata()->num_rows();
    out.resize(rows);

    shared_ptr<parquet::ColumnReader> col = rg.Column(col_idx);
    auto* r = static_cast<parquet::Int64Reader*>(col.get());

    int64_t done = 0;
    while (done < rows)
    {
      int64_t values_read = 0;
      int64_t levels = r->ReadBatch(rows - done, nullptr, nullptr, out.data() + done, &values_read);
      if (levels == 0 && values_read == 0) break;
      done += values_read;
    }

    if (done != rows) throw runtime_error("Short read in required column");
  }

  bool next_rg(
      int64_t start_ns, int64_t end_ns, const TopSelect& sel,
      vector<int64_t>& v_ts, vector<int64_t>& v_apx, vector<int64_t>& v_aq,
      vector<int64_t>& v_bpx, vector<int64_t>& v_bq, vector<int64_t>& v_val,
      // sampled extras
      vector<int64_t>& v_min_bpx, vector<int64_t>& v_max_bpx,
      vector<int64_t>& v_min_apx, vector<int64_t>& v_max_apx,
      vector<int64_t>& v_min_bts, vector<int64_t>& v_max_bts,
      vector<int64_t>& v_min_ats, vector<int64_t>& v_max_ats)
  {
    while (true)
    {
      if (rg_idx >= md->num_row_groups()) return false;

      shared_ptr<parquet::RowGroupReader> rg = reader->RowGroup(rg_idx++);

      const int ts_i = find_col_idx(schema, "ts");
      if (ts_i < 0) throw runtime_error("top: missing ts");

      vector<int64_t> ts_all;
      read_required_i64_column(*rg, ts_i, ts_all);

      size_t cnt = 0;
      for (int64_t t : ts_all) if (t >= start_ns && t < end_ns) ++cnt;
      if (cnt == 0) continue;

      v_ts.resize(cnt);
      if (sel.ask_px)     v_apx.resize(cnt);    else v_apx.clear();
      if (sel.ask_qty)    v_aq.resize(cnt);     else v_aq.clear();
      if (sel.bid_px)     v_bpx.resize(cnt);    else v_bpx.clear();
      if (sel.bid_qty)    v_bq.resize(cnt);     else v_bq.clear();
      if (sel.valu)       v_val.resize(cnt);    else v_val.clear();

      if (sel.min_bid_px) v_min_bpx.resize(cnt); else v_min_bpx.clear();
      if (sel.max_bid_px) v_max_bpx.resize(cnt); else v_max_bpx.clear();
      if (sel.min_ask_px) v_min_apx.resize(cnt); else v_min_apx.clear();
      if (sel.max_ask_px) v_max_apx.resize(cnt); else v_max_apx.clear();
      if (sel.min_bid_ts) v_min_bts.resize(cnt); else v_min_bts.clear();
      if (sel.max_bid_ts) v_max_bts.resize(cnt); else v_max_bts.clear();
      if (sel.min_ask_ts) v_min_ats.resize(cnt); else v_min_ats.clear();
      if (sel.max_ask_ts) v_max_ats.resize(cnt); else v_max_ats.clear();

      {
        size_t w = 0;
        for (size_t i = 0; i < ts_all.size(); ++i) {
          int64_t t = ts_all[i];
          if (t >= start_ns && t < end_ns) v_ts[w++] = t;
        }
      }

      auto read_and_scatter = [&](const char* name, vector<int64_t>& out_vec)
      {
        const int idx = find_col_idx(schema, name);
        if (idx < 0) throw runtime_error(string("top: missing ") + name);
        vector<int64_t> tmp;
        read_required_i64_column(*rg, idx, tmp);

        size_t w = 0;
        for (size_t i = 0; i < ts_all.size(); ++i) {
          int64_t t = ts_all[i];
          if (t >= start_ns && t < end_ns) out_vec[w++] = tmp[i];
        }
      };

      if (sel.ask_px)     read_and_scatter("ask_px", v_apx);
      if (sel.ask_qty)    read_and_scatter("ask_qty", v_aq);
      if (sel.bid_px)     read_and_scatter("bid_px", v_bpx);
      if (sel.bid_qty)    read_and_scatter("bid_qty", v_bq);
      if (sel.valu)       read_and_scatter("valu",   v_val);

      if (sel.min_bid_px) read_and_scatter("min_bid_px", v_min_bpx);
      if (sel.max_bid_px) read_and_scatter("max_bid_px", v_max_bpx);
      if (sel.min_ask_px) read_and_scatter("min_ask_px", v_min_apx);
      if (sel.max_ask_px) read_and_scatter("max_ask_px", v_max_apx);
      if (sel.min_bid_ts) read_and_scatter("min_bid_ts", v_min_bts);
      if (sel.max_bid_ts) read_and_scatter("max_bid_ts", v_max_bts);
      if (sel.min_ask_ts) read_and_scatter("min_ask_ts", v_min_ats);
      if (sel.max_ask_ts) read_and_scatter("max_ask_ts", v_max_ats);

      return true;
    }
  }
};

struct FileStreamerTradeCols
{
  unique_ptr<parquet::ParquetFileReader> reader;
  shared_ptr<parquet::FileMetaData> md;
  const parquet::SchemaDescriptor* schema = nullptr;
  int rg_idx = 0;

  explicit FileStreamerTradeCols(string path)
  {
    //cerr << path << endl;
    reader = parquet::ParquetFileReader::OpenFile(path, /*memory_map=*/false);
    md     = reader->metadata();
    schema = md->schema();
  }

  static void read_required_i64_column(
      parquet::RowGroupReader& rg, int col_idx, vector<int64_t>& out)
  {
    const int64_t rows = rg.metadata()->num_rows();
    out.resize(rows);

    shared_ptr<parquet::ColumnReader> col = rg.Column(col_idx);
    auto* r = static_cast<parquet::Int64Reader*>(col.get());

    int64_t done = 0;
    while (done < rows)
    {
      int64_t values_read = 0;
      int64_t levels = r->ReadBatch(rows - done, nullptr, nullptr, out.data() + done, &values_read);
      if (levels == 0 && values_read == 0) break;
      done += values_read;
    }

    if (done != rows) throw runtime_error("Short read in required column");
  }

  static void read_required_bool_column(
      parquet::RowGroupReader& rg, int col_idx, vector<uint8_t>& out)
  {
    static_assert(sizeof(bool) == 1, "bool must be 1 byte");
    const int64_t rows = rg.metadata()->num_rows();
    out.resize(rows);

    shared_ptr<parquet::ColumnReader> col = rg.Column(col_idx);
    auto* r = static_cast<parquet::BoolReader*>(col.get());

    int64_t done = 0;
    while (done < rows)
    {
      int64_t values_read = 0;
      int64_t levels = r->ReadBatch(rows - done, nullptr, nullptr,
                                    reinterpret_cast<bool*>(out.data()) + done,
                                    &values_read);
      if (levels == 0 && values_read == 0) break;
      done += values_read;
    }

    if (done != rows) throw runtime_error("Short read in required bool column");
  }

  bool next_rg(
      int64_t start_ns, int64_t end_ns, const TradeSelect& sel,
      vector<int64_t>& v_ts, vector<int64_t>& v_px, vector<int64_t>& v_qty,
      vector<int64_t>& v_tid, vector<int64_t>& v_boid, vector<int64_t>& v_soid,
      vector<int64_t>& v_ttime, vector<uint8_t>& v_isMkt, vector<int64_t>& v_evt)
  {
    while (true)
    {
      if (rg_idx >= md->num_row_groups()) return false;

      shared_ptr<parquet::RowGroupReader> rg = reader->RowGroup(rg_idx++);

      const int ts_i = find_col_idx(schema, "ts");
      if (ts_i < 0) throw runtime_error("trade: missing ts");

      vector<int64_t> ts_all;
      read_required_i64_column(*rg, ts_i, ts_all);

      size_t cnt = 0;
      for (int64_t t : ts_all) if (t >= start_ns && t < end_ns) ++cnt;
      if (cnt == 0) continue;

      v_ts.resize(cnt);
      if (sel.px)            v_px.resize(cnt);     else v_px.clear();
      if (sel.qty)           v_qty.resize(cnt);    else v_qty.clear();
      if (sel.tradeId)       v_tid.resize(cnt);    else v_tid.clear();
      if (sel.buyerOrderId)  v_boid.resize(cnt);   else v_boid.clear();
      if (sel.sellerOrderId) v_soid.resize(cnt);   else v_soid.clear();
      if (sel.tradeTime)     v_ttime.resize(cnt);  else v_ttime.clear();
      if (sel.isMarket)      v_isMkt.resize(cnt);  else v_isMkt.clear();
      if (sel.eventTime)     v_evt.resize(cnt);    else v_evt.clear();

      {
        size_t w = 0;
        for (size_t i = 0; i < ts_all.size(); ++i) {
          int64_t t = ts_all[i];
          if (t >= start_ns && t < end_ns) v_ts[w++] = t;
        }
      }

      auto read_and_scatter_i64 = [&](const char* name, vector<int64_t>& out_vec)
      {
        const int idx = find_col_idx(schema, name);
        if (idx < 0) throw runtime_error(string("trade: missing ") + name);
        vector<int64_t> tmp;
        read_required_i64_column(*rg, idx, tmp);

        size_t w = 0;
        for (size_t i = 0; i < ts_all.size(); ++i) {
          int64_t t = ts_all[i];
          if (t >= start_ns && t < end_ns) out_vec[w++] = tmp[i];
        }
      };

      auto read_and_scatter_bool = [&](const char* name, vector<uint8_t>& out_vec)
      {
        const int idx = find_col_idx(schema, name);
        if (idx < 0) throw runtime_error(string("trade: missing ") + name);
        vector<uint8_t> tmp;
        read_required_bool_column(*rg, idx, tmp);

        size_t w = 0;
        for (size_t i = 0; i < ts_all.size(); ++i) {
          int64_t t = ts_all[i];
          if (t >= start_ns && t < end_ns) out_vec[w++] = tmp[i];
        }
      };

      if (sel.px)            read_and_scatter_i64("px", v_px);
      if (sel.qty)           read_and_scatter_i64("qty", v_qty);
      if (sel.tradeId)       read_and_scatter_i64("tradeId", v_tid);
      if (sel.buyerOrderId)  read_and_scatter_i64("buyerOrderId", v_boid);
      if (sel.sellerOrderId) read_and_scatter_i64("sellerOrderId", v_soid);
      if (sel.tradeTime)     read_and_scatter_i64("tradeTime", v_ttime);
      if (sel.isMarket)      read_and_scatter_bool("isMarket", v_isMkt);
      if (sel.eventTime)     read_and_scatter_i64("eventTime", v_evt);

      return true;
    }
  }
};

struct FileStreamerDeltaCols
{
  unique_ptr<parquet::ParquetFileReader> reader;
  shared_ptr<parquet::FileMetaData> md;
  const parquet::SchemaDescriptor* schema = nullptr;
  int rg_idx = 0;

  explicit FileStreamerDeltaCols(string path)
  {
    //cerr << path << endl;
    reader = parquet::ParquetFileReader::OpenFile(path, /*memory_map=*/false);
    md     = reader->metadata();
    schema = md->schema();
  }

  bool next_rg(
      int64_t start_ns, int64_t end_ns, const DeltaSelect& sel,
      vector<int64_t>& v_ts, vector<int64_t>& v_fid, vector<int64_t>& v_lid, vector<int64_t>& v_evt,
      vector<uint32_t>& ask_off, vector<int64_t>& ask_px, vector<int64_t>& ask_qty,
      vector<uint32_t>& bid_off, vector<int64_t>& bid_px, vector<int64_t>& bid_qty)
  {
    while (true)
    {
      if (rg_idx >= md->num_row_groups()) return false;

      shared_ptr<parquet::RowGroupReader> rg = reader->RowGroup(rg_idx++);
      auto rmd = rg->metadata();
      int64_t rows = rmd->num_rows();

      int ts_i = find_col_idx(schema, "ts");
      if (ts_i < 0) throw runtime_error("depth: missing ts");
      Int64Cursor ts(rg->Column(ts_i), schema->Column(ts_i));

      optional<Int64Cursor> fid;
      optional<Int64Cursor> lid;
      optional<Int64Cursor> evt;

      if (sel.firstId)
      {
        int fid_i = find_col_idx(schema, "firstId");
        if (fid_i < 0) throw runtime_error("depth: missing firstId");
        fid.emplace(rg->Column(fid_i), schema->Column(fid_i));
      }
      if (sel.lastId)
      {
        int lid_i = find_col_idx(schema, "lastId");
        if (lid_i < 0) throw runtime_error("depth: missing lastId");
        lid.emplace(rg->Column(lid_i), schema->Column(lid_i));
      }
      if (sel.eventTime)
      {
        int evt_i = find_col_idx(schema, "eventTime");
        if (evt_i < 0) throw runtime_error("depth: missing eventTime");
        evt.emplace(rg->Column(evt_i), schema->Column(evt_i));
      }

      bool need_asks = (sel.ask_px || sel.ask_qty);
      bool need_bids = (sel.bid_px || sel.bid_qty);

      optional<Int64Cursor> apx;
      optional<Int64Cursor> aqty;
      optional<Int64Cursor> bpx;
      optional<Int64Cursor> bqty;

      if (need_asks)
      {
        if (sel.ask_px)
        {
          int apx_i = find_col_idx(schema, "ask.list.element.px");
          if (apx_i < 0) throw runtime_error("depth: missing ask px");
          apx.emplace(rg->Column(apx_i), schema->Column(apx_i));
        }
        if (sel.ask_qty)
        {
          int aqty_i = find_col_idx(schema, "ask.list.element.qty");
          if (aqty_i < 0) throw runtime_error("depth: missing ask qty");
          aqty.emplace(rg->Column(aqty_i), schema->Column(aqty_i));
        }
      }

      if (need_bids)
      {
        if (sel.bid_px)
        {
          int bpx_i = find_col_idx(schema, "bid.list.element.px");
          if (bpx_i < 0) throw runtime_error("depth: missing bid px");
          bpx.emplace(rg->Column(bpx_i), schema->Column(bpx_i));
        }
        if (sel.bid_qty)
        {
          int bqty_i = find_col_idx(schema, "bid.list.element.qty");
          if (bqty_i < 0) throw runtime_error("depth: missing bid qty");
          bqty.emplace(rg->Column(bqty_i), schema->Column(bqty_i));
        }
      }

      v_ts.clear();
      v_fid.clear();
      v_lid.clear();
      v_evt.clear();
      ask_off.clear();
      ask_px.clear();
      ask_qty.clear();
      bid_off.clear();
      bid_px.clear();
      bid_qty.clear();

      v_ts.reserve(rows);
      if (sel.firstId)   v_fid.reserve(rows);
      if (sel.lastId)    v_lid.reserve(rows);
      if (sel.eventTime) v_evt.reserve(rows);
      if (need_asks) { ask_off.reserve(rows + 1); ask_off.push_back(0); }
      if (need_bids) { bid_off.reserve(rows + 1); bid_off.push_back(0); }

      for (int64_t r = 0; r < rows; ++r)
      {
        Entry e_ts = ts.take();

        Entry e_fid{};
        Entry e_lid{};
        Entry e_evt{};
        if (sel.firstId)   e_fid = fid->take();
        if (sel.lastId)    e_lid = lid->take();
        if (sel.eventTime) e_evt = evt->take();

        bool in_range = (e_ts.value >= start_ns && e_ts.value < end_ns);

        uint32_t asks_added = 0;
        uint32_t bids_added = 0;

        if (need_asks)
        {
          if (sel.ask_px && sel.ask_qty)
          {
            asks_added = append_list_pairs_for_row_typed(
                *apx, *aqty,
                in_range ? &ask_px : nullptr,
                in_range ? &ask_qty : nullptr);
          }
          else if (sel.ask_px)
          {
            asks_added = append_list_from_leaf_for_row(*apx, in_range ? &ask_px : nullptr);
          }
          else if (sel.ask_qty)
          {
            asks_added = append_list_from_leaf_for_row(*aqty, in_range ? &ask_qty : nullptr);
          }
        }

        if (need_bids)
        {
          if (sel.bid_px && sel.bid_qty)
          {
            bids_added = append_list_pairs_for_row_typed(
                *bpx, *bqty,
                in_range ? &bid_px : nullptr,
                in_range ? &bid_qty : nullptr);
          }
          else if (sel.bid_px)
          {
            bids_added = append_list_from_leaf_for_row(*bpx, in_range ? &bid_px : nullptr);
          }
          else if (sel.bid_qty)
          {
            bids_added = append_list_from_leaf_for_row(*bqty, in_range ? &bid_qty : nullptr);
          }
        }

        if (in_range)
        {
          v_ts.push_back(e_ts.value);
          if (sel.firstId)   v_fid.push_back(e_fid.value);
          if (sel.lastId)    v_lid.push_back(e_lid.value);
          if (sel.eventTime) v_evt.push_back(e_evt.value);

          if (need_asks) ask_off.push_back(ask_off.back() + asks_added);
          if (need_bids) bid_off.push_back(bid_off.back() + bids_added);
        }
      }

      if (!v_ts.empty()) return true;
    }
  }
};

// ======== ShardedDB (PIMPL) ========

struct ShardedDB::Impl
{
  string root_;
  optional<string> sampling_;

  Impl(string root, optional<string> sampling)
  : root_(move(root)), sampling_(move(sampling)) {}

  unique_ptr<TopBatchReader>   get_top  (int64_t s, int64_t e, const string& symb, optional<string> market, TopSelect sel) const;
  unique_ptr<TradeBatchReader> get_trade(int64_t s, int64_t e, const string& symb, optional<string> market, TradeSelect sel) const;
  unique_ptr<DeltaBatchReader> get_depth(int64_t s, int64_t e, const string& symb, optional<string> market, DeltaSelect sel) const;
};

struct ShardedDB::TopBatchReader::Impl
{
  vector<Candidate> files_;
  size_t file_idx_ = 0;
  unique_ptr<FileStreamerTopCols> fs_;
  int64_t start_ns_;
  int64_t end_ns_;
  TopSelect sel_;

  // batch buffers
  vector<int64_t> ts_;
  vector<int64_t> apx_;
  vector<int64_t> aq_;
  vector<int64_t> bpx_;
  vector<int64_t> bq_;
  vector<int64_t> val_;
  // sampled extras
  vector<int64_t> min_bpx_, max_bpx_, min_apx_, max_apx_;
  vector<int64_t> min_bts_, max_bts_, min_ats_, max_ats_;

  // current file basename (lifetime until next() is called again)
  string cur_file_base_;

  Impl(vector<Candidate> files, int64_t s, int64_t e, TopSelect sel)
  : files_(move(files)), start_ns_(s), end_ns_(e), sel_(sel)
  {
    if (g_debug) {
      cerr << "[debug] top: " << files_.size() << " candidate files\n";
      for (const auto& c : files_) {
        cerr << "  - " << c.path << " [" << iso_from_ns(c.file_start_ns)
             << " .. " << iso_from_ns(c.file_end_ns) << ")\n";
      }
    }
  }

  bool next(TopColsView& out)
  {
    while (true)
    {
      if (!fs_)
      {
        if (file_idx_ >= files_.size()) return false;

        // Prefetch next file (if any)
        if (file_idx_ + 1 < files_.size()) prefetch_path(files_[file_idx_ + 1].path);

        try
        {
          fs_ = make_unique<FileStreamerTopCols>(files_[file_idx_].path);
          cur_file_base_ = fs::path(files_[file_idx_].path).filename().string();
        }
        catch (const exception& e)
        {
          cerr << "WARN: open failed: " << files_[file_idx_].path << " : " << e.what() << "\n";
          ++file_idx_;
          continue;
        }
      }

      bool ok = false;

      try
      {
        ok = fs_->next_rg(
            start_ns_, end_ns_, sel_,
            ts_, apx_, aq_, bpx_, bq_, val_,
            min_bpx_, max_bpx_, min_apx_, max_apx_,
            min_bts_, max_bts_, min_ats_, max_ats_);
      }
      catch (const exception& e)
      {
        cerr << "WARN: read failed: " << files_[file_idx_].path << " : " << e.what() << "\n";
        ok = false;
      }

      if (!ok)
      {
        fs_.reset();
        ++file_idx_;
        continue;
      }

      if (ts_.empty()) continue;

      out.ts        = sel_.ts        ? ts_.data()       : nullptr;
      out.ask_px    = sel_.ask_px    ? apx_.data()      : nullptr;
      out.ask_qty   = sel_.ask_qty   ? aq_.data()       : nullptr;
      out.bid_px    = sel_.bid_px    ? bpx_.data()      : nullptr;
      out.bid_qty   = sel_.bid_qty   ? bq_.data()       : nullptr;
      out.valu      = sel_.valu      ? val_.data()      : nullptr;

      out.min_bid_px = sel_.min_bid_px ? min_bpx_.data() : nullptr;
      out.max_bid_px = sel_.max_bid_px ? max_bpx_.data() : nullptr;
      out.min_ask_px = sel_.min_ask_px ? min_apx_.data() : nullptr;
      out.max_ask_px = sel_.max_ask_px ? max_apx_.data() : nullptr;
      out.min_bid_ts = sel_.min_bid_ts ? min_bts_.data() : nullptr;
      out.max_bid_ts = sel_.max_bid_ts ? max_bts_.data() : nullptr;
      out.min_ask_ts = sel_.min_ask_ts ? min_ats_.data() : nullptr;
      out.max_ask_ts = sel_.max_ask_ts ? max_ats_.data() : nullptr;

      out.file = cur_file_base_.c_str();
      out.n = ts_.size();
      return true;
    }
  }
};

struct ShardedDB::TradeBatchReader::Impl
{
  vector<Candidate> files_;
  size_t file_idx_ = 0;
  unique_ptr<FileStreamerTradeCols> fs_;
  int64_t start_ns_;
  int64_t end_ns_;
  TradeSelect sel_;

  vector<int64_t> ts_;
  vector<int64_t> px_;
  vector<int64_t> qty_;
  vector<int64_t> tid_;
  vector<int64_t> boid_;
  vector<int64_t> soid_;
  vector<int64_t> ttime_;
  vector<uint8_t> isMkt_;
  vector<int64_t> evt_;

  string cur_file_base_;

  Impl(vector<Candidate> files, int64_t s, int64_t e, TradeSelect sel)
  : files_(move(files)), start_ns_(s), end_ns_(e), sel_(sel)
  {
    if (g_debug) {
      cerr << "[debug] trade: " << files_.size() << " candidate files\n";
      for (const auto& c : files_) {
        cerr << "  - " << c.path << " [" << iso_from_ns(c.file_start_ns)
             << " .. " << iso_from_ns(c.file_end_ns) << ")\n";
      }
    }
  }

  bool next(TradeColsView& out)
  {
    while (true)
    {
      if (!fs_)
      {
        if (file_idx_ >= files_.size()) return false;

        // Prefetch next file (if any)
        if (file_idx_ + 1 < files_.size()) prefetch_path(files_[file_idx_ + 1].path);

        try
        {
          fs_ = make_unique<FileStreamerTradeCols>(files_[file_idx_].path);
          cur_file_base_ = fs::path(files_[file_idx_].path).filename().string();
        }
        catch (const exception& e)
        {
          cerr << "WARN: open failed: " << files_[file_idx_].path << " : " << e.what() << "\n";
          ++file_idx_;
          continue;
        }
      }

      bool ok = false;

      try
      {
        ok = fs_->next_rg(
            start_ns_, end_ns_, sel_,
            ts_, px_, qty_, tid_, boid_, soid_, ttime_, isMkt_, evt_);
      }
      catch (const exception& e)
      {
        cerr << "WARN: read failed: " << files_[file_idx_].path << " : " << e.what() << "\n";
        ok = false;
      }

      if (!ok)
      {
        fs_.reset();
        ++file_idx_;
        continue;
      }

      if (ts_.empty()) continue;

      out.ts            = sel_.ts            ? ts_.data()     : nullptr;
      out.px            = sel_.px            ? px_.data()     : nullptr;
      out.qty           = sel_.qty           ? qty_.data()    : nullptr;
      out.tradeId       = sel_.tradeId       ? tid_.data()    : nullptr;
      out.buyerOrderId  = sel_.buyerOrderId  ? boid_.data()   : nullptr;
      out.sellerOrderId = sel_.sellerOrderId ? soid_.data()   : nullptr;
      out.tradeTime     = sel_.tradeTime     ? ttime_.data()  : nullptr;
      out.isMarket      = sel_.isMarket      ? isMkt_.data()  : nullptr;
      out.eventTime     = sel_.eventTime     ? evt_.data()    : nullptr;

      out.file = cur_file_base_.c_str();
      out.n    = ts_.size();
      return true;
    }
  }
};

struct ShardedDB::DeltaBatchReader::Impl
{
  vector<Candidate> files_;
  size_t file_idx_ = 0;
  unique_ptr<FileStreamerDeltaCols> fs_;
  int64_t start_ns_;
  int64_t end_ns_;
  DeltaSelect sel_;

  vector<int64_t> ts_;
  vector<int64_t> fid_;
  vector<int64_t> lid_;
  vector<int64_t> evt_;
  vector<uint32_t> ask_off_;
  vector<uint32_t> bid_off_;
  vector<int64_t>  ask_px_;
  vector<int64_t>  ask_qty_;
  vector<int64_t>  bid_px_;
  vector<int64_t>  bid_qty_;

  string cur_file_base_;

  Impl(vector<Candidate> files, int64_t s, int64_t e, DeltaSelect sel)
  : files_(move(files)), start_ns_(s), end_ns_(e), sel_(sel)
  {
    if (g_debug) {
      cerr << "[debug] depth: " << files_.size() << " candidate files\n";
      for (const auto& c : files_) {
        cerr << "  - " << c.path << " [" << iso_from_ns(c.file_start_ns)
             << " .. " << iso_from_ns(c.file_end_ns) << ")\n";
      }
    }
  }

  bool next(DeltaColsView& out)
  {
    while (true)
    {
      if (!fs_)
      {
        if (file_idx_ >= files_.size()) return false;

        // Prefetch next file (if any)
        if (file_idx_ + 1 < files_.size()) prefetch_path(files_[file_idx_ + 1].path);

        try
        {
          fs_ = make_unique<FileStreamerDeltaCols>(files_[file_idx_].path);
          cur_file_base_ = fs::path(files_[file_idx_].path).filename().string();
        }
        catch (const exception& e)
        {
          cerr << "WARN: open failed: " << files_[file_idx_].path << " : " << e.what() << "\n";
          ++file_idx_;
          continue;
        }
      }

      bool ok = false;

      try
      {
        ok = fs_->next_rg(
            start_ns_, end_ns_, sel_,
            ts_, fid_, lid_, evt_,
            ask_off_, ask_px_, ask_qty_,
            bid_off_, bid_px_, bid_qty_);
      }
      catch (const exception& e)
      {
        cerr << "WARN: read failed: " << files_[file_idx_].path << " : " << e.what() << "\n";
        ok = false;
      }

      if (!ok)
      {
        fs_.reset();
        ++file_idx_;
        continue;
      }

      if (ts_.empty()) continue;

      out.ts        = sel_.ts        ? ts_.data()  : nullptr;
      out.firstId   = sel_.firstId   ? fid_.data() : nullptr;
      out.lastId    = sel_.lastId    ? lid_.data() : nullptr;
      out.eventTime = sel_.eventTime ? evt_.data() : nullptr;

      bool have_asks = sel_.ask_px || sel_.ask_qty;
      bool have_bids = sel_.bid_px || sel_.bid_qty;

      out.ask_off = have_asks ? ask_off_.data() : nullptr;
      out.ask_px  = sel_.ask_px ? ask_px_.data() : nullptr;
      out.ask_qty = sel_.ask_qty ? ask_qty_.data() : nullptr;

      out.bid_off = have_bids ? bid_off_.data() : nullptr;
      out.bid_px  = sel_.bid_px ? bid_px_.data() : nullptr;
      out.bid_qty = sel_.bid_qty ? bid_qty_.data() : nullptr;

      out.file = cur_file_base_.c_str();
      out.n    = ts_.size();
      return true;
    }
  }
};

// ---- ShardedDB methods

ShardedDB::ShardedDB(std::string root, std::optional<std::string> sampling)
: impl_(make_unique<Impl>(move(root), move(sampling))) {}

ShardedDB::~ShardedDB() = default;
ShardedDB::ShardedDB(ShardedDB&&) noexcept = default;
ShardedDB& ShardedDB::operator=(ShardedDB&&) noexcept = default;

ShardedDB::TopBatchReader::TopBatchReader(std::unique_ptr<Impl> impl) : impl_(std::move(impl)) {}
ShardedDB::TopBatchReader::TopBatchReader(TopBatchReader&&) noexcept = default;
ShardedDB::TopBatchReader& ShardedDB::TopBatchReader::operator=(TopBatchReader&&) noexcept = default;
ShardedDB::TopBatchReader::~TopBatchReader() = default;
bool ShardedDB::TopBatchReader::next(TopColsView& out) { return impl_->next(out); }

ShardedDB::TradeBatchReader::TradeBatchReader(std::unique_ptr<Impl> impl) : impl_(std::move(impl)) {}
ShardedDB::TradeBatchReader::TradeBatchReader(TradeBatchReader&&) noexcept = default;
ShardedDB::TradeBatchReader& ShardedDB::TradeBatchReader::operator=(TradeBatchReader&&) noexcept = default;
ShardedDB::TradeBatchReader::~TradeBatchReader() = default;
bool ShardedDB::TradeBatchReader::next(TradeColsView& out) { return impl_->next(out); }

ShardedDB::DeltaBatchReader::DeltaBatchReader(std::unique_ptr<Impl> impl) : impl_(std::move(impl)) {}
ShardedDB::DeltaBatchReader::DeltaBatchReader(DeltaBatchReader&&) noexcept = default;
ShardedDB::DeltaBatchReader& ShardedDB::DeltaBatchReader::operator=(DeltaBatchReader&&) noexcept = default;
ShardedDB::DeltaBatchReader::~DeltaBatchReader() = default;
bool ShardedDB::DeltaBatchReader::next(DeltaColsView& out) { return impl_->next(out); }

// Market-aware
unique_ptr<ShardedDB::TopBatchReader> ShardedDB::Impl::get_top(int64_t s, int64_t e, const string& symb, optional<string> market, TopSelect sel) const
{
  auto files = candidate_files_strict(root_, symb, "top", market, s, e, sampling_);
  auto impl = make_unique<TopBatchReader::Impl>(move(files), s, e, sel);
  return make_unique<TopBatchReader>(move(impl));
}
unique_ptr<ShardedDB::TradeBatchReader> ShardedDB::Impl::get_trade(int64_t s, int64_t e, const string& symb, optional<string> market, TradeSelect sel) const
{
  auto files = candidate_files_strict(root_, symb, "trade", market, s, e, nullopt);
  auto impl = make_unique<TradeBatchReader::Impl>(move(files), s, e, sel);
  return make_unique<TradeBatchReader>(move(impl));
}
unique_ptr<ShardedDB::DeltaBatchReader> ShardedDB::Impl::get_depth(int64_t s, int64_t e, const string& symb, optional<string> market, DeltaSelect sel) const
{
  auto files = candidate_files_strict(root_, symb, "depth", market, s, e, nullopt);
  auto impl = make_unique<DeltaBatchReader::Impl>(move(files), s, e, sel);
  return make_unique<DeltaBatchReader>(move(impl));
}

// Public API (market-aware)
unique_ptr<ShardedDB::TopBatchReader>
ShardedDB::get_top_cols(int64_t s, int64_t e, const string& symb, optional<string> market, TopSelect sel) const
{
  return impl_->get_top(s, e, symb, move(market), sel);
}
unique_ptr<ShardedDB::TradeBatchReader>
ShardedDB::get_trade_cols(int64_t s, int64_t e, const string& symb, optional<string> market, TradeSelect sel) const
{
  return impl_->get_trade(s, e, symb, move(market), sel);
}
unique_ptr<ShardedDB::DeltaBatchReader>
ShardedDB::get_depth_cols(int64_t s, int64_t e, const string& symb, optional<string> market, DeltaSelect sel) const
{
  return impl_->get_depth(s, e, symb, move(market), sel);
}

// Backward compatible (search both markets)
unique_ptr<ShardedDB::TopBatchReader>   ShardedDB::get_top_cols  (int64_t s, int64_t e, const string& symb, TopSelect sel) const   { return impl_->get_top(s, e, symb, nullopt, sel); }
unique_ptr<ShardedDB::TradeBatchReader> ShardedDB::get_trade_cols(int64_t s, int64_t e, const string& symb, TradeSelect sel) const { return impl_->get_trade(s, e, symb, nullopt, sel); }
unique_ptr<ShardedDB::DeltaBatchReader> ShardedDB::get_depth_cols(int64_t s, int64_t e, const string& symb, DeltaSelect sel) const { return impl_->get_depth(s, e, symb, nullopt, sel); }


