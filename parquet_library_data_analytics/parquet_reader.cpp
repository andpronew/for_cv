// parquet_reader.cpp (thin CLI over parquet_reader_lib + direct PX reader for market-aware px layout)
// Build:
//   g++ -std=gnu++23 -O3 parquet_reader.cpp parquet_reader_lib.cpp -lparquet -larrow -lzstd -o parquet_reader
//   g++ -std=gnu++23 -O3 -DNDEBUG -march=native -mtune=native -flto=auto -fno-plt parquet_reader.cpp parquet_reader_lib.cpp -lparquet -larrow -lzstd -o parquet_reader -g

#include "parquet_reader_lib.h"

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/api/reader.h>

#include <cctype>
#include <cstdint>
#include <cstdlib>
#include <ctime>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <limits>
#include <optional>
#include <regex>
#include <sstream>
#include <string>
#include <vector>
#include <cmath>   // for std::llround
#include <algorithm>
#include <fstream>
#include <chrono>  // perf timing

#if defined(__linux__)
  #include <fcntl.h>
  #include <sys/stat.h>
  #include <unistd.h>
#endif

using namespace std;
namespace fs = std::filesystem;

// ---------- OS + prefetch helpers (Linux) ----------

static inline void prefetch_path(const std::string& path) {
#if defined(__linux__)
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

static std::string detect_os_pretty() {
#if defined(__linux__)
  std::ifstream f("/etc/os-release");
  std::string line;
  while (std::getline(f, line)) {
    const std::string k = "PRETTY_NAME=";
    if (line.rfind(k, 0) == 0) {
      std::string v = line.substr(k.size());
      if (!v.empty() && v.front()=='"' && v.back()=='"') v = v.substr(1, v.size()-2);
      return v;
    }
  }
  return std::string();
#else
  return std::string();
#endif
}

// ---------- small helpers ----------

static string norm_token(string s) {
  for (char& c : s) c = (char)tolower((unsigned char)c);
  string out; out.reserve(s.size());
  for (char c : s) if ((c>='a'&&c<='z')||(c>='0'&&c<='9')) out.push_back(c);
  return out;
}

static vector<string> split_csv(const string& s) {
  vector<string> v; string cur;
  for (char c : s) { if (c==','){ if(!cur.empty()){ v.push_back(cur); cur.clear(); } } else cur.push_back(c); }
  if (!cur.empty()) v.push_back(cur);
  return v;
}

static bool is_number_like(const string& s) {
  bool dot=false, sign=false, digit=false, exp=false;
  for (size_t i=0;i<s.size();++i){
    char c=s[i];
    if (c=='+'||c=='-'){ if (i!=0) return false; sign=true; }
    else if (isdigit((unsigned char)c)) digit=true;
    else if (c=='.'){ if (dot) return false; dot=true; }
    else if (c=='e'||c=='E'){ if (exp || !digit) return false; exp=true; dot=true; digit=false; }
    else return false;
  }
  return digit;
}

// Old human ISO (seconds) kept for debug; main human output uses ms now.
static string iso_from_ns(int64_t ns) {
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

// Human ISO8601 with milliseconds
static string iso_from_ns_ms(int64_t ns) {
  time_t s = static_cast<time_t>(ns / 1'000'000'000LL);
  int64_t ms = (ns / 1'000'000LL) % 1000;
  tm tm{}; gmtime_r(&s, &tm);
  ostringstream o;
  o << setfill('0')
    << setw(4) << (tm.tm_year + 1900) << '-'
    << setw(2) << (tm.tm_mon + 1) << '-'
    << setw(2) << tm.tm_mday << 'T'
    << setw(2) << tm.tm_hour << ':'
    << setw(2) << tm.tm_min  << ':'
    << setw(2) << tm.tm_sec  << '.'
    << setw(3) << ms << 'Z';
  return o.str();
}

static inline int64_t to_ns(double sec){
  long double x = static_cast<long double>(sec) * 1'000'000'000.0L;
  if (x < static_cast<long double>(numeric_limits<int64_t>::min())) return numeric_limits<int64_t>::min();
  if (x > static_cast<long double>(numeric_limits<int64_t>::max())) return numeric_limits<int64_t>::max();
  return static_cast<int64_t>(std::llround(x));
}

// ---------- printing helpers ----------

enum class TsFormat { Human, RawNs };
enum class IdxMode { Printed, Raw, None };

struct PrintCfg {
  TsFormat ts_fmt = TsFormat::Human;   // default human (ISO with .mmm)
  bool pxqty_double = true;            // default double scaled 1e8 (fixed)
  bool raw_override = false;           // if true, px/qty printed raw even if pxqty_double=true
  int precision_px = 8;                // fixed decimals for px
  int precision_qty = 8;               // fixed decimals for qty
  optional<int64_t> gap_ns;            // if set, print only boundary pairs for gaps >= this
  bool print_fn = false;               // print filename events (stderr) when a new file starts
  IdxMode idx_mode = IdxMode::None;    // default none (suppressed)
  bool header = false;                 // print header row
};

static void print_ts_fmt(ostream& os, int64_t ns, TsFormat fmt) {
  if (fmt == TsFormat::Human) {
    os << iso_from_ns_ms(ns);
  } else {
    os << ns; // raw nanoseconds
  }
}

static void print_scaled1e8_fixed(ostream& os, int64_t raw, int precision) {
  std::ios::fmtflags f = os.flags();
  auto p = os.precision();
  os.setf(std::ios::fixed);
  os << setprecision(precision) << (static_cast<long double>(raw) / 1'0000'0000.0L);
  os.flags(f);
  os.precision(p);
}

static inline void print_px_val(ostream& os, int64_t raw, const PrintCfg& pcfg) {
  if (pcfg.pxqty_double && !pcfg.raw_override) print_scaled1e8_fixed(os, raw, pcfg.precision_px);
  else os << raw;
}
static inline void print_qty_val(ostream& os, int64_t raw, const PrintCfg& pcfg) {
  if (pcfg.pxqty_double && !pcfg.raw_override) print_scaled1e8_fixed(os, raw, pcfg.precision_qty);
  else os << raw;
}

static inline void print_gap_s_ms(ostream& os, int64_t dt_ns) {
  std::ios::fmtflags f = os.flags();
  auto p = os.precision();
  os.setf(std::ios::fixed);
  os << setprecision(3) << (static_cast<long double>(dt_ns) / 1'000'000'000.0L);
  os.flags(f);
  os.precision(p);
}

static inline void print_prefix(ostream& os,
                                const PrintCfg& pcfg,
                                const string* /*filename1_opt*/,
                                const string* /*filename2_opt*/,
                                optional<uint64_t> idx1,
                                optional<uint64_t> idx2)
{
  bool need_idx = (pcfg.idx_mode != IdxMode::None);
  if (!need_idx) return;

  bool first = true;
  auto put = [&](const string& s){
    if (!first) os << " ;";
    os << s;
    first = false;
  };

  if (pcfg.idx_mode == IdxMode::Raw && idx1.has_value() && idx2.has_value()) {
    put(to_string(*idx1) + "," + to_string(*idx2));
  } else if (idx1.has_value()) {
    put(to_string(*idx1));
  } else {
    put("-");
  }
  os << " ;";
}

// ---------- file-open printer with raw idx + perf (M rec/s) ----------

struct FnPrinter {
  bool enabled = false;
  bool have = false;
  std::string last_fn;
  uint64_t last_idx = 0; // raw index at previous file switch
  std::chrono::steady_clock::time_point last_tp{};

  void open(const std::string& new_fn, uint64_t cur_idx) {
    if (!enabled) return;
    auto now = std::chrono::steady_clock::now();
    if (!have) {
      std::cerr << "[file idx=" << cur_idx << "] " << new_fn << "\n";
      have = true;
      last_fn = new_fn;
      last_idx = cur_idx;
      last_tp = now;
      return;
    }
    if (new_fn == last_fn) return; // avoid duplicate
    double dt = std::chrono::duration<double>(now - last_tp).count();
    uint64_t rows = (cur_idx >= last_idx ? cur_idx - last_idx : 0);
    double rateM = (dt > 0.0) ? (rows / dt) / 1e6 : 0.0;

    std::ios::fmtflags f = std::cerr.flags();
    auto p = std::cerr.precision();
    std::cerr.setf(std::ios::fixed);
    std::cerr << "[file idx=" << cur_idx
              << " ; rate=" << std::setprecision(3) << rateM << "M rec/s] "
              << new_fn << "\n";
    std::cerr.flags(f);
    std::cerr.precision(p);

    last_fn = new_fn;
    last_idx = cur_idx;
    last_tp = now;
  }

  void finish(uint64_t cur_idx) {
    if (!enabled || !have) return;
    auto now = std::chrono::steady_clock::now();
    double dt = std::chrono::duration<double>(now - last_tp).count();
    uint64_t rows = (cur_idx >= last_idx ? cur_idx - last_idx : 0);
    double rateM = (dt > 0.0) ? (rows / dt) / 1e6 : 0.0;

    std::ios::fmtflags f = std::cerr.flags();
    auto p = std::cerr.precision();
    std::cerr.setf(std::ios::fixed);
    std::cerr << "[file idx=" << cur_idx
              << " ; rate=" << std::setprecision(3) << rateM << "M rec/s] "
              << last_fn << " (end)\n";
    std::cerr.flags(f);
    std::cerr.precision(p);
  }
};

// ---------- parse TYPE ----------

struct ParsedType {
  string base;                // "top" | "trade" | "depth"
  optional<string> market;    // "spot" | "fut" (optional)
};

static ParsedType parse_type(string t) {
  for (char& c : t) c = (char)tolower((unsigned char)c);
  if (t=="top" || t=="trade" || t=="depth") return {t, nullopt};
  if (t=="top_fut")    return {"top",   string("fut")};
  if (t=="top_spot")   return {"top",   string("spot")};
  if (t=="trade_fut")  return {"trade", string("fut")};
  if (t=="trade_spot") return {"trade", string("spot")};
  if (t=="depth_fut")  return {"depth", string("fut")};
  if (t=="depth_spot") return {"depth", string("spot")};
  throw runtime_error("Unsupported TYPE: " + t + " (use top|trade|depth or *_fut|*_spot)");
}

// ---------- CSV -> selects ----------

static TopSelect make_top_select_from_csv(const string& csv)
{
  TopSelect sel{};
  sel.ts=false; sel.ask_px=false; sel.ask_qty=false; sel.bid_px=false; sel.bid_qty=false; sel.valu=false;
  sel.min_bid_px=sel.max_bid_px=sel.min_ask_px=sel.max_ask_px=false;
  sel.min_bid_ts=sel.max_bid_ts=sel.min_ask_ts=sel.max_ask_ts=false;

  for (const string& t : split_csv(csv)) {
    string k = norm_token(t);
    if (k=="ts"||k=="time") sel.ts = true;
    else if (k=="askpx"||k=="px"||k=="ask"||k=="askprice") sel.ask_px = true;
    else if (k=="askqty"||k=="qty"||k=="asksize") sel.ask_qty = true;
    else if (k=="bidpx"||k=="bid"||k=="bidprice") sel.bid_px = true;
    else if (k=="bidqty"||k=="bidsize") sel.bid_qty = true;
    else if (k=="valu"||k=="value"||k=="vol"||k=="volume") sel.valu = true;
    else if (k=="minbidpx") sel.min_bid_px = true;
    else if (k=="maxbidpx") sel.max_bid_px = true;
    else if (k=="minaskpx") sel.min_ask_px = true;
    else if (k=="maxaskpx") sel.max_ask_px = true;
    else if (k=="minbidts") sel.min_bid_ts = true;
    else if (k=="maxbidts") sel.max_bid_ts = true;
    else if (k=="minaskts") sel.min_ask_ts = true;
    else if (k=="maxaskts") sel.max_ask_ts = true;
  }
  return sel;
}

static DeltaSelect make_depth_select_from_csv(const string& csv)
{
  DeltaSelect sel{};
  sel.ts=false; sel.firstId=false; sel.lastId=false; sel.eventTime=false;
  sel.ask_px=false; sel.ask_qty=false; sel.bid_px=false; sel.bid_qty=false;

  for (const string& t : split_csv(csv)) {
    string k = norm_token(t);
    if (k=="ts"||k=="time") sel.ts = true;
    else if (k=="firstid"||k=="fid") sel.firstId = true;
    else if (k=="lastid"||k=="lid") sel.lastId = true;
    else if (k=="eventtime"||k=="evt"||k=="event") sel.eventTime = true;
    else if (k=="askpx"||k=="px"||k=="ask"||k=="askprice") sel.ask_px = true;
    else if (k=="askqty"||k=="qty"||k=="asksize") sel.ask_qty = true;
    else if (k=="bidpx"||k=="bid"||k=="bidprice") sel.bid_px = true;
    else if (k=="bidqty"||k=="bidsize") sel.bid_qty = true;
  }
  return sel;
}

static TradeSelect make_trade_select_from_csv(const string& csv)
{
  TradeSelect sel{};
  sel.ts=false; sel.px=false; sel.qty=false; sel.tradeId=false;
  sel.buyerOrderId=false; sel.sellerOrderId=false; sel.tradeTime=false;
  sel.isMarket=false; sel.eventTime=false;

  for (const string& t : split_csv(csv)) {
    string k = norm_token(t);
    if (k=="ts"||k=="time") sel.ts = true;
    else if (k=="px"||k=="price") sel.px = true;
    else if (k=="qty"||k=="size"||k=="quantity") sel.qty = true;
    else if (k=="tradeid"||k=="tid") sel.tradeId = true;
    else if (k=="buyerorderid"||k=="boid") sel.buyerOrderId = true;
    else if (k=="sellerorderid"||k=="soid") sel.sellerOrderId = true;
    else if (k=="tradetime"||k=="ttime") sel.tradeTime = true;
    else if (k=="ismarket"||k=="market") sel.isMarket = true;
    else if (k=="eventtime"||k=="evt"||k=="event") sel.eventTime = true;
  }
  return sel;
}

// ---------- Parquet (px) helpers ----------

static int find_col_idx(const parquet::SchemaDescriptor* schema, const string& name) {
  for (int i = 0; i < schema->num_columns(); ++i)
    if (schema->Column(i)->path()->ToDotString() == name) return i;
  return -1;
}

static void read_i64_column(parquet::RowGroupReader& rg, int col_idx, vector<int64_t>& out) {
  const int64_t rows = rg.metadata()->num_rows();
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
  if (done != rows) throw runtime_error("Short read");
}

struct PxFile {
  int y=0, m=0;
  string path;
};
static vector<PxFile> list_px_months(const string& root, const string& symb, const string& market /*spot|fut*/) {
  vector<PxFile> out;
  string dir_name = string("top_px_") + market;
  fs::path dir = fs::path(root) / dir_name / symb;
  if (!fs::exists(dir) || !fs::is_directory(dir)) return out;

  // bn_top_px_spot_<SYMBOL>_<Y>_<M>.parquet   OR   ..._fut_...
  const string pre = string("bn_top_px_") + market + "_" + symb + "_";
  regex pat("^" + pre + R"(([0-9]{4})_([0-9]{1,2})\.parquet$)");

  for (auto& ent : fs::directory_iterator(dir)) {
    if (!ent.is_regular_file() || ent.path().extension() != ".parquet") continue;
    string f = ent.path().filename().string(); smatch m;
    if (regex_match(f, m, pat) && m.size()==3) {
      PxFile pf; pf.y = stoi(m[1].str()); pf.m = stoi(m[2].str()); pf.path = ent.path().string();
      out.push_back(std::move(pf));
    }
  }
  sort(out.begin(), out.end(), [](const PxFile& a, const PxFile& b){ return tie(a.y,a.m) < tie(b.y,b.m); });
  return out;
}

// ----- helpers to render rows to strings -----

static string render_px_row_content(int64_t ts, int64_t ap, int64_t bp,
                                    bool need_ts, bool need_ask, bool need_bid,
                                    const PrintCfg& pcfg)
{
  ostringstream os;
  bool first = true;
  auto put_sep = [&]{ if (!first) os<<';'; first=false; };

  if (need_ts) { put_sep(); print_ts_fmt(os, ts, pcfg.ts_fmt); }
  if (need_ask){ put_sep(); print_px_val(os, ap, pcfg); }
  if (need_bid){ put_sep(); print_px_val(os, bp, pcfg); }
  return os.str();
}

// Build header strings
static string header_from_names(const vector<string>& names) {
  ostringstream os;
  for (size_t i=0;i<names.size();++i){ if(i) os<<';'; os<<names[i]; }
  return os.str();
}

// ==================== px direct (lazy strings in gap mode) ====================
static int dump_px_direct(const string& root,
                          const string& symb,
                          const string& market,     // "spot" | "fut"
                          int64_t start_ns,
                          int64_t end_ns,
                          uint64_t seen_every,
                          bool debug,
                          const TopSelect& sel_from_csv,
                          const PrintCfg& pcfg,
                          bool prefetch)
{
  TopSelect sel = sel_from_csv;
  bool print_ts  = sel.ts;
  bool need_bid = sel.bid_px;
  bool need_ask = sel.ask_px;
  if (!print_ts && !need_bid && !need_ask) { print_ts = need_bid = need_ask = true; }

  auto files = list_px_months(root, symb, market);
  if (files.empty()) {
    cerr << "ERROR(px): no files under " << (fs::path(root)/("top_px_"+market)/symb).string() << "\n";
    return 2;
  }

  if (debug) {
    cerr << "[debug:px] root=" << root << " market=" << market << " symb=" << symb
         << " window=[" << iso_from_ns(start_ns) << " .. " << iso_from_ns(end_ns) << ")\n";
    cerr << "[debug:px] files:\n";
    for (auto& f: files) cerr << "  " << f.path << "\n";
  }

  if (pcfg.header) {
    vector<string> names;
    if (pcfg.idx_mode != IdxMode::None) names.push_back("idx");
    auto add_cols = [&](const string& prefix){
      if (print_ts) names.push_back(prefix + "ts");
      if (need_ask) names.push_back(prefix + "ask_px");
      if (need_bid) names.push_back(prefix + "bid_px");
    };
    if (pcfg.gap_ns) { add_cols("first_"); add_cols("last_"); names.push_back("gap"); }
    else { add_cols(""); }
    cout << header_from_names(names) << '\n';
  }

  uint64_t printed_idx = 0;
  uint64_t raw_idx_global = 0;
  bool gap_mode = pcfg.gap_ns.has_value();

  // gap-state
  bool have_prev = false;
  int64_t prev_ts = 0;
  uint64_t prev_raw_idx = 0;
  size_t prev_i = 0;
  bool prev_str_ready = false;
  string prev_str;

  FnPrinter fnp; fnp.enabled = pcfg.print_fn;

  for (size_t fi=0; fi<files.size(); ++fi) {
    const auto& f = files[fi];

    if (prefetch && fi + 1 < files.size())
      prefetch_path(files[fi + 1].path);

    std::unique_ptr<parquet::ParquetFileReader> reader;
    try {
      reader = parquet::ParquetFileReader::OpenFile(f.path, /*memory_map=*/true);
      if (pcfg.print_fn) fnp.open(fs::path(f.path).filename().string(), raw_idx_global);
    } catch (const std::exception& e) {
      cerr << "ERROR(px): open failed: " << f.path << " : " << e.what() << "\n";
      continue;
    }
    auto md = reader->metadata();
    auto schema = md->schema();
    int ts_i  = find_col_idx(schema, "ts");
    int bp_i  = find_col_idx(schema, "bid_px");
    int ap_i  = find_col_idx(schema, "ask_px");
    if (ts_i<0 || bp_i<0 || ap_i<0) { cerr << "ERROR(px): missing ts/bid_px/ask_px in " << f.path << "\n"; continue; }

    vector<int64_t> v_ts, v_bp, v_ap;

    for (int rg=0; rg<md->num_row_groups(); ++rg) {
      auto rg_reader = reader->RowGroup(rg);
      read_i64_column(*rg_reader, ts_i, v_ts);
      read_i64_column(*rg_reader, bp_i, v_bp);
      read_i64_column(*rg_reader, ap_i, v_ap);

      size_t n = v_ts.size();
      if (!gap_mode) {
        uint64_t raw_idx_local = 0;
        for (size_t i=0;i<n;++i) {
          int64_t t = v_ts[i];
          if (t < start_ns || t >= end_ns) continue;
          ++raw_idx_local; ++raw_idx_global;
          if (raw_idx_local % seen_every != 0) continue;

          string content = render_px_row_content(t, v_ap[i], v_bp[i], print_ts, need_ask, need_bid, pcfg);
          ++printed_idx;
          optional<uint64_t> idx_print = (pcfg.idx_mode==IdxMode::Printed? optional<uint64_t>(printed_idx) :
                                          pcfg.idx_mode==IdxMode::Raw? optional<uint64_t>(raw_idx_global) : nullopt);
          print_prefix(cout, pcfg, nullptr, nullptr, idx_print, nullopt);
          cout << content << '\n';
        }
      } else {
        // GAP MODE (lazy string building) + FIRST print
        for (size_t i=0;i<n;++i) {
          int64_t t = v_ts[i];
          if (t < start_ns || t >= end_ns) continue;
          ++raw_idx_global;

          if (!have_prev) {
            // print FIRST edge: first;first;0
            string first_line = render_px_row_content(t, v_ap[i], v_bp[i],
                                                      print_ts, need_ask, need_bid, pcfg);
            ++printed_idx;
            optional<uint64_t> printed_idx_opt = (pcfg.idx_mode==IdxMode::Printed)? optional<uint64_t>(printed_idx) : nullopt;
            optional<uint64_t> raw_same = (pcfg.idx_mode==IdxMode::Raw)? optional<uint64_t>(raw_idx_global) : nullopt;
            print_prefix(cout, pcfg, nullptr, nullptr,
                         printed_idx_opt.has_value()? printed_idx_opt : raw_same,
                         raw_same);
            cout << first_line << ';' << first_line << ';';
            print_gap_s_ms(cout, 0);
            cout << '\n';

            have_prev = true;
            prev_ts = t;
            prev_i = i;
            prev_raw_idx = raw_idx_global;
            prev_str = std::move(first_line);
            prev_str_ready = true;
            continue;
          }

          if (t - prev_ts >= *pcfg.gap_ns) {
            if (!prev_str_ready) {
              prev_str = render_px_row_content(v_ts[prev_i], v_ap[prev_i], v_bp[prev_i],
                                               print_ts, need_ask, need_bid, pcfg);
              prev_str_ready = true;
            }
            string cur = render_px_row_content(t, v_ap[i], v_bp[i], print_ts, need_ask, need_bid, pcfg);

            ++printed_idx;
            optional<uint64_t> printed_idx_opt = (pcfg.idx_mode==IdxMode::Printed)? optional<uint64_t>(printed_idx) : nullopt;
            optional<uint64_t> raw1 = (pcfg.idx_mode==IdxMode::Raw)? optional<uint64_t>(prev_raw_idx) : nullopt;
            optional<uint64_t> raw2 = (pcfg.idx_mode==IdxMode::Raw)? optional<uint64_t>(raw_idx_global) : nullopt;
            print_prefix(cout, pcfg, nullptr, nullptr,
                         printed_idx_opt.has_value()? printed_idx_opt : raw1, raw2);
            cout << prev_str << ';' << cur << ';';
            print_gap_s_ms(cout, t - prev_ts);
            cout << '\n';
          }

          // Move window to current row (do NOT render yet)
          prev_ts = t;
          prev_i = i;
          prev_raw_idx = raw_idx_global;
          prev_str_ready = false;
        }

        // End of RG: materialize prev string to carry across RG boundary
        if (have_prev && !prev_str_ready) {
          prev_str = render_px_row_content(v_ts[prev_i], v_ap[prev_i], v_bp[prev_i],
                                           print_ts, need_ask, need_bid, pcfg);
          prev_str_ready = true;
        }
      }
    }
  }

  // GAP MODE: print LAST edge if we saw anything
  if (gap_mode && have_prev) {
    ++printed_idx;
    optional<uint64_t> printed_idx_opt = (pcfg.idx_mode==IdxMode::Printed)? optional<uint64_t>(printed_idx) : nullopt;
    optional<uint64_t> raw_same = (pcfg.idx_mode==IdxMode::Raw)? optional<uint64_t>(prev_raw_idx) : nullopt;
    print_prefix(cout, pcfg, nullptr, nullptr,
                 printed_idx_opt.has_value()? printed_idx_opt : raw_same, raw_same);
    cout << prev_str << ';' << prev_str << ';';
    print_gap_s_ms(cout, 0);
    cout << '\n';
  }

  fnp.finish(raw_idx_global);
  return 0;
}

// ========================== main ==========================

int main(int argc, char** argv)
{
  if (argc < 4) {
    cerr << "Usage: " << argv[0]
         << " <root> <symb> <type: top|trade|depth or top_fut|top_spot|trade_fut|trade_spot|depth_fut|depth_spot>\n"
         << "        [--sampling=px|100ms|1s|60s] [--start=SEC] [--end=SEC]\n"
         << "        [--gap=SEC]                (default: off; in gap mode a 'gap' column with seconds.mmm is appended)\n"
         << "        [--header]                 (default: off)\n"
         << "        [--raw-ts]                 (default: human ISO with .mmm; raw = nanoseconds)\n"
         << "        [--raw]                    (default: px/qty doubles scaled 1e8)\n"
         << "        [--precision-px=N]         (default: 8)\n"
         << "        [--precision-qty=N]        (default: 8)\n"
         << "        [--print-fn]               (stderr: file switch + raw idx + M rec/s)\n"
         << "        [--prefetch]               (Linux: readahead next file)\n"
         << "        [--idx=printed|raw|none]   (default: none)\n"
         << "        [--seen_every=N]           (default: 1)\n"
         << "        [--debug] [columns_csv]\n"
         << "Defaults:\n"
         << "  window [2023-01-01, 2036-01-01)\n"
         << "  ts: human ISO with milliseconds (use --raw-ts for raw nanoseconds)\n"
         << "  px/qty: doubles scaled by 1e8, fixed, precision as above (use --raw for int64)\n";
    return 1;
  }

  const string root = argv[1];
  const string symb = argv[2];
  const string type_in = argv[3];

  ParsedType T;
  try { T = parse_type(type_in); }
  catch (const exception& e) { cerr << "ERROR: " << e.what() << "\n"; return 1; }

  // defaults: 2023-01-01 .. 2036-01-01
  double start_sec = 1672531200.0;
  double end_sec   = 2082758400.0;
  bool start_set=false, end_set=false;

  optional<string> sampling;
  bool debug=false;
  bool prefetch=false;
  uint64_t seen_every = 1;
  string columns_csv;

  PrintCfg pcfg;

  for (int i=4; i<argc; ++i) {
    string a = argv[i];
    if (a.rfind("--sampling=",0)==0) {
      sampling = a.substr(11);
      if (!(*sampling=="px" || *sampling=="100ms" || *sampling=="1s" || *sampling=="60s")) {
        cerr << "ERROR: --sampling must be px, 100ms, 1s, or 60s\n"; return 1;
      }
    } else if (a.rfind("--start=",0)==0) {
      start_sec = stod(a.substr(8)); start_set = true;
    } else if (a.rfind("--end=",0)==0) {
      end_sec = stod(a.substr(6)); end_set = true;
    } else if (a.rfind("--gap=",0)==0) {
      double g = stod(a.substr(6));
      if (g < 0) { cerr << "ERROR: --gap must be >= 0\n"; return 1; }
      pcfg.gap_ns = to_ns(g);
    } else if (a=="--header") {
      pcfg.header = true;
    } else if (a=="--raw-ts") {
      pcfg.ts_fmt = TsFormat::RawNs;
    } else if (a=="--raw") {
      pcfg.raw_override = true;
    } else if (a.rfind("--precision-px=",0)==0) {
      int p = stoi(a.substr(15)); if (p < 0) p = 0; pcfg.precision_px = p;
    } else if (a.rfind("--precision-qty=",0)==0) {
      int p = stoi(a.substr(16)); if (p < 0) p = 0; pcfg.precision_qty = p;
    } else if (a=="--print-fn") {
      pcfg.print_fn = true;
    } else if (a=="--prefetch") {
      prefetch = true;
    } else if (a.rfind("--idx=",0)==0) {
      string v = a.substr(6);
      if (v=="printed") pcfg.idx_mode = IdxMode::Printed;
      else if (v=="raw") pcfg.idx_mode = IdxMode::Raw;
      else if (v=="none") pcfg.idx_mode = IdxMode::None;
      else { cerr << "ERROR: --idx must be printed|raw|none\n"; return 1; }
    } else if (a=="--debug") {
      debug = true;
    } else if (a.rfind("--seen_every=",0)==0 || a.rfind("--seen-every=",0)==0
            || a.rfind("--print-every=",0)==0 || a.rfind("--every=",0)==0) {
      size_t pos = a.find('=');
      if (pos==string::npos || pos+1>=a.size()) { cerr << "ERROR: " << a << " needs N\n"; return 1; }
      unsigned long long v = 0;
      try { v = stoull(a.substr(pos+1)); } catch(...) { cerr << "ERROR: bad N for " << a << "\n"; return 1; }
      if (v==0) v=1;
      seen_every = static_cast<uint64_t>(v);
    } else if (is_number_like(a) && !start_set) {
      start_sec = stod(a); start_set = true;
    } else if (is_number_like(a) && !end_set) {
      end_sec = stod(a); end_set = true;
    } else {
      columns_csv = a;
    }
  }

  if (end_sec <= start_sec) { cerr << "ERROR: end <= start\n"; return 1; }

  const int64_t start_ns = to_ns(start_sec);
  const int64_t end_ns   = to_ns(end_sec);

  ShardedDB::set_debug(debug);
  ShardedDB::set_prefetch(prefetch);  // no-op if not implemented in your lib

  if (debug) {
    cerr << "[debug] root=" << root << " symb=" << symb << " type=" << T.base << "\n";
    cerr << "[debug] window: [" << iso_from_ns(start_ns) << " .. " << iso_from_ns(end_ns) << ")\n";
    if (sampling) cerr << "[debug] sampling=" << *sampling << "\n";
    if (T.market)  cerr << "[debug] market=" << *T.market << "\n";
    if (pcfg.gap_ns) cerr << "[debug] gap_ns=" << *pcfg.gap_ns << " ns\n";
    cerr << "[debug] ts_fmt=" << (pcfg.ts_fmt==TsFormat::Human?"human-ms":"raw-ns")
         << " pxqty=" << (pcfg.raw_override?"raw":"double")
         << " prec_px=" << pcfg.precision_px << " prec_qty=" << pcfg.precision_qty
         << " print_fn=" << (pcfg.print_fn?"yes":"no")
         << " prefetch=" << (prefetch?"yes":"no")
         << " idx=" << (pcfg.idx_mode==IdxMode::Printed?"printed":pcfg.idx_mode==IdxMode::Raw?"raw":"none")
         << " header=" << (pcfg.header?"yes":"no")
         << " seen_every=" << seen_every << "\n";
    std::string os = detect_os_pretty();
    if (!os.empty()) cerr << "[debug] os=" << os << "\n";
  }

  // Fast path: top + px sampling -> read from top_px_{market}/... directly
  if (T.base == "top" && sampling && *sampling == "px") {
    if (!T.market) { cerr << "ERROR: px sampling requires market-specific type: use top_spot or top_fut\n"; return 1; }
    TopSelect sel{}; if (!columns_csv.empty()) sel = make_top_select_from_csv(columns_csv);
    return dump_px_direct(root, symb, *T.market, start_ns, end_ns, seen_every, debug, sel, pcfg, prefetch);
  }

  // Otherwise delegate to ShardedDB (ticks, time-sampled tops, trades, depth)
  ShardedDB db(root, sampling);

  // ================= TOP =================
  if (T.base == "top")
  {
    TopSelect sel{};
    if (!columns_csv.empty()) sel = make_top_select_from_csv(columns_csv);
    bool have_ts_to_print = sel.ts;

    TopSelect sel_int = sel;
    if (pcfg.gap_ns && !sel_int.ts) sel_int.ts = true;

    auto rdr = db.get_top_cols(start_ns, end_ns, symb, T.market, sel_int);

    if (pcfg.header) {
      vector<string> names;
      if (pcfg.idx_mode != IdxMode::None) names.push_back("idx");
      auto add_cols = [&](const string& prefix){
        if (sel.ts && have_ts_to_print) names.push_back(prefix + "ts");
        if (sel.ask_px) names.push_back(prefix + "ask_px");
        if (sel.ask_qty) names.push_back(prefix + "ask_qty");
        if (sel.bid_px) names.push_back(prefix + "bid_px");
        if (sel.bid_qty) names.push_back(prefix + "bid_qty");
        if (sel.valu)    names.push_back(prefix + "valu");
        if (sel.min_bid_px) names.push_back(prefix + "min_bid_px");
        if (sel.max_bid_px) names.push_back(prefix + "max_bid_px");
        if (sel.min_ask_px) names.push_back(prefix + "min_ask_px");
        if (sel.max_ask_px) names.push_back(prefix + "max_ask_px");
        if (sel.min_bid_ts) names.push_back(prefix + "min_bid_ts");
        if (sel.max_bid_ts) names.push_back(prefix + "max_bid_ts");
        if (sel.min_ask_ts) names.push_back(prefix + "min_ask_ts");
        if (sel.max_ask_ts) names.push_back(prefix + "max_ask_ts");
      };
      if (pcfg.gap_ns) { add_cols("first_"); add_cols("last_"); names.push_back("gap"); }
      else { add_cols(""); }
      cout << header_from_names(names) << '\n';
    }

    TopColsView v{};
    uint64_t printed_idx = 0;
    uint64_t raw_idx = 0;
    bool gap_mode = pcfg.gap_ns.has_value();

    FnPrinter fnp; fnp.enabled = pcfg.print_fn;

    auto render_line = [&](size_t i)->string{
      ostringstream os;
      bool first = true;
      auto put_sep=[&]{ if(!first) os<<';'; first=false; };

      if (v.ts && have_ts_to_print) { put_sep(); print_ts_fmt(os, v.ts[i], pcfg.ts_fmt); }
      if (v.ask_px){ put_sep(); print_px_val(os, v.ask_px[i], pcfg); }
      if (v.ask_qty){ put_sep(); print_qty_val(os, v.ask_qty[i], pcfg); }
      if (v.bid_px){ put_sep(); print_px_val(os, v.bid_px[i], pcfg); }
      if (v.bid_qty){ put_sep(); print_qty_val(os, v.bid_qty[i], pcfg); }
      if (v.valu){ put_sep(); os<<v.valu[i]; }

      if (v.min_bid_px){ put_sep(); print_px_val(os, v.min_bid_px[i], pcfg); }
      if (v.max_bid_px){ put_sep(); print_px_val(os, v.max_bid_px[i], pcfg); }
      if (v.min_ask_px){ put_sep(); print_px_val(os, v.min_ask_px[i], pcfg); }
      if (v.max_ask_px){ put_sep(); print_px_val(os, v.max_ask_px[i], pcfg); }

      if (v.min_bid_ts){ put_sep(); os<<v.min_bid_ts[i]; }
      if (v.max_bid_ts){ put_sep(); os<<v.max_bid_ts[i]; }
      if (v.min_ask_ts){ put_sep(); os<<v.min_ask_ts[i]; }
      if (v.max_ask_ts){ put_sep(); os<<v.max_ask_ts[i]; }
      return os.str();
    };

    if (!gap_mode) {
      uint64_t local_seen = 0;
      while (rdr->next(v)) {
        if (pcfg.print_fn) {
          string fn = v.file ? v.file : string("-");
          fnp.open(fn, raw_idx);
        }
        for (size_t i=0;i<v.n;++i) {
          ++raw_idx;
          ++local_seen;
          if (local_seen % seen_every != 0) continue;
          string line = render_line(i);
          ++printed_idx;
          optional<uint64_t> idx_print = (pcfg.idx_mode==IdxMode::Printed? optional<uint64_t>(printed_idx) :
                                          pcfg.idx_mode==IdxMode::Raw? optional<uint64_t>(raw_idx) : nullopt);
          print_prefix(cout, pcfg, nullptr, nullptr, idx_print, nullopt);
          cout << line << '\n';
        }
      }
      fnp.finish(raw_idx);
    } else {
      // lazy strings in gap mode + FIRST/LAST edges
      bool have_prev = false;
      int64_t prev_ts = 0;
      uint64_t prev_raw_idx = 0;
      size_t prev_i = 0;
      bool prev_str_ready = false;
      string prev_str;

      while (rdr->next(v)) {
        if (pcfg.print_fn) {
          string fn = v.file ? v.file : string("-");
          fnp.open(fn, raw_idx);
        }
        for (size_t i=0;i<v.n;++i) {
          ++raw_idx;
          int64_t t = v.ts ? v.ts[i] : 0;

          if (!have_prev) {
            // FIRST edge
            string first_line = render_line(i);
            ++printed_idx;
            optional<uint64_t> printed_idx_opt = (pcfg.idx_mode==IdxMode::Printed)? optional<uint64_t>(printed_idx) : nullopt;
            optional<uint64_t> raw_same = (pcfg.idx_mode==IdxMode::Raw)? optional<uint64_t>(raw_idx) : nullopt;
            print_prefix(cout, pcfg, nullptr, nullptr,
                         printed_idx_opt.has_value()? printed_idx_opt : raw_same, raw_same);
            cout << first_line << ';' << first_line << ';';
            print_gap_s_ms(cout, 0);
            cout << '\n';

            have_prev = true; prev_ts = t; prev_i = i; prev_raw_idx = raw_idx;
            prev_str = std::move(first_line); prev_str_ready = true;
            continue;
          }

          if (t - prev_ts >= *pcfg.gap_ns) {
            if (!prev_str_ready) { prev_str = render_line(prev_i); prev_str_ready = true; }
            string cur = render_line(i);
            ++printed_idx;
            optional<uint64_t> printed_idx_opt = (pcfg.idx_mode==IdxMode::Printed)? optional<uint64_t>(printed_idx) : nullopt;
            optional<uint64_t> raw1 = (pcfg.idx_mode==IdxMode::Raw)? optional<uint64_t>(prev_raw_idx) : nullopt;
            optional<uint64_t> raw2 = (pcfg.idx_mode==IdxMode::Raw)? optional<uint64_t>(raw_idx) : nullopt;
            print_prefix(cout, pcfg, nullptr, nullptr,
                         printed_idx_opt.has_value()? printed_idx_opt : raw1, raw2);
            cout << prev_str << ';' << cur << ';';
            print_gap_s_ms(cout, t - prev_ts);
            cout << '\n';
          }

          // slide window
          prev_ts = t;
          prev_i = i;
          prev_raw_idx = raw_idx;
          prev_str_ready = false;
        }
        // materialize prev for cross-block carry
        if (have_prev && !prev_str_ready) {
          prev_str = render_line(prev_i);
          prev_str_ready = true;
        }
      }

      // LAST edge
      if (have_prev) {
        ++printed_idx;
        optional<uint64_t> printed_idx_opt = (pcfg.idx_mode==IdxMode::Printed)? optional<uint64_t>(printed_idx) : nullopt;
        optional<uint64_t> raw_same = (pcfg.idx_mode==IdxMode::Raw)? optional<uint64_t>(prev_raw_idx) : nullopt;
        print_prefix(cout, pcfg, nullptr, nullptr,
                     printed_idx_opt.has_value()? printed_idx_opt : raw_same, raw_same);
        cout << prev_str << ';' << prev_str << ';';
        print_gap_s_ms(cout, 0);
        cout << '\n';
      }

      fnp.finish(raw_idx);
    }
  }
  // ================= TRADE =================
  else if (T.base == "trade")
  {
    TradeSelect sel{};
    if (!columns_csv.empty()) sel = make_trade_select_from_csv(columns_csv);

    TradeSelect sel_int = sel;
    if (pcfg.gap_ns && !sel_int.ts) sel_int.ts = true;

    auto rdr = db.get_trade_cols(start_ns, end_ns, symb, T.market, sel_int);

    if (pcfg.header) {
      vector<string> names;
      if (pcfg.idx_mode != IdxMode::None) names.push_back("idx");
      auto add_cols = [&](const string& prefix){
        if (sel.ts) names.push_back(prefix + "ts");
        if (sel.px) names.push_back(prefix + "px");
        if (sel.qty) names.push_back(prefix + "qty");
        if (sel.tradeId) names.push_back(prefix + "tradeId");
        if (sel.buyerOrderId) names.push_back(prefix + "buyerOrderId");
        if (sel.sellerOrderId) names.push_back(prefix + "sellerOrderId");
        if (sel.tradeTime) names.push_back(prefix + "tradeTime");
        if (sel.isMarket) names.push_back(prefix + "isMarket");
        if (sel.eventTime) names.push_back(prefix + "eventTime");
      };
      if (pcfg.gap_ns) { add_cols("first_"); add_cols("last_"); names.push_back("gap"); }
      else { add_cols(""); }
      cout << header_from_names(names) << '\n';
    }

    TradeColsView v{};
    uint64_t printed_idx = 0;
    uint64_t raw_idx = 0;
    bool gap_mode = pcfg.gap_ns.has_value();

    FnPrinter fnp; fnp.enabled = pcfg.print_fn;

    auto render_line = [&](size_t i)->string{
      ostringstream os;
      bool first=true;
      auto put_sep=[&]{ if(!first) os<<';'; first=false; };
      if (v.ts && sel.ts)      { put_sep(); print_ts_fmt(os, v.ts[i], pcfg.ts_fmt); }
      if (v.px)                { put_sep(); print_px_val(os, v.px[i], pcfg); }
      if (v.qty)               { put_sep(); print_qty_val(os, v.qty[i], pcfg); }
      if (v.tradeId)           { put_sep(); os<<v.tradeId[i]; }
      if (v.buyerOrderId)      { put_sep(); os<<v.buyerOrderId[i]; }
      if (v.sellerOrderId)     { put_sep(); os<<v.sellerOrderId[i]; }
      if (v.tradeTime)         { put_sep(); os<<v.tradeTime[i]; }
      if (v.isMarket)          { put_sep(); os<<int(v.isMarket[i]); }
      if (v.eventTime)         { put_sep(); os<<v.eventTime[i]; }
      return os.str();
    };

    if (!gap_mode) {
      uint64_t local_seen = 0;
      while (rdr->next(v)) {
        if (pcfg.print_fn) {
          string fn = v.file ? v.file : string("-");
          fnp.open(fn, raw_idx);
        }
        for (size_t i=0;i<v.n;++i) {
          ++raw_idx;
          ++local_seen;
          if (local_seen % seen_every != 0) continue;
          string line = render_line(i);
          ++printed_idx;
          optional<uint64_t> idx_print = (pcfg.idx_mode==IdxMode::Printed? optional<uint64_t>(printed_idx) :
                                          pcfg.idx_mode==IdxMode::Raw? optional<uint64_t>(raw_idx) : nullopt);
          print_prefix(cout, pcfg, nullptr, nullptr, idx_print, nullopt);
          cout << line << '\n';
        }
      }
      fnp.finish(raw_idx);
    } else {
      bool have_prev = false;
      int64_t prev_ts = 0;
      uint64_t prev_raw_idx = 0;
      size_t prev_i = 0;
      bool prev_str_ready = false;
      string prev_str;

      while (rdr->next(v)) {
        if (pcfg.print_fn) {
          string fn = v.file ? v.file : string("-");
          fnp.open(fn, raw_idx);
        }
        for (size_t i=0;i<v.n;++i) {
          ++raw_idx;
          int64_t t = v.ts ? v.ts[i] : 0;

          if (!have_prev) {
            // FIRST edge
            string first_line = render_line(i);
            ++printed_idx;
            optional<uint64_t> printed_idx_opt = (pcfg.idx_mode==IdxMode::Printed)? optional<uint64_t>(printed_idx) : nullopt;
            optional<uint64_t> raw_same = (pcfg.idx_mode==IdxMode::Raw)? optional<uint64_t>(raw_idx) : nullopt;
            print_prefix(cout, pcfg, nullptr, nullptr,
                         printed_idx_opt.has_value()? printed_idx_opt : raw_same, raw_same);
            cout << first_line << ';' << first_line << ';';
            print_gap_s_ms(cout, 0);
            cout << '\n';

            have_prev = true; prev_ts = t; prev_i = i; prev_raw_idx = raw_idx;
            prev_str = std::move(first_line); prev_str_ready = true;
            continue;
          }

          if (t - prev_ts >= *pcfg.gap_ns) {
            if (!prev_str_ready) { prev_str = render_line(prev_i); prev_str_ready = true; }
            string cur = render_line(i);
            ++printed_idx;
            optional<uint64_t> printed_idx_opt = (pcfg.idx_mode==IdxMode::Printed)? optional<uint64_t>(printed_idx) : nullopt;
            optional<uint64_t> raw1 = (pcfg.idx_mode==IdxMode::Raw)? optional<uint64_t>(prev_raw_idx) : nullopt;
            optional<uint64_t> raw2 = (pcfg.idx_mode==IdxMode::Raw)? optional<uint64_t>(raw_idx) : nullopt;
            print_prefix(cout, pcfg, nullptr, nullptr,
                         printed_idx_opt.has_value()? printed_idx_opt : raw1, raw2);
            cout << prev_str << ';' << cur << ';';
            print_gap_s_ms(cout, t - prev_ts);
            cout << '\n';
          }

          prev_ts = t;
          prev_i = i;
          prev_raw_idx = raw_idx;
          prev_str_ready = false;
        }
        if (have_prev && !prev_str_ready) { prev_str = render_line(prev_i); prev_str_ready = true; }
      }

      // LAST edge
      if (have_prev) {
        ++printed_idx;
        optional<uint64_t> printed_idx_opt = (pcfg.idx_mode==IdxMode::Printed)? optional<uint64_t>(printed_idx) : nullopt;
        optional<uint64_t> raw_same = (pcfg.idx_mode==IdxMode::Raw)? optional<uint64_t>(prev_raw_idx) : nullopt;
        print_prefix(cout, pcfg, nullptr, nullptr,
                     printed_idx_opt.has_value()? printed_idx_opt : raw_same, raw_same);
        cout << prev_str << ';' << prev_str << ';';
        print_gap_s_ms(cout, 0);
        cout << '\n';
      }

      fnp.finish(raw_idx);
    }
  }
  // ================= DEPTH =================
  else if (T.base == "depth")
  {
    DeltaSelect sel{};
    if (!columns_csv.empty()) sel = make_depth_select_from_csv(columns_csv);
    DeltaSelect sel_int = sel;
    if (pcfg.gap_ns && !sel_int.ts) sel_int.ts = true;

    auto rdr = db.get_depth_cols(start_ns, end_ns, symb, T.market, sel_int);

    if (pcfg.header) {
      vector<string> names;
      if (pcfg.idx_mode != IdxMode::None) names.push_back("idx");
      auto add_cols = [&](const string& prefix){
        names.push_back(prefix + "ts");
        names.push_back(prefix + "firstId");
        names.push_back(prefix + "lastId");
        names.push_back(prefix + "eventTime");
        names.push_back(prefix + "asks");
        names.push_back(prefix + "bids");
      };
      if (pcfg.gap_ns) { add_cols("first_"); add_cols("last_"); names.push_back("gap"); }
      else { add_cols(""); }
      cout << header_from_names(names) << '\n';
    }

    DeltaColsView v{};
    uint64_t printed_idx = 0;
    uint64_t raw_idx = 0;
    bool gap_mode = pcfg.gap_ns.has_value();

    FnPrinter fnp; fnp.enabled = pcfg.print_fn;

    auto render_line = [&](size_t i)->string{
      ostringstream os;
      bool first=true;
      auto putv=[&](const string& s){ if(!first) os<<';'; os<<s; first=false; };

      if (v.ts) { ostringstream t; print_ts_fmt(t, v.ts[i], pcfg.ts_fmt); putv(t.str()); } else putv("0");
      if (v.firstId) putv(std::to_string(v.firstId[i])); else putv("0");
      if (v.lastId)  putv(std::to_string(v.lastId[i]));  else putv("0");
      if (v.eventTime) putv(std::to_string(v.eventTime[i])); else putv("0");

      os << ';';
      if (v.ask_off && (v.ask_px || v.ask_qty)) {
        uint32_t a0=v.ask_off[i], a1=v.ask_off[i+1];
        for (uint32_t k=a0;k<a1;++k) {
          if (k>a0) os<<',';
          if (v.ask_px) { print_px_val(os, v.ask_px[k], pcfg); }
          if (v.ask_px && v.ask_qty) os<<'(';
          if (v.ask_qty) { print_qty_val(os, v.ask_qty[k], pcfg); }
          if (v.ask_px && v.ask_qty) os<<')';
        }
      }

      os << ';';
      if (v.bid_off && (v.bid_px || v.bid_qty)) {
        uint32_t b0=v.bid_off[i], b1=v.bid_off[i+1];
        for (uint32_t k=b0;k<b1;++k) {
          if (k>b0) os<<',';
          if (v.bid_px) { print_px_val(os, v.bid_px[k], pcfg); }
          if (v.bid_px && v.bid_qty) os<<'(';
          if (v.bid_qty) { print_qty_val(os, v.bid_qty[k], pcfg); }
          if (v.bid_px && v.bid_qty) os<<')';
        }
      }
      return os.str();
    };

    if (!gap_mode) {
      while (rdr->next(v)) {
        if (pcfg.print_fn) {
          string fn = v.file ? v.file : string("-");
          fnp.open(fn, raw_idx);
        }
        for (size_t i=0;i<v.n;++i) {
          ++raw_idx;
          ++printed_idx;
          optional<uint64_t> idx_print = (pcfg.idx_mode==IdxMode::Printed? optional<uint64_t>(printed_idx) :
                                          pcfg.idx_mode==IdxMode::Raw? optional<uint64_t>(raw_idx) : nullopt);
          print_prefix(cout, pcfg, nullptr, nullptr, idx_print, nullopt);
          cout << render_line(i) << '\n';
        }
      }
      fnp.finish(raw_idx);
    } else {
      bool have_prev = false;
      int64_t prev_ts = 0;
      uint64_t prev_raw_idx = 0;
      size_t prev_i = 0;
      bool prev_str_ready = false;
      string prev_str;

      while (rdr->next(v)) {
        if (pcfg.print_fn) {
          string fn = v.file ? v.file : string("-");
          fnp.open(fn, raw_idx);
        }
        for (size_t i=0;i<v.n;++i) {
          ++raw_idx;
          int64_t t = v.ts ? v.ts[i] : 0;

          if (!have_prev) {
            // FIRST edge
            string first_line = render_line(i);
            ++printed_idx;
            optional<uint64_t> printed_idx_opt = (pcfg.idx_mode==IdxMode::Printed)? optional<uint64_t>(printed_idx) : nullopt;
            optional<uint64_t> raw_same = (pcfg.idx_mode==IdxMode::Raw)? optional<uint64_t>(raw_idx) : nullopt;
            print_prefix(cout, pcfg, nullptr, nullptr,
                         printed_idx_opt.has_value()? printed_idx_opt : raw_same, raw_same);
            cout << first_line << ';' << first_line << ';';
            print_gap_s_ms(cout, 0);
            cout << '\n';

            have_prev = true; prev_ts = t; prev_i = i; prev_raw_idx = raw_idx;
            prev_str = std::move(first_line); prev_str_ready = true;
            continue;
          }

          if (t - prev_ts >= *pcfg.gap_ns) {
            if (!prev_str_ready) { prev_str = render_line(prev_i); prev_str_ready = true; }
            string cur = render_line(i);
            ++printed_idx;
            optional<uint64_t> printed_idx_opt = (pcfg.idx_mode==IdxMode::Printed)? optional<uint64_t>(printed_idx) : nullopt;
            optional<uint64_t> raw1 = (pcfg.idx_mode==IdxMode::Raw)? optional<uint64_t>(prev_raw_idx) : nullopt;
            optional<uint64_t> raw2 = (pcfg.idx_mode==IdxMode::Raw)? optional<uint64_t>(raw_idx) : nullopt;
            print_prefix(cout, pcfg, nullptr, nullptr,
                         printed_idx_opt.has_value()? printed_idx_opt : raw1, raw2);
            cout << prev_str << ';' << cur << ';';
            print_gap_s_ms(cout, t - prev_ts);
            cout << '\n';
          }

          prev_ts = t;
          prev_i = i;
          prev_raw_idx = raw_idx;
          prev_str_ready = false;
        }
        if (have_prev && !prev_str_ready) { prev_str = render_line(prev_i); prev_str_ready = true; }
      }

      // LAST edge
      if (have_prev) {
        ++printed_idx;
        optional<uint64_t> printed_idx_opt = (pcfg.idx_mode==IdxMode::Printed)? optional<uint64_t>(printed_idx) : nullopt;
        optional<uint64_t> raw_same = (pcfg.idx_mode==IdxMode::Raw)? optional<uint64_t>(prev_raw_idx) : nullopt;
        print_prefix(cout, pcfg, nullptr, nullptr,
                     printed_idx_opt.has_value()? printed_idx_opt : raw_same, raw_same);
        cout << prev_str << ';' << prev_str << ';';
        print_gap_s_ms(cout, 0);
        cout << '\n';
      }

      fnp.finish(raw_idx);
    }
  }
  else {
    cerr << "Internal error: unknown base type\n";
    return 2;
  }

  return 0;
}

