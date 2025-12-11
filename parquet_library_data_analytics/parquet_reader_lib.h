// parquet_reader_lib.h
// Public types + high-level DB interface (no Parquet headers exposed)

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

// ======== Zero-copy columnar views (valid until next() is called) ========

struct TopColsView
{
  const int64_t* ts       = nullptr;
  const int64_t* ask_px   = nullptr;
  const int64_t* ask_qty  = nullptr;
  const int64_t* bid_px   = nullptr;
  const int64_t* bid_qty  = nullptr;
  const int64_t* valu     = nullptr;

  // Sampled extra columns (present when reading sampled top_px files)
  const int64_t* min_bid_px = nullptr;
  const int64_t* max_bid_px = nullptr;
  const int64_t* min_ask_px = nullptr;
  const int64_t* max_ask_px = nullptr;
  const int64_t* min_bid_ts = nullptr;
  const int64_t* max_bid_ts = nullptr;
  const int64_t* min_ask_ts = nullptr;
  const int64_t* max_ask_ts = nullptr;

  const char*    file    = nullptr; // basename of current file (valid until next())
  size_t n = 0;
};

struct DeltaColsView
{
  const int64_t* ts        = nullptr;
  const int64_t* firstId   = nullptr;
  const int64_t* lastId    = nullptr;
  const int64_t* eventTime = nullptr;

  const uint32_t* ask_off = nullptr;
  const int64_t*  ask_px  = nullptr;
  const int64_t*  ask_qty = nullptr;

  const uint32_t* bid_off = nullptr;
  const int64_t*  bid_px  = nullptr;
  const int64_t*  bid_qty = nullptr;

  const char*     file    = nullptr; // basename of current file (valid until next())
  size_t n = 0;
};

struct TradeColsView
{
  const int64_t* ts            = nullptr;
  const int64_t* px            = nullptr;
  const int64_t* qty           = nullptr;
  const int64_t* tradeId       = nullptr;
  const int64_t* buyerOrderId  = nullptr;
  const int64_t* sellerOrderId = nullptr;
  const int64_t* tradeTime     = nullptr;
  const uint8_t* isMarket      = nullptr;  // 0/1
  const int64_t* eventTime     = nullptr;

  const char*    file          = nullptr; // basename of current file (valid until next())
  size_t n = 0;
};

// ======== Column selection ========

struct TopSelect
{
  bool ts      = true;
  bool ask_px  = true;
  bool ask_qty = true;
  bool bid_px  = true;
  bool bid_qty = true;
  bool valu    = true;

  // Sampled columns
  bool min_bid_px = false;
  bool max_bid_px = false;
  bool min_ask_px = false;
  bool max_ask_px = false;
  bool min_bid_ts = false;
  bool max_bid_ts = false;
  bool min_ask_ts = false;
  bool max_ask_ts = false;
};

struct DeltaSelect
{
  bool ts        = true;
  bool firstId   = true;
  bool lastId    = true;
  bool eventTime = true;

  bool ask_px  = true;
  bool ask_qty = true;
  bool bid_px  = true;
  bool bid_qty = true;
};

struct TradeSelect
{
  bool ts            = true;
  bool px            = true;
  bool qty           = true;
  bool tradeId       = true;
  bool buyerOrderId  = true;
  bool sellerOrderId = true;
  bool tradeTime     = true;
  bool isMarket      = true;
  bool eventTime     = true;
};

// ======== Public DB + columnar-batch readers ========

class ShardedDB
{
public:
  explicit ShardedDB(std::string root, std::optional<std::string> sampling = std::nullopt);
  ~ShardedDB();
  ShardedDB(const ShardedDB&) = delete;
  ShardedDB& operator=(const ShardedDB&) = delete;
  ShardedDB(ShardedDB&&) noexcept;
  ShardedDB& operator=(ShardedDB&&) noexcept;

  // Enable/disable verbose debug printing (file discovery + processing)
  static void set_debug(bool enabled);
  // Enable/disable Linux prefetch (posix_fadvise/readahead)
  static void set_prefetch(bool enabled);

  struct TopBatchReader
  {
    struct Impl;

    TopBatchReader(TopBatchReader&&) noexcept;
    TopBatchReader& operator=(TopBatchReader&&) noexcept;
    ~TopBatchReader();

    explicit TopBatchReader(std::unique_ptr<Impl> impl);

    bool next(TopColsView& out);

  private:
    std::unique_ptr<Impl> impl_;
    TopBatchReader(const TopBatchReader&) = delete;
    TopBatchReader& operator=(const TopBatchReader&) = delete;
  };

  struct TradeBatchReader
  {
    struct Impl;

    TradeBatchReader(TradeBatchReader&&) noexcept;
    TradeBatchReader& operator=(TradeBatchReader&&) noexcept;
    ~TradeBatchReader();

    explicit TradeBatchReader(std::unique_ptr<Impl> impl);

    bool next(TradeColsView& out);

  private:
    std::unique_ptr<Impl> impl_;
    TradeBatchReader(const TradeBatchReader&) = delete;
    TradeBatchReader& operator=(const TradeBatchReader&) = delete;
  };

  struct DeltaBatchReader
  {
    struct Impl;

    DeltaBatchReader(DeltaBatchReader&&) noexcept;
    DeltaBatchReader& operator=(DeltaBatchReader&&) noexcept;
    ~DeltaBatchReader();

    explicit DeltaBatchReader(std::unique_ptr<Impl> impl);

    bool next(DeltaColsView& out);

  private:
    std::unique_ptr<Impl> impl_;
    DeltaBatchReader(const DeltaBatchReader&) = delete;
    DeltaBatchReader& operator=(DeltaBatchReader&) = delete;
  };

  // New overloads (market-aware): market = "fut" | "spot"
  std::unique_ptr<TopBatchReader>   get_top_cols  (int64_t start_ns, int64_t end_ns, const std::string& symb, std::optional<std::string> market, TopSelect sel = {}) const;
  std::unique_ptr<TradeBatchReader> get_trade_cols(int64_t start_ns, int64_t end_ns, const std::string& symb, std::optional<std::string> market, TradeSelect sel = {}) const;
  std::unique_ptr<DeltaBatchReader> get_depth_cols(int64_t start_ns, int64_t end_ns, const std::string& symb, std::optional<std::string> market, DeltaSelect sel = {}) const;

  // Backward-compatible overloads (search both fut & spot)
  std::unique_ptr<TopBatchReader>   get_top_cols  (int64_t start_ns, int64_t end_ns, const std::string& symb, TopSelect sel = {}) const;
  std::unique_ptr<TradeBatchReader> get_trade_cols(int64_t start_ns, int64_t end_ns, const std::string& symb, TradeSelect sel = {}) const;
  std::unique_ptr<DeltaBatchReader> get_depth_cols(int64_t start_ns, int64_t end_ns, const std::string& symb, DeltaSelect sel = {}) const;

private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

