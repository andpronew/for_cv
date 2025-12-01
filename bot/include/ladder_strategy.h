#pragma once

#include <string>
#include <fstream>
#include <deque>
#include <map>
#include <mutex>
#include <unordered_map>
#include <nlohmann/json.hpp>
#include <vector>

using std::string;

class BinanceClient; // forward

// small helper to represent a filled BUY entry (for FIFO pairing)
struct BuyEntry {
    long long order_id = 0;
    string clientOrderId;
    double remaining_qty = 0.0;   // amount still available to match (initially executedQty)
    double avg_price = 0.0;       // average price paid (quote/base units consistent with API)
    double commission_quote = 0.0; // commission in quote currency (e.g. FDUSD)
    long long timestamp_ms = 0;
};

class LadderStrategy
{
public:
    // Constructor signature required by bot.cpp
    LadderStrategy(BinanceClient &client,
                   const string &symbol,
                   int ladder_size,
                   double ladder_step,
                   double order_size,
                   double initial_capital_usdt,
                   int order_timeout_sec);

    // destructor (must be defined to avoid link errors)
    ~LadderStrategy();

    // Start the strategy (blocking)
    void run();

    // visible for tests / debug
    double get_available_capital_usdt() const;

private:
    BinanceClient &client_;
    string symbol_;
    int ladder_size_;
    double ladder_step_;
    double order_size_;

    // runtime state
    std::ofstream order_log_file_;            // append-only logs/orders.txt
    mutable std::mutex mtx_;                  // protects tracked orders & buy_queue_ & capital state

    // map to track orders by orderId, storing full JSON for each
    std::map<long long, nlohmann::json> tracked_orders_;

    // FIFO queue of filled BUY entries to pair against SELLs
    std::deque<BuyEntry> buy_queue_;

    // accumulated realised profit in quote currency
    double profit_accum_;

    // capital management
    double capital_usdt_;            // free capital available (not including reserved)
    double reserved_capital_usdt_;   // total currently reserved for pending BUY orders
    double btc_balance_;             // total BTC we believe we hold (from fills)

    // config / runtime params
    int order_timeout_sec_;          // seconds after which a tracked order is considered stale locally
    bool prevent_loss_sells_;        // if true, avoid placing SELLs that lead to expected negative profit
    double min_profit_quote_;        // minimal acceptable expected profit in quote currency
    double min_price_buffer_usdt_;   // minimal buffer vs bestBid when placing SELL to avoid immediate taker
    int order_check_interval_sec_; 
        // folder to store orders.txt (and other logs) — relative to project root
    std::string logs_folder_;
// main loop sleep seconds

    // local reservation bookkeeping (reserve before place_order -> map local_id / orderId -> reserved amount)
    long long next_local_reserve_id_;
    std::unordered_map<long long, double> reserved_local_usdt_;   // local_reserve_id -> amount
    std::unordered_map<long long, double> reserved_by_order_usdt_; // orderId -> amount (after attach)

    // ---------------- methods ----------------

    // format ms -> ISO string (helper)
    static std::string ms_to_iso(long long ms_since_epoch);
    // sum commission from order JSON (const)
    double sum_commission(const nlohmann::json &order) const;

    // place ladder orders; pairs == -1 -> place full ladder_size_ pairs,
    // otherwise place 'pairs' pairs (each pair contains 1 buy + 1 sell candidate)
    void place_ladder_orders(double mid_price, int pairs = -1);

    // poll current open orders and update tracked_orders_, detect fills,
    // log created & filled orders to orders.txt, update buy_queue_, compute realised profit
    void poll_open_orders();

    // write single order (as JSON) to orders.txt (append)
    void log_order_to_file(const nlohmann::json &order);

    // estimate expected profit if we sell 'qty' at 'sell_price', using FIFO buy_queue_
    double expected_profit_if_sell_at(double sell_price, double qty) const;

    // helper to process a filled order (called when poll_open_orders detects FILLED)
    void process_filled_order(const nlohmann::json &order_json);

    // ---------------- reservation helpers ----------------
    double get_available_capital_for_tests() const; // internal alias

    bool reserve_capital_for_order(long long &out_local_reserve_id, double amount);
    void attach_reservation_to_order(long long local_reserve_id, long long order_id);
    void release_reservation_for_order(long long order_id, double used_usdt, bool return_residual_to_capital);
    void rollback_local_reservation(long long local_reserve_id);

    // extract executed info helper (avg_price, executed_qty, commission_quote)
    void extract_exec_info(const nlohmann::json &j, double &avg_price, double &executed_qty, double &commission_quote) const;

    // helper to write sorted snapshot (optional)
    void write_sorted_snapshot();

    // временные локальные резервы (пока ордер не подтверждён сервером)
std::unordered_map<long long, double> temp_local_reservations_; // local_reserve_id -> reserved_usdt

// резервы, привязанные уже к реальным orderId на бирже
std::unordered_map<long long, double> order_reservations_; // orderId -> reserved_usdt

// список незакрытых (не-сыгранных) buy-ордров для локального сопоставления (если нужен)
std::vector<nlohmann::json> unmatched_buys_;



// Если в header sum_commission объявлена const — оставьте как есть:
// double sum_commission(const nlohmann::json &order) const;
};

