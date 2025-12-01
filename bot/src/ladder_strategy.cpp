// src/ladder_strategy.cpp
#include "ladder_strategy.h"
#include "binance_client.hpp"
#include "logging.h"

#include <nlohmann/json.hpp>

#include <chrono>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <fstream>
#include <thread>
#include <mutex>
#include <map>
#include <unordered_map>
#include <unordered_set>
#include <deque>
#include <vector>
#include <cmath>
#include <stdexcept>
#include <filesystem>

using json = nlohmann::json;
using namespace std;

// ------------------------ Вспомогательные ------------------------

// Конвертация миллисекунд с эпохи -> ISO 8601 string (member)
string LadderStrategy::ms_to_iso(long long ms_since_epoch)
{
    using namespace std::chrono;
    milliseconds ms(ms_since_epoch);
    system_clock::time_point tp(ms);
    std::time_t t = system_clock::to_time_t(tp);
    std::tm tm{};
#if defined(_WIN32) || defined(_WIN64)
    gmtime_s(&tm, &t);
#else
    gmtime_r(&t, &tm);
#endif
    auto rem_ms = ms.count() % 1000;
    std::ostringstream oss;
    oss << std::put_time(&tm, "%Y-%m-%d %H:%M:%S") << '.' << std::setw(3) << std::setfill('0') << rem_ms << 'Z';
    return oss.str();
}

// ------------------------ Конструктор / Деструктор ------------------------

// Вставьте / замените существующий определение конструктора этим блоком

LadderStrategy::LadderStrategy(BinanceClient &client,
                               const std::string &symbol,
                               int ladder_size,
                               double ladder_step,
                               double order_size,
                               double initial_capital_usdt, // совпадает с header
                               int order_timeout_sec)       // совпадает с header
    : client_(client),
      symbol_(symbol),
      ladder_size_(ladder_size),
      ladder_step_(ladder_step),
      order_size_(order_size),
      /* order_log_file_ (opened in body) */
      profit_accum_(0.0),
      capital_usdt_(initial_capital_usdt),
      reserved_capital_usdt_(0.0),
      btc_balance_(0.0),
      order_timeout_sec_(order_timeout_sec),
      prevent_loss_sells_(true),
      min_profit_quote_(0.0),
      min_price_buffer_usdt_(0.0),
      order_check_interval_sec_(1),
      next_local_reserve_id_(-1),
      reserved_local_usdt_(),
      reserved_by_order_usdt_()
{
    // Попытка гарантировать, что папка logs существует.
    try {
        std::filesystem::create_directories("logs"); // безопасно, если уже есть
    } catch (const std::exception &e) {
        log_message(std::string("[LadderStrategy] warning: cannot create logs dir: ") + e.what());
        // продолжаем — попытаемся открыть файл (возможно папка уже есть или ошибка будет при открытии)
    }

    // Откроем файл orders.txt в папке logs (append)
    try {
        std::string path = std::string("logs") + "/orders.txt";
        order_log_file_.open(path, std::ios::app);
        if (!order_log_file_.is_open()) {
            log_message(std::string("[LadderStrategy] failed to open orders file: ") + path);
        } else {
            log_message(std::string("[LadderStrategy] opened orders file: ") + path);
            // Попытаемся записать заголовок только если файл пуст (без жёсткой проверки — безопасно добавит повторно заголовок)
            order_log_file_ << "timestamp | SYMBOL | SIDE | PRICE | QTY | executedQty | ORDERID | STATUS | commission | capital_usdt | btc_balance | profit_accum\n";
            order_log_file_.flush();
        }
    } catch (const std::exception &e) {
        log_message(std::string("[LadderStrategy] exception opening orders file: ") + e.what());
    }

    // Лог создания стратегии — корректно форматируем order_size_
    {
        std::ostringstream oss;
        oss << std::fixed << std::setprecision(8) << order_size_;
        log_message(std::string("[LadderStrategy] LadderStrategy created with order_size = ") + oss.str());
    }

    // Здесь можно инициализировать дополнительные структуры / восстановить состояние из файлов,
    // но мы сохраняем все ваши текущие наработки — ничего не удаляем.
}


LadderStrategy::~LadderStrategy()
{
    log_message("LadderStrategy destroyed");
}

// ------------------------ комиссии / утилиты ------------------------

double LadderStrategy::sum_commission(const nlohmann::json &order) const
{
    // Попытаться извлечь комиссию из order["fills"] или из поля "commission"
    try {
        double total = 0.0;
        if (order.contains("fills") && order["fills"].is_array()) {
            for (const auto &f : order["fills"]) {
                if (f.contains("commission")) {
                    // commission в quote asset
                    total += f.value("commission", 0.0);
                } else if (f.contains("commission")) {
                    total += f.value("commission", 0.0);
                }
            }
        }
        // fallback
        if (total == 0.0 && order.contains("commission")) {
            total = order.value("commission", 0.0);
        }
        return total;
    } catch (...) {
        return 0.0;
    }
}

// ------------------------ Резервирование капитала (локально) ------------------------

bool LadderStrategy::reserve_capital_for_order(long long &out_local_reserve_id, double amount)
{
    std::lock_guard<std::mutex> lg(mtx_);
    // amount - сумма в quote (USDT/FDUSD)
    if (amount <= 0.0) return false;
    if (capital_usdt_ + 1e-12 < amount) {
        // not enough free capital
        return false;
    }
    capital_usdt_ -= amount;
    reserved_capital_usdt_ += amount;
    long long id = next_local_reserve_id_--;
    temp_local_reservations_[id] = amount;
    out_local_reserve_id = id;

    std::ostringstream oss;
    oss << "[reserve_debug] reserved local_id=" << id << " amount=" << std::fixed << std::setprecision(8) << amount
        << " capital_now=" << capital_usdt_
        << " reserved_total=" << reserved_capital_usdt_;
    log_message(oss.str());
    return true;
}

void LadderStrategy::attach_reservation_to_order(long long local_reserve_id, long long order_id)
{
    std::lock_guard<std::mutex> lg(mtx_);
    auto it = temp_local_reservations_.find(local_reserve_id);
    if (it == temp_local_reservations_.end()) {
        std::ostringstream oss;
        oss << "[reserve_debug] attach failed: local_id=" << local_reserve_id << " not found";
        log_message(oss.str());
        return;
    }
    double amount = it->second;
    temp_local_reservations_.erase(it);
    order_reservations_[order_id] = amount;

    std::ostringstream oss;
    oss << "[reserve_debug] attach local_id=" << local_reserve_id << " -> orderId=" << order_id
        << " amount=" << std::fixed << std::setprecision(8) << amount;
    log_message(oss.str());
}

void LadderStrategy::release_reservation_for_order(long long order_id, double used_usdt, bool return_residual_to_capital)
{
    std::lock_guard<std::mutex> lg(mtx_);
    auto it = order_reservations_.find(order_id);
    if (it == order_reservations_.end()) {
        // nothing reserved
        return;
    }
    double reserved = it->second;
    double residual = reserved - used_usdt;
    if (residual < 0.0) residual = 0.0;
    reserved_capital_usdt_ -= reserved;
    if (return_residual_to_capital && residual > 0.0) {
        capital_usdt_ += residual;
    }
    order_reservations_.erase(it);

    std::ostringstream oss;
    oss << "[reserve_debug] release orderId=" << order_id
        << " reserved=" << std::fixed << std::setprecision(8) << reserved
        << " used=" << used_usdt
        << " residual=" << residual
        << " capital_now=" << capital_usdt_
        << " reserved_total=" << reserved_capital_usdt_;
    log_message(oss.str());
}

void LadderStrategy::rollback_local_reservation(long long local_reserve_id)
{
    std::lock_guard<std::mutex> lg(mtx_);
    auto it = temp_local_reservations_.find(local_reserve_id);
    if (it == temp_local_reservations_.end()) return;
    double amount = it->second;
    temp_local_reservations_.erase(it);
    reserved_capital_usdt_ -= amount;
    capital_usdt_ += amount;

    std::ostringstream oss;
    oss << "[reserve_debug] rollback local_id=" << local_reserve_id
        << " amount=" << std::fixed << std::setprecision(8) << amount
        << " capital_now=" << capital_usdt_
        << " reserved_total=" << reserved_capital_usdt_;
    log_message(oss.str());
}

// ------------------------ Размещение лестницы ордеров (BUY) ------------------------

void LadderStrategy::place_ladder_orders(double mid_price, int size)
{
    if (size <= 0) return;
    // place BUY ladder below mid_price, step = ladder_step_
    for (int i = 0; i < size; ++i) {
        double price = mid_price - (i+1) * ladder_step_;
        // compute required quote (USDT) to buy order_size_ at price
        double needed_quote = price * order_size_;
        long long local_reserve_id = 0;
        bool reserved = reserve_capital_for_order(local_reserve_id, needed_quote);
        if (!reserved) {
            // not enough capital, stop placing further buys
            log_message("[place_ladder_orders] Not enough capital to reserve for next BUY; stopping ladder placement.");
            return;
        }

        // Place LIMIT BUY (client will check maker condition)
        try {
            string resp = client_.place_order(symbol_, "BUY", "LIMIT", price, order_size_);
            // Extract order id if present
            try {
                auto j = json::parse(resp);
                if (j.contains("orderId")) {
                    long long id = j["orderId"].get<long long>();
                    attach_reservation_to_order(local_reserve_id, id);
                    log_order_response(resp);
                } else {
                    // no orderId -> rollback
                    rollback_local_reservation(local_reserve_id);
                    log_message(string("[place_ladder_orders] place_order returned without orderId: ") + resp);
                }
            } catch (const std::exception &e) {
                // parse fail -> rollback
                rollback_local_reservation(local_reserve_id);
                std::ostringstream oss;
                oss << "[place_ladder_orders] parse place_order response failed: " << e.what();
                log_message(oss.str());
            }
        } catch (const std::exception &e) {
            // API call failed -> rollback
            rollback_local_reservation(local_reserve_id);
            std::ostringstream oss;
            oss << "[place_ladder_orders] place_order exception: " << e.what();
            log_message(oss.str());
        }
    }
}

// ------------------------ Опрос открытых ордеров и логика ------------------------

void LadderStrategy::poll_open_orders()
{
    try {
        string resp = client_.get_open_orders(symbol_);
        auto parsed = json::parse(resp, nullptr, false);
        if (parsed.is_discarded()) {
            log_message("[poll_open_orders] parse error for open orders");
            return;
        }
        if (!parsed.is_array()) {
            log_message(string("[poll_open_orders] unexpected open orders response: ") + resp);
            return;
        }

        // collect open ids
        std::unordered_set<long long> open_ids;
        for (const auto &ord : parsed) {
            long long id = ord.value("orderId", 0LL);
            open_ids.insert(id);
        }

        // go through orders and log them
        for (const auto &order : parsed) {
            log_order_to_file(order);
        }

        // reconcile our order_reservations_: if some order_reservations_ keys are NOT in open_ids => release reservation
        {
            std::vector<long long> to_release;
            {
                std::lock_guard<std::mutex> lg(mtx_);
                for (const auto &kv : order_reservations_) {
                    long long orderId = kv.first;
                    if (open_ids.find(orderId) == open_ids.end()) {
                        to_release.push_back(orderId);
                    }
                }
            }
            for (long long oid : to_release) {
                // release returns residual to capital
                release_reservation_for_order(oid, 0.0, true);
            }
        }
    } catch (const std::exception &e) {
        std::ostringstream oss;
        oss << "[poll_open_orders] exception: " << e.what();
        log_message(oss.str());
    }
}

// ------------------------ Запись ордера в orders.txt ------------------------

void LadderStrategy::log_order_to_file(const nlohmann::json &order)
{
    try {
        // Extract useful fields
        std::string ts = "";
        if (order.contains("time")) {
            long long t = order.value("time", 0LL);
            ts = ms_to_iso(t);
        } else {
            // use system time
            auto now = std::chrono::system_clock::now();
            long long ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
            ts = ms_to_iso(ms);
        }

        string symbol = order.value("symbol", symbol_);
        string side = order.value("side", "");
        string price = order.value("price", string("0"));
        string origQty = order.value("origQty", string("0"));
        string executedQty = order.value("executedQty", string("0"));
        long long id = order.value("orderId", 0LL);
        string status = order.value("status", "");
        double commission = sum_commission(order);

        // Write line
        std::ofstream ofs(logs_folder_ + "/orders.txt", std::ios::app);
        if (!ofs.is_open()) {
            log_message("[log_order_to_file] failed to open orders.txt for append");
            return;
        }

        // lock to read capital/btc/profit snapshot
        double capital_snapshot = 0.0, btc_snapshot = 0.0, profit_snapshot = 0.0;
        {
            std::lock_guard<std::mutex> lg(mtx_);
            capital_snapshot = capital_usdt_;
            btc_snapshot = btc_balance_;
            profit_snapshot = profit_accum_;
        }
        ofs << ts << " | " << symbol << " | " << side << " | " << price << " | " << origQty
            << " | " << executedQty << " | " << id << " | " << status << " | "
            << std::fixed << std::setprecision(8) << commission << " | "
            << std::fixed << std::setprecision(8) << capital_snapshot << " | "
            << std::fixed << std::setprecision(8) << btc_snapshot << " | "
            << std::fixed << std::setprecision(8) << profit_snapshot << "\n";
        ofs.close();

        // keep track of unmatched buys (simple heuristic)
        if (side == "BUY" && status == "NEW") {
            std::lock_guard<std::mutex> lg(mtx_);
            unmatched_buys_.push_back(order);
            // trim unmatched buys to avoid unbounded growth
            while (unmatched_buys_.size() > static_cast<size_t>(ladder_size_ * 4)) {
                unmatched_buys_.erase(unmatched_buys_.begin());
            }
        }

    } catch (const std::exception &e) {
        std::ostringstream oss;
        oss << "[log_order_to_file] exception: " << e.what();
        log_message(oss.str());
    }
}

// ------------------------ Разбор заполнения (avg price, qty, commission) ------------------------

void LadderStrategy::extract_exec_info(const nlohmann::json &j, double &avg_price, double &executed_qty, double &commission_quote) const
{
    avg_price = 0.0;
    executed_qty = 0.0;
    commission_quote = 0.0;

    // If there are "fills" - compute weighted average
    if (j.contains("fills") && j["fills"].is_array()) {
        double tot = 0.0;
        double vol = 0.0;
        for (const auto &f : j["fills"]) {
            double fp = 0.0;
            double fq = 0.0;
            try {
                if (f.contains("price")) fp = stod(f.value("price", string("0")));
                if (f.contains("qty")) fq = stod(f.value("qty", string("0")));
            } catch (...) {
                // ignore parse errors
            }
            tot += fp * fq;
            vol += fq;
            commission_quote += f.value("commission", 0.0);
        }
        if (vol > 0.0) {
            avg_price = tot / vol;
            executed_qty = vol;
        }
        return;
    }

    // Else try direct fields
    try {
        if (j.contains("avgPrice")) {
            avg_price = stod(j.value("avgPrice", string("0")));
        }
    } catch (...) {}
    try {
        if (j.contains("executedQty")) {
            executed_qty = stod(j.value("executedQty", string("0")));
        }
    } catch (...) {}
    commission_quote = j.value("commission", 0.0);
}

// ------------------------ Обработка заполненного ордера ------------------------

void LadderStrategy::process_filled_order(const nlohmann::json &order_json)
{
    try {
        string side = order_json.value("side", "");
        double avg_price = 0.0, executed_qty = 0.0, commission_quote = 0.0;
        extract_exec_info(order_json, avg_price, executed_qty, commission_quote);

        long long orderId = order_json.value("orderId", 0LL);

        if (side == "BUY") {
            // Deduct reserved amount and add btc balance
            double used_quote = avg_price * executed_qty + commission_quote;
            release_reservation_for_order(orderId, used_quote, false); // do not return residual (we used used_quote from reserved)
            {
                std::lock_guard<std::mutex> lg(mtx_);
                btc_balance_ += executed_qty;
                // commission assumed in quote; profit_accum_ unchanged on buys
            }

            std::ostringstream oss;
            oss << "[process_filled_order] BUY filled: orderId=" << orderId
                << " qty=" << executed_qty << " avg=" << avg_price
                << " commission=" << commission_quote;
            log_message(oss.str());

            // After buy, optionally create a SELL take-profit order for that executed qty
            // But we avoid immediate sells into loss: check min_profit_quote_ and prevent_loss_sells_
            if (executed_qty > 0.0) {
                double target_sell_price = avg_price + min_price_buffer_usdt_; // basic buffer
                // If prevent_loss_sells_ true, ensure target_sell_price gives at least min_profit_quote_
                if (prevent_loss_sells_) {
                    if ((target_sell_price - avg_price) * executed_qty < min_profit_quote_) {
                        // If profit would be < min_profit_quote_, postpone placing sell
                        std::ostringstream oss2;
                        oss2 << "[process_filled_order] skipping immediate SELL: expected profit too low. buy_avg=" << avg_price;
                        log_message(oss2.str());
                    } else {
                        // Place SELL LIMIT above current bestBid to be maker
                        try {
                            auto [bestBid, bestAsk] = client_.get_book_ticker(symbol_);
                            if (target_sell_price <= bestBid + 1e-12) target_sell_price = bestBid + min_price_buffer_usdt_;
                            string resp = client_.place_order(symbol_, "SELL", "LIMIT", target_sell_price, executed_qty);
                            log_order_response(resp);
                        } catch (const std::exception &e) {
                            std::ostringstream oss3;
                            oss3 << "[process_filled_order] failed to place SELL: " << e.what();
                            log_message(oss3.str());
                        }
                    }
                } else {
                    // allow sell even if small profit
                    try {
                        auto [bestBid, bestAsk] = client_.get_book_ticker(symbol_);
                        if (target_sell_price <= bestBid + 1e-12) target_sell_price = bestBid + min_price_buffer_usdt_;
                        string resp = client_.place_order(symbol_, "SELL", "LIMIT", target_sell_price, executed_qty);
                        log_order_response(resp);
                    } catch (const std::exception &e) {
                        std::ostringstream oss3;
                        oss3 << "[process_filled_order] failed to place SELL: " << e.what();
                        log_message(oss3.str());
                    }
                }
            }
        }
        else if (side == "SELL") {
            // SELL filled: increase capital_usdt_ by proceeds minus commission, reduce btc_balance_
            double proceeds_quote = avg_price * executed_qty;
            double net = proceeds_quote - commission_quote;
            {
                std::lock_guard<std::mutex> lg(mtx_);
                capital_usdt_ += net;
                // subtract BTC
                btc_balance_ -= executed_qty;
                // compute realized profit in base units of BTC or quote? For simplicity accumulate executed_qty*(sell_avg - buy_avg) is not computed here.
                // In logs we attempt to write realized profit elsewhere when matching buy/sell pairs.
            }

            std::ostringstream oss;
            oss << "[process_filled_order] SELL filled: orderId=" << orderId
                << " qty=" << executed_qty << " avg=" << avg_price
                << " commission=" << commission_quote << " net=" << net;
            log_message(oss.str());
        }

    } catch (const std::exception &e) {
        std::ostringstream oss;
        oss << "[process_filled_order] exception: " << e.what();
        log_message(oss.str());
    }
}

// ------------------------ Основной цикл run ------------------------

void LadderStrategy::run()
{
    log_message("Starting ladder strategy...");
    log_message(string("LadderStrategy running for symbol: ") + symbol_);

    while (true) {
        try {
            double mid_price = client_.get_price(symbol_);
            // place ladder by default every iteration
            place_ladder_orders(mid_price, ladder_size_);
        } catch (const std::exception &e) {
            std::ostringstream oss;
            oss << "[LadderStrategy] run failed: " << e.what();
            log_message(oss.str());
        }

        // poll open orders and process fills
        try {
            string resp = client_.get_open_orders(symbol_);
            auto parsed = json::parse(resp, nullptr, false);
            if (!parsed.is_discarded() && parsed.is_array()) {
                for (const auto &order : parsed) {
                    // log each open order
                    log_order_to_file(order);
                    // If filled (executedQty > 0 or status FILLED), process it
                    string status = order.value("status", "");
                    double executedQty = 0.0;
                    try {
                        executedQty = stod(order.value("executedQty", string("0")));
                    } catch (...) { executedQty = 0.0; }
                    if (status == "FILLED" || executedQty > 0.0) {
                        process_filled_order(order);
                    }
                }
            }
            // also release reservations for non-open orders
            poll_open_orders();
        } catch (const std::exception &e) {
            std::ostringstream oss;
            oss << "[run] poll/open processing error: " << e.what();
            log_message(oss.str());
        }

        std::this_thread::sleep_for(std::chrono::seconds(order_check_interval_sec_));
    }
}


