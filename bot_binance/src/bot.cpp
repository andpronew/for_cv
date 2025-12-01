// src/bot.cpp
#include "binance_client.hpp"
#include "ladder_strategy.h"
#include "logging.h"
#include <nlohmann/json.hpp>
#include <fstream>
#include <iostream>
#include <thread>
#include <chrono>
#include <iomanip>
#include <sstream>

using namespace std;
using json = nlohmann::json;

void run_bot()
{
    // Load config.json (single source for all params)
    ifstream config_file("config.json");
    if (!config_file.is_open())
    {
        cerr << "Error: cannot open config.json" << endl;
        return;
    }

    json config;
    try {
        config_file >> config;
    } catch (const std::exception &e) {
        cerr << "Error: failed to parse config.json: " << e.what() << endl;
        return;
    }

    // Read parameters from config.json (single source of truth)
    const string api_key               = config.value("api_key", string(""));
    const string secret_key            = config.value("secret_key", string(""));
    const bool   sandbox               = config.value("sandbox", true);
    const string symbol                = config.value("symbol", string("BTCFDUSD"));

    const int    poll_interval         = config.value("poll_interval", 5); // seconds (used below)
    const double test_order_qty        = config.value("test_order_qty", 0.0001);
    const bool   aggressive_limit_test = config.value("aggressive_limit_test", true);

    const int    ladder_size           = config.value("ladder_size", 5);
    const double ladder_step           = config.value("ladder_step", 1.0);
    const double order_size            = config.value("order_size", 0.0001);

    // NEW: capital and order_timeout read from config.json
    const double capital               = config.value("capital", 100.0);
    const int    order_timeout         = config.value("order_timeout", 30);

    // Additional protection params (read but applied inside LadderStrategy if supported)
    const bool   prevent_loss_sells    = config.value("prevent_loss_sells", true);
    const double min_profit_quote      = config.value("min_profit_quote", 0.0);
    const double min_price_buffer_usdt = config.value("min_price_buffer_usdt", 0.0);
    const int    order_check_interval  = config.value("order_check_interval", 1);

    // Log loaded config
    {
        std::ostringstream oss;
        oss << "Config: symbol=" << symbol
            << " ladder_size=" << ladder_size
            << " ladder_step=" << ladder_step
            << " order_size=" << order_size
            << " capital=" << std::fixed << std::setprecision(8) << capital
            << " order_timeout=" << order_timeout
            << " poll_interval=" << poll_interval;
        log_message(oss.str());
    }

    // Create Binance client
    BinanceClient client(api_key, secret_key, sandbox);

    // Optional quick test orders to verify connectivity/execution on testnet
    try
    {
        double mid_price = client.get_price(symbol);
        {
            ostringstream oss;
            oss << "Current mid price: " << fixed << setprecision(8) << mid_price;
            log_message(oss.str());
        }

        if (aggressive_limit_test)
        {
            double buy_price  = mid_price + 1.0;
            double sell_price = mid_price - 1.0;
            log_message("=== Placing aggressive LIMIT BUY order (test) ===");
            string buy_resp = client.place_order(symbol, "BUY", "LIMIT", buy_price, test_order_qty);
            log_order_response(buy_resp);

            log_message("=== Placing aggressive LIMIT SELL order (test) ===");
            string sell_resp = client.place_order(symbol, "SELL", "LIMIT", sell_price, test_order_qty);
            log_order_response(sell_resp);
        }
        else
        {
            double price = mid_price;
            log_message("=== Placing conservative LIMIT BUY order (test) ===");
            string buy_resp = client.place_order(symbol, "BUY", "LIMIT", price, test_order_qty);
            log_order_response(buy_resp);

            log_message("=== Placing conservative LIMIT SELL order (test) ===");
            string sell_resp = client.place_order(symbol, "SELL", "LIMIT", price, test_order_qty);
            log_order_response(sell_resp);
        }
    }
    catch (const std::exception &e)
    {
        log_message(string("[run_bot] test limit orders failed: ") + e.what());
    }

    // Create LadderStrategy with 7 args (matches header):
    // LadderStrategy(BinanceClient&, const string&, int, double, double, double, int)
    try
    {
        LadderStrategy strategy(client, symbol, ladder_size, ladder_step, order_size, capital, order_timeout);

        // If LadderStrategy later exposes setters for additional config options, call them here:
        // e.g. strategy.set_prevent_loss_sells(prevent_loss_sells); ...

        // Run strategy in separate thread so main thread can still poll open orders periodically
        std::thread strategy_thread([&strategy]() {
            try {
                strategy.run();
            } catch (const std::exception &e) {
                log_message(string("[strategy_thread] LadderStrategy::run threw: ") + e.what());
            }
        });

        // Main thread does periodic polling for open orders (uses poll_interval so unused warning disappears)
        while (true)
        {
            try {
                client.poll_open_orders(symbol);
            } catch (const std::exception &e) {
                log_message(string("[run_bot] poll_open_orders failed: ") + e.what());
            }
            std::this_thread::sleep_for(std::chrono::seconds(poll_interval));
        }

        if (strategy_thread.joinable()) strategy_thread.join();
    }
    catch (const std::exception &e)
    {
        log_message(string("[run_bot] failed to create/run LadderStrategy: ") + e.what());
        return;
    }
}

