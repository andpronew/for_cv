# 🤖 C++17 Market-Making Ladder Bot for Binance (Testnet)

C++17 market-making bot designed for Binance Spot (supports Testnet during development).
The bot implements a ladder trading strategy: it places a symmetric set of passive limit BUY/SELL orders around the mid-price, captures the spread when orders fill, and tracks realised profit using FIFO inventory accounting.

The architecture is modular, production-oriented, and built entirely on standard modern C++ + libcurl + OpenSSL + nlohmann_json.

### 📂 Project Structure
```text
BotAndr/
│
├── CMakeLists.txt                       # Build configuration (CMake ≥ 3.16)
├── config.json                           # All runtime parameters (symbol, sizes, API keys, etc.)
│
├── main.cpp                              # Entry point (loads config, starts bot)
├── src/
│   ├── bot.cpp                           # Bot runner: orchestrates strategy + Binance client
│   ├── binance_client.cpp                # REST/HTTP implementation, signing, order placement
│   ├── ladder_strategy.cpp               # Market-making logic (ladder placement + profit tracking)
│   ├── logging.cpp                       # Simple thread-safe logger
│   └── ... other helpers ...
│
├── include/
│   ├── binance_client.hpp
│   ├── ladder_strategy.h
│   ├── logging.h
│   └── ... headers ...
│
├── logs/
│   ├── bot_YYYY-MM-DD_HHMMSS.txt         # Runtime log (rotated each run)
│   ├── orders.txt                        # Append-only order log (raw events)
│   ├── orders_sorted.txt                 # Sorted + enriched order history with profit tracking
│   └── ...
│
└── build/                                # Out-of-source build
```
### ⚙️ Build Instructions
Requirements:
```
CMake ≥ 3.16

C++17 compiler

libcurl

OpenSSL (Crypto)

Threads

nlohmann_json (header-only)
```
Build:
```
rm -rf build
cmake -S . -B build
cmake --build build --clean-first
```

CMake automatically links:
```
CURL,

OpenSSL::Crypto,

Threads,

nlohmann_json (if installed; otherwise include json.hpp manually).
```
📄 CMakeLists.txt (auto-includes all sources):


CMakeLists

🔧 Configuration (config.json)

Example:
```
{
  "api_key": "YOUR_KEY",
  "secret_key": "YOUR_SECRET",
  "sandbox": true,
  "symbol": "BTCFDUSD",
  "poll_interval": 5,
  "ladder_size": 5,
  "ladder_step": 1.0,
  "order_size": 0.0001,
  "test_order_qty": 0.0001,
  "aggressive_limit_test": true,
  "order_timeout": 30,
  "capital": 100.0,
  "min_profit_quote": 0.0,
  "order_check_interval": 1,
  "prevent_loss_sells": true
}
```

📌 Important:
The symbol here controls the entire bot — REST endpoints, orders, strategy behaviour.
"sandbox": true forces the bot to use Binance Testnet and bypasses commission checks.

Config reference:

config

### 🧠 Architecture Overview
1. BinanceClient — REST API wrapper
```
Implements:

HMAC-SHA256 signing (OpenSSL)

perform_request() (curl wrapper)

build_query_string() / signed_query()

place_order() overloads
```
Query endpoints:
```
/api/v3/ticker/bookTicker

/api/v3/openOrders

/api/v3/order

/sapi/v1/asset/tradeFee
```
Features:
```
Maker safety checks (avoid taker fees)

Zero-commission pair check (stubbed to true in Testnet)

Automatic logging of each request/response

Graceful handling of missing fields in Binance responses

Full separation of I/O (HTTP) from strategy logic
```
2. LadderStrategy — Market-Making Engine

### 📄 Implementation references:
```
Headers: ladder_strategy.h
Implementation: ladder_strategy.cpp
```
How it works:
```
Reads config (ladder_size, ladder_step, order_size).

On startup:

Fetches mid_price (from bookTicker).

Places symmetric BUY and SELL limit orders.

For example, with ladder_size=5 and ladder_step=1.0:

BUY:  mid - 1·step
BUY:  mid - 2·step
...
SELL: mid + 1·step
SELL: mid + 2·step


Maintains internal bookkeeping:

tracked_orders_

FIFO buy_queue_ for cost basis

running profit: profit_accum_

Every poll cycle:

fetch open orders

detect fills or partial fills

update logs and profit

refill missing ladder orders

SELL placement is blocked if expected profit < 0
(safety mechanism)

For each filled SELL:

matches against FIFO BUY queue

calculates realised profit

logs the realised P&L into orders_sorted.txt.

Safety features

Prevents SELL orders that would realise negative profit

Respects maker-only constraint (never sends IOC/FOK)

Commission handling:

Extracts commission from JSON (when available)

Incorporates commission into profit calculations

Handles partial fills

Ignores negative/invalid responses gracefully

Ensures ladder is always consistent (replace missing orders)
```
3. Bot — Orchestration Layer

The main loop in bot.cpp:
```
Reads config.json

Creates BinanceClient + LadderStrategy

Calls strategy.run()

Handles all exceptions and logs critical errors

Timestamps and rotates runtime logs in logs/
```
### 📂 Sequence diagram 

The bot's end-to-end order flow, from startup through continuous operation. It illustrates how the main thread and strategy thread interact with the Binance API and track orders/profits:
```
Main Thread                LadderStrategy Thread              Binance API
    |                              |                              |
    |-- Load config.json ----------|                              |
    |   (API keys, params)         |                              |
    |-- Init BinanceClient ------->| (API keys, sandbox mode)     |
    |                              |                              |
    |-- [Optional] Place small test orders to verify connection ->| 
    |                              |<-- (Test BUY/SELL responses) |
    |                              |                              |
    |-- Launch LadderStrategy thread --> LadderStrategy::run()    |
    |                              |-- Start strategy loop ------>|
    |                              |   (logs "Starting strategy") |
    |                              |                              |
    |                              |   **Loop each order_check_interval (e.g. 1s)**:
    |                              |   1. **Get current price** ->| (GET /ticker/price)
    |                              |      <--- mid-price (latest) |
    |                              |   2. **Place ladder BUY orders**:
    |                              |      For i = 1 to ladder_size:
    |                              |         compute buy_price = mid_price - i*ladder_step
    |                              |         reserve `capital` for order
    |                              |         place LIMIT BUY ->| (POST /order)
    |                              |         <--- orderId returned (order open) |
    |                              |         attach reservation to orderId
    |                              |      (If not enough capital, stop placing more buys)
    |                              |   3. **Check open orders** ->| (GET /openOrders)
    |                              |      <--- list of open orders (JSON) --------|
    |                              |   4. **Process open orders**:
    |                              |      log each open order to `logs/orders.txt`
    |                              |      for each order:
    |                              |         if `status == FILLED` or executedQty > 0:
    |                              |            LadderStrategy::process_filled_order:
    |                              |            if BUY filled:
    |                              |               release reserved capital (spent)
    |                              |               increase BTC balance (executedQty)
    |                              |               *if profit >= min_profit_quote*: 
    |                              |                   target_sell_price = buy_avg + buffer 
    |                              |                   ensure target_sell_price > bestBid
    |                              |                   place LIMIT SELL ->| (POST /order)
    |                              |                   <--- orderId for sell (open) -|
    |                              |               *if profit < min_profit_quote*:
    |                              |                   **skip** immediate sell (hold asset)
    |                              |            if SELL filled:
    |                              |               add sale proceeds to capital_usdt
    |                              |               decrease BTC balance (sold quantity)
    |                              |               (realized profit added to capital)
    |                              |      end for
    |                              |   5. **Reconcile reservations**:
    |                              |      for any orderId no longer in open orders:
    |                              |          release any remaining reserved capital
    |                              |   6. **Sleep** for `order_check_interval` seconds
    |                              |   -- Loop repeats (go to step 1) --
    |                              |                              |
    |<<-- (Meanwhile, in parallel) -->>|                              |
    |   **Poll open orders** every `poll_interval` seconds:        |
    |   Main -> BinanceClient.poll_open_orders(symbol) ->| (GET /openOrders)
    |                              |<--- open orders JSON --------|
    |   logs summary of each open order (id, side, price, status) to console
    |                              |                              |
    |   ... (Main thread continues logging while strategy thread runs) ...
```

### 📜 Logging System
1. logs/bot_YYYY-MM-DD_HHMMSS.txt

High-level execution log:
```
startup

API requests

errors

ladder events

profit updates
```
2. logs/orders.txt

Append-only raw log of order events:
```
order creation

fills

partial fills

commission

timestamps
```
3. logs/orders_sorted.txt

A derived, tabular file that sorts all orders and adds:
```
realised profit

profit_accum (cumulative)

buy/sell matching details

Ideal for external analytics or Python/Pandas processing.
```
### 💰 Profit Accounting (FIFO)

The bot maintains a FIFO queue of BUY fills:
```
BUY  qty=0.1 at price 20000
BUY  qty=0.2 at price 20100
BUY  qty=0.1 at price 19950
```

When a SELL of quantity q fills, it consumes from the queue:
```
SELL qty=0.15 at 20200
  → consumes 0.1 from 20000
  → consumes 0.05 from 20100
```

Profit = Σ (sell_price - buy_price) * matched_qty - commissions

### 🔐 Security & API Handling
```
Uses HMAC-SHA256 via OpenSSL::Crypto

All secret keys stored only in config.json and in RAM

No logging of secret_key

Requests include timestamp + signature

Sandbox trading supported (no real money)
```
### ⚠️ Known Limitations / TODO
```
Rate-limit handling (HTTP 429 / 418) can be improved

State persistence across restarts (tracked_orders_, buy_queue_)

Missing advanced parameters (tick size / minQty from exchangeInfo)

Add true dry-run simulation mode

Move logging to JSON for easier analytics

Add GoogleTest suite for strategy and client
```
### ▶️ Running the Bot
./bot

Make sure:
```
config.json is in the project root

logs/ directory exists

you are connected to the internet

sandbox=true if you use Testnet
```
### 🧪 Development Workflow

Clean & rebuild:
```
rm -rf build
cmake -S . -B build
cmake --build build --clean-first
```
### 📈 Example Ladder Operation
Initial mid_price = 30000
```
BUY  @ 29999
BUY  @ 29998
BUY  @ 29997
BUY  @ 29996
BUY  @ 29995

SELL @ 30001
SELL @ 30002
SELL @ 30003
SELL @ 30004
SELL @ 30005
```
As orders fill, ladder_strategy:
```
logs fills

updates FIFO inventory

computes realised P&L

replaces missing orders

avoids unprofitable SELLs
```
### 🧭 Design Goals

✔ Robust to API failures

✔ Profit-first ladder logic

✔ Maker-only trading

✔ Easy debugging through logs

✔ Avoid accidental market orders

✔ Safe for Testnet experimentation

### 🏁 Summary

The Bot is a fully working, modular trading bot built in modern C++17.
It is structured for clarity, extensibility, and correctness, and includes:

✔ A strong Binance API client with signing & error handling

✔ A production-ready ladder market-making engine

✔ FIFO profit accounting

✔ Detailed logging and replayable history

✔ Testnet support and safety checks

✔ The project can be further extended into a full-scale algorithmic trading system.
