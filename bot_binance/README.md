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

C++17-compatible compiler (e.g. GCC 9+, Clang 10+, or MSVC 2019+), CMake (version 3.16 or newer) to configure the build.

Dependencies: The bot uses several libraries:
```
cURL – for HTTP requests to the Binance API.

OpenSSL – for HMAC SHA256 signing of API requests.

nlohmann/json – for JSON parsing (header-only library).
```
Installing Dependencies:
```
On Debian/Ubuntu: install development packages via apt, e.g.
sudo apt install libcurl4-openssl-dev libssl-dev
(and ensure OpenSSL is installed). For JSON, either install the nlohmann-json3-dev package
or include the single-header json.hpp in the include/ directory.

On Windows: use vcpkg or another package manager to install curl, openssl, and nlohmann-json,
or add their include/lib paths to CMake.

On macOS: use Homebrew, e.g. brew install curl openssl nlohmann-json.

API Keys: You will need Binance API credentials. For testing, generate API keys on the Binance Testnet (if sandbox=true in config).
Never commit real API keys to the repository. Keep your config.json (which contains keys) out of version control
or use environment variables to inject keys for testing.
```
Building:
```
Clone the repository: https://github.com/andpronew/for_cv/tree/main/bot_binance

Configure with CMake: It’s recommended to build out-of-source. Create a build directory and run CMake:

mkdir build && cd build  
cmake ..

This will find required libraries and generate the build system (Makefile or Visual Studio project files, depending on your platform).

Compile the project:

cmake --build . --target bot -- -j4
```

This produces the bot executable (or bot.exe on Windows) in the build directory. You can also use your IDE or simply run make -j4 in the build directory if using Unix Makefiles.

Run the bot: Ensure a config.json is present in the working directory. Then run: ./bot

By default, it will log to bot_output.txt and place orders on the Binance Testnet (sandbox mode). Always test in sandbox mode first!


Requirements:
```
CMake ≥ 3.16

C++17 compiler

libcurl

OpenSSL (Crypto)

Threads

nlohmann_json (header-only)
```

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

All configuration parameters for the bot are explained below. 

```
The bot is configured via a JSON file (config.json) located in the working directory.
Below are all the fields, what they mean, and how they affect the bot’s behavior:

api_key (string, required): Your Binance API key for authentication. This key is used to sign requests.
Note: Even for sandbox:true, you must provide a Testnet API key. Never share this publicly.

secret_key (string, required): Your Binance API secret key. Used for request signature generation.
Keep this secret safe.

sandbox (boolean, optional, default: true): If true, the bot connects to Binance Testnet (sandbox environment)
instead of the production API. Use true for testing and only set false when you are ready to trade real funds.

symbol (string, optional, default: "BTCFDUSD"): The trading pair symbol the bot will operate on. For example,
"BTCUSDT" for Bitcoin/USDT. Make sure the symbol is valid on Binance. The default "BTCFDUSD" is an example
(BTC traded against FDUSD stablecoin). Change this to your desired market.

poll_interval (integer, optional, default: 5 seconds): How often (in seconds) the main thread polls for open orders.
This controls the frequency of console logging of order status. For example, with 5, the bot logs open order status every 5 seconds.

ladder_size (integer, optional, default: 5): The number of buy orders to place in each “ladder”. The strategy will attempt
to place this many limit BUY orders below the current market price on each cycle. For example, if ladder_size=5, it will place
5 buy orders at incrementally lower prices (as defined by ladder_step).

Acceptable range: 1 or higher (there’s no hard-coded upper limit, but be mindful of your capital; a larger ladder
will require more capital).

Effect: Higher ladder_size means more orders spread out below the price, potentially catching more market dips, but also
using more capital at once.

ladder_step (double, optional, default: 1.0): Price gap between consecutive ladder orders, in the quote currency units.
For instance, if ladder_step=1.0 on a BTC/USDT pair and current price is 30000, the bot places buys at 29999, 29998, etc. (1 USDT apart).

Must be a positive number. It can be fractional (e.g. 0.5 for 50 cents gap). If set too low, orders will cluster near
the current price; if too high, orders spread further away (less likely to fill unless a big price move).

order_size (double, optional, default: 0.0001): The quantity of the base asset to buy or sell in each order. For BTC/USDT,
this would be in BTC. Default 0.0001 BTC is a small trade size.

Ensure this meets Binance’s minimum trade size for the chosen symbol. For example, many markets have a minimum order size
around $10 equivalent.

Increasing order_size will increase the amount of asset per trade and thus the capital used for each buy.

test_order_qty (double, optional, default: 0.0001): The quantity used for the initial test orders placed at startup. On launch,
the bot can place a quick buy and sell (in Testnet) to verify everything is working. This is the size of those test orders, typically kept very small.

Note: This is separate from the main strategy’s order_size to allow non-intrusive testing.

aggressive_limit_test (boolean, optional, default: true): Controls how the startup test orders are placed:

If true: the bot places an aggressive buy (above market price) and an aggressive sell (below market price) at startup.
These orders are likely to fill immediately on the Testnet, confirming the bot’s ability to execute trades.

If false: the bot places both test orders at the current mid-market price. These may not fill right away (or at all)
but are safer in real markets since they won’t execute immediately.

This setting only affects the initial connectivity test, not the main ladder strategy.

capital (double, optional, default: 100.0): The starting capital in the quote currency that the bot is allowed to use for trading.
For example, if symbol is BTCUSDT, this is in USDT.

The bot will never exceed this amount in active buy orders. It “reserves” capital whenever it places a buy.
If you set this too low relative to order_size * ladder_size, the bot might not place all intended orders
(it will stop once capital is exhausted).

This should generally match or be less than the actual funds you have in your Binance account for the quote asset.
Safety: It’s wise to set this to a portion of your actual balance to limit risk.

order_timeout (integer, optional, default: 30 seconds): (Planned feature) The time after which a placed order is considered “stale”.
The intention is that if an order remains unfilled for this many seconds, the bot could cancel it to free up capital.

Note: At present, the bot’s code reads this value but does not yet actively cancel orders after the timeout. This is a roadmap item.
Future versions may use this to cancel or replace stale orders.

You can still use this field for documentation; just be aware it currently does not trigger automatic cancellations.

prevent_loss_sells (boolean, optional, default: true): A protective flag to avoid selling at a loss. If true, the strategy will not
immediately place a SELL for a filled buy if doing so would realize a loss (or less profit than desired).

Specifically, if prevent_loss_sells=true, the bot checks the expected profit of an immediate sell. If the profit would be below the
threshold (min_profit_quote), it skips placing the sell order for now. The bought asset is held (recorded in the bot’s internal BTC
balance) in hopes of a better price later.

If false, the bot will place a sell as soon as a buy fills, even if the profit is negligible or negative (this could realize losses,
so use caution).

min_profit_quote (double, optional, default: 0.0): The minimum profit (in quote currency terms) that the bot seeks for each buy->sell cycle.
This works in tandem with prevent_loss_sells:

When a buy order fills, the bot calculates (target_sell_price - buy_price) * quantity. If this value is below min_profit_quote, and
prevent_loss_sells is true, the bot will hold instead of selling immediately.

For example, if min_profit_quote=1.0, the bot will only execute an immediate sell if it would yield at least 1.0 USDT (or whatever
the quote currency is) profit. Smaller profits (or losses) cause the sell to be deferred.

Default 0.0 means essentially “no minimum profit required” (just don’t sell at a loss). Set this to a positive value to ensure a
minimum profit per trade.

Acceptable range: 0 or positive. (Negative values would imply you allow a fixed loss per trade, which isn’t typical.)

min_price_buffer_usdt (double, optional, default: 0.0): A small price buffer added to the buy price when calculating the target sell price,
and also used to ensure maker orders:

After a buy fills, initial target_sell_price is set to buy_price + min_price_buffer_usdt. This is essentially your desired price increase
(in quote currency) for the sell.

Additionally, before placing a sell, the bot looks at the current order book best bid. If target_sell_price is not greater than the best bid,
it will bump it up to bestBid + min_price_buffer_usdt. This prevents placing a sell at or below the best bid, which would execute immediately
as a taker order.

Recommendation: Set this to a small positive value (like a few cents or dollars depending on the asset) to ensure your sell orders sit just
above the current bid. A value of 0.0 means the sell could be placed exactly at the best bid – which might get filled instantly (turning you
into a taker and possibly incurring fees). For safety, even a minimal buffer (0.01, etc.) helps maintain maker status.

order_check_interval (integer, optional, default: 1 second): The frequency (in seconds) at which the LadderStrategy thread cycles through
its main loop.

This essentially controls how often the bot places new ladder orders and checks for filled orders. Default 1 means the strategy loop runs
every second.

If you set this higher (e.g. 5 or 10), the bot will operate more slowly – placing orders and checking fills less frequently, which could be
safer on API rate limits but also means slower response to market movements.

Lower than 1 is not allowed (must be at least 1 second to avoid a tight busy-loop and hitting API too often).

Note: The main thread’s poll_interval is separate; that one is just for logging. This order_check_interval is the actual strategy pace.

Configuration Tips and Safety

API Permissions: The API key you use should have trading enabled (and IP restrictions set if possible for security). If you only want to
test placement and cancellation, you can also enable only trading and perhaps not withdrawals.

Starting Capital vs. Actual Funds: The capital parameter lets you limit how much of your funds the bot will use. It’s good practice to
set this lower than your total account balance, so the bot leaves some margin. The bot will never exceed this value in active orders.

Selecting a Symbol: Ensure the symbol is correctly formatted (Binance uses uppercase, e.g. "ETHUSDT"). If using a stablecoin or newer asset,
double-check if it has zero maker fees if you keep the default safety check. The bot’s default safety check (in code) will prevent trading
on symbols that have maker fees. Binance often has specific pairs with 0 fees (like certain stablecoin pairs).

Real Trading Caution: When sandbox is false, you are trading real money. Double-check all parameters. It’s advisable to start with very
small order_size and capital until you are confident in the bot’s behavior. Monitor the bot’s output and your Binance account when you
first run on real markets.

Logging: The bot produces two log outputs:

bot_output.txt: General log of actions and messages (also printed to console).

logs/orders.txt: Detailed order log (one line per order status update, including timestamps, prices, quantities, and a running total of
capital, asset balance, and profit).
Ensure the logs/ directory exists (the bot will attempt to create it on startup). These logs are invaluable for understanding what the bot
is doing and for debugging configuration issues.

The config.json file is the single source of truth for the bot’s runtime settings. Update it carefully and restart the bot for changes to
take effect. If you add new parameters (through code changes), document them here to help users configure the bot correctly.
```

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
### 📂 Sequence diagram: Order Flow

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
Notes: The main thread initializes the bot and spawns the LadderStrategy in a separate thread. The LadderStrategy continuously places a “ladder” of buy orders below the current market price and manages corresponding sell orders for profit-taking. The strategy tracks available capital, reserved funds for open orders, and the quantity of asset (e.g. BTC) held from filled buys. Profit is realized when sells execute above their buy price. The main thread concurrently polls and logs open orders for visibility, while the strategy thread handles order placement and fill processing.

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
