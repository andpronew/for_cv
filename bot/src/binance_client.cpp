// src/binance_client.cpp
#include "binance_client.hpp"
#include "logging.h"

#include <nlohmann/json.hpp>

#include <curl/curl.h>

#include <openssl/hmac.h>
#include <openssl/evp.h>
#include <iostream>
#include <chrono>
#include <iomanip>
#include <sstream>
#include <stdexcept>
#include <cstring>
#include <algorithm>

using json = nlohmann::json;
using namespace std;

// ------------------------ low-level helpers ------------------------

static size_t curl_write_cb(void *contents, size_t size, size_t nmemb, void *userp)
{
    size_t realsize = size * nmemb;
    string *s = static_cast<string *>(userp);
    s->append(static_cast<char *>(contents), realsize);
    return realsize;
}

static string hmac_sha256_hex(const string &key, const string &data)
{
    unsigned char result[EVP_MAX_MD_SIZE];
    unsigned int len = 0;

    HMAC_CTX *ctx = HMAC_CTX_new();
    if (!ctx)
        throw runtime_error("HMAC_CTX_new failed");

    if (HMAC_Init_ex(ctx, reinterpret_cast<const unsigned char*>(key.data()), static_cast<int>(key.size()),
                     EVP_sha256(), nullptr) != 1)
    {
        HMAC_CTX_free(ctx);
        throw runtime_error("HMAC_Init_ex failed");
    }

    if (HMAC_Update(ctx, reinterpret_cast<const unsigned char*>(data.data()), static_cast<int>(data.size())) != 1)
    {
        HMAC_CTX_free(ctx);
        throw runtime_error("HMAC_Update failed");
    }

    if (HMAC_Final(ctx, result, &len) != 1)
    {
        HMAC_CTX_free(ctx);
        throw runtime_error("HMAC_Final failed");
    }

    HMAC_CTX_free(ctx);

    std::ostringstream oss;
    oss << std::hex << std::setfill('0');
    for (unsigned int i = 0; i < len; ++i)
    {
        oss << setw(2) << static_cast<unsigned int>(result[i]);
    }
    return oss.str();
}

static string urlencode_basic(const string &s)
{
    // conservative urlencode used for parameters (sufficient for typical keys/values)
    std::ostringstream oss;
    for (unsigned char c : s)
    {
        if (isalnum(c) || c == '-' || c == '_' || c == '.' || c == '~')
            oss << c;
        else
        {
            oss << '%' << std::uppercase << std::hex << setw(2) << int(c) << std::nouppercase << std::dec;
        }
    }
    return oss.str();
}

string BinanceClient::now_timestamp_ms() const
{
    using namespace std::chrono;
    auto ms = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
    return to_string(ms);
}

string BinanceClient::build_api_url(const string &base_url, const string &endpoint_tail)
{
    // endpoint_tail e.g. "v3/order" or "v3/openOrders?..." (we allow passing the query tail too)
    if (base_url.empty()) return string("https://testnet.binance.vision/api/") + endpoint_tail;

    // If base_url already contains "/api" at end, avoid doubling
    if (base_url.size() >= 4 && base_url.substr(base_url.size() - 4) == "/api")
    {
        return base_url + "/" + endpoint_tail;
    }

    if (base_url.back() == '/')
        return base_url + "api/" + endpoint_tail;

    return base_url + "/api/" + endpoint_tail;
}

string BinanceClient::build_query_string(const map<string, string> &params) const
{
    std::ostringstream oss;
    bool first = true;
    for (const auto &kv : params)
    {
        if (!first) oss << "&";
        first = false;
        oss << kv.first << "=" << urlencode_basic(kv.second);
    }
    return oss.str();
}

string BinanceClient::signed_query(const string &query) const
{
    string sig = hmac_sha256_hex(secret_key_, query);
    return query + "&signature=" + sig;
}

// ------------------------ networking ------------------------

string BinanceClient::perform_request(const string &method, const string &url, const string &post_fields, bool use_api_key) const
{
    CURL *curl = curl_easy_init();
    if (!curl)
    {
        log_message("[perform_request] curl_easy_init failed");
        throw runtime_error("curl_easy_init failed");
    }

    string response;
    struct curl_slist *headers = nullptr;
    headers = curl_slist_append(headers, "Content-Type: application/x-www-form-urlencoded");
    if (use_api_key)
    {
        string h = "X-MBX-APIKEY: " + api_key_;
        headers = curl_slist_append(headers, h.c_str());
    }

    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, curl_write_cb);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT, 15L);

    // Support methods: GET (default), POST, DELETE
    if (method == "POST")
    {
        curl_easy_setopt(curl, CURLOPT_POST, 1L);
        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, post_fields.c_str());
        curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE_LARGE, (curl_off_t)post_fields.size());
    }
    else if (method == "DELETE")
    {
        // use custom request DELETE; no body expected (parameters in query)
        curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "DELETE");
        // ensure not using HTTPGET/POST
    }
    else
    {
        // default to GET
        curl_easy_setopt(curl, CURLOPT_HTTPGET, 1L);
    }

    CURLcode res = curl_easy_perform(curl);
    long http_code = 0;
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);

    if (res != CURLE_OK)
    {
        std::ostringstream oss;
        oss << "[perform_request] curl_easy_perform() failed: " << curl_easy_strerror(res) << " url=" << url;
        log_message(oss.str());
        if (!response.empty())
            log_message(string("[perform_request] partial response: ") + response);

        curl_slist_free_all(headers);
        curl_easy_cleanup(curl);
        throw runtime_error(string("curl error: ") + curl_easy_strerror(res));
    }

    {
        std::ostringstream oss;
        oss << "[perform_request] url=" << url << " method=" << method << " http_code=" << http_code << " response_len=" << response.size();
        log_message(oss.str());
    }

    if (method == "POST" && response.empty())
    {
        std::ostringstream oss;
        oss << "[perform_request] WARNING: empty response for POST. post_fields_len=" << post_fields.size();
        if (!post_fields.empty()) oss << " post_fields_prefix=" << post_fields.substr(0, std::min<size_t>(512, post_fields.size()));
        log_message(oss.str());
    }

    if (!response.empty())
    {
        string preview = response.size() > 1024 ? response.substr(0, 1024) + "..." : response;
        log_message(string("[perform_request] response_preview: ") + preview);
    }

    curl_slist_free_all(headers);
    curl_easy_cleanup(curl);
    return response;
}

// ------------------------ construction & destruction ------------------------

 BinanceClient::BinanceClient(const string &api_key, const string &secret_key, const string &base_url)
    : api_key_(api_key), secret_key_(secret_key), base_url_(base_url)
 {
     // Инициализируем флаг по base_url: если testnet – считаем sandbox
    std::string lower = base_url_;
    std::transform(lower.begin(), lower.end(), lower.begin(), ::tolower);
    sandbox_ = (lower.find("testnet") != std::string::npos);
     std::ostringstream oss;
     oss << "[DEBUG] BinanceClient constructed. api_key.len=" << api_key_.size()
         << " secret_key.len=" << secret_key_.size()
         << " base_url=" << base_url_;
     log_message(oss.str());
 
     curl_global_init(CURL_GLOBAL_DEFAULT);
 }

BinanceClient::BinanceClient(const string &api_key, const string &secret_key, bool sandbox)
     : BinanceClient(api_key,
                     secret_key,
                     (sandbox ? string("https://testnet.binance.vision/api") : string("https://api.binance.com/api")))
 {
    // Делегирующий конструктор не может инициализировать поля,
    // поэтому выставляем флаг здесь.
     this->sandbox_ = sandbox;
 }
 BinanceClient::~BinanceClient() {}

// ------------------------ market helpers ------------------------

double BinanceClient::get_price(const string &symbol) const
{
    string url = build_api_url(base_url_, string("v3/ticker/price?symbol=") + symbol);
    std::ostringstream dbg;
    dbg << "[get_price] url=" << url;
    log_message(dbg.str());

    string resp = perform_request("GET", url, "", false);
    try
    {
        auto j = json::parse(resp);
        if (j.contains("price"))
        {
            string price_s = j["price"].get<string>();
            return stod(price_s);
        }
        throw runtime_error("get_price: no price field");
    }
    catch (const std::exception &e)
    {
        log_message(string("[get_price] parse error: ") + e.what());
        throw;
    }
}

pair<double,double> BinanceClient::get_book_ticker(const string &symbol) const
{
    string url = build_api_url(base_url_, string("v3/ticker/bookTicker?symbol=") + symbol);
    log_message(string("[get_book_ticker] url=") + url);
    string resp = perform_request("GET", url, "", false);
    try
    {
        auto j = json::parse(resp);
        double bid = 0.0, ask = 0.0;
        if (j.contains("bidPrice")) bid = stod(j["bidPrice"].get<string>());
        if (j.contains("askPrice")) ask = stod(j["askPrice"].get<string>());
        return {bid, ask};
    }
    catch (const std::exception &e)
    {
        log_message(string("[get_book_ticker] parse error: ") + e.what());
        throw;
    }
}

// ------------------------ trade fee ------------------------
// GET /sapi/v1/asset/tradeFee?symbol=XXX
TradeFee BinanceClient::get_trade_fee(const string &symbol) const
{
    try
    {
        map<string, string> params;
        params["symbol"] = symbol;
        params["timestamp"] = now_timestamp_ms();
        string query = build_query_string(params);
        string sig = hmac_sha256_hex(secret_key_, query);
        string url = build_api_url(base_url_, string("sapi/v1/asset/tradeFee?") + query + "&signature=" + sig);

        log_message(string("[get_trade_fee] url_preview: ") + (url.size() > 200 ? url.substr(0,200) + "..." : url));
        string resp = perform_request("GET", url, "", true);

        auto j = json::parse(resp);
        if (!j.is_array() || j.empty())
        {
            throw runtime_error(string("get_trade_fee: unexpected response: ") + resp);
        }
        auto obj = j[0];
        TradeFee tf;
        tf.makerCommission = obj.value("makerCommission", 0);
        tf.takerCommission = obj.value("takerCommission", 0);
        return tf;
    }
    catch (const std::exception &e)
    {
        log_message(string("[get_trade_fee] failed: ") + e.what());
        throw;
    }
}

// Public check if pair has zero maker commission

bool BinanceClient::is_zero_commission_pair(const string &symbol) const
{
    if (this->sandbox_)
    {
        cout << "[is_zero_commission_pair] sandbox mode: assume 0% commission for " << symbol << endl;
        return true;
    }

    try
    {
        TradeFee fee_info = get_trade_fee(symbol);
        cout << "[is_zero_commission_pair] maker=" << fee_info.makerCommission
             << ", taker=" << fee_info.takerCommission << endl;

        return (fee_info.makerCommission == 0.0 && fee_info.takerCommission == 0.0);
    }
    catch (const exception &e)
    {
        cerr << "[is_zero_commission_pair] error: " << e.what() << endl;
        return false;
    }
}

// ------------------------ place_order (string version) ------------------------
string BinanceClient::place_order(const string &symbol,
                                  const string &side,
                                  const string &type,
                                  const string &price,
                                  const string &qty,
                                  const string &time_in_force) const
{
    try
    {
        map<string, string> params;
        params["symbol"] = symbol;
        params["side"] = side;
        params["type"] = type;
        params["quantity"] = qty;

        if (type == "LIMIT")
        {
            params["price"] = price;
            params["timeInForce"] = time_in_force;
        }

        params["timestamp"] = now_timestamp_ms();

        string query = build_query_string(params);
        string sig = hmac_sha256_hex(secret_key_, query);
        string full_post_fields = query + "&signature=" + sig;

        string url = build_api_url(base_url_, "v3/order");

        std::ostringstream dbg;
        dbg << "[place_order] url=" << (url.size() > 200 ? url.substr(0,200) + "..." : url)
            << " post_fields_len=" << full_post_fields.size();
        log_message(dbg.str());

        string response = perform_request("POST", url, full_post_fields, true);

        // log structured order response (existing helper)
        log_order_response(response);

        return response;
    }
    catch (const std::exception &e)
    {
        log_message(string("[place_order] exception: ") + e.what());
        throw;
    }
}

// ------------------------ place_order (numeric version + safety checks) ------------------------
string BinanceClient::place_order(const string &symbol,
                                  const string &side,
                                  const string &type,
                                  double price,
                                  double quantity) const
{
    // format qty and price
    std::ostringstream qty_ss;
    qty_ss << std::fixed << std::setprecision(8) << quantity;
    std::ostringstream price_ss;
    price_ss << std::fixed << std::setprecision(8) << price;
    string qty_str = qty_ss.str();
    string price_str = price_ss.str();

    // If LIMIT, perform safety checks:
    if (type == "LIMIT")
    {
        // 1) ensure pair has zero maker commission
        if (!is_zero_commission_pair(symbol))
        {
            log_message(string("[place_order] ABORT: makerCommission != 0 for ") + symbol);
            return string("");
        }

        // 2) check bookTicker to avoid immediate taker trades
        try
        {
            auto [bestBid, bestAsk] = get_book_ticker(symbol);
            if (bestBid == 0.0 && bestAsk == 0.0)
            {
                log_message("[place_order] Warning: empty bookTicker; aborting LIMIT placement for safety");
                return string("");
            }

            if (side == "BUY")
            {
                // to remain maker, buy price must be < bestAsk
                if (!(price < bestAsk))
                {
                    std::ostringstream oss;
                    oss << "[place_order] ABORT: BUY LIMIT price >= bestAsk (" << price << " >= " << bestAsk << "); would be taker";
                    log_message(oss.str());
                    return string("");
                }
            }
            else if (side == "SELL")
            {
                // to remain maker, sell price must be > bestBid
                if (!(price > bestBid))
                {
                    std::ostringstream oss;
                    oss << "[place_order] ABORT: SELL LIMIT price <= bestBid (" << price << " <= " << bestBid << "); would be taker";
                    log_message(oss.str());
                    return string("");
                }
            }
        }
        catch (const std::exception &e)
        {
            log_message(string("[place_order] pre-check failed: ") + e.what());
            return string("");
        }
    }

    // All checks passed (or non-LIMIT) -> call string overload with GTC for LIMIT
    string tif = "GTC";
    return place_order(symbol, side, type, price_str, qty_str, tif);
}

// ------------------------ get_order ------------------------
string BinanceClient::get_order(const string &symbol, long long order_id) const
{
    try
    {
        map<string, string> params;
        params["symbol"] = symbol;
        params["orderId"] = to_string(order_id);
        params["timestamp"] = now_timestamp_ms();
        string query = build_query_string(params);
        string sig = hmac_sha256_hex(secret_key_, query);
        string url = build_api_url(base_url_, string("v3/order?") + query + "&signature=" + sig);

        log_message(string("[get_order] url_preview: ") + (url.size() > 200 ? url.substr(0,200) + "..." : url));
        string resp = perform_request("GET", url, "", true);
        log_order_response(resp);
        return resp;
    }
    catch (const std::exception &e)
    {
        log_message(string("[get_order] error: ") + e.what());
        throw;
    }
}

// ------------------------ cancel_order (NEW) ------------------------
string BinanceClient::cancel_order(const string &symbol, long long order_id) const
{
    try
    {
        map<string, string> params;
        params["symbol"] = symbol;
        params["orderId"] = to_string(order_id);
        params["timestamp"] = now_timestamp_ms();
        string query = build_query_string(params);
        string sig = hmac_sha256_hex(secret_key_, query);
        string url = build_api_url(base_url_, string("v3/order?") + query + "&signature=" + sig);

        log_message(string("[cancel_order] url_preview: ") + (url.size() > 200 ? url.substr(0,200) + "..." : url));
        // use DELETE method per Binance API
        string resp = perform_request("DELETE", url, "", true);
        log_order_response(resp);
        return resp;
    }
    catch (const std::exception &e)
    {
        log_message(string("[cancel_order] error: ") + e.what());
        throw;
    }
}

// ------------------------ get_open_orders & poll_open_orders ------------------------
string BinanceClient::get_open_orders(const string &symbol) const
{
    try
    {
        map<string, string> params;
        params["symbol"] = symbol;
        params["timestamp"] = now_timestamp_ms();
        string query = build_query_string(params);
        string sig = hmac_sha256_hex(secret_key_, query);
        string url = build_api_url(base_url_, string("v3/openOrders?") + query + "&signature=" + sig);

        log_message(string("[get_open_orders] url_preview: ") + (url.size() > 200 ? url.substr(0,200) + "..." : url));
        string resp = perform_request("GET", url, "", true);
        log_message(string("[get_open_orders] response len=") + to_string(resp.size()));
        log_message(string("[get_open_orders] raw: ") + resp);
        return resp;
    }
    catch (const std::exception &e)
    {
        log_message(string("[get_open_orders] error: ") + e.what());
        throw;
    }
}

void BinanceClient::poll_open_orders(const string& symbol) const
{
    try
    {
        string resp = get_open_orders(symbol); // already logs raw

        auto parsed = json::parse(resp, nullptr, false);
        if (parsed.is_discarded())
        {
            log_message(string("[poll_open_orders] parse error or non-json response: ") + resp);
            return;
        }

        if (!parsed.is_array())
        {
            if (parsed.is_object() && parsed.contains("code") && parsed.contains("msg"))
            {
                std::ostringstream oss;
                oss << "[poll_open_orders] API error: code=" << parsed.value("code", 0)
                    << " msg=" << parsed.value("msg", string(""));
                log_message(oss.str());
            }
            else
            {
                log_message(string("[poll_open_orders] unexpected response (not array): ") + resp);
            }
            return;
        }

        if (parsed.empty())
        {
            log_message("[poll_open_orders] No open orders for symbol " + symbol);
            return;
        }

        log_message(string("[poll_open_orders] Found ") + to_string(parsed.size()) + " open orders for symbol " + symbol);

        for (const auto& order : parsed)
        {
            std::ostringstream oss;
            oss  << "[poll_open_orders] "
                 << "id="          << order.value("orderId", 0LL)
                 << " " << order.value("side", string(""))
                 << " " << order.value("type", string(""))
                 << " " << order.value("symbol", string(""))
                 << " price="      << order.value("price", string(""))
                 << " qty="        << order.value("origQty", string(""))
                 << " status="     << order.value("status", string(""))
                 << " exec="       << order.value("executedQty", string(""))
                 << " tif="        << order.value("timeInForce", string(""));
            log_message(oss.str());
        }
    }
    catch (const std::exception& e)
    {
        log_message(string("[poll_open_orders] error: ") + e.what());
    }
}

