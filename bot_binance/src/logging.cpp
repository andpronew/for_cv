// src/logging.cpp
#include "logging.h"
#include <cstdio>
#include <ctime>
#include <mutex>
#include <string>
#include <unistd.h>   // fileno, fsync
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <nlohmann/json.hpp>
#include <iostream>

using json = nlohmann::json;

static FILE *g_log_fp = nullptr;
static std::mutex g_log_mutex;
static std::string g_log_path = "";

static std::string format_time_from_epoch_ms(long long ms)
{
    time_t sec = (time_t)(ms / 1000);
    struct tm tm_buf;
    localtime_r(&sec, &tm_buf);
    char buf[64];
    strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", &tm_buf);
    return std::string(buf);
}

void init_logger(const std::string &path)
{
    std::lock_guard<std::mutex> lock(g_log_mutex);
    if (g_log_fp) return;
    g_log_path = path.empty() ? "./bot_output.txt" : path;
    // ensure directory exists is left to user (we don't create dirs here)
    g_log_fp = fopen(g_log_path.c_str(), "a");
    if (!g_log_fp)
    {
        // best-effort: fallback to stdout
        std::cerr << "[logging] Failed to open log file: " << g_log_path << ", logging to stdout only\n";
        g_log_fp = nullptr;
        return;
    }
    setvbuf(g_log_fp, nullptr, _IOLBF, 0); // line buffering
    time_t t = time(nullptr);
    char ts[64];
    strftime(ts, sizeof(ts), "%Y-%m-%d %H:%M:%S", localtime(&t));
    fprintf(g_log_fp, "=== Andr bot log started at %s ===\n", ts);
    fprintf(g_log_fp,
            "%-19s | %-5s | %-6s | %-8s | %-4s | %-14s | %-11s | %-12s | %-7s | %s\n",
            "timestamp", "LEVEL", "ACTION", "SYMBOL", "SIDE", "PRICE", "QTY", "ORDERID", "STATUS", "clientOrderId");
    fflush(g_log_fp);
    fsync(fileno(g_log_fp));
}

void log_message(const std::string &msg)
{
    std::lock_guard<std::mutex> lock(g_log_mutex);
    if (!g_log_fp)
    {
        // lazy init with default path
        init_logger(g_log_path.empty() ? "./bot_output.txt" : g_log_path);
    }

    // timestamp
    time_t t = time(nullptr);
    char ts[64];
    strftime(ts, sizeof(ts), "%Y-%m-%d %H:%M:%S", localtime(&t));
    std::string line = std::string(ts) + " | INFO  | MSG    | " + msg;

    // print to console
    std::cout << line << std::endl;

    // append to file if possible
    if (g_log_fp)
    {
        fprintf(g_log_fp, "%s\n", line.c_str());
        fflush(g_log_fp);
        fsync(fileno(g_log_fp));
    }
}

void log_order_response(const std::string &response_json)
{
    std::lock_guard<std::mutex> lock(g_log_mutex);
    if (!g_log_fp)
    {
        init_logger(g_log_path.empty() ? "./bot_output.txt" : g_log_path);
    }

    // parse JSON safely
    try
    {
        if (response_json.empty())
        {
            // write empty raw
            time_t t = time(nullptr);
            char ts[64];
            strftime(ts, sizeof(ts), "%Y-%m-%d %H:%M:%S", localtime(&t));
            std::string line = std::string(ts) + " | ERR   | PARSE  | [empty response]";
            std::cout << line << std::endl;
            if (g_log_fp)
            {
                fprintf(g_log_fp, "%s\nRAW: \n", line.c_str());
                fflush(g_log_fp);
                fsync(fileno(g_log_fp));
            }
            return;
        }

        auto j = json::parse(response_json);

        long long transact = 0;
        if (j.contains("transactTime"))
        {
            try { transact = j["transactTime"].get<long long>(); } catch (...) { transact = 0; }
        }
        std::string ts = transact ? format_time_from_epoch_ms(transact) : format_time_from_epoch_ms(time(nullptr) * 1000);

        std::string symbol = j.value("symbol", "");
        std::string side = j.value("side", "");
        std::string price = j.value("price", "");
        std::string origQty = j.value("origQty", "");
        std::string orderId = j.contains("orderId") ? std::to_string(j["orderId"].get<long long>()) : "";
        std::string status = j.value("status", "");
        std::string clientOrderId = j.value("clientOrderId", "");

        // formatted line
        char linebuf[512];
        snprintf(linebuf, sizeof(linebuf), "%-19s | %-5s | %-6s | %-8s | %-4s | %-14s | %-11s | %-12s | %-7s | %s",
                 ts.c_str(), "INFO", "ORDER", symbol.c_str(), side.c_str(), price.c_str(), origQty.c_str(), orderId.c_str(), status.c_str(), clientOrderId.c_str());

        std::string line(linebuf);

        // console
        std::cout << line << std::endl;
        std::cout << "RAW: " << response_json << std::endl;

        // file
        if (g_log_fp)
        {
            fprintf(g_log_fp, "%s\n", line.c_str());
            fprintf(g_log_fp, "RAW: %s\n", response_json.c_str());
            fflush(g_log_fp);
            fsync(fileno(g_log_fp));
        }
    }
    catch (const std::exception &e)
    {
        time_t t = time(nullptr);
        char ts[64];
        strftime(ts, sizeof(ts), "%Y-%m-%d %H:%M:%S", localtime(&t));
        char linebuf[256];
        snprintf(linebuf, sizeof(linebuf), "%-19s | %-5s | %-6s | %s", ts, "ERR", "PARSE", e.what());
        std::string line(linebuf);
        std::cout << line << std::endl;
        std::cout << "RAW: " << response_json << std::endl;
        if (g_log_fp)
        {
            fprintf(g_log_fp, "%s\n", line.c_str());
            fprintf(g_log_fp, "RAW: %s\n", response_json.c_str());
            fflush(g_log_fp);
            fsync(fileno(g_log_fp));
        }
    }
}

