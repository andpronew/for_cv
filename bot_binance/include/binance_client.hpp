#pragma once

#include <string>
#include <map>
#include <utility>

struct TradeFee {
    double makerCommission{0.0};
    double takerCommission{0.0};
};

class BinanceClient {
public:
    // Конструкторы/деструктор
    BinanceClient(const std::string& api_key,
                  const std::string& secret_key,
                  const std::string& base_url);
    BinanceClient(const std::string& api_key,
                  const std::string& secret_key,
                  bool sandbox);
    ~BinanceClient();

    // --- Вспомогательные (как в вашем .cpp) ---
    std::string now_timestamp_ms() const;
    static std::string build_api_url(const std::string& base_url, const std::string& endpoint_tail);
    std::string build_query_string(const std::map<std::string, std::string>& params) const;
    std::string signed_query(const std::string& query) const;
    std::string perform_request(const std::string& method,
                                const std::string& url,
                                const std::string& post_fields,
                                bool use_api_key) const;

    // --- Маркет данные ---
    double get_price(const std::string& symbol) const;
    std::pair<double,double> get_book_ticker(const std::string& symbol) const;

    // --- Комиссии ---
    TradeFee get_trade_fee(const std::string& symbol) const;

    // --- Публичная проверка нулевой комиссии ---
    bool is_zero_commission_pair(const std::string& symbol) const;

    // --- Работа с ордерами (как в вашем .cpp) ---
    std::string place_order(const std::string& symbol,
                            const std::string& side,
                            const std::string& type,
                            const std::string& price,
                            const std::string& qty,
                            const std::string& time_in_force) const;

    std::string place_order(const std::string& symbol,
                            const std::string& side,
                            const std::string& type,
                            double price,
                            double quantity) const;

    std::string get_order(const std::string& symbol, long long order_id) const;
    std::string get_open_orders(const std::string& symbol) const;
    void        poll_open_orders(const std::string& symbol) const;

    // --- Новое: отмена ордера ---
    std::string cancel_order(const std::string& symbol, long long order_id) const;

private:
    // Ключи/база/флаг песочницы
    std::string api_key_;
    std::string secret_key_;
    std::string base_url_;
    bool        sandbox_{false};
};

