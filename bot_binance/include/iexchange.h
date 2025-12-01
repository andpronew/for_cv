#pragma once
#include <string>

using namespace std;

class IExchange
{
public:
    virtual ~IExchange() = default;
    virtual double get_price(const string& symbol) = 0;
    virtual void place_limit_order(const string& symbol,
                                   const string& side,
                                   double price,
                                   double quantity) = 0;
};
