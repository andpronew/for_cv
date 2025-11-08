/*Here’s a C++ program that converts a number from an arbitrary base b1 to another base b2
(up to base 16).Digits above 9 are represented using letters A–F.
Example run :
Enter base of input
number(b1) : 16 Enter base to convert to(b2) : 2 Enter number in base 16 : 1F Equivalent number in base 2 : 11111
*/

#include "my_converter.h"
#include <string>
#include <algorithm>
#include <cctype>
#include <iostream> // for error output

// Convert a single character to its numeric value
int char_to_val(char c)
{
    if (std::isdigit(static_cast<unsigned char>(c)))
    {
        return c - '0';
    }
    else
    {
        // Convert letter to uppercase and then to value 10-15
        return std::toupper(static_cast<unsigned char>(c)) - 'A' + 10;
    }
}
char val_to_char(int val)
{
    if (val < 10)
    {
        return '0' + val;
    }
    else
    {
        return 'A' + (val - 10);
    }
}

long long to_decimal(const std::string &num, int b1)
{
    long long value = 0;
    for (char c : num)
    {
        int digit = char_to_val(c);
        if (digit >= b1)
        {
            std::cerr << "Invalid digit '" << c << "' for base " << b1 << std::endl;
            std::exit(1);
        }
        value = value * b1 + digit;
    }
    return value;
}

std::string from_decimal(long long decimal, int b2)
{
    if (decimal == 0)
    {
        return "0";
    }
    std::string result;
    while (decimal > 0)
    {
        int digit = decimal % b2;
        result.push_back(val_to_char(digit));
        decimal /= b2;
    }
    std::reverse(result.begin(), result.end());
    return result;
}
