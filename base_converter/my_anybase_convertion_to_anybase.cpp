/*Here’s a C++ program that converts a number from an arbitrary base b1 to another base b2
(up to base 16).Digits above 9 are represented using letters A–F.
Example run :
Enter base of input
number(b1) : 16 Enter base to convert to(b2) : 2 Enter number in base 16 : 1F Equivalent number in base 2 : 11111
*/
#include <iostream>
#include <string>
#include <algorithm>
#include <cctype>

    // Convert a single character to its numeric value
    int char_to_val(char c)
{
    if(isdigit(c))
        return c - '0';
    else
        return toupper(c) - 'A' + 10; 
}

// Convert a numeric value to its character representation
char val_to_char(int val)
{
    if (val<10)
        return '0' + val;
    else
        return 'A' + (val - 10);
}
// Convert from base b1 to decimal
long long to_decimal(const std::string &num, int b1)
{
    long long value{};
    for(char c : num)
    {
        int digit = char_to_val(c);
        if (digit >= b1)
        {
            std::cerr << "Invalid digit '" << c << "' for base " << b1 << std::endl;
            exit(1);
        }
        value = value * b1 + digit;
    }
    return value;
}
// Convert from decimal to base b2
std::string from_decimal(long long decimal, int b2)
{
    if (decimal == 0)
        return "0";
    std::string result;
    while (decimal>0)
    {
        int digit = decimal % b2;
        result.push_back(val_to_char(digit));
        decimal /= b2;
    }
    reverse(result.begin(), result.end());
    return result;
}

int main()
{
    int b1, b2;
    std::string number;
    std::cout << "Enter base of input number (b1): ";
    std::cin >> b1;
    std::cout << "Enter base to convert to (b2): ";
    std::cin >> b2;

    std::cout << "Enter number in base " << b1 << ": " ;
    std::cin >> number;

    if (b1<2 || b1>16 || b2 <2 || b2>16)
    {
        std::cerr << "Bases must be 2-16" << std::endl;
        return 1;
    }

    long long decimal_value = to_decimal(number, b1);
    std::string converted = from_decimal(decimal_value, b2);

    std::cout << "Equivalent number in base " << b2 << ": " << converted << std::endl;

    return 0;
}