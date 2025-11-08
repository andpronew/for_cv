#include <iostream>
#include <string>
#include "my_converter.h"

int main()
{
    int b1{}, b2{};
    std::string number;

    std::cout << "Enter base of input number (b1): ";
    if(!(std::cin >> b1))
    {
        return 1; // Handle EOF or non-integer input
    }
    
    std::cout << "Enter base to convert to (b2): ";
    if (!(std::cin >> b2))
    {
        return 1; // Handle EOF or non-integer input
    }

    std::cout << "Enter number in base " << b1 << ": " << std::endl;
    if (!(std::cin >> number))
    {
        return 1; // Handle EOF or non-integer input
    }

    if (b1 < 2 || b1 > 16 || b2 < 2 || b2 > 16)
    {
        std::cerr << "Bases must be 2-16" << std::endl;
        return 1;
    }

    // Perform conversion
    long long decimal_value = to_decimal(number, b1);
    std::string converted = from_decimal(decimal_value, b2);

    std::cout << "Equivalent number in base " << b2 << ": " << converted << std::endl;

    return 0;
}