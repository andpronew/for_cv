#include "my_binary_operations.h"
#include <iostream>
#include <vector>
#include <string>
#include <algorithm>
int main()
{
    std::string sa, sb;
    char op{};
    std::cout << "Enter 2 binary figures and operation ('+' or '-'): ";
    if (!(std::cin >> sa >> sb >> op))
        return 1;

    std::string result = sum_and_carry(sa, sb, op);
    if (result.empty() && sa.size() != sb.size())
        return 1;

    std::cout << "Result: " << result << std::endl;
    return 0;
}