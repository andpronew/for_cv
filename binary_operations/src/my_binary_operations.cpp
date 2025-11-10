#include "my_binary_operations.h"
#include <iostream>
#include <vector>
#include <string>
#include <algorithm>

void add_one(std::string &bin)
{
    int carry = 1;
    for (int i = bin.size() - 1; i >= 0 && carry; --i)
    {
        if (bin[i] == '0')
        {
            bin[i] = '1';
            carry = 0;
        }
        else
        {
            bin[i] = '0';
        }
    }
}

std::string sum_and_carry(std::string sa, std::string sb, char op)
{
    if (sa.size() != sb.size())
    {
        std::cerr << "Error: binary strings must have equal length." << std::endl;
        return std::string();
    }

    int carry{};
    int n = sa.size();

    if (op == '-')
    {
        for (char &bit : sb)
            bit = (bit == '1') ? '0' : '1';

        add_one(sb);
        carry = 0;
    }

    std::string sum(n, '0');
    for (int i = static_cast<int>(n) - 1; i >= 0; --i)
    {
        int bit_a = (sa[i] == '1');
        int bit_b = (sb[i] == '1');
        int s = bit_a ^ bit_b ^ carry;
        carry = (bit_a & bit_b) | (bit_a & carry) | (bit_b & carry);
        sum[i] = s ? '1' : '0';
    }
    std::cout << "Sum: " << sum << std::endl;

    if (carry)
        std::cout << "Carry: " << carry << std::endl;

    return sum;
}