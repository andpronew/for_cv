#ifndef CONVERTER_H
#define CONVERTER_H

#include <string>

// Convert a single character (0-9, A-F) to its numeric value.
int char_to_val(char c);

// Convert a numeric value (0-15) to its character representation (0-9, A-F).
char val_to_char(int val);

// Convert a number (as string in base b1) to decimal (base 10) as a long long.
long long to_decimal(const std::string &num, int b1);

// Convert a decimal number (given as long long) to a target base b2, as a string.
std::string from_decimal(long long decimal, int b2);

#endif // CONVERTER_H