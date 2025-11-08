#include <gtest/gtest.h>
#include "my_converter.h"

// Test char_to_val with numeric characters and letters (case-insensitive).
TEST(CharToValTest, HandlesDigitsAndLetters)
{
    EXPECT_EQ(char_to_val('0'), 0);
    EXPECT_EQ(char_to_val('5'), 5);
    EXPECT_EQ(char_to_val('7'), 7);
    EXPECT_EQ(char_to_val('9'), 9);
    EXPECT_EQ(char_to_val('A'), 10);
    EXPECT_EQ(char_to_val('F'), 15);
    EXPECT_EQ(char_to_val('b'), 11); // lowercase should also work
    EXPECT_EQ(char_to_val('c'), 12);
}

// Test val_to_char for values 0-15.
TEST(ValToCharTest, HandleValues)
{
    EXPECT_EQ(val_to_char(0), '0');
    EXPECT_EQ(val_to_char(7), '7');
    EXPECT_EQ(val_to_char(10), 'A');
    EXPECT_EQ(val_to_char(12), 'C');
    EXPECT_EQ(val_to_char(15), 'F');
    EXPECT_EQ(val_to_char(11), 'B');
    EXPECT_EQ(val_to_char(14), 'E');
}

// Test converting from various bases to decimal.
TEST(ToDecimalTest, ConvertsToDecimalCorrectly)
{
    EXPECT_EQ(to_decimal("0", 2), 0);
    EXPECT_EQ(to_decimal("1", 2), 1);
    EXPECT_EQ(to_decimal("101", 2), 5);
    EXPECT_EQ(to_decimal("21", 3), 7);
    EXPECT_EQ(to_decimal("A", 11), 10);
    EXPECT_EQ(to_decimal("1F", 16), 31);
    EXPECT_EQ(to_decimal("10", 10), 10);
    EXPECT_EQ(to_decimal("11", 8), 9);
}

// Test converting from decimal to various bases.
TEST(FromDecimalTest, ConvertsFromDecimalCorrectly)
{
    EXPECT_EQ(from_decimal(0, 2), "0");
    EXPECT_EQ(from_decimal(5, 2), "101");
    EXPECT_EQ(from_decimal(7, 3), "21");
    EXPECT_EQ(from_decimal(10, 11), "A");
    EXPECT_EQ(from_decimal(31, 16), "1F");
    EXPECT_EQ(from_decimal(11, 8), "13");
    EXPECT_EQ(from_decimal(255, 16), "FF");
    EXPECT_EQ(from_decimal(10, 10), "10");
}
// Test round-trip conversions (to_decimal then from_decimal should give original for valid inputs).
TEST(RoundTripTest, BaseToBaseRoundTrip)
{
    // We use uppercase in expected results because converter functions produce uppercase for A-F.
    EXPECT_EQ(from_decimal(to_decimal("101", 2), 2), "101");
    EXPECT_EQ(from_decimal(to_decimal("abc", 16), 16), "ABC");
    EXPECT_EQ(from_decimal(to_decimal("FF", 16), 8), "377");
    EXPECT_EQ(from_decimal(to_decimal("377", 8), 16), "FF");
    EXPECT_EQ(from_decimal(to_decimal("26", 10), 10), "26");
}

// (Optional) It can be added more tests for invalid inputs, but since the functions call exit on invalid input,
// such tests would terminate the test runner. Those cases are better tested via integration or by modifying the functions to throw exceptions instead of exiting.
