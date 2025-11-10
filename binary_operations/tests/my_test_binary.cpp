#include <gtest/gtest.h>
#include "../src/my_binary_operations.h"

// --- Addition Tests ---
TEST (BinaryOperationsTest, SimpleAddition)
{
    EXPECT_EQ(sum_and_carry("0001", "0010", '+'), "0011");
    EXPECT_EQ(sum_and_carry("0101", "0011", '+'), "1000");
}
TEST(BinaryOperationsTest, AdditionWithCarry)
{
    EXPECT_EQ(sum_and_carry("1111", "0001", '+'), "0000"); // 15 + 1 = 0 (carry ignored)
}

// --- Subtraction Tests ---
TEST(BinaryOperationsTest, SimpleSubtraction)
{
    EXPECT_EQ(sum_and_carry("0101", "0011", '-'), "0010"); // 5 - 3 = 2
    EXPECT_EQ(sum_and_carry("11111", "01010", '-'), "10101"); // -1 - 10 = -11
}
TEST(BinaryOperationsTest, BorrowPropagation)
{
    EXPECT_EQ(sum_and_carry("1000", "0001", '-'), "0111"); // 8 - 1 = 7
}

// --- Edge Case Tests ---
TEST(BinaryOperationsTest, EqualInputs)
{
    EXPECT_EQ(sum_and_carry("1010", "1010", '-'), "0000"); // same inputs → 0
}
TEST(BinaryOperationsTest, LengthMismatch)
{
    testing::internal::CaptureStderr();
    std::string result = sum_and_carry("101", "1100", '+');
    std::string err = testing::internal::GetCapturedStderr();

    EXPECT_TRUE(result.empty());
    EXPECT_NE(err.find("Error"), std::string::npos);
}
