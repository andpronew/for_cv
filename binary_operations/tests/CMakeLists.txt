cmake_minimum_required(VERSION 3.10)
project(my_binary CXX)

# Locate Google Test libraries. Ensure GTest is installed or find_package knows where to find it.
find_package(GTest REQUIRED)
include_directories(${GTEST_INCLUDE_DIRS})

# Build the test executable from the test source file
add_executable(my_binary_tests my_test_binary.cpp)

# Link test executable with converter library and GoogleTest libraries
target_link_libraries(my_binary_tests PRIVATE binary_lib ${GTEST_LIBRARIES} ${GTEST_MAIN_LIBRARIES})

# Alternatively, with CMake's imported targets (if available):
# target_link_libraries(my_binary_tests PRIVATE binary_lib GTest::GTest GTest::Main)

# Register the test with CTest
add_test(NAME my_binary_tests COMMAND my_binary_tests)
