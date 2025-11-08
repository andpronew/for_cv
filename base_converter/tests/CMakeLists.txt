cmake_minimum_required(VERSION 3.10)
project(base_converter CXX)

# Locate Google Test libraries. Ensure GTest is installed or find_package knows where to find it.
find_package(GTest REQUIRED)
include_directories(${GTEST_INCLUDE_DIRS})

# Build the test executable from the test source file
add_executable(BaseConverterTests my_test_converter.cpp)

# Link test executable with converter library and GoogleTest libraries
target_link_libraries(BaseConverterTests PRIVATE converter_lib ${GTEST_LIBRARIES} ${GTEST_MAIN_LIBRARIES})

# Alternatively, with CMake's imported targets (if available):
# target_link_libraries(BaseConverterTests PRIVATE converter_lib GTest::GTest GTest::Main)

# Register the test with CTest
add_test(NAME BaseConverterTests COMMAND BaseConverterTests)
