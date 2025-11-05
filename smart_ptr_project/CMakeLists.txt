cmake_minimum_required(VERSION 3.10)
project(LuhnValidator CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Main executable
add_executable(smart_ptr_project src/my_pointers.cpp)
target_include_directories(smart_ptr_project PRIVATE include)

cmake_minimum_required(VERSION 3.10)
project(LuhnValidator CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Main executable
add_executable(smart_ptr src/my_pointers.cpp)
target_include_directories(smart_ptr PRIVATE include)

# GoogleTest
enable_testing()
find_package(GTest REQUIRED)
include(GoogleTest)

add_executable(test_ptr tests/smart_ptrs_test.cpp)
target_include_directories(test_ptr PRIVATE include)
target_link_libraries(test_ptr GTest::gtest_main)

gtest_discover_tests(test_ptr)

