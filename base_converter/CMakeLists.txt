cmake_minimum_required(VERSION 3.10)
project(BaseConverter CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED YES)

enable_testing()  # Enable CTest framework

add_subdirectory(src)
add_subdirectory(tests)

