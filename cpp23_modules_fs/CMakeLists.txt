cmake_minimum_required(VERSION 3.31)
project(hello_world)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED 23)

#set(CMAKE_CXX_SCAN_FOR_MODULES ON)

add_executable(hello)

target_sources(hello PRIVATE
    main.cpp
PRIVATE
FILE_SET CXX_MODULES
FILES
    my_module.cpp
)