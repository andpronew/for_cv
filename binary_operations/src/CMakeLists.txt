# Build the conversion logic into a static library
add_library(binary_lib STATIC my_binary_operations.cpp)

# Ensure the library can find its header (my_converter.h is in the same directory)
target_include_directories(binary_lib PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

# Build the main application, link it with the conversion library
add_executable(my_binary main.cpp)
target_link_libraries(my_binary PRIVATE binary_lib)
