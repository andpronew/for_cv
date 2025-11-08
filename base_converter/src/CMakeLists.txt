# Build the conversion logic into a static library
add_library(converter_lib STATIC my_converter.cpp)

# Ensure the library can find its header (my_converter.h is in the same directory)
target_include_directories(converter_lib PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

# Build the main application, link it with the conversion library
add_executable(BaseConverterApp main.cpp)
target_link_libraries(BaseConverterApp PRIVATE converter_lib)
