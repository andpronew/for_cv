cmake_minimum_required(VERSION 3.16)
project(AndrBot LANGUAGES CXX)

# --- стандарт C++ ---
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# --- пути include ---
include_directories(
    ${PROJECT_SOURCE_DIR}/include
)

# --- исходники ---
file(GLOB SRC_FILES
    ${PROJECT_SOURCE_DIR}/src/*.cpp
    ${PROJECT_SOURCE_DIR}/strategies/*.cpp
)

# Ensure ladder strategy is included explicitly (prevents glob/cmake-caching issues)
list(APPEND SRC_FILES ${PROJECT_SOURCE_DIR}/src/ladder_strategy.cpp)

# Ensure logging is included explicitly
list(APPEND SRC_FILES ${PROJECT_SOURCE_DIR}/src/logging.cpp)

# main + все найденные исходники
add_executable(bot
    ${PROJECT_SOURCE_DIR}/main.cpp
    ${SRC_FILES}
)

# --- потоки ---
find_package(Threads REQUIRED)
target_link_libraries(bot PRIVATE Threads::Threads)

# --- libcurl (robust handling: use target if available, otherwise variables) ---
find_package(CURL REQUIRED)

if (TARGET CURL::libcurl)
    target_link_libraries(bot PRIVATE CURL::libcurl)
else()
    if (DEFINED CURL_LIBRARIES)
        target_link_libraries(bot PRIVATE ${CURL_LIBRARIES})
    endif()
    if (DEFINED CURL_INCLUDE_DIRS)
        target_include_directories(bot PRIVATE ${CURL_INCLUDE_DIRS})
    endif()
endif()

# --- OpenSSL (HMAC) ---
find_package(OpenSSL REQUIRED)
if (TARGET OpenSSL::Crypto)
    target_link_libraries(bot PRIVATE OpenSSL::Crypto)
else()
    # fallback: older CMake may provide OpenSSL::SSL / OpenSSL::Crypto names differently
    if (TARGET OpenSSL::SSL)
        target_link_libraries(bot PRIVATE OpenSSL::SSL)
    else()
        target_link_libraries(bot PRIVATE ${OPENSSL_LIBRARIES})
        target_include_directories(bot PRIVATE ${OPENSSL_INCLUDE_DIR})
    endif()
endif()

# --- nlohmann::json (header-only) ---
find_package(nlohmann_json QUIET)
if (TARGET nlohmann_json::nlohmann_json)
    target_link_libraries(bot PRIVATE nlohmann_json::nlohmann_json)
else()
    message(WARNING "nlohmann_json not found via find_package — ensure single-header json.hpp is available in include/ or install nlohmann_json.")
endif()

# --- опции компилятора ---
if (MSVC)
    target_compile_options(bot PRIVATE /W4 /permissive-)
else()
    target_compile_options(bot PRIVATE -Wall -Wextra -pedantic)
endif()

# Allow suppressing one noisy deprecated warning if needed
target_compile_options(bot PRIVATE -Wno-deprecated-declarations)

# --- дополнительные свойства/диагностика ---
# Установим RPATH на linux для удобства (опционально)
if(UNIX AND NOT APPLE)
    set_target_properties(bot PROPERTIES BUILD_WITH_INSTALL_RPATH TRUE)
endif()

# --- подсказки для пользователя ---
message(STATUS "Project source dir: ${PROJECT_SOURCE_DIR}")
message(STATUS "CMake generator: ${CMAKE_GENERATOR}")
message(STATUS "C++ standard: ${CMAKE_CXX_STANDARD}")

