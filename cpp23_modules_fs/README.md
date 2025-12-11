# *Base Converter Project with CMake and GoogleTest*

**Goal**: The program converts numbers from an input base b1 to an output base b2 (with bases between 2 and 16 including). Digits above 9 are represented by letters A–F

**Skills**: to learn deeply aconversion between different base numbers

**Description**: The code provides functions to map characters to values and vice versa, convert an input string in base b1 to a decimal value, and convert a decimal value to a string in base b2


### 📁 Project Structure
```text
base_converter/
├── CMakeLists.txt
├── src/
│   ├── my_converter.h
│   ├── my_converter.cpp
│   └── main.cpp
└── tests/
|   └── my_test_conversion.cpp
└── README.md
```

### Build project and tests
```
mkdir build && cd build
cmake ..
make
```
### Run demo
~/base_converter/build/src$ ./BaseConverterApp

### Run unit tests
./BaseConverterTests

### 🎯 Demo Output (with explanations):
```
Enter base of input number (b1): 2
Enter base to convert to (b2): 10
Enter number in base 2: 101
Equivalent number in base 10: 5
```


# 📘 *Bulk Rename Utility — Modern C++23 + Modules + CMake 3.31*

A minimal but fully functional example of C++23 Modules, Clang 18, and modern CMake (3.31) using FILE_SET CXX_MODULES.
The project implements a simple bulk file renamer using std::filesystem, demonstrating clean modular design and modern C++ tooling.

### 🚀 Features

C++23 Modules (export module, import my_module)

Modern CMake 3.31 with FILE_SET CXX_MODULES

Clang 18 toolchain configuration

Uses std::filesystem for directory traversal and safe file operations

Demonstrates exception handling and input validation

Minimal and clean structure — great as a template for future module-based projects

### 📂 Project Structure
```text
/
├── CMakeLists.txt
├── main.cpp
└── my_module.cpp   (C++23 module)
```

### 🧠 What the Program Does

The application:

Asks the user for a directory path

Iterates over all regular files inside

Renames each file to:

<old_stem>_hi_Mike_<old_extension>


Example:

photo.jpg → photo_hi_Mike_.jpg


This demonstrates real-world usage of:

std::filesystem::directory_iterator

std::filesystem::path

Safe renaming with fs::rename()

Module export/import patterns

### 🛠️ Building the Project

Requires:

Clang 18

CMake 3.31+

Ninja 1.11+

Build commands
rm -rf build
cmake -B build -G Ninja \
    -DCMAKE_CXX_COMPILER=clang++-18 \
    -DCMAKE_C_COMPILER=clang-18
cmake --build build

### ▶️ Running the Program
./build/hello
Enter a path: files


Use either:

absolute paths (/home/.../files)

or run the program from the directory containing your folder.

### 🧩 Code Example (Module Overview)
export module my_module;

export namespace my_module
{
    void bulk_rename(const std::string& path_str);
}


And its implementation uses modern C++23 and std::filesystem to safely rename files.

### 🎯 Purpose of the Project

This small project serves as:

A practical demonstration of C++23 module compilation

A reference for configuring CMake 3.31 + Clang 18

A minimal template for filesystem utilities

A portfolio-ready example showing modern C++ skills:

Competence with very recent C++ standards

Ability to configure toolchains manually

Understanding of modular design practices

Correct and safe usage of the filesystem API
