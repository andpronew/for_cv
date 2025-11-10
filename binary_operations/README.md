# 🧮 Binary Operations Library (C++)

A simple C++ project that demonstrates **manual binary addition and subtraction**
using **two’s complement arithmetic** — exactly how real CPU ALUs perform it.

### 📁 Project Structure
```text
binary_operations/
├── CMakeLists.txt
├── src/
│ ├── my_binary_operations.cpp
│ ├── my_binary_operations.h
│ ├── main.cpp
│ └── CMakeLists.txt
├── tests/
│ ├── my_test_binary.cpp
│ └── CMakeLists.txt
└── external/googletest/
└── README.md
```

- **`my_binary_operations.cpp/.h`** — core logic for addition and subtraction  
- **`main.cpp`** — CLI to test operations manually  
- **`my_test_binary.cpp`** — unit tests (GoogleTest)  

---

## ⚙️ Build Instructions

### 1. Clone repository
```bash
git clone https://github.com/yourname/binary_operations.git
cd binary_operations
```

### 2. Get GoogleTest (if not included)

git clone https://github.com/google/googletest.git external/googletest

### 3. Build project and tests
```
mkdir build && cd build
cmake ..
make
```
### Run demo
~/binary_operations/build/src$ ./my_binary

### Run unit tests
ctest --output-on-failure

~/binary_operations/build/tests$ ./my_binary_tests

### 🎯 Demo Output (with explanations):
```
./my_binary
Enter 2 binary figures and operation ('+' or '-'): 11011 11111 -
Sum: 11100
Result: 11100
./my_binary
Enter 2 binary figures and operation ('+' or '-'): 11011 11111 +
Sum: 11010
Carry: 1
Result: 11010

```
```
✅ Unit Test Output:
./my_binary_tests
Running main() from ./googletest/src/gtest_main.cc
[==========] Running 6 tests from 1 test suite.
[----------] Global test environment set-up.
[----------] 6 tests from BinaryOperationsTest
[ RUN      ] BinaryOperationsTest.SimpleAddition
Sum: 0011
Sum: 1000
[       OK ] BinaryOperationsTest.SimpleAddition (0 ms)
[ RUN      ] BinaryOperationsTest.AdditionWithCarry
Sum: 0000
Carry: 1
[       OK ] BinaryOperationsTest.AdditionWithCarry (0 ms)
[ RUN      ] BinaryOperationsTest.SimpleSubtraction
Sum: 0010
Carry: 1
Sum: 10101
Carry: 1
[       OK ] BinaryOperationsTest.SimpleSubtraction (0 ms)
[ RUN      ] BinaryOperationsTest.BorrowPropagation
Sum: 0111
Carry: 1
[       OK ] BinaryOperationsTest.BorrowPropagation (0 ms)
[ RUN      ] BinaryOperationsTest.EqualInputs
Sum: 0000
Carry: 1
[       OK ] BinaryOperationsTest.EqualInputs (0 ms)
[ RUN      ] BinaryOperationsTest.LengthMismatch
[       OK ] BinaryOperationsTest.LengthMismatch (0 ms)
[----------] 6 tests from BinaryOperationsTest (0 ms total)

[----------] Global test environment tear-down
[==========] 6 tests from 1 test suite ran. (0 ms total)
[  PASSED  ] 6 tests.
```
