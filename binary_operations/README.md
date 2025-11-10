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

```
✅ Unit Test Output:
./BaseConverterTests
Running main() from ./googletest/src/gtest_main.cc
[==========] Running 5 tests from 5 test suites.
[----------] Global test environment set-up.
[----------] 1 test from CharToValTest
[ RUN      ] CharToValTest.HandlesDigitsAndLetters
[       OK ] CharToValTest.HandlesDigitsAndLetters (0 ms)
[----------] 1 test from CharToValTest (0 ms total)

[----------] 1 test from ValToCharTest
[ RUN      ] ValToCharTest.HandleValues
[       OK ] ValToCharTest.HandleValues (0 ms)
[----------] 1 test from ValToCharTest (0 ms total)

[----------] 1 test from ToDecimalTest
[ RUN      ] ToDecimalTest.ConvertsToDecimalCorrectly
[       OK ] ToDecimalTest.ConvertsToDecimalCorrectly (0 ms)
[----------] 1 test from ToDecimalTest (0 ms total)

[----------] 1 test from FromDecimalTest
[ RUN      ] FromDecimalTest.ConvertsFromDecimalCorrectly
[       OK ] FromDecimalTest.ConvertsFromDecimalCorrectly (0 ms)
[----------] 1 test from FromDecimalTest (0 ms total)

[----------] 1 test from RoundTripTest
[ RUN      ] RoundTripTest.BaseToBaseRoundTrip
[       OK ] RoundTripTest.BaseToBaseRoundTrip (0 ms)
[----------] 1 test from RoundTripTest (0 ms total)

[----------] Global test environment tear-down
[==========] 5 tests from 5 test suites ran. (0 ms total)
[  PASSED  ] 5 tests.
```

