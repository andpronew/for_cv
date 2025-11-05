Smart Pointers Library (UniquePtr, SharedPtr, WeakPtr)

This project is a clean, educational C++ implementation of three core smart pointer types:

UniquePtr – exclusive ownership model (like std::unique_ptr)

SharedPtr – reference-counted shared ownership (like std::shared_ptr)

WeakPtr – non-owning observer of a SharedPtr (like std::weak_ptr)

✨ Features

✅ Header-only design with inline documentation

✅ Thread-safe reference counting using std::atomic

✅ Move semantics and copy control

✅ Fully tested with GoogleTest

✅ Clean separation of demo and test code

✅ C++17 compliant

📁 Project Structure
smart_ptr_project/
├── include/
│   ├── my_unique_ptr.hpp       # UniquePtr implementation
│   ├── my_shared_ptr.hpp       # SharedPtr implementation
│   └── my_weak_ptr.hpp         # WeakPtr implementation
├── src/
│   └── my_pointers.cpp         # Demonstration of pointer usage
├── tests/
│   └── smart_ptrs_test.cpp     # Unit tests with GoogleTest
├── CMakeLists.txt
└── README.md

🛠️ Build Instructions

Dependencies:

CMake ≥ 3.10

A C++17-compliant compiler (GCC/Clang/MSVC)

GoogleTest
 installed or added as a submodule

🧪 Build & Run
# Clone the repository and navigate into it
git clone https://github.com/andpronew/for_cv.git
cd for_cv

# Build project and tests
mkdir build && cd build
cmake ..
make

# Run demo
./smart_ptr

# Run unit tests
./test_ptr

🎯 Demo Output
--- UniquePtr Demo ---
Test(1) constructed
Hello from Test(1)
u1 is now nullptr after move

--- SharedPtr & WeakPtr Demo ---
Test(2) constructed
Hello from Test(2)
Ref count: 2
Hello from Test(2)
Ref count after s2 destroyed: 1
Test(2) destroyed
Test(1) destroyed

✅ Unit Test Output
[==========] Running 6 tests from 3 test suites.
[----------] Global test environment set-up.
[----------] 2 tests from UniquePtrTest
[ RUN      ] UniquePtrTest.BasicOperations
[       OK ] UniquePtrTest.BasicOperations (0 ms)
[ RUN      ] UniquePtrTest.MoveOwnership
[       OK ] UniquePtrTest.MoveOwnership (0 ms)

[----------] 2 tests from SharedPtrTest
[ RUN      ] SharedPtrTest.BasicReferenceCounting
[       OK ] SharedPtrTest.BasicReferenceCounting (0 ms)
[ RUN      ] SharedPtrTest.MoveSemantics
[       OK ] SharedPtrTest.MoveSemantics (0 ms)

[----------] 2 tests from WeakPtrTest
[ RUN      ] WeakPtrTest.LockAccess
[       OK ] WeakPtrTest.LockAccess (0 ms)
[ RUN      ] WeakPtrTest.LockOnExpired
[       OK ] WeakPtrTest.LockOnExpired (0 ms)

[  PASSED  ] 6 tests.

📚 Learning Goals

This implementation demonstrates:

Ownership semantics in C++

RAII (Resource Acquisition Is Initialization)

Manual memory management

Atomic reference counting

Clean API design and move-only/copyable object behavior
