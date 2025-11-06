**Smart Pointers Library** (UniquePtr, SharedPtr, WeakPtr)

Goal: Show understanding of memory management and RAII.
Skills: new/delete, ownership semantics, move constructors, destructors.
Description: Recreated std::unique_ptr, std::shared_ptr, std::weak_ptr with basic reference counting. Included safety checks, copy/move semantics, and optional debug logs, as well as thread-safe ref counting.

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

🎯 Demo Output (with explanations):
--- UniquePtr Demo ---
Test(1) constructed:
The program creates a UniquePtr<Test> using new Test(1). The Test constructor logs this message.
Hello from Test(1):
Call a method like u1->greet() or (*u1).greet(), which prints the message inside Test::greet().
u1 is now nullptr after move:
Move u1 into another UniquePtr (e.g., u2 = std::move(u1)).
u1 becomes null (ptr = nullptr):
Check if (!u1) and print this message.

This proves UniquePtr correctly releases ownership on move.

--- SharedPtr & WeakPtr Demo ---
Test(2) constructed:
Create a SharedPtr<Test> s1(new Test(2)). The constructor logs this message.
Hello from Test(2):
Call a method like s1->greet() or (*s1).greet().
Ref count: 2:
Assign SharedPtr<Test> s2 = s1;, increasing the reference count to 2. Then call s1.use_count() or s2.use_count() to print this.
Hello from Test(2):
Call s2->greet() (same object as s1, just a new reference).
Ref count after s2 destroyed: 1:
s2 goes out of scope.
Reference count decreases to 1:
Print this with s1.use_count().
Test(2) destroyed:
s1 goes out of scope, Ref count reaches 0, custom SharedPtr::release() deletes the object, destructor logs this message.
Test(1) destroyed:
This is from the earlier UniquePtr<Test> u2 going out of scope at the very end of main() (after u1 was moved into u2); since u2 owns Test(1), it deletes it.
This confirms the destructors work correctly, and ownership is handled as expected.

Summary:
Message						                      Meaning / Trigger
Test(1) constructed				            UniquePtr<Test> u1(new Test(1))
Hello from Test(1)				             u1->greet()
u1 is now nullptr after move	      u1 moved to u2 → u1.ptr = nullptr
Test(1) destroyed				              u2 goes out of scope → deletes Test(1)
Test(2) constructed				            SharedPtr<Test> s1(new Test(2))
Hello from Test(2) (twice)			      s1->greet() and s2->greet()
Ref count: 2				 	                 s1 and s2 both point to the same object
Ref count after s2 destroyed: 1		  s2 goes out of scope
Test(2) destroyed				              s1 goes out of scope → last SharedPtr releases the object

✅ Unit Test Output:
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
