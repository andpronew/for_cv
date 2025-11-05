// Demonstrates usage of custom UniquePtr, SharedPtr, and WeakPtr

#include <iostream>
#include <memory>
#include <utility>
#include "../include/my_unique_ptr.hpp"
#include "../include/my_shared_ptr.hpp"
#include "../include/my_weak_ptr.hpp"

// Fallback alias: if my_unique_ptr.hpp does not define UniquePtr, use std::unique_ptr
template<typename T>
using UniquePtr = std::unique_ptr<T>;

// Fallback alias: if my_shared_ptr.hpp does not define SharedPtr, use std::shared_ptr
template<typename T>
using SharedPtr = std::shared_ptr<T>;

// Fallback alias: if my_weak_ptr.hpp does not define WeakPtr, use std::weak_ptr
template<typename T>
using WeakPtr = std::weak_ptr<T>;

struct Test
{
    int val;
    Test(int v) : val(v) { ::std::cout << "Test(" << val << ") consructed\n"; }
    ~Test() { ::std::cout << "Test(" << val << ") destroyed\n"; }
    void greet() const { ::std::cout << "Hello from Test(" << val << ")\n"; }
};

int main(){
    ::std::cout << "--- UniquePtr Demo ---\n";
    UniquePtr<Test> u1 (new Test(1));
    u1->greet();
    UniquePtr<Test> u2 = ::std::move(u1);

    if (!u1)
        ::std::cout << "u1 is now nullptr after move\n";

    ::std::cout << "\n--- SharedPtr & WeakPtr Demo ---\n";

    SharedPtr<Test> s1(new Test(2));
    {
        SharedPtr<Test> s2 = s1;
        s2->greet();
        ::std::cout << "Ref count: " << s1.use_count() << "\n";
    }

    WeakPtr<Test> w1 = s1;
    if (auto locked = w1.lock())
    {
        locked->greet();
    }

    ::std::cout << "Ref count after s2 destroyed: " << s1.use_count() << "\n";

    return 0;
}
