#ifndef SMART_PTRS_HPP
#define SMART_PTRS_HPP

#include <cstddef> // for std::nullptr_t
#include <atomic>  // for std::atomic
#include <cassert> // for assertions

namespace my_ptr
{
    // ========================= UNIQUE PTR =========================
    // Manages exclusive ownership of a dynamically allocated object
    template <typename T>

    class UniquePtr
    {
    private:
        T *ptr; // Raw pointer to managed object

    public:
        // Constructor: takes ownership of a raw pointer
        explicit UniquePtr(T *p = nullptr) : ptr(p) {}

        // Destructor: deletes the managed object
        ~UniquePtr() { delete ptr; }

        // Deleted copy constructor & copy assignment
        UniquePtr(const UniquePtr &) = delete;
        UniquePtr &operator=(const UniquePtr &) = delete;

        // Move constructor: transfers ownership from another UniquePtr        
        UniquePtr(UniquePtr &&other) noexcept : ptr(other.ptr)
        {
            other.ptr = nullptr;
        }
        // Move assignment: safely deletes current and takes ownership
        UniquePtr &operator=(UniquePtr &&other) noexcept
        {
            if (this != &other)
            {
                delete ptr;
                ptr = other.ptr;
                other.ptr = nullptr;
            }
            return *this;
        }

        // Dereference and access operators
        T &operator*() const { return *ptr; }
        T *operator->() const { return ptr; }

        // Access to raw pointer
        T *get() const { return ptr; }

        // Relinquish ownership and return raw pointer
        T *release()
        {
            T *temp = ptr;
            ptr = nullptr;
            return temp;
        }

        // Safe boolean context check
        explicit operator bool() const { return ptr != nullptr; }

        // Replace owned pointer with another (and delete current one)
        void reset(T *p = nullptr)
        {
            delete ptr;
            ptr = p;
        }
};

} // namespace my_ptr

#endif // SMART_PTRS_HPP