#ifndef SMART_PTRS_HPP
#define SMART_PTRS_HPP

#include <cstddef> // for std::nullptr_t
#include <atomic>  // for std::atomic
#include <cassert> // for assertions

namespace my_ptr
{
    // ========================= SHARED PTR =========================
    // Reference-counted smart pointer for shared ownership
    template <typename T>
    class SharedPtr
    {
    private:
        T *ptr; // Raw pointer to managed object
        std::atomic<int> *count; // Reference counter

        // Helper to decrement count and delete if needed
        void release()
        {
            if (count && --(*count) == 0)
            {
                delete pr;
                delete count;
            }
        }
    
    public:
        // Constructor: owns a new object or nullptr
        explicit SharedPtr(T *p = nullptr) : ptr(p), count(p ? new std::atomic<int>(1) : nullptr) {}

        // Destructor: calls release to handle reference count
        ~SharedPtr() { release(); }

        // Copy constructor: increases ref count
        SharedPtr(const SharedPtr &other) : ptr(other.ptr), count(other.count)
        {
            if(count)
                ++(*count);
        }
        // Copy assignment: releases current, copies other's data
        SharedPtr &operator=(const SharedPtr &other)
        {
            if (this != &other)
            {
                release();
                ptr = other.ptr;
                count = other.count;
                if (count)
                    ++(*count);
            }
            return *this;
        }
        // Move constructor: transfers ownership, no increment
        SharedPtr(SharedPtr &&other) noexcept : ptr(other.ptr), count(other.count)
        {
            other.ptr = nullptr;
            other.count = nullptr;
        }
        // Move assignment: releases current and transfers ownership
        SharedPtr &operator=(SharedPtr &&other) noexcept
        {
            if (this != &other)
            {
                release();
                ptr = other.ptr;
                count = other.count;
                other.ptr = nullptr;
                other.count = nullptr;
            }
            return *this;
        }
        // Dereference and access
        T &operator*() const { return *ptr; }
        T *operator->() const { return ptr; }
        T *get() const { return ptr; }

        explicit operator bool() const { return ptr != nullptr; }

        // Return current ref count (0 if null)
        int use_count() const { return count ? count->load() : 0; }

        // Grant WeakPtr access
        friend class WeakPtr <T>;
    };
}// namespace my_ptr

#endif // SMART_PTRS_HPP
