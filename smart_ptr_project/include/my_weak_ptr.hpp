#ifndef SMART_PTRS_HPP
#define SMART_PTRS_HPP

#include <cstddef> // for std::nullptr_t
#include <atomic>  // for std::atomic
#include <cassert> // for assertions

namespace my_ptr
{
    // ========================= WEAK PTR =========================
    // Non-owning smart pointer that observes a SharedPtr
    template <typename T>
    class WeakPtr
    {
    private:
        T *ptr;                  // Pointer to object
        std::atomic<int> *count; // Pointer to shared ref count

    public:
        // Default constructor: empty WeakPtr
        WeakPtr() : ptr(nullptr), count(nullptr) {}
        // Construct from SharedPtr
        WeakPtr(const SharedPtr<T> &shared) : ptr(shared.ptr), count(shared.count) {}

        // Attempt to acquire a SharedPtr
        SharedPtr<T> lock() const
        {
            if (count && count->load() > 0)
            {
                return SharedPtr<T>(*this); // create SharedPtr if still valid
            }
            return SharedPtr<T>(); // return empty
        }
};
} // namespace my_ptr

#endif // SMART_PTRS_HPP