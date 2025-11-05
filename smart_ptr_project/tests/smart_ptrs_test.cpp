// smart_ptrs_test.cpp

#include "../include/my_unique_ptr.hpp"
#include "../include/my_shared_ptr.hpp"
#include "../include/my_weak_ptr.hpp"
#include <gtest/gtest.h>
#include <memory>

using namespace my_ptr;

struct Dummy
{
    int val;
    Dummy(int v) : val(v) {}
    ~Dummy() = default;
};

// UniquePtr Tests
TEST(UniquePtrTest, BasicOperations)
{
    UniquePtr<Dummy> ptr(new Dummy(5));
    EXPECT_EQ(ptr->val, 5);
    ptr->val = 10;
    EXPECT_EQ((*ptr).val, 10);
}

TEST(UniquePtrTest, MoveOwnership)
{
    UniquePtr<Dummy> ptr1(new Dummy(42));
    UniquePtr<Dummy> ptr2 = std::move(ptr1);
    EXPECT_FALSE(ptr1);
    EXPECT_EQ(ptr2->val, 42);
}
// SharedPtr Tests
TEST(SharedPtrTest, BasicReferenceCounting)
{
    std::shared_ptr<Dummy> sp1(new Dummy(1));
    EXPECT_EQ(sp1.use_count(), 1);
    {
        std::shared_ptr<Dummy> sp2 = sp1;
        EXPECT_EQ(sp1.use_count(), 2);
    }
    EXPECT_EQ(sp1.use_count(), 1);
}
TEST(SharedPtrTest, MoveSemantics)
{
    std::shared_ptr<Dummy> sp1(new Dummy(7));
    std::shared_ptr<Dummy> sp2 = std::move(sp1);
    EXPECT_FALSE(sp1);
    EXPECT_EQ(sp2->val, 7);
    EXPECT_EQ(sp2.use_count(), 1);
}

// WeakPtr Tests
TEST(WeakPtrTest, LockAccess)
{
    std::shared_ptr<Dummy> sp1(new Dummy(9));
    std::weak_ptr<Dummy> wp = sp1;
    auto locked = wp.lock();
    EXPECT_TRUE(locked);
    EXPECT_EQ(locked->val, 9);
}
TEST(WeakPtrTest, LockOnExpired)
{
    std::weak_ptr<Dummy> wp;
    {
        std::shared_ptr<Dummy> sp(new Dummy(11));
        wp = sp;
    }
    EXPECT_FALSE(wp.lock());
}


int main(int argc, char **argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}