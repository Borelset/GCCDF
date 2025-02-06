//
// Created by ldb on 8/15/23.
//

#include <random>
#include <cstdint>
#include <vector>
#include <optional>
#include "gtest/gtest.h"
#include "../MetadataManager/MetadataManager.h"

namespace {
    class LCTreeTest1 : public ::testing::Test {
    protected:
        void SetUp() override {
            for (int i=0; i<16; ++i)
                sz.push_back(16);
            lctree.ingest_gray({0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}, sz, {});
            lctree.ingest_gray({16, 17, 18, 19, 20, 21, 22, 23}, {16, 16, 16, 16, 16, 16, 16, 16}, {0, 1, 2, 3, 4, 5, 6, 7});
            lctree.ingest_gray({24, 25, 26, 27}, {16, 16, 16, 16}, {0, 1, 2, 3, 8, 9, 10, 11, 16, 17, 18, 19});
        }
        std::vector<uint64_t> sz;
        ScalableChunkLifeCycle lctree{64};
    };
    class LCTreeTest2 : public ::testing::Test {
    protected:
        void SetUp() override {
            lctree.ingest_gray({0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}, {16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16}, {});
            lctree.ingest_gray({12, 13, 14, 15}, {16, 16, 16, 16}, {0, 1, 2, 3});
            lctree.ingest_gray({16, 17, 18, 19}, {16, 16, 16, 16}, {0, 1, 4, 5, 6, 7, 12, 13});
        }
        std::vector<uint64_t> sz;
        ScalableChunkLifeCycle lctree{64};
    };
    class LCTreeTest3 : public ::testing::Test {
    protected:
        void SetUp() override {
            lctree.ingest_gray({0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}, {16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16}, {});
            lctree.ingest_gray({12, 13, 14, 15, 16, 17, 18, 19}, {16, 16, 16, 16, 16, 16, 16, 16}, {0, 1, 2, 3, 4, 5, 6, 7});
            lctree.ingest_gray({}, {}, {0, 1, 2, 3, 10, 11, 12, 13, 14, 15});
        }
        std::vector<uint64_t> sz;
        ScalableChunkLifeCycle lctree{64};
    };
    TEST_F(LCTreeTest1, Init) {
        EXPECT_EQ(lctree.num_of_leaves(), 8);
        EXPECT_EQ(lctree.num_of_inners(), 7);
        EXPECT_EQ(lctree.num_of_empty_leaves(), 1);
        EXPECT_EQ(lctree.locate_id(12)->next, nullptr);
        EXPECT_EQ(lctree.locate_id(12), lctree.locate_id(13));
        EXPECT_EQ(lctree.locate_id(13), lctree.locate_id(14));
        EXPECT_EQ(lctree.locate_id(14), lctree.locate_id(15));
        EXPECT_TRUE(lctree.locate_id(8)->leaf);
        EXPECT_EQ(lctree.locate_id(8)->next, lctree.locate_id(12));
        EXPECT_EQ(lctree.locate_id(12)->prev, lctree.locate_id(8));
        EXPECT_EQ(lctree.locate_id(8), lctree.locate_id(9));
        EXPECT_EQ(lctree.locate_id(9), lctree.locate_id(10));
        EXPECT_EQ(lctree.locate_id(10), lctree.locate_id(11));
        EXPECT_TRUE(lctree.locate_id(0)->leaf);
        EXPECT_EQ(lctree.locate_id(0)->next, lctree.locate_id(8));
        EXPECT_EQ(lctree.locate_id(8)->prev, lctree.locate_id(0));
        EXPECT_EQ(lctree.locate_id(0), lctree.locate_id(1));
        EXPECT_EQ(lctree.locate_id(1), lctree.locate_id(2));
        EXPECT_EQ(lctree.locate_id(2), lctree.locate_id(3));
        EXPECT_TRUE(lctree.locate_id(4)->leaf);
        EXPECT_EQ(lctree.locate_id(4)->next, lctree.locate_id(0));
        EXPECT_EQ(lctree.locate_id(0)->prev, lctree.locate_id(4));
        EXPECT_EQ(lctree.locate_id(4), lctree.locate_id(5));
        EXPECT_EQ(lctree.locate_id(5), lctree.locate_id(6));
        EXPECT_EQ(lctree.locate_id(6), lctree.locate_id(7));
        EXPECT_TRUE(lctree.locate_id(20)->leaf);
        EXPECT_EQ(lctree.locate_id(20)->next, lctree.locate_id(4));
        EXPECT_EQ(lctree.locate_id(4)->prev, lctree.locate_id(20));
        EXPECT_EQ(lctree.locate_id(20), lctree.locate_id(21));
        EXPECT_EQ(lctree.locate_id(21), lctree.locate_id(22));
        EXPECT_EQ(lctree.locate_id(22), lctree.locate_id(23));
        EXPECT_TRUE(lctree.locate_id(16)->leaf);
        EXPECT_EQ(lctree.locate_id(16)->next, lctree.locate_id(20));
        EXPECT_EQ(lctree.locate_id(20)->prev, lctree.locate_id(16));
        EXPECT_EQ(lctree.locate_id(16), lctree.locate_id(17));
        EXPECT_EQ(lctree.locate_id(17), lctree.locate_id(18));
        EXPECT_EQ(lctree.locate_id(18), lctree.locate_id(19));
        EXPECT_TRUE(lctree.locate_id(24)->leaf);
        EXPECT_EQ(lctree.locate_id(24)->next, lctree.locate_id(16));
        EXPECT_EQ(lctree.locate_id(16)->prev, lctree.locate_id(24));
        EXPECT_EQ(lctree.locate_id(24), lctree.locate_id(25));
        EXPECT_EQ(lctree.locate_id(25), lctree.locate_id(26));
        EXPECT_EQ(lctree.locate_id(26), lctree.locate_id(27));
        EXPECT_TRUE(lctree.locate_id(24)->leaf);
        EXPECT_TRUE(lctree.locate_id(24)->prev->chunk_list->empty());
        for (int i=0; i<28; ++i) {
            auto lc_bitmap = lctree.get_lifecycle(i);
            EXPECT_TRUE(lc_bitmap);
            EXPECT_EQ(lc_bitmap->size(), 0);
        }
    }
    TEST_F(LCTreeTest1, Destruction) {
        lctree.delete_gray(4);
        lctree.delete_gray(5);
        lctree.delete_gray(5);
        lctree.delete_gray(4);
        lctree.delete_gray(3);
        EXPECT_EQ(lctree.num_of_inners(), 3);
        EXPECT_EQ(lctree.num_of_leaves(), 4);
        EXPECT_EQ(lctree.num_of_empty_leaves(), 1);
        EXPECT_TRUE(lctree.locate_id(8)->leaf);
        EXPECT_EQ(lctree.locate_id(8)->next, nullptr);
        EXPECT_EQ(lctree.locate_id(8), lctree.locate_id(9));
        EXPECT_EQ(lctree.locate_id(9), lctree.locate_id(10));
        EXPECT_EQ(lctree.locate_id(10), lctree.locate_id(11));
        EXPECT_EQ(lctree.locate_id(11), lctree.locate_id(12));
        EXPECT_EQ(lctree.locate_id(12), lctree.locate_id(13));
        EXPECT_EQ(lctree.locate_id(13), lctree.locate_id(14));
        EXPECT_EQ(lctree.locate_id(14), lctree.locate_id(15));
        EXPECT_TRUE(lctree.locate_id(0)->leaf);
        EXPECT_EQ(lctree.locate_id(0)->next, lctree.locate_id(8));
        EXPECT_EQ(lctree.locate_id(8)->prev, lctree.locate_id(0));
        EXPECT_EQ(lctree.locate_id(0), lctree.locate_id(1));
        EXPECT_EQ(lctree.locate_id(1), lctree.locate_id(2));
        EXPECT_EQ(lctree.locate_id(2), lctree.locate_id(3));
        EXPECT_EQ(lctree.locate_id(3), lctree.locate_id(4));
        EXPECT_EQ(lctree.locate_id(4), lctree.locate_id(5));
        EXPECT_EQ(lctree.locate_id(5), lctree.locate_id(6));
        EXPECT_EQ(lctree.locate_id(6), lctree.locate_id(7));
        EXPECT_TRUE(lctree.locate_id(16)->leaf);
        EXPECT_EQ(lctree.locate_id(16)->next, lctree.locate_id(0));
        EXPECT_EQ(lctree.locate_id(0)->prev, lctree.locate_id(16));
        EXPECT_EQ(lctree.locate_id(16), lctree.locate_id(17));
        EXPECT_EQ(lctree.locate_id(17), lctree.locate_id(18));
        EXPECT_EQ(lctree.locate_id(18), lctree.locate_id(19));
        EXPECT_EQ(lctree.locate_id(19), lctree.locate_id(20));
        EXPECT_EQ(lctree.locate_id(20), lctree.locate_id(21));
        EXPECT_EQ(lctree.locate_id(21), lctree.locate_id(22));
        EXPECT_EQ(lctree.locate_id(22), lctree.locate_id(23));
        EXPECT_TRUE(lctree.locate_id(16)->prev->leaf);
        EXPECT_TRUE(lctree.locate_id(16)->prev->chunk_list->empty());
        lctree.delete_gray(2);
        EXPECT_EQ(lctree.num_of_inners(), 1);
        EXPECT_EQ(lctree.num_of_leaves(), 2);
        EXPECT_EQ(lctree.num_of_empty_leaves(), 1);
        lctree.delete_gray(1);
        EXPECT_EQ(lctree.num_of_inners(), 1);
        EXPECT_EQ(lctree.num_of_leaves(), 0);
        EXPECT_EQ(lctree.num_of_empty_leaves(), 0);
    }
    TEST_F(LCTreeTest2, Init) {
        EXPECT_EQ(lctree.num_of_leaves(), 6);
        EXPECT_EQ(lctree.num_of_inners(), 5);
        EXPECT_EQ(lctree.num_of_empty_leaves(), 1);
        EXPECT_TRUE(lctree.locate_id(8)->leaf);
        EXPECT_EQ(lctree.locate_id(8)->next, nullptr);
        EXPECT_EQ(lctree.locate_id(8), lctree.locate_id(9));
        EXPECT_EQ(lctree.locate_id(9), lctree.locate_id(10));
        EXPECT_EQ(lctree.locate_id(10), lctree.locate_id(11));
        EXPECT_TRUE(lctree.locate_id(4)->leaf);
        EXPECT_EQ(lctree.locate_id(4)->next, lctree.locate_id(8));
        EXPECT_EQ(lctree.locate_id(8)->prev, lctree.locate_id(4));
        EXPECT_EQ(lctree.locate_id(4), lctree.locate_id(5));
        EXPECT_EQ(lctree.locate_id(5), lctree.locate_id(6));
        EXPECT_EQ(lctree.locate_id(6), lctree.locate_id(7));
        EXPECT_TRUE(lctree.locate_id(0)->leaf);
        EXPECT_EQ(lctree.locate_id(0)->next, lctree.locate_id(4));
        EXPECT_EQ(lctree.locate_id(4)->prev, lctree.locate_id(0));
        EXPECT_EQ(lctree.locate_id(0), lctree.locate_id(1));
        EXPECT_EQ(lctree.locate_id(1), lctree.locate_id(2));
        EXPECT_EQ(lctree.locate_id(2), lctree.locate_id(3));
        EXPECT_TRUE(lctree.locate_id(12)->leaf);
        EXPECT_EQ(lctree.locate_id(12)->next, lctree.locate_id(0));
        EXPECT_EQ(lctree.locate_id(0)->prev, lctree.locate_id(12));
        EXPECT_EQ(lctree.locate_id(12), lctree.locate_id(13));
        EXPECT_EQ(lctree.locate_id(13), lctree.locate_id(14));
        EXPECT_EQ(lctree.locate_id(14), lctree.locate_id(15));
        EXPECT_TRUE(lctree.locate_id(16)->prev->leaf);
        EXPECT_EQ(lctree.locate_id(16)->next, lctree.locate_id(12));
        EXPECT_EQ(lctree.locate_id(12)->prev, lctree.locate_id(16));
        EXPECT_EQ(lctree.locate_id(16), lctree.locate_id(17));
        EXPECT_EQ(lctree.locate_id(17), lctree.locate_id(18));
        EXPECT_EQ(lctree.locate_id(18), lctree.locate_id(19));
        EXPECT_TRUE(lctree.locate_id(16)->prev->chunk_list->empty());
        int expect[20] = {1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0};
        for (int i=0; i<20; ++i) {
            auto lc_bitmap = lctree.get_lifecycle(i);
            EXPECT_TRUE(lc_bitmap);
            EXPECT_EQ(lc_bitmap->size(), expect[i]);
        }
    }
    TEST_F(LCTreeTest2, DestructionV3) {
        lctree.delete_gray(3);
        EXPECT_EQ(lctree.num_of_inners(), 3);
        EXPECT_EQ(lctree.num_of_leaves(), 4);
        EXPECT_EQ(lctree.num_of_empty_leaves(), 1);
        EXPECT_TRUE(lctree.locate_id(4)->leaf);
        EXPECT_EQ(lctree.locate_id(4)->next, nullptr);
        EXPECT_EQ(lctree.locate_id(4), lctree.locate_id(5));
        EXPECT_EQ(lctree.locate_id(5), lctree.locate_id(6));
        EXPECT_EQ(lctree.locate_id(6), lctree.locate_id(7));
        EXPECT_EQ(lctree.locate_id(8), lctree.locate_id(9));
        EXPECT_EQ(lctree.locate_id(9), lctree.locate_id(10));
        EXPECT_EQ(lctree.locate_id(10), lctree.locate_id(11));
        EXPECT_TRUE(lctree.locate_id(0)->leaf);
        EXPECT_EQ(lctree.locate_id(0)->next, lctree.locate_id(4));
        EXPECT_EQ(lctree.locate_id(4)->prev, lctree.locate_id(0));
        EXPECT_EQ(lctree.locate_id(0), lctree.locate_id(1));
        EXPECT_EQ(lctree.locate_id(1), lctree.locate_id(2));
        EXPECT_EQ(lctree.locate_id(2), lctree.locate_id(3));
        EXPECT_TRUE(lctree.locate_id(12)->leaf);
        EXPECT_EQ(lctree.locate_id(12)->next, lctree.locate_id(0));
        EXPECT_EQ(lctree.locate_id(0)->prev, lctree.locate_id(12));
        EXPECT_EQ(lctree.locate_id(12), lctree.locate_id(13));
        EXPECT_EQ(lctree.locate_id(13), lctree.locate_id(14));
        EXPECT_EQ(lctree.locate_id(14), lctree.locate_id(15));
        EXPECT_TRUE(lctree.locate_id(12)->prev->leaf);
        EXPECT_TRUE(lctree.locate_id(12)->prev->chunk_list->empty());
        VariableBitset lc_bitmap;
        for (int i=0; i<16; ++i) {
            auto lc_bitmap = lctree.get_lifecycle(i);
            EXPECT_TRUE(lc_bitmap);
            EXPECT_EQ(lc_bitmap->size(), 0);
        }
        for (int i=17; i<20; ++i) {
            auto lc_bitmap = lctree.get_lifecycle(i);
            EXPECT_FALSE(lc_bitmap);
        }
    }
    TEST_F(LCTreeTest2, DestructionV2) {
        lctree.delete_gray(2);
        EXPECT_EQ(lctree.num_of_inners(), 3);
        EXPECT_EQ(lctree.num_of_leaves(), 4);
        EXPECT_EQ(lctree.num_of_empty_leaves(), 1);
        EXPECT_TRUE(lctree.locate_id(2)->leaf);
        EXPECT_EQ(lctree.locate_id(2)->next, nullptr);
        EXPECT_EQ(lctree.locate_id(2), lctree.locate_id(3));
        EXPECT_EQ(lctree.locate_id(3), lctree.locate_id(8));
        EXPECT_EQ(lctree.locate_id(8), lctree.locate_id(9));
        EXPECT_EQ(lctree.locate_id(9), lctree.locate_id(10));
        EXPECT_EQ(lctree.locate_id(10), lctree.locate_id(11));
        EXPECT_TRUE(lctree.locate_id(0)->leaf);
        EXPECT_EQ(lctree.locate_id(0)->next, lctree.locate_id(2));
        EXPECT_EQ(lctree.locate_id(2)->prev, lctree.locate_id(0));
        EXPECT_EQ(lctree.locate_id(0), lctree.locate_id(1));
        EXPECT_EQ(lctree.locate_id(1), lctree.locate_id(4));
        EXPECT_EQ(lctree.locate_id(4), lctree.locate_id(5));
        EXPECT_EQ(lctree.locate_id(5), lctree.locate_id(6));
        EXPECT_EQ(lctree.locate_id(6), lctree.locate_id(7));
        EXPECT_TRUE(lctree.locate_id(12)->leaf);
        EXPECT_EQ(lctree.locate_id(12)->next, lctree.locate_id(0));
        EXPECT_EQ(lctree.locate_id(0)->prev, lctree.locate_id(12));
        EXPECT_EQ(lctree.locate_id(12), lctree.locate_id(13));
        EXPECT_EQ(lctree.locate_id(13), lctree.locate_id(16));
        EXPECT_EQ(lctree.locate_id(16), lctree.locate_id(17));
        EXPECT_EQ(lctree.locate_id(17), lctree.locate_id(18));
        EXPECT_EQ(lctree.locate_id(18), lctree.locate_id(19));
        EXPECT_TRUE(lctree.locate_id(12)->prev->leaf);
        EXPECT_TRUE(lctree.locate_id(12)->prev->chunk_list->empty());
        EXPECT_EQ(lctree.locate_id(12)->prev->prev, nullptr);
        bool expect_find[20] = {true, true, true, true, true, true, true, true, true, true, true, true, true, true, false, false, true, true, true, true};
        for (int i=0; i<20; ++i) {
            auto lc_bitmap = lctree.get_lifecycle(i);
            if (expect_find[i])
                EXPECT_TRUE(lc_bitmap);
            else
                EXPECT_FALSE(lc_bitmap);
            if (lc_bitmap)
                EXPECT_EQ(lc_bitmap->size(), 0);
        }
    }
    TEST_F(LCTreeTest2, DestructionV1) {
        lctree.delete_gray(1);
        EXPECT_EQ(lctree.num_of_inners(), 2);
        EXPECT_EQ(lctree.num_of_leaves(), 3);
        EXPECT_EQ(lctree.num_of_empty_leaves(), 1);
        EXPECT_TRUE(lctree.locate_id(0)->leaf);
        EXPECT_EQ(lctree.locate_id(0)->next, nullptr);
        EXPECT_EQ(lctree.locate_id(0), lctree.locate_id(1));
        EXPECT_EQ(lctree.locate_id(1), lctree.locate_id(2));
        EXPECT_EQ(lctree.locate_id(2), lctree.locate_id(3));
        EXPECT_EQ(lctree.locate_id(3), lctree.locate_id(12));
        EXPECT_EQ(lctree.locate_id(12), lctree.locate_id(13));
        EXPECT_EQ(lctree.locate_id(13), lctree.locate_id(14));
        EXPECT_EQ(lctree.locate_id(14), lctree.locate_id(15));
        EXPECT_TRUE(lctree.locate_id(4)->leaf);
        EXPECT_EQ(lctree.locate_id(4)->next, lctree.locate_id(0));
        EXPECT_EQ(lctree.locate_id(0)->prev, lctree.locate_id(4));
        EXPECT_EQ(lctree.locate_id(4), lctree.locate_id(5));
        EXPECT_EQ(lctree.locate_id(5), lctree.locate_id(6));
        EXPECT_EQ(lctree.locate_id(6), lctree.locate_id(7));
        EXPECT_EQ(lctree.locate_id(7), lctree.locate_id(16));
        EXPECT_EQ(lctree.locate_id(16), lctree.locate_id(17));
        EXPECT_EQ(lctree.locate_id(17), lctree.locate_id(18));
        EXPECT_EQ(lctree.locate_id(18), lctree.locate_id(19));
        EXPECT_TRUE(lctree.locate_id(4)->prev->leaf);
        EXPECT_TRUE(lctree.locate_id(4)->prev->chunk_list->empty());
        EXPECT_EQ(lctree.locate_id(4)->prev->prev, nullptr);
        std::optional<VariableBitset> lc_bitmap;
        lc_bitmap = lctree.get_lifecycle(0);
        EXPECT_EQ(lc_bitmap->get_byte(0), 0b00000001);
        lc_bitmap = lctree.get_lifecycle(1);
        EXPECT_EQ(lc_bitmap->get_byte(0), 0b00000001);
        lc_bitmap = lctree.get_lifecycle(2);
        EXPECT_EQ(lc_bitmap->get_byte(0), 0b00000000);
        lc_bitmap = lctree.get_lifecycle(3);
        EXPECT_EQ(lc_bitmap->get_byte(0), 0b00000000);
        lc_bitmap = lctree.get_lifecycle(12);
        EXPECT_EQ(lc_bitmap->get_byte(0), 0b00000001);
        lc_bitmap = lctree.get_lifecycle(13);
        EXPECT_EQ(lc_bitmap->get_byte(0), 0b00000001);
        lc_bitmap = lctree.get_lifecycle(14);
        EXPECT_EQ(lc_bitmap->get_byte(0), 0b00000000);
        lc_bitmap = lctree.get_lifecycle(15);
        EXPECT_EQ(lc_bitmap->get_byte(0), 0b00000000);
        lc_bitmap = lctree.get_lifecycle(4);
        EXPECT_EQ(lc_bitmap->size(), 0);
        lc_bitmap = lctree.get_lifecycle(5);
        EXPECT_EQ(lc_bitmap->size(), 0);
        lc_bitmap = lctree.get_lifecycle(6);
        EXPECT_EQ(lc_bitmap->size(), 0);
        lc_bitmap = lctree.get_lifecycle(7);
        EXPECT_EQ(lc_bitmap->size(), 0);
        lc_bitmap = lctree.get_lifecycle(16);
        EXPECT_EQ(lc_bitmap->size(), 0);
        lc_bitmap = lctree.get_lifecycle(17);
        EXPECT_EQ(lc_bitmap->size(), 0);
        lc_bitmap = lctree.get_lifecycle(18);
        EXPECT_EQ(lc_bitmap->size(), 0);
        lc_bitmap = lctree.get_lifecycle(19);
        EXPECT_EQ(lc_bitmap->size(), 0);
        bool expect_find[20] = {true, true, true, true, true, true, true, true, false, false, false, false, true, true, true, true, true, true, true, true};
        int expect[20] = {1, 1, 1, 1, 0, 0, 0, 0, -1, -1, -1, -1, 1, 1, 1, 1, 0, 0, 0, 0};
        for (int i=0; i<20; ++i) {
            auto lc_bitmap = lctree.get_lifecycle(i);
            if (expect_find[i])
                EXPECT_TRUE(lc_bitmap);
            else
                EXPECT_FALSE(lc_bitmap);
            if (lc_bitmap)
                EXPECT_EQ(lc_bitmap->size(), expect[i]);
        }
    }
    TEST_F(LCTreeTest2, DestructionV12) {
        EXPECT_EQ(lctree.num_of_inners(), 5);
        EXPECT_EQ(lctree.num_of_leaves(), 6);
        EXPECT_EQ(lctree.num_of_empty_leaves(), 1);
        lctree.delete_gray(1);
        EXPECT_EQ(lctree.num_of_inners(), 2);
        EXPECT_EQ(lctree.num_of_leaves(), 3);
        EXPECT_EQ(lctree.num_of_empty_leaves(), 1);
        lctree.delete_gray(2);
        EXPECT_EQ(lctree.num_of_inners(), 1);
        EXPECT_EQ(lctree.num_of_leaves(), 2);
        EXPECT_EQ(lctree.num_of_empty_leaves(), 1);
        EXPECT_TRUE(lctree.locate_id(0)->leaf);
        EXPECT_EQ(lctree.locate_id(0), lctree.locate_id(1));
        EXPECT_EQ(lctree.locate_id(1), lctree.locate_id(4));
        EXPECT_EQ(lctree.locate_id(4), lctree.locate_id(5));
        EXPECT_EQ(lctree.locate_id(5), lctree.locate_id(6));
        EXPECT_EQ(lctree.locate_id(6), lctree.locate_id(7));
        EXPECT_EQ(lctree.locate_id(7), lctree.locate_id(12));
        EXPECT_EQ(lctree.locate_id(12), lctree.locate_id(13));
        EXPECT_EQ(lctree.locate_id(13), lctree.locate_id(16));
        EXPECT_EQ(lctree.locate_id(16), lctree.locate_id(17));
        EXPECT_EQ(lctree.locate_id(17), lctree.locate_id(18));
        EXPECT_EQ(lctree.locate_id(18), lctree.locate_id(19));
        EXPECT_TRUE(lctree.locate_id(0)->prev->leaf);
        EXPECT_TRUE(lctree.locate_id(0)->prev->chunk_list->empty());
        EXPECT_EQ(lctree.locate_id(0)->prev->prev, nullptr);
        bool expect_find[20] = {true, true, false, false, true, true, true, true, false, false, false, false, true, true, false, false, true, true, true, true};
        int expect[20] = {0, 0, -1, -1, 0, 0, 0, 0, -1, -1, -1, -1, 0, 0, -1, -1, 0, 0, 0, 0};
        for (int i=0; i<20; ++i) {
            auto lc_bitmap = lctree.get_lifecycle(i);
            if (expect_find[i])
                EXPECT_TRUE(lc_bitmap);
            else
                EXPECT_FALSE(lc_bitmap);
            if (lc_bitmap)
                EXPECT_EQ(lc_bitmap->size(), expect[i]);
        }
    }
    TEST_F(LCTreeTest2, DestructionV21) {
        lctree.delete_gray(2);
        lctree.delete_gray(1);
        EXPECT_EQ(lctree.num_of_inners(), 1);
        EXPECT_EQ(lctree.num_of_leaves(), 2);
        EXPECT_EQ(lctree.num_of_empty_leaves(), 1);
        EXPECT_TRUE(lctree.locate_id(0)->leaf);
        EXPECT_EQ(lctree.locate_id(0), lctree.locate_id(1));
        EXPECT_EQ(lctree.locate_id(1), lctree.locate_id(4));
        EXPECT_EQ(lctree.locate_id(4), lctree.locate_id(5));
        EXPECT_EQ(lctree.locate_id(5), lctree.locate_id(6));
        EXPECT_EQ(lctree.locate_id(6), lctree.locate_id(7));
        EXPECT_EQ(lctree.locate_id(7), lctree.locate_id(12));
        EXPECT_EQ(lctree.locate_id(12), lctree.locate_id(13));
        EXPECT_EQ(lctree.locate_id(13), lctree.locate_id(16));
        EXPECT_EQ(lctree.locate_id(16), lctree.locate_id(17));
        EXPECT_EQ(lctree.locate_id(17), lctree.locate_id(18));
        EXPECT_EQ(lctree.locate_id(18), lctree.locate_id(19));
        EXPECT_TRUE(lctree.locate_id(0)->prev->leaf);
        EXPECT_TRUE(lctree.locate_id(0)->prev->chunk_list->empty());
        EXPECT_EQ(lctree.locate_id(0)->prev->prev, nullptr);
        bool expect_find[20] = {true, true, false, false, true, true, true, true, false, false, false, false, true, true, false, false, true, true, true, true};
        int expect[20] = {0, 0, -1, -1, 0, 0, 0, 0, -1, -1, -1, -1, 0, 0, -1, -1, 0, 0, 0, 0};
        for (int i=0; i<20; ++i) {
            auto lc_bitmap = lctree.get_lifecycle(i);
            if (expect_find[i])
                EXPECT_TRUE(lc_bitmap);
            else
                EXPECT_FALSE(lc_bitmap);
            if (lc_bitmap)
                EXPECT_EQ(lc_bitmap->size(), expect[i]);
        }
    }
    TEST_F(LCTreeTest2, DestructionETIngestion) {
        lctree.delete_gray(1);
        lctree.ingest_gray({20, 21, 22, 23, 24, 25, 26, 27}, {16, 16, 16, 16, 16, 16, 16, 16}, {0, 1, 2});
        lctree.ingest_gray({28, 29, 30, 31}, {16, 16, 16, 16}, {3, 4, 5, 15, 16, 17});
        EXPECT_EQ(lctree.num_of_leaves(), 9);
        EXPECT_EQ(lctree.num_of_inners(), 8);
        EXPECT_EQ(lctree.num_of_empty_leaves(), 3);
        EXPECT_TRUE(lctree.locate_id(2)->leaf);
        EXPECT_EQ(lctree.locate_id(2)->next, nullptr);
        EXPECT_EQ(lctree.locate_id(2), lctree.locate_id(3));
        EXPECT_EQ(lctree.locate_id(3), lctree.locate_id(14));
        EXPECT_EQ(lctree.locate_id(14), lctree.locate_id(15));
        EXPECT_TRUE(lctree.locate_id(0)->leaf);
        EXPECT_EQ(lctree.locate_id(0)->next, lctree.locate_id(2));
        EXPECT_EQ(lctree.locate_id(2)->prev, lctree.locate_id(0));
        EXPECT_EQ(lctree.locate_id(0), lctree.locate_id(1));
        EXPECT_EQ(lctree.locate_id(1), lctree.locate_id(12));
        EXPECT_EQ(lctree.locate_id(12), lctree.locate_id(13));
        EXPECT_TRUE(lctree.locate_id(6)->leaf);
        EXPECT_EQ(lctree.locate_id(6)->next, lctree.locate_id(0));
        EXPECT_EQ(lctree.locate_id(0)->prev, lctree.locate_id(6));
        EXPECT_EQ(lctree.locate_id(6), lctree.locate_id(7));
        EXPECT_EQ(lctree.locate_id(7), lctree.locate_id(18));
        EXPECT_EQ(lctree.locate_id(18), lctree.locate_id(19));
        EXPECT_TRUE(lctree.locate_id(4)->leaf);
        EXPECT_EQ(lctree.locate_id(4)->next, lctree.locate_id(6));
        EXPECT_EQ(lctree.locate_id(6)->prev, lctree.locate_id(4));
        EXPECT_EQ(lctree.locate_id(4), lctree.locate_id(5));
        EXPECT_EQ(lctree.locate_id(5), lctree.locate_id(16));
        EXPECT_EQ(lctree.locate_id(16), lctree.locate_id(17));
        EXPECT_TRUE(lctree.locate_id(20)->leaf);
        EXPECT_TRUE(lctree.locate_id(4)->prev->leaf);
        EXPECT_TRUE(lctree.locate_id(4)->prev->chunk_list->empty());
        EXPECT_TRUE(lctree.locate_id(4)->prev->prev->leaf);
        EXPECT_EQ(lctree.locate_id(4)->prev->prev, lctree.locate_id(20));
        EXPECT_TRUE(lctree.locate_id(20)->next->chunk_list->empty());
        EXPECT_EQ(lctree.locate_id(20)->next->next, lctree.locate_id(4));
        EXPECT_EQ(lctree.locate_id(20), lctree.locate_id(21));
        EXPECT_EQ(lctree.locate_id(21), lctree.locate_id(22));
        EXPECT_EQ(lctree.locate_id(22), lctree.locate_id(23));
        EXPECT_EQ(lctree.locate_id(23), lctree.locate_id(24));
        EXPECT_EQ(lctree.locate_id(24), lctree.locate_id(25));
        EXPECT_EQ(lctree.locate_id(25), lctree.locate_id(26));
        EXPECT_EQ(lctree.locate_id(26), lctree.locate_id(27));
        EXPECT_TRUE(lctree.locate_id(28)->leaf);
        EXPECT_EQ(lctree.locate_id(28)->next->next, lctree.locate_id(20));
        EXPECT_EQ(lctree.locate_id(20)->prev->prev, lctree.locate_id(28));
        EXPECT_EQ(lctree.locate_id(28), lctree.locate_id(29));
        EXPECT_EQ(lctree.locate_id(29), lctree.locate_id(30));
        EXPECT_EQ(lctree.locate_id(30), lctree.locate_id(31));
        EXPECT_TRUE(lctree.locate_id(28)->prev->leaf);
        EXPECT_TRUE(lctree.locate_id(28)->prev->chunk_list->empty());
        EXPECT_EQ(lctree.locate_id(28)->prev->prev, nullptr);
        bool expect_find[32] = {true, true, true, true, true, true, true, true, false, false, false, false, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true};
        int expect_size[32] = {2, 2, 2, 2, 0, 0, 0, 0, -1, -1, -1, -1, 2, 2, 2, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
        int8_t expect[32] = {0b0000'0001, 0b0000'0001, 0b0000'0001, 0b0000'0010, -1, -1, -1, -1, -1, -1, -1, -1, 0, 0, 0, 0b0000'0010, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1};
        for (int i=0; i<20; ++i) {
            auto lc_bitmap = lctree.get_lifecycle(i);
            EXPECT_EQ((bool) lc_bitmap, expect_find[i]);
            if (lc_bitmap) {
                EXPECT_EQ(lc_bitmap->size(), expect_size[i]);
                if (expect_size[i] > 0)
                    EXPECT_EQ(lc_bitmap->get_byte(0), expect[i]);
            }
        }
    }
    TEST_F(LCTreeTest3, Init) {
        EXPECT_EQ(lctree.num_of_leaves(), 7);
        EXPECT_EQ(lctree.num_of_inners(), 6);
        EXPECT_EQ(lctree.num_of_empty_leaves(), 2);
        EXPECT_TRUE(lctree.locate_id(8)->leaf);
        EXPECT_EQ(lctree.locate_id(8)->next, nullptr);
        EXPECT_EQ(lctree.locate_id(8), lctree.locate_id(9));
        EXPECT_EQ(lctree.locate_id(9), lctree.locate_id(10));
        EXPECT_EQ(lctree.locate_id(10), lctree.locate_id(11));
        EXPECT_TRUE(lctree.locate_id(0)->leaf);
        EXPECT_EQ(lctree.locate_id(0)->next, lctree.locate_id(8));
        EXPECT_EQ(lctree.locate_id(8)->prev, lctree.locate_id(0));
        EXPECT_EQ(lctree.locate_id(0), lctree.locate_id(1));
        EXPECT_EQ(lctree.locate_id(1), lctree.locate_id(2));
        EXPECT_EQ(lctree.locate_id(2), lctree.locate_id(3));
        EXPECT_TRUE(lctree.locate_id(4)->leaf);
        EXPECT_EQ(lctree.locate_id(4)->next, lctree.locate_id(0));
        EXPECT_EQ(lctree.locate_id(0)->prev, lctree.locate_id(4));
        EXPECT_EQ(lctree.locate_id(4), lctree.locate_id(5));
        EXPECT_EQ(lctree.locate_id(5), lctree.locate_id(6));
        EXPECT_EQ(lctree.locate_id(6), lctree.locate_id(7));
        EXPECT_TRUE(lctree.locate_id(16)->leaf);
        EXPECT_EQ(lctree.locate_id(16)->next, lctree.locate_id(4));
        EXPECT_EQ(lctree.locate_id(4)->prev, lctree.locate_id(16));
        EXPECT_EQ(lctree.locate_id(16), lctree.locate_id(17));
        EXPECT_EQ(lctree.locate_id(17), lctree.locate_id(18));
        EXPECT_EQ(lctree.locate_id(18), lctree.locate_id(19));
        EXPECT_TRUE(lctree.locate_id(12)->leaf);
        EXPECT_EQ(lctree.locate_id(12)->next, lctree.locate_id(16));
        EXPECT_EQ(lctree.locate_id(16)->prev, lctree.locate_id(12));
        EXPECT_EQ(lctree.locate_id(12), lctree.locate_id(13));
        EXPECT_EQ(lctree.locate_id(13), lctree.locate_id(14));
        EXPECT_EQ(lctree.locate_id(14), lctree.locate_id(15));
        EXPECT_TRUE(lctree.locate_id(12)->prev->leaf);
        EXPECT_TRUE(lctree.locate_id(12)->prev->chunk_list->empty());
        EXPECT_TRUE(lctree.locate_id(12)->prev->prev->leaf);
        EXPECT_TRUE(lctree.locate_id(12)->prev->prev->chunk_list->empty());
        EXPECT_EQ(lctree.locate_id(12)->prev->prev->prev, nullptr);
        bool expect_find[20] = {true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true};
        int expect_size[20] = {0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0};
        int8_t expect[20] = {-1, -1 ,-1 , -1, -1, -1, -1, -1, 0, 0, 0b0000'0001, 0b0000'0001, -1, -1, -1, -1, -1, -1, -1, -1};
        for (int i=0; i<20; ++i) {
            auto lc_bitmap = lctree.get_lifecycle(i);
            EXPECT_EQ((bool) lc_bitmap, expect_find[i]);
            if (lc_bitmap) {
                EXPECT_EQ(lc_bitmap->size(), expect_size[i]);
                if (expect_size[i] > 0)
                    EXPECT_EQ(lc_bitmap->get_byte(0), expect[i]);
            }
        }
    }
    TEST_F(LCTreeTest3, DestructionV3) {
        lctree.delete_gray(3);
        EXPECT_EQ(lctree.num_of_inners(), 3);
        EXPECT_EQ(lctree.num_of_leaves(), 4);
        EXPECT_EQ(lctree.num_of_empty_leaves(), 1);
        EXPECT_TRUE(lctree.locate_id(8)->leaf);
        EXPECT_EQ(lctree.locate_id(8)->next, nullptr);
        EXPECT_EQ(lctree.locate_id(8), lctree.locate_id(9));
        EXPECT_EQ(lctree.locate_id(9), lctree.locate_id(10));
        EXPECT_EQ(lctree.locate_id(10), lctree.locate_id(11));
        EXPECT_TRUE(lctree.locate_id(8)->leaf);
        EXPECT_EQ(lctree.locate_id(0)->next, lctree.locate_id(8));
        EXPECT_EQ(lctree.locate_id(8)->prev, lctree.locate_id(0));
        EXPECT_EQ(lctree.locate_id(0), lctree.locate_id(1));
        EXPECT_EQ(lctree.locate_id(1), lctree.locate_id(2));
        EXPECT_EQ(lctree.locate_id(2), lctree.locate_id(3));
        EXPECT_EQ(lctree.locate_id(3), lctree.locate_id(4));
        EXPECT_EQ(lctree.locate_id(4), lctree.locate_id(5));
        EXPECT_EQ(lctree.locate_id(5), lctree.locate_id(6));
        EXPECT_EQ(lctree.locate_id(6), lctree.locate_id(7));
        EXPECT_TRUE(lctree.locate_id(12)->leaf);
        EXPECT_EQ(lctree.locate_id(12)->next, lctree.locate_id(4));
        EXPECT_EQ(lctree.locate_id(4)->prev, lctree.locate_id(12));
        EXPECT_EQ(lctree.locate_id(12), lctree.locate_id(13));
        EXPECT_EQ(lctree.locate_id(13), lctree.locate_id(14));
        EXPECT_EQ(lctree.locate_id(14), lctree.locate_id(15));
        EXPECT_EQ(lctree.locate_id(16), lctree.locate_id(17));
        EXPECT_EQ(lctree.locate_id(17), lctree.locate_id(18));
        EXPECT_EQ(lctree.locate_id(18), lctree.locate_id(19));
        EXPECT_TRUE(lctree.locate_id(12)->prev->leaf);
        EXPECT_TRUE(lctree.locate_id(12)->prev->chunk_list->empty());
        EXPECT_EQ(lctree.locate_id(12)->prev->prev, nullptr);
        bool expect_find[20] = {true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true};
        int expect_size[20] = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
        int8_t expect[20] = {-1, -1 ,-1 , -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1};
        for (int i=0; i<20; ++i) {
            auto lc_bitmap = lctree.get_lifecycle(i);
            EXPECT_EQ((bool) lc_bitmap, expect_find[i]);
            if (lc_bitmap) {
                EXPECT_EQ(lc_bitmap->size(), expect_size[i]);
                if (expect_size[i] > 0)
                    EXPECT_EQ(lc_bitmap->get_byte(0), expect[i]);
            }
        }
    }
    TEST_F(LCTreeTest3, DestructionV2) {
        lctree.delete_gray(2);
        EXPECT_EQ(lctree.num_of_inners(), 3);
        EXPECT_EQ(lctree.num_of_leaves(), 4);
        EXPECT_EQ(lctree.num_of_empty_leaves(), 1);
        EXPECT_TRUE(lctree.locate_id(4)->leaf);
        EXPECT_EQ(lctree.locate_id(4)->next, nullptr);
        EXPECT_EQ(lctree.locate_id(4), lctree.locate_id(5));
        EXPECT_EQ(lctree.locate_id(5), lctree.locate_id(6));
        EXPECT_EQ(lctree.locate_id(6), lctree.locate_id(7));
        EXPECT_EQ(lctree.locate_id(7), lctree.locate_id(8));
        EXPECT_EQ(lctree.locate_id(8), lctree.locate_id(9));
        EXPECT_TRUE(lctree.locate_id(0)->leaf);
        EXPECT_EQ(lctree.locate_id(0)->next, lctree.locate_id(4));
        EXPECT_EQ(lctree.locate_id(4)->prev, lctree.locate_id(0));
        EXPECT_EQ(lctree.locate_id(0), lctree.locate_id(1));
        EXPECT_EQ(lctree.locate_id(1), lctree.locate_id(2));
        EXPECT_EQ(lctree.locate_id(2), lctree.locate_id(3));
        EXPECT_EQ(lctree.locate_id(3), lctree.locate_id(10));
        EXPECT_EQ(lctree.locate_id(10), lctree.locate_id(11));
        EXPECT_TRUE(lctree.locate_id(12)->leaf);
        EXPECT_EQ(lctree.locate_id(12)->next, lctree.locate_id(0));
        EXPECT_EQ(lctree.locate_id(0)->prev, lctree.locate_id(12));
        EXPECT_EQ(lctree.locate_id(12), lctree.locate_id(13));
        EXPECT_EQ(lctree.locate_id(13), lctree.locate_id(14));
        EXPECT_EQ(lctree.locate_id(14), lctree.locate_id(15));
        EXPECT_TRUE(lctree.locate_id(12)->prev->leaf);
        EXPECT_TRUE(lctree.locate_id(12)->prev->chunk_list->empty());
        EXPECT_EQ(lctree.locate_id(12)->prev->prev, nullptr);
        bool expect_find[20] = {true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, false, false, false, false};
        int expect_size[20] = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, -1, -1, -1, -1};
        int8_t expect[20] = {-1, -1 ,-1 , -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1};
        for (int i=0; i<20; ++i) {
            auto lc_bitmap = lctree.get_lifecycle(i);
            EXPECT_EQ((bool) lc_bitmap, expect_find[i]);
            if (lc_bitmap) {
                EXPECT_EQ(lc_bitmap->size(), expect_size[i]);
                if (expect_size[i] > 0)
                    EXPECT_EQ(lc_bitmap->get_byte(0), expect[i]);
            }
        }
    }
    TEST_F(LCTreeTest3, DestructionV1) {
        lctree.delete_gray(1);
        EXPECT_EQ(lctree.num_of_inners(), 3);
        EXPECT_EQ(lctree.num_of_leaves(), 4);
        EXPECT_EQ(lctree.num_of_empty_leaves(), 1);
        EXPECT_TRUE(lctree.locate_id(4)->leaf);
        EXPECT_EQ(lctree.locate_id(4)->next, nullptr);
        EXPECT_EQ(lctree.locate_id(4), lctree.locate_id(5));
        EXPECT_EQ(lctree.locate_id(5), lctree.locate_id(6));
        EXPECT_EQ(lctree.locate_id(6), lctree.locate_id(7));
        EXPECT_EQ(lctree.locate_id(7), lctree.locate_id(16));
        EXPECT_EQ(lctree.locate_id(16), lctree.locate_id(17));
        EXPECT_EQ(lctree.locate_id(17), lctree.locate_id(18));
        EXPECT_EQ(lctree.locate_id(18), lctree.locate_id(19));
        EXPECT_TRUE(lctree.locate_id(0)->leaf);
        EXPECT_EQ(lctree.locate_id(0)->next, lctree.locate_id(4));
        EXPECT_EQ(lctree.locate_id(4)->prev, lctree.locate_id(0));
        EXPECT_EQ(lctree.locate_id(0), lctree.locate_id(1));
        EXPECT_EQ(lctree.locate_id(1), lctree.locate_id(2));
        EXPECT_EQ(lctree.locate_id(2), lctree.locate_id(3));
        EXPECT_EQ(lctree.locate_id(3), lctree.locate_id(12));
        EXPECT_EQ(lctree.locate_id(12), lctree.locate_id(13));
        EXPECT_EQ(lctree.locate_id(13), lctree.locate_id(14));
        EXPECT_EQ(lctree.locate_id(14), lctree.locate_id(15));
        EXPECT_TRUE(lctree.locate_id(10)->leaf);
        EXPECT_EQ(lctree.locate_id(10)->next, lctree.locate_id(0));
        EXPECT_EQ(lctree.locate_id(0)->prev, lctree.locate_id(10));
        EXPECT_EQ(lctree.locate_id(10), lctree.locate_id(11));
        EXPECT_TRUE(lctree.locate_id(10)->prev->leaf);
        EXPECT_TRUE(lctree.locate_id(10)->prev->chunk_list->empty());
        EXPECT_EQ(lctree.locate_id(10)->prev->prev, nullptr);
        bool expect_find[20] = {true, true, true, true, true, true, true, true, false, false, true, true, true, true, true, true, true, true, true, true};
        int expect_size[20] = {0, 0, 0, 0, 0, 0, 0, 0, -1, -1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
        int8_t expect[20] = {-1, -1 ,-1 , -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1};
        for (int i=0; i<20; ++i) {
            auto lc_bitmap = lctree.get_lifecycle(i);
            EXPECT_EQ((bool) lc_bitmap, expect_find[i]);
            if (lc_bitmap) {
                EXPECT_EQ(lc_bitmap->size(), expect_size[i]);
                if (expect_size[i] > 0)
                    EXPECT_EQ(lc_bitmap->get_byte(0), expect[i]);
            }
        }
    }
    TEST_F(LCTreeTest3, DestructionV12) {
        lctree.delete_gray(1);
        lctree.delete_gray(2);
        EXPECT_EQ(lctree.num_of_inners(), 1);
        EXPECT_EQ(lctree.num_of_leaves(), 2);
        EXPECT_EQ(lctree.num_of_empty_leaves(), 1);
        EXPECT_TRUE(lctree.locate_id(0)->leaf);
        EXPECT_EQ(lctree.locate_id(0), lctree.locate_id(1));
        EXPECT_EQ(lctree.locate_id(1), lctree.locate_id(2));
        EXPECT_EQ(lctree.locate_id(2), lctree.locate_id(3));
        EXPECT_EQ(lctree.locate_id(3), lctree.locate_id(10));
        EXPECT_EQ(lctree.locate_id(10), lctree.locate_id(11));
        EXPECT_EQ(lctree.locate_id(11), lctree.locate_id(12));
        EXPECT_EQ(lctree.locate_id(12), lctree.locate_id(13));
        EXPECT_EQ(lctree.locate_id(13), lctree.locate_id(14));
        EXPECT_EQ(lctree.locate_id(14), lctree.locate_id(15));
        EXPECT_TRUE(lctree.locate_id(0)->prev->leaf);
        EXPECT_TRUE(lctree.locate_id(0)->prev->chunk_list->empty());
        EXPECT_EQ(lctree.locate_id(0)->prev->prev, nullptr);
        for (int i=0; i<4; ++i) {
            auto lc_bitmap = lctree.get_lifecycle(i);
            EXPECT_TRUE(lc_bitmap);
            EXPECT_EQ(lc_bitmap->size(), 0);
        }
        for (int i=10; i<16; ++i) {
            auto lc_bitmap = lctree.get_lifecycle(i);
            EXPECT_TRUE(lc_bitmap);
            EXPECT_EQ(lc_bitmap->size(), 0);
        }
    }
    TEST_F(LCTreeTest3, DestructionV21) {
        lctree.delete_gray(2);
        lctree.delete_gray(1);
        EXPECT_EQ(lctree.num_of_inners(), 1);
        EXPECT_EQ(lctree.num_of_leaves(), 2);
        EXPECT_EQ(lctree.num_of_empty_leaves(), 1);
        EXPECT_TRUE(lctree.locate_id(0)->leaf);
        EXPECT_EQ(lctree.locate_id(0), lctree.locate_id(1));
        EXPECT_EQ(lctree.locate_id(1), lctree.locate_id(2));
        EXPECT_EQ(lctree.locate_id(2), lctree.locate_id(3));
        EXPECT_EQ(lctree.locate_id(3), lctree.locate_id(10));
        EXPECT_EQ(lctree.locate_id(10), lctree.locate_id(11));
        EXPECT_EQ(lctree.locate_id(11), lctree.locate_id(12));
        EXPECT_EQ(lctree.locate_id(12), lctree.locate_id(13));
        EXPECT_EQ(lctree.locate_id(13), lctree.locate_id(14));
        EXPECT_EQ(lctree.locate_id(14), lctree.locate_id(15));
        EXPECT_TRUE(lctree.locate_id(0)->prev->leaf);
        EXPECT_TRUE(lctree.locate_id(0)->prev->chunk_list->empty());
        EXPECT_EQ(lctree.locate_id(0)->prev->prev, nullptr);
        for (int i=0; i<4; ++i) {
            auto lc_bitmap = lctree.get_lifecycle(i);
            EXPECT_TRUE(lc_bitmap);
            EXPECT_EQ(lc_bitmap->size(), 0);
        }
        for (int i=10; i<16; ++i) {
            auto lc_bitmap = lctree.get_lifecycle(i);
            EXPECT_TRUE(lc_bitmap);
            EXPECT_EQ(lc_bitmap->size(), 0);
        }
    }
    TEST_F(LCTreeTest3, DestructionETIngestion) {
        lctree.delete_gray(1);
        lctree.ingest_gray({20, 21, 22, 23, 24, 25, 26, 27}, {16, 16, 16, 16, 16, 16, 16, 16}, {0, 1, 2});
        lctree.ingest_gray({28, 29, 30, 31}, {16, 16, 16, 16}, {3, 4, 5, 10, 15, 16, 17});
        EXPECT_EQ(lctree.num_of_leaves(), 11);
        EXPECT_EQ(lctree.num_of_inners(), 10);
        EXPECT_EQ(lctree.num_of_empty_leaves(), 3);
        EXPECT_TRUE(lctree.locate_id(6)->leaf);
        EXPECT_EQ(lctree.locate_id(6)->next, nullptr);
        EXPECT_EQ(lctree.locate_id(6), lctree.locate_id(7));
        EXPECT_EQ(lctree.locate_id(7), lctree.locate_id(18));
        EXPECT_EQ(lctree.locate_id(18), lctree.locate_id(19));
        EXPECT_TRUE(lctree.locate_id(4)->leaf);
        EXPECT_EQ(lctree.locate_id(4)->next, lctree.locate_id(6));
        EXPECT_EQ(lctree.locate_id(6)->prev, lctree.locate_id(4));
        EXPECT_EQ(lctree.locate_id(4), lctree.locate_id(5));
        EXPECT_EQ(lctree.locate_id(5), lctree.locate_id(16));
        EXPECT_EQ(lctree.locate_id(16), lctree.locate_id(17));
        EXPECT_TRUE(lctree.locate_id(4)->prev->leaf);
        EXPECT_TRUE(lctree.locate_id(4)->prev->chunk_list->empty());
        EXPECT_TRUE(lctree.locate_id(0)->leaf);
        EXPECT_EQ(lctree.locate_id(0)->next->next, lctree.locate_id(4));
        EXPECT_EQ(lctree.locate_id(4)->prev->prev, lctree.locate_id(0));
        EXPECT_EQ(lctree.locate_id(0), lctree.locate_id(1));
        EXPECT_EQ(lctree.locate_id(1), lctree.locate_id(2));
        EXPECT_TRUE(lctree.locate_id(3)->leaf);
        EXPECT_EQ(lctree.locate_id(3)->next, lctree.locate_id(0));
        EXPECT_EQ(lctree.locate_id(0)->prev, lctree.locate_id(3));
        EXPECT_EQ(lctree.locate_id(3), lctree.locate_id(15));
        EXPECT_TRUE(lctree.locate_id(12)->leaf);
        EXPECT_EQ(lctree.locate_id(12)->next, lctree.locate_id(3));
        EXPECT_EQ(lctree.locate_id(3)->prev, lctree.locate_id(12));
        EXPECT_EQ(lctree.locate_id(12), lctree.locate_id(13));
        EXPECT_EQ(lctree.locate_id(13), lctree.locate_id(14));
        EXPECT_TRUE(lctree.locate_id(10)->leaf);
        EXPECT_EQ(lctree.locate_id(10)->next, lctree.locate_id(12));
        EXPECT_EQ(lctree.locate_id(12)->prev, lctree.locate_id(10));
        EXPECT_EQ(lctree.locate_id(10), lctree.locate_id(11));
        EXPECT_TRUE(lctree.locate_id(20)->leaf);
        EXPECT_EQ(lctree.locate_id(20)->next, lctree.locate_id(10));
        EXPECT_EQ(lctree.locate_id(10)->prev, lctree.locate_id(20));
        EXPECT_EQ(lctree.locate_id(20), lctree.locate_id(21));
        EXPECT_EQ(lctree.locate_id(21), lctree.locate_id(22));
        EXPECT_EQ(lctree.locate_id(22), lctree.locate_id(23));
        EXPECT_EQ(lctree.locate_id(23), lctree.locate_id(24));
        EXPECT_EQ(lctree.locate_id(24), lctree.locate_id(25));
        EXPECT_EQ(lctree.locate_id(25), lctree.locate_id(26));
        EXPECT_EQ(lctree.locate_id(26), lctree.locate_id(27));
        EXPECT_TRUE(lctree.locate_id(20)->prev->leaf);
        EXPECT_TRUE(lctree.locate_id(20)->prev->chunk_list->empty());
        EXPECT_TRUE(lctree.locate_id(28)->leaf);
        EXPECT_EQ(lctree.locate_id(28)->next->next, lctree.locate_id(20));
        EXPECT_EQ(lctree.locate_id(20)->prev->prev, lctree.locate_id(28));
        EXPECT_EQ(lctree.locate_id(28), lctree.locate_id(29));
        EXPECT_EQ(lctree.locate_id(29), lctree.locate_id(30));
        EXPECT_EQ(lctree.locate_id(30), lctree.locate_id(31));
        EXPECT_TRUE(lctree.locate_id(28)->prev->leaf);
        EXPECT_TRUE(lctree.locate_id(28)->prev->chunk_list->empty());
        EXPECT_EQ(lctree.locate_id(28)->prev->prev, nullptr);
        bool expect_find[32] = {true, true, true, true, true, true, true, true, false, false, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true};
        int expect_size[32] = {1, 1, 1, 0, 0, 0, 0, 0, -1, -1, 2, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
        int8_t expect[32] = {0, 0, 0, -1, -1, -1, -1, -1, -1, -1, 0b0000'0010, 0, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1};
        for (int i=0; i<20; ++i) {
            auto lc_bitmap = lctree.get_lifecycle(i);
            EXPECT_EQ((bool) lc_bitmap, expect_find[i]);
            if (lc_bitmap) {
                EXPECT_EQ(lc_bitmap->size(), expect_size[i]);
                if (expect_size[i] > 0)
                    EXPECT_EQ(lc_bitmap->get_byte(0), expect[i]);
            }
        }
    }
}
