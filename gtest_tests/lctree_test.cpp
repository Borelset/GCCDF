//
// Created by ldb on 8/15/23.
//

#include <random>
#include <cstdint>
#include "gtest/gtest.h"
#include "../MetadataManager/MetadataManager.h"

namespace {
    class LCTreeTest1 : public ::testing::Test {
    protected:
        void SetUp() override {
            std::random_device dev;
            std::mt19937_64 rd(dev());
            std::uniform_int_distribution<std::mt19937_64::result_type> dist(0, UINT64_MAX);
            for (int i=0; i<28; ++i) {
                SHA1FP sha1fp;
                sha1fp.fp1 = i;
                sha1fp.fp2 = dist(rd);
                sha1fp.fp3 = dist(rd);
                sha1fp.fp4 = dist(rd);
                fp.push_back(sha1fp);
            }
            for (int i=0; i<16; ++i)
                sz.push_back(16);
            lctree.ingest_new_version(std::vector<SHA1FP>{fp[0], fp[1], fp[2], fp[3], fp[4], fp[5], fp[6], fp[7], fp[8], fp[9], fp[10], fp[11], fp[12], fp[13], fp[14], fp[15]}, sz, std::vector<SHA1FP>{});
            lctree.ingest_new_version(std::vector<SHA1FP>{fp[16], fp[17], fp[18], fp[19], fp[20], fp[21], fp[22], fp[23]}, std::vector<uint64_t>{16, 16, 16, 16, 16, 16, 16, 16}, std::vector<SHA1FP>{fp[0], fp[1], fp[2], fp[3], fp[4], fp[5], fp[6], fp[7]});
            lctree.ingest_new_version(std::vector<SHA1FP>{fp[24], fp[25], fp[26], fp[27]}, std::vector<uint64_t>{16, 16, 16, 16}, std::vector<SHA1FP>{fp[0], fp[1], fp[2], fp[3], fp[8], fp[9], fp[10], fp[11], fp[16], fp[17], fp[18], fp[19]});
        }
        std::vector<SHA1FP> fp;
        std::vector<uint64_t> sz;
        ScalableChunkLifeCycle lctree{64};
    };
    class LCTreeTest2 : public ::testing::Test {
    protected:
        void SetUp() override {
            for (int i=0; i<20; ++i) {
                SHA1FP sha1fp;
                sha1fp.fp1 = i;
                sha1fp.fp2 = dist(rd);
                sha1fp.fp3 = dist(rd);
                sha1fp.fp4 = dist(rd);
                fp.push_back(sha1fp);
            }
            lctree.ingest_new_version(std::vector<SHA1FP>{fp[0], fp[1], fp[2], fp[3], fp[4], fp[5], fp[6], fp[7], fp[8], fp[9], fp[10], fp[11]}, std::vector<uint64_t>{16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16}, std::vector<SHA1FP>{});
            lctree.ingest_new_version(std::vector<SHA1FP>{fp[12], fp[13], fp[14], fp[15]}, std::vector<uint64_t>{16, 16, 16, 16}, std::vector<SHA1FP>{fp[0], fp[1], fp[2], fp[3]});
            lctree.ingest_new_version(std::vector<SHA1FP>{fp[16], fp[17], fp[18], fp[19]}, std::vector<uint64_t>{16, 16, 16, 16}, std::vector<SHA1FP>{fp[0], fp[1], fp[4], fp[5], fp[6], fp[7], fp[12], fp[13]});
        }
        std::vector<SHA1FP> fp;
        std::vector<uint64_t> sz;
        ScalableChunkLifeCycle lctree{64};
        std::random_device dev;
        std::mt19937_64 rd{dev()};
        std::uniform_int_distribution<std::mt19937_64::result_type> dist{0, UINT64_MAX};
    };
    class LCTreeTest3 : public ::testing::Test {
    protected:
        void SetUp() override {
            for (int i=0; i<20; ++i) {
                SHA1FP sha1fp;
                sha1fp.fp1 = i;
                sha1fp.fp2 = dist(rd);
                sha1fp.fp3 = dist(rd);
                sha1fp.fp4 = dist(rd);
                fp.push_back(sha1fp);
            }
            lctree.ingest_new_version(std::vector<SHA1FP>{fp[0], fp[1], fp[2], fp[3], fp[4], fp[5], fp[6], fp[7], fp[8], fp[9], fp[10], fp[11]}, std::vector<uint64_t>{16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16}, std::vector<SHA1FP>{});
            lctree.ingest_new_version(std::vector<SHA1FP>{fp[12], fp[13], fp[14], fp[15], fp[16], fp[17], fp[18], fp[19]}, std::vector<uint64_t>{16, 16, 16, 16, 16, 16, 16, 16}, std::vector<SHA1FP>{fp[0], fp[1], fp[2], fp[3], fp[4], fp[5], fp[6], fp[7]});
            lctree.ingest_new_version(std::vector<SHA1FP>{}, std::vector<uint64_t>{}, std::vector<SHA1FP>{fp[0], fp[1], fp[2], fp[3], fp[10], fp[11], fp[12], fp[13], fp[14], fp[15]});
        }
        std::vector<SHA1FP> fp;
        std::vector<uint64_t> sz;
        ScalableChunkLifeCycle lctree{64};
        std::random_device dev;
        std::mt19937_64 rd{dev()};
        std::uniform_int_distribution<std::mt19937_64::result_type> dist{0, UINT64_MAX};
    };
    TEST_F(LCTreeTest1, Init) {
        EXPECT_EQ(lctree.num_of_leaves(), 8);
        EXPECT_EQ(lctree.num_of_inners(), 7);
        EXPECT_EQ(lctree.num_of_empty_leaves(), 1);
        EXPECT_EQ(lctree.locate_sha1fp(fp[0])->next, nullptr);
        EXPECT_EQ(lctree.locate_sha1fp(fp[0]), lctree.locate_sha1fp(fp[1]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[1]), lctree.locate_sha1fp(fp[2]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[2]), lctree.locate_sha1fp(fp[3]));
        EXPECT_TRUE(lctree.locate_sha1fp(fp[0])->leaf);
        EXPECT_EQ(lctree.locate_sha1fp(fp[4])->next, lctree.locate_sha1fp(fp[0]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[0])->prev, lctree.locate_sha1fp(fp[4]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[4]), lctree.locate_sha1fp(fp[5]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[5]), lctree.locate_sha1fp(fp[6]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[6]), lctree.locate_sha1fp(fp[7]));
        EXPECT_TRUE(lctree.locate_sha1fp(fp[4])->leaf);
        EXPECT_EQ(lctree.locate_sha1fp(fp[8])->next, lctree.locate_sha1fp(fp[4]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[4])->prev, lctree.locate_sha1fp(fp[8]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[8]), lctree.locate_sha1fp(fp[9]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[9]), lctree.locate_sha1fp(fp[10]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[10]), lctree.locate_sha1fp(fp[11]));
        EXPECT_TRUE(lctree.locate_sha1fp(fp[8])->leaf);
        EXPECT_EQ(lctree.locate_sha1fp(fp[12])->next, lctree.locate_sha1fp(fp[8]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[8])->prev, lctree.locate_sha1fp(fp[12]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[12]), lctree.locate_sha1fp(fp[13]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[13]), lctree.locate_sha1fp(fp[14]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[14]), lctree.locate_sha1fp(fp[15]));
        EXPECT_TRUE(lctree.locate_sha1fp(fp[12])->leaf);
        EXPECT_EQ(lctree.locate_sha1fp(fp[16])->next, lctree.locate_sha1fp(fp[12]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[12])->prev, lctree.locate_sha1fp(fp[16]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[16]), lctree.locate_sha1fp(fp[17]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[17]), lctree.locate_sha1fp(fp[18]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[18]), lctree.locate_sha1fp(fp[19]));
        EXPECT_TRUE(lctree.locate_sha1fp(fp[16])->leaf);
        EXPECT_EQ(lctree.locate_sha1fp(fp[20])->next, lctree.locate_sha1fp(fp[16]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[16])->prev, lctree.locate_sha1fp(fp[20]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[20]), lctree.locate_sha1fp(fp[21]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[21]), lctree.locate_sha1fp(fp[22]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[22]), lctree.locate_sha1fp(fp[23]));
        EXPECT_TRUE(lctree.locate_sha1fp(fp[20])->leaf);
        EXPECT_EQ(lctree.locate_sha1fp(fp[24])->next, lctree.locate_sha1fp(fp[20]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[20])->prev, lctree.locate_sha1fp(fp[24]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[24]), lctree.locate_sha1fp(fp[25]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[25]), lctree.locate_sha1fp(fp[26]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[26]), lctree.locate_sha1fp(fp[27]));
        EXPECT_TRUE(lctree.locate_sha1fp(fp[24])->leaf);
        EXPECT_TRUE(lctree.locate_sha1fp(fp[24])->prev->chunk_list->empty());
        for (const auto fp_item : fp) {
            VariableBitset lc_bitmap;
            bool find = lctree.get_lifecycle(fp_item, lc_bitmap);
            EXPECT_TRUE(find);
            EXPECT_EQ(lc_bitmap.size(), 0);
        }
    }
    TEST_F(LCTreeTest1, Destruction) {
        lctree.delete_version(4);
        lctree.delete_version(5);
        lctree.delete_version(5);
        lctree.delete_version(4);
        lctree.delete_version(3);
        EXPECT_EQ(lctree.num_of_inners(), 3);
        EXPECT_EQ(lctree.num_of_leaves(), 4);
        EXPECT_EQ(lctree.num_of_empty_leaves(), 1);
        EXPECT_TRUE(lctree.locate_sha1fp(fp[0])->leaf);
        EXPECT_EQ(lctree.locate_sha1fp(fp[0])->next, nullptr);
        EXPECT_EQ(lctree.locate_sha1fp(fp[0]), lctree.locate_sha1fp(fp[1]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[1]), lctree.locate_sha1fp(fp[2]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[2]), lctree.locate_sha1fp(fp[3]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[3]), lctree.locate_sha1fp(fp[4]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[4]), lctree.locate_sha1fp(fp[5]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[5]), lctree.locate_sha1fp(fp[6]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[6]), lctree.locate_sha1fp(fp[7]));
        EXPECT_TRUE(lctree.locate_sha1fp(fp[8])->leaf);
        EXPECT_EQ(lctree.locate_sha1fp(fp[8])->next, lctree.locate_sha1fp(fp[7]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[7])->prev, lctree.locate_sha1fp(fp[8]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[8]), lctree.locate_sha1fp(fp[9]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[9]), lctree.locate_sha1fp(fp[10]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[10]), lctree.locate_sha1fp(fp[11]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[11]), lctree.locate_sha1fp(fp[12]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[12]), lctree.locate_sha1fp(fp[13]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[13]), lctree.locate_sha1fp(fp[14]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[14]), lctree.locate_sha1fp(fp[15]));
        EXPECT_TRUE(lctree.locate_sha1fp(fp[16])->leaf);
        EXPECT_EQ(lctree.locate_sha1fp(fp[16])->next, lctree.locate_sha1fp(fp[15]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[15])->prev, lctree.locate_sha1fp(fp[16]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[16]), lctree.locate_sha1fp(fp[17]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[17]), lctree.locate_sha1fp(fp[18]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[18]), lctree.locate_sha1fp(fp[19]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[19]), lctree.locate_sha1fp(fp[20]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[20]), lctree.locate_sha1fp(fp[21]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[21]), lctree.locate_sha1fp(fp[22]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[22]), lctree.locate_sha1fp(fp[23]));
        EXPECT_TRUE(lctree.locate_sha1fp(fp[23])->prev->leaf);
        EXPECT_TRUE(lctree.locate_sha1fp(fp[23])->prev->chunk_list->empty());
        lctree.delete_version(2);
        EXPECT_EQ(lctree.num_of_inners(), 1);
        EXPECT_EQ(lctree.num_of_leaves(), 2);
        EXPECT_EQ(lctree.num_of_empty_leaves(), 1);
        lctree.delete_version(1);
        EXPECT_EQ(lctree.num_of_inners(), 1);
        EXPECT_EQ(lctree.num_of_leaves(), 0);
        EXPECT_EQ(lctree.num_of_empty_leaves(), 0);
    }
    TEST_F(LCTreeTest2, Init) {
        EXPECT_EQ(lctree.num_of_leaves(), 6);
        EXPECT_EQ(lctree.num_of_inners(), 5);
        EXPECT_EQ(lctree.num_of_empty_leaves(), 1);
        EXPECT_TRUE(lctree.locate_sha1fp(fp[0])->leaf);
        EXPECT_EQ(lctree.locate_sha1fp(fp[0])->next, nullptr);
        EXPECT_EQ(lctree.locate_sha1fp(fp[0]), lctree.locate_sha1fp(fp[1]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[1]), lctree.locate_sha1fp(fp[2]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[2]), lctree.locate_sha1fp(fp[3]));
        EXPECT_TRUE(lctree.locate_sha1fp(fp[4])->leaf);
        EXPECT_EQ(lctree.locate_sha1fp(fp[4])->next, lctree.locate_sha1fp(fp[0]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[0])->prev, lctree.locate_sha1fp(fp[4]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[4]), lctree.locate_sha1fp(fp[5]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[5]), lctree.locate_sha1fp(fp[6]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[6]), lctree.locate_sha1fp(fp[7]));
        EXPECT_TRUE(lctree.locate_sha1fp(fp[8])->leaf);
        EXPECT_EQ(lctree.locate_sha1fp(fp[8])->next, lctree.locate_sha1fp(fp[4]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[4])->prev, lctree.locate_sha1fp(fp[8]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[8]), lctree.locate_sha1fp(fp[9]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[9]), lctree.locate_sha1fp(fp[10]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[10]), lctree.locate_sha1fp(fp[11]));
        EXPECT_TRUE(lctree.locate_sha1fp(fp[12])->leaf);
        EXPECT_EQ(lctree.locate_sha1fp(fp[12])->next, lctree.locate_sha1fp(fp[8]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[8])->prev, lctree.locate_sha1fp(fp[12]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[12]), lctree.locate_sha1fp(fp[13]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[13]), lctree.locate_sha1fp(fp[14]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[14]), lctree.locate_sha1fp(fp[15]));
        EXPECT_TRUE(lctree.locate_sha1fp(fp[16])->leaf);
        EXPECT_EQ(lctree.locate_sha1fp(fp[16])->next, lctree.locate_sha1fp(fp[12]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[12])->prev, lctree.locate_sha1fp(fp[16]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[16]), lctree.locate_sha1fp(fp[17]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[17]), lctree.locate_sha1fp(fp[18]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[18]), lctree.locate_sha1fp(fp[19]));
        EXPECT_TRUE(lctree.locate_sha1fp(fp[16])->prev->leaf);
        EXPECT_TRUE(lctree.locate_sha1fp(fp[16])->prev->chunk_list->empty());
        int expect[20] = {1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0};
        for (int i=0; i<20; ++i) {
            VariableBitset lc_bitmap;
            bool find = lctree.get_lifecycle(fp[i], lc_bitmap);
            EXPECT_TRUE(find);
            EXPECT_EQ(lc_bitmap.size(), expect[i]);
        }
    }
    TEST_F(LCTreeTest2, DestructionV3) {
        lctree.delete_version(3);
        EXPECT_EQ(lctree.num_of_inners(), 3);
        EXPECT_EQ(lctree.num_of_leaves(), 4);
        EXPECT_EQ(lctree.num_of_empty_leaves(), 1);
        EXPECT_TRUE(lctree.locate_sha1fp(fp[0])->leaf);
        EXPECT_EQ(lctree.locate_sha1fp(fp[0])->next, nullptr);
        EXPECT_EQ(lctree.locate_sha1fp(fp[0]), lctree.locate_sha1fp(fp[1]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[1]), lctree.locate_sha1fp(fp[2]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[2]), lctree.locate_sha1fp(fp[3]));
        EXPECT_TRUE(lctree.locate_sha1fp(fp[4])->leaf);
        EXPECT_EQ(lctree.locate_sha1fp(fp[4])->next, lctree.locate_sha1fp(fp[0]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[0])->prev, lctree.locate_sha1fp(fp[4]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[4]), lctree.locate_sha1fp(fp[5]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[5]), lctree.locate_sha1fp(fp[6]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[6]), lctree.locate_sha1fp(fp[7]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[8]), lctree.locate_sha1fp(fp[9]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[9]), lctree.locate_sha1fp(fp[10]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[10]), lctree.locate_sha1fp(fp[11]));
        EXPECT_TRUE(lctree.locate_sha1fp(fp[12])->leaf);
        EXPECT_EQ(lctree.locate_sha1fp(fp[12])->next, lctree.locate_sha1fp(fp[8]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[8])->prev, lctree.locate_sha1fp(fp[12]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[12]), lctree.locate_sha1fp(fp[13]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[13]), lctree.locate_sha1fp(fp[14]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[14]), lctree.locate_sha1fp(fp[15]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[16]), lctree.locate_sha1fp(fp[17]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[17]), lctree.locate_sha1fp(fp[18]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[18]), lctree.locate_sha1fp(fp[19]));
        EXPECT_TRUE(lctree.locate_sha1fp(fp[12])->prev->leaf);
        EXPECT_TRUE(lctree.locate_sha1fp(fp[12])->prev->chunk_list->empty());
        VariableBitset lc_bitmap;
        for (int i=0; i<16; ++i) {
            VariableBitset lc_bitmap;
            bool find = lctree.get_lifecycle(fp[i], lc_bitmap);
            EXPECT_TRUE(find);
            EXPECT_EQ(lc_bitmap.size(), 0);
        }
        for (int i=17; i<20; ++i) {
            VariableBitset lc_bitmap;
            bool find = lctree.get_lifecycle(fp[i], lc_bitmap);
            EXPECT_FALSE(find);
        }
    }
    TEST_F(LCTreeTest2, DestructionV2) {
        lctree.delete_version(2);
        EXPECT_EQ(lctree.num_of_inners(), 3);
        EXPECT_EQ(lctree.num_of_leaves(), 4);
        EXPECT_EQ(lctree.num_of_empty_leaves(), 1);
        EXPECT_TRUE(lctree.locate_sha1fp(fp[0])->leaf);
        EXPECT_EQ(lctree.locate_sha1fp(fp[0])->next, nullptr);
        EXPECT_EQ(lctree.locate_sha1fp(fp[0]), lctree.locate_sha1fp(fp[1]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[1]), lctree.locate_sha1fp(fp[4]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[4]), lctree.locate_sha1fp(fp[5]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[5]), lctree.locate_sha1fp(fp[6]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[6]), lctree.locate_sha1fp(fp[7]));
        EXPECT_TRUE(lctree.locate_sha1fp(fp[2])->leaf);
        EXPECT_EQ(lctree.locate_sha1fp(fp[2])->next, lctree.locate_sha1fp(fp[0]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[0])->prev, lctree.locate_sha1fp(fp[2]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[2]), lctree.locate_sha1fp(fp[3]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[3]), lctree.locate_sha1fp(fp[8]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[8]), lctree.locate_sha1fp(fp[9]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[9]), lctree.locate_sha1fp(fp[10]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[10]), lctree.locate_sha1fp(fp[11]));
        EXPECT_TRUE(lctree.locate_sha1fp(fp[12])->leaf);
        EXPECT_EQ(lctree.locate_sha1fp(fp[12])->next, lctree.locate_sha1fp(fp[2]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[2])->prev, lctree.locate_sha1fp(fp[12]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[12]), lctree.locate_sha1fp(fp[13]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[13]), lctree.locate_sha1fp(fp[16]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[16]), lctree.locate_sha1fp(fp[17]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[17]), lctree.locate_sha1fp(fp[18]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[18]), lctree.locate_sha1fp(fp[19]));
        EXPECT_TRUE(lctree.locate_sha1fp(fp[12])->prev->leaf);
        EXPECT_TRUE(lctree.locate_sha1fp(fp[12])->prev->chunk_list->empty());
        EXPECT_EQ(lctree.locate_sha1fp(fp[12])->prev->prev, nullptr);
        bool expect_find[20] = {true, true, true, true, true, true, true, true, true, true, true, true, true, true, false, false, true, true, true, true};
        for (int i=0; i<20; ++i) {
            VariableBitset lc_bitmap;
            bool find = lctree.get_lifecycle(fp[i], lc_bitmap);
            EXPECT_EQ(find, expect_find[i]);
            if (find)
                EXPECT_EQ(lc_bitmap.size(), 0);
        }
    }
    TEST_F(LCTreeTest2, DestructionV1) {
        lctree.delete_version(1);
        EXPECT_EQ(lctree.num_of_inners(), 2);
        EXPECT_EQ(lctree.num_of_leaves(), 3);
        EXPECT_EQ(lctree.num_of_empty_leaves(), 1);
        EXPECT_TRUE(lctree.locate_sha1fp(fp[0])->leaf);
        EXPECT_EQ(lctree.locate_sha1fp(fp[0])->next, nullptr);
        EXPECT_EQ(lctree.locate_sha1fp(fp[0]), lctree.locate_sha1fp(fp[1]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[1]), lctree.locate_sha1fp(fp[2]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[2]), lctree.locate_sha1fp(fp[3]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[3]), lctree.locate_sha1fp(fp[12]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[12]), lctree.locate_sha1fp(fp[13]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[13]), lctree.locate_sha1fp(fp[14]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[14]), lctree.locate_sha1fp(fp[15]));
        EXPECT_TRUE(lctree.locate_sha1fp(fp[4])->leaf);
        EXPECT_EQ(lctree.locate_sha1fp(fp[4])->next, lctree.locate_sha1fp(fp[0]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[0])->prev, lctree.locate_sha1fp(fp[4]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[4]), lctree.locate_sha1fp(fp[5]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[5]), lctree.locate_sha1fp(fp[6]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[6]), lctree.locate_sha1fp(fp[7]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[7]), lctree.locate_sha1fp(fp[16]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[16]), lctree.locate_sha1fp(fp[17]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[17]), lctree.locate_sha1fp(fp[18]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[18]), lctree.locate_sha1fp(fp[19]));
        EXPECT_TRUE(lctree.locate_sha1fp(fp[4])->prev->leaf);
        EXPECT_TRUE(lctree.locate_sha1fp(fp[4])->prev->chunk_list->empty());
        EXPECT_EQ(lctree.locate_sha1fp(fp[4])->prev->prev, nullptr);
        VariableBitset lc_bitmap;
        lctree.get_lifecycle(fp[0], lc_bitmap);
        EXPECT_EQ(lc_bitmap.get_byte(0), 0b00000001);
        lctree.get_lifecycle(fp[1], lc_bitmap);
        EXPECT_EQ(lc_bitmap.get_byte(0), 0b00000001);
        lctree.get_lifecycle(fp[2], lc_bitmap);
        EXPECT_EQ(lc_bitmap.get_byte(0), 0b00000000);
        lctree.get_lifecycle(fp[3], lc_bitmap);
        EXPECT_EQ(lc_bitmap.get_byte(0), 0b00000000);
        lctree.get_lifecycle(fp[12], lc_bitmap);
        EXPECT_EQ(lc_bitmap.get_byte(0), 0b00000001);
        lctree.get_lifecycle(fp[13], lc_bitmap);
        EXPECT_EQ(lc_bitmap.get_byte(0), 0b00000001);
        lctree.get_lifecycle(fp[14], lc_bitmap);
        EXPECT_EQ(lc_bitmap.get_byte(0), 0b00000000);
        lctree.get_lifecycle(fp[15], lc_bitmap);
        EXPECT_EQ(lc_bitmap.get_byte(0), 0b00000000);
        lctree.get_lifecycle(fp[4], lc_bitmap);
        EXPECT_EQ(lc_bitmap.size(), 0);
        lctree.get_lifecycle(fp[5], lc_bitmap);
        EXPECT_EQ(lc_bitmap.size(), 0);
        lctree.get_lifecycle(fp[6], lc_bitmap);
        EXPECT_EQ(lc_bitmap.size(), 0);
        lctree.get_lifecycle(fp[7], lc_bitmap);
        EXPECT_EQ(lc_bitmap.size(), 0);
        lctree.get_lifecycle(fp[16], lc_bitmap);
        EXPECT_EQ(lc_bitmap.size(), 0);
        lctree.get_lifecycle(fp[17], lc_bitmap);
        EXPECT_EQ(lc_bitmap.size(), 0);
        lctree.get_lifecycle(fp[18], lc_bitmap);
        EXPECT_EQ(lc_bitmap.size(), 0);
        lctree.get_lifecycle(fp[19], lc_bitmap);
        EXPECT_EQ(lc_bitmap.size(), 0);
        bool expect_find[20] = {true, true, true, true, true, true, true, true, false, false, false, false, true, true, true, true, true, true, true, true};
        int expect[20] = {1, 1, 1, 1, 0, 0, 0, 0, -1, -1, -1, -1, 1, 1, 1, 1, 0, 0, 0, 0};
        for (int i=0; i<20; ++i) {
            VariableBitset lc_bitmap;
            bool find = lctree.get_lifecycle(fp[i], lc_bitmap);
            EXPECT_EQ(find, expect_find[i]);
            if (find)
                EXPECT_EQ(lc_bitmap.size(), expect[i]);
        }
    }
    TEST_F(LCTreeTest2, DestructionV12) {
        EXPECT_EQ(lctree.num_of_inners(), 5);
        EXPECT_EQ(lctree.num_of_leaves(), 6);
        EXPECT_EQ(lctree.num_of_empty_leaves(), 1);
        lctree.delete_version(1);
        EXPECT_EQ(lctree.num_of_inners(), 2);
        EXPECT_EQ(lctree.num_of_leaves(), 3);
        EXPECT_EQ(lctree.num_of_empty_leaves(), 1);
        lctree.delete_version(2);
        EXPECT_EQ(lctree.num_of_inners(), 1);
        EXPECT_EQ(lctree.num_of_leaves(), 2);
        EXPECT_EQ(lctree.num_of_empty_leaves(), 1);
        EXPECT_TRUE(lctree.locate_sha1fp(fp[0])->leaf);
        EXPECT_EQ(lctree.locate_sha1fp(fp[0]), lctree.locate_sha1fp(fp[1]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[1]), lctree.locate_sha1fp(fp[4]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[4]), lctree.locate_sha1fp(fp[5]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[5]), lctree.locate_sha1fp(fp[6]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[6]), lctree.locate_sha1fp(fp[7]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[7]), lctree.locate_sha1fp(fp[12]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[12]), lctree.locate_sha1fp(fp[13]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[13]), lctree.locate_sha1fp(fp[16]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[16]), lctree.locate_sha1fp(fp[17]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[17]), lctree.locate_sha1fp(fp[18]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[18]), lctree.locate_sha1fp(fp[19]));
        EXPECT_TRUE(lctree.locate_sha1fp(fp[0])->prev->leaf);
        EXPECT_TRUE(lctree.locate_sha1fp(fp[0])->prev->chunk_list->empty());
        EXPECT_EQ(lctree.locate_sha1fp(fp[0])->prev->prev, nullptr);
        bool expect_find[20] = {true, true, false, false, true, true, true, true, false, false, false, false, true, true, false, false, true, true, true, true};
        int expect[20] = {0, 0, -1, -1, 0, 0, 0, 0, -1, -1, -1, -1, 0, 0, -1, -1, 0, 0, 0, 0};
        for (int i=0; i<20; ++i) {
            VariableBitset lc_bitmap;
            bool find = lctree.get_lifecycle(fp[i], lc_bitmap);
            EXPECT_EQ(find, expect_find[i]);
            if (find)
                EXPECT_EQ(lc_bitmap.size(), expect[i]);
        }
    }
    TEST_F(LCTreeTest2, DestructionV21) {
        lctree.delete_version(2);
        lctree.delete_version(1);
        EXPECT_EQ(lctree.num_of_inners(), 1);
        EXPECT_EQ(lctree.num_of_leaves(), 2);
        EXPECT_EQ(lctree.num_of_empty_leaves(), 1);
        EXPECT_TRUE(lctree.locate_sha1fp(fp[0])->leaf);
        EXPECT_EQ(lctree.locate_sha1fp(fp[0]), lctree.locate_sha1fp(fp[1]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[1]), lctree.locate_sha1fp(fp[4]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[4]), lctree.locate_sha1fp(fp[5]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[5]), lctree.locate_sha1fp(fp[6]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[6]), lctree.locate_sha1fp(fp[7]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[7]), lctree.locate_sha1fp(fp[12]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[12]), lctree.locate_sha1fp(fp[13]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[13]), lctree.locate_sha1fp(fp[16]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[16]), lctree.locate_sha1fp(fp[17]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[17]), lctree.locate_sha1fp(fp[18]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[18]), lctree.locate_sha1fp(fp[19]));
        EXPECT_TRUE(lctree.locate_sha1fp(fp[0])->prev->leaf);
        EXPECT_TRUE(lctree.locate_sha1fp(fp[0])->prev->chunk_list->empty());
        EXPECT_EQ(lctree.locate_sha1fp(fp[0])->prev->prev, nullptr);
        bool expect_find[20] = {true, true, false, false, true, true, true, true, false, false, false, false, true, true, false, false, true, true, true, true};
        int expect[20] = {0, 0, -1, -1, 0, 0, 0, 0, -1, -1, -1, -1, 0, 0, -1, -1, 0, 0, 0, 0};
        for (int i=0; i<20; ++i) {
            VariableBitset lc_bitmap;
            bool find = lctree.get_lifecycle(fp[i], lc_bitmap);
            EXPECT_EQ(find, expect_find[i]);
            if (find)
                EXPECT_EQ(lc_bitmap.size(), expect[i]);
        }
    }
    TEST_F(LCTreeTest2, DestructionETIngestion) {
        lctree.delete_version(1);
        for (int i=20; i<32; ++i) {
            SHA1FP sha1fp;
            sha1fp.fp1 = i;
            sha1fp.fp2 = (dist(rd));
            sha1fp.fp3 = (dist(rd));
            sha1fp.fp4 = (dist(rd));
            fp.push_back(sha1fp);
        }
        lctree.ingest_new_version(std::vector<SHA1FP>{fp[20], fp[21], fp[22], fp[23], fp[24], fp[25], fp[26], fp[27]}, {16, 16, 16, 16, 16, 16, 16, 16}, std::vector<SHA1FP>{fp[0], fp[1], fp[2]});
        lctree.ingest_new_version(std::vector<SHA1FP>{fp[28], fp[29], fp[30], fp[31]}, {16, 16, 16, 16}, std::vector<SHA1FP>{fp[3], fp[4], fp[5], fp[15], fp[16], fp[17]});
        EXPECT_EQ(lctree.num_of_leaves(), 9);
        EXPECT_EQ(lctree.num_of_inners(), 8);
        EXPECT_EQ(lctree.num_of_empty_leaves(), 3);
        EXPECT_TRUE(lctree.locate_sha1fp(fp[0])->leaf);
        EXPECT_EQ(lctree.locate_sha1fp(fp[0])->next, nullptr);
        EXPECT_EQ(lctree.locate_sha1fp(fp[0]), lctree.locate_sha1fp(fp[1]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[1]), lctree.locate_sha1fp(fp[12]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[12]), lctree.locate_sha1fp(fp[13]));
        EXPECT_TRUE(lctree.locate_sha1fp(fp[2])->leaf);
        EXPECT_EQ(lctree.locate_sha1fp(fp[2])->next, lctree.locate_sha1fp(fp[0]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[0])->prev, lctree.locate_sha1fp(fp[2]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[2]), lctree.locate_sha1fp(fp[3]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[3]), lctree.locate_sha1fp(fp[14]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[14]), lctree.locate_sha1fp(fp[15]));
        EXPECT_TRUE(lctree.locate_sha1fp(fp[4])->leaf);
        EXPECT_TRUE(lctree.locate_sha1fp(fp[2])->prev->leaf);
        EXPECT_TRUE(lctree.locate_sha1fp(fp[2])->prev->chunk_list->empty());
        EXPECT_EQ(lctree.locate_sha1fp(fp[2])->prev->prev, lctree.locate_sha1fp(fp[4]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[4])->next, lctree.locate_sha1fp(fp[2])->prev);
        EXPECT_EQ(lctree.locate_sha1fp(fp[4])->next->next, lctree.locate_sha1fp(fp[2]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[4]), lctree.locate_sha1fp(fp[5]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[5]), lctree.locate_sha1fp(fp[16]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[16]), lctree.locate_sha1fp(fp[17]));
        EXPECT_TRUE(lctree.locate_sha1fp(fp[6])->leaf);
        EXPECT_EQ(lctree.locate_sha1fp(fp[6])->next, lctree.locate_sha1fp(fp[4]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[4])->prev, lctree.locate_sha1fp(fp[6]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[6]), lctree.locate_sha1fp(fp[7]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[7]), lctree.locate_sha1fp(fp[18]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[18]), lctree.locate_sha1fp(fp[19]));
        EXPECT_TRUE(lctree.locate_sha1fp(fp[20])->leaf);
        EXPECT_TRUE(lctree.locate_sha1fp(fp[6])->prev->leaf);
        EXPECT_TRUE(lctree.locate_sha1fp(fp[6])->prev->chunk_list->empty());
        EXPECT_EQ(lctree.locate_sha1fp(fp[6])->prev->prev, lctree.locate_sha1fp(fp[20]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[20])->next, lctree.locate_sha1fp(fp[6])->prev);
        EXPECT_EQ(lctree.locate_sha1fp(fp[20])->next->next, lctree.locate_sha1fp(fp[6]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[20]), lctree.locate_sha1fp(fp[21]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[21]), lctree.locate_sha1fp(fp[22]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[22]), lctree.locate_sha1fp(fp[23]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[23]), lctree.locate_sha1fp(fp[24]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[24]), lctree.locate_sha1fp(fp[25]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[25]), lctree.locate_sha1fp(fp[26]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[26]), lctree.locate_sha1fp(fp[27]));
        EXPECT_TRUE(lctree.locate_sha1fp(fp[28])->leaf);
        EXPECT_EQ(lctree.locate_sha1fp(fp[28])->next, lctree.locate_sha1fp(fp[20]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[20])->prev, lctree.locate_sha1fp(fp[28]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[28]), lctree.locate_sha1fp(fp[29]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[29]), lctree.locate_sha1fp(fp[30]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[30]), lctree.locate_sha1fp(fp[31]));
        EXPECT_TRUE(lctree.locate_sha1fp(fp[28])->prev->leaf);
        EXPECT_TRUE(lctree.locate_sha1fp(fp[28])->prev->chunk_list->empty());
        EXPECT_EQ(lctree.locate_sha1fp(fp[28])->prev->prev, nullptr);
        bool expect_find[32] = {true, true, true, true, true, true, true, true, false, false, false, false, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true};
        int expect_size[32] = {2, 2, 2, 2, 0, 0, 0, 0, -1, -1, -1, -1, 2, 2, 2, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
        int8_t expect[32] = {0b0000'0001, 0b0000'0001, 0b0000'0001, 0b0000'0010, -1, -1, -1, -1, -1, -1, -1, -1, 0, 0, 0, 0b0000'0010, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1};
        for (int i=0; i<20; ++i) {
            VariableBitset lc_bitmap;
            bool find = lctree.get_lifecycle(fp[i], lc_bitmap);
            EXPECT_EQ(find, expect_find[i]);
            if (find) {
                EXPECT_EQ(lc_bitmap.size(), expect_size[i]);
                if (expect_size[i] > 0)
                    EXPECT_EQ(lc_bitmap.get_byte(0), expect[i]);
            }
        }
    }
    TEST_F(LCTreeTest3, Init) {
        EXPECT_EQ(lctree.num_of_leaves(), 7);
        EXPECT_EQ(lctree.num_of_inners(), 6);
        EXPECT_EQ(lctree.num_of_empty_leaves(), 2);
        EXPECT_TRUE(lctree.locate_sha1fp(fp[0])->leaf);
        EXPECT_EQ(lctree.locate_sha1fp(fp[0])->next, nullptr);
        EXPECT_EQ(lctree.locate_sha1fp(fp[0]), lctree.locate_sha1fp(fp[1]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[1]), lctree.locate_sha1fp(fp[2]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[2]), lctree.locate_sha1fp(fp[3]));
        EXPECT_TRUE(lctree.locate_sha1fp(fp[4])->leaf);
        EXPECT_EQ(lctree.locate_sha1fp(fp[4])->next, lctree.locate_sha1fp(fp[0]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[0])->prev, lctree.locate_sha1fp(fp[4]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[4]), lctree.locate_sha1fp(fp[5]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[5]), lctree.locate_sha1fp(fp[6]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[6]), lctree.locate_sha1fp(fp[7]));
        EXPECT_TRUE(lctree.locate_sha1fp(fp[8])->leaf);
        EXPECT_EQ(lctree.locate_sha1fp(fp[8])->next, lctree.locate_sha1fp(fp[4]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[4])->prev, lctree.locate_sha1fp(fp[8]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[8]), lctree.locate_sha1fp(fp[9]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[9]), lctree.locate_sha1fp(fp[10]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[10]), lctree.locate_sha1fp(fp[11]));
        EXPECT_TRUE(lctree.locate_sha1fp(fp[12])->leaf);
        EXPECT_EQ(lctree.locate_sha1fp(fp[12])->next, lctree.locate_sha1fp(fp[8]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[8])->prev, lctree.locate_sha1fp(fp[12]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[12]), lctree.locate_sha1fp(fp[13]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[13]), lctree.locate_sha1fp(fp[14]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[14]), lctree.locate_sha1fp(fp[15]));
        EXPECT_TRUE(lctree.locate_sha1fp(fp[16])->leaf);
        EXPECT_EQ(lctree.locate_sha1fp(fp[16])->next, lctree.locate_sha1fp(fp[12]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[12])->prev, lctree.locate_sha1fp(fp[16]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[16]), lctree.locate_sha1fp(fp[17]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[17]), lctree.locate_sha1fp(fp[18]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[18]), lctree.locate_sha1fp(fp[19]));
        EXPECT_TRUE(lctree.locate_sha1fp(fp[16])->prev->leaf);
        EXPECT_TRUE(lctree.locate_sha1fp(fp[16])->prev->chunk_list->empty());
        EXPECT_NE(lctree.locate_sha1fp(fp[16])->prev->prev, nullptr);
        EXPECT_EQ(lctree.locate_sha1fp(fp[16])->prev->prev->prev, nullptr);
        bool expect_find[20] = {true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true};
        int expect_size[20] = {0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0};
        int8_t expect[20] = {-1, -1 ,-1 , -1, -1, -1, -1, -1, 0, 0, 0b0000'0001, 0b0000'0001, -1, -1, -1, -1, -1, -1, -1, -1};
        for (int i=0; i<20; ++i) {
            VariableBitset lc_bitmap;
            bool find = lctree.get_lifecycle(fp[i], lc_bitmap);
            EXPECT_EQ(find, expect_find[i]);
            if (find) {
                EXPECT_EQ(lc_bitmap.size(), expect_size[i]);
                if (expect_size[i] > 0)
                    EXPECT_EQ(lc_bitmap.get_byte(0), expect[i]);
            }
        }
    }
    TEST_F(LCTreeTest3, DestructionV3) {
        lctree.delete_version(3);
        EXPECT_EQ(lctree.num_of_inners(), 3);
        EXPECT_EQ(lctree.num_of_leaves(), 4);
        EXPECT_EQ(lctree.num_of_empty_leaves(), 1);
        EXPECT_TRUE(lctree.locate_sha1fp(fp[0])->leaf);
        EXPECT_EQ(lctree.locate_sha1fp(fp[0])->next, nullptr);
        EXPECT_EQ(lctree.locate_sha1fp(fp[0]), lctree.locate_sha1fp(fp[1]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[1]), lctree.locate_sha1fp(fp[2]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[2]), lctree.locate_sha1fp(fp[3]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[3]), lctree.locate_sha1fp(fp[4]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[4]), lctree.locate_sha1fp(fp[5]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[5]), lctree.locate_sha1fp(fp[6]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[6]), lctree.locate_sha1fp(fp[7]));
        EXPECT_TRUE(lctree.locate_sha1fp(fp[8])->leaf);
        EXPECT_EQ(lctree.locate_sha1fp(fp[8])->next, lctree.locate_sha1fp(fp[0]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[0])->prev, lctree.locate_sha1fp(fp[8]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[8]), lctree.locate_sha1fp(fp[9]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[9]), lctree.locate_sha1fp(fp[10]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[10]), lctree.locate_sha1fp(fp[11]));
        EXPECT_TRUE(lctree.locate_sha1fp(fp[12])->leaf);
        EXPECT_EQ(lctree.locate_sha1fp(fp[12])->next, lctree.locate_sha1fp(fp[8]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[8])->prev, lctree.locate_sha1fp(fp[12]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[12]), lctree.locate_sha1fp(fp[13]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[13]), lctree.locate_sha1fp(fp[14]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[14]), lctree.locate_sha1fp(fp[15]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[16]), lctree.locate_sha1fp(fp[17]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[17]), lctree.locate_sha1fp(fp[18]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[18]), lctree.locate_sha1fp(fp[19]));
        EXPECT_TRUE(lctree.locate_sha1fp(fp[12])->prev->leaf);
        EXPECT_TRUE(lctree.locate_sha1fp(fp[12])->prev->chunk_list->empty());
        EXPECT_EQ(lctree.locate_sha1fp(fp[12])->prev->prev, nullptr);
        bool expect_find[20] = {true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true};
        int expect_size[20] = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
        int8_t expect[20] = {-1, -1 ,-1 , -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1};
        for (int i=0; i<20; ++i) {
            VariableBitset lc_bitmap;
            bool find = lctree.get_lifecycle(fp[i], lc_bitmap);
            EXPECT_EQ(find, expect_find[i]);
            if (find) {
                EXPECT_EQ(lc_bitmap.size(), expect_size[i]);
                if (expect_size[i] > 0)
                    EXPECT_EQ(lc_bitmap.get_byte(0), expect[i]);
            }
        }
    }
    TEST_F(LCTreeTest3, DestructionV2) {
        lctree.delete_version(2);
        EXPECT_EQ(lctree.num_of_inners(), 3);
        EXPECT_EQ(lctree.num_of_leaves(), 4);
        EXPECT_EQ(lctree.num_of_empty_leaves(), 1);
        EXPECT_TRUE(lctree.locate_sha1fp(fp[0])->leaf);
        EXPECT_EQ(lctree.locate_sha1fp(fp[0])->next, nullptr);
        EXPECT_EQ(lctree.locate_sha1fp(fp[0]), lctree.locate_sha1fp(fp[1]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[1]), lctree.locate_sha1fp(fp[2]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[2]), lctree.locate_sha1fp(fp[3]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[3]), lctree.locate_sha1fp(fp[10]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[10]), lctree.locate_sha1fp(fp[11]));
        EXPECT_TRUE(lctree.locate_sha1fp(fp[4])->leaf);
        EXPECT_EQ(lctree.locate_sha1fp(fp[4])->next, lctree.locate_sha1fp(fp[0]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[0])->prev, lctree.locate_sha1fp(fp[4]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[4]), lctree.locate_sha1fp(fp[5]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[5]), lctree.locate_sha1fp(fp[6]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[6]), lctree.locate_sha1fp(fp[7]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[7]), lctree.locate_sha1fp(fp[8]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[8]), lctree.locate_sha1fp(fp[9]));
        EXPECT_TRUE(lctree.locate_sha1fp(fp[12])->leaf);
        EXPECT_EQ(lctree.locate_sha1fp(fp[12])->next, lctree.locate_sha1fp(fp[4]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[4])->prev, lctree.locate_sha1fp(fp[12]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[12]), lctree.locate_sha1fp(fp[13]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[13]), lctree.locate_sha1fp(fp[14]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[14]), lctree.locate_sha1fp(fp[15]));
        EXPECT_TRUE(lctree.locate_sha1fp(fp[12])->prev->leaf);
        EXPECT_TRUE(lctree.locate_sha1fp(fp[12])->prev->chunk_list->empty());
        EXPECT_EQ(lctree.locate_sha1fp(fp[12])->prev->prev, nullptr);
        bool expect_find[20] = {true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, false, false, false, false};
        int expect_size[20] = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, -1, -1, -1, -1};
        int8_t expect[20] = {-1, -1 ,-1 , -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1};
        for (int i=0; i<20; ++i) {
            VariableBitset lc_bitmap;
            bool find = lctree.get_lifecycle(fp[i], lc_bitmap);
            EXPECT_EQ(find, expect_find[i]);
            if (find) {
                EXPECT_EQ(lc_bitmap.size(), expect_size[i]);
                if (expect_size[i] > 0)
                    EXPECT_EQ(lc_bitmap.get_byte(0), expect[i]);
            }
        }
    }
    TEST_F(LCTreeTest3, DestructionV1) {
        lctree.delete_version(1);
        EXPECT_EQ(lctree.num_of_inners(), 3);
        EXPECT_EQ(lctree.num_of_leaves(), 4);
        EXPECT_EQ(lctree.num_of_empty_leaves(), 1);
        EXPECT_TRUE(lctree.locate_sha1fp(fp[0])->leaf);
        EXPECT_EQ(lctree.locate_sha1fp(fp[0])->next, nullptr);
        EXPECT_EQ(lctree.locate_sha1fp(fp[0]), lctree.locate_sha1fp(fp[1]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[1]), lctree.locate_sha1fp(fp[2]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[2]), lctree.locate_sha1fp(fp[3]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[3]), lctree.locate_sha1fp(fp[12]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[12]), lctree.locate_sha1fp(fp[13]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[13]), lctree.locate_sha1fp(fp[14]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[14]), lctree.locate_sha1fp(fp[15]));
        EXPECT_TRUE(lctree.locate_sha1fp(fp[4])->leaf);
        EXPECT_EQ(lctree.locate_sha1fp(fp[4])->next, lctree.locate_sha1fp(fp[0]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[0])->prev, lctree.locate_sha1fp(fp[4]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[4]), lctree.locate_sha1fp(fp[5]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[5]), lctree.locate_sha1fp(fp[6]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[6]), lctree.locate_sha1fp(fp[7]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[7]), lctree.locate_sha1fp(fp[16]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[16]), lctree.locate_sha1fp(fp[17]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[17]), lctree.locate_sha1fp(fp[18]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[18]), lctree.locate_sha1fp(fp[19]));
        EXPECT_TRUE(lctree.locate_sha1fp(fp[10])->leaf);
        EXPECT_EQ(lctree.locate_sha1fp(fp[10])->next, lctree.locate_sha1fp(fp[4]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[4])->prev, lctree.locate_sha1fp(fp[10]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[10]), lctree.locate_sha1fp(fp[11]));
        EXPECT_TRUE(lctree.locate_sha1fp(fp[10])->prev->leaf);
        EXPECT_TRUE(lctree.locate_sha1fp(fp[10])->prev->chunk_list->empty());
        EXPECT_EQ(lctree.locate_sha1fp(fp[10])->prev->prev, nullptr);
        bool expect_find[20] = {true, true, true, true, true, true, true, true, false, false, true, true, true, true, true, true, true, true, true, true};
        int expect_size[20] = {0, 0, 0, 0, 0, 0, 0, 0, -1, -1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
        int8_t expect[20] = {-1, -1 ,-1 , -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1};
        for (int i=0; i<20; ++i) {
            VariableBitset lc_bitmap;
            bool find = lctree.get_lifecycle(fp[i], lc_bitmap);
            EXPECT_EQ(find, expect_find[i]);
            if (find) {
                EXPECT_EQ(lc_bitmap.size(), expect_size[i]);
                if (expect_size[i] > 0)
                    EXPECT_EQ(lc_bitmap.get_byte(0), expect[i]);
            }
        }
    }
    TEST_F(LCTreeTest3, DestructionV12) {
        lctree.delete_version(1);
        lctree.delete_version(2);
        EXPECT_EQ(lctree.num_of_inners(), 1);
        EXPECT_EQ(lctree.num_of_leaves(), 2);
        EXPECT_EQ(lctree.num_of_empty_leaves(), 1);
        EXPECT_TRUE(lctree.locate_sha1fp(fp[0])->leaf);
        EXPECT_EQ(lctree.locate_sha1fp(fp[0]), lctree.locate_sha1fp(fp[1]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[1]), lctree.locate_sha1fp(fp[2]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[2]), lctree.locate_sha1fp(fp[3]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[3]), lctree.locate_sha1fp(fp[10]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[10]), lctree.locate_sha1fp(fp[11]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[11]), lctree.locate_sha1fp(fp[12]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[12]), lctree.locate_sha1fp(fp[13]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[13]), lctree.locate_sha1fp(fp[14]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[14]), lctree.locate_sha1fp(fp[15]));
        EXPECT_TRUE(lctree.locate_sha1fp(fp[0])->prev->leaf);
        EXPECT_TRUE(lctree.locate_sha1fp(fp[0])->prev->chunk_list->empty());
        EXPECT_EQ(lctree.locate_sha1fp(fp[0])->prev->prev, nullptr);
        for (int i=0; i<4; ++i) {
            VariableBitset lc_bitmap;
            bool find = lctree.get_lifecycle(fp[i], lc_bitmap);
            EXPECT_TRUE(find);
            EXPECT_EQ(lc_bitmap.size(), 0);
        }
        for (int i=10; i<16; ++i) {
            VariableBitset lc_bitmap;
            bool find = lctree.get_lifecycle(fp[i], lc_bitmap);
            EXPECT_TRUE(find);
            EXPECT_EQ(lc_bitmap.size(), 0);
        }
    }
    TEST_F(LCTreeTest3, DestructionV21) {
        lctree.delete_version(2);
        lctree.delete_version(1);
        EXPECT_EQ(lctree.num_of_inners(), 1);
        EXPECT_EQ(lctree.num_of_leaves(), 2);
        EXPECT_EQ(lctree.num_of_empty_leaves(), 1);
        EXPECT_TRUE(lctree.locate_sha1fp(fp[0])->leaf);
        EXPECT_EQ(lctree.locate_sha1fp(fp[0]), lctree.locate_sha1fp(fp[1]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[1]), lctree.locate_sha1fp(fp[2]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[2]), lctree.locate_sha1fp(fp[3]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[3]), lctree.locate_sha1fp(fp[10]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[10]), lctree.locate_sha1fp(fp[11]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[11]), lctree.locate_sha1fp(fp[12]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[12]), lctree.locate_sha1fp(fp[13]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[13]), lctree.locate_sha1fp(fp[14]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[14]), lctree.locate_sha1fp(fp[15]));
        EXPECT_TRUE(lctree.locate_sha1fp(fp[0])->prev->leaf);
        EXPECT_TRUE(lctree.locate_sha1fp(fp[0])->prev->chunk_list->empty());
        EXPECT_EQ(lctree.locate_sha1fp(fp[0])->prev->prev, nullptr);
        for (int i=0; i<4; ++i) {
            VariableBitset lc_bitmap;
            bool find = lctree.get_lifecycle(fp[i], lc_bitmap);
            EXPECT_TRUE(find);
            EXPECT_EQ(lc_bitmap.size(), 0);
        }
        for (int i=10; i<16; ++i) {
            VariableBitset lc_bitmap;
            bool find = lctree.get_lifecycle(fp[i], lc_bitmap);
            EXPECT_TRUE(find);
            EXPECT_EQ(lc_bitmap.size(), 0);
        }
    }
    TEST_F(LCTreeTest3, DestructionETIngestion) {
        lctree.delete_version(1);
        for (int i=20; i<32; ++i) {
            SHA1FP sha1fp;
            sha1fp.fp1 = i;
            sha1fp.fp2 = (dist(rd));
            sha1fp.fp3 = (dist(rd));
            sha1fp.fp4 = (dist(rd));
            fp.push_back(sha1fp);
        }
        lctree.ingest_new_version(std::vector<SHA1FP>{fp[20], fp[21], fp[22], fp[23], fp[24], fp[25], fp[26], fp[27]}, {16, 16, 16, 16, 16, 16, 16, 16}, std::vector<SHA1FP>{fp[0], fp[1], fp[2]});
        lctree.ingest_new_version(std::vector<SHA1FP>{fp[28], fp[29], fp[30], fp[31]}, {16, 16, 16, 16}, std::vector<SHA1FP>{fp[3], fp[4], fp[5], fp[10], fp[15], fp[16], fp[17]});
        EXPECT_EQ(lctree.num_of_leaves(), 11);
        EXPECT_EQ(lctree.num_of_inners(), 10);
        EXPECT_EQ(lctree.num_of_empty_leaves(), 3);
        EXPECT_TRUE(lctree.locate_sha1fp(fp[0])->leaf);
        EXPECT_EQ(lctree.locate_sha1fp(fp[0])->next, nullptr);
        EXPECT_EQ(lctree.locate_sha1fp(fp[0]), lctree.locate_sha1fp(fp[1]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[1]), lctree.locate_sha1fp(fp[2]));
        EXPECT_TRUE(lctree.locate_sha1fp(fp[3])->leaf);
        EXPECT_EQ(lctree.locate_sha1fp(fp[3])->next, lctree.locate_sha1fp(fp[0]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[0])->prev, lctree.locate_sha1fp(fp[3]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[3]), lctree.locate_sha1fp(fp[15]));
        EXPECT_TRUE(lctree.locate_sha1fp(fp[12])->leaf);
        EXPECT_EQ(lctree.locate_sha1fp(fp[12])->next, lctree.locate_sha1fp(fp[3]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[3])->prev, lctree.locate_sha1fp(fp[12]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[12]), lctree.locate_sha1fp(fp[13]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[13]), lctree.locate_sha1fp(fp[14]));
        EXPECT_TRUE(lctree.locate_sha1fp(fp[4])->leaf);
        EXPECT_TRUE(lctree.locate_sha1fp(fp[12])->prev->leaf);
        EXPECT_TRUE(lctree.locate_sha1fp(fp[12])->prev->chunk_list->empty());
        EXPECT_EQ(lctree.locate_sha1fp(fp[12])->prev->prev, lctree.locate_sha1fp(fp[4]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[4])->next, lctree.locate_sha1fp(fp[12])->prev);
        EXPECT_EQ(lctree.locate_sha1fp(fp[4])->next->next, lctree.locate_sha1fp(fp[12]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[4]), lctree.locate_sha1fp(fp[5]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[5]), lctree.locate_sha1fp(fp[16]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[16]), lctree.locate_sha1fp(fp[17]));
        EXPECT_TRUE(lctree.locate_sha1fp(fp[6])->leaf);
        EXPECT_EQ(lctree.locate_sha1fp(fp[6])->next, lctree.locate_sha1fp(fp[4]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[4])->prev, lctree.locate_sha1fp(fp[6]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[6]), lctree.locate_sha1fp(fp[7]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[7]), lctree.locate_sha1fp(fp[18]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[18]), lctree.locate_sha1fp(fp[19]));
        EXPECT_TRUE(lctree.locate_sha1fp(fp[10])->leaf);
        EXPECT_EQ(lctree.locate_sha1fp(fp[10])->next, lctree.locate_sha1fp(fp[6]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[6])->prev, lctree.locate_sha1fp(fp[10]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[10]), lctree.locate_sha1fp(fp[11]));
        EXPECT_TRUE(lctree.locate_sha1fp(fp[20])->leaf);
        EXPECT_TRUE(lctree.locate_sha1fp(fp[10])->prev->leaf);
        EXPECT_TRUE(lctree.locate_sha1fp(fp[10])->prev->chunk_list->empty());
        EXPECT_EQ(lctree.locate_sha1fp(fp[10])->prev->prev, lctree.locate_sha1fp(fp[20]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[20])->next, lctree.locate_sha1fp(fp[10])->prev);
        EXPECT_EQ(lctree.locate_sha1fp(fp[20])->next->next, lctree.locate_sha1fp(fp[10]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[20]), lctree.locate_sha1fp(fp[21]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[21]), lctree.locate_sha1fp(fp[22]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[22]), lctree.locate_sha1fp(fp[23]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[23]), lctree.locate_sha1fp(fp[24]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[24]), lctree.locate_sha1fp(fp[25]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[25]), lctree.locate_sha1fp(fp[26]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[26]), lctree.locate_sha1fp(fp[27]));
        EXPECT_TRUE(lctree.locate_sha1fp(fp[28])->leaf);
        EXPECT_EQ(lctree.locate_sha1fp(fp[28])->next, lctree.locate_sha1fp(fp[20]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[20])->prev, lctree.locate_sha1fp(fp[28]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[28]), lctree.locate_sha1fp(fp[29]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[29]), lctree.locate_sha1fp(fp[30]));
        EXPECT_EQ(lctree.locate_sha1fp(fp[30]), lctree.locate_sha1fp(fp[31]));
        EXPECT_TRUE(lctree.locate_sha1fp(fp[28])->prev->leaf);
        EXPECT_TRUE(lctree.locate_sha1fp(fp[28])->prev->chunk_list->empty());
        EXPECT_EQ(lctree.locate_sha1fp(fp[28])->prev->prev, nullptr);
        bool expect_find[32] = {true, true, true, true, true, true, true, true, false, false, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true};
        int expect_size[32] = {1, 1, 1, 0, 0, 0, 0, 0, -1, -1, 2, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
        int8_t expect[32] = {0, 0, 0, -1, -1, -1, -1, -1, -1, -1, 0b0000'0010, 0, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1};
        for (int i=0; i<20; ++i) {
            VariableBitset lc_bitmap;
            bool find = lctree.get_lifecycle(fp[i], lc_bitmap);
            EXPECT_EQ(find, expect_find[i]);
            if (find) {
                EXPECT_EQ(lc_bitmap.size(), expect_size[i]);
                if (expect_size[i] > 0)
                    EXPECT_EQ(lc_bitmap.get_byte(0), expect[i]);
            }
        }
    }
}
