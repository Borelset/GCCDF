//
// Created by ldb on 8/4/23.
//

#include "gtest/gtest.h"
#include "../MetadataManager/MetadataManager.h"

namespace {
    TEST(variable_bitset, fundamentals) {
        VariableBitset bitmap;
        EXPECT_DEATH(bitmap.get_byte(0), "");
        EXPECT_FALSE(bitmap.set(2));
        bitmap.shift_left(8);
        EXPECT_EQ(bitmap.size(), 8);
        EXPECT_TRUE(bitmap.set(2));
        EXPECT_TRUE(bitmap.set(3));
        EXPECT_TRUE(bitmap.set(7));
        EXPECT_EQ(bitmap.get_byte(0), 0b10001100);
        EXPECT_TRUE(bitmap.flip(5));
        EXPECT_TRUE(bitmap.reset(5));
        EXPECT_TRUE(bitmap.flip(3));
        EXPECT_FALSE(bitmap.set_byte(1, 0b11000011));
        EXPECT_FALSE(bitmap.test(3));
        EXPECT_FALSE(bitmap.test(5));
        EXPECT_DEATH(bitmap.test(8), "");
        EXPECT_DEATH(bitmap.get_byte(1), "");
        bitmap.shift_right(4);
        EXPECT_EQ(bitmap.size(), 4);
        EXPECT_EQ(bitmap.get_byte(0), 0b00001000);
        EXPECT_TRUE(bitmap.set_byte(0, 0b11000011));
        EXPECT_EQ(bitmap.get_byte(0), 0b00000011);
    }
    TEST(variable_bitset, shift) {
        VariableBitset bitmap;
        EXPECT_FALSE(bitmap.set(2));
        bitmap.shift_left(14);
        EXPECT_TRUE(bitmap.set_byte(0, 0b00111100));
        EXPECT_TRUE(bitmap.set_byte(1, 0b11100011));
        EXPECT_EQ(bitmap.get_byte(0), 0b00111100);
        EXPECT_EQ(bitmap.get_byte(1), 0b00100011);
        EXPECT_DEATH(bitmap.test(14), "");
        bitmap.shift_right(5);
        EXPECT_EQ(bitmap.get_byte(1), 0b00000001);
        bitmap.shift_right(2);
        EXPECT_DEATH(bitmap.get_byte(1), "");
        EXPECT_EQ(bitmap.get_byte(0), 0b01000110);
        bitmap.shift_left(9);
        EXPECT_DEATH(bitmap.get_byte(2), "");
        EXPECT_EQ(bitmap.get_byte(0), 0b00000000);
        bitmap.shift_left(9);
        EXPECT_EQ(bitmap.get_byte(3), 0b00000001);
        bitmap.shift_right(1);
        EXPECT_DEATH(bitmap.get_byte(3), "");
        EXPECT_EQ(bitmap.get_byte(2), 0b10001100);
    }
    TEST(variable_bitset, add_delete) {
        VariableBitset bitmap;
        EXPECT_FALSE(bitmap.set(2));
        bitmap.add_bit(1);
        ASSERT_TRUE(bitmap.test(0));
        bitmap.delete_bit(0);
        bitmap.shift_left(14);
        ASSERT_TRUE(bitmap.set_byte(0, 0b00111100));
        ASSERT_TRUE(bitmap.set_byte(1, 0b11100011));
        ASSERT_EQ(bitmap.get_byte(0), 0b00111100);
        ASSERT_EQ(bitmap.get_byte(1), 0b00100011);
        bitmap.add_bit(1);
        ASSERT_EQ(bitmap.get_byte(1), 0b01100011);
        bitmap.add_bit(1);
        ASSERT_EQ(bitmap.get_byte(1), 0b11100011);
        EXPECT_DEATH(bitmap.get_byte(2), "");
        bitmap.add_bit(0);
        ASSERT_EQ(bitmap.get_byte(2), 0b00000000);
        EXPECT_TRUE(bitmap.set(16));
        EXPECT_FALSE(bitmap.set(17));
        bitmap.add_bit(1);
        bitmap.add_bit(1);
        bitmap.add_bit(1);
        EXPECT_TRUE(bitmap.reset(17));
        ASSERT_EQ(bitmap.get_byte(2), 0b00001101);
        ASSERT_EQ(bitmap.size(), 20);
        bitmap.delete_bit(18);
        ASSERT_DEATH(bitmap.test(19), "");
        ASSERT_EQ(bitmap.get_byte(2), 0b00000101);
        bitmap.delete_bit(3);
        ASSERT_DEATH(bitmap.test(18), "");
        ASSERT_EQ(bitmap.get_byte(0), 0b10011100);
        ASSERT_EQ(bitmap.get_byte(1), 0b11110001);
        ASSERT_EQ(bitmap.get_byte(2), 0b00000010);
        bitmap.delete_bit(16);
        ASSERT_EQ(bitmap.get_byte(1), 0b11110001);
        bitmap.delete_bit(16);
        ASSERT_EQ(bitmap.get_byte(1), 0b11110001);
        ASSERT_DEATH(bitmap.get_byte(2), "");
        bitmap.add_bit(1);
        ASSERT_EQ(bitmap.get_byte(1), 0b11110001);
        bitmap.delete_bit(13);
        ASSERT_DEATH(bitmap.get_byte(2), "");
        ASSERT_EQ(bitmap.get_byte(0), 0b10011100);
        ASSERT_EQ(bitmap.get_byte(1), 0b11110001);
    }
}
