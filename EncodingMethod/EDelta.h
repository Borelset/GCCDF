//
// Created by BorelsetR on 2019/7/17.
//

#ifndef REDUNDANCY_DETECTION_EDELTA_H
#define REDUNDANCY_DETECTION_EDELTA_H

#include "EncodingMethod.h"
#include "../RollHash/Gear.h"
#include "../RollHash/RollHash.h"
#include "../Utility/xxhash.h"
#include "../RollHash/Rabin.h"
#include "../Utility/FileOperator.h"
#include <map>
#include <vector>

const int ChunkAmountOnce = 10;

struct Block {
    uint64_t pos;
    uint64_t length;
    uint64_t type;
    uint64_t hash;
};

class EDelta : public EncodingMethod {
public:
    EDelta(HashType hashType) {
        switch (hashType) {
            case HashType::Rabin:
                rollHash = new Rabin();
                break;
            case HashType::Gear:
                rollHash = new Gear();
                break;
        }
    }

    ~EDelta() {
        printf("EDelta release\n");

        delete rollHash;
    }

    virtual void
    encoding(uint8_t *baseBuffer, uint64_t baseLength, uint8_t *derivedBuffer, uint64_t derivedLength,
             uint8_t *outputBuffer, uint64_t *outputLength) override {
        derivedBlocks.clear();
        baseBlocks.clear();
        directory.clear();

        baseBlocks.reserve(10);
        derivedBlocks.reserve(10);
        uint64_t basePos = 0, derivedPos = 0;
        ChunkHead chunkHead;
        CompleteChunk completeChunk;

        uint64_t testCounter = 0;

        int start = findFront(baseBuffer, baseLength, derivedBuffer, derivedLength);
        if (start) {
            completeChunk.head.type = (uint64_t) ChunkType::Exist;
            completeChunk.head.length = start;
            completeChunk.body.pos = 0;
            memcpy(outputBuffer, &completeChunk, sizeof(CompleteChunk));
            *outputLength += sizeof(CompleteChunk);
            //printf("Chunk #%lu, base:%lu, length:%lu, type:%lu\n", testCounter, completeChunk.body.pos, completeChunk.head.length, completeChunk.head.type);
            testCounter++;
        }
        int end = 0;
        if (start != derivedLength) {
            end = findEnd(baseBuffer, baseLength, derivedBuffer, derivedLength, start);
        }

        basePos += start;
        derivedPos += start;

        uint64_t derivedRecordPos = start;

        char testBuffer[1024];
        char format[128];

        bool endFlag = true;
        uint64_t startIndex = 0;

        uint64_t baseDetectLength, derivedDetectLength;

        while (1) {
            if (baseLength - basePos < end) {
                baseDetectLength = 0;
            } else {
                baseDetectLength = baseLength - basePos - end;
            }
            if (derivedLength - derivedPos < end) {
                derivedDetectLength = 0;
            } else {
                derivedDetectLength = derivedLength - derivedPos - end;
            }

            uint64_t baseChunkLength = baseChunking(baseBuffer + basePos, baseDetectLength, ChunkAmountOnce,
                                                    basePos, &startIndex);
            //sprintf(format, "baseChunk:\n%%.%lus\n", baseChunkLength);
            //printf(format, baseBuffer + basePos);
            uint64_t derivedChunkLength = derivedChunking(derivedBuffer + derivedPos, derivedDetectLength,
                                                          ChunkAmountOnce, derivedPos);
            //sprintf(format, "derivedChunk:\n%%.%lus\n", derivedChunkLength);
            //printf(format, derivedBuffer + derivedPos);

/*
            for (Block b : baseBlocks) {
                printf("Base pos:%lu, length:%lu, hash:%lu\n", b.pos, b.length, b.hash);
            }
            for (Block b : derivedBlocks) {
                printf("Derived pos:%lu, length:%lu, hash:%lu\n", b.pos, b.length, b.hash);
            }
*/


            if (baseChunkLength == 0 && derivedChunkLength == 0) {
                uint64_t lastChunkLength = derivedLength - end - derivedRecordPos;

                chunkHead.type = (uint64_t) ChunkType::New;
                chunkHead.length = lastChunkLength;
                memcpy(outputBuffer + *outputLength, &chunkHead, sizeof(ChunkHead));
                *outputLength += sizeof(ChunkHead);
                memcpy(outputBuffer + *outputLength, derivedBuffer + derivedRecordPos,
                       lastChunkLength);
                *outputLength += lastChunkLength;
                break;
            }

            bool flags = true;

            for (Block block: derivedBlocks) {
                if (directory.find(block.hash) != directory.end()) {
                    flags = false;
                    break;
                }
            }
            if (!flags) {
                uint64_t baseExtents = 0;
                uint64_t derivedExtents = 0;
                for (auto block = derivedBlocks.begin(); block != derivedBlocks.end(); block++) {
                    auto iter = directory.find(block->hash);
                    if (iter != directory.end()) {
                        int index = iter->second;
                        uint64_t detectBase = baseBlocks[index].pos;
                        uint64_t detectDerived = block->pos;
                        Block &testBlock = baseBlocks[index]; //
                        uint64_t towards = findTowards(baseBuffer + detectBase + baseBlocks[index].length,
                                                       baseLength - detectBase - baseBlocks[index].length - end,
                                                       derivedBuffer + detectDerived + block->length,
                                                       derivedLength - detectDerived - block->length - end);
                        uint64_t back = findBack(baseBuffer + detectBase,
                                                 detectBase,
                                                 derivedBuffer + detectDerived,
                                                 detectDerived);
                        if (detectDerived < back + derivedRecordPos) {
                            back = detectDerived - derivedRecordPos;
                        }
                        chunkHead.type = (uint64_t) ChunkType::New;
                        chunkHead.length = detectDerived - back - derivedRecordPos;
                        memcpy(outputBuffer + *outputLength, &chunkHead, sizeof(ChunkHead));
                        *outputLength += sizeof(ChunkHead);
                        memcpy(outputBuffer + *outputLength, derivedBuffer + derivedRecordPos,
                               detectDerived - back - derivedRecordPos);
                        *outputLength += detectDerived - back - derivedRecordPos;
                        //printf("Chunk #%lu, base:%lu, length:%lu, type:%lu\n", testCounter, 0, chunkHead.length, chunkHead.type);
                        testCounter++;

                        completeChunk.head.type = (uint64_t) ChunkType::Exist;
                        completeChunk.head.length = back + block->length + towards;
                        completeChunk.body.pos = detectBase - back;
                        memcpy(outputBuffer + *outputLength, &completeChunk, sizeof(CompleteChunk));
                        *outputLength += sizeof(CompleteChunk);

                        derivedRecordPos = detectDerived + block->length + towards;
                        derivedBlocks.clear();
                        baseBlocks.clear();
                        directory.clear();
                        startIndex = 0;

                        basePos = detectBase + block->length + towards;
                        derivedPos = derivedRecordPos;
                        //printf("Chunk #%lu, base:%lu, length:%lu, type:%lu\n", testCounter, completeChunk.body.pos, completeChunk.head.length, completeChunk.head.type);
                        //printf("base start:%lu, derived start:%lu, towards:%lu, back:%lu\n", detectBase, detectDerived, towards, back);
                        testCounter++;

                        break;
                    }
                }
                if (derivedPos == derivedLength) {
                    endFlag = false;
                    break;
                }
            } else {
                basePos += baseChunkLength;
                derivedPos += derivedChunkLength;
            }
        }

        if (endFlag && end > 0) {
            completeChunk.head.type = (uint64_t) ChunkType::Exist;
            completeChunk.head.length = end;
            completeChunk.body.pos = baseLength - end;
            memcpy(outputBuffer + *outputLength, &completeChunk, sizeof(CompleteChunk));
            *outputLength += sizeof(CompleteChunk);
        }

    }

    virtual void decoding(uint8_t *baseBuffer, uint64_t baseLength, uint8_t *encodedBuffer, uint64_t encodedLength,
                          uint8_t *rebuildBuffer, uint64_t *rebuildLength) {
        ChunkHead *chunkHeadPtr;
        ChunkBody *chunkBodyPtr;
        uint64_t readLength = 0;
        while (readLength < encodedLength) {
            chunkHeadPtr = (ChunkHead *) (encodedBuffer + readLength);
            if (chunkHeadPtr->type == (uint64_t) ChunkType::Exist) {
                chunkBodyPtr = (ChunkBody *) (encodedBuffer + readLength + sizeof(ChunkHead));
                readLength += sizeof(CompleteChunk);
                memcpy(rebuildBuffer + *rebuildLength, baseBuffer + chunkBodyPtr->pos, chunkHeadPtr->length);
                *rebuildLength += chunkHeadPtr->length;
            } else {
                readLength += sizeof(ChunkHead);
                memcpy(rebuildBuffer + *rebuildLength, encodedBuffer + readLength, chunkHeadPtr->length);
                *rebuildLength += chunkHeadPtr->length;
                readLength += chunkHeadPtr->length;
            }
        }
    }

private:
    int findFront(uint8_t *baseBuffer, uint64_t baseLength, uint8_t *derivedBuffer, uint64_t derivedLength) {

        uint64_t start = 0;
        uint64_t *basePtr = (uint64_t *) baseBuffer;
        uint64_t *derivedPtr = (uint64_t *) derivedBuffer;
        uint64_t minLength = baseLength > derivedLength ? derivedLength : baseLength;
        int i = 0;
        while (basePtr[i] == derivedPtr[i]) {
            i++;
            start += 8;
            if (start >= minLength) return minLength;
        }
        i = start;
        while (baseBuffer[i] == derivedBuffer[i]) {
            i++;
            start++;
            if (start >= minLength) return minLength;
        }
        return start;
    }

    int
    findEnd(uint8_t *baseBuffer, uint64_t baseLength, uint8_t *derivedBuffer, uint64_t derivedLength, uint64_t start) {

        uint64_t end = 0;
        uint64_t *basePtr = (uint64_t *) baseBuffer;
        uint64_t *derivedPtr = (uint64_t *) derivedBuffer;
        int i = 0;

        uint64_t maxFindEnd = (baseLength > derivedLength ? derivedLength : baseLength) - start;

        // **********************************************************************************************
        // 这里访存效率是否会有问题？？？ 因为实际上很大概率一个uint64_t会横跨两个cache frame //todo
        basePtr = (uint64_t *) (baseBuffer + baseLength - 8);
        derivedPtr = (uint64_t *) (derivedBuffer + derivedLength - 8);
        while (basePtr[-i] == derivedPtr[-i]) {
            i++;
            end += 8;
            if (end >= maxFindEnd) return maxFindEnd;
        }
        i = end;
        while (baseBuffer[baseLength - i - 1] == derivedBuffer[derivedLength - i - 1]) {
            i++;
            end++;
            if (end >= maxFindEnd) return maxFindEnd;
        }
        // ***********************************************************************************************
        return end;
    }

    uint64_t findTowards(uint8_t *baseBuffer, uint64_t baseLength, uint8_t *derivedBuffer, uint64_t derivedLength) {
        uint64_t length = baseLength > derivedLength ? derivedLength : baseLength;
        uint64_t counter = 0;
        for (uint64_t i = 0; i < length; i++) {
            char t1 = baseBuffer[i];
            char t2 = derivedBuffer[i];
            if (baseBuffer[i] == derivedBuffer[i]) {
                counter++;
            } else {
                break;
            }
        }
        return counter;
    }

    uint64_t findBack(uint8_t *baseBuffer, uint64_t baseLength, uint8_t *derivedBuffer, uint64_t derivedLength) {
        uint64_t length = baseLength > derivedLength ? derivedLength : baseLength;
        uint64_t counter = 0;
        for (uint64_t i = 1; i < length; i++) {
            if (baseBuffer[-i] == derivedBuffer[-i]) {
                counter++;
            } else {
                break;
            }
        }
        return counter;
    }

    uint64_t baseChunking(uint8_t *buffer, uint64_t length, int blockOnce, uint64_t pos, uint64_t *startIndex) {
        uint64_t chunkLength = 0, i = 0, base = 0;
        uint64_t fp = 0;
        uint64_t mask = rollHash->getDeltaMask();
        Block block;
        for (int k = 0; k < blockOnce; k++) {
            for (; i < length; i++) {
                fp = rollHash->rolling(buffer + i);
                if (!(fp & mask)) {
                    block.pos = base + pos;
                    block.length = i - base + 1;
                    block.hash = XXH64(buffer + base, i - base + 1, 0x7fcaf1);
                    baseBlocks.push_back(block);
                    i++;
                    base = i;
                    if (directory.find(block.hash) == directory.end())
                        directory[block.hash] = *startIndex;
                    (*startIndex)++;
                    if (base == length) return i;
                    break;
                }
            }
        }
        return i;
    }

    uint64_t derivedChunking(uint8_t *buffer, uint64_t length, int blockOnce, uint64_t pos) {
        uint64_t chunkLength = 0, i = 0, base = 0;
        uint64_t fp = 0;
        uint64_t mask = rollHash->getDeltaMask();
        Block block;
        for (int k = 0; k < blockOnce; k++) {
            for (; i < length; i++) {
                fp = rollHash->rolling(buffer + i);
                if (!(fp & mask)) {
                    block.pos = base + pos;
                    block.length = i - base + 1;
                    block.hash = XXH64(buffer + base, i - base + 1, 0x7fcaf1);
                    derivedBlocks.push_back(block);
                    i++;
                    base = i;
                    if (base == length) return i;
                    break;
                }
            }

        }
        return i;
    }

    RollHash *rollHash;
    std::vector<Block> baseBlocks;
    std::vector<Block> derivedBlocks;
    std::map<uint64_t, uint64_t> directory;
};

#endif //REDUNDANCY_DETECTION_EDELTA_H
