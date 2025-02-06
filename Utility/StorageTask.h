//
// Created by BorelsetR on 2019/7/23.
//

#ifndef ODESS_COMPRESSIONTASK_H
#define ODESS_COMPRESSIONTASK_H

#include "Chunk.h"
#include "Lock.h"
#include <list>
#include <tuple>
#include <string_view>

constexpr int MIX_GROUP_SIZE = 5;

enum class TaskType {
    Compression,
    Decompression,
};

struct SHA1FP {
    //std::tuple<uint32_t, uint32_t, uint32_t, uint32_t, uint32_t> fp;
    uint64_t fp1;
    uint32_t fp2, fp3, fp4;

    void print() const {
        printf("%lu:%d:%d:%d\n", fp1, fp2, fp3, fp4);
    }
};

struct WriteHead {
    uint64_t id;
    SHA1FP fp;
//    SHA1FP bfp;
    uint64_t type:2;
    uint64_t length:62;
};

/*
bool operator == (const SHA1FP& left, const SHA1FP& right)
{
    if(left.fp[0] == right.fp[0] &&
            left.fp[1] == right.fp[1] &&
            left.fp[2] == right.fp[2] &&
            left.fp[3] == right.fp[3] &&
            left.fp[4] == right.fp[4]){
        return true;
    }else{
        return false;
    }
}

bool operator < (const SHA1FP& left, const SHA1FP& right)
{
    for(int i=0; i<5; i++){
        if(left.fp[i] < right.fp[i]){
            return true;
        }
    }
    return false;
}
 */

struct SFSet {
    uint64_t sf1;
    uint64_t sf2;
    uint64_t sf3;
    uint64_t sf4;
    uint64_t sf5;
    uint64_t sf6;
    uint64_t sf7;
    uint64_t sf8;
    uint64_t sf9;
    uint64_t sf10;
    uint64_t sf11;
    uint64_t sf12;
};

struct StageTimePoint {
    uint64_t read = 0;
    uint64_t chunk = 0;
    uint64_t dedup = 0;
    uint64_t write = 0;
};

struct Location {
    uint64_t fid;
    uint64_t pos;
    uint32_t length;
//    uint32_t oriLength;
//    uint64_t bfid = -1;
//    uint64_t bpos = -1;
//    uint32_t blength = -1;
//    SHA1FP baseFP;
};

struct BlockHead {
    uint64_t id;
    SHA1FP sha1Fp;
    uint8_t type;
    uint32_t length;
//    SHA1FP baseFP;
//    SFSet features;

};


struct CacheBlock {
    uint8_t *block;
    uint64_t type: 1;
    uint64_t length: 63;
    SHA1FP baseFP;
};

struct DedupTask {
    uint8_t *buffer;
    uint64_t pos;
    uint64_t length;
    SHA1FP fp;
    uint64_t fileID;
    StageTimePoint *stageTimePoint;
    CountdownLatch *countdownLatch = nullptr;
    uint64_t index;
    uint64_t type;
    bool inCache = false;
    bool rejectDelta = false;
    bool rejectDedup = false;
    uint64_t baseCid;
    uint64_t cid;
    bool eoi; // end of input;
    bool end_of_mix_group;
};

struct WriteTask {
    int type;
    Location location;
    uint8_t *buffer;
    uint64_t pos;
    uint64_t bufferLength;
    uint8_t *deltaBuffer;
    uint64_t deltaBufferLength;
    uint64_t fileID;
    uint64_t id;
    SHA1FP sha1Fp;
    uint64_t sf1;
    uint64_t sf2;
    uint64_t sf3;
    uint64_t sf4;
    uint64_t sf5;
    uint64_t sf6;
    uint64_t sf7;
    uint64_t sf8;
    uint64_t sf9;
    uint64_t sf10;
    uint64_t sf11;
    uint64_t sf12;
    SHA1FP baseFP;
    StageTimePoint *stageTimePoint;
    CountdownLatch *countdownLatch = nullptr;
    uint64_t index;
    bool eoi; // end of input;
};

struct ChunkTask {
    uint8_t *buffer = nullptr;
    uint64_t length;
    uint64_t fileID;
    uint64_t end;
    bool eoi; // end of input;
    bool end_of_mix_group;
    CountdownLatch *countdownLatch = nullptr;
    uint64_t index;
};

struct StorageTask {
    std::string path;
    uint8_t *buffer = nullptr;
    uint64_t length;
    uint64_t fileID;
    uint64_t end;
    bool eoi; // end of input;
    bool end_of_mix_group;
    CountdownLatch *countdownLatch = nullptr;
    StageTimePoint stageTimePoint;

    void destruction() {
        if (buffer) free(buffer);
    }
};

struct RestoreTask {
    int recipeID;
    std::string outputPath;
    CountdownLatch *countdownLatch = nullptr;
};


struct RecipeUnit{
    WriteHead writeHead;
    Location location;
};

#endif //ODESS_COMPRESSIONTASK_H
