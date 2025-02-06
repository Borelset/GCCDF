//
// Created by BorelsetR on 2019/7/17.
//

#ifndef REDUNDANCY_DETECTION_CHUNK_H
#define REDUNDANCY_DETECTION_CHUNK_H

#include <cstdint>

enum class ChunkType {
    Exist,
    New,
};

struct ChunkHead {
    uint64_t type:1;
    uint64_t length:63;
};

struct ChunkBody {
    uint64_t pos;
};

struct CompleteChunk {
    ChunkHead head;
    ChunkBody body;
};

#endif //REDUNDANCY_DETECTION_CHUNK_H
