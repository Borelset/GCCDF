//
// Created by BorelsetR on 2019/7/17.
//

#ifndef REDUNDANCY_DETECTION_ENCODINGMETHOD_H
#define REDUNDANCY_DETECTION_ENCODINGMETHOD_H

#include <cstdio>
#include <cstdint>
#include <list>
#include <string>
#include "../Utility/Chunk.h"

enum class EncodingType {
    EDelta,
};

class EncodingMethod {
public:
    virtual void encoding(uint8_t *baseBuffer, uint64_t baseLength, uint8_t *derivedBuffer, uint64_t derivedLength,
                          uint8_t *outputBuffer, uint64_t *outputLength) {
        printf("EncodingMethod error encountered\n");
    }

    virtual void decoding(uint8_t *baseBuffer, uint64_t baseLength, uint8_t *encodedBuffer, uint64_t encodedLength,
                          uint8_t *rebuildBuffer, uint64_t *rebuildLength) {
        printf("EncodingMethod error encountered\n");
    }
};

#endif //REDUNDANCY_DETECTION_ENCODINGMETHOD_H
