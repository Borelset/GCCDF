//
// Created by BorelsetR on 2019/7/16.
//

#ifndef REDUNDANCY_DETECTION_DETECTMETHOD_H
#define REDUNDANCY_DETECTION_DETECTMETHOD_H

#include <vector>
#include <cstdint>
#include <cstdio>
#include "../Utility/StorageTask.h"

class DetectMethod {
public:
    virtual void detect(uint8_t *inputPtr, uint64_t length) {
        printf("DetectMethod error encountered\n");
    }

    virtual void detectTest(uint8_t *inputPtr, uint64_t length) {
        printf("DetectMethod error encountered\n");
    }

    virtual int getResult(SFSet *result) {
        printf("DetectMethod error encountered\n");
        return 0;
    }

    virtual int setTotalLength(uint64_t length) {
        printf("DetectMethod error encountered\n");
        return 0;
    }

    virtual int resetHash() {
        printf("DetectMethod error encountered\n");
        return 0;
    }
};

#endif //REDUNDANCY_DETECTION_DETECTMETHOD_H
