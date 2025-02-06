//
// Created by BorelsetR on 2019/8/19.
//

#ifndef ODESSSTORAGE_FINESSESKIP_H
#define ODESSSTORAGE_FINESSESKIP_H


#include "../RollHash/RollHash.h"
#include "DetectMethod.h"
#include "isa-l_crypto/mh_sha1.h"
#include "../Utility/xxhash.h"

class FinesseSkip : public DetectMethod {
public:
    FinesseSkip(int k, RollHash *rollHash) {
        chunkAmount = k;
        recordsList = (uint64_t *) malloc(sizeof(uint64_t) * k);
        separatePos = (uint64_t *) malloc(sizeof(uint64_t) * (k + 1));
        for (int i = 0; i < k; i++) {
            recordsList[i] = 0;
        }
    }

    ~FinesseSkip() {
        printf("FinesseSkip release\n");

        free(recordsList);
        free(separatePos);
    }

    void detect(uint8_t *inputPtr, uint64_t length) override {
        for (uint64_t i = 0; i < chunkAmount; i++) {
            segDetect(inputPtr + separatePos[i], separatePos[i + 1] - separatePos[i], i);
        }
        /*
        for (uint64_t i = 0; i < length; i++) {
            uint64_t hashValue = usingHash->rolling(inputPtr + i);
            if (hashValue > recordsList[pos])
                recordsList[pos] = hashValue;
            if (i == separatePos[pos]) {
                if (pos != chunkAmount - 1)
                    pos++;
            }
        }
         */
        /*
        for (int i = 0; i < chunkAmount; i++) {
            printf("feature #%d : %lu\n", i, recordsList[i]);
        }
         */
    }

    int getResult(SFSet *result) override {
        uint64_t max[4];
        uint64_t middle[4];
        uint64_t min[4];
        for (int i = 0; i < 4; i++) {
            bool result1, result2, result3;
            result1 = recordsList[i * 3 + 0] >= recordsList[i * 3 + 1];
            result2 = recordsList[i * 3 + 1] >= recordsList[i * 3 + 2];
            result3 = recordsList[i * 3 + 2] >= recordsList[i * 3 + 0];
            if (result1) {
                if (result2) {
                    memcpy(&max[i], &recordsList[i * 3 + 0], sizeof(uint64_t));
                    memcpy(&middle[i], &recordsList[i * 3 + 1], sizeof(uint64_t));
                    memcpy(&min[i], &recordsList[i * 3 + 2], sizeof(uint64_t));
                } else if (result3) {
                    memcpy(&max[i], &recordsList[i * 3 + 2], sizeof(uint64_t));
                    memcpy(&middle[i], &recordsList[i * 3 + 0], sizeof(uint64_t));
                    memcpy(&min[i], &recordsList[i * 3 + 1], sizeof(uint64_t));
                } else {
                    memcpy(&max[i], &recordsList[i * 3 + 0], sizeof(uint64_t));
                    memcpy(&middle[i], &recordsList[i * 3 + 2], sizeof(uint64_t));
                    memcpy(&min[i], &recordsList[i * 3 + 1], sizeof(uint64_t));
                }
            } else {
                if (!result2) {
                    memcpy(&max[i], &recordsList[i * 3 + 2], sizeof(uint64_t));
                    memcpy(&middle[i], &recordsList[i * 3 + 1], sizeof(uint64_t));
                    memcpy(&min[i], &recordsList[i * 3 + 0], sizeof(uint64_t));
                } else if (result3) {
                    memcpy(&max[i], &recordsList[i * 3 + 1], sizeof(uint64_t));
                    memcpy(&middle[i], &recordsList[i * 3 + 2], sizeof(uint64_t));
                    memcpy(&min[i], &recordsList[i * 3 + 0], sizeof(uint64_t));
                } else {
                    memcpy(&max[i], &recordsList[i * 3 + 1], sizeof(uint64_t));
                    memcpy(&middle[i], &recordsList[i * 3 + 0], sizeof(uint64_t));
                    memcpy(&min[i], &recordsList[i * 3 + 2], sizeof(uint64_t));
                }
            }
        }

        result->sf1 = XXH64(max, sizeof(uint64_t) * 4, 0x7fcaf1);
        result->sf2 = XXH64(middle, sizeof(uint64_t) * 4, 0x7fcaf1);
        result->sf3 = XXH64(min, sizeof(uint64_t) * 4, 0x7fcaf1);
        /*
        uint64_t sf;
        uint8_t* ptr = (uint8_t*)&max;
        resetHash();
        for(int i=0; i< sizeof(uint64_t) * 4; i++){
            sf = usingHash->rolling(ptr + i);
        }
        result->sf1 = sf;

        ptr = (uint8_t*)&middle;
        resetHash();
        for(int i=0; i< sizeof(uint64_t) * 4; i++){
            sf = usingHash->rolling(ptr + i);
        }
        result->sf2 = sf;

        ptr = (uint8_t*)&min;
        resetHash();
        for(int i=0; i< sizeof(uint64_t) * 4; i++){
            sf = usingHash->rolling(ptr + i);
        }
        result->sf3 = sf;
         */

        return chunkAmount;
    }

    virtual int setTotalLength(uint64_t length) override {
        resetHash();
        int segmentLength = length / chunkAmount;
        for (int i = 0; i < chunkAmount; i++) {
            separatePos[i] = segmentLength * i;
        }
        separatePos[chunkAmount] = length;
        for (int i = 0; i < chunkAmount; i++) {
            recordsList[i] = 0;
        }
        return 0;
    }

    virtual int resetHash() override {
        usingHash.reset();
        return 0;
    }

private:

    void segDetect(uint8_t *inputPtr, uint64_t length, int segIndex) {
        for (uint64_t i = 0; i < length; i++) {
            uint64_t hashValue = usingHash.rolling(inputPtr + i);
            if (hashValue > recordsList[segIndex])
                recordsList[segIndex] = hashValue;
            i += 8;
        }
    }

    int chunkAmount;
    Rabin usingHash;
    uint64_t *recordsList;
    uint64_t *separatePos;

};


#endif //ODESSSTORAGE_FINESSESKIP_H
