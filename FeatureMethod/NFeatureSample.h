//
// Created by BorelsetR on 2019/8/7.
//

#ifndef ODESSSTORAGE_NFEATURESAMPLE_H
#define ODESSSTORAGE_NFEATURESAMPLE_H

#include <cstdint>
#include <cstdlib>
#include <random>
#include "../RollHash/RollHash.h"
#include "DetectMethod.h"
#include "../Utility/xxhash.h"

DEFINE_int32(OdessShiftBits, 1, "bit shift for gear in odess, decide the window size.");

DEFINE_uint64(OdessSamplingMask, 0x0000400303410000, "Sampling mask for Odess");

class NFeatureSample : public DetectMethod {
public:
    NFeatureSample(int k, RollHash *rollHash) {
        std::uniform_int_distribution<uint64_t>::param_type paramA(DistributionAMin, DistributionAMax);
        std::uniform_int_distribution<uint64_t>::param_type paramB(DistributionBMin, DistributionBMax);
        distributionA.param(paramA);
        distributionB.param(paramB);
        featureAmount = k;
        recordsList = (uint64_t *) malloc(sizeof(uint64_t) * k);
        transformListA = (int *) malloc(sizeof(int) * k);
        transformListB = (int *) malloc(sizeof(int) * k);
        for (int i = 0; i < k; i++) {
            transformListA[i] = argRandomA();
            transformListB[i] = argRandomB();
            recordsList[i] = 0; // min uint64_t
        }
        usingHash = rollHash;
        matrix = usingHash->getMatrix();
        mask = FLAGS_OdessSamplingMask;
        printf("Sampling Mask : %lx\n", mask);
    }

    ~NFeatureSample() {
        printf("NFeature release\n");

        free(recordsList);
        free(transformListA);
        free(transformListB);
    }

    //0x0000400303410000
    void detect(uint8_t *inputPtr, uint64_t length) override {
        uint64_t hashValue = 0;
        for (uint64_t i = 0; i < length; i++) {
            hashValue = (hashValue << FLAGS_OdessShiftBits) + matrix[*(inputPtr + i)];
            //uint64_t hashValue = usingHash->rolling(inputPtr + i);
            if (!(hashValue & mask)) {
                for (int j = 0; j < featureAmount; j++) {
                    uint64_t transResult = (hashValue * transformListA[j] + transformListB[j]);
                    //uint64_t transResult = featureTranformation(hashValue, j);
                    if (transResult > recordsList[j])
                        recordsList[j] = transResult;
                }
            }
        }

        if (0) {
            for (int i = 0; i < featureAmount; i++) {
                printf("feature #%d : %lu\n", i, recordsList[i]);
            }
        }
    }

    void detectTest(uint8_t *inputPtr, uint64_t length) override {
        for (uint64_t i = 0; i < length; i++) {
            char test = *(inputPtr + i);
            uint64_t hashValue = usingHash->rolling(inputPtr + i);
            for (int j = 0; j < featureAmount; j++) {
                uint64_t transResult = featureTranformation(hashValue, j);
                if (transResult > recordsList[j])
                    recordsList[j] = transResult;
            }
        }

        if (1) {
            for (int i = 0; i < featureAmount; i++) {
                printf("feature #%d : %lu\n", i, recordsList[i]);
            }
        }
    }

    int getResult(SFSet *result) override {
        //uint64_t sf = 0;
        //uint64_t records;
        //uint8_t *ptr = (uint8_t *) &records;
        /*
        for(int i=0; i<3; i++){
            resetHash();
            for(int j=0; j<4; j++){
                records = recordsList[i*4+j];
                for(int k=0; k< sizeof(uint64_t); k++){
                    sf = usingHash->rolling(ptr + k);
                }
            }
        }
        */
        //resetHash();
        if (featureAmount == 24) {
            result->sf1 = XXH64(&recordsList[0 * 8], sizeof(uint64_t) * 8, 0x7fcaf1);
            result->sf2 = XXH64(&recordsList[1 * 8], sizeof(uint64_t) * 8, 0x7fcaf1);
            result->sf3 = XXH64(&recordsList[2 * 8], sizeof(uint64_t) * 8, 0x7fcaf1);
        } else if (featureAmount == 18) {
            result->sf1 = XXH64(&recordsList[0 * 6], sizeof(uint64_t) * 6, 0x7fcaf1);
            result->sf2 = XXH64(&recordsList[1 * 6], sizeof(uint64_t) * 6, 0x7fcaf1);
            result->sf3 = XXH64(&recordsList[2 * 6], sizeof(uint64_t) * 6, 0x7fcaf1);
        } else if (featureAmount == 15) {
            result->sf1 = XXH64(&recordsList[0 * 5], sizeof(uint64_t) * 5, 0x7fcaf1);
            result->sf2 = XXH64(&recordsList[1 * 5], sizeof(uint64_t) * 5, 0x7fcaf1);
            result->sf3 = XXH64(&recordsList[2 * 5], sizeof(uint64_t) * 5, 0x7fcaf1);
        } else if (featureAmount == 12) {
            result->sf1 = XXH64(&recordsList[0 * 4], sizeof(uint64_t) * 4, 0x7fcaf1);
            result->sf2 = XXH64(&recordsList[1 * 4], sizeof(uint64_t) * 4, 0x7fcaf1);
            result->sf3 = XXH64(&recordsList[2 * 4], sizeof(uint64_t) * 4, 0x7fcaf1);
        } else if (featureAmount == 9) {
            result->sf1 = XXH64(&recordsList[0 * 3], sizeof(uint64_t) * 3, 0x7fcaf1);
            result->sf2 = XXH64(&recordsList[1 * 3], sizeof(uint64_t) * 3, 0x7fcaf1);
            result->sf3 = XXH64(&recordsList[2 * 3], sizeof(uint64_t) * 3, 0x7fcaf1);
        } else if (featureAmount == 6) {
            result->sf1 = XXH64(&recordsList[0 * 2], sizeof(uint64_t) * 2, 0x7fcaf1);
            result->sf2 = XXH64(&recordsList[1 * 2], sizeof(uint64_t) * 2, 0x7fcaf1);
            result->sf3 = XXH64(&recordsList[2 * 2], sizeof(uint64_t) * 2, 0x7fcaf1);
        } else if (featureAmount == 3) {
            result->sf1 = XXH64(&recordsList[0 * 1], sizeof(uint64_t) * 1, 0x7fcaf1);
            result->sf2 = XXH64(&recordsList[1 * 1], sizeof(uint64_t) * 1, 0x7fcaf1);
            result->sf3 = XXH64(&recordsList[2 * 1], sizeof(uint64_t) * 1, 0x7fcaf1);
        }

        return featureAmount;
    }

    virtual int setTotalLength(uint64_t length) override {
        resetHash();
        for (int i = 0; i < featureAmount; i++) {
            recordsList[i] = 0; // min uint64_t
        }
        return 0;
    }

    virtual int resetHash() override {
        usingHash->reset();
        return 0;
    }

private:
    int featureAmount;
    uint64_t *recordsList;
    int *transformListA;
    int *transformListB;
    RollHash *usingHash;
    uint64_t *matrix;

    std::default_random_engine randomEngine;
    std::uniform_int_distribution<uint64_t> distributionA;
    std::uniform_int_distribution<uint64_t> distributionB;

    uint64_t mask;

    uint64_t argRandomA() {
        return distributionA(randomEngine);
    }

    uint64_t argRandomB() {
        return distributionB(randomEngine);
    }

    uint64_t featureTranformation(uint64_t hash, int index) {
        return hash * transformListA[index] + transformListB[index];
    }
};

#endif //ODESSSTORAGE_NFEATURESAMPLE_H
