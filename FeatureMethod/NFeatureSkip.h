//
// Created by BorelsetR on 2019/8/7.
//

#ifndef ODESSSTORAGE_NFEATURESKIP_H
#define ODESSSTORAGE_NFEATURESKIP_H

#include <cstdint>
#include <cstdlib>
#include <random>
#include "../RollHash/RollHash.h"
#include "DetectMethod.h"
#include "../Utility/xxhash.h"

class NFeatureSkip : public DetectMethod {
public:
    NFeatureSkip(int k, RollHash *rollHash) : featureAmount(12) {
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
    }

    ~NFeatureSkip() {
        printf("NFeature release\n");

        free(recordsList);
        free(transformListA);
        free(transformListB);
    }

    void detect(uint8_t *inputPtr, uint64_t length) override {
        for (uint64_t i = 0; i < length; i++) {
            //uint64_t hashValue = usingHash->rolling(inputPtr + i);
            uint64_t hashValue = (hashValue << 1) + matrix[*(inputPtr + i)];
            for (int j = 0; j < featureAmount; j++) {
                uint64_t transResult = hashValue * transformListA[j] + transformListB[j];
                //uint64_t transResult = featureTranformation(hashValue, j) & mod32Mask;
                if (transResult > recordsList[j])
                    recordsList[j] = transResult;
            }
            i += 128;
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
        result->sf1 = XXH64(&recordsList[0 * 4], sizeof(uint64_t) * 4, 0x7fcaf1);
        result->sf2 = XXH64(&recordsList[1 * 4], sizeof(uint64_t) * 4, 0x7fcaf1);
        result->sf3 = XXH64(&recordsList[2 * 4], sizeof(uint64_t) * 4, 0x7fcaf1);

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

#endif //ODESSSTORAGE_NFEATURESKIP_H
