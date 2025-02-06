//
// Created by BorelsetR on 2019/7/23.
//

#ifndef ODESS_DETECTOR_H
#define ODESS_DETECTOR_H

#include "../Utility/FileOperator.h"
#include "NFeature.h"
#include "FinesseFeature.h"

const uint64_t DetectorBufferSize = 4096;

enum class DetectType {
    NFeature,
    Finesse,
};

class Detector {
public:
    Detector(DetectType dType, int featureAmount, RollHash *rollHash) {
        switch (dType) {
            case DetectType::NFeature:
                detectMethod = new NFeature(featureAmount, rollHash);
                break;
            case DetectType::Finesse:
                detectMethod = new FinesseFeature(featureAmount, rollHash);
                break;
        }
    }

    int doDetect(uint8_t *baseBuffer, uint64_t baselength,
                 uint8_t *derivedBuffer, uint64_t derivedLength) {
        std::vector<uint64_t> feature1, feature2;

        detectMethod->setTotalLength(baselength);
        detectMethod->detect(baseBuffer, baselength);
        detectMethod->getResult(&feature1);
        detectMethod->resetHash();

        detectMethod->setTotalLength(baselength);
        detectMethod->detect(derivedBuffer, derivedLength);
        detectMethod->getResult(&feature2);
        detectMethod->resetHash();

        /*
        FileOperator fileOperator1((char *) firstPath.c_str(), FileOpenType::Read);
        detectMethod->setTotalLength(FileOperator::size(firstPath.c_str()));
        while (readBytes = fileOperator1.read(buffer, DetectorBufferSize)) {
            detectMethod->detect(buffer, DetectorBufferSize);
        }
        detectMethod->getResult(&feature1);
        detectMethod->resetHash();

        FileOperator fileOperator2((char *) secondPath.c_str(), FileOpenType::Read);
        detectMethod->setTotalLength(FileOperator::size(secondPath.c_str()));
        while (readBytes = fileOperator2.read(buffer, DetectorBufferSize)) {
            detectMethod->detect(buffer, DetectorBufferSize);
        }
        detectMethod->getResult(&feature2);
        detectMethod->resetHash();
         */

        // 判断是否相似，目前只返回0 todo

        if (1) {
            return 1;
        } else {
            return 0;
        }
    }

    ~Detector() {
        free(detectMethod);
    }

private:
    DetectMethod *detectMethod;
};

#endif //ODESS_DETECTOR_H
