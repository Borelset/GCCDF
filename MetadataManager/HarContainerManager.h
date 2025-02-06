/*
 * Author   : Xiangyu Zou
 * Date     : 07/01/2021
 * Time     : 20:03
 * Project  : OdessStorage
 This source code is licensed under the GPLv2
 */

#ifndef ODESSSTORAGE_HARCONTAINERMANAGER_H
#define ODESSSTORAGE_HARCONTAINERMANAGER_H
#include "MetadataManager.h"

extern const int ContainerSize;

// DEFINE_double(HARThreshold, 0.3, "");


struct HarEntry {
    uint64_t cid;
    uint64_t size;
};

bool HarOrder(const HarEntry &first, const HarEntry &second) {
    return first.size > second.size;
}

float HARSparseLimit = 0.01;

class HarContainerManager {
public:
    HarContainerManager(double HARThreshold = 0.7) {
        this->HARThreshold = HARThreshold;
    }

    int init() {
        InvolvedContainerList.clear();
        return 0;
    }

    bool isRecorded(uint64_t cid) {
        return InvolvedContainerList.find(cid) != InvolvedContainerList.end();
    }

    bool isSparse(uint64_t cid) {
        return SparseContainerList.find(cid) != SparseContainerList.end();
    }

    int addRecord(uint64_t cid, uint64_t size){
        auto iter2 = InvolvedContainerList.find(cid);
        if(iter2 != InvolvedContainerList.end()){
            iter2->second += size;
        }else{
            InvolvedContainerList[cid] = size;
        }
        return 0;
    }

    int add_and_update(uint64_t cid, uint64_t size){
        this->addRecord(cid, size);
        this->update();
        return 0;
    }

    int deleteRecord(uint64_t cid) {
        InvolvedContainerList.erase(cid);
        return 0;
    }


    int update(){
        uint64_t totalSize = 0;
        uint64_t sparseSize = 0;
        std::list<HarEntry> tempMap;
        uint64_t sparse = 0;
        for (const auto &entry: InvolvedContainerList) {
            totalSize += entry.second;
            if (1.0 * entry.second / ContainerSize < HARThreshold) {
                sparseSize += entry.second;
                tempMap.push_back({entry.first, entry.second});
                sparse++;
            }
        }
        printf("sparse candidate: %lu\n", tempMap.size());

        tempMap.sort(HarOrder);

        while (1.0 * sparseSize / totalSize > HARSparseLimit) {
            const auto iter = tempMap.begin();
            sparseSize -= iter->size;
            sparse--;
            tempMap.erase(iter);
        }

//        SparseContainerList.clear();

        for (auto item: tempMap) {
            SparseContainerList.insert(item.cid);
        }

        for (auto it = SparseContainerList.begin(); it != SparseContainerList.end(); ) {
            if (!(GlobalMetadataManagerPtr->isAvaialableContainer(*it))) {
                it = SparseContainerList.erase(it);
            } else {
                ++it;
            }
        }
        return 0;
    }

private:
    std::set<uint64_t> SparseContainerList;
    std::unordered_map<uint64_t, uint64_t> InvolvedContainerList;

    int srMonitorThreshold = 2560;
    double HARThreshold;
};

#endif //ODESSSTORAGE_HARCONTAINERMANAGER_H
