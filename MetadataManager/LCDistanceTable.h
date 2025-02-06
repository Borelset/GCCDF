//
// Created by Borelset on 2021/11/25.
//

#ifndef ODESSSTORAGE_LCDISTANCETABLE_H
#define ODESSSTORAGE_LCDISTANCETABLE_H


class LCDistanceTable {
public:
    void addLCDistance(uint64_t cid1, uint64_t cid2, uint32_t distance) {
        distanceTable[cid1][distance] = cid2;
        distanceTable[cid2][distance] = cid1;
    }

    int getNearest(uint64_t cid, uint64_t *result, uint32_t *distance) {
        auto iter = distanceTable.find(cid);
        if (iter == distanceTable.end() || iter->second.empty()) return 0;

        *result = iter->second.begin()->second;
        *distance = iter->second.begin()->first;
        return 1;
    }

    void removeCID(uint64_t cid) {
        distanceTable.erase(cid);
        for (auto iter = distanceTable.begin(); iter != distanceTable.end(); iter++) {
            for (auto iter2 = iter->second.begin(); iter2 != iter->second.end(); iter2++) {
                if (iter2->second == cid) {
                    iter->second.erase(iter2);
                    break;
                }
            }
        }
    }

private:
    std::unordered_map<uint64_t, std::map<uint32_t, uint64_t>> distanceTable;
};

#endif //ODESSSTORAGE_LCDISTANCETABLE_H
