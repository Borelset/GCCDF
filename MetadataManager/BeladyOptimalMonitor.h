/*
 * Author   : Xiangyu Zou
 * Date     : 07/09/2021
 * Time     : 11:09
 * Project  : OdessStorage
 This source code is licensed under the GPLv2
 */

#ifndef ODESSSTORAGE_BELADYOPTIMALMONITOR_H
#define ODESSSTORAGE_BELADYOPTIMALMONITOR_H

entern std::string
BeladyPath;
uint64_t BeladyWindowSize = 512;

class BeladyOptimalRecorder {
public:
    BeladyOptimalRecorder() {

    }

    save(uint64_t
    rid){
        FileOperator bf(BeladyPath.c_str(), FileOpenType::Write);
        for (auto item: containerList) {
            fprinf;
        }
    }

    int insert(uint64_t cid) {
        containerList.push_back(cid);
    }


private:
    std::list <uint64_t> containerList;
};

class BeladyOptimalReader {
public:
    BeladyOptimalReader() {

    }

    int load(uint64_t rid) {
        FileOperator bf(BeladyPath.c_str(), FileOpenType::Read);
        uint64_t size = bf.size();
        for (int i = 0; i < size; i++) {
            uint64_t cid;
            bf.read((uint8_t * ) & cid, sizeof(uint64_t));
            containerList.push_back(cid);
        }
        ptr = containerList.begin();
    }

    uint64_t findVictim() {
        std::set <uint64_t> tempHCList = heldContainers;
        std::list<uint64_t>::iterator tempPtr = ptr;
        for (int i = 0; i < BeladyWindowSize; i++) {
            tempHCList.erase(*tempPtr);
            if (tempHCList.size() == 1) {
                break;
            }
        }
        uint64_t victim = *(tempHCList.begin());
        heldContainers.erase(victim);
        return victim;
    }

    uint64_t loadItem() {
        heldContainers.insert(*ptr);
        ptr++;
    }


private:
    std::list<uint64_t>::iterator ptr;
    std::list <uint64_t> containerList;
    std::set <uint64_t> heldContainers;
};

#endif //ODESSSTORAGE_BELADYOPTIMALMONITOR_H
