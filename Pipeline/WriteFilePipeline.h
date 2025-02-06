//
// Created by BorelsetR on 2019/7/29.
//

#ifndef ODESSNEW_WRITEFILEPIPELINE_H
#define ODESSNEW_WRITEFILEPIPELINE_H


#include "../MetadataManager/MetadataManager.h"
#include "../Utility/FileOperator.h"

uint64_t WriteDuration = 0;

DEFINE_string(ChunkFilePath, "/mnt/ssd-r/OdessHome/chunkFiles/%lu", "chunk path");
DEFINE_string(LogicFilePath, "/mnt/ssd-1/OdessHome/logicFiles/%lu", "logic path");
DEFINE_int32(ContainerSize, 4, "Unit:MB");

std::string LogicFilePath = FLAGS_LogicFilePath;
std::string ChunkFilePath = FLAGS_ChunkFilePath;

uint64_t GlobalWorkloadSize = 0, GlobalDeduplicatedSize = 0;

#define MemoryMatadata
#define DEBUG

const uint64_t MaxTask = 100;

const int ContainerSize = FLAGS_ContainerSize * 1024 * 1024;

class WriteFilePipeline {
public:
    WriteFilePipeline() : fid(0), length(0), runningFlag(true), taskAmount(0), mutexLock(), condition(mutexLock) {
        ::LogicFilePath = FLAGS_LogicFilePath;
        ::ChunkFilePath = FLAGS_ChunkFilePath;
        worker = new std::thread(std::bind(&WriteFilePipeline::writeFileCallback, this));
        sprintf(buffer, ChunkFilePath.c_str(), fid);
        chunkFileOperator = new FileOperator(buffer, FileOpenType::Write);
        assert(chunkFileOperator->getSize() == 0);
    }

    int addTask(const WriteTask &writeTask) {
        MutexLockGuard mutexLockGuard(mutexLock);
        /*
        taskAmount += writeList->size();
        if (recieveList.empty()) {
            recieveList.swap(*writeList);
        } else {
            for (const auto &writeTask : (*writeList)) {
                recieveList.push_back(writeTask);
            }
        }
        condition.notify();
         */
        recieveList.push_back(writeTask);
        taskAmount++;
        condition.notify();
        return 0;
    }

    uint64_t getFid() {
        return fid++;
    }

    ~WriteFilePipeline() {
        // todo worker destruction
        if (chunkFileOperator) delete chunkFileOperator;
        runningFlag = false;
        condition.notifyAll();
        worker->join();
    }

    void getStatistics() {
#ifdef DEBUG
        printf("write duration:%lu\n", duration);
        // printf("current deduplication ratio:%f\n", (float) workloadSize / leftSize);
        printf("Global deduplication ratio:%f\n", (float) GlobalWorkloadSize / GlobalDeduplicatedSize);
#endif
    }

private:
    void writeFileCallback() {
        WriteHead writeHead;
        BlockHead blockHead;
        Location location;
        uint64_t total = 0;
        uint64_t newChunk = 0;
        uint64_t similarChunk = 0;
        struct timeval t0, t1;
        uint8_t *writeBuffer = (uint8_t *) malloc(1024);
        uint64_t writeBufferLength = 1024;

        uint32_t locLength, oriLength;
        std::unordered_set<uint8_t*> bufferSet;

        while (runningFlag) {
            {
                MutexLockGuard mutexLockGuard(mutexLock);
                while (!taskAmount) {
                    condition.wait();
                    if (!runningFlag) break;
                }
                if (!runningFlag) continue;
                taskAmount = 0;
                condition.notify();
                taskList.swap(recieveList);
            }
#ifdef DEBUG
            gettimeofday(&t0, NULL);
#endif
            //printf("first index:%lu\n", taskList.begin()->index);
            for (auto &writeTask : taskList) {
                if (writeTask.eoi)
                    continue;
                bufferSet.insert(writeTask.buffer);
                if (logicFileOperator.empty()) {
                    // sprintf(buffer, LogicFilePath.c_str(), writeTask.fileID);
                    // logicFileOperator = new FileOperator(buffer, FileOpenType::Write);

                    sprintf(buffer, ChunkFilePath.c_str(), fid);
                    chunkFileOperator = new FileOperator(buffer, FileOpenType::Write);
                    assert(chunkFileOperator->getSize() == 0);
                }
                if (this->workloadSize.find(writeTask.fileID) == this->workloadSize.end())
                    workloadSize[writeTask.fileID] = 0;
                workloadSize[writeTask.fileID] += writeTask.bufferLength;

                switch (writeTask.type) {
                    case 0:
                        writeHead.type = 0;
                        writeHead.length = sizeof(Location);
                        writeHead.fp = writeTask.sha1Fp;
                        writeHead.id = writeTask.id;
//                        writeHead.bfp = writeTask.location.baseFP;
                        writeTask.location.length = writeTask.bufferLength + sizeof(BlockHead);

                        if (logicFileOperator.find(writeTask.fileID) == logicFileOperator.end()) {
                            sprintf(buffer, LogicFilePath.c_str(), writeTask.fileID);
                            logicFileOperator[writeTask.fileID] = new FileOperator(buffer, FileOpenType::Write);
                        }

                        logicFileOperator[writeTask.fileID]->write((uint8_t *) &writeHead, sizeof(WriteHead));
                        logicFileOperator[writeTask.fileID]->write((uint8_t *) &writeTask.location, sizeof(Location));

                        break;
                    case 1:
                        assert(0);
//                        writeHead.type = 1;
//                        writeHead.length = sizeof(Location);
//                        writeHead.fp = writeTask.sha1Fp;
////                        writeHead.bfp = writeTask.baseFP;
//
//                        blockHead.type = 1;
//                        blockHead.sha1Fp = writeTask.sha1Fp;
//                        blockHead.length = writeTask.deltaBufferLength + sizeof(BlockHead);
////                        blockHead.baseFP = writeTask.baseFP;
//                        chunkFileOperator->write((uint8_t *) &blockHead, sizeof(BlockHead));
//                        chunkFileOperator->write(writeTask.deltaBuffer, writeTask.deltaBufferLength);
//                        //chunkFileOperator->fdatasync();
//                        length += writeTask.deltaBufferLength + sizeof(BlockHead);
//
//                        logicFileOperator->write((uint8_t *) &writeHead, sizeof(WriteHead));
//                        logicFileOperator->write((uint8_t *) &writeTask.location, sizeof(Location));
//                        free(writeTask.deltaBuffer);
//
//                        leftSize += writeTask.deltaBufferLength;
//                        GlobalDeduplicatedSize += writeTask.deltaBufferLength;
//
//                        similarChunk++;
//                        break;
                    case 2:
                        blockHead.type = 0;
                        blockHead.sha1Fp = writeTask.sha1Fp;
                        blockHead.length = writeTask.bufferLength + sizeof(BlockHead);
                        blockHead.id = writeTask.id;
//                        SFSet features = {writeTask.sf1, writeTask.sf2, writeTask.sf3};
//                        blockHead.features = features;
                        chunkFileOperator->write((uint8_t *) &blockHead, sizeof(BlockHead));
                        chunkFileOperator->write(writeTask.buffer + writeTask.pos, writeTask.bufferLength);
                        length += writeTask.bufferLength + sizeof(BlockHead);

                        writeHead.type = 0;
                        writeHead.length = sizeof(Location);
                        writeHead.fp = writeTask.sha1Fp;
                        writeHead.id = writeTask.id;

                        locLength = writeTask.bufferLength + sizeof(BlockHead);
                        location = {
                                fid, length, locLength
                        };

                        if (logicFileOperator.find(writeTask.fileID) == logicFileOperator.end()) {
                            sprintf(buffer, LogicFilePath.c_str(), writeTask.fileID);
                            logicFileOperator[writeTask.fileID] = new FileOperator(buffer, FileOpenType::Write);
                        }

                        logicFileOperator[writeTask.fileID]->write((uint8_t *) &writeHead, sizeof(WriteHead));
                        logicFileOperator[writeTask.fileID]->write((uint8_t *) &location, sizeof(Location));

                        leftSize += writeTask.bufferLength;

                        newChunk++;
                        break;
                }

                if (writeTask.countdownLatch) {
                    printf("write done\n");
                    for (auto& kv : this->logicFileOperator) {
                        kv.second->fsync();
                        delete kv.second;
                    }
                    this->logicFileOperator.clear();
                    // free(writeTask.buffer);
                    for (const auto& ptr : bufferSet)
                        free(ptr);
                    bufferSet.clear();

                    chunkFileOperator->fsync();
                    chunkFileOperator->releaseBufferedData();
                    GlobalMetadataManagerPtr->addAvailableContainer(fid);
                    delete chunkFileOperator;
                    chunkFileOperator = nullptr;
                    length = 0;
                    fid++;
                    getStatistics();

                    //GlobalMetadataManagerPtr->scanForFP(this->DEBUG_SHA1FP);
                    puts("--------------------");
                    // GlobalMetadataManagerPtr->traverseFile(1);

                    for (const auto& kv : this->workloadSize) {
                        GlobalWorkloadSize += kv.second;
                        GlobalMetadataManagerPtr->insertBackupSize(kv.first, kv.second);
                    }
                    GlobalDeduplicatedSize += leftSize;

                    leftSize = 0;
                    workloadSize.clear();
                    duration = 0;

                    writeTask.countdownLatch->countDown();
                }

                if (length >= ContainerSize) {
                    //sync();
//                    chunkFileOperator->fsync();
//                    chunkFileOperator->releaseBufferedData();
                    GlobalMetadataManagerPtr->addAvailableContainer(fid);
                    delete chunkFileOperator;
                    chunkFileOperator = nullptr;
                    length = 0;
                    fid++;
                    sprintf(buffer, ChunkFilePath.c_str(), fid);
                    chunkFileOperator = new FileOperator(buffer, FileOpenType::Write);
                    assert(chunkFileOperator->getSize() == 0);
                }
            }
            taskList.clear();
#ifdef DEBUG
            total++;
            gettimeofday(&t1, NULL);
            duration += (t1.tv_sec - t0.tv_sec) * 1000000 + t1.tv_usec - t0.tv_usec;
            WriteDuration += (t1.tv_sec - t0.tv_sec) * 1000000 + t1.tv_usec - t0.tv_usec;
#endif
        }
    }

    std::unordered_map<uint64_t, FileOperator*> logicFileOperator;
    FileOperator *chunkFileOperator;
    char buffer[256];
    uint64_t fid;
    uint64_t length;
    bool runningFlag;
    std::thread *worker;
    uint64_t taskAmount;
    std::list<WriteTask> taskList;
    std::list<WriteTask> recieveList;
    MutexLock mutexLock;
    Condition condition;
    uint64_t duration = 0;
    uint64_t queueAmount = 0;

    uint64_t leftSize = 0;
    std::unordered_map<uint64_t, uint64_t> workloadSize;
    const SHA1FP DEBUG_SHA1FP = {.fp1 = 15844408918433038934llu, .fp2 = 854042722u, .fp3 = 1615005289u, .fp4 = 3183670265u};
    const uint64_t DEBUG_ID = 73842;
    const uint64_t DEBUG_CID = 1436;
};

static WriteFilePipeline *GlobalWriteFilePipelinePtr;

#endif //ODESSNEW_WRITEFILEPIPELINE_H
