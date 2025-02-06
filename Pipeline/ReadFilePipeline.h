//
// Created by BorelsetR on 2019/7/29.
//

#ifndef ODESSNEW_READFILEPIPELINE_H
#define ODESSNEW_READFILEPIPELINE_H

#include <sys/time.h>
#include "../Utility/StorageTask.h"
#include "../Utility/FileOperator.h"
#include "ChunkingPipeline.h"

uint64_t  ReadDuration = 0;

const uint64_t ReadPipelineChunkSize = (uint64_t) 128 * 1024 * 1024;

class ReadFilePipeline {
public:
    ReadFilePipeline() : runningFlag(true), taskAmount(0), mutexLock(), condition(mutexLock) {
        worker = new std::thread(std::bind(&ReadFilePipeline::readFileCallback, this));
    }

    int addTask(StorageTask *storageTask) {
        MutexLockGuard mutexLockGuard(mutexLock);
        taskList.push_back(storageTask);
        taskAmount++;
        condition.notify();
        return 0;
    }

    ~ReadFilePipeline() {
        // todo worker destruction
        runningFlag = false;
        condition.notifyAll();
        worker->join();
    }

    void getStatistics() {
#ifdef DEBUG
        printf("read duration:%lu\n", duration);
#endif
    }

private:
    void readFileCallback() {
        StorageTask *storageTask;
        struct timeval t0, t1;
        struct timeval rt1, rt2, rt3, rt4;
        ChunkTask chunkTask;
        while (runningFlag) {
            {
                MutexLockGuard mutexLockGuard(mutexLock);
                while (!taskAmount) {
                    condition.wait();
                    if (!runningFlag) break;
                }
                if (!runningFlag) continue;
                taskAmount--;
                storageTask = taskList.front();
                taskList.pop_front();
            }

            chunkTask.eoi = storageTask->eoi;

            if (chunkTask.eoi) {
                GlobalChunkingPipelinePtr->addTask(chunkTask);
                continue;
            }

            CountdownLatch *cd = storageTask->countdownLatch;
            FileOperator fileOperator((char *) storageTask->path.c_str(), FileOpenType::Read);
            storageTask->length = FileOperator::size((char *) storageTask->path.c_str());
            storageTask->buffer = (uint8_t *) malloc(storageTask->length);
            uint64_t readOffset = 0;
            uint64_t readOnce = 0;
            chunkTask.fileID = storageTask->fileID;
            chunkTask.buffer = storageTask->buffer;
            chunkTask.length = storageTask->length;
            chunkTask.end_of_mix_group = storageTask->end_of_mix_group;

#ifdef DEBUG
            gettimeofday(&t0, NULL);
#endif
            bool first = true;
            while (readOnce = fileOperator.read(storageTask->buffer + readOffset, ReadPipelineChunkSize)) {
                if (!first)
                    GlobalChunkingPipelinePtr->addTask(chunkTask);
                //gettimeofday(&rt1, NULL);
                readOffset += readOnce;
                chunkTask.end = readOffset;
                //gettimeofday(&rt2, NULL);
                //read += (rt2.tv_sec - rt1.tv_sec) * 1000000 + rt2.tv_usec - rt1.tv_usec;
                //gettimeofday(&rt3, NULL);
                //add += (rt3.tv_sec - rt2.tv_sec) * 1000000 + rt3.tv_usec - rt2.tv_usec;
                if (first)
                    first = false;
            }
            chunkTask.countdownLatch = cd;
            GlobalChunkingPipelinePtr->addTask(chunkTask);
            chunkTask.countdownLatch = nullptr;
            cd->countDown();
            printf("read done\n");
            printf("Total Size:%lu\n", readOffset);
#ifdef DEBUG
            gettimeofday(&t1, NULL);
            storageTask->stageTimePoint.read = (t1.tv_sec - t0.tv_sec) * 1000000 + t1.tv_usec - t0.tv_usec;
            duration = (t1.tv_sec - t0.tv_sec) * 1000000 + t1.tv_usec - t0.tv_usec;
            ReadDuration += (t1.tv_sec - t0.tv_sec) * 1000000 + t1.tv_usec - t0.tv_usec;
            printf("read duration:%lu\n", duration);
#endif

        }
    }

    bool runningFlag;
    std::thread *worker;
    uint64_t taskAmount;
    std::list<StorageTask *> taskList;
    MutexLock mutexLock;
    Condition condition;
    uint64_t duration = 0;
    uint64_t totalSize = 0;
};

static ReadFilePipeline *GlobalReadPipelinePtr;

#endif //ODESSNEW_READFILEPIPELINE_H
