//
// Created by BorelsetR on 2019/8/31.
//

#ifndef ODESSSTORAGE_HASHINGPIPELINE_H
#define ODESSSTORAGE_HASHINGPIPELINE_H


#include "isa-l_crypto/mh_sha1.h"
#include "openssl/sha.h"
#include "DeduplicationPipeline.h"
#include <assert.h>



class HashingPipeline {
public:
    HashingPipeline() : runningFlag(true), taskAmount(0), mutexLock(), condition(mutexLock) {
        worker = new std::thread(std::bind(&HashingPipeline::hashingWorkerCallbackNew, this));
    }

    int addTask(const DedupTask &dedupTask) {
        MutexLockGuard mutexLockGuard(mutexLock);
        /*
        if (receiceList.empty()) {
            taskAmount += dedupList->size();
            receiceList.swap(*dedupList);
            condition.notifyAll();
        } else {
            for (const auto &dedupTask: *dedupList) {
                receiceList.push_back(dedupTask);
            }
            taskAmount += dedupList->size();
            dedupList->clear();
            condition.notifyAll();
        }
         */
        receiceList.push_back(dedupTask);
        taskAmount++;
        condition.notifyAll();
        return 0;
    }

    ~HashingPipeline() {

        runningFlag = false;
        condition.notifyAll();
        worker->join();
    }

    void getStatistics() {
#ifdef DEBUG
        printf("hash duration:%lu\n", HashDuration);
#endif
    }

private:
    void hashingWorkerCallback() {
        isal_mh_sha1_ctx ctx;
        //SHA_CTX ctx;
        struct timeval t0, t1, t2;

//        FileOperator lengthLog("length.log", FileOpenType::ReadWrite);
//        uint8_t* logBuffer = (uint8_t*)malloc(1024);


        while (runningFlag) {
            {
                MutexLockGuard mutexLockGuard(mutexLock);
                while (!taskAmount) {
                    condition.wait();
                    if (!runningFlag) break;
                }
                if (!runningFlag) continue;
                taskAmount = 0;
                taskList.swap(receiceList);
            }

            gettimeofday(&t0, NULL);
            for (auto &dedupTask : taskList) {
//                sprintf((char*)logBuffer, "%lu\n", dedupTask.length);
//                lengthLog.write(logBuffer, strlen((char*)logBuffer));
                //SHA1_Init(&ctx);
                //SHA1_Update(&ctx, dedupTask.buffer + dedupTask.pos, (uint32_t) dedupTask.length);
                //SHA1_Final((unsigned char *) &dedupTask.fp, &ctx);
                isal_mh_sha1_init(&ctx);
                isal_mh_sha1_update(&ctx, dedupTask.buffer + dedupTask.pos, (uint32_t) dedupTask.length);
                isal_mh_sha1_finalize(&ctx, &dedupTask.fp);
                if (dedupTask.countdownLatch) {
                    dedupTask.countdownLatch->countDown();
                }
                GlobalDeduplicationPipelinePtr->addTask(dedupTask);
            }
            taskList.clear();
            gettimeofday(&t1, NULL);
            HashDuration += (t1.tv_sec - t0.tv_sec) * 1000000 + t1.tv_usec - t0.tv_usec;
        }
    }

    void hashingWorkerCallbackNew() {
        isal_mh_sha1_ctx ctx;
        //SHA_CTX ctx;
        struct timeval t0, t1, t2;
        int loop_count = 0;

//        FileOperator lengthLog("length.log", FileOpenType::ReadWrite);
//        uint8_t* logBuffer = (uint8_t*)malloc(1024);


        while (runningFlag) {
            {
                MutexLockGuard mutexLockGuard(mutexLock);
                while (!taskAmount) {
                    condition.wait();
                    if (!runningFlag) break;
                }
                if (!runningFlag) continue;
                taskAmount = 0;
                taskList.swap(receiceList);
            }

            gettimeofday(&t0, NULL);
            for (auto &dedupTask : taskList) {
                if (dedupTask.eoi) {
                    if (loop_count < MIX_GROUP_SIZE)
                        this->interleave(loop_count);
                    GlobalDeduplicationPipelinePtr->addTask(dedupTask);
                    break;
                }
                isal_mh_sha1_init(&ctx);
                isal_mh_sha1_update(&ctx, dedupTask.buffer + dedupTask.pos, (uint32_t) dedupTask.length);
                isal_mh_sha1_finalize(&ctx, &dedupTask.fp);
                int temp_loop_count = loop_count;
                if (dedupTask.countdownLatch) {
                    dedupTask.countdownLatch->countDown();
                    if (!dedupTask.end_of_mix_group) {
                        dedupTask.countdownLatch->countDown();
                        dedupTask.countdownLatch->countDown();
                    }
                    ++loop_count;
                }
                this->bufferedTaskList[temp_loop_count].push_back(dedupTask);
                if (loop_count == MIX_GROUP_SIZE) {
                    loop_count = 0;
                    this->interleave(MIX_GROUP_SIZE);
                }
            }
            taskList.clear();
            gettimeofday(&t1, NULL);
            HashDuration += (t1.tv_sec - t0.tv_sec) * 1000000 + t1.tv_usec - t0.tv_usec;
        }
    }

    void interleave(int num_of_list = MIX_GROUP_SIZE) {
        std::vector<uint8_t*> buf_list;
        DedupTask temp;
        bool last = false;
        for (int i = 0; i < num_of_list; ++i)
            buf_list.push_back(this->bufferedTaskList[i].front().buffer);
        while (true) {
            bool empty = true;
            for (int i = 0; i < num_of_list; ++i)
                if (!bufferedTaskList[i].empty()) {
                    empty = false;
                    break;
                }
            if (empty)
                break;
            for (int i = 0; i < num_of_list; ++i)
                if (!bufferedTaskList[i].empty()) {
                    auto& task = bufferedTaskList[i].front();
                    if (task.countdownLatch)
                        if (i == num_of_list - 1) {
                            assert(!last);
                            last = true;
                            temp = std::move(task);
                            bufferedTaskList[i].pop_front();
                            continue;
                        }
                        else
                            task.countdownLatch = nullptr;
                    GlobalDeduplicationPipelinePtr->addTask(task);
                    bufferedTaskList[i].pop_front();
                }
        }
        if (last)
            GlobalDeduplicationPipelinePtr->addTask(temp);
    }

    std::thread *worker;
    std::list<DedupTask> taskList;
    std::list<DedupTask> receiceList;
    std::list<DedupTask> bufferedTaskList[MIX_GROUP_SIZE];
    int taskAmount;
    bool runningFlag;
    MutexLock mutexLock;
    Condition condition;
    uint64_t HashDuration = 0;
    
};

static HashingPipeline *GlobalHashingPipelinePtr;


#endif //ODESSSTORAGE_HASHINGPIPELINE_H
