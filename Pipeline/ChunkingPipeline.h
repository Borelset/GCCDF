//
// Created by BorelsetR on 2019/7/29.
//

#ifndef ODESSNEW_CHUNKINGPIPELINE_H
#define ODESSNEW_CHUNKINGPIPELINE_H

#include <sys/time.h>
#include "../RollHash/Gear.h"
#include "../RollHash/Rabin.h"
#include "gflags/gflags.h"
#include <thread>
#include "isa-l_crypto/mh_sha1.h"
#include "openssl/sha.h"
#include "HashingPipeline.h"
#include "../RollHash/rabin_chunking.h"

uint64_t ChunkDuration = 0;

DEFINE_string(ChunkingMethod, "Gear", "chunking method in chunking");
DEFINE_int32(ExpectSize, 4096, "average chunk size");
//DEFINE_int32(MinChunkSize, FLAGS_ExpectSize/4, "min chunk size");
//DEFINE_int32(MaxChunkSize, FLAGS_ExpectSize*8, "max chunk size");

class ChunkingPipeline {
public:
    ChunkingPipeline()
            : taskAmount(0),
              runningFlag(true),
              mutexLock(),
              condition(mutexLock) {
        if (FLAGS_ChunkingMethod == std::string("Gear")) {
            rollHash = new Gear();
            matrix = rollHash->getMatrix();
            worker = new std::thread(std::bind(&ChunkingPipeline::chunkingWorkerCallbackGearNew, this));
        } else if (FLAGS_ChunkingMethod == std::string("Rabin")) {
            worker = new std::thread(std::bind(&ChunkingPipeline::chunkingWorkerCallbackRabin, this));
        } else if (FLAGS_ChunkingMethod == std::string("Fixed")){
            worker = new std::thread(std::bind(&ChunkingPipeline::chunkingWorkerCallbackFixed, this));
        }
        MaxChunkSize = FLAGS_ExpectSize*8;
        MinChunkSize = FLAGS_ExpectSize/4;

        printf("Max chunk size=%d, Min chunk size=%d\n", MaxChunkSize, MinChunkSize);
    }

    int addTask(ChunkTask chunkTask) {
        MutexLockGuard mutexLockGuard(mutexLock);
        taskList.push_back(chunkTask);
        taskAmount++;
        condition.notify();
        return 0;
    }

    void getStatistics() {
        printf("chunk duration:%lu\n", duration);
#ifdef DEBUG1
        printf("sha1:%lu, lock:%lu\n", sha1, lock);
        printf("inLoop:%lu\n", inLoop);
#endif
    }

    ~ChunkingPipeline() {
        // todo worker destruction
        delete rollHash;
        runningFlag = false;
        condition.notifyAll();
        worker->join();
    }


    int getMaxChunkSize(){
        return MaxChunkSize;
    }

private:
    void chunkingWorkerCallbackGear() {
        mh_sha1_ctx ctx;
        //SHA_CTX ctx;
        uint64_t posPtr = 0;
        uint64_t base = 0;
        uint64_t fp = 0;
        const uint64_t chunkMask = 0x0000d90f03530000;
        const uint64_t chunkMask2 = 0x0000d90003530000;
        uint64_t rabinMask = 8191;
        uint64_t counter = 0;
        uint8_t *data = nullptr;
        DedupTask dedupTask;
        CountdownLatch *cd;
        bool newFileFlag = true;
        std::list<DedupTask> saveList;
        ChunkTask chunkTask;
        uint64_t s = 0, e = 0, cs = 0, n = 0;
#ifdef DEBUG
        struct timeval t1, t0;
        struct timeval lt0, lt1;
        struct timeval ct0, ct1, ct2, ct3, ct4, ct5, ct6;
#endif
        while (runningFlag) {
            {
                MutexLockGuard mutexLockGuard(mutexLock);
                while (!taskAmount) {
                    condition.wait();
                    if (!runningFlag) break;
                }
                if (!runningFlag) continue;
                taskAmount--;
                chunkTask = taskList.front();
                taskList.pop_front();
            }
#ifdef DEBUG
            gettimeofday(&t0, NULL);
#endif
            if (newFileFlag) {
                posPtr = 0;
                base = 0;
                fp = 0;
                data = chunkTask.buffer;
                newFileFlag = false;
            }
            uint64_t end = chunkTask.end;

            dedupTask.buffer = chunkTask.buffer;
            dedupTask.length = chunkTask.length;
            dedupTask.fileID = chunkTask.fileID;

            if (!chunkTask.countdownLatch) {

                while (end - posPtr > MaxChunkSize) {

                    //fp = rollHash->rolling(data + posPtr);
#ifdef DEBUG1
                    gettimeofday(&ct0, NULL);
#endif
                    fp = (fp << 1) + matrix[data[posPtr]];
                    //fp = rollHashRabin.rolling(data + posPtr);
#ifdef DEBUG1
                    gettimeofday(&ct4, NULL);
                    inLoop += (ct4.tv_sec - ct0.tv_sec) * 1000000 + ct4.tv_usec - ct0.tv_usec;
#endif
                    if (posPtr - base < 8192) {
                        if (!(fp & chunkMask)) {
#ifdef DEBUG1
                            gettimeofday(&ct1, NULL);
#endif
                            dedupTask.pos = base;
                            dedupTask.length = posPtr - base + 1;
                            dedupTask.index = order++;

#ifdef DEBUG1
                            gettimeofday(&ct2, NULL);
                            sha1 += (ct2.tv_sec - ct1.tv_sec) * 1000000 + ct2.tv_usec - ct1.tv_usec;
#endif

                            GlobalHashingPipelinePtr->addTask(dedupTask);

                            base = posPtr + 1;
                            posPtr += MinChunkSize;

#ifdef DEBUG1
                            gettimeofday(&ct3, NULL);
                            lock += (ct3.tv_sec - ct2.tv_sec) * 1000000 + ct3.tv_usec - ct2.tv_usec;
#endif
                        }
                    } else {
                        if (!(fp & chunkMask2) || posPtr - base > MaxChunkSize) {
#ifdef DEBUG1
                            gettimeofday(&ct1, NULL);
#endif
                            dedupTask.pos = base;
                            dedupTask.length = posPtr - base + 1;
                            dedupTask.index = order++;

#ifdef DEBUG1
                            gettimeofday(&ct2, NULL);
                            sha1 += (ct2.tv_sec - ct1.tv_sec) * 1000000 + ct2.tv_usec - ct1.tv_usec;
#endif

                            GlobalHashingPipelinePtr->addTask(dedupTask);

                            base = posPtr + 1;
                            posPtr += MinChunkSize;

#ifdef DEBUG1
                            gettimeofday(&ct3, NULL);
                            lock += (ct3.tv_sec - ct2.tv_sec) * 1000000 + ct3.tv_usec - ct2.tv_usec;
#endif
                        }
                    }
                    posPtr++;

                }
            } else {
                while (posPtr < end) {
#ifdef DEBUG1
                    gettimeofday(&ct0, NULL);
#endif
                    fp = (fp << (uint64_t) 1) + matrix[data[posPtr]];
                    //fp = rollHashRabin.rolling(data + posPtr);
#ifdef DEBUG1
                    gettimeofday(&ct4, NULL);
                    inLoop += (ct4.tv_sec - ct0.tv_sec) * 1000000 + ct4.tv_usec - ct0.tv_usec;
#endif
                    if (posPtr - base < 8192) {
                        if (!(fp & chunkMask)) {
#ifdef DEBUG1
                            gettimeofday(&ct1, NULL);
#endif
                            dedupTask.pos = base;
                            dedupTask.length = posPtr - base + 1;
                            dedupTask.index = order++;

#ifdef DEBUG1
                            gettimeofday(&ct2, NULL);
                            sha1 += (ct2.tv_sec - ct1.tv_sec) * 1000000 + ct2.tv_usec - ct1.tv_usec;
#endif

                            GlobalHashingPipelinePtr->addTask(dedupTask);

                            base = posPtr + 1;
                            posPtr += MinChunkSize;

#ifdef DEBUG1
                            gettimeofday(&ct3, NULL);
                            lock += (ct3.tv_sec - ct2.tv_sec) * 1000000 + ct3.tv_usec - ct2.tv_usec;
#endif
                        }
                    } else {
                        if (!(fp & chunkMask2) || posPtr - base > MaxChunkSize) {
#ifdef DEBUG1
                            gettimeofday(&ct1, NULL);
#endif
                            dedupTask.pos = base;
                            dedupTask.length = posPtr - base + 1;
                            dedupTask.index = order++;

#ifdef DEBUG1
                            gettimeofday(&ct2, NULL);
                            sha1 += (ct2.tv_sec - ct1.tv_sec) * 1000000 + ct2.tv_usec - ct1.tv_usec;
#endif

                            GlobalHashingPipelinePtr->addTask(dedupTask);

                            base = posPtr + 1;
                            posPtr += MinChunkSize;

#ifdef DEBUG1
                            gettimeofday(&ct3, NULL);
                            lock += (ct3.tv_sec - ct2.tv_sec) * 1000000 + ct3.tv_usec - ct2.tv_usec;
#endif
                        }
                    }
                    posPtr++;
                }
                if (base != posPtr) {
                    dedupTask.pos = base;
                    dedupTask.length = end - base;
                    dedupTask.index = order++;

                    dedupTask.countdownLatch = chunkTask.countdownLatch;

                    GlobalHashingPipelinePtr->addTask(dedupTask);
                }
                chunkTask.countdownLatch->countDown();
                newFileFlag = true;
                dedupTask.countdownLatch = nullptr;
            }


#ifdef DEBUG
            gettimeofday(&t1, NULL);
            duration += (t1.tv_sec - t0.tv_sec) * 1000000 + t1.tv_usec - t0.tv_usec;
            ChunkDuration += (t1.tv_sec - t0.tv_sec) * 1000000 + t1.tv_usec - t0.tv_usec;
#endif
        }
    }

    void chunkingWorkerCallbackGearNew() {
        mh_sha1_ctx ctx;
        //SHA_CTX ctx;
        uint64_t posPtr = 0;
        uint64_t base = 0;
        uint64_t fp = 0;

        if(FLAGS_ExpectSize == 8192){
            chunkMask =  0x0000d90f03530000;//32
            chunkMask2 = 0x0000d90003530000;//2
        }else if(FLAGS_ExpectSize == 4096){
            chunkMask =  0x0000d90703530000;//16
            chunkMask2 = 0x0000590003530000;//1
        }else if(FLAGS_ExpectSize == 16384){
            chunkMask =  0x0000d90f13530000;//64
            chunkMask2 = 0x0000d90103530000;//4
        }

        uint64_t rabinMask = 8191;
        uint64_t counter = 0;
        uint8_t *data = nullptr;
        DedupTask dedupTask;
        CountdownLatch *cd;
        bool newFileFlag = true;
        std::list<DedupTask> saveList;
        ChunkTask chunkTask;
        uint64_t s = 0, e = 0, cs = 0, n = 0;
        bool flag = false;
#ifdef DEBUG
        struct timeval t1, t0;
        struct timeval lt0, lt1;
        struct timeval ct0, ct1, ct2, ct3, ct4, ct5, ct6;
#endif
        while (runningFlag) {
            {
                MutexLockGuard mutexLockGuard(mutexLock);
                while (!taskAmount) {
                    condition.wait();
                    if (!runningFlag) break;
                }
                if (!runningFlag) continue;
                taskAmount--;
                chunkTask = taskList.front();
                taskList.pop_front();
            }

            dedupTask.eoi = chunkTask.eoi;

            if (chunkTask.eoi) {
                GlobalHashingPipelinePtr->addTask(dedupTask);
                continue;
            }

            gettimeofday(&t0, NULL);

            if (newFileFlag) {
                posPtr = 0;
                base = 0;
                fp = 0;
                data = chunkTask.buffer;
                newFileFlag = false;
                flag = false;
            }
            uint64_t end = chunkTask.end;

            dedupTask.buffer = chunkTask.buffer;
            dedupTask.length = chunkTask.length;
            dedupTask.fileID = chunkTask.fileID;
            dedupTask.end_of_mix_group = chunkTask.end_of_mix_group;

            if (!chunkTask.countdownLatch) {
                while(end - posPtr > MaxChunkSize){
                    int chunkSize = fastcdc_chunk_data(data+posPtr, end-posPtr);
                    dedupTask.pos = base;
                    dedupTask.length = chunkSize;
                    dedupTask.index++;
                    GlobalHashingPipelinePtr->addTask(dedupTask);
                    base += chunkSize;
                    posPtr += chunkSize;
                }
                /*
                while (end - posPtr > FLAGS_MaxChunkSize) {
                    fp = (fp << 1) + matrix[data[posPtr]];
                    if (posPtr - base < 8192) {
                        if (!(fp & chunkMask)) {
                            dedupTask.pos = base;
                            dedupTask.length = posPtr - base + 1;
                            dedupTask.index = order++;
                            GlobalHashingPipelinePtr->addTask(dedupTask);

                            base = posPtr + 1;
                            posPtr += FLAGS_MinChunkSize;

                        }
                    } else {
                        if (!(fp & chunkMask2) || posPtr - base > FLAGS_MaxChunkSize) {
                            dedupTask.pos = base;
                            dedupTask.length = posPtr - base + 1;
                            dedupTask.index = order++;

                            GlobalHashingPipelinePtr->addTask(dedupTask);

                            base = posPtr + 1;
                            posPtr += FLAGS_MinChunkSize;
                        }
                    }
                    posPtr++;
                }
                 */
            } else {
                /*
                while (posPtr < end) {
                    fp = (fp << (uint64_t) 1) + matrix[data[posPtr]];

                    if (posPtr - base < 8192) {
                        if (!(fp & chunkMask)) {
                            dedupTask.pos = base;
                            dedupTask.length = posPtr - base + 1;
                            dedupTask.index = order++;

                            GlobalHashingPipelinePtr->addTask(dedupTask);

                            base = posPtr + 1;
                            posPtr += FLAGS_MinChunkSize;

                        }
                    } else {
                        if (!(fp & chunkMask2) || posPtr - base > FLAGS_MaxChunkSize) {
                            dedupTask.pos = base;
                            dedupTask.length = posPtr - base + 1;
                            dedupTask.index = order++;

                            GlobalHashingPipelinePtr->addTask(dedupTask);

                            base = posPtr + 1;
                            posPtr += FLAGS_MinChunkSize;
                        }
                    }
                    posPtr++;
                }
                if (base != posPtr) {
                    dedupTask.pos = base;
                    dedupTask.length = end - base;
                    dedupTask.index = order++;

                    dedupTask.countdownLatch = chunkTask.countdownLatch;

                    GlobalHashingPipelinePtr->addTask(dedupTask);
                }
                 */
                while(end != posPtr){
                    int chunkSize = fastcdc_chunk_data(data+posPtr, end-posPtr);
                    dedupTask.pos = base;
                    dedupTask.length = chunkSize;
                    dedupTask.index++;
                    if(end == posPtr + chunkSize){
                        dedupTask.countdownLatch = chunkTask.countdownLatch;
                        flag = true;
                    }
                    GlobalHashingPipelinePtr->addTask(dedupTask);
                    base += chunkSize;
                    posPtr += chunkSize;
                }
            }
            if(flag){
                chunkTask.countdownLatch->countDown();
                newFileFlag = true;
                dedupTask.countdownLatch = nullptr;
            }


#ifdef DEBUG
            gettimeofday(&t1, NULL);
            duration += (t1.tv_sec - t0.tv_sec) * 1000000 + t1.tv_usec - t0.tv_usec;
            ChunkDuration += (t1.tv_sec - t0.tv_sec) * 1000000 + t1.tv_usec - t0.tv_usec;
#endif
        }
    }

    void chunkingWorkerCallbackRabin() {
        mh_sha1_ctx ctx;
        //SHA_CTX ctx;
        uint64_t posPtr = 0;
        uint64_t base = 0;
        uint64_t fp = 0;
        const uint64_t chunkMask = 0x0000d90f03530000;
        const uint64_t chunkMask2 = 0x0000d90003530000;
        uint64_t rabinMask = FLAGS_ExpectSize-1;
        uint64_t counter = 0;
        uint8_t *data = nullptr;
        DedupTask dedupTask;
        CountdownLatch *cd;
        bool newFileFlag = true;
        std::list<DedupTask> saveList;
        ChunkTask chunkTask;
        uint64_t s = 0, e = 0, cs = 0, n = 0;
#ifdef DEBUG
        struct timeval t1, t0;
        struct timeval lt0, lt1;
        struct timeval ct0, ct1, ct2, ct3, ct4, ct5, ct6;
#endif
        while (runningFlag) {
            {
                MutexLockGuard mutexLockGuard(mutexLock);
                while (!taskAmount) {
                    condition.wait();
                    if (!runningFlag) break;
                }
                if (!runningFlag) continue;
                taskAmount--;
                chunkTask = taskList.front();
                taskList.pop_front();
            }
#ifdef DEBUG
            gettimeofday(&t0, NULL);
#endif
            if (newFileFlag) {
                posPtr = 0;
                base = 0;
                fp = 0;
                data = chunkTask.buffer;
                newFileFlag = false;
            }
            uint64_t end = chunkTask.end;

            dedupTask.buffer = chunkTask.buffer;
            dedupTask.length = chunkTask.length;
            dedupTask.fileID = chunkTask.fileID;

            if (!chunkTask.countdownLatch) {

                while (end - posPtr > MaxChunkSize) {

                    //fp = rollHash->rolling(data + posPtr);
#ifdef DEBUG1
                    gettimeofday(&ct0, NULL);
#endif
                    //fp = (fp << 1) + matrix[data[posPtr]];
                    fp = rollHashRabin.rolling(data + posPtr);
#ifdef DEBUG1
                    gettimeofday(&ct4, NULL);
                    inLoop += (ct4.tv_sec - ct0.tv_sec) * 1000000 + ct4.tv_usec - ct0.tv_usec;
#endif
                    if ((fp & rabinMask) == 0x78) {
#ifdef DEBUG1
                        gettimeofday(&ct1, NULL);
#endif
                        dedupTask.pos = base;
                        dedupTask.length = posPtr - base + 1;
                        dedupTask.index = order++;
                        cs += posPtr - base + 1;
                        n++;

#ifdef DEBUG1
                        gettimeofday(&ct2, NULL);
                            sha1 += (ct2.tv_sec - ct1.tv_sec) * 1000000 + ct2.tv_usec - ct1.tv_usec;
#endif
                        GlobalHashingPipelinePtr->addTask(dedupTask);

                        base = posPtr + 1;
                        posPtr += MinChunkSize;

#ifdef DEBUG1
                        gettimeofday(&ct3, NULL);
                            lock += (ct3.tv_sec - ct2.tv_sec) * 1000000 + ct3.tv_usec - ct2.tv_usec;
#endif

                    }
                    /*
                    if (posPtr - base < 8192) {
                        if (!(fp & chunkMask)) {
#ifdef DEBUG1
                            gettimeofday(&ct1, NULL);
#endif
                            dedupTask.pos = base;
                            dedupTask.length = posPtr - base + 1;
                            dedupTask.index = order++;

#ifdef DEBUG1
                            gettimeofday(&ct2, NULL);
                            sha1 += (ct2.tv_sec - ct1.tv_sec) * 1000000 + ct2.tv_usec - ct1.tv_usec;
#endif

                            GlobalHashingPipelinePtr->addTask(dedupTask);

                            base = posPtr + 1;
                            posPtr += FLAGS_MinChunkSize;

#ifdef DEBUG1
                            gettimeofday(&ct3, NULL);
                            lock += (ct3.tv_sec - ct2.tv_sec) * 1000000 + ct3.tv_usec - ct2.tv_usec;
#endif
                        }
                    } else {
                        if (!(fp & chunkMask2) || posPtr - base > FLAGS_MaxChunkSize) {
#ifdef DEBUG1
                            gettimeofday(&ct1, NULL);
#endif
                            dedupTask.pos = base;
                            dedupTask.length = posPtr - base + 1;
                            dedupTask.index = order++;

#ifdef DEBUG1
                            gettimeofday(&ct2, NULL);
                            sha1 += (ct2.tv_sec - ct1.tv_sec) * 1000000 + ct2.tv_usec - ct1.tv_usec;
#endif

                            GlobalHashingPipelinePtr->addTask(dedupTask);

                            base = posPtr + 1;
                            posPtr += FLAGS_MinChunkSize;

#ifdef DEBUG1
                            gettimeofday(&ct3, NULL);
                            lock += (ct3.tv_sec - ct2.tv_sec) * 1000000 + ct3.tv_usec - ct2.tv_usec;
#endif
                        }
                    }
                     */
                    posPtr++;

                }
            } else {
                while (posPtr < end) {
#ifdef DEBUG1
                    gettimeofday(&ct0, NULL);
#endif
                    //fp = (fp << (uint64_t) 1) + matrix[data[posPtr]];
                    fp = rollHashRabin.rolling(data + posPtr);
#ifdef DEBUG1
                    gettimeofday(&ct4, NULL);
                    inLoop += (ct4.tv_sec - ct0.tv_sec) * 1000000 + ct4.tv_usec - ct0.tv_usec;
#endif
                    if ((fp & rabinMask) == 0x78) {
#ifdef DEBUG1
                        gettimeofday(&ct1, NULL);
#endif
                        dedupTask.pos = base;
                        dedupTask.length = posPtr - base + 1;
                        dedupTask.index = order++;
                        cs += posPtr - base + 1;
                        n++;

#ifdef DEBUG1
                        gettimeofday(&ct2, NULL);
                            sha1 += (ct2.tv_sec - ct1.tv_sec) * 1000000 + ct2.tv_usec - ct1.tv_usec;
#endif

                        GlobalHashingPipelinePtr->addTask(dedupTask);

                        base = posPtr + 1;
                        posPtr += MinChunkSize;

#ifdef DEBUG1
                        gettimeofday(&ct3, NULL);
                            lock += (ct3.tv_sec - ct2.tv_sec) * 1000000 + ct3.tv_usec - ct2.tv_usec;
#endif

                    }
                    /*
                    if (posPtr - base < 8192) {
                        if (!(fp & chunkMask)) {
#ifdef DEBUG1
                            gettimeofday(&ct1, NULL);
#endif
                            dedupTask.pos = base;
                            dedupTask.length = posPtr - base + 1;
                            dedupTask.index = order++;

#ifdef DEBUG1
                            gettimeofday(&ct2, NULL);
                            sha1 += (ct2.tv_sec - ct1.tv_sec) * 1000000 + ct2.tv_usec - ct1.tv_usec;
#endif

                            GlobalHashingPipelinePtr->addTask(dedupTask);

                            base = posPtr + 1;
                            posPtr += FLAGS_MinChunkSize;

#ifdef DEBUG1
                            gettimeofday(&ct3, NULL);
                            lock += (ct3.tv_sec - ct2.tv_sec) * 1000000 + ct3.tv_usec - ct2.tv_usec;
#endif
                        }
                    } else {
                        if (!(fp & chunkMask2) || posPtr - base > FLAGS_MaxChunkSize) {
#ifdef DEBUG1
                            gettimeofday(&ct1, NULL);
#endif
                            dedupTask.pos = base;
                            dedupTask.length = posPtr - base + 1;
                            dedupTask.index = order++;

#ifdef DEBUG1
                            gettimeofday(&ct2, NULL);
                            sha1 += (ct2.tv_sec - ct1.tv_sec) * 1000000 + ct2.tv_usec - ct1.tv_usec;
#endif

                            GlobalHashingPipelinePtr->addTask(dedupTask);

                            base = posPtr + 1;
                            posPtr += FLAGS_MinChunkSize;

#ifdef DEBUG1
                            gettimeofday(&ct3, NULL);
                            lock += (ct3.tv_sec - ct2.tv_sec) * 1000000 + ct3.tv_usec - ct2.tv_usec;
#endif
                        }
                    }
                     */
                    posPtr++;
                }
                if (base != posPtr) {
                    dedupTask.pos = base;
                    dedupTask.length = end - base;
                    dedupTask.index = order++;

                    dedupTask.countdownLatch = chunkTask.countdownLatch;

                    GlobalHashingPipelinePtr->addTask(dedupTask);
                }
                chunkTask.countdownLatch->countDown();
                newFileFlag = true;
                dedupTask.countdownLatch = nullptr;
            }


#ifdef DEBUG
            gettimeofday(&t1, NULL);
            duration += (t1.tv_sec - t0.tv_sec) * 1000000 + t1.tv_usec - t0.tv_usec;
#endif
        }

    }

    int fastcdc_chunk_data(unsigned char *p, uint64_t n) {

        uint64_t fingerprint = 0, digest;
        int i = MinChunkSize, Mid = MinChunkSize + FLAGS_ExpectSize;
        //return n;

        if (n <= MinChunkSize) //the minimal  subChunk Size.
            return n;
        //windows_reset();
        if (n > MaxChunkSize)
            n = MaxChunkSize;
        else if (n < Mid)
            Mid = n;
        while (i < Mid) {
            fingerprint = (fingerprint << 1) + (matrix[p[i]]);
            if ((!(fingerprint & chunkMask))) { //AVERAGE*2, *4, *8
                return i;
            }
            i++;
        }
        while (i < n) {
            fingerprint = (fingerprint << 1) + (matrix[p[i]]);
            if ((!(fingerprint & chunkMask2))) { //Average/2, /4, /8
                return i;
            }
            i++;
        }
        return i;
    }


    void chunkingWorkerCallbackFixed() {
        mh_sha1_ctx ctx;
        //SHA_CTX ctx;
        uint64_t posPtr = 0;
        uint64_t base = 0;
        uint64_t fp = 0;

        uint64_t rabinMask = 8191;
        uint64_t counter = 0;
        uint8_t *data = nullptr;
        DedupTask dedupTask;
        CountdownLatch *cd;
        bool newFileFlag = true;
        std::list<DedupTask> saveList;
        ChunkTask chunkTask;
        uint64_t s = 0, e = 0, cs = 0, n = 0;
        bool flag = false;
#ifdef DEBUG
        struct timeval t1, t0;
        struct timeval lt0, lt1;
        struct timeval ct0, ct1, ct2, ct3, ct4, ct5, ct6;
#endif
        while (runningFlag) {
            {
                MutexLockGuard mutexLockGuard(mutexLock);
                while (!taskAmount) {
                    condition.wait();
                    if (!runningFlag) break;
                }
                if (!runningFlag) continue;
                taskAmount--;
                chunkTask = taskList.front();
                taskList.pop_front();
            }

            gettimeofday(&t0, NULL);

            if (newFileFlag) {
                posPtr = 0;
                base = 0;
                fp = 0;
                data = chunkTask.buffer;
                newFileFlag = false;
                flag = false;
            }
            uint64_t end = chunkTask.end;

            dedupTask.buffer = chunkTask.buffer;
            dedupTask.length = chunkTask.length;
            dedupTask.fileID = chunkTask.fileID;

            if (!chunkTask.countdownLatch) {
                while(end - posPtr > MaxChunkSize){
                    int chunkSize = fix_chunk_data(data+posPtr, end-posPtr);
                    dedupTask.pos = base;
                    dedupTask.length = chunkSize;
                    dedupTask.index++;
                    GlobalHashingPipelinePtr->addTask(dedupTask);
                    base += chunkSize;
                    posPtr += chunkSize;
                }
            } else {
                while(end != posPtr){
                    int chunkSize = fix_chunk_data_end(data+posPtr, end-posPtr);
                    dedupTask.pos = base;
                    dedupTask.length = chunkSize;
                    dedupTask.index++;
                    if(end == posPtr + chunkSize){
                        dedupTask.countdownLatch = chunkTask.countdownLatch;
                        flag = true;
                    }
                    GlobalHashingPipelinePtr->addTask(dedupTask);
                    base += chunkSize;
                    posPtr += chunkSize;
                }
            }
            if(flag){
                chunkTask.countdownLatch->countDown();
                newFileFlag = true;
                dedupTask.countdownLatch = nullptr;
            }


#ifdef DEBUG
            gettimeofday(&t1, NULL);
            duration += (t1.tv_sec - t0.tv_sec) * 1000000 + t1.tv_usec - t0.tv_usec;
            ChunkDuration += (t1.tv_sec - t0.tv_sec) * 1000000 + t1.tv_usec - t0.tv_usec;
#endif
        }
    }

    int fix_chunk_data(unsigned char *p, uint64_t n) {
        return FLAGS_ExpectSize;
    }

    int fix_chunk_data_end(unsigned char *p, uint64_t n) {
        if(n < FLAGS_ExpectSize){
            return n;
        }else {
            return FLAGS_ExpectSize;
        }
    }

    RollHash *rollHash;
    Rabin rollHashRabin;
    std::thread *worker;
    std::list<ChunkTask> taskList;
    std::list<ChunkTask> receiveList;
    int taskAmount;
    bool runningFlag;
    MutexLock mutexLock;
    Condition condition;
    uint64_t *matrix;
    uint64_t duration = 0;
    uint64_t sha1 = 0;
    uint64_t lock = 0;
    uint64_t inLoop = 0;
    uint64_t lt;
    uint64_t order = 0;
    uint64_t chunkMask;
    uint64_t chunkMask2;
    uint64_t g_condition_mask[12] = {
            0x00001803110,// 64B
            0x000018035100,// 128B
            0x00001800035300,// 256B
            0x000019000353000,// 512B
            0x0000590003530000,// 1KB
            0x0000d90003530000,// 2KB
            0x0000d90103530000,// 4KB
            0x0000d90303530000,// 8KB
            0x0000d90313530000,// 16KB
            0x0000d90f03530000,// 32KB
            0x0000d90303537000,// 64KB
            0x0000d90703537000// 128KB
    };

    int MaxChunkSize;
    int MinChunkSize;
};

static ChunkingPipeline *GlobalChunkingPipelinePtr;

#endif //ODESSNEW_CHUNKINGPIPELINE_H
