//
// Created by BorelsetR on 2019/7/31.
//

#ifndef ODESSNEW_RESTOREPIPLELINE_H
#define ODESSNEW_RESTOREPIPLELINE_H

#include "../MetadataManager/MetadataManager.h"
#include "../MetadataManager/ChunkCache.h"

DEFINE_string(RestoreMethod, "Lifecycle", "restore method in restore");

extern std::string LogicFilePath;
extern std::string ChunkFilePath;

extern const int ContainerSize;
extern std::set<uint64_t> fid_created_during_gc;


class FileFlusher{
public:
    FileFlusher(FileOperator* f): runningFlag(true), taskAmount(0), mutexLock(), condition(mutexLock), fileOperator(f){
        worker = new std::thread(std::bind(&FileFlusher::fileFlusherCallback, this));
    }

    int addTask(uint64_t task) {
        MutexLockGuard mutexLockGuard(mutexLock);
        taskList.push_back(task);
        taskAmount++;
        condition.notify();
        return 0;
    }

    ~FileFlusher(){
        addTask(-1);
        worker->join();
    }


private:

    void fileFlusherCallback(){
        uint64_t task;
        while (likely(runningFlag)) {
            {
                MutexLockGuard mutexLockGuard(mutexLock);
                while (!taskAmount) {
                    condition.wait();
                    if (unlikely(!runningFlag)) break;
                }
                if (unlikely(!runningFlag)) continue;
                taskAmount--;
                task = taskList.front();
                taskList.pop_front();
            }

            if(task == -1){
                break;
            }

            fileOperator->fdatasync();
        }
    }

    std::thread* worker;
    bool runningFlag;
    uint64_t taskAmount;
    std::list<uint64_t> taskList;
    MutexLock mutexLock;
    Condition condition;
    FileOperator* fileOperator;
};

struct ContainerBuffer{
    uint8_t* containerBuffer;
    uint64_t length;
};

class Writer{
public:
    Writer(FileOperator* f, std::unordered_map<SHA1FP, std::unordered_set<uint64_t>, TupleHasher, TupleEqualer>* ot): runningFlag(true), taskAmount(0), mutexLock(), condition(mutexLock), fileOperator(f){
        fileOperator = f;
        offsetTable = ot;
        worker = new std::thread(std::bind(&Writer::writerCallback, this));
    }

    int addTask(ContainerBuffer task) {
        MutexLockGuard mutexLockGuard(mutexLock);
        taskList.push_back(task);
        taskAmount++;
        condition.notify();
        return 0;
    }

    ~Writer(){
        addTask({nullptr, 0});
        worker->join();
    }


private:

    void writerCallback(){
        ContainerBuffer task;
        FileFlusher fileFlusher(fileOperator);
        while (likely(runningFlag)) {
            {
                MutexLockGuard mutexLockGuard(mutexLock);
                while (!taskAmount) {
                    condition.wait();
                    if (unlikely(!runningFlag)) break;
                }
                if (unlikely(!runningFlag)) continue;
                taskAmount--;
                task = taskList.front();
                taskList.pop_front();
            }

            if(task.containerBuffer == nullptr){
                break;
            }

            uint64_t pos = 0;
            BlockHead *headPtr = nullptr;

            while (pos < task.length) {
                headPtr = (BlockHead *) (task.containerBuffer + pos);
                assert(pos + sizeof(BlockHead) < task.length - 1);

                auto it = offsetTable->find(headPtr->sha1Fp);
                if (it != offsetTable->end()) {
                    uint64_t length = headPtr->length - sizeof(BlockHead);
                    for (const auto& offset : it->second) {
                        fileOperator->seek(offset);
                        fileOperator->write(task.containerBuffer + pos + sizeof(BlockHead), length);
                    }
                    offsetTable->erase(it);
                }
                pos += headPtr->length;
            }
            fileFlusher.addTask(1);
            free(task.containerBuffer);
        }
    }

    std::unordered_map<SHA1FP, std::unordered_set<uint64_t>, TupleHasher, TupleEqualer>* offsetTable;
    std::thread* worker;
    bool runningFlag;
    uint64_t taskAmount;
    std::list<ContainerBuffer> taskList;
    MutexLock mutexLock;
    Condition condition;
    FileOperator* fileOperator;
};

struct RestoreWrite{
    uint8_t* buffer = nullptr;
    uint64_t type:1;
    uint64_t endFlag:1;
    uint64_t length:63;
    uint64_t originLength;
    SHA1FP FP;
    SHA1FP baseFP;
    Location location;

    RestoreWrite(uint8_t* buf, uint64_t t, uint64_t l, uint64_t ol, const SHA1FP& fp, const SHA1FP& bfp, const Location& loc){
        buffer = (uint8_t*)malloc(l);
        memcpy(buffer, buf, l);
        originLength = ol;
        type = t;
        length = l;
        baseFP = bfp;
        location = loc;
        FP = fp;
        endFlag = 0;
    }

    RestoreWrite(uint64_t ef){
        endFlag = ef;
    }

    ~RestoreWrite(){
        if(buffer){
            free(buffer);
        }
    }
};

class RestorePipeline {
public:
    RestorePipeline() : taskAmount(0), runningFlag(true), mutexLock(),
                                          condition(mutexLock) {

    }

    int addTask(RestoreWrite *restoreWrite) {
        MutexLockGuard mutexLockGuard(mutexLock);
        taskList.push_back(restoreWrite);
        taskAmount++;
        condition.notify();
        return 0;
    }

    int runTask(RestoreTask *r){
        if (FLAGS_RestoreMethod == std::string("Sequence")) {
            workerRead = new std::thread(std::bind(&RestorePipeline::restoreCallbackSequence, this, r->recipeID));
        } else if (FLAGS_RestoreMethod == std::string("Lifecycle")) {
            workerRead = new std::thread(std::bind(&RestorePipeline::restoreCallbackLifecycle, this, r->recipeID));
        }
//        workerWrite = new std::thread(std::bind(&RestorePipeline::writeFileCallback, this));
        restoreTask = r;
        return 0;
    }

    ~RestorePipeline() {
        // todo worker destruction
//        runningFlag = false;
//        condition.notifyAll();
//        workerWrite->join();
        workerRead->join();
    }

private:
    void restoreCallbackLifecycle(uint64_t recipeID){
        char recipePath[256];
        sprintf(recipePath, FLAGS_LogicFilePath.c_str(), recipeID);
        FileOperator recipeFO(recipePath, FileOpenType::Read);
        uint64_t recipeSize = recipeFO.getSize();
        uint8_t *recipeBuffer = (uint8_t *) malloc(recipeSize);
        recipeFO.read(recipeBuffer, recipeSize);

        FileOperator restoreFile((char *) restoreTask->outputPath.c_str(), FileOpenType::ReadWrite);
        uint64_t size = 0, total_size = 0, total_size_newContainer = 0, size_new_container = 0;
        uint64_t container_read_count = 0, newContainer_read_count = 0;
        uint64_t total_container_read_count = 0, total_newContainer_read_count = 0;

        uint64_t count = recipeSize / (sizeof(WriteHead) + sizeof(Location));
        printf("%lu chunks\n", count);

        /* get information from recipes first */
        RecipeUnit *recipeUnit = (RecipeUnit *) recipeBuffer;
        uint64_t offset = 0;

        struct timeval t0, t1;
        gettimeofday(&t0, NULL);

        std::unordered_set<uint64_t> usedContainers;
        std::unordered_set<uint64_t> usedNewContainers;

        std::set<uint64_t> containerList;
        std::unordered_map<SHA1FP, std::unordered_set<uint64_t>, TupleHasher, TupleEqualer> chunkOffsetTable;

        uint64_t gc_new_visited_chunk_total_size = 0, gc_new_container_total_size = 0;

        for (int i = 0; i < count; i++) {
            WriteHead &wh = recipeUnit[i].writeHead;
            Location &loc = recipeUnit[i].location;

            containerList.insert(loc.fid);
            chunkOffsetTable[wh.fp].insert(offset);
            offset += loc.length - sizeof(BlockHead);
        }
        size = offset;
        Writer writer(&restoreFile, &chunkOffsetTable);

        free(recipeBuffer);

        /* start to restore */
        for (const auto& cid : containerList) {

            uint8_t* containerBuffer = nullptr;
            uint64_t readSize = -1;
            {
                FileOperator* containerFp = ChunkFileManager::get(cid);
                uint64_t containerSize = containerFp->getSize();
                containerBuffer = (uint8_t *) malloc(containerSize);
                readSize = containerFp->read(containerBuffer, containerSize);
                assert(readSize == containerSize);
                delete containerFp;
            }
            total_size += readSize;

            writer.addTask({containerBuffer, readSize});
        }

        restoreFile.fsync();
        restoreTask->countdownLatch->countDown();
        gettimeofday(&t1, NULL);
        uint64_t total = (t1.tv_sec - t0.tv_sec) * 1000000 + t1.tv_usec - t0.tv_usec;
        printf("restore speed: %f MB/s\n", (float) size / total);

        printf("read amplification: %f\n", (float) total_size / size);
        printf("old container read amplification: %f\n", (float) (total_size - total_size_newContainer) / (size - size_new_container));
        printf("new container read amplification: %f\n", (float) total_size_newContainer / size_new_container);
        
        printf("used containers: %lu\n", usedContainers.size());
        printf("used new containers: %lu\n", usedNewContainers.size());
        printf("new containers ratio: %f\n", (float) (usedNewContainers.size()) / (usedContainers.size()));

        printf("new container utilization: %f\n", (double) gc_new_visited_chunk_total_size / total_size_newContainer);

        assert(usedContainers.size() == container_read_count);
        assert(usedNewContainers.size() == newContainer_read_count);
        printf("average read count: %f\n", (float) total_container_read_count / container_read_count);
        if (container_read_count - newContainer_read_count == 0)    printf("old container average read count: %f\n", (float) 0);
        else    printf("old container average read count: %f\n", (float) (total_container_read_count - total_newContainer_read_count) / (container_read_count - newContainer_read_count));
        printf("new container average read count: %f\n", (float) total_newContainer_read_count / newContainer_read_count);
    }

    void restoreCallbackSequence(uint64_t recipeID) {
        ChunkCache chunkCache;

        char recipePath[256];
        sprintf(recipePath, LogicFilePath.c_str(), recipeID);
        FileOperator recipeFO(recipePath, FileOpenType::Read);
        uint64_t recipeSize = recipeFO.getSize();
        uint8_t *recipeBuffer = (uint8_t *) malloc(recipeSize);
        recipeFO.read(recipeBuffer, recipeSize);

        FileOperator restoreFile((char *) restoreTask->outputPath.c_str(), FileOpenType::ReadWrite);
//        uint8_t* decodeBuffer = (uint8_t*)malloc(65536 * 2);
//        usize_t decodeSize;
        uint64_t size = 0;
        uint64_t size_new_container = 0;

        uint64_t count = recipeSize / (sizeof(WriteHead) + sizeof(Location));
        printf("%lu chunks\n", count);

        RecipeUnit *recipeUnit = (RecipeUnit *) recipeBuffer;
        chunkCache.PreprocessRecipe(recipeUnit, count);

        struct timeval t0, t1;
        gettimeofday(&t0, NULL);

        std::unordered_set<uint64_t> usedContainers;
        std::unordered_set<uint64_t> usedNewContainers;
        std::unordered_map<uint64_t, uint64_t> containerRefCount;

        uint64_t gc_new_visited_chunk_total_size = 0;
        uint64_t gc_new_container_total_size = 0;

        uint64_t container_read_count = 0, newContainer_read_count = 0;
        uint64_t total_container_read_count = 0, total_newContainer_read_count = 0;

        for (int i = 0; i < count; i++) {
            CachedChunk processingChunk;
            WriteHead &wh = recipeUnit[i].writeHead;
            Location &loc = recipeUnit[i].location;
            int r = chunkCache.getRecord(loc.fid, wh.fp, &processingChunk, nullptr);
            if (!r) {
                chunkCache.PrefetchingContainer(loc.fid);

                total_container_read_count++;
                bool is_created_during_gc = (::fid_created_during_gc.find(loc.fid) != ::fid_created_during_gc.end());
                if (is_created_during_gc) {
                    total_newContainer_read_count ++;
                    usedNewContainers.insert(loc.fid);
                }
                if (containerRefCount.find(loc.fid) == containerRefCount.end())
                    containerRefCount[loc.fid] = 1;
                else
                    ++containerRefCount[loc.fid];
                bool is_used_container = (usedContainers.find(loc.fid) != usedContainers.end());
                if (!is_used_container) {
                    container_read_count ++;
                    if (is_created_during_gc) {
                        newContainer_read_count ++;
                    }
                    usedContainers.insert(loc.fid);
                }

                r = chunkCache.getRecord(loc.fid, wh.fp, &processingChunk, nullptr);
                assert(r);
            }
            assert(processingChunk.type == 0);
            restoreFile.write(processingChunk.buffer, processingChunk.length);
            if (::fid_created_during_gc.find(loc.fid) != ::fid_created_during_gc.end() && this->utilizedChunks[loc.fid].find(wh.fp) == this->utilizedChunks[loc.fid].end()) {
                this->utilizedChunks[loc.fid].insert(wh.fp);
                gc_new_visited_chunk_total_size += processingChunk.length;
            }
            size += processingChunk.length;
            if (::fid_created_during_gc.find(loc.fid) != ::fid_created_during_gc.end())
                size_new_container += processingChunk.length;
        }
//        RestoreWrite* restoreWriteEnd = new RestoreWrite(true);
//        addTask(restoreWriteEnd);
        restoreFile.fsync();
//        free(decodeBuffer);
        restoreTask->countdownLatch->countDown();
        gettimeofday(&t1, NULL);
        uint64_t total = (t1.tv_sec - t0.tv_sec) * 1000000 + t1.tv_usec - t0.tv_usec;
        for (const auto& cid : this->utilizedChunks) {
            gc_new_container_total_size += chunkCache.containerDataSize(cid.first);
        }
        printf("restore speed: %f MB/s\n", (float) size / total);

        printf("read amplification: %f\n", (float) chunkCache.getPreloadSize() / size);
        printf("old container read amplification: %f\n", (float) (chunkCache.getPreloadSize() - chunkCache.getPreloadNewContainerSize()) / (size - size_new_container));
        printf("new container read amplification: %f\n", (float) chunkCache.getPreloadNewContainerSize() / size_new_container);
        
        printf("used containers: %lu\n", usedContainers.size());
        printf("used new containers: %lu\n", usedNewContainers.size());
        printf("new containers ratio: %f\n", (float) (usedNewContainers.size()) / (usedContainers.size()));
        
        printf("new container utilization: %f\n", (double) gc_new_visited_chunk_total_size / gc_new_container_total_size);

        assert(usedContainers.size() == container_read_count);
        assert(usedNewContainers.size() == newContainer_read_count);
        printf("average read count: %f\n", (float) total_container_read_count / container_read_count);
        if (container_read_count - newContainer_read_count == 0)    printf("old container average read count: %f\n", (float) 0);
        else    printf("old container average read count: %f\n", (float) (total_container_read_count - total_newContainer_read_count) / (container_read_count - newContainer_read_count));
        printf("new container average read count: %f\n", (float) total_newContainer_read_count / newContainer_read_count);
        std::fstream refcount_out("refcount.log", std::ios::out);
        for (const auto& kv : containerRefCount) {
            refcount_out << kv.first << "," << kv.second << std::endl;
        }
    }



//    void writeFileCallback() {
////        FileOperator lengthLog("length.log", FileOpenType::ReadWrite);
////        uint8_t* logBuffer = (uint8_t*)malloc(1024);
//        struct timeval t0, t1;
//        gettimeofday(&t0, NULL);
//        RestoreWrite *restoreWrite;
//        uint8_t* decodeBuffer = (uint8_t*)malloc(65536 * 2);
//        usize_t decodeSize;
//        uint64_t size = 0;
//
//        FileOperator restoreFile((char*)restoreTask->outputPath.c_str(), FileOpenType::ReadWrite);
//
//        while (runningFlag) {
//            {
//                MutexLockGuard mutexLockGuard(mutexLock);
//                while (!taskAmount) {
//                    condition.wait();
//                    if (!runningFlag) break;
//                }
//                if (!runningFlag) continue;
//                taskAmount--;
//                restoreWrite = taskList.front();
//                taskList.pop_front();
//            }
//            if(restoreWrite->endFlag){
//                delete restoreWrite;
//                break;
//            }else{
//                if(restoreWrite->type){
//                    CachedChunk cachedChunk;
//                    int r = chunkCache.getRecord(restoreWrite->baseFP, &cachedChunk);
//                    if(!r || cachedChunk.type == 1){
//                        PrefetchingCache(restoreWrite->location.bfid);
//                        r = chunkCache.getRecord(restoreWrite->baseFP, &cachedChunk);
//                        assert(cachedChunk.type == 0);
//                        assert(r);
//                    }
//                    r = xd3_decode_memory(restoreWrite->buffer, restoreWrite->length, cachedChunk.buffer, cachedChunk.length, decodeBuffer, &decodeSize, 65536*2, XD3_COMPLEVEL_1);
//                    assert(r == 0);
//                    assert(decodeSize == restoreWrite->originLength);
//                    //chunkCache.addRecord_original_replace(restoreWrite->FP, decodeBuffer, decodeSize);
//                    restoreFile.write(decodeBuffer, decodeSize);
//                    size += decodeSize;
////                    sprintf((char*)logBuffer, "%lu\n", decodeSize);
////                    lengthLog.write(logBuffer, strlen((char*)logBuffer));
//                }else{
//                    restoreFile.write(restoreWrite->buffer, restoreWrite->length);
//                    size += restoreWrite->length;
////                    sprintf((char*)logBuffer, "%lu\n", restoreWrite->length);
////                    lengthLog.write(logBuffer, strlen((char*)logBuffer));
//                }
//            }
//            delete restoreWrite;
//        }
//        restoreFile.fsync();
//        free(decodeBuffer);
//        restoreTask->countdownLatch->countDown();
//        gettimeofday(&t1, NULL);
//        uint64_t total = (t1.tv_sec - t0.tv_sec) * 1000000 + t1.tv_usec - t0.tv_usec;
//        printf("restore speed: %f MB/s\n", (float)size / total);
//        printf("random seeks:%lu\n", prefetchingTimes);
//        printf("read amplification: %f\n", (float)prefetchingSize / size);
//        printf("average prefetching time:%f\n", (float)prefetchingDuration/prefetchingTimes);
//    }

//    void PrefetchingCache(int fid){
//        struct timeval t0, t1;
//         gettimeofday(&t0, NULL);
//        prefetchingTimes++;
//
//        uint8_t* preloadBuffer = (uint8_t*)malloc(ContainerSize*2);
//        int readSize = -1;
//        {
//            FileOperator* fileOperatorPtr = ChunkFileManager::get(fid);
//            uint64_t cSize = fileOperatorPtr->getSize();
//            readSize = fileOperatorPtr->read(preloadBuffer, cSize);
//            fileOperatorPtr->releaseBufferedData();
//            delete fileOperatorPtr;
//        }
//
//        prefetchingSize += readSize;
//        int preLoadPos = 0;
//        BlockHead *headPtr;
//        //assert(readSize>=ContainerSize && readSize<=ContainerSize*2);
//
//        while (preLoadPos < readSize) {
//            headPtr = (BlockHead *) (preloadBuffer + preLoadPos);
//            assert(preLoadPos + sizeof(BlockHead) < readSize-1);
//            chunkCache.addRecord(headPtr->sha1Fp, preloadBuffer + preLoadPos + sizeof(BlockHead),
//                                 headPtr->length - sizeof(BlockHead), headPtr->type);
//            preLoadPos += headPtr->length;
//        }
//        assert(preLoadPos == readSize);
//
//        free(preloadBuffer);
//        gettimeofday(&t1, NULL);
//        prefetchingDuration += (t1.tv_sec - t0.tv_sec) * 1000000 + t1.tv_usec - t0.tv_usec;
//    }

    RestoreTask* restoreTask;
    bool runningFlag;
    std::thread *workerRead;
    std::thread *workerWrite;
    uint64_t taskAmount;
    std::list<RestoreWrite *> taskList;
    std::unordered_map<uint64_t, std::unordered_set<SHA1FP, TupleHasher, TupleEqualer>> utilizedChunks;
    MutexLock mutexLock;
    Condition condition;

    uint64_t prefetchingSize = 0;
    uint64_t prefetchingTimes = 0;
    uint64_t prefetchingDuration = 0;
};

static RestorePipeline *GlobalRestorePipelinePtr;


#endif //ODESSNEW_RESTOREPIPLELINE_H
