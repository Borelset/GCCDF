//
// Created by BorelsetR on 2019/8/2.
//



#ifndef ODESSSTORAGE_CHUNKCACHE_H
#define ODESSSTORAGE_CHUNKCACHE_H

#include "MetadataManager.h"
#include "../Utility/Lock.h"
#include <set>

DEFINE_uint64(CacheSize, 128, "number of containers");

extern std::set<uint64_t> fid_created_during_gc;

uint64_t mallocSize = 0, freeSize = 0;

struct ContainerCache{
    std::unordered_map<SHA1FP, std::list<CacheBlock>, TupleHasher, TupleEqualer> chunkTable;
};

struct CachedChunk{
    uint8_t* buffer = nullptr;
    uint64_t type:1;
    uint64_t length:63;

    int set(const CacheBlock& cacheBlock){
        buffer = (uint8_t*)malloc(cacheBlock.length);
        mallocSize += cacheBlock.length;
        memcpy(buffer, cacheBlock.block, cacheBlock.length);
        type = cacheBlock.type;
        length = cacheBlock.length;
        return 0;
    }
// 
    ~CachedChunk(){
        if(buffer) free(buffer);
        freeSize += length;
    }
};

//const uint64_t TotalItemThreshold = 65536;
uint64_t ContainerThreshold = FLAGS_CacheSize;

class ChunkCache {
public:
    ChunkCache() : totalSize(0), write(0), read(0) {
        /*
        for(int i=0; i<TotalItemThreshold; i++){
            uint8_t* tempBuffer = (uint8_t*)malloc(65536);
            assert(tempBuffer != nullptr);
            // CacheBlock cacheBlock = {
                    tempBuffer, 0, 0
            };
            idleList.push_back(cacheBlock);
        }
         */
        ::ContainerThreshold = FLAGS_CacheSize;
    }

    void statistics(){
        printf("total size:%lu\n", totalSize);
        printf("cache write:%lu, cache read:%lu\n", write, read);
    }

    void clear(){
        localRelease(-1);
        printf("random seeks:%lu\n", prefetchingTimes);
        printf("prefetchingSize: %lu\n", prefetchingSize);
        printf("average prefetching time:%f\n", (float) prefetchingDuration / prefetchingTimes);
        printf("malloc size:%lu, free size:%lu\n", mallocSize, freeSize);
        for (const auto &container: containerCache) {
            for (const auto &entry: container.second.chunkTable) {
                for (auto &item: entry.second) {
                    free(item.block);
                    freeSize += item.length;
                }
            }
        }
        containerCache.clear();
        containerPosition.clear();
        curPosition = 0;
        totalSize = 0;
        counter = 0;
        prefetchingSize = 0;
        prefetchingNewContainerSize = 0;
        prefetchingTimes = 0;
        prefetchingDuration = 0;
    }

    ~ChunkCache(){
        statistics();
        clear();
    }

    uint64_t getPreloadSize(){
        return prefetchingSize;
    }

    uint64_t getPreloadNewContainerSize(){
        return prefetchingNewContainerSize;
    }

    void PreprocessRecipe(RecipeUnit *recipeUnit, uint64_t unitsNum) {
        MutexLockGuard optLockGuard(optLock);
        for (int i = 0; i < unitsNum; i++) {
            Location &loc = recipeUnit[i].location;
            containerPosition[loc.fid].insert(i);
        }

        curPosition = 0;
    }

    void PrefetchingContainer(int fid){
        struct timeval t0, t1;
        gettimeofday(&t0, NULL);
        prefetchingTimes++;

        uint8_t* preloadBuffer = (uint8_t*)malloc(ContainerSize*2);
        mallocSize += ContainerSize*2;
        int readSize = -1;
        {
            FileOperator* fileOperatorPtr = ChunkFileManager::get(fid);
            uint64_t cSize = fileOperatorPtr->getSize();
            readSize = fileOperatorPtr->read(preloadBuffer, cSize);
            fileOperatorPtr->releaseBufferedData();
            delete fileOperatorPtr;
            //assert(cSize >= ContainerSize && cSize <= ContainerSize*2);
        }

        prefetchingSize += readSize;
        if (::fid_created_during_gc.find(fid) != ::fid_created_during_gc.end())
            prefetchingNewContainerSize += readSize;

        int preLoadPos = 0;
        BlockHead *headPtr;

        auto iter = containerCache.find(fid);
        assert(iter == containerCache.end());
        ContainerCache tempCache;
        uint64_t tableSize = 0;

        while (preLoadPos < readSize) {
            headPtr = (BlockHead *) (preloadBuffer + preLoadPos);
            assert(preLoadPos + sizeof(BlockHead) < readSize - 1);

            uint8_t *cacheBuffer = (uint8_t *) malloc(headPtr->length - sizeof(BlockHead));
            mallocSize += headPtr->length - sizeof(BlockHead);
            memcpy(cacheBuffer, preloadBuffer + preLoadPos + sizeof(BlockHead), headPtr->length - sizeof(BlockHead));

            if (headPtr->type) {
//                CacheBlock insertItem = {cacheBuffer, headPtr->type, headPtr->length - sizeof(BlockHead),
//                                         headPtr->baseFP};
//                tempCache.chunkTable[headPtr->sha1Fp].push_back(insertItem);
            } else {
                CacheBlock insertItem = {cacheBuffer, headPtr->type, headPtr->length - sizeof(BlockHead)};
                tempCache.chunkTable[headPtr->sha1Fp].push_back(insertItem);
            }

            tableSize += headPtr->length - sizeof(BlockHead);

            preLoadPos += headPtr->length;
        }
        this->containerSize[fid] = tableSize;
        if(preLoadPos != readSize){
            printf("fid: %d preLoadPos: %d readSize: %d\n", fid, preLoadPos, readSize);
            assert(preLoadPos == readSize);
        }
        {
            MutexLockGuard cacheLockGuard(cacheLock);
            containerCache[fid] = tempCache;
            {
                MutexLockGuard optLockGuard(optLock);
                counter ++;
                totalSize += tableSize;
                write += tableSize;
            }
            kick();
        }

        free(preloadBuffer);
        freeSize += ContainerSize*2;
        gettimeofday(&t1, NULL);
        prefetchingDuration += (t1.tv_sec - t0.tv_sec) * 1000000 + t1.tv_usec - t0.tv_usec;
    }

    void addLocalCache(const SHA1FP &sha1Fp, uint8_t *buffer, uint64_t length){
        uint8_t *cacheBuffer = (uint8_t *) malloc(length);
        mallocSize += length;
        memcpy(cacheBuffer, buffer, length);
        CacheBlock insertItem = {
                cacheBuffer, 0, length
        };
        localCache[sha1Fp].push_back(insertItem);
        localSize += length;
        write += length;
    }

    void localRelease(uint64_t fid){
        ContainerCache tempCache;
        tempCache.chunkTable.swap(localCache);

        {
            MutexLockGuard cacheLockGuard(cacheLock);
            containerCache[fid] = tempCache;
            totalSize += localSize;
            {
                MutexLockGuard optLockGuard(optLock);
                counter++;
            }
            kick();
        }
        localCache.clear();
        localSize = 0;
    }

    int getLocalCache(const SHA1FP &sha1Fp, CachedChunk *cachedChunk, SHA1FP *baseFP) {
        if (baseFP == nullptr) {
            auto iter = localCache.find(sha1Fp);
            if (iter != localCache.end()) {
                for (auto &item: iter->second) {
                    if (item.type == 0) {
                        cachedChunk->set(item);
                        read += cachedChunk->length;
                        return 1;
                    }
                }
            } else {
                return 0;
            }
        } else {
            auto iter = localCache.find(sha1Fp);
            if (iter != localCache.end()) {
                for (auto &item: iter->second) {
                    if (item.baseFP == *baseFP) {
                        cachedChunk->set(item);
                        read += cachedChunk->length;
                        return 1;
                    }
                }
            } else {
                return 0;
            }
        }

    }

    int tryGetLocalCache(const SHA1FP &sha1Fp) {
        auto iter = localCache.find(sha1Fp);
        if (iter != localCache.end()) {
            for (auto &item: iter->second) {
                if (item.type == 0) {
                    return 1;
                }
            }
        } else {
            return 0;
        }
    }

    uint64_t getNextPos(uint64_t cid) {
        auto iterPos = containerPosition[cid].lower_bound(curPosition);
        if (iterPos == containerPosition[cid].end()) {
            return UINT64_MAX;
        } else {
            return *iterPos;
        }
    }

    void kick() {
        MutexLockGuard optLockGuard(optLock);
        while (counter > ContainerThreshold) {
            uint64_t farthestCid = 0;
            uint64_t farthestPos = 0;

            for (auto &entry : containerCache) {
                uint64_t nextPos = getNextPos(entry.first);
                if (nextPos >= farthestPos) {
                    farthestPos = nextPos;
                    farthestCid = entry.first;
                }
            }

            auto iterCache = containerCache.find(farthestCid);
            assert(iterCache != containerCache.end());
            for (auto &entry: iterCache->second.chunkTable) {
                for (auto &item: entry.second) {
                    free(item.block);
                    freeSize += item.length;
                    totalSize -= item.length;
                }
            }
            containerCache.erase(iterCache);
            counter--;
        }
    }

    int getRecord(uint64_t cid, const SHA1FP &sha1Fp, CachedChunk *cachedChunk, SHA1FP *baseFP) {
        {
            MutexLockGuard cacheLockGuard(cacheLock);
            auto iterCon = containerCache.find(cid);
            if (iterCon == containerCache.end()) {
                return 0;
            }
            std::unordered_map<SHA1FP, std::list<CacheBlock>, TupleHasher, TupleEqualer> &table = iterCon->second.chunkTable;
            auto iterCache = table.find(sha1Fp);
            assert(iterCache != table.end());
            if (baseFP == nullptr) {
                for (auto &item: iterCache->second) {
                    if (item.type == 0) {
                        cachedChunk->set(item);
                        read += cachedChunk->length;
                        {
                            MutexLockGuard optLockGuard(optLock);
                            curPosition ++;
                        }
                        return 1;
                    }
                }
            } else {
                for (auto &item: iterCache->second) {
                    if (item.baseFP == *baseFP) {
                        cachedChunk->set(item);
                        read += cachedChunk->length;
                        {
                            MutexLockGuard optLockGuard(optLock);
                            curPosition ++;
                        }
                        return 1;
                    }
                }
            }
            assert(0);
        }
    }

    int tryGetRecord(uint64_t cid, const SHA1FP &sha1Fp) {
        {
            MutexLockGuard cacheLockGuard(cacheLock);
            auto iterCon = containerCache.find(cid);
            if (iterCon == containerCache.end()) {
                return 0;
            }
            std::unordered_map<SHA1FP, std::list<CacheBlock>, TupleHasher, TupleEqualer> &table = iterCon->second.chunkTable;
            auto iterCache = table.find(sha1Fp);
            assert(iterCache != table.end());
            for (auto &item: iterCache->second) {
                if (item.type == 0) {
                {
                    MutexLockGuard optLockGuard(optLock);
                    curPosition ++;
                }
                    return 1;
                }
            }
            return 0;

        }
    }

    uint64_t containerDataSize(uint64_t cid) const {
        return this->containerSize.at(cid);
    }


private:
    uint64_t totalSize;
    uint64_t counter = 0;
    std::unordered_map<uint64_t, ContainerCache> containerCache;
    std::unordered_map<uint64_t, uint64_t> containerSize;
    std::unordered_map<SHA1FP, std::list<CacheBlock>, TupleHasher, TupleEqualer> localCache;
    std::unordered_map<uint64_t, std::set<uint64_t>> containerPosition;
    uint64_t curPosition;
    MutexLock cacheLock;
    MutexLock optLock;
    uint64_t write, read;

    uint64_t prefetchingSize = 0;
    uint64_t prefetchingNewContainerSize = 0;
    uint64_t prefetchingTimes = 0;
    uint64_t prefetchingDuration = 0;

    uint64_t localSize = 0;
};

#endif //ODESSSTORAGE_CHUNKCACHE_H