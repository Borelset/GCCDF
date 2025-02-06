/*
 * Author   : Xiangyu Zou
 * Date     : 07/27/2021
 * Time     : 15:45
 * Project  : OdessStorage
 This source code is licensed under the GPLv2
 */


#ifndef ODESSSTORAGE_GARBAGECOLLECTION_H
#define ODESSSTORAGE_GARBAGECOLLECTION_H

#include <vector>
#include <algorithm>
#include <iterator>
#include <cstdint>
#include <fstream>
#include <sstream>
#include <bitset>
#include <unordered_map>
#include <list>
#include <deque>
#include <random>
#include <set>
#include "../MetadataManager/LCDistanceTable.h"
#include "../MetadataManager/GCCache.h"

DEFINE_int32(NearThreshold, 2, "threshold");
DEFINE_bool(LCbasedGC, true, "lifecycle-based GC");
DEFINE_uint64(MigrationCacheSize, 64, "number of containers");
DEFINE_string(MigrationStrategy, "cache", "migration policy");
DEFINE_bool(MergingOrder, true, "migration policy");
DEFINE_int32(CachingLimit, 100, "Limit to how many containers can be cached in a sub-migration");

extern std::string LogicFilePath;
extern std::string ChunkFilePath;

extern uint64_t GlobalWorkloadSize;
extern uint64_t GlobalDeduplicatedSize;
extern const int ContainerSize;

std::set<uint64_t> fid_created_during_gc;

struct RecordTableEntry {
    uint64_t type: 1;
    uint64_t cid: 63;
};

struct MigrationInfo {
    uint64_t old_cid;
    uint64_t old_offset;
    uint64_t dst_cid;
    uint64_t dst_offset;
};

struct RTEEqualer {
    bool operator()(const RecordTableEntry &lhs, const RecordTableEntry &rhs) const {
        return lhs.type == rhs.type && lhs.cid == rhs.cid;
    }
};

bool onceSymbol = true;
std::map<uint64_t, uint64_t> fileRecorder;

class GarbageCollection {
public:
    GarbageCollection() = default;
    ~GarbageCollection(){onceSymbol = false;}

    void run(const std::set<int> &deleteBV, uint64_t threshold, int v_start, int v_end) {
        struct timeval t0, t1, tbuild0, tbuild1;
        gettimeofday(&t0, NULL);
        char path[512];
        memset(path, 0, 512);
        // std::set<int> &availBackups = GlobalMetadataManagerPtr->getBackups();
        gettimeofday(&tbuild0, NULL);
        buildRecordTable(deleteBV, v_start, v_end); // collect ownership information in the meantime
        gettimeofday(&tbuild1, NULL);
        totalBuildTime += (tbuild1.tv_sec - tbuild0.tv_sec) * 1000000 + (tbuild1.tv_usec - tbuild0.tv_usec);
        this->gc_cache_load_size = 0;

        this->generatedContainerCount = 0;

        printf("GC v_start: %d, v_end: %d, ID: %lu\n", v_start, v_end, this->DEBUG_ID);

        // identify containers to migrate

        std::stringstream ss;
        std::string file_name;
        ss >> file_name;

        int invalid = 0, processed = 0, skipped = 0;

        int count = 0;

        auto itr = involvedContainerTable.begin();
        while (itr != involvedContainerTable.end()) {
            auto cid = *itr;
            sprintf(path, ChunkFilePath.data(), cid);
            uint64_t size = 0;

            // read chunk file and copy to buffer

            uint8_t *buffer = nullptr;
            {
                struct timeval t5, t6;
                gettimeofday(&t5, NULL);
                // read containers
                FileOperator GCcontainer(path, FileOpenType::Read);
                size = GCcontainer.getSize();
                buffer = (uint8_t *) malloc(size);
                uint64_t readSize = GCcontainer.read(buffer, size);
                assert(readSize == size);
                gettimeofday(&t6, NULL);
                totalReadIO += readSize;
                totalReadTime += (t6.tv_sec - t5.tv_sec) * 1000000 + (t6.tv_usec - t5.tv_usec);
            }

            totalcontainer++;

            int r = checkThreshold(buffer, size, cid, threshold, path);
            switch (r) {
                case 2:
                    processingInvalid(buffer, size, cid, path);
                    if (FLAGS_LCbasedGC)
                        involvedContainerTable.erase(itr++);
                    else
                        ++itr;
                    ++invalid;
                    break;
                case 1:
                    if (FLAGS_LCbasedGC) {
                        int vsize = processingContainer(buffer, size, cid);
                        count += vsize;
                    }
                    else
                        processingContainerSeq(buffer, size, cid, path);
                    ++itr;
                    ++processed;
                    break;
                case 0:
                    //identifyInvalid(buffer, size, cid);
                    involvedContainerTable.erase(itr++);
                    ++skipped;
                    // skip
                    break;
            }
            free(buffer);
            if (FLAGS_LCbasedGC && count >= (FLAGS_CachingLimit * 4 * 1024 * 1024)) {
                printf("GC::cached %d containers\n", count);
                if (!migrateFile) {
                    updateMigrateFile();
                }
                migrateGrouped();
                count = 0;
                printf("GC::one turn\n");
                involvedChunks.clear();
                chunkLocation.clear();
                assert(gc_cache.get_count()==0);
                migratedChunks.clear();
            }
        }
        if(FLAGS_LCbasedGC && count > 0){
            printf("GC::cached %d bytes\n", count);
            if (!migrateFile) {
                updateMigrateFile();
            }
            migrateGrouped();
            count = 0;
            printf("GC::one turn\n");
            involvedChunks.clear();
            chunkLocation.clear();
            assert(gc_cache.get_count()==0);
            migratedChunks.clear();
        }

        printf("Size of unique chunks before GC: %lu\n", GlobalDeduplicatedSize);
        if (FLAGS_LCbasedGC) {
            assert(processed == this->involvedContainerTable.size());
            printf("GC containers -- invalid: %d, to be processed: %d, skipped: %d\n", invalid, processed, skipped);

            for (const auto& cid : this->involvedContainerTable) {
                sprintf(path, ChunkFilePath.data(), cid);
                remove(path);
                GlobalMetadataManagerPtr->removeAvailableContainer(cid);
                if (FLAGS_RewritingMethod == std::string("har")) {
                    GlobalDeduplicationPipelinePtr->har_manager.deleteRecord(cid);
                }
            }
            if (FLAGS_RewritingMethod == std::string("har")) {
                GlobalDeduplicationPipelinePtr->har_manager.update();
            }
            this->involvedContainerTable.clear();
        }

        printf("Containers generated during GC: %lu\n", this->generatedContainerCount);
        printf("bpSkip: %lu, directDelete: %lu\n", bpSkip, directDelete);
        bpSkip = 0;
        directDelete = 0;

        releaseMigrateFile();
        printf("Total Read IO: %lu, Total Write IO: %lu, migrate: %lu, container: %lu\n", totalReadIO, totalWriteIO,
               migrate, totalcontainer);
        printf("GC cache load size: %lu\n", this->gc_cache_load_size);
        printf("GC cache peak count: %lu\n", this->gc_cache.get_peak_count());
        printf("GC cache peak size: %lu\n", this->gc_cache.get_peak_size());
        printf("GC migration size: %lu\n", this->totalMigrationSize);
        printf("update Recipe\n");
        updateRecipe(GlobalMetadataManagerPtr->getBackups());

        // // deal with chunk merging
        // GlobalMetadataManagerPtr->SChunkLifeCycle.transfer_ownership(this->mergedIDs, this->destReference, GlobalMetadataManagerPtr->GRAY_ENCODING);

        // printf("AverageLCBefore: %f, AverageLCAfter: %f\n", (float) LCTotalBefore / CCountBefore,
        //        (float) LCTotalAfter / CCountAfter);
        printf("GC processed: %lu, GC generated: %lu\n", CCountBefore, CCountAfter);
        printf("Size of unique chunks after GC: %lu\n", GlobalDeduplicatedSize);

        //GlobalMetadataManagerPtr->scanForFP(this->DEBUG_SHA1FP);
        puts("--------------------");
        printf("GC v_start: %d, v_end: %d, ID: %lu\n", v_start, v_end, this->DEBUG_ID);
        // GlobalMetadataManagerPtr->traverseFile(1);

        recordTable.clear();
        involvedContainerTable.clear();

        GlobalMetadataManagerPtr->indexExamine();
        gettimeofday(&t1, NULL);
        totalGCTime = (t1.tv_sec - t0.tv_sec) * 1000000 + (t1.tv_usec - t0.tv_usec);
        printf("Total GC time:%lu, GC Speed:%f\n", totalGCTime,
               (float) totalReadIO / totalGCTime);

        printf("read container time:%lu, write container time:%lu, marktime:%lu, tabletime:%lu\n",
                totalReadTime, totalWriteTime, totalBuildTime, totalTableTime);

        printf("----------------------------------------------\n");
        totalGCTime = 0;
        totalReadTime = 0;
        totalWriteTime = 0;
    }

private:
    uint64_t getNextContainer(uint64_t cid) {
        uint64_t nearest = -1;
        uint32_t distance = -1;
        uint64_t simplest = -1;
        uint32_t smallestLC = -1;
        std::set<uint64_t> &lc1 = GlobalMetadataManagerPtr->getLifecycle(cid);
        std::vector<uint64_t> intersection;
        for (auto item: involvedContainerTable) {
            intersection.clear();
            std::set<uint64_t> &lc2 = GlobalMetadataManagerPtr->getLifecycle(item);
            std::set_intersection(lc1.begin(), lc1.end(), lc2.begin(), lc2.end(),
                                  std::insert_iterator<std::vector<uint64_t>>(intersection, intersection.begin()));
            uint32_t d = lc2.size() - intersection.size();
            if (d < distance) {
                distance = d;
                nearest = item;
            }
            if (lc2.size() < smallestLC) {
                smallestLC = lc2.size();
                simplest = item;
            }
        }

        if (distance <= FLAGS_NearThreshold) {
            return nearest;
        } else {
            bpSkip++;
            return simplest;
        }
    }

    int checkThreshold(uint8_t *buffer, uint64_t size, uint64_t cid, uint64_t threshold, char *path) {
        uint64_t pos = 0;
        BlockHead *headPtr = nullptr;
        uint64_t validSize = 0;
        uint64_t counter = 0;
        while (pos < size) {
            headPtr = (BlockHead *) (buffer + pos);
            assert(pos + sizeof(BlockHead) < size);
            counter++;
            auto iter = recordTable.find(headPtr->id);
            if (iter != recordTable.end()) {
                assert(headPtr->type == 0);
                validSize += headPtr->length;
            }
            pos += headPtr->length;
        }
        assert(pos == size);

        if (validSize == 0) return 2;
        else if (validSize * 100 < size * (100 - threshold)) return 1;
        else return 0;
    }

    void processingInvalid(uint8_t *buffer, uint64_t size, uint64_t cid, char *path) {

        uint64_t pos = 0;
        BlockHead *headPtr = nullptr;

        GlobalMetadataManagerPtr->removeAvailableContainer(cid);

        while (pos < size) {
            headPtr = (BlockHead *) (buffer + pos);
            assert(pos + sizeof(BlockHead) < size);
            assert(headPtr->type == 0);
            GlobalMetadataManagerPtr->removeRecord(headPtr->sha1Fp, cid);
            GlobalDeduplicatedSize -= headPtr->length - sizeof(BlockHead);
            pos += headPtr->length;
        }
        assert(pos == size);

        directDelete++;
        remove(path);

        // GlobalMetadataManagerPtr->deleteContainerLC(cid);
    }

    void processingContainerSeq(uint8_t *buffer, uint64_t size, uint64_t cid, char *path) {
        if (!migrateFile) {
            updateMigrateFile();
        }
        CCountBefore++;

        uint64_t pos = 0;
        BlockHead *headPtr = nullptr;
        uint64_t validSize = 0;

        remove(path);
        GlobalMetadataManagerPtr->removeAvailableContainer(cid);
        pos = 0;

        while (pos < size) {
            headPtr = (BlockHead *) (buffer + pos);
            assert(pos + sizeof(BlockHead) < size);
            auto iter = recordTable.find(headPtr->id);
            if (iter != recordTable.end()) {
                assert(headPtr->type == 0);
                migrateChunk(buffer + pos, headPtr->length, headPtr->sha1Fp, cid, headPtr->id);
                validSize += headPtr->length - sizeof(BlockHead);
            }
            else {
            GlobalMetadataManagerPtr->removeRecord(headPtr->sha1Fp, cid);
            GlobalDeduplicatedSize -= headPtr->length - sizeof(BlockHead);
            }
            pos += headPtr->length;
        }
        assert(pos == size);

        totalWriteIO += validSize;
        migrate++;
    }

    int processingContainer(uint8_t *buffer, uint64_t size, uint64_t cid) {
        CCountBefore++;

        uint64_t pos = 0;
        BlockHead *headPtr = nullptr;
        uint64_t validSize = 0;
        Location loc;

        pos = 0;

        while (pos < size) {
            headPtr = (BlockHead *) (buffer + pos);
            assert(pos + sizeof(BlockHead) < size);
            auto iter = recordTable.find(headPtr->id);
            auto fid_set = GlobalMetadataManagerPtr->findRecordSet(headPtr->sha1Fp);
            if (iter != recordTable.end()) {
                assert(headPtr->type == 0);
                ChunkStart chunk_start;
                chunk_start.cid = cid;
                chunk_start.offset = pos;
                auto itr = this->chunkFile.find(headPtr->id);
                assert(itr != this->chunkFile.end());
                for(auto item: itr->second){
                    this->involvedChunks[item].insert(headPtr->id);
                }
                this->chunkLocation[headPtr->id] = chunk_start;
                // migrateChunk(buffer + pos, headPtr->length, headPtr->sha1Fp, cid);
                validSize += headPtr->length - sizeof(BlockHead);
                gc_cache.put(headPtr->id, headPtr->sha1Fp, buffer + pos, headPtr->length, chunk_start);
            }
            else if (fid_set.find(cid) != fid_set.end()) {
                GlobalMetadataManagerPtr->removeRecord(headPtr->sha1Fp, cid);
                GlobalDeduplicatedSize -= headPtr->length - sizeof(BlockHead);
            }
            pos += headPtr->length;
        }
        assert(pos == size);

        totalWriteIO += validSize;
        migrate++;

        return validSize;

        // GlobalMetadataManagerPtr->deleteContainerLC(cid);
    }
    
    void identifyInvalid(uint8_t* buffer, uint64_t size, uint64_t cid) {
        uint64_t pos = 0;
        BlockHead *headPtr = nullptr;
        Location loc;

        while (pos < size) {
            headPtr = (BlockHead *) (buffer + pos);
            assert(pos + sizeof(BlockHead) < size);
            assert(headPtr->type == 0);

            auto fid_set = GlobalMetadataManagerPtr->findRecordSet(headPtr->sha1Fp);
            if (fid_set.find(cid) != fid_set.end()) {
                GlobalMetadataManagerPtr->removeRecord(headPtr->sha1Fp, cid);
                GlobalDeduplicatedSize -= headPtr->length - sizeof(BlockHead);
            }

            pos += headPtr->length;
        }
        assert(pos == size);
    }

    void updateMigrateFile() {
        if (migrateFile) {
            ::fid_created_during_gc.insert(currentCid);
            // write container
            struct timeval t2, t3;
            //migrateFile->fsync();
            //migrateFile->releaseBufferedData();
            GlobalMetadataManagerPtr->addAvailableContainer(currentCid);
            delete migrateFile;
            migrateFile = nullptr;
            GCContainerTable.clear();

            // LCTotalAfter += GlobalMetadataManagerPtr->getLifecycle(currentCid).size();
            CCountAfter++;
        }
        char path[512];
        currentCid = GlobalDeduplicationPipelinePtr->getFid();
        sprintf(path, ChunkFilePath.data(), currentCid);
        migrateFile = new FileOperator(path, FileOpenType::Write);
        assert(migrateFile->getSize() == 0);
        ++this->generatedContainerCount;
        migrateWriteLength = 0;
    }

    void releaseMigrateFile() {
        if (migrateFile) {
            ::fid_created_during_gc.insert(currentCid);
            // write container
            struct timeval t2, t3;
            migrateFile->fsync();
            migrateFile->releaseBufferedData();
            GlobalMetadataManagerPtr->addAvailableContainer(currentCid);
            delete migrateFile;
            migrateFile = nullptr;
            migrateWriteLength = 0;
            currentCid = -1;
            CCountAfter++;
            GCContainerTable.clear();
        }
    }

    inline bool isMigrateFileFull() {
        return migrateWriteLength >= ContainerSize;
    }

    void migrateChunk(uint8_t *buffer, uint64_t length, SHA1FP sha1Fp, uint64_t old_cid, uint64_t id) {
        assert(migrateFile != nullptr);

        if (this->isMigrateFileFull()) {
            updateMigrateFile();
        }
        // std::set<uint64_t> oldLC = GlobalMetadataManagerPtr->getLifecycle(old_cid);
        // assert(oldLC.size() != 0);
        // for (auto item: oldLC) {
        //     GlobalMetadataManagerPtr->addLifecycle(currentCid, item);
        // }

        auto iter = GCContainerTable.find(sha1Fp);
        if (iter != GCContainerTable.end()) {
            GlobalMetadataManagerPtr->insertGCTable(sha1Fp, old_cid, currentCid, id, iter->second);
//            GlobalMetadataManagerPtr->insertGCTable(sha1Fp, old_cid, currentCid, iter->second);
            GlobalMetadataManagerPtr->checkMigrateRecord(sha1Fp, old_cid, currentCid, iter->second);
            // this->mergedIDs.insert(id);
            // this->mergedIDs.insert(iter->second);
            // if (this->destReference.find(iter->second) == this->destReference.end())
            //     this->destReference[iter->second] = std::unordered_set<uint64_t>{};
            GlobalDeduplicatedSize -= length - sizeof(BlockHead);
            return;
        }

        // write container
        struct timeval t2, t3;
        gettimeofday(&t2, NULL);

        uint64_t writesize = migrateFile->write(buffer, length);
        assert(writesize == length);
        migrateWriteLength += length;

        gettimeofday(&t3, NULL);
        totalWriteTime += (t3.tv_sec - t2.tv_sec) * 1000000 + (t3.tv_usec - t2.tv_usec);

        GlobalMetadataManagerPtr->insertGCTable(sha1Fp, old_cid, currentCid, id, id);
        GlobalMetadataManagerPtr->updateRecord(sha1Fp, old_cid, currentCid, id);
        GCContainerTable.insert(std::make_pair(sha1Fp, id));
        this->totalMigrationSize += length - sizeof(BlockHead);
        if (FLAGS_RewritingMethod == std::string("har")) {
            GlobalDeduplicationPipelinePtr->har_manager.addRecord(currentCid, length);
        }
    }

    void migrateGrouped() {
        printf("Migrate Group\n");
        struct timeval ttable0, ttable1, twrite0, twrite1;
        gettimeofday(&ttable0, NULL);
        ScalableChunkLifeCycleNew lc_tree;
        std::vector<uint64_t> chunks_to_add, chunks_to_update;
        printf("%lu files involved in total\n", this->involvedChunks.size());
        if(FLAGS_MergingOrder){
            for (auto kv_iter = this->involvedChunks.rbegin(); kv_iter != this->involvedChunks.rend(); kv_iter++){
                //printf("Scan Chunks in some file..\n");
                if(onceSymbol) fileRecorder[kv_iter->first]++;
                chunks_to_add.clear();
                chunks_to_update.clear();
                for (const auto& chunk_id : kv_iter->second) {
                    if(lc_tree.checkExisting(chunk_id)){
                        chunks_to_update.push_back(chunk_id);
                    }else{
                        chunks_to_add.push_back(chunk_id);
                    }
                }
                //printf("Update tree..\n");
                //printf("add num:%lu, update num:%lu\n", chunks_to_add.size(), chunks_to_update.size());
                lc_tree.ingest_new_version(chunks_to_add, chunks_to_update);
            }
        }
        else{
            for (auto kv_iter = this->involvedChunks.begin(); kv_iter != this->involvedChunks.end(); kv_iter++){
                //printf("Scan Chunks in some file..\n");
                if(onceSymbol) fileRecorder[kv_iter->first]++;
                chunks_to_add.clear();
                chunks_to_update.clear();
                for (const auto& chunk_id : kv_iter->second) {
                    if(lc_tree.checkExisting(chunk_id)){
                        chunks_to_update.push_back(chunk_id);
                    }else{
                        chunks_to_add.push_back(chunk_id);
                    }
                }
                //printf("Update tree..\n");
                //printf("add num:%lu, update num:%lu\n", chunks_to_add.size(), chunks_to_update.size());
                lc_tree.ingest_new_version(chunks_to_add, chunks_to_update);
            }
        }
        if(onceSymbol){
            for(auto item: fileRecorder){
                printf("[fileRecorder] #%lu file: %lu\n", item.first, item.second);
            }
        }
        printf("[LeafNode] num:%d\n", lc_tree.getLeafNodeCount());
        gettimeofday(&ttable1, NULL);
        totalTableTime += (ttable1.tv_sec - ttable0.tv_sec) * 1000000 + (ttable1.tv_usec - ttable0.tv_usec);

        gettimeofday(&twrite0, NULL);
        if(FLAGS_MergingOrder){
            auto tr_ptr = lc_tree.getTail();
            while(tr_ptr != lc_tree.getHead()){
                for(auto item: tr_ptr->getItems()){
                    if(migratedChunks.find(item) != migratedChunks.end()) continue;
                    node_type nd;
                    ChunkStart& temp = this->chunkLocation[item];
                    bool find = this->gc_cache.lookup(temp, nd);
                    this->gc_cache.kick(this->chunkLocation[item]);
                    assert(find);
                    this->migrateChunk(nd.data, nd.sz, nd.sha1fp, nd.cs.cid, nd.id);
                    migratedChunks.insert(item);
                    free(nd.data);
                }
                tr_ptr = tr_ptr->prev;
            }
        }else{
            int n = lc_tree.getLeafNodeCount();
            for(int i=0; i<n; i++){
                auto tr_ptr = lc_tree.getRandom();
                for(auto item: tr_ptr->getItems()){
                    if(migratedChunks.find(item) != migratedChunks.end()) continue;
                    node_type nd;
                    ChunkStart& temp = this->chunkLocation[item];
                    bool find = this->gc_cache.lookup(temp, nd);
                    this->gc_cache.kick(this->chunkLocation[item]);
                    assert(find);
                    this->migrateChunk(nd.data, nd.sz, nd.sha1fp, nd.cs.cid, nd.id);
                    migratedChunks.insert(item);
                    free(nd.data);
                }
            }
        }

        gettimeofday(&twrite1, NULL);
        if(migrateFile != nullptr) migrateFile->fsync();
        totalWriteTime += (twrite1.tv_sec - twrite0.tv_sec) * 1000000 + (twrite1.tv_usec - twrite0.tv_usec);
    }

    void updateRecipe(const std::set<int> &bvs) {
        printf("update %lu recipes\n", bvs.size());
        char path[512];
        for (int i : bvs) {
            printf("processing Backup #%d's recipe\n", i);
            uint8_t *buffer = nullptr;
            uint8_t *buffer_new = nullptr;
            uint64_t recipeSize = 0;
            {
                sprintf(path, LogicFilePath.data(), i);
                FileOperator recipe(path, FileOpenType::Read);
                recipeSize = recipe.getSize();
                buffer = (uint8_t *) malloc(recipeSize);
                buffer_new = (uint8_t *) malloc(recipeSize);
                recipe.read(buffer, recipeSize);
            }
            uint64_t count = recipeSize / (sizeof(WriteHead) + sizeof(Location));
            RecipeUnit *oldRecipeUnit = (RecipeUnit *) buffer;
            RecipeUnit *newRecipeUnit = (RecipeUnit *) buffer_new;

            for (int j = 0; j < count; j++) {
                WriteHead &wh = oldRecipeUnit[j].writeHead;
                Location &loc = oldRecipeUnit[j].location;
                assert(wh.type == 0);
                // if (this->destReference.find(wh.id) != this->destReference.end())
                //     this->destReference[wh.id].insert(i);
                if (!GlobalMetadataManagerPtr->isAvaialableContainer(loc.fid)) {
                    uint64_t id;
                    id = wh.id;
                    auto r = GlobalMetadataManagerPtr->lookupGCTable(wh.id, loc.fid);
                    assert(r.has_value());
                    loc.fid = r.value().first;
                    if (id != wh.id) {
                        // assert(this->destReference.find(wh.id) != this->destReference.end());
                        // this->destReference[wh.id].insert(i);
                    }
                    wh.id = r.value().second;
                }
                memcpy(&newRecipeUnit[j], &oldRecipeUnit[j], sizeof(RecipeUnit));
            }
            remove(path);
            {
                FileOperator newRecipe(path, FileOpenType::Write);
                newRecipe.write(buffer_new, recipeSize);
            }
            free(buffer);
            free(buffer_new);
        }
        GlobalMetadataManagerPtr->clearGCTable();
    }

    // build record table and update obt
    void buildRecordTable(const std::set<int> &dbv, int v_start, int v_end) {
        char path[512];
        std::set<int> &availBackups = GlobalMetadataManagerPtr->getBackups();
        for (int i = 0; i <= v_end; ++i) {
            if (availBackups.find(i) == availBackups.end())
                continue;
            std::vector<uint64_t> chunks_to_add{}, chunks_to_update{};
            std::vector<uint64_t> chunks_size{};
            std::unordered_set<uint64_t> unique_chunks;
            sprintf(path, LogicFilePath.data(), i);
            FileOperator recipe(path, FileOpenType::Read);
            uint64_t size = recipe.getSize();
            uint8_t *buffer = (uint8_t *) malloc(size);
            recipe.read(buffer, size);
            uint64_t count = size / (sizeof(WriteHead) + sizeof(Location));
            RecipeUnit *recipeUnit = (RecipeUnit *) buffer;

            for (int j = 0; j < count; j++) {
                WriteHead &wh = recipeUnit[j].writeHead;
                Location &loc = recipeUnit[j].location;
                assert(wh.type == 0);
                if (dbv.find(i) == dbv.end()) {
                    recordTable.insert(wh.id);
                    this->chunkFile[wh.id].insert(i);
                }
            }

            free(buffer);
        }
        for (int i : dbv) {
            sprintf(path, LogicFilePath.data(), i);
            FileOperator recipe(path, FileOpenType::Read);
            uint64_t size = recipe.getSize();
            uint8_t *buffer = (uint8_t *) malloc(size);
            recipe.read(buffer, size);
            uint64_t count = size / (sizeof(WriteHead) + sizeof(Location));
            RecipeUnit *recipeUnit = (RecipeUnit *) buffer;

            for (int j = 0; j < count; j++) {
                WriteHead &wh = recipeUnit[j].writeHead;
                Location &loc = recipeUnit[j].location;
                assert(wh.type == 0);
                involvedContainerTable.insert(loc.fid);
            }
            free(buffer);
            GlobalWorkloadSize -= GlobalMetadataManagerPtr->lookupBackupSize(i);
        }
    }

    std::unordered_set<uint64_t> recordTable;
    std::set<uint64_t> involvedContainerTable;
    std::unordered_map<uint64_t, std::set<uint64_t>> chunkFile; // Chunk ID -> File ID
    std::map<uint64_t, std::set<uint64_t>> involvedChunks; // FileID -> chunks
    std::unordered_map<uint64_t, ChunkStart> chunkLocation;

    uint64_t totalReadIO = 0;
    uint64_t totalWriteIO = 0;
    uint64_t totalUnmovedSize = 0;
    uint64_t totalMigrationSize = 0;
    uint64_t migrate = 0;
    uint64_t directDelete = 0;
    uint64_t totalcontainer = 0;
    uint64_t totalReadTime = 0;
    uint64_t totalBuildTime = 0;
    uint64_t totalWriteTime = 0;
    uint64_t totalTableTime = 0;
    uint64_t totalGCTime = 0;

    FileOperator *migrateFile = nullptr;
    uint64_t migrateWriteLength = 0;
    uint64_t currentCid = -1;
    uint64_t tempCid = -1;

    gc_hashtable gc_cache;
    uint64_t gc_cache_load_size;

    std::unordered_map<SHA1FP, uint64_t, TupleHasher, TupleEqualer> GCContainerTable; // SHA1FP -> id

    // std::unordered_set<uint64_t> mergedIDs; // old_id & new_id
    // std::unordered_map<uint64_t, std::unordered_set<uint64_t>> destReference; // new_id -> set of file #

    //LCDistanceTable lcDistanceTable;

    // uint64_t LCTotalBefore = 0, LCTotalAfter = 0;
    uint64_t CCountBefore = 0, CCountAfter = 0;

    std::unordered_set<uint64_t> migratedChunks;

    size_t generatedContainerCount;

    uint64_t bpSkip = 0;
    const SHA1FP DEBUG_SHA1FP = {.fp1 = 15844408918433038934llu, .fp2 = 854042722u, .fp3 = 1615005289u, .fp4 = 3183670265u};
    const uint64_t DEBUG_ID = 73842;
    const uint64_t DEBUG_CID = 1436;
};

#endif //ODESSSTORAGE_GARBAGECOLLECTION_H
