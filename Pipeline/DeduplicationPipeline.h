//
// Created by BorelsetR on 2019/7/29.
//

#ifndef ODESSNEW_DEDUPLICATIONPIPELINE_H
#define ODESSNEW_DEDUPLICATIONPIPELINE_H


#include "../MetadataManager/MetadataManager.h"
//#include "../MetadataManager/MetadataManagerPersist.h"
#include "WriteFilePipeline.h"
#include "../FeatureMethod/FinesseFeature.h"
#include "../FeatureMethod/NFeature.h"
#include "../FeatureMethod/NFeatureSkip.h"
#include "../FeatureMethod/NFeatureSample.h"
#include "../EncodingMethod/EDelta.h"
#include "../EncodingMethod/EncodingMethod.h"
#include "../EncodingMethod/dDelta/dDelta.h"
//#include "../EncodingMethod/xdelta3.h"
#include "../EncodingMethod/zdlib.h"
#include <assert.h>
#include "../FeatureMethod/FinesseSkip.h"
#include "../Utility/ChunkFileManager.h"
#include "../MetadataManager/ChunkCache.h"
#include "../MetadataManager/HarContainerManager.h"

bool likely(bool input){
    return __builtin_expect(input, 1);
}

bool unlikely(bool input) {
    return __builtin_expect(input, 0);
}

uint64_t DedupDuration = 0;

DEFINE_bool(dedup, true, "");
DEFINE_bool(enable_rewriting, true, "");
DEFINE_uint64(CappingThreshold,
              20, "CappingThreshold");
DEFINE_uint64(SMRThreshold, 20, "SMRThreshold");
DEFINE_string(RewritingMethod, "capping", "Rewriting method used");

const int PreLoadSize = 2 * 1024 * 1024;

enum class DeltaEncoding {
    xDelta,
    zDelta,
};

bool CappingOrder(const std::pair<uint64_t, uint64_t> first, std::pair<uint64_t, uint64_t> second) {
    return first.second > second.second;
}

class DeduplicationPipeline {
    friend class GarbageCollection;
public:
    DeduplicationPipeline()
            : taskAmount(0),
              runningFlag(true),
              mutexLock(),
              condition(mutexLock),
              flowCondition(flowLock),
              totalRatio(0.0),
              count(0) {

        rollHash = new Gear();
        har_manager.init();

        worker = new std::thread(std::bind(&DeduplicationPipeline::deduplicationWorkerCallback, this));
    }

    int addTask(const DedupTask &dedupTask) {
        MutexLockGuard mutexLockGuard(mutexLock);
        receiceList.push_back(dedupTask);
        taskAmount++;
        condition.notifyAll();
        return 0;
    }

    ~DeduplicationPipeline() {

        // todo worker destruction
        delete rollHash;

        runningFlag = false;
        condition.notifyAll();
        worker->join();
    }

    void getStatistics() {
        GlobalMetadataManagerPtr->getSize();
    }

    int setMaxChunkSize(int chunkSize) {
        MaxChunkSize = chunkSize;
        return 0;
    }

    uint64_t getFid() {
        uint64_t wfid = GlobalWriteFilePipelinePtr->getFid();
        assert(wfid == fid);
        return fid++;
    }
    HarContainerManager har_manager;
private:
    void deduplicationWorkerCallback() {
        uint64_t segmentLength = 0;
        uint64_t SegmentThreshold = 20 * 1024 * 1024;
        std::list<DedupTask> detectList;

        while (likely(runningFlag)) {
            {
                MutexLockGuard mutexLockGuard(mutexLock);
                while (!taskAmount) {
                    condition.wait();
                    if (unlikely(!runningFlag)) break;
                }
                if (unlikely(!runningFlag)) continue;
                //printf("get task\n");
                taskAmount = 0;
                condition.notify();
                taskList.swap(receiceList);
            }

            if (FLAGS_RewritingMethod == std::string("smr")) {
                // SMR
                for (const auto &dedupTask : taskList) {
                    detectList.push_back(dedupTask);
                    segmentLength += dedupTask.length;
                    if (segmentLength > SegmentThreshold || dedupTask.countdownLatch) {

                        processingWaitingList(detectList);
                        smr(detectList);
                        doDedup(detectList);

                        segmentLength = 0;
                        detectList.clear();
                    }
                }
            } else if (FLAGS_RewritingMethod == std::string("har")) {
                // HAR
                for (auto &dedupTask : taskList) {
                    detectList.push_back(dedupTask);
                    if (dedupTask.countdownLatch) {
                        processingWaitingList(detectList);
                        doDedup(detectList);
                        har_manager.update();
                        har_manager.init();
                        detectList.clear();
                    }
                }
            } else {
                if (FLAGS_RewritingMethod != std::string("capping"))
                    std::cout << "Invalid rewriting method. The system will use the default method Capping." << std::endl;
                // Capping
                for (const auto &dedupTask : taskList) {
                    detectList.push_back(dedupTask);
                    segmentLength += dedupTask.length;
                    if (segmentLength > SegmentThreshold || dedupTask.countdownLatch) {

                        processingWaitingList(detectList);
                        cappingDedupChunks(detectList);
                        doDedup(detectList);

                        segmentLength = 0;
                        detectList.clear();
                    }
                }
            }

            taskList.clear();
        }

    }

    void processingWaitingList(std::list<DedupTask> &wl) {

        for (auto &entry: wl) {
            Location tempLocation;
            uint8_t *currentTask = entry.buffer + entry.pos;

            int result = 0;
            if (likely(FLAGS_dedup)) {
                result = GlobalMetadataManagerPtr->findRecord(entry.fp, &tempLocation);
            }

            if (result) {
                entry.type = 0;
                entry.cid = tempLocation.fid;
            } else {
                entry.type = 1;
            }
        }
    }

    std::map<uint64_t, uint64_t> baseChunkPositions;
    void cappingDedupChunks(std::list<DedupTask> &wl) {
        baseChunkPositions.clear();
        for (auto &entry: wl) {
            if (entry.type == 0) {
                uint64_t cid = entry.cid;
                auto iter = baseChunkPositions.find(cid);
                if (iter == baseChunkPositions.end()) {
                    baseChunkPositions.insert({cid, 1});
                } else {
                    baseChunkPositions[cid]++;
                }
            }
        }

        if (baseChunkPositions.size() > FLAGS_CappingThreshold) {
            std::list<std::pair<uint64_t, uint64_t>> orderList;
            for (auto &entry : baseChunkPositions) {
                orderList.emplace_back(entry.first, entry.second);
            }
            orderList.sort(CappingOrder);

            for (int i = 0; i < FLAGS_CappingThreshold; i++) {
                auto iter = orderList.begin();
                orderList.erase(iter);
            }

            for (auto &item: orderList) {
                baseChunkPositions[item.first] = 0;
            }

            for (auto &entry: wl) {
                if (entry.type == 0) {
                    uint64_t key = entry.cid;
                    if (baseChunkPositions[key] == 0) {
                        entry.rejectDedup = true;
                    }
                }
            }
        }
    }

    std::set<uint64_t> smr_containers;
    void smr(std::list<DedupTask> &wl) {
        std::map<uint64_t, std::unordered_set<SHA1FP, TupleHasher, TupleEqualer>> chunks_categorized;
        std::set<uint64_t> containers;
        std::set<uint64_t> difference_set;
        uint64_t number_of_unique_chunks = 0;
        smr_containers.clear();
        for (auto &entry: wl) {
            if (entry.type == 0) {
                uint64_t cid = entry.cid;
                chunks_categorized[cid].insert(entry.fp);
                containers.insert(cid);
            }
        }

        int i = 0;
        while (smr_containers.size() <= FLAGS_SMRThreshold) {
            std::set_difference(containers.begin(), containers.end(),
                        smr_containers.begin(), smr_containers.end(),
                        std::inserter(difference_set, difference_set.end()));
            uint64_t max_increase_of_unique_chunks = 0;
            std::optional<uint64_t> container_to_select = std::nullopt;
            for (int container : difference_set) {
                std::unordered_set<SHA1FP, TupleHasher, TupleEqualer> unique_chunks;
                for (const auto& fp : chunks_categorized[container]) {
                    // check for uniqueness of a chunk
                    bool unique = unique_chunks.find(fp) == unique_chunks.end();
                    if (unique)
                        for (uint64_t c : smr_containers)
                            if (chunks_categorized[c].find(fp) != chunks_categorized[c].end()) {
                                unique = false;
                                break;
                            }
                    if (unique)
                        unique_chunks.insert(fp);
                }
                if (unique_chunks.size() > max_increase_of_unique_chunks) {
                    max_increase_of_unique_chunks = unique_chunks.size();
                    container_to_select = container;
                }
            }
            if (container_to_select.has_value()) {
                smr_containers.insert(container_to_select.value());
            } else
                break;
        }
    }

    void doDedup(std::list<DedupTask> &wl) {
        uint8_t *baseBuffer = (uint8_t *) malloc(1024);
        uint64_t baseBufferLength = 1024;

        // static std::vector<uint64_t> chunks_to_add{}, chunks_to_update{};
        // static std::vector<uint64_t> chunks_size{};
        // static std::unordered_set<uint64_t> unique_chunks;

        for (auto &entry: wl) {
            WriteTask writeTask;
            memset(&writeTask, 0, sizeof(WriteTask));
            writeTask.eoi = entry.eoi;
            if (writeTask.eoi) {
                GlobalWriteFilePipelinePtr->addTask(writeTask);
                continue;
            }
            writeTask.fileID = entry.fileID;
            writeTask.stageTimePoint = entry.stageTimePoint;
            writeTask.index = entry.index;
            uint8_t *currentTask = entry.buffer + entry.pos;

            int result = 0;
            if (likely(FLAGS_dedup)) {
                result = GlobalMetadataManagerPtr->findRecord_targetfid(entry.fp, &writeTask.location, fid);
                int result_temp = result;
                if(FLAGS_enable_rewriting){
                    if (result) {
                        if (FLAGS_RewritingMethod == std::string("smr")) {
                            // SMR
                            auto citer = this->smr_containers.find(writeTask.location.fid);
                            if (citer == this->smr_containers.end()) {
                                if(writeTask.location.fid != fid){
                                    result = 0;
                                    denyDedup++;
                                    memset(&writeTask.location, 0, sizeof(Location));
                                }
                            }
                        } else if (FLAGS_RewritingMethod == std::string("har")) {
                            // HAR
                            if(har_manager.isSparse(writeTask.location.fid) && writeTask.location.fid != fid){
                                result = 0;
                                denyDedup++;
                                memset(&writeTask.location, 0, sizeof(Location));
                            }
                        } else {
                            // Capping
                            auto citer = baseChunkPositions.find(writeTask.location.fid);
                            if (citer == baseChunkPositions.end() || citer->second == 0) {
                                if(writeTask.location.fid != fid){
                                    result = 0; // capping reject similar chunks.
                                    denyDedup++;
                                    memset(&writeTask.location, 0, sizeof(Location));
                                }
                            }
                        }
                    }
                    if (!result) {
                        auto iter = ContainerTable.find(entry.fp);
                        if (iter != ContainerTable.end()) {
                            result = 1;
                            writeTask.location.fid = fid;
                        } else if (FLAGS_RewritingMethod == std::string("har") && !result_temp) {
                        }
                    }
                }
            }

            if (result) {
                writeTask.sha1Fp = entry.fp;
                writeTask.type = 0;
                writeTask.bufferLength = entry.length;
                writeTask.buffer = entry.buffer;
                har_manager.addRecord(writeTask.location.fid, entry.length);
                writeTask.id = (GlobalMetadataManagerPtr->findRecordSet(entry.fp))[writeTask.location.fid];

                // GlobalMetadataManagerPtr->addLifecycle(writeTask.location.fid, entry.fileID);
                // if (unique_chunks.find(writeTask.id) == unique_chunks.end())
                //     chunks_to_update.push_back(writeTask.id);

                dedup++;
            } else {

                writeTask.type = 2;
                writeTask.buffer = entry.buffer;
                writeTask.pos = entry.pos;
                writeTask.bufferLength = entry.length;
                writeTask.sha1Fp = entry.fp;
                writeTask.id = count++;
                GlobalMetadataManagerPtr->addRecord(writeTask.sha1Fp, fid, writeTask.id);

                // GlobalMetadataManagerPtr->addLifecycle(fid, entry.fileID);

                currentLength += writeTask.bufferLength + sizeof(BlockHead);

                unique++;

                ContainerTable.insert(writeTask.sha1Fp);

                // chunks_to_add.push_back(writeTask.id);
                // chunks_size.push_back(entry.length);
                // unique_chunks.insert(writeTask.id);
            }

            if (unlikely(entry.countdownLatch)) {
                writeTask.countdownLatch = entry.countdownLatch;
                // struct timeval t0, t1;
                // gettimeofday(&t0, NULL);
                // if (GlobalMetadataManagerPtr->GRAY_ENCODING)
                //     GlobalMetadataManagerPtr->SChunkLifeCycle.ingest_gray(chunks_to_add, chunks_size, chunks_to_update);
                // else
                //     GlobalMetadataManagerPtr->SChunkLifeCycle.ingest_new_version(chunks_to_add, chunks_size, chunks_to_update);
                // gettimeofday(&t1, NULL);
                // uint64_t elapsed = (t1.tv_sec-t0.tv_sec)*1000000 + t1.tv_usec-t0.tv_usec;
                // std::cout << "ABT ingestion elapsed time: " << elapsed << " microsecond(s)" << std::endl;
                // GlobalMetadataManagerPtr->SChunkLifeCycle.get_category_statistics();
                // chunks_to_add.clear();
                // chunks_size.clear();
                // chunks_to_update.clear();
                // unique_chunks.clear();
                printf("unique:%lu, dedup:%lu, denyDedup:%lu\n", unique,
                       dedup, denyDedup);
                printf("dedup duration:%lu\n", duration);
                fid++;
                ContainerTable.clear();
                currentLength = 0;
                duration = 0;
                GlobalWriteFilePipelinePtr->addTask(writeTask);
                printf("dedup done\n");
                entry.countdownLatch->countDown();
            } else {
                if (currentLength >= ContainerSize) {
                    currentLength = 0;
                    fid++;
                    ContainerTable.clear();
                }
                GlobalWriteFilePipelinePtr->addTask(writeTask);
            }
            writeTask.countdownLatch = nullptr;
        }
        free(baseBuffer);
    }

    RollHash *rollHash;
    std::thread *worker;
    std::list<DedupTask> taskList;
    std::list<DedupTask> receiceList;
    int taskAmount;
    bool runningFlag;
    MutexLock mutexLock;
    Condition condition;

    float totalRatio;
    uint64_t count;

    uint64_t unique = 0, dedup = 0;

    MutexLock flowLock;
    Condition flowCondition;

    int MaxChunkSize;

    uint64_t duration = 0;

    uint64_t fid = 0;
    uint64_t currentLength = 0;

    uint64_t denyDedup = 0;

    std::unordered_set<SHA1FP, TupleHasher, TupleEqualer> ContainerTable;
};

static DeduplicationPipeline *GlobalDeduplicationPipelinePtr;

#endif //ODESSNEW_DEDUPLICATIONPIPELINE_H
