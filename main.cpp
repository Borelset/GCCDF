#include <iostream>

#include "Pipeline/ReadFilePipeline.h"
#include "Pipeline/RestorePipleline.h"
#include "gflags/gflags.h"
#include <fstream>
#include <random>
#include <vector>
#include <algorithm>
#include <chrono>
#include <iomanip>
#include "Pipeline/GarbageCollection.h"
#include <sys/time.h>

DEFINE_string(Path, "", "storage path");
DEFINE_string(RestorePath, "", "restore path");
DEFINE_string(task, "", "task type");
DEFINE_string(BatchFilePath, "", "batch process file path");
DEFINE_int32(RecipeID, 0, "recipe id");
DEFINE_int32(DeletePercentage, 30, "deletion percentage");
DEFINE_int32(GCInterval, 4, "frequency of garbage collection");
DEFINE_int32(GCThreshold, 0, "gc percentage");
DEFINE_bool(GrayEncoding, false, "whether gray encoding is enabled");
DEFINE_bool(Random, false, "whether classification is random in ABT");
DEFINE_int32(SplitThreshold, ContainerSize, "ABT tree split threshold");

extern uint64_t ChunkDuration;
extern uint64_t DedupDuration;
extern uint64_t HashDuration;
extern uint64_t ReadDuration;
extern uint64_t WriteDuration;

#define MemoryMetadata

const uint64_t DEBUG_ID = 1129577;
const auto global_timestamp = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());

int roll(int versions, std::mt19937& rd) {
    std::uniform_int_distribution<int> distrib(0, versions * (versions + 1) / 2 - 1);
    int number = distrib(rd);
    int ret = -1;
    int delta = versions;
    while (number >= 0) {
        ++ret;
        number -= delta--;
    }
    return ret;
}

int pick(int version, std::set<int> versions) {
    auto itr = versions.begin();
    for (int i = 0; i < version; ++i)
        ++itr;
    return *itr;
}

void print_elapsed() {
    uint64_t elapsed = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now()) - global_timestamp;
    std::cout << "[" << std::setw(2) << std::setfill('0') << elapsed / 3'600 << ":" << std::setw(2) << std::setfill('0') << elapsed / 60 % 60 << ":" << std::setw(2) << std::setfill('0') << elapsed % 60 << "]" << std::setw(0) << std::setfill(' ');
}

std::string getFilename(const std::string &path) {
    std::size_t pos = path.find_last_of("/\\");
    return path.substr(pos + 1);
}

int countFileLines(const std::string& filePath) {
    std::ifstream infile(filePath);
    if (!infile) {
        std::cerr << "Failed to open file: " << filePath << std::endl;
        return -1;
    }

    // Use istream_iterator and std::distance to count lines
    std::istream_iterator<std::string> begin(infile);
    std::istream_iterator<std::string> end;
    int lineCount = std::distance(begin, end);

    infile.close();
    return lineCount;
}

void restore(int restoreID, const std::string &restorePath) {
    GlobalRestorePipelinePtr = new RestorePipeline();
    CountdownLatch countdownLatch(1);
    RestoreTask restoreTask = {
            restoreID,
            restorePath,
            &countdownLatch,
    };
    GlobalRestorePipelinePtr->runTask(&restoreTask);
    countdownLatch.wait();
    delete GlobalRestorePipelinePtr;
}

void show_key_params() {
    std::cout << "********************************* " << std::endl;
    std::cout << "             params               " << std::endl;
    std::cout << "********************************* " << std::endl;
    std::cout << "" << std::endl;

    std::cout << "ChunkFilePath = " << FLAGS_ChunkFilePath << std::endl;
    std::cout << "LogicFilePath = " << FLAGS_LogicFilePath << std::endl;
    std::cout << "RestorePath = " << FLAGS_RestorePath << std::endl;
    std::cout << "LCbasedGC = " << FLAGS_LCbasedGC << std::endl;
    std::cout << "enable_rewriting = " << FLAGS_enable_rewriting << std::endl;
    std::cout << "CappingThreshold = " << FLAGS_CappingThreshold << std::endl;
    std::cout << "GCInterval = " << FLAGS_GCInterval << std::endl;
    std::cout << "DeletePercentage = " << FLAGS_DeletePercentage << std::endl;
    std::cout << "CachingLimit = " << FLAGS_CachingLimit << std::endl;
    std::cout << "" << std::endl;
    std::cout << "********************************* " << std::endl;
}

int main(int argc, char **argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    const uint64_t SEED = 0x2f5a;
    const uint64_t SEED2 = 0x56acdf9e;
    std::string putStr("put");
    std::string getStr("get");
    std::string batchStr("batch");
    std::string statusStr("status");

    std::mt19937 mt(SEED);

    show_key_params();

    std::set<int> deleted_version_set;
    std::set<int> global_versions;

    const uint64_t GC_INTERVAL = FLAGS_GCInterval;
    const uint64_t DELETE_VERSIONS = GC_INTERVAL * FLAGS_DeletePercentage / 100;

    uint64_t deletedSize = 0;

    auto exec_del = [&mt, &deleted_version_set, &global_versions, &deletedSize, DELETE_VERSIONS](int deleteID, int v_end) {
        int deleted = deleteID;
        deleted_version_set.insert(deleted);
        global_versions.erase(deleted);
        GlobalMetadataManagerPtr->deleteBackups(deleted);
        deletedSize += GlobalMetadataManagerPtr->lookupBackupSize(deleted);
        printf("deleted Backup#%d\n", deleted);
    };

    auto exec_gc = [&deleted_version_set, &deletedSize](int v_start, int v_end) {
        GarbageCollection garbageCollection;
        garbageCollection.run(deleted_version_set, FLAGS_GCThreshold, v_start, v_end);
        deletedSize = 0;
        deleted_version_set.clear();
    };

    if (FLAGS_task == putStr) {
        struct timeval t0, t1;
        gettimeofday(&t0, NULL);

        StorageTask storageTask;
        CountdownLatch countdownLatch(4);
        storageTask.path = FLAGS_Path;
        storageTask.countdownLatch = &countdownLatch;
        storageTask.fileID = 123123123;
        GlobalReadPipelinePtr->addTask(&storageTask);
        countdownLatch.wait();

        gettimeofday(&t1, NULL);
        float totalDuration = (float) (t1.tv_sec - t0.tv_sec) + (float) (t1.tv_usec - t0.tv_usec) / 1000000;
        printf("Total duration:%fs, Speed:%fMB/s\n", totalDuration,
               (float) storageTask.length / totalDuration / 1024 / 1024);
        printf("read duration:%luus, chunk duration:%luus, dedup duration:%luus, write duration:%lu\nus",
               storageTask.stageTimePoint.read, storageTask.stageTimePoint.chunk, storageTask.stageTimePoint.dedup,
               storageTask.stageTimePoint.write);

    } else if (FLAGS_task == getStr) {
        restore(FLAGS_RecipeID, FLAGS_RestorePath);
    } else if (FLAGS_task == statusStr) {
        printf("Not implemented\n");
    } else if (FLAGS_task == batchStr) {
        GlobalReadPipelinePtr = new ReadFilePipeline();
        GlobalChunkingPipelinePtr = new ChunkingPipeline();
        GlobalHashingPipelinePtr = new HashingPipeline();
        GlobalDeduplicationPipelinePtr = new DeduplicationPipeline();
        GlobalWriteFilePipelinePtr = new WriteFilePipeline();
#ifdef MemoryMetadata
        GlobalMetadataManagerPtr = new MetadataManager(FLAGS_GrayEncoding, FLAGS_SplitThreshold);
#else
        GlobalMetadataManagerPersistPtr = new MetadataManagerPersist();
#endif
        GlobalDeduplicationPipelinePtr->setMaxChunkSize(FLAGS_ExpectSize * 8 * 1.2);

        uint64_t totalSize = 0;
        std::ifstream infile;
        infile.open(FLAGS_BatchFilePath);
        std::string subPath;
        uint64_t counter = 0;
        std::cout << "Batch path: " << FLAGS_BatchFilePath << std::endl;
        int lineOfFiles = countFileLines(FLAGS_BatchFilePath);
        GlobalDeduplicationPipelinePtr->har_manager.init();

        if (GC_INTERVAL % MIX_GROUP_SIZE != 0) {
            std::cout << "GC interval should be an integer times of the mix group size!" << std::endl;
            return -1;
        }

        while (std::getline(infile, subPath)) {
            printf("----------------------------------------------\n");
            // print_elapsed();
            std::cout << "Task: " << subPath << std::endl;
            struct timeval t0, t1;
            gettimeofday(&t0, NULL);

            StorageTask storageTask;
            CountdownLatch countdownLatch(5);
            storageTask.path = subPath;
            storageTask.countdownLatch = &countdownLatch;
            storageTask.fileID = counter;
            storageTask.end_of_mix_group = counter + 1 == lineOfFiles || (counter + 1) % MIX_GROUP_SIZE == 0;
            GlobalReadPipelinePtr->addTask(&storageTask);
            GlobalMetadataManagerPtr->addBackups(counter);
            countdownLatch.wait();

            /***********************************************/

            gettimeofday(&t1, NULL);
            float totalDuration = (float) (t1.tv_sec - t0.tv_sec) + (float) (t1.tv_usec - t0.tv_usec) / 1000000;
            printf("Task duration:%fs, Task Size:%lu, Speed:%fMB/s\n", totalDuration, storageTask.length,
                   (float) storageTask.length / totalDuration / 1024 / 1024);
            printf("read duration:%luus, chunk duration:%luus, dedup duration:%luus, write duration:%luus\n",
                   storageTask.stageTimePoint.read, storageTask.stageTimePoint.chunk, storageTask.stageTimePoint.dedup,
                   storageTask.stageTimePoint.write);
            GlobalReadPipelinePtr->getStatistics();
            GlobalChunkingPipelinePtr->getStatistics();
            GlobalDeduplicationPipelinePtr->getStatistics();
            totalSize += storageTask.length;
            global_versions.insert(counter);
            std::cout << "Done" << std::endl;
            printf("----------------------------------------------\n");
            /***********************************************/

            ++counter;

            if (counter >= 100 && (counter % GC_INTERVAL == 0)) {
                auto avb = GlobalMetadataManagerPtr->getBackups();
                int delCounter = 0;
                for(auto item: avb){
                    if(delCounter % 5 == 0){
                        exec_del(item, counter);
                    }
                    delCounter++;
                }
                // print_elapsed();
                std::cout << "GC start" << std::endl;
                exec_gc(counter + 1 - GC_INTERVAL, counter);
                // print_elapsed();
                std::cout << "GC end" << std::endl;
            }

        }
        {
            StorageTask storageTask;
            storageTask.path = "";
            storageTask.countdownLatch = nullptr;
            storageTask.fileID = 0;
            storageTask.eoi = true;
            GlobalReadPipelinePtr->addTask(&storageTask);
        }

        printf("----------------------------------------------\n");

//        gettimeofday(&total1, NULL);
//        float duration = (float) (total1.tv_sec - total0.tv_sec) + (float) (total1.tv_usec - total0.tv_usec) / 1000000;

        printf("==============================================\n");
//        printf("Total duration:%fs, Total Size:%lu, Speed:%fMB/s\n", duration, totalSize,
//               (float) totalSize / duration / 1024 / 1024);
        GlobalDeduplicationPipelinePtr->getStatistics();
        // print_elapsed();
        printf("task finish\n");
        printf("==============================================\n");

        delete GlobalReadPipelinePtr;
        delete GlobalChunkingPipelinePtr;
        delete GlobalHashingPipelinePtr;
        delete GlobalDeduplicationPipelinePtr;
        delete GlobalWriteFilePipelinePtr;
        delete GlobalMetadataManagerPtr;
    } else {
        printf("Usage: Odess [run type] [args..]\n");
        printf("Put file: Odess --task=put --Path=[path]\n");
        printf("Get file: Odess --task=get --basePath=[basepath] --RestorePath=[restore path]\n");
        printf("Status: Odess --task=status\n");
        printf("use --ChunkFilePath=[chunk file path] to specify the chunk files storage path, default path is /home/zxy/OdessHome/chunkFiles\n");
        printf("use --LogicFilePath=[logic file path] to specify the logic files storage path, default path is /home/zxy/OdessHome/logicFiles\n");
    }
}
