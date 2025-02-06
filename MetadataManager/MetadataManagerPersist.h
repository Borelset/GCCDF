//
// Created by BorelsetR on 2019/9/1.
//

#ifndef ODESSSTORAGE_METADATAMANAGERPERSIST_H
#define ODESSSTORAGE_METADATAMANAGERPERSIST_H

#include "../Utility/StorageTask.h"
#include "rocksdb/db.h"
#include "rocksdb/table.h"

DEFINE_string(DBPATH, "/data/OdessHome/DB/", "DB path");

class MetadataManagerPersist {
public:
    MetadataManagerPersist() {
        options.create_if_missing = true;
        rocksdb::Status s = rocksdb::DB::Open(options, FLAGS_DBPATH, &db);
        if (s.ok()) {
            rocksdb::ColumnFamilyHandle *sf1;
            s = db->CreateColumnFamily(rocksdb::ColumnFamilyOptions(), "sf1", &sf1);
            assert(s.ok());

            rocksdb::ColumnFamilyHandle *sf2;
            s = db->CreateColumnFamily(rocksdb::ColumnFamilyOptions(), "sf2", &sf2);
            assert(s.ok());

            rocksdb::ColumnFamilyHandle *sf3;
            s = db->CreateColumnFamily(rocksdb::ColumnFamilyOptions(), "sf3", &sf3);
            assert(s.ok());

            delete sf1;
            delete sf2;
            delete sf3;
            delete db;
        }

        column_families.push_back(rocksdb::ColumnFamilyDescriptor(
                rocksdb::kDefaultColumnFamilyName, rocksdb::ColumnFamilyOptions()));
        column_families.push_back(rocksdb::ColumnFamilyDescriptor(
                "sf1", rocksdb::ColumnFamilyOptions()));
        column_families.push_back(rocksdb::ColumnFamilyDescriptor(
                "sf2", rocksdb::ColumnFamilyOptions()));
        column_families.push_back(rocksdb::ColumnFamilyDescriptor(
                "sf3", rocksdb::ColumnFamilyOptions()));

        s = rocksdb::DB::Open(options, FLAGS_DBPATH, column_families, &handles, &db);
        std::cout << s.ToString() << std::endl;
        assert(s.ok());
    }

    void addRecord(const SHA1FP &sha1Fp, const Location &location, uint64_t sf1, uint64_t sf2, uint64_t sf3) {
        rocksdb::WriteBatch dbWriteBatch;
        const rocksdb::Slice keySlice = rocksdb::Slice((const char *) &sha1Fp, sizeof(SHA1FP));
        dbWriteBatch.Put(handles[0], keySlice, rocksdb::Slice((const char *) &location, sizeof(Location)));
        dbWriteBatch.Put(handles[1], rocksdb::Slice((const char *) &sf1, sizeof(uint64_t)), keySlice);
        dbWriteBatch.Put(handles[2], rocksdb::Slice((const char *) &sf2, sizeof(uint64_t)), keySlice);
        dbWriteBatch.Put(handles[3], rocksdb::Slice((const char *) &sf3, sizeof(uint64_t)), keySlice);
        db->Write(rocksdb::WriteOptions(), &dbWriteBatch);
    }

    void addRecordNotReplace(const SHA1FP &sha1Fp, const Location &location, uint64_t sf1, uint64_t sf2, uint64_t sf3) {

    }

    void addRecordNotFeature(const SHA1FP &sha1Fp, const Location &location, uint64_t sf1, uint64_t sf2, uint64_t sf3) {
        rocksdb::WriteBatch dbWriteBatch;
        const rocksdb::Slice keySlice = rocksdb::Slice((const char *) &sha1Fp, sizeof(SHA1FP));
        dbWriteBatch.Put(handles[0], keySlice, rocksdb::Slice((const char *) &location, sizeof(Location)));
        db->Write(rocksdb::WriteOptions(), &dbWriteBatch);
    }

    int findRecord(const SHA1FP &sha1Fp, Location *location) {
        rocksdb::PinnableSlice result;
        rocksdb::Status s = db->Get(rocksdb::ReadOptions(), handles[0],
                                    rocksdb::Slice((const char *) &sha1Fp, sizeof(SHA1FP)), &result);
        if (s.ok()) {
            memcpy(location, result.data(), sizeof(Location));
            result.Reset();
            return 1;
        } else {
            return 0;
        }
    }

    int findSimilarity(uint64_t sf1, uint64_t sf2, uint64_t sf3, Location *location, SHA1FP *sha1Fp) {
        rocksdb::PinnableSlice shaResult;
        rocksdb::PinnableSlice locResult;
        rocksdb::Status s = db->Get(rocksdb::ReadOptions(), handles[1],
                                    rocksdb::Slice((const char *) &sf1, sizeof(uint64_t)), &shaResult);
        if (s.ok()) {
            memcpy(sha1Fp, shaResult.data(), sizeof(SHA1FP));
            db->Get(rocksdb::ReadOptions(), handles[0], shaResult, &locResult);
            memcpy(location, locResult.data(), sizeof(Location));
            shaResult.Reset();
            locResult.Reset();
            return 1;
        }
        s = db->Get(rocksdb::ReadOptions(), handles[2], rocksdb::Slice((const char *) &sf2, sizeof(uint64_t)),
                    &shaResult);
        if (s.ok()) {
            memcpy(sha1Fp, shaResult.data(), sizeof(SHA1FP));
            db->Get(rocksdb::ReadOptions(), handles[0], shaResult, &locResult);
            memcpy(location, locResult.data(), sizeof(Location));
            shaResult.Reset();
            locResult.Reset();
            return 1;
        }
        s = db->Get(rocksdb::ReadOptions(), handles[3], rocksdb::Slice((const char *) &sf3, sizeof(uint64_t)),
                    &shaResult);
        if (s.ok()) {
            memcpy(sha1Fp, shaResult.data(), sizeof(SHA1FP));
            db->Get(rocksdb::ReadOptions(), handles[0], shaResult, &locResult);
            memcpy(location, locResult.data(), sizeof(Location));
            shaResult.Reset();
            locResult.Reset();
            return 1;
        }
        return 0;
    }


private:
    rocksdb::DB *db;
    rocksdb::Options options;
    std::vector<rocksdb::ColumnFamilyDescriptor> column_families;
    std::vector<rocksdb::ColumnFamilyHandle *> handles;
};

static MetadataManagerPersist *GlobalMetadataManagerPersistPtr;


#endif //ODESSSTORAGE_METADATAMANAGERPERSIST_H
