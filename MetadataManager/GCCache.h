#ifndef ODESSSTORAGE_GCCACHE_H
#define ODESSSTORAGE_GCCACHE_H

#include <fstream>
#include <cstdint>
#include <list>
#include <unordered_map>
#include <map>
#include <mutex>
#include "MetadataManager.h"
#include "../Utility/FileOperator.h"

using std::mutex;

extern std::string ChunkFilePath;

struct ChunkStart {
    uint64_t cid;
    uint64_t offset;
};

struct ChunkStartHasher {
    std::size_t operator()(const ChunkStart& key) const {
        return (key.cid & ((0L << 32) - 1)) << 32 | (key.offset & ((1L << 32) - 1));
    }
};

struct ChunkStartEqualer {
    bool operator()(const ChunkStart& lhs, const ChunkStart& rhs) const {
        return lhs.cid == rhs.cid && lhs.offset == rhs.offset;
    }
};

struct ChunkStartLess {
    bool operator()(const ChunkStart& lhs, const ChunkStart& rhs) const {
        return lhs.cid < rhs.cid || (lhs.cid == rhs.cid && lhs.offset < rhs.offset);
    }
}; 

// a read-only lru cache

struct node_type {
    node_type() = default;
    ~node_type() = default;
    uint64_t id;
    SHA1FP sha1fp;
    uint8_t* data;
    ChunkStart cs;
    size_t sz;
};
struct list_node {
    list_node() = default;
    ~list_node() = default;
    node_type data;
    list_node* prev;
    list_node* next;
};

class gc_hashtable {
public:
    gc_hashtable() : peak_cnt_(0), cnt_(0), peak_sz_(0), sz_(0) {};
    void put(uint64_t id, SHA1FP sha1fp, const uint8_t* buf, size_t sz, ChunkStart cs) {
        node_type nd;
        nd.id = id;
        nd.sha1fp = sha1fp;
        nd.sz = sz;
        nd.data = (uint8_t*) malloc(sz);
        memcpy(nd.data, buf, sz);
        nd.cs = cs;
        htbl_[cs] = nd;
        update_(nd.sz);
    }
    bool lookup(ChunkStart cs, node_type& nd) {
        if (htbl_.find(cs) == htbl_.end())
            return false;
        nd = htbl_[cs];
        return true;
    }
    void kick(ChunkStart cs) {
        auto iter = htbl_[cs];
        uint64_t sz = iter.sz;
        htbl_.erase(cs);
        update_kick_(sz);
    }
    auto kick_iterator(std::map<const ChunkStart, node_type, ChunkStartLess>::const_iterator& itr) {
        uint64_t sz = itr->second.sz;
        auto ret_itr = htbl_.erase(itr);
        update_kick_(sz);
        return ret_itr;
    }
    inline uint64_t get_count() const {
        return cnt_;
    }
    inline uint64_t get_peak_count() const {
        return peak_cnt_;
    }
    inline uint64_t get_size() const {
        return sz_;
    }
    inline uint64_t get_peak_size() const {
        return peak_sz_;
    }
    ~gc_hashtable() {
        for (const auto& kv : htbl_)
            free(kv.second.data);
    }
    const auto& get_hashtable() const {
        return htbl_;
    }
protected:
    inline void update_(uint64_t sz) {
        ++cnt_;
        sz_ += sz;
        if (cnt_ > peak_cnt_)
            peak_cnt_ = cnt_;
        if (sz_ > peak_sz_)
            peak_sz_ = sz_;
    }
    inline void update_kick_(uint64_t sz) {
        assert(sz_ >= sz);
        --cnt_;
        sz_ -= sz;
    }
    std::map<ChunkStart, node_type, ChunkStartLess> htbl_;
    uint64_t peak_cnt_;
    uint64_t cnt_;
    uint64_t peak_sz_;
    uint64_t sz_;
};

#endif
