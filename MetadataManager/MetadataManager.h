//
// Created by BorelsetR on 2019/7/29.
//

#ifndef ODESSNEW_MATADATAMANAGER_H
#define ODESSNEW_MATADATAMANAGER_H

#include <map>
#include "../Utility/StorageTask.h"
#include "../Utility/FileOperator.h"
#include <unordered_map>
#include <unordered_set>
#include <set>
#include <bitset>
#include <vector>
#include <csignal>
#include <queue>
#include <algorithm>
#include <fstream>
#include <cmath>
#include <sstream>
#include <iomanip>
#include <optional>
#include <utility>
#include <sys/time.h>

extern const int ContainerSize;
extern std::string LogicFilePath;

struct TupleHasher {
    std::size_t
    operator()(const SHA1FP &key) const {
        return key.fp1;
    }
};

struct TupleEqualer {
    bool operator()(const SHA1FP &lhs, const SHA1FP &rhs) const {
        return lhs.fp1 == rhs.fp1 && lhs.fp2 == rhs.fp2 && lhs.fp3 == rhs.fp3 && lhs.fp4 == rhs.fp4;
    }
};

bool operator==(const SHA1FP &lhs, const SHA1FP &rhs) {
    return lhs.fp1 == rhs.fp1 && lhs.fp2 == rhs.fp2 && lhs.fp3 == rhs.fp3 && lhs.fp4 == rhs.fp4;
}


// 定义二叉树节点结构
struct Node {
    Node* left;   // 左子节点
    Node* right;  // 右子节点
    Node* prev;   // 双向链表中的前一个叶子节点
    Node* next;   // 双向链表中的后一个叶子节点
    std::unordered_set<uint64_t> items;  // 节点中的列表，包含多个Item

    // 返回该节点的所有items
    std::unordered_set<uint64_t> getItems() const {
        return items;
    }
    // 向该节点的items列表中添加新的Item
    void addItem(uint64_t id) {
        items.insert(id);
    }
    // 构造函数
    Node() : left(nullptr), right(nullptr), prev(nullptr), next(nullptr) {}
};


class BinaryTree {
public:
    Node* root;  // 树的根节点
    Node* head;  // 双向链表的头节点（第一个叶子节点）
    Node* tail;  // 双向链表的尾节点（最后一个叶子节点）

    BinaryTree() : root(nullptr), head(nullptr), tail(nullptr) {}

    // 初始化二叉树：创建根节点并更新叶子节点链表
    void initializeTree() {
        // 创建根节点
        Node* newRoot = new Node();

        // 设置根节点
        root = newRoot;

        // 初始化时，根节点既是头也是尾
        head = tail = newRoot;

        // 设置根节点的前后指针为空，因为它是唯一的节点
        newRoot->prev = nullptr;
        newRoot->next = nullptr;
    }

    // 分裂指定节点，但不修改其items内容
    void splitNode(Node* node) {
        if (!node || (node->left != nullptr || node->right != nullptr)) {
            // 如果节点为空，或者该节点已经有子节点（说明已经分裂），则不处理
            return;
        }

        // 创建新的左、右子节点
        Node* leftNode = new Node();
        Node* rightNode = new Node();

        // 将当前节点的左右子节点指向新创建的子节点
        node->left = leftNode;
        node->right = rightNode;

        Node* oldPre = node->prev;
        Node* oldNext = node->next;

        if(oldPre != nullptr){
            oldPre->next = node->left;
            node->left->prev = oldPre;
        }

        node->left->next = node->right;
        node->right->prev = node->left;

        if(oldNext != nullptr){
            oldNext->prev = node->right;
            node->right->next = oldNext;
        }

        if(head == node){
            head = node->left;
        }
        if(tail == node){
            tail = node->right;
        }
    }

    void printTree() const {
        printNode(root, 0);
    }

    void printNode(Node* node, int depth) const {
        if (!node) return;

        // 打印当前节点的深度信息
        for (int i = 0; i < depth; ++i) {
            std::cout << "--";  // 用缩进来表示层级
        }

        // 打印节点信息，包括是否为叶子节点和列表的大小
        std::cout << "Node at depth " << depth;
        if (!node->left && !node->right) {
            std::cout << " (Leaf)";
        }
        std::cout << " - Items count: " << node->getItems().size() << std::endl;

        // 递归打印左、右子节点
        if (node->left) {
            printNode(node->left, depth + 1);
        }
        if (node->right) {
            printNode(node->right, depth + 1);
        }
    }

    // 获取节点的items列表
    std::unordered_set<uint64_t> getNodeItems(Node* node) const {
        if (node) {
            return node->getItems();
        } else {
            return std::unordered_set<uint64_t>(); // 如果节点为空，返回空列表
        }
    }

    int countLeafNodes() const {
        int count = 0;
        Node* current = head;

        // 从链表的头节点开始，遍历到尾节点
        while (current != nullptr) {
            ++count;
            current = current->next;  // 继续访问下一个叶子节点
        }

        return count;
    }
};

#define COUNTTHRESHOLD 200

class ScalableChunkLifeCycleNew{
public:
    // 析构函数：在对象销毁时释放所有动态分配的内存
    ~ScalableChunkLifeCycleNew() {
        // 递归释放树中的所有节点
        deleteAllNodes(lifecycleTree.root);
    }

    void ingest_new_version(const std::vector<uint64_t>& chunks_to_add, const std::vector<uint64_t>& chunks_to_update){
        if(lifecycleTree.root == nullptr){
            printf("ScalableChunkLifeCycleNew::InitRoot, %lu\n", chunks_to_add.size());
            lifecycleTree.initializeTree();
            lifecycleTree.splitNode(lifecycleTree.root);
            for(auto i : chunks_to_add){
                lifecycleTree.tail->addItem(i);
                localLifecycle[i] = lifecycleTree.tail;
            }
        }else{
            printf("ScalableChunkLifeCycleNew::Other Level, %lu, %lu\n", chunks_to_add.size(), chunks_to_update.size());
            std::unordered_map<Node*, std::unordered_set<uint64_t>> pre_category;
            for(auto i : chunks_to_update){
                Node* nptr = localLifecycle[i];
                pre_category[nptr].insert(i);
            }

            Node* tr_ptr = lifecycleTree.tail;
            while(tr_ptr != lifecycleTree.head){
                Node* next = tr_ptr->prev;

                std::unordered_set<uint64_t>& r_list = pre_category[tr_ptr];
                if(r_list.size() >= COUNTTHRESHOLD){
                    lifecycleTree.splitNode(tr_ptr);
                    for(auto item: tr_ptr->items){
                        if(r_list.find(item) == r_list.end()){
                            tr_ptr->left->addItem(item);
                            localLifecycle[item] = tr_ptr->left;
                        }else{
                            localLifecycle[item] = tr_ptr->right;
                        }
                    }
                    tr_ptr->right->items.swap(r_list);
                    r_list.clear();
                }

                tr_ptr = next;
            }

            lifecycleTree.splitNode(lifecycleTree.head);
            Node* secondLeft = lifecycleTree.head->next;
            for(auto item: chunks_to_add){
                secondLeft->addItem(item);
            }

        }
    }

    Node* getTail(){
        return lifecycleTree.tail;
    }

    Node* getHead(){
        return lifecycleTree.head;
    }

    Node* getRandom(){
        int count = getLeafNodeCount();
        int idx = rand()%count;
        Node* result = getHead();
        for(int i=0; i<idx; i++){
            result = result->next;
        }
        Node* pre = result->prev;
        Node* next = result->next;

        if(pre){
            pre->next = next;
        }else{
            lifecycleTree.head = next;
        }
        if(next){
            next->prev = pre;
        }else{
            lifecycleTree.tail = pre;
        }

        return result;
    }

    int checkExisting(uint64_t id){
        return localLifecycle.find(id) == localLifecycle.end() ? 0 : 1;
    }

    int getLeafNodeCount(){
        return lifecycleTree.countLeafNodes();
    }

private:

    // 递归删除所有节点
    void deleteAllNodes(Node* node) {
        if (node == nullptr) return;

        // 递归删除左子节点和右子节点
        deleteAllNodes(node->left);
        deleteAllNodes(node->right);

        // 释放当前节点
        delete node;
    }

    BinaryTree lifecycleTree;
    std::unordered_map<uint64_t, Node*> localLifecycle;
};


//class VariableBitset {
//public:
//    VariableBitset() : len_(0) {
//    }
//    VariableBitset(const VariableBitset&) = default;
//    size_t size() const {
//        return len_;
//    }
//    bool test(size_t pos) const {
//        assert(pos < len_);
//        return bits_[pos / 8] >> (pos % 8) & 1;
//    }
//    bool set(size_t pos) {
//        if (pos >= len_)
//            return false;
//        bits_[pos / 8] |= static_cast<char>(1 << (pos % 8));
//        return true;
//    }
//    bool reset(size_t pos) {
//        if (pos >= len_)
//            return false;
//        bits_[pos / 8] &= static_cast<char>(~(1 << (pos % 8)));
//        return true;
//    }
//    bool flip(size_t pos) {
//        if (pos >= len_)
//            return false;
//        bits_[pos / 8] ^= static_cast<char>(1 << (pos % 8));
//        return true;
//    }
//    uint8_t get_byte(size_t byte) {
//        assert(len_ > 0 && byte <= (len_-1) / 8);
//        return bits_[byte];
//    }
//    bool set_byte(size_t byte, uint8_t data) {
//        if (len_ == 0 || byte > (len_-1) / 8)
//            return false;
//        uint8_t mask = (1 << (len_ % 8 == 0 ? 8 : len_ % 8)) - 1;
//        bits_[byte] = data & mask;
//        return true;
//    }
//    void shift_left(size_t n_bits) {
//        size_t n_bytes = n_bits / 8;
//        size_t move_bits = n_bits % 8;
//        shift_left_by_bytes_(n_bytes);
//        len_ += move_bits;
//        size_t msb_bits = len_ % 8 == 0 ? 8 : len_ % 8;
//        if (move_bits + msb_bits > 8) // overflow
//            bits_.push_back(0);
//        for (size_t i = len_ / 8; i > 0; --i)
//            bits_[i] = bits_[i] << move_bits | bits_[i-1] >> (8 - move_bits);
//        bits_[0] <<= move_bits;
//    }
//    void shift_right(size_t n_bits) {
//        if (n_bits >= len_) {
//            bits_.clear();
//            len_ = 0;
//            return;
//        }
//        size_t n_bytes = n_bits / 8;
//        size_t move_bits = n_bits % 8;
//        shift_right_by_bytes_(n_bytes);
//        for (int i=0; i<(len_-1)/8; ++i)
//            bits_[i] = (bits_[i+1] & ((1 << move_bits) - 1)) << (8 - move_bits) | bits_[i] >> move_bits;
//        size_t msb_bits = len_ % 8 == 0 ? 8 : len_ % 8;
//        if (move_bits >= msb_bits)
//            bits_.pop_back();
//        else
//            bits_[(len_ - 1) / 8] >>= move_bits;
//        len_ -= move_bits;
//    }
//    void add_bit(bool bit) {
//        if (len_ % 8 == 0)
//            bits_.push_back(bit);
//        else
//            bits_[len_ / 8] |= (bit << (len_ % 8));
//        ++len_;
//    }
//    void delete_bit(size_t pos) {
//        size_t starting_byte = pos / 8;
//        size_t pos_within_byte = pos % 8;
//        if (pos >= len_)
//            return;
//        if (starting_byte < (len_-1) / 8) {
//            bits_[starting_byte] = (bits_[starting_byte + 1] & 1) << 7 | (bits_[starting_byte] >> (pos_within_byte + 1)) << (pos_within_byte) | (bits_[starting_byte] & ((1 << pos_within_byte) - 1));
//            int i;
//            for (i=starting_byte+1; i<(len_-1)/8; ++i)
//                bits_[i] = (bits_[i+1] & 1) << 7 | bits_[i] >> 1;
//            if (len_ % 8 == 1)
//                bits_.pop_back();
//            else
//                bits_[i] >>= 1;
//        } else if (len_ % 8 == 1)
//            bits_.pop_back();
//        else
//            bits_[starting_byte] = (bits_[starting_byte] >> (pos_within_byte + 1)) << (pos_within_byte) | (bits_[starting_byte] & ((1 << pos_within_byte) - 1));
//        --len_;
//    }
//    void clear() {
//        bits_.clear();
//        len_ = 0;
//    }
//    void print_bitmap() const {
//        for (int i=0; i<len_; ++i)
//            putchar(this->test(i) ? '1' : '0');
//        putchar('\n');
//    }
//    std::string to_string() const {
//        return std::string(bits_.begin(), bits_.end());
//    }
//private:
//    void shift_left_by_bytes_(size_t n_bytes) {
//        for (int i=0; i<n_bytes; ++i)
//            bits_.push_back(0);
//        for (int i=(len_-1)/8; i>=0; --i)
//            bits_[i + n_bytes] = bits_[i];
//        for (int i=n_bytes-1; i>=0; --i)
//            bits_[i] = 0;
//        len_ += n_bytes * 8;
//    }
//    void shift_right_by_bytes_(size_t n_bytes) {
//        for (int i=0; i<=(len_-1)/8; ++i)
//                bits_[i] = bits_[i + n_bytes];
//        for (int i=0; i<n_bytes; ++i)
//            bits_.pop_back();
//        len_ -= n_bytes * 8;
//    }
//    std::vector<uint8_t> bits_;
//    size_t len_;
//};

class ScalableChunkLifeCycle {
public:
    struct chunk_info {
        uint64_t id;
//        VariableBitset lifecycle;
        uint64_t size;
    };
    struct LifeCycleTreeNode {
        bool leaf;
        union {
            LifeCycleTreeNode* left; // used when node is not leaf
            std::vector<chunk_info>* chunk_list; // used when leaf is true; record all chunk fingerprints that belong to this lifecycle
        };
        union {
            LifeCycleTreeNode* right; // used when node is not leaf
            LifeCycleTreeNode* next; // used when leaf is true; point to next leaf node; good for traversal
        };
        bool parity;
        uint64_t size; // used when leaf is true; total size of all chunks represented by this node
        LifeCycleTreeNode* prev;
        void turn_leaf(std::vector<chunk_info>* lst, LifeCycleTreeNode* nxt, uint64_t sz, LifeCycleTreeNode* pr) {
            this->leaf = true;
            this->chunk_list = lst;
            this->next = nxt;
            this->size = sz;
            this->prev = pr;
        }
        void turn_inner(LifeCycleTreeNode* left, LifeCycleTreeNode* right) {
            this->leaf = false;
            this->left = left;
            this->right = right;
        }
        explicit LifeCycleTreeNode(bool is_leaf = true, bool parity = true) : leaf(is_leaf), chunk_list(nullptr), next(nullptr), prev(nullptr), size(0), parity(parity) {}
    };
    using ingest_func_t = void (*)(const std::vector<uint64_t>&, const std::vector<uint64_t>&, const std::vector<uint64_t>&);
    using delete_func_t = void (*)(int);
    explicit ScalableChunkLifeCycle(int THRESHOLD = ContainerSize) : versions_(0), leaves_(0), inners_(1), THRESHOLD(THRESHOLD) {
        root_ = new LifeCycleTreeNode(false);
        begin_ = nullptr;
    }
    void ingest_new_version(const std::vector<uint64_t>& chunks_to_add, const std::vector<uint64_t>& chunks_size, const std::vector<uint64_t>& chunks_to_update) {
        printf("ingest THRESHOLD is %d\n", THRESHOLD);
        assert(chunks_to_add.size() == chunks_size.size());
        if (begin_ == nullptr) { // only root node
            assert(chunks_to_update.empty());
            root_->left = new LifeCycleTreeNode();
            root_->right = new LifeCycleTreeNode();
            root_->left->chunk_list = new std::vector<chunk_info>();
            root_->left->next = root_->right;
            root_->left->prev = nullptr;
            root_->right->chunk_list = new std::vector<chunk_info>();
            root_->right->next = nullptr;
            root_->left->prev = root_->left;
            begin_ = root_->left;
            for (auto i = 0; i < chunks_to_add.size(); ++i) {
                root_->right->chunk_list->push_back(
                        {.id = chunks_to_add.at(i), .size = chunks_size.at(i)});
                root_->right->size += chunks_size.at(i);
                chunk_lifecycle_[chunks_to_add.at(i)] = root_->right;
            }
            leaves_ += 2;
        }
        else {
            printf("processing level\n");
            std::unordered_map<LifeCycleTreeNode*, std::unordered_set<uint64_t>> chunks_categorized;
            for (auto id : chunks_to_update) {
                auto fp_itr = chunk_lifecycle_.find(id);
                assert(fp_itr != chunk_lifecycle_.end());
                chunks_categorized[fp_itr->second].insert(id);
            }
            auto* ptr = root_;
            while (!ptr->leaf)
                ptr = ptr->left;
            LifeCycleTreeNode* leftmost = begin_;
            assert(begin_->leaf);
            assert(ptr == leftmost);
            LifeCycleTreeNode* leaf_ptr = begin_;
            LifeCycleTreeNode* prev_leaf = nullptr;
            while (leaf_ptr != nullptr) {
                printf("traverse node\n");
                auto* temp_chunk_list = leaf_ptr->chunk_list;
                auto old_next = leaf_ptr->next;
                bool is_leftmost = begin_ == leaf_ptr;
                if (leaf_ptr->size > THRESHOLD || is_leftmost) {
                    leaf_ptr->turn_inner(new LifeCycleTreeNode(), new LifeCycleTreeNode());
                    leaf_ptr->left->chunk_list = new std::vector<chunk_info>();
                    leaf_ptr->left->next = leaf_ptr->right;
                    leaf_ptr->left->size = 0;
                    leaf_ptr->left->prev = prev_leaf;
                    leaf_ptr->right->chunk_list = new std::vector<chunk_info>();
                    leaf_ptr->right->next = old_next;
                    leaf_ptr->right->size = 0;
                    leaf_ptr->right->prev = leaf_ptr->left;
                    if (prev_leaf != nullptr)
                        prev_leaf->next = leaf_ptr->left;
                    if (old_next != nullptr)
                        old_next->prev = leaf_ptr->right;
                    std::unordered_set<uint64_t> id_set{};
                    if (chunks_categorized.find(leaf_ptr) !=  chunks_categorized.end())
                        id_set = chunks_categorized[leaf_ptr];
//                    printf("splitting; list size = %zu\n", temp_chunk_list->size());
                    assert(is_leftmost || !temp_chunk_list->empty());
                    if (is_leftmost) {
                        begin_ = leaf_ptr->left;
                        for (auto i=0; i<chunks_to_add.size(); ++i) {
                            leaf_ptr->right->chunk_list->push_back({.id = chunks_to_add.at(i), .size = chunks_size.at(i)});
                            leaf_ptr->right->size += chunks_size.at(i);
                            chunk_lifecycle_[chunks_to_add.at(i)] = leaf_ptr->right;
                        }
                    }
                    for (const auto &chunk: *temp_chunk_list) {
                        if (id_set.find(chunk.id) != id_set.end()) {
                            leaf_ptr->right->chunk_list->push_back(chunk);
                            leaf_ptr->right->size += chunk.size;
                            chunk_lifecycle_[chunk.id] = leaf_ptr->right;
                        } else {
                            leaf_ptr->left->chunk_list->push_back(chunk);
                            leaf_ptr->left->size += chunk.size;
                            chunk_lifecycle_[chunk.id] = leaf_ptr->left;
                        }
                    }
                    ++inners_;
                    ++leaves_;
                    delete temp_chunk_list;
                    prev_leaf = leaf_ptr->right;
                } else {
                    prev_leaf = leaf_ptr;
                }
                leaf_ptr = old_next;
            }
        }
        ++versions_;
    }
    inline const LifeCycleTreeNode* locate_id(uint64_t id) {
        return chunk_lifecycle_.find(id) == chunk_lifecycle_.end() ? nullptr : chunk_lifecycle_[id];
    }


    int num_of_leaves() const {
        return leaves_;
    }
    int num_of_empty_leaves() const {
        int ret = 0;
//        puts("leaf traversal");
        for (const auto* ptr = begin_; ptr != nullptr; ptr = ptr->next) {
//            printf("pointer: %p\n", ptr);
            if (ptr->chunk_list->empty())
                ++ret;
        }
        return ret;
    }
    int num_of_inners() const {
        return inners_;
    }
    void print_leaf_statistics() {
        uint64_t empty = 0;
        uint64_t nonempty = 0;
        for (auto ptr = begin_; ptr != nullptr; ptr = ptr->next)
            ptr->chunk_list->empty() ? ++empty : ++nonempty;
    }
    void get_category_statistics() {
        int category_num = 0;
        int non_zero_category_num = 0;

        for (const auto* ptr = begin_; ptr != nullptr; ptr = ptr->next) {
            int tmp = ptr->chunk_list->size();
            if (tmp != 0)   non_zero_category_num ++;
            category_num ++;
        }

        printf("All category num: %d\n", category_num);
        printf("Non-zero category num: %d\n", non_zero_category_num);
    }
    void set_split_threshold(int THRESHOLD) {
        this->THRESHOLD = THRESHOLD;
    }

    void near_exact_lifecycle_count() {
        std::cout << "Counting near-exact lifecycles" << std::endl;
        std::fstream fs("haha4.txt", std::ios::out);
        for (const auto* ptr = begin_; ptr != nullptr; ptr = ptr->next)
            if (!ptr->chunk_list->empty())
                fs << ptr->chunk_list->size() << std::endl;
        fs.close();
    }

    struct Color {
        int r, g, b;

        Color(int red, int green, int blue) : r(red), g(green), b(blue) {}
        std::string toHex() const {
            std::stringstream ss;
            ss << "#" << std::hex << std::setw(2) << std::setfill('0') << r
            << std::setw(2) << std::setfill('0') << g
            << std::setw(2) << std::setfill('0') << b;
            return ss.str();
        }
    };

    Color interpolateColor(const Color& start, const Color& end, float t) {
        int r = static_cast<int>((1.0f - t) * start.r + t * end.r);
        int g = static_cast<int>((1.0f - t) * start.g + t * end.g);
        int b = static_cast<int>((1.0f - t) * start.b + t * end.b);

        return Color(r, g, b);
    }

    std::string colors[100];
    int degree = 0;
    void draw(const std::string &outf) {
        Color blue(115, 115, 255);
        Color red(255, 115, 115);
        for (int i = 0; i < 5; i ++) {
            float t = static_cast<float>(i) / 4.0f;
            Color interpolated = interpolateColor(blue, red, t);
            colors[i] = interpolated.toHex();
        }

        std::vector<uint64_t> chunk_list_size;
        for (const auto* ptr = begin_; ptr != nullptr; ptr = ptr->next) {
            chunk_list_size.push_back(ptr->chunk_list->size());
        }
        std::sort(chunk_list_size.begin(), chunk_list_size.end());

        degree = (chunk_list_size[chunk_list_size.size() - 1] - chunk_list_size[0]) / 5 + 1;

        std::ofstream out(outf);
        out << "digraph G {" << std::endl;
        out << "edge [dir=none];\nnode [style=filled fillcolor=\"#dedede\" fixedsize=true]" << std::endl;
        std::string name = "";
        to_graph(root_, out, name, 0);


        std::string leaf_prefix("LEAF_");
        std::string internal_prefix("INT_");
        std::queue<std::pair<LifeCycleTreeNode*, int>> q;
        q.push(std::make_pair(root_, 0));
        while (!q.empty()) {
            int size = q.size();
            out << "{rank=same ";

            while (size > 0) {
                const auto head = q.front();
                q.pop();
                size --;
                if (!(head.first->leaf)) {
                    if (root_ == head.first) {
                        assert(head.first->left && head.first->right || !head.first->left && !head.first->right);
                        if (head.first->left == nullptr && head.first->right == nullptr)
                            continue;
                    }
                    assert(head.first->left != nullptr && head.first->right != nullptr);
                    q.push(std::make_pair(head.first->left, head.second + 1));
                    q.push(std::make_pair(head.first->right, head.second + 1));
                    out << (head.first->left->leaf ? leaf_prefix : internal_prefix) << std::to_string(reinterpret_cast<uint64_t>(head.first->left)) << " " << (head.first->right->leaf ? leaf_prefix : internal_prefix) << std::to_string(reinterpret_cast<uint64_t>(head.first->right)) << " ";
                }
            }

            out << "};\n";
        }

        out << "}" << std::endl;
        out.close();
    }

    void to_graph(LifeCycleTreeNode* node, std::ofstream &out, std::string& name, int level) {
        const static std::string leaf_prefix("LEAF_");
        const static std::string internal_prefix("INT_");
        if (node->leaf) {
            // Print node name
            out << leaf_prefix << std::to_string(reinterpret_cast<uint64_t>(node));
            // Print node properties
            out << "[shape=circle label=\""+ name + "\" ";
            // Print color of the node
            int size = node->chunk_list->size();
            if (size == 0) {
                out << "];\n";
            } else {
                std::string color;
                color = colors[size / degree];
                out << "fillcolor=\""<< color << "\"];\n";
            }

            // Print Leaf node link if there is a next page
            // if (node->next) {
            //     out << leaf_prefix << std::to_string(reinterpret_cast<uint64_t>(node)) << " -> " << leaf_prefix << std::to_string(reinterpret_cast<uint64_t>(node->next)) << ";\n";
            // }
            return ;
        }
        // Print node name
        out << internal_prefix << std::to_string(reinterpret_cast<uint64_t>(node));
        // Print node properties
        out << "[shape=circle label=\""+ name + "\"];\n";
        // Print leaves
        to_graph(node->left, out, name, level + 1);
        out << internal_prefix << std::to_string(reinterpret_cast<uint64_t>(node)) << ":p" << std::to_string(reinterpret_cast<uint64_t>(node->left)) << " -> ";
        if (node->left->leaf) {
            out << leaf_prefix << std::to_string(reinterpret_cast<uint64_t>(node->left)) << ";\n";
        } else {
            out << internal_prefix << std::to_string(reinterpret_cast<uint64_t>(node->left)) << ";\n";
        }

        to_graph(node->right, out, name, level + 1);
        out << internal_prefix << reinterpret_cast<uint64_t>(node) << ":p" << reinterpret_cast<uint64_t>(node->right) << " -> ";
        if (node->right->leaf) {
            out << leaf_prefix << std::to_string(reinterpret_cast<uint64_t>(node->right)) << ";\n";
        } else {
            out << internal_prefix << std::to_string(reinterpret_cast<uint64_t>(node->right)) << ";\n";
        }
    }

    bool check_regularity(LifeCycleTreeNode* node) {
        if (node == root_ && begin_ == nullptr)
            return true;
        if (node->leaf)
            return true;
        return node->left != nullptr && node->right != nullptr && check_regularity(node->left) && check_regularity(node->right);
    }
    int get_level(LifeCycleTreeNode* node) {
        if (node == nullptr)
            return 0;
        if (node->leaf)
            return 1;
        int ll = get_level(node->left);
        int rl = get_level(node->right);
        return 1 + (ll >= rl ? ll : rl);
    }

    void debug_id_to_leaf_print() const {
#ifndef NDEBUG
        puts("------------id-to-leaf map------------");
        for (const auto& item : chunk_lifecycle_)
            printf("%lu -> %p\n", item.first, item.second);
#endif
    }
    ~ScalableChunkLifeCycle() {
        delete_lc_tree_(root_);
    }
    const LifeCycleTreeNode* const get_leaf_begin() const {
        return begin_;
    }

    private:
    void delete_lc_tree_(LifeCycleTreeNode* lc_tree) {
        if (lc_tree != nullptr) {
            if (lc_tree->leaf) {
                delete lc_tree->chunk_list;
                delete lc_tree;
            }
            else {
                delete_lc_tree_(lc_tree->left);
                delete_lc_tree_(lc_tree->right);
                delete lc_tree;
            }
        }
    }
    void mirror_tree_gray_(LifeCycleTreeNode* tree) {
        if (tree == root_ || tree == nullptr)
            return;
        if (!tree->leaf) {
            mirror_tree_gray_(tree->left);
            mirror_tree_gray_(tree->right);
            std::swap(tree->left, tree->right);
            tree->left->parity = true;
            tree->right->parity = false;
        }
    }
    LifeCycleTreeNode* root_;
    LifeCycleTreeNode* begin_;
    std::unordered_map<uint64_t, LifeCycleTreeNode*> chunk_lifecycle_;
    int versions_;
    std::set<int> obsolete_versions_;
    int leaves_; // # of leaf nodes
    int inners_; // # of inner nodes
    int THRESHOLD;
    const uint64_t DEBUG_ID = 1129577;
};

class MetadataManager {
public:
    MetadataManager(bool gray_encoding = true, uint64_t split_threshold = ContainerSize) : GRAY_ENCODING(gray_encoding) {}

    ~MetadataManager() {
        table.clear();
        gctable.clear();
        ContainerLifecycle.clear();
        AvailableContainers.clear();
        AvailableBackups.clear();
    }

    void getSize() {
        uint64_t fpSize = table.size();
        printf("FP:%lu\n", fpSize);
        return;
    }

    int addLifecycle(uint64_t cid, uint64_t bid) {
        auto iter = ContainerLifecycle[cid].find(bid);
        if (iter != ContainerLifecycle[cid].end()) {
            return 1;
        } else {
            ContainerLifecycle[cid].insert(bid);
            return 0;
        }

    }

    // have to make sure the container exists or program fails
    std::set<uint64_t> &getLifecycle(uint64_t cid) {
        if (cid == -1) return ContainerLifecycle[cid];
        assert(ContainerLifecycle.find(cid) != ContainerLifecycle.end());
        return ContainerLifecycle[cid];
    }

    void removeBackup(uint64_t bid) {
        for (auto iter = ContainerLifecycle.begin(); iter != ContainerLifecycle.end(); iter++) {
            iter->second.erase(bid);
        }
    }

    void deleteContainerLC(uint64_t cid) {
        auto iter = ContainerLifecycle.find(cid);
        assert(iter != ContainerLifecycle.end());
        ContainerLifecycle.erase(iter);
        return;
    }

//============================FP============================
    void addRecord(const SHA1FP &sha1Fp, uint64_t fid, uint64_t id) {
        table[sha1Fp].insert(std::make_pair(fid, id));
        return;
    }

    int findRecord(const SHA1FP &sha1Fp, Location *location) {
        auto iter = table.find(sha1Fp);
        if (iter != table.end()) {
            assert(iter->second.size() > 0);
            auto cidIter = iter->second.begin();
            assert(cidIter != iter->second.end());
            location->fid = cidIter->first;
            return 1;
        } else {
            return 0;
        }
    }

    int findRecord_targetfid(const SHA1FP &sha1Fp, Location *location, uint64_t fid) {
        auto iter = table.find(sha1Fp);
        if (iter != table.end()) {
            assert(iter->second.size() > 0);
            for(auto i : iter->second){
                if(i.first == fid) {
                    location->fid = fid;
                    return 1;
                }
            }
            auto cidIter = iter->second.begin();
            assert(cidIter != iter->second.end());
            location->fid = cidIter->first;
            return 1;
        } else {
            return 0;
        }
    }

    // ID -> vector of (SHA1FP, fid)
    std::vector<std::pair<SHA1FP, uint64_t>> findRecordById(const uint64_t id) {
        std::vector<std::pair<SHA1FP, uint64_t>> ret;
        for (const auto& kv : this->table)
            for (const auto& loc : kv.second)
                if (loc.second == id)
                    ret.push_back(std::make_pair(kv.first, loc.first));
        return ret;
    }

    std::map<uint64_t, uint64_t> findRecordSet(const SHA1FP &sha1Fp) {
        auto iter = table.find(sha1Fp);
        if (iter != table.end()) {
            assert(iter->second.size() > 0);
            return iter->second;
        } else {
            return std::map<uint64_t, uint64_t>{};
        }
    }

    void removeRecord(const SHA1FP &sha1Fp, uint64_t cid) {
        auto iter = table.find(sha1Fp);
        assert(iter != table.end());

        auto cidIter = iter->second.find(cid);
        assert(cidIter != iter->second.end());

        iter->second.erase(cid);

        if (iter->second.size() == 0) {
            table.erase(iter);
        }

        return;
    }

    void updateRecord(const SHA1FP &sha1Fp, uint64_t old_cid, uint64_t new_cid, uint64_t new_id) {
        auto iter = table.find(sha1Fp);
        assert(iter != table.end());

        auto cidIter = iter->second.find(old_cid);
        assert(cidIter != iter->second.end());

        iter->second.erase(old_cid);
        iter->second.insert(std::make_pair(new_cid, new_id));

        return;
    }

    void checkMigrateRecord(const SHA1FP &sha1Fp, uint64_t old_cid, uint64_t new_cid, uint64_t new_id) {
        auto iter = table.find(sha1Fp);
        assert(iter != table.end());

        auto cidIter = iter->second.find(old_cid);
        assert(cidIter != iter->second.end());

        iter->second.erase(old_cid);
        iter->second.insert(std::make_pair(new_cid, new_id));
        uint64_t temp = iter->second.find(new_cid)->second;
        assert(temp == new_id);


        return;
    }
//============================FP============================

//============================Backup============================
    void addBackups(int b) {

        AvailableBackups.insert(b);
        return;
    }

    void deleteBackups(int b) {
        AvailableBackups.erase(b);
        // removeBackup(b);
        // struct timeval t0, t1;
        // gettimeofday(&t0, NULL);
        // if (this->GRAY_ENCODING)
        //     SChunkLifeCycle.delete_gray(b+1);
        // else
        //     SChunkLifeCycle.delete_version(b+1);
        // gettimeofday(&t1, NULL);
        // uint64_t elapsed = (t1.tv_sec-t0.tv_sec)*1000000 + t1.tv_usec-t0.tv_usec;
        // std::cout << "ABT deletion elapsed time: " << elapsed << " microsecond(s)" << std::endl;
        return;
    }

    int getNthBackupIndex(int n) {
        assert(AvailableBackups.size() >= n);
        auto iter = AvailableBackups.begin();
        for (int i = 1; i < n; i++) {
            ++iter;
        }
        assert(iter != AvailableBackups.end());
        return *iter;
    }

    std::set<int> &getBackups() {
        return AvailableBackups;
    }

    void insertBackupSize(int cid, uint64_t size) {
        BackupSize[cid] = size;
    }

    uint64_t lookupBackupSize(int cid) {
        assert(BackupSize.find(cid) != BackupSize.end());
        return BackupSize[cid];
    }
//============================Backup============================


//============================GC Migration============================
    void insertGCTable(SHA1FP fp, uint64_t oldCID, uint64_t newCID, uint64_t id, uint64_t newid) {
        gctable[id][oldCID] = {newCID, newid};
        return;
    }

    std::optional<std::pair<uint64_t, uint64_t>> lookupGCTable(uint64_t id, uint64_t oldCID) {
        auto iter = gctable.find(id);
        if (iter != gctable.end()) {
            auto citer = iter->second.find(oldCID);
            if (citer != iter->second.end()) {
                return citer->second;
            }
        }
        return std::nullopt;
    }

    void clearGCTable() {
        gctable.clear();
        return;
    }
//============================GC Migration============================

//============================container============================
    void addAvailableContainer(int n) {
        AvailableContainers.insert(n);
        return;
    }

    void removeAvailableContainer(int n) {
        assert(AvailableContainers.find(n) != AvailableContainers.end());
        AvailableContainers.erase(n);
        return;
    }

    bool isAvaialableContainer(int n) {
        return AvailableContainers.find(n) != AvailableContainers.end();
    }

    size_t countAvailableContainer() const {
        return AvailableContainers.size();
    }
//============================container============================

//============================examine============================
    void indexExamine() {
        for (auto set: table) {
            for (auto item: set.second) {
                auto iter = AvailableContainers.find(item.first);
                assert(iter != AvailableContainers.end());
            }
        }
        return;
    }
//============================examine============================

    void scanForFP(SHA1FP fp) {
        char path[512];
        std::set<int> &availBackups = this->getBackups();
        for (auto i : availBackups) {
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
                if (wh.fp == DEBUG_SHA1FP)
                    printf("Found fingerprint: the ID is %lu, in File %d, in Container %lu\n", wh.id, i, loc.fid);
            }
        }
    }

    void traverseFile(int fileID) {
        assert(this->AvailableBackups.find(fileID) != this->AvailableBackups.end());
        char path[512];
        
        sprintf(path, LogicFilePath.data(), fileID);
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
            printf("Chunk in File %d: FP: %lu-%u-%u-%u, ID %lu, in Container %lu\n", fileID, wh.fp.fp1, wh.fp.fp2, wh.fp.fp3, wh.fp.fp4, wh.id, loc.fid);
        }
    }

//    ScalableChunkLifeCycle SChunkLifeCycle;
    const bool GRAY_ENCODING;

private:
    std::unordered_map<SHA1FP, std::map<uint64_t, uint64_t>, TupleHasher, TupleEqualer> table;
    std::unordered_map<uint64_t, std::unordered_map<uint64_t, std::pair<uint64_t,uint64_t>>> gctable; // SHA1FP -> oldCID -> <newCID, ID>
    std::map<uint64_t, std::set<uint64_t>> ContainerLifecycle;
    std::set<int> AvailableBackups;
    std::set<int> AvailableContainers;
    std::unordered_map<int, uint64_t> BackupSize;
    const SHA1FP DEBUG_SHA1FP = {.fp1 = 15844408918433038934llu, .fp2 = 854042722u, .fp3 = 1615005289u, .fp4 = 3183670265u};
    const uint64_t DEBUG_ID = 73842;
    const uint64_t DEBUG_CID = 1436;
};

static MetadataManager *GlobalMetadataManagerPtr;

#endif //ODESSNEW_MATADATAMA.sha1fp = info.sha1fpNAGER_H