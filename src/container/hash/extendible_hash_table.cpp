#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
  this->dir_.push_back(std::make_shared<Bucket>(bucket_size_, global_depth_));
}  // 最初，桶的长度是 2^0 即 1，应该提前建立一个桶，它的大小是桶大小，local_depth 是全局深度 0

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);  // 作用类似于 lock guard
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  std::lock_guard<std::mutex> guard_lock(latch_);  // 加锁
  std::shared_ptr<Bucket> bucket_ptr = FindBucket(key);
  return bucket_ptr->Find(key, value);  // 找到对应的桶，然后在桶中查找即可
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  std::lock_guard<std::mutex> guard_lock(latch_);  // 加锁
  std::shared_ptr<Bucket> bucket_ptr = FindBucket(key);
  return bucket_ptr->Remove(key);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::ResetDirectory() -> void {
  std::set<std::shared_ptr<Bucket>> bucket_visited{};
  // 遍历 directory 的前半部分，这前半部分的桶是已经被安排好的
  for (size_t idx = 0; idx < this->dir_.size(); ++idx) {
    std::shared_ptr<Bucket> bucket_ptr = dir_[idx];
    // 利用集合，确保每个桶仅被访问一次
    if (bucket_visited.find(bucket_ptr) != bucket_visited.end()) {
      continue;
    }
    // 记录某个桶被访问了
    bucket_visited.insert(bucket_ptr);
    int local_depth = bucket_ptr->GetDepth();
    // 在 directory 的后半部分，安排相应的桶(注意：分裂的那个桶需要被忽略)
    for (size_t j = 0; j < this->dir_.size(); ++j) {
      if (this->dir_[j] == nullptr && LowBitEquals(j, idx, local_depth)) {
        this->dir_[j] = bucket_ptr;  // 桶的重新映射
      }
    }
  }
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::SplitBucket(size_t raw_bucket_idx) {
  std::shared_ptr<Bucket> bucket_ptr = this->dir_[raw_bucket_idx];  // 原来的桶
  if (bucket_ptr->GetDepth() == this->global_depth_) {
    // 数组扩容
    this->dir_.resize(this->dir_.size() * 2);
    this->global_depth_++;
  }
  bucket_ptr->IncrementDepth();  // 原来的满了的桶增加 local_depth
  int new_local_depth = bucket_ptr->GetDepth();
  std::shared_ptr<Bucket> new_bucket_ptr = std::make_shared<Bucket>(bucket_size_, new_local_depth);
  std::list<std::pair<K, V>> &full_bucket_list = bucket_ptr->GetItems();  // 注意它返回的是引用
  std::vector<K> key_to_be_reomved{};                                     // 记录将要被移动的 key
  // 将部分元素插入到新的桶中
  for (auto &elem : full_bucket_list) {
    if (!LowBitEquals(IndexOf(elem.first), raw_bucket_idx, new_local_depth)) {
      key_to_be_reomved.push_back(elem.first);
      new_bucket_ptr->Insert(elem.first, elem.second);  // 插入到新的桶中
    }
  }
  // 原来的桶需要将不合适的元素移除
  for (const K &key : key_to_be_reomved) {
    bucket_ptr->Remove(key);
  }
  int new_bucket_idx{-1};
  // 在桶的数据整理完毕后，需要为这些桶找到它们的索引。
  for (size_t i = 0; i < this->dir_.size(); ++i) {
    // 检测桶分裂之前，一个桶占据多少索引
    if (!LowBitEquals(i, raw_bucket_idx, new_local_depth - 1)) {
      continue;
    }
    if (LowBitEquals(i, raw_bucket_idx, new_local_depth)) {
      this->dir_[i] = bucket_ptr;
    } else {
      if (new_bucket_idx == -1) {
        new_bucket_idx = i;
      }                                // 随意记录一个即可
      this->dir_[i] = new_bucket_ptr;  // 不属于原来那个桶的索引，就是新桶的索引
    }
  }
  this->num_buckets_++;  // 分裂完成后，桶的个数 + 1
  //! note 如果一分为二的情况下，有桶还是满的，你需要对该桶继续分裂。即可能分裂失败，有的桶是空的
  if (bucket_ptr->IsOverFlow()) {
    SplitBucket(raw_bucket_idx);
    return;
  }
  if (new_bucket_ptr->IsOverFlow()) {
    SplitBucket(new_bucket_idx);
    return;
  }
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  std::lock_guard<std::mutex> guard_lock(latch_);  // 为了线程安全，加锁，这个锁直到 insert 结束才被释放。
  std::shared_ptr<Bucket> bucket_ptr = FindBucket(key);  // 首先查找对应的插入位置，一般而言，桶已经被初始化好了
  if (!bucket_ptr->IsFull()) {                           // 如果桶还没有满，直接插入即可
    bucket_ptr->Insert(key, value);
    return;  //! \bug 在直接插入之后，你需要返回
  }
  // 桶满了，需要分类讨论
  if (bucket_ptr->Insert(key, value)) {
    return;  // 这是因为可能存在 key 相同的情况
  }
  size_t raw_bucket_idx = IndexOf(key);  // 扩容之前， 新插入的 key 在哪个桶？
  // 此时，不可能出现 key 相同的情况了。将新元素插入合适位置，先插入，再拆分
  this->dir_[raw_bucket_idx]->GetItems().push_back(std::make_pair(key, value));
  // 溢出的桶拆分
  SplitBucket(raw_bucket_idx);
  // 重新安排桶，使得扩容后，所有指针都有所指向，不能是空值
  ResetDirectory();
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  auto it = Find(key);
  if (it != list_.end()) {
    value = it->second;
    return true;
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  auto it = Find(key);
  if (it == list_.end()) {
    return false;
  }
  list_.erase(it);  // 移除元素
  return true;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  auto it = Find(key);
  if (it != list_.end()) {  // 如果 key 存在，则更新其值
    it->second = value;
    return true;
  }
  if (IsFull()) {
    return false;
  }  // 如果桶满了，那么直接返回 false
  // 其它一般情况：直接在尾部插入值即可
  list_.push_back(std::make_pair(key, value));
  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
