/**
 * extendible_hash_table.h
 *
 * Implementation of in-memory hash table using extendible hashing
 */

#pragma once

#include <algorithm>
#include <list>
#include <memory>
#include <mutex>  // NOLINT
#include <set>
#include <utility>
#include <vector>

#include "container/hash/hash_table.h"
namespace bustub {
/**
 * ExtendibleHashTable implements a hash table using the extendible hashing algorithm.
 * @note 可扩展哈希表维护了映射关系 [page_id --> frame_id]，所谓的 page 就是从磁盘中读入的页，
 * frame 就是盛放 page 的地方。
 * @tparam K key type
 * @tparam V value type
 */
template <typename K, typename V>
class ExtendibleHashTable : public HashTable<K, V> {
 public:
  /**
   * Bucket class for each hash table bucket that the directory points to.
   */
  class Bucket {
   public:
    explicit Bucket(size_t size, int depth = 0);

    /** @brief Check if a bucket is full. */
    inline auto IsFull() const -> bool { return list_.size() == size_; }
    inline auto IsOverFlow() const -> bool { return list_.size() > size_; }

    /** @brief Get the local depth of the bucket. */
    inline auto GetDepth() const -> int { return depth_; }

    /** @brief Increment the local depth of a bucket. */
    inline void IncrementDepth() { depth_++; }

    inline auto GetItems() -> std::list<std::pair<K, V>> & { return list_; }

    /**
     * @brief Find the value associated with the given key in the bucket.
     * @param key The key to be searched.
     * @param[out] value The value associated with the key.
     * @return True if the key is found, false otherwise.
     */
    auto Find(const K &key, V &value) -> bool;

    /**
     * 查找某个 key 在 桶中是否存在
     * @param key 需要被查找的 key
     * @return 返回迭代器
     */
    auto Find(const K &key) -> typename std::list<std::pair<K, V>>::iterator {
      return std::find_if(list_.begin(), list_.end(),
                          [&key](const std::pair<K, V> &elem) { return elem.first == key; });
    }

    /**
     *
     * TODO(P1): Add implementation
     *
     * @brief Given the key, remove the corresponding key-value pair in the bucket.
     * @param key The key to be deleted.
     * @return True if the key exists, false otherwise.
     */
    auto Remove(const K &key) -> bool;

    /**
     *
     * TODO(P1): Add implementation
     *
     * @brief Insert the given key-value pair into the bucket.
     *      1. If a key already exists, the value should be updated.
     *      2. If the bucket is full, do nothing and return false.
     * @param key The key to be inserted.
     * @param value The value to be inserted.
     * @return True if the key-value pair is inserted, false otherwise.
     */
    auto Insert(const K &key, const V &value) -> bool;

   private:                            // 注意：你可能需要加锁来保护数据安全
    size_t size_;                      // 桶的大小
    int depth_;                        // 对于可扩展哈希表来说，这是桶的深度(local depth)
    std::list<std::pair<K, V>> list_;  // 用双向链表真正存储数据
  };

  /**
   *
   * TODO(P1): Add implementation
   *
   * @brief Create a new ExtendibleHashTable.
   * @param bucket_size: fixed size for each bucket
   */
  explicit ExtendibleHashTable(size_t bucket_size);

  /**
   * @brief Get the global depth of the directory.
   * @return The global depth of the directory.
   */
  auto GetGlobalDepth() const -> int;

  /**
   * @brief Get the local depth of the bucket that the given directory index points to.
   * @param dir_index The index in the directory.
   * @return The local depth of the bucket.
   */
  auto GetLocalDepth(int dir_index) const -> int;

  /**
   * @brief Get the number of buckets in the directory.
   * @return The number of buckets in the directory.
   */
  auto GetNumBuckets() const -> int;

  /**
   *
   * TODO(P1): Add implementation
   *
   * @brief Find the value associated with the given key.
   *
   * Use IndexOf(key) to find the directory index the key hashes to.
   *
   * @param key The key to be searched.
   * @param[out] value The value associated with the key.
   * @return True if the key is found, false otherwise.
   */
  auto Find(const K &key, V &value) -> bool override;

  /**
   *
   * TODO(P1): Add implementation
   *
   * @brief Insert the given key-value pair into the hash table.
   * If a key already exists, the value should be updated.
   * If the bucket is full and can't be inserted, do the following steps before retrying:
   *    1. If the local depth of the bucket is equal to the global depth,
   *        increment the global depth and double the size of the directory.
   *    2. Increment the local depth of the bucket.
   *    3. Split the bucket and redistribute directory pointers & the kv pairs in the bucket.
   *
   * @param key The key to be inserted.
   * @param value The value to be inserted.
   */
  void Insert(const K &key, const V &value) override;

  /**
   *
   * TODO(P1): Add implementation
   *
   * @brief Given the key, remove the corresponding key-value pair in the hash table.
   * Shrink & Combination is not required for this project
   * @param key The key to be deleted.
   * @return True if the key exists, false otherwise.
   */
  auto Remove(const K &key) -> bool override;

  /**
   * 桶的分裂。将一个桶分成两个部分。如果原来 local_depth = 2, 现在 local_depth = 3，则桶会被1分为2，二者距离是 2^3 -
   * 2^2 = 8 - 4 = 4
   * 注意：桶的分裂是在桶满了，并且新的值要进入这个桶，但是没有进入这个桶之前。所以桶分裂后还需要手动将新的键值对插入
   * @param raw_bucket_idx 要分裂的桶的索引
   */
  void SplitBucket(size_t raw_bucket_idx);

  /**
   * 在增加了新的桶之后，重新安排 directory，因为有的条目可能要指向同一个桶
   */
  auto ResetDirectory() -> void;

  /**
   * 一个 bucket_ptr 对应着 this.dir_ 的哪些索引？
   */
  // auto IndexOfBucket(const std::shared_ptr<Bucket>& bucket_ptr) -> vector;

  /**
   * 根据 key，找到合适的桶
   * @return 指向桶的指针，特殊情况：如果没有桶，则返回 nullptr(即这个智能指针是 nullptr)
   */
  auto FindBucket(const K &key) -> std::shared_ptr<Bucket> {
    if (dir_.empty()) {
      return nullptr;
    }
    size_t idx = IndexOf(key);
    return dir_[idx];
  }

  /**
   * 判断两个数字的低 n 比特位是否一致
   */
  auto LowBitEquals(size_t idx1, size_t idx2, int n) -> bool {
    int mask = (1 << n) - 1;
    return (idx1 & mask) == (idx2 & mask);
  }

 private:
  // TODO(student): You may add additional private members and helper functions and remove the ones
  // you don't need.

  int global_depth_;                          // The global depth of the directory
  size_t bucket_size_;                        // The size of a bucket
  int num_buckets_;                           // The number of buckets in the hash table
  mutable std::mutex latch_;                  // 互斥量，或许需要在合适的时候加锁
  std::vector<std::shared_ptr<Bucket>> dir_;  // The directory of the hash table

  // The following functions are completely optional, you can delete them if you have your own ideas.

  /**
   * @brief Redistribute the kv pairs in a full bucket.
   * @param bucket The bucket to be redistributed.
   */
  auto RedistributeBucket(std::shared_ptr<Bucket> bucket) -> void;

  /*****************************************************************
   * Must acquire latch_ first before calling the below functions. *
   *****************************************************************/

  /**
   * @brief For the given key, return the entry index in the directory where the key hashes to.
   * @param key The key to be hashed.
   * @return The entry index in the directory.
   */
  auto IndexOf(const K &key) -> size_t;

  auto GetGlobalDepthInternal() const -> int;
  auto GetLocalDepthInternal(int dir_index) const -> int;
  auto GetNumBucketsInternal() const -> int;
};

}  // namespace bustub
