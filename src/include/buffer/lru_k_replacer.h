#pragma once

#include <limits>
#include <list>
#include <map>
#include <mutex>  // NOLINT
#include <queue>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/macros.h"

namespace bustub {

/**
 * LRUKReplacer implements the LRU-k replacement policy.
 *
 * The LRU-k algorithm evicts a frame whose backward k-distance is maximum
 * of all frames. Backward k-distance is computed as the difference in time between
 * current timestamp and the timestamp of kth previous access.
 *
 * A frame with less than k historical references is given
 * +inf as its backward k-distance. When multiple frames have +inf backward k-distance,
 * classical LRU algorithm is used to choose victim.
 * @note 它以 frame_id 记录每个 frame 的访问情况，是否固定，以及决定哪个 frame_id 对应的 frame 应该被驱逐？
 */
class LRUKReplacer {
 public:
  /**
   *
   * TODO(P1): Add implementation
   *
   * @brief a new LRUKReplacer.
   * @param num_frames the maximum number of frames the LRUReplacer will be required to store
   */
  explicit LRUKReplacer(size_t num_frames, size_t k);

  DISALLOW_COPY_AND_MOVE(LRUKReplacer);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Destroys the LRUReplacer.
   */
  ~LRUKReplacer() = default;

  /**
   * @brief Find the frame with largest backward k-distance and evict that frame. Only frames
   * that are marked as 'evictable' are candidates for eviction.
   *
   * A frame with less than k historical references is given +inf as its backward k-distance.
   * If multiple frames have inf backward k-distance, then evict the frame with the earliest
   * timestamp overall.
   *
   * Successful eviction of a frame should decrement the size of replacer and remove the frame's
   * access history.
   * @note frame_id 是从1 开始计数的
   * @param[out] frame_id id of frame that is evicted.
   * @return true if a frame is evicted successfully, false if no frames can be evicted.
   */
  auto Evict(frame_id_t *frame_id) -> bool;

  /**
   * @brief Record the event that the given frame id is accessed at current timestamp.
   * Create a new entry for access history if frame id has not been seen before.
   *
   * If frame id is invalid (ie. larger than replacer_size_), throw an exception. You can
   * also use BUSTUB_ASSERT to abort the process if frame id is invalid.
   *
   * @param frame_id id of frame that received a new access.
   */
  void RecordAccess(frame_id_t frame_id);

  /**
   * @brief Toggle whether a frame is evictable or non-evictable. This function also
   * controls replacer's size. Note that size is equal to number of evictable entries.
   *
   * If a frame was previously evictable and is to be set to non-evictable, then size should
   * decrement. If a frame was previously non-evictable and is to be set to evictable,
   * then size should increment.
   *
   * If frame id is invalid, throw an exception or abort the process.
   *
   * For other scenarios, this function should terminate without modifying anything.
   *
   * @param frame_id id of frame whose 'evictable' status will be modified
   * @param set_evictable whether the given frame is evictable or not
   */
  void SetEvictable(frame_id_t frame_id, bool set_evictable);

  /**
   * @brief Remove an evictable frame from replacer, along with its access history.
   * This function should also decrement replacer's size if removal is successful.
   *
   * Note that this is different from evicting a frame, which always remove the frame
   * with largest backward k-distance. This function removes specified frame id,
   * no matter what its backward k-distance is.
   *
   * If Remove is called on a non-evictable frame, throw an exception or abort the
   * process.
   *
   * If specified frame is not found, directly return from this function.
   *
   * @param frame_id id of frame to be removed
   */
  void Remove(frame_id_t frame_id);

  void UnSafeRemove(frame_id_t frame_id);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Return replacer's size, which tracks the number of evictable frames.
   *
   * @return size_t
   */
  auto Size() -> size_t;

  /**
   * 获取当前访问计数序列
   */
  auto GetAccessCount() -> size_t;

  /**
   * 将某个时间戳加入到一个 page 的访问队列中
   * 注意：队列大小上限是k，如果超过k，则第一个元素出队，最后一个元素入队
   * @param time_stamp 访问时的时间戳
   */
  void PushQueue(std::queue<size_t> &access_queue, size_t time_stamp);

  /**
   * 一个辅助函数，设置 evict_pair，使得它始终包含最小时间戳及其对应的 frame_id。
   * 如果 evict_pair 尚未初始化(第二个值是 -1)，那么执行赋值操作，无需判断时间戳大小。
   */
  void SetEvictPair(std::pair<size_t, frame_id_t> &evict_pair, frame_id_t frame_id,
                    const std::queue<size_t> &access_queue);

  /**
   * 检查某个 frame_id 是否有效。注意 frame_id 从 1 开始，所以 无效的情况是 frame_id > replacer_size_
   */
  inline auto IsValid(frame_id_t frame_id) -> bool { return static_cast<size_t>(frame_id) < replacer_size_; }
  /**
   * 判断某个 frame 是否能够被驱逐。只有记录在 map 中并且是 false 的情况才是不能被驱除的
   */
  inline auto NotEvictable(frame_id_t frame_id) -> bool {
    return (frame_evictable_.find(frame_id) != frame_evictable_.end()) && (!frame_evictable_[frame_id]);
  }

 private:
  // TODO(student): implement me! You can replace these member variables as you like.
  // Remove maybe_unused if you start using them.
  [[maybe_unused]] size_t current_timestamp_{0};
  size_t curr_size_{0};  // 记录 evitable page 的数目(也就是 unpinned)，初始这个值是0
  size_t replacer_size_;  // buffer pool 的大小，表示能够存放的页的最多数量(在 MySQL 中，一个页是 16KB)
  size_t k_;              // LRU-K 算法的这个 K 。
  // 提示：你的核心数据结构应该是一个 set，用队列记录最近 k 次访问先后的时间戳
  // 使用一个 map 记录某个 frame 是否能够被驱逐(一个 frame 使用 frame_id_t 来表示即可)
  std::unordered_map<frame_id_t, bool>
      frame_evictable_{};  // 记录每个页能否被驱逐[它的元素数目应该和下面 access_records 的数目一致]
  std::map<frame_id_t, std::queue<size_t>> access_records_{};  // 记录每个页的访问时间戳[用队列维护]
  size_t access_count_{0};                                     // 记录访问次数的计数器
  std::mutex latch_;
};

}  // namespace bustub
