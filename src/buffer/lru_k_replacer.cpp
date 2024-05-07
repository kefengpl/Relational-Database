/**
 * 加锁模式：皆采用全局锁，当然，这样相当于你的并发没有任何价值
 */

#include "buffer/lru_k_replacer.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> latch_guard(latch_);    // 加入全局锁进行保护
  std::pair<size_t, frame_id_t> less_k_evict{0, -1};  // 少于 k 次的访问中，应该驱逐那个？
  std::pair<size_t, frame_id_t> k_evict{0, -1};  // k 次的访问中，应该驱逐哪个？[记录最早的时间戳 --> 对应的 frame_id]
  // 都找队列第一个元素即可
  for (const auto &access_record : access_records_) {
    frame_id_t cur_frame_id{access_record.first};
    if (NotEvictable(cur_frame_id)) {
      continue;
    }
    const std::queue<size_t> &cur_access_queue = access_record.second;
    if (cur_access_queue.size() == k_) {  // 已经达到 k 次的访问
      SetEvictPair(k_evict, cur_frame_id, cur_access_queue);
    } else {
      SetEvictPair(less_k_evict, cur_frame_id, cur_access_queue);
    }
  }

  // 判断：如果没有元素被驱逐，则返回false
  if (less_k_evict.second == -1 && k_evict.second == -1) {
    return false;
  }
  // 能驱逐访问未满 k 次的，优先驱逐
  if (less_k_evict.second != -1) {
    *frame_id = less_k_evict.second;  //! \bug 注意记录输出的 id
    UnSafeRemove(less_k_evict.second);
  } else {
    // 不行的话驱逐 访问满 k 次的，但是往前数 k 次的这个访问时间戳最小
    *frame_id = k_evict.second;
    UnSafeRemove(k_evict.second);
  }
  return true;
}

void LRUKReplacer::SetEvictPair(std::pair<size_t, frame_id_t> &evict_pair, frame_id_t frame_id,
                                const std::queue<size_t> &access_queue) {
  if (evict_pair.second == -1) {
    evict_pair.first = access_queue.front();  // 记录时间戳
    evict_pair.second = frame_id;             // 记录 frame id
  } else {
    if (access_queue.front() < evict_pair.first) {
      evict_pair.first = access_queue.front();  // 你需要记录满足 k 次的情况下最早的时间戳及其 frame_id
      evict_pair.second = frame_id;
    }
  }
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::lock_guard<std::mutex> latch_guard(latch_);  // 加锁，对下面的部分进行保护
  // 1. 如果 frame_id 无效，直接抛出异常
  BUSTUB_ASSERT(IsValid(frame_id), "Invalid frame id.");
  size_t current_count = access_count_++;
  // 2. 其余情况，你需要记录其访问
  if (access_records_.find(frame_id) == access_records_.end()) {
    std::queue<size_t> access_queue{};
    PushQueue(access_queue, current_count);
    access_records_[frame_id] = access_queue;
  } else {
    PushQueue(access_records_[frame_id], current_count);
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> latch_guard(latch_);  // 加锁，对下面的部分进行保护
  // 1. 如果 frame_id 无效，直接抛出异常
  BUSTUB_ASSERT(IsValid(frame_id), "Invalid frame id.");
  // 1.5 如果 这个 frame_id 不存在，则直接返回[根本没有访问记录]
  if (access_records_.find(frame_id) == access_records_.end()) {
    return;
  }
  // 2. 其它正常情况，需要将 frame_id 对应的标记进行修改，并修改 curr_size_(当前可驱逐 page 的数量)
  if (frame_evictable_.find(frame_id) == frame_evictable_.end()) {  // 新建元组，默认是 true
    frame_evictable_[frame_id] = true;
    curr_size_++;
  }
  if (frame_evictable_[frame_id] == set_evictable) {
    return;
  }
  if (frame_evictable_[frame_id] && !set_evictable) {
    --curr_size_;
  } else if (!frame_evictable_[frame_id] && set_evictable) {
    ++curr_size_;
  }
  frame_evictable_[frame_id] = set_evictable;
}

void LRUKReplacer::UnSafeRemove(frame_id_t frame_id) {
  BUSTUB_ASSERT(IsValid(frame_id), "Invalid frame id.");
  BUSTUB_ASSERT(!NotEvictable(frame_id), "This frame is not evictable.");
  // 失败情况：如果这个 id 根本不存在，则直接返回即可
  if (access_records_.find(frame_id) == access_records_.end()) {
    return;
  }
  // 其它情况：需要删除对应记录
  access_records_.erase(frame_id);
  frame_evictable_.erase(frame_id);
  --curr_size_;  // 可驱逐的 frame 数量 - 1
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> latch_guard(latch_);
  UnSafeRemove(frame_id);
}

auto LRUKReplacer::Size() -> size_t {
  std::lock_guard<std::mutex> latch_guard(latch_);
  return this->curr_size_;
}

void LRUKReplacer::PushQueue(std::queue<size_t> &access_queue, size_t time_stamp) {
  if (access_queue.size() < this->k_) {
    access_queue.push(time_stamp);
  } else {
    access_queue.pop();
    access_queue.push(time_stamp);
  }
}

}  // namespace bustub
