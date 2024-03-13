#pragma once

#include <list>
#include <mutex>  // NOLINT
#include <unordered_map>

#include "buffer/buffer_pool_manager.h"
#include "buffer/lru_k_replacer.h"
#include "common/config.h"
#include "container/hash/extendible_hash_table.h"
#include "recovery/log_manager.h"
#include "storage/disk/disk_manager.h"
#include "storage/page/page.h"

namespace bustub {

/**
 * BufferPoolManager reads disk pages to and from its internal buffer pool.
 */
class BufferPoolManagerInstance : public BufferPoolManager {
 public:
  /**
   * @brief Creates a new BufferPoolManagerInstance.
   * @param pool_size the size of the buffer pool
   * @param disk_manager the disk manager
   * @param replacer_k the lookback constant k for the LRU-K replacer
   * @param log_manager the log manager (for testing only: nullptr = disable logging). Please ignore this for P1.
   */
  BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k = LRUK_REPLACER_K,
                            LogManager *log_manager = nullptr);

  /**
   * @brief Destroy an existing BufferPoolManagerInstance.
   */
  ~BufferPoolManagerInstance() override;

  /** @brief Return the size (number of frames) of the buffer pool. */
  auto GetPoolSize() -> size_t override { return pool_size_; }

  /** @brief Return the pointer to all the pages in the buffer pool. */
  auto GetPages() -> Page * { return pages_; }

 protected:
  /**
   * @brief Create a new page in the buffer pool. Set page_id to the new page's id, or nullptr if all frames
   * are currently in use and not evictable (in another word, pinned).
   *
   * You should pick the replacement frame from either the free list or the replacer (always find from the free list
   * first), and then call the AllocatePage() method to get a new page id. If the replacement frame has a dirty page,
   * you should write it back to the disk first. You also need to reset the memory and metadata for the new page.
   *
   * Remember to "Pin" the frame by calling replacer.SetEvictable(frame_id, false)
   * so that the replacer wouldn't evict the frame before the buffer pool manager "Unpin"s it.
   * Also, remember to record the access history of the frame in the replacer for the lru-k algorithm to work.
   *
   * @note 是在 frame 中创建一个空的 page 对象。相当于创建了一个空的 page，其意义在于：其它组件可以通过这个空的 page
   * 写回磁盘。
   * @param[out] page_id id of created page
   * @return nullptr if no new pages could be created, otherwise pointer to new page
   */
  auto NewPgImp(page_id_t *page_id) -> Page * override;

  /**
   * @brief Fetch the requested page from the buffer pool. Return nullptr if page_id needs to be fetched from the disk
   * but all frames are currently in use and not evictable (in another word, pinned).
   *
   * First search for page_id in the buffer pool. If not found, pick a replacement frame from either the free list or
   * the replacer (always find from the free list first), read the page from disk by calling disk_manager_->ReadPage(),
   * and replace the old page in the frame. Similar to NewPgImp(), if the old page is dirty, you need to write it back
   * to disk and update the metadata of the new page
   *
   * In addition, remember to disable eviction and record the access history of the frame like you did for NewPgImp().
   * @note 与第一个函数的不同之处在于，这个是尝试从 buffer_pool 中读取磁盘。如果 buffer_pool 有数据，则无需从磁盘读取
   * @param page_id id of page to be fetched
   * @return nullptr if page_id cannot be fetched, otherwise pointer to the requested page
   */
  auto FetchPgImp(page_id_t page_id) -> Page * override;

  /**
   * @brief Unpin the target page from the buffer pool. If page_id is not in the buffer pool or its pin count is already
   * 0, return false.
   *
   * Decrement the pin count of a page. If the pin count reaches 0, the frame should be evictable by the replacer.
   * Also, set the dirty flag on the page to indicate if the page was modified.
   *
   * @param page_id id of page to be unpinned
   * @param is_dirty true if the page should be marked as dirty, false otherwise
   * @return false if the page is not in the page table or its pin count is <= 0 before this call, true otherwise
   */
  auto UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool override;

  /**
   * @brief Flush the target page to disk. 将一个页写回磁盘。
   *
   * Use the DiskManager::WritePage() method to flush a page to disk, REGARDLESS of the dirty flag.
   * Unset the dirty flag of the page after flushing.
   *
   * @param page_id id of page to be flushed, cannot be INVALID_PAGE_ID
   * @return false if the page could not be found in the page table, true otherwise
   */
  auto FlushPgImp(page_id_t page_id) -> bool override;
  auto UnsafeFlushPgImp(page_id_t page_id) -> bool;

  /**
   * @brief Flush all the pages in the buffer pool to disk.
   */
  void FlushAllPgsImp() override;

  /**
   * @brief Delete a page from the buffer pool. If page_id is not in the buffer pool, do nothing and return true. If the
   * page is pinned and cannot be deleted, return false immediately.
   *
   * After deleting the page from the page table, stop tracking the frame in the replacer and add the frame
   * back to the free list. Also, reset the page's memory and metadata. Finally, you should call DeallocatePage() to
   * imitate freeing the page on the disk.
   *
   * @param page_id id of page to be deleted
   * @return false if the page exists but could not be deleted, true if the page didn't exist or deletion succeeded
   */
  auto DeletePgImp(page_id_t page_id) -> bool override;

  /**
   * @brief PageGuard wrappers for FetchPage
   *
   * Functionality should be the same as FetchPage, except
   * that, depending on the function called, a guard is returned.
   * If FetchPageRead or FetchPageWrite is called, it is expected that
   * the returned page already has a read or write latch held, respectively.
   *
   * @param page_id, the id of the page to fetch
   * @return PageGuard holding the fetched page
   */
  // auto FetchPageBasic(page_id_t page_id) -> BasicPageGuard;
  // auto FetchPageRead(page_id_t page_id) -> ReadPageGuard;
  // auto FetchPageWrite(page_id_t page_id) -> WritePageGuard;

  /**
   * TODO(P1): Add implementation
   *
   * @brief PageGuard wrapper for NewPage
   *
   * Functionality should be the same as NewPage, except that
   * instead of returning a pointer to a page, you return a
   * BasicPageGuard structure.
   *
   * @param[out] page_id, the id of the new page
   * @return BasicPageGuard holding a new page
   */
  // auto NewPageGuarded(page_id_t *page_id) -> BasicPageGuard;

  /**
   * 在 pages_ 数组 中寻找 page_id 对应的页
   * @return 如果成功找到这个 page，就返回地址；其它情况返回 nullptr
   */
  auto FindPage(page_id_t page_id) -> Page *;

  /**
   * 为一个 page 分配新 frame
   * @note 这个函数不会向磁盘发起读取或者写入，它仅用来给 page 分配 frame
   * @param new_page 是否是添加新的空白页(用于写入)
   * @param page_id 如果是新的空白页，那么会生成新的 page_id , 储存到 page_id 中；如果 new_page 是 false，那么 page_id
   * 就是传入的 page_id
   * @return 分配的 frame 的 page 指针。在无法分配 frame 的情况下，返回 nullptr
   */
  auto AllocateFrameForPage(bool new_page, page_id_t *page_id) -> Page *;

  /**
   * 用来固定某个 page，
   * ① page pin_count_ 计数 + 1
   * ② LRUK 替换策略管理器 记录一次访问
   * ③ pin_count > 0 设置为不可驱逐
   */
  void PinPage(Page *page, frame_id_t frame_id);

  /**
   * 把一个 page 的 data 及其所有元数据都完全清空
   */
  void ClaerPage(Page *page) {
    if (page == nullptr) {
      return;
    }
    page->ResetMemory();   // 清空 page
    page->pin_count_ = 0;  // 恢复如初，注意把 META DATA 也要恢复！
    page->is_dirty_ = false;
    page->page_id_ = INVALID_PAGE_ID;
  }

  /** Number of pages in the buffer pool. */
  const size_t pool_size_;
  /** The next page id to be allocated  */
  std::atomic<page_id_t> next_page_id_ = 0;
  /** Bucket size for the extendible hash table */
  const size_t bucket_size_ = 4;

  /** Array of buffer pool pages. 它是索引就是 frame_id. 里面的 Page 维护了这个页的各种状态，以及 page 包含的数据 */
  Page *pages_;
  /** Pointer to the disk manager. */
  DiskManager *disk_manager_ __attribute__((__unused__));
  /** Pointer to the log manager. Please ignore this for P1. */
  LogManager *log_manager_ __attribute__((__unused__));
  /** Page table for keeping track of buffer pool pages. [page_id --> frame_id] */
  ExtendibleHashTable<page_id_t, frame_id_t> *page_table_;
  /** Replacer to find unpinned pages for replacement. */
  LRUKReplacer *replacer_;
  /** List of free frames that don't have any pages on them. frame_id 是从 0 开始的，
   * 比如 pool_size_ 是 5，则 frame_id 是 0, 1, 2, 3, 4 */
  std::list<frame_id_t> free_list_;
  /** This latch protects shared data structures. We recommend updating this comment to describe what it protects. */
  std::mutex latch_;

  /**
   * @brief Allocate a page on disk. Caller should acquire the latch before calling this function.
   * @return the id of the allocated page
   */
  auto AllocatePage() -> page_id_t;

  /**
   * @brief Deallocate a page on disk. Caller should acquire the latch before calling this function.
   * @param page_id id of the page to deallocate
   */
  void DeallocatePage(__attribute__((unused)) page_id_t page_id) {
    // This is a no-nop right now without a more complex data structure to track deallocated pages
  }
};
}  // namespace bustub
