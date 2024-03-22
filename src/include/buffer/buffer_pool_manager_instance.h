#pragma once

#include <list>
#include <mutex>  // NOLINT
#include <unordered_map>
#include <utility>

#include "buffer/buffer_pool_manager.h"
#include "buffer/lru_k_replacer.h"
#include "common/config.h"
#include "container/hash/extendible_hash_table.h"
#include "recovery/log_manager.h"
#include "storage/disk/disk_manager.h"
#include "storage/page/page.h"

namespace bustub {
class BasicPageGuard;
class ReadPageGuard;
class WritePageGuard;
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
  auto FetchPageBasic(page_id_t page_id) -> BasicPageGuard;
  auto FetchPageRead(page_id_t page_id) -> ReadPageGuard;
  auto FetchPageWrite(page_id_t page_id) -> WritePageGuard;

  /**
   * @brief PageGuard wrapper for NewPage
   *
   * Functionality should be the same as NewPage, except that
   * instead of returning a pointer to a page, you return a
   * BasicPageGuard structure.
   *
   * @param[out] page_id, the id of the new page
   * @return BasicPageGuard holding a new page
   * @note 包装器，用于实现自动 unpin
   */
  auto NewPageGuarded(page_id_t *page_id) -> BasicPageGuard;
  auto NewWritePageGuarded(page_id_t *page_id) -> WritePageGuard;

  /**
   * 反映当前缓冲池空闲(可以驱逐 + free frame)
  */
  auto GetAvailableSize() -> int;

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
   * @note 由于这个 page 是要给其它线程写入的，不能一边写入，一边驱逐，所以必须 pin
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
   * @note 提示：fetch 到的 page 是用于给其它线程读取的，所以必须 pin，不能在读取的过程中被 evict 了
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
   * 在 pages_ 数组 中寻找 page_id 对应的页
   * @return 如果成功找到这个 page，就返回地址；其它情况返回 nullptr
   */
  auto FindPage(page_id_t page_id, frame_id_t &frame_id) -> Page *;

  /**
   * 为一个 page 分配新 frame
   * @note 这个函数不会向磁盘发起读取或者写入，它仅用来给 page 分配 frame
   * @param new_page 是否是添加新的空白页(用于写入)
   * @param page_id 如果是新的空白页，那么会生成新的 page_id , 储存到 page_id 中；如果 new_page 是 false，那么 page_id
   * 就是传入的 page_id
   * @return 分配的 frame 的 page 指针。在无法分配 frame 的情况下，返回 nullptr
   */
  auto AllocateFrameForPage(bool new_page, page_id_t *page_id, frame_id_t &allocated_frame) -> Page *;

  /**
   * 用来固定某个 page，
   * ① page pin_count_ 计数 + 1
   * ② LRUK 替换策略管理器 记录一次访问
   * ③ pin_count > 0 设置为不可驱逐
   */
  void PinPage(Page *page, frame_id_t frame_id);

  /**
   * 把一个 page 的 data 及其所有元数据都完全清空。
   * @note 本质上是在清空 buffer_pool_manager_ 的一个 frame
   */
  void ClearPage(Page *page);

  friend class BasicPageGuard;
  /** Number of pages in the buffer pool. */
  const size_t pool_size_;
  /** The next page id to be allocated  */
  std::atomic<page_id_t> next_page_id_ = 0;
  /** Bucket size for the extendible hash table */
  const size_t bucket_size_ = 4;

  /** Array of buffer pool pages. 它是索引就是 frame_id. 里面的 Page 维护了这个页的各种状态，以及 page 包含的数据 */
  Page *pages_;
  /** Pointer to the disk manager. */
  DiskManager *disk_manager_;
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
  std::recursive_mutex latch_;

  /**
   * @brief Allocate a page on disk. Caller should acquire the latch before calling this function.
   * @return the id of the allocated page
   */
  auto AllocatePage() -> page_id_t;

  /**
   * @brief Deallocate a page on disk. Caller should acquire the latch before calling this function.
   * @param page_id id of the page to deallocate
   */
  void DeallocatePage(page_id_t page_id) {
    // This is a no-nop right now without a more complex data structure to track deallocated pages
  }
};

/**
 * 这个类很重要，其意义在于实现 pin 的 RAII 管理，降低 B+ Tree 索引工程的难度
 */
class BasicPageGuard {
 public:
  BasicPageGuard() {
    this->bpm_ = nullptr;
    this->page_ = nullptr;
    this->is_dirty_ = false;
  }

  BasicPageGuard(BufferPoolManagerInstance *bpm, Page *page) : bpm_(bpm), page_(page) {}
  // 所有的拷贝构造都被禁用了
  BasicPageGuard(const BasicPageGuard &) = delete;
  auto operator=(const BasicPageGuard &) -> BasicPageGuard & = delete;

  /**
   * 所有成员变量恢复初始的默认状态
   */
  void ClearMembers() {
    this->bpm_ = nullptr;
    this->page_ = nullptr;
    this->is_dirty_ = false;
  }

  /**
   * @brief Move constructor for BasicPageGuard
   *
   * When you call BasicPageGuard(std::move(other_guard)), you
   * expect that the new guard will behave exactly like the other
   * one. In addition, the old page guard should not be usable. For
   * example, it should not be possible to call .Drop() on both page
   * guards and have the pin count decrease by 2.
   * @note 转移对象所有权，传入的右值对象指针直接清零即可
   */
  BasicPageGuard(BasicPageGuard &&that) noexcept;

  /**
   * @brief Drop a page guard
   *
   * Dropping a page guard should clear all contents
   * (so that the page guard is no longer useful), and
   * it should tell the BPM that we are done using this page,
   * per the specification in the writeup.
   * @note 使得对应 page 的 pin 减去 1
   */
  void Drop();

  /**
   * @brief Move assignment for BasicPageGuard
   *
   * Similar to a move constructor, except that the move
   * assignment assumes that BasicPageGuard already has a page
   * being guarded. Think carefully about what should happen when
   * a guard replaces its held page with a different one, given
   * the purpose of a page guard.
   */
  auto operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard &;

  /**
   * @brief Destructor for BasicPageGuard
   *
   * When a page guard goes out of scope, it should behave as if
   * the page guard was dropped.
   */
  ~BasicPageGuard();

  /**
   * @brief Upgrade a BasicPageGuard to a ReadPageGuard
   *
   * The protected page is not evicted from the buffer pool during the upgrade,
   * and the basic page guard should be made invalid after calling this function.
   * @note 升级过程会给 page 加上读锁，在升级之前，这个 page 就已经 pinned 了
   * @return an upgraded ReadPageGuard
   */
  auto UpgradeRead() -> ReadPageGuard;

  /**
   * @brief Upgrade a BasicPageGuard to a WritePageGuard
   *
   * The protected page is not evicted from the buffer pool during the upgrade,
   * and the basic page guard should be made invalid after calling this function.
   * @note 升级过程会给 page 加上写锁，在升级之前，这个 page 就已经 pinned 了
   * @return an upgraded WritePageGuard
   */
  auto UpgradeWrite() -> WritePageGuard;

  /**
   * page_ 是 null 的时候，返回 -1 (INVALID_PAGE_ID)
   */
  auto PageId() -> page_id_t;

  auto PagePinCount() -> int;

  /**
   * 当 page 是 null 的时候，返回 nullptr
   */
  auto GetData() -> char * { return page_ == nullptr ? nullptr : page_->GetData(); }

  auto IsClear() -> bool { return page_ == nullptr && bpm_ == nullptr && !is_dirty_; }

  /**
   * 空间已经分配好了，获取 page
   */
  template <class T>
  auto As() -> T * {
    return reinterpret_cast<T *>(GetData());
  }

  /**
   * 获取数据，然后将这个 page 标记为脏
   */
  auto GetDataMut() -> char * {
    is_dirty_ = true;
    return page_->GetData();
  }

  /**
   * 如果你准备写入 page，就可以调用这个功能
   */
  template <class T>
  auto AsMut() -> T * {
    return reinterpret_cast<T *>(GetDataMut());
  }

 private:
  friend class ReadPageGuard;
  friend class WritePageGuard;

  BufferPoolManagerInstance *bpm_{nullptr};
  Page *page_{nullptr};
  bool is_dirty_{false};
};

class ReadPageGuard {
 public:
  ReadPageGuard() = default;
  ReadPageGuard(BufferPoolManagerInstance *bpm, Page *page);
  ReadPageGuard(const ReadPageGuard &) = delete;
  auto operator=(const ReadPageGuard &) -> ReadPageGuard & = delete;

  /**
   * 补充移动构造函数
   */
  explicit ReadPageGuard(BasicPageGuard &&that) noexcept : guard_{std::move(that)} {
    if (this->guard_.page_ != nullptr) {
      //! \bug 只有通过 basic 构造(升级)才会加上锁，其它情况不加锁
      this->guard_.page_->RLatch();  // 加上读锁
    }
  }
  /**
   * @brief Move constructor for ReadPageGuard
   * @note 通过其它 page_guard (非 basic) 应该维护锁的状态保持原样
   *
   * Very similar to BasicPageGuard. You want to create
   * a ReadPageGuard using another ReadPageGuard.
   */
  ReadPageGuard(ReadPageGuard &&that) noexcept : guard_{std::move(that.guard_)} {}

  /**
   * @brief Move assignment for ReadPageGuard
   *
   * Very similar to BasicPageGuard. Given another ReadPageGuard,
   * replace the contents of this one with that one.
   */
  auto operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
    if (this == &that) {
      return *this;
    }
    Drop();                           // 先放弃当前自己的资源
    guard_ = std::move(that.guard_);  // 等号赋值转移所有权，这可以保证锁的状态是不变的。
    return *this;
  }

  /**
   * @brief Drop a ReadPageGuard
   *
   * ReadPageGuard's Drop should behave similarly to BasicPageGuard,
   * except that ReadPageGuard has an additional resource - the latch!
   * However, you should think VERY carefully about in which order you
   * want to release these resources.
   * @note 根据《数据库系统概念》中的提示，应该先释放锁，再 unpin page
   * “再一个块执行任何操作之前，进程必须钉住这个块，随后获得封锁，必须再对块解除钉住之前释放封锁”
   */
  void Drop() {
    //! \bug 一个锁不能释放两次！
    if (this->guard_.IsClear()) {
      return;
    }
    if (this->guard_.page_ != nullptr) {
      this->guard_.page_->RUnlatch();  // 先释放读锁
    }
    this->guard_.Drop();  // unpin_page
  }

  /**
   * @brief Destructor for ReadPageGuard
   *
   * Just like with BasicPageGuard, this should behave
   * as if you were dropping the guard.
   */
  ~ReadPageGuard() { Drop(); }

  auto PageId() -> page_id_t { return guard_.PageId(); }

  auto GetData() -> const char * { return guard_.GetData(); }

  template <class T>
  auto As() -> T * {
    return guard_.As<T>();
  }

 private:
  BasicPageGuard guard_;
};

class WritePageGuard {
 public:
  WritePageGuard() = default;
  WritePageGuard(BufferPoolManagerInstance *bpm, Page *page);
  WritePageGuard(const WritePageGuard &) = delete;
  auto operator=(const WritePageGuard &) -> WritePageGuard & = delete;

  explicit WritePageGuard(BasicPageGuard &&that) noexcept : guard_{std::move(that)} {
    //! \bug 只有通过 basic 构造(升级)才会加上锁，其它情况不加锁
    if (this->guard_.page_ != nullptr) {
      this->guard_.page_->WLatch();  // 加上写锁
      // std::cout << this->guard_.page_->GetPageId() << " 写锁已经获取" << std::endl;
      this->guard_.is_dirty_ = true;  // 页被写脏了
    }
  }

  /**
   * @brief Move constructor for WritePageGuard
   * @note 通过其它 page_guard (非 basic) 应该维护锁的状态保持原样
   * Very similar to BasicPageGuard. You want to create
   * a WritePageGuard using another WritePageGuard.
   */
  WritePageGuard(WritePageGuard &&that) noexcept : guard_{std::move(that.guard_)} {}

  /**
   * @brief Move assignment for WritePageGuard
   *
   * Very similar to BasicPageGuard. Given another WritePageGuard,
   * replace the contents of this one with that one.
   * @bug 对面的资源进来了，自己原来的锁是不是要释放掉？应该先释放内部资源
   */
  auto operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
    if (this == &that) {
      return *this;
    }
    Drop();  // 先释放自己内部的资源[注意：互斥锁一次只能获得一个]
    guard_ = std::move(that.guard_);
    return *this;
  }

  /**
   * @brief Drop a WritePageGuard
   *
   * WritePageGuard's Drop should behave similarly to BasicPageGuard,
   * except that WritePageGuard has an additional resource - the latch!
   * However, you should think VERY carefully about in which order you
   * want to release these resources.
   */
  void Drop() {
    //! \bug 一个锁不能释放两次！
    if (this->guard_.IsClear()) {
      return;
    }
    if (this->guard_.page_ != nullptr) {
      this->guard_.page_->WUnlatch();  // 先释放写锁
    }
    this->guard_.Drop();  // unpin_page
  }

  /**
   * @brief Destructor for WritePageGuard
   *
   * Just like with BasicPageGuard, this should behave
   * as if you were dropping the guard.
   */
  ~WritePageGuard() { Drop(); }

  auto PageId() -> page_id_t { return guard_.PageId(); }

  auto GetData() -> const char * { return guard_.GetData(); }

  template <class T>
  auto As() -> const T * {
    return guard_.As<T>();
  }

  auto GetDataMut() -> char * { return guard_.GetDataMut(); }

  template <class T>
  auto AsMut() -> T * {
    return guard_.AsMut<T>();
  }

 private:
  BasicPageGuard guard_;
};

}  // namespace bustub
