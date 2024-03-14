#include "buffer/buffer_pool_manager_instance.h"

#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  std::lock_guard<std::recursive_mutex> guard(latch_);
  frame_id_t allocated_frame{};
  return AllocateFrameForPage(true, page_id, allocated_frame);
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  Page *page{nullptr};
  frame_id_t allocated_frame{};
  // 缩小锁的范围
  std::unique_lock<std::recursive_mutex> guard(latch_);
  frame_id_t frame_id{};
  page = FindPage(page_id, frame_id);
  if (page != nullptr) {
    PinPage(page, frame_id);
    return page;  // 如果找到 page，返回指针
  }
  page = AllocateFrameForPage(false, &page_id, allocated_frame);
  if (page == nullptr) {
    return page;  // 分配 frame 失败，返回 nullptr
  }
  disk_manager_->ReadPage(page_id, page->GetData());  // 将数据从磁盘中读入
  return page;
}

auto BufferPoolManagerInstance::AllocateFrameForPage(bool new_page, page_id_t *page_id, frame_id_t &allocated_frame)
    -> Page * {
  if (!free_list_.empty()) {
    allocated_frame = free_list_.front();
    free_list_.pop_front();
  } else {  // free list 没有空闲空间，则需要淘汰一些页面了
    bool evict_success = replacer_->Evict(&allocated_frame);
    if (!evict_success) {
      return nullptr;
    }
  }
  // 当获得了合适的 frame_id 之后，就可以分配内存了
  Page *page = &pages_[allocated_frame];
  if (page == nullptr) {
    return nullptr;
  }
  if (page->IsDirty()) {  // 将旧页面写回磁盘。
    UnsafeFlushPgImp(page->GetPageId());
  }
  page_table_->Remove(page->GetPageId());  // 你应该将它从 page_table 的映射关系移除
  ClaerPage(page);
  if (new_page) {
    *page_id = AllocatePage();  // 如果是新页，分配新的 page_id，否则沿用原来的 page_id
  }
  page->page_id_ = *page_id;  // 给新的页分配 page_id
  page_table_->Insert(
      *page_id,
      allocated_frame);  // 在 page_table_ 中记录 page_id --> frame_id 这一对映射关系
                         // pin 计数 + 1，并且禁止淘汰该页面，因为有线程要读取 或者 写入 这个 page，它不能被淘汰
  PinPage(page, allocated_frame);
  return page;
}

void BufferPoolManagerInstance::PinPage(Page *page, frame_id_t frame_id) {
  page->pin_count_++;  // 添加 pin
  //! \note  pin frame [易出错操作]
  replacer_->RecordAccess(frame_id);         // 记录一个对该 frame 的访问
  replacer_->SetEvictable(frame_id, false);  // 设置为不可驱逐
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::lock_guard<std::recursive_mutex> guard(latch_);
  frame_id_t frame_id{};
  Page *page{FindPage(page_id, frame_id)};
  if (page == nullptr) {
    return false;
  }
  if (page->GetPinCount() <= 0) {
    return false;
  }
  if (--page->pin_count_ == 0) {              // 如果 pin_count 恰好减为 0
    replacer_->SetEvictable(frame_id, true);  // 将对应的 frame_id 设置为可驱逐
  }
  //! \bug 理解这个参数：如果这个 page 是脏的，则 is_dirty 是 true
  if (is_dirty) {
    page->is_dirty_ = is_dirty;
  }
  return true;
}

auto BufferPoolManagerInstance::UnsafeFlushPgImp(page_id_t page_id) -> bool {
  // 特殊情况：page_id 是无效的(-1)
  BUSTUB_ASSERT(page_id != INVALID_PAGE_ID, "Invalid Page id.");
  frame_id_t frame_id{};
  Page *page{FindPage(page_id, frame_id)};
  if (page == nullptr) {
    return false;  // 如果找不到对应的 page，则返回 false
  }
  page->is_dirty_ = false;
  disk_manager_->WritePage(page_id, page->GetData());
  return true;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  std::lock_guard<std::recursive_mutex> guard(latch_);
  return UnsafeFlushPgImp(page_id);
}

auto BufferPoolManagerInstance::FindPage(page_id_t page_id, frame_id_t &frame_id) -> Page * {
  bool found_page = page_table_->Find(page_id, frame_id);  // 找到 frame_id
  if (found_page) {
    return &pages_[frame_id];  // 输出遍历：这个 page 的地址指针
  }
  return nullptr;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  std::lock_guard<std::recursive_mutex> guard(latch_);
  // 遍历 page_ 数组，如果某个页非空闲[页有效]，那么就把它写回自盘
  for (size_t i = 0; i < pool_size_; ++i) {
    Page *page = &pages_[i];
    if (page->page_id_ == INVALID_PAGE_ID) {
      continue;
    }                                     // 忽略无效页
    UnsafeFlushPgImp(page->GetPageId());  // 否则，将页面写回磁盘
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  std::lock_guard<std::recursive_mutex> guard(latch_);
  frame_id_t frame_id{};
  bool found_page = page_table_->Find(page_id, frame_id);  // 找到 frame_id
  if (!found_page) {
    return true;
  }
  Page *page = &pages_[frame_id];
  if (page->GetPinCount() > 0) {
    return false;
  }                                // pin count > 0，无法被移除
  replacer_->Remove(frame_id);     // 将对应的 frame_id 移除
  page_table_->Remove(page_id);    // 从页表中移除映射关系
  free_list_.push_back(frame_id);  // 恢复空闲链表
  ClaerPage(page);                 // page 清空
  DeallocatePage(page_id);         // 这是一个“模拟”的功能，它假设释放了磁盘上的一片内存
  return true;
}

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
  if (this == &that) {
    return;
  }  // 避免自我构造
  this->bpm_ = that.bpm_;
  this->page_ = that.page_;
  this->is_dirty_ = that.is_dirty_;
  that.ClearMembers();
}

void BasicPageGuard::Drop() {
  if (this->bpm_ == nullptr || this->page_ == nullptr) {
    return;
  }
  // 通知管理器 pin - 1
  bpm_->UnpinPgImp(page_->GetPageId(), is_dirty_);
  // 将该对象恢复默认状态
  this->ClearMembers();
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

/**
 * page_guard 代码区
 */
auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  if (this == &that) {
    return *this;
  }              // 避免自我构造
  this->Drop();  // 自己这个页放弃管理
  this->bpm_ = that.bpm_;
  this->page_ = that.page_;
  this->is_dirty_ = that.is_dirty_;
  that.ClearMembers();
  return *this;
}

BasicPageGuard::~BasicPageGuard() { Drop(); };  // NOLINT

auto BasicPageGuard::UpgradeRead() -> ReadPageGuard { return ReadPageGuard(std::move(*this)); }

auto BasicPageGuard::UpgradeWrite() -> WritePageGuard {
  return WritePageGuard(std::move(*this));  // 升级
}

/**
 * 组合 page_guard 形成的包装方法区
 */
auto BufferPoolManagerInstance::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard {
  std::lock_guard<std::recursive_mutex> guard(latch_);
  return BasicPageGuard(this, NewPgImp(page_id));
}

auto BufferPoolManagerInstance::FetchPageBasic(page_id_t page_id) -> BasicPageGuard {
  std::lock_guard<std::recursive_mutex> guard(latch_);
  return BasicPageGuard(this, FetchPgImp(page_id));
}

auto BufferPoolManagerInstance::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  std::lock_guard<std::recursive_mutex> guard(latch_);
  return BasicPageGuard(this, FetchPgImp(page_id)).UpgradeRead();
}

auto BufferPoolManagerInstance::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  std::lock_guard<std::recursive_mutex> guard(latch_);
  return BasicPageGuard(this, FetchPgImp(page_id)).UpgradeWrite();
}

}  // namespace bustub
