#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {
//! \note 加锁和unpin是每个线程内部的事情，应该使用线程局部变量
// 用于记录 page_guard 序列，便于及时释放。递归每加一层，就添加一个元素。[用于 Insert 函数]
thread_local std::vector<WritePageGuard *> guard_queue{};
// 用于记录 page_guard 序列，便于及时释放。递归每加一层，就添加一个元素。[用于 Remove 函数]
thread_local std::vector<WritePageGuard *> remove_guard_queue{};

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(
          dynamic_cast<BufferPoolManagerInstance *>(buffer_pool_manager)),  // 使用动态类型转换强转为 bpml
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {
        page_id_t temp_id{};
        root_guard_ = buffer_pool_manager->NewWritePageGuarded(temp_id); 
      }

/**
 * Helper function to decide whether current b+tree is empty
 * @note 当根结点 id 是 无效值 的时候，说明这个 B+ 树是空的
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return root_page_id_ == INVALID_PAGE_ID; }

/**
 * 初始化根结点 (Create Root Page，并且最开始根就是叶子结点)，但是不刷回磁盘
 * 提示：这个项目的假设 ROOT_PAGE 的 ID 固定 是0，所以直接 FETCH 就好
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InitializeRoot() -> WritePageGuard {
  // std::lock_guard<std::recursive_mutex> guard(latch_);
  root_page_id_ = HEADER_PAGE_ID;
  return buffer_pool_manager_->FetchPageWrite(HEADER_PAGE_ID);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::GuardDrop(std::vector<WritePageGuard *> &guard_queue) {
  size_t n{guard_queue.size()};
  for (size_t i = 0; i < n - 1; ++i) {
    guard_queue[i]->Drop();
  }
  std::swap(guard_queue[n - 1], guard_queue[0]);
  guard_queue.resize(1);
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::SearchLeaf(const KeyType &key, LeafPage *page) -> int {
  if (page == nullptr) {
    return -1;
  }
  int key_num{page->GetKeyNum()};
  if (key_num == 0) {
    return -1;
  }
  // 使用二分查找，注意：叶子结点的下标从 0 开始
  int left{0};
  int right{key_num - 1};
  while (left <= right) {
    int mid{(right - left) / 2 + left};
    int compare_res{comparator_(page->KeyAt(mid), key)};
    if (compare_res == 0) {
      return mid;
    }
    if (compare_res > 0) {
      right = mid - 1;
    } else {
      left = mid + 1;
    }
  }
  return -1;  // 查找失败，返回 -1
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::SearchLeafInsert(const KeyType &key, LeafPage *page) -> int {
  if (page == nullptr) {
    return -1;
  }
  int key_num{page->GetKeyNum()};
  if (key_num == 0) {
    return -1;
  }
  // 使用二分查找，注意：叶子结点的下标从 0 开始
  int left{0};
  int right{key_num - 1};
  while (left <= right) {
    int mid{(right - left) / 2 + left};
    int compare_res{comparator_(page->KeyAt(mid), key)};
    if (compare_res >= 0) {
      if (mid == 0 || comparator_(page->KeyAt(mid - 1), key) < 0) {
        return mid;
      }
      right = mid - 1;
    } else {
      left = mid + 1;
    }
  }
  return key_num;  // 如果查找失败，说明所有元素都 < key，即 key 应该插到最后一个
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::SearchInternalAccuracy(const KeyType &key, InternalPage *page) -> int {
  int key_num{page->GetKeyNum()};
  // 寻找第一个 >= key 的 index
  int left{1};
  int right{key_num};
  while (left <= right) {
    int mid{(right - left) / 2 + left};
    int compare_res{comparator_(page->KeyAt(mid), key)};
    if (compare_res == 0) {
      return mid;
    }
    if (compare_res > 0) {
      right = mid - 1;
    } else {
      left = mid + 1;
    }
  }
  return -1;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::SearchInternal(const KeyType &key, InternalPage *page) -> int {
  int key_num{page->GetKeyNum()};
  // 寻找第一个 >= key 的 index
  int left{1};
  int right{key_num};
  while (left <= right) {
    int mid{(right - left) / 2 + left};
    int compare_res{comparator_(page->KeyAt(mid), key)};
    if (compare_res >= 0) {
      if (mid == 1 || comparator_(page->KeyAt(mid - 1), key) < 0) {
        return mid;
      }
      right = mid - 1;
    } else {
      left = mid + 1;
    }
  }
  // 此时，我们找到了合适的索引。如果查找失败，则指针应该是最左侧指针，恰为 target_idx 的初始值 0
  return 0;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::SearchInternalFind(const KeyType &key, InternalPage *page) -> int {
  if (page == nullptr) {
    return -1;
  }  // 如果 page 不存在，返回 -1
  int key_num{page->GetKeyNum()};
  // 寻找最后一个 <= key 的 index，如果找不到，就返回0，表示所有元素都大于 key，说明应该在最左侧寻找
  int left{1};
  int right{key_num};
  while (left <= right) {
    int mid{(right - left) / 2 + left};
    int compare_res{comparator_(page->KeyAt(mid), key)};
    if (compare_res <= 0) {
      if (mid == key_num || comparator_(page->KeyAt(mid + 1), key) > 0) {
        return mid;
      }
      left = mid + 1;
    } else {
      right = mid - 1;
    }
  }
  // 此时，我们找到了合适的索引。如果查找失败，则指针应该是最左侧指针，恰为 target_idx 的初始值 0
  return 0;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::SearchBPlusTree(const KeyType &key, page_id_t page_id, ReadPageGuard &parent_guard)
    -> std::optional<ValueType> {
  ReadPageGuard page_guard = buffer_pool_manager_->FetchPageRead(page_id);
  if (page_guard.PageId() == INVALID_PAGE_ID) {
    return std::nullopt;
  }
  parent_guard.Drop();  // 立即释放双亲结点的锁和 UNPIN
  // 根结点不存在的情况，无法找到任何页
  BPlusTreePage *page{PageFromGuard<BPlusTreePage>(page_guard)};
  if (page == nullptr) {
    return std::nullopt;
  }
  if (page->IsLeafPage()) {
    LeafPage *leaf_page{PageFromGuard<LeafPage>(page_guard)};
    int find_idx{SearchLeaf(key, leaf_page)};
    if (find_idx == -1) {
      return std::nullopt;
    }
    if (leaf_page == nullptr) {
      return std::nullopt;
    }
    return std::optional<ValueType>{leaf_page->ValueAt(find_idx)};
  }
  // 现在这个 page 是非叶子结点，所以需要查找合适的指向下一步的指针
  InternalPage *internal_page{PageFromGuard<InternalPage>(page_guard)};
  int target_idx{SearchInternalFind(key, internal_page)};
  if (internal_page == nullptr) {
    return std::nullopt;
  }
  return SearchBPlusTree(key, internal_page->ValueAt(target_idx), page_guard);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::SearchTargetLeaf(const KeyType &key, page_id_t page_id, ReadPageGuard &parent_guard)
    -> std::optional<page_id_t> {
  ReadPageGuard page_guard = buffer_pool_manager_->FetchPageRead(page_id);
  if (page_guard.PageId() == INVALID_PAGE_ID) {
    return std::nullopt;
  }
  parent_guard.Drop();  // 立即释放双亲结点的锁和 UNPIN
  // 根结点不存在的情况，无法找到任何页
  BPlusTreePage *page{PageFromGuard<BPlusTreePage>(page_guard)};
  if (page == nullptr) {
    return std::nullopt;
  }
  if (page->IsLeafPage()) {
    LeafPage *leaf_page{PageFromGuard<LeafPage>(page_guard)};
    int find_idx{SearchLeaf(key, leaf_page)};
    if (find_idx == -1) {
      return std::nullopt;
    }
    if (leaf_page == nullptr) {
      return std::nullopt;
    }
    return std::optional<page_id_t>{leaf_page->GetPageId()};
  }
  // 现在这个 page 是非叶子结点，所以需要查找合适的指向下一步的指针
  InternalPage *internal_page{PageFromGuard<InternalPage>(page_guard)};
  int target_idx{SearchInternalFind(key, internal_page)};
  if (internal_page == nullptr) {
    return std::nullopt;
  }
  return SearchTargetLeaf(key, internal_page->ValueAt(target_idx), page_guard);
}

/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  std::lock_guard<std::recursive_mutex> guard{latch_};
  ReadPageGuard dummy_guard{};
  std::optional<ValueType> value{SearchBPlusTree(key, root_page_id_, dummy_guard)};
  if (!value.has_value()) {
    return false;
  }
  result->push_back(value.value());  // 将返回值添加到结果
  return true;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertLeaf(const KeyType &key, const ValueType &value, LeafPage *page) -> void {
  int key_num{page->GetKeyNum()};
  if (key_num == 0) {  // 初始空的情况
    MappingType *leaf_array = page->GetArray();
    leaf_array[0] = MappingType(key, value);
    page->IncreaseSize(1);  // 注意元素个数 + 1
    return;
  }
  // 使用二分查找，寻找插入位置，即第一个 > k 的元素的索引，注意：叶子结点的下标从 0 开始
  int left{0};
  int right{key_num - 1};
  int insert_idx{key_num};  // 如果没有找到，说明所有元素皆小于 k，直接设置为 key_num 即可(溢出一个)
  while (left <= right) {
    int mid{(right - left) / 2 + left};
    int compare_res{comparator_(page->KeyAt(mid), key)};
    if (compare_res > 0) {
      if (mid == 0 || comparator_(page->KeyAt(mid - 1), key) <= 0) {
        insert_idx = mid;
        break;
      }
      right = mid - 1;
    } else {
      left = mid + 1;
    }
  }
  MappingType *leaf_array = page->GetArray();
  for (int i = key_num; i > insert_idx; --i) {
    leaf_array[i] = leaf_array[i - 1];
  }
  leaf_array[insert_idx] = MappingType(key, value);
  page->IncreaseSize(1);  // 元素个数 + 1
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::SplitLeaf(LeafPage *old_page, LeafPage *new_page, MappingType &inserting_pair) -> void {
  int leave_num{static_cast<int>(std::ceil(old_page->GetMaxSize() / 2))};  // 留在原来页的元素个数
  int n{old_page->GetSize()};                                              // 元素个数
  MappingType *new_array = new_page->GetArray();
  MappingType *old_array = old_page->GetArray();
  MappingType overflow_pair{};  // 使用溢出 pair 在 old_page 上进行“模拟”插入
  int insert_idx{SearchLeafInsert(inserting_pair.first, old_page)};
  if (insert_idx == n) {
    overflow_pair = inserting_pair;
  } else {
    // 假装插入这个元素
    for (int i = n; i > insert_idx; --i) {
      if (i == n) {
        overflow_pair = old_array[i - 1];
      } else {
        old_array[i] = old_array[i - 1];
      }
    }
    old_array[insert_idx] = inserting_pair;
  }
  // 假装 size + 1
  old_page->IncreaseSize(1);
  int size_change{n + 1 - leave_num};
  for (int i = 0; i < size_change; ++i) {
    if (i + leave_num == n) {
      new_array[i] = overflow_pair;
    } else {
      new_array[i] = old_array[i + leave_num];
    }
  }
  old_page->IncreaseSize(-size_change);
  new_page->IncreaseSize(size_change);
  new_page->SetNextPageId(old_page->GetNextPageId());
  old_page->SetNextPageId(new_page->GetPageId());
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertInternalPage(std::optional<page_id_t> old_page_id, const KeyType &key,
                                        const page_id_t &new_page_id, InternalPage *internal_page) -> bool {
  if (internal_page == nullptr) {
    return false;
  }
  // 结点已经满了，禁止插入
  if (internal_page->IsFull()) {
    return false;
  }
  InternalPair *internal_array = internal_page->GetArray();
  // 特殊情况：如果内部页是新成立的
  if (internal_page->GetKeyNum() == 0) {
    internal_array[1] = InternalPair{key, new_page_id};
    internal_array[0].second = old_page_id.value_or(INVALID_PAGE_ID);  // 内部结点最左侧指针
    internal_page->IncreaseSize(1);
    return true;
  }
  int insert_idx{SearchInternal(key, internal_page)};
  int n{internal_page->GetKeyNum()};  // 注意：key 的索引从1开始，如果有n个，则索引是 {1, 2, ..., n}
  // 如果搜到的结果是0，则要插入到最后
  insert_idx = (insert_idx == 0 ? n + 1 : insert_idx);
  for (int i = n + 1; i > insert_idx; --i) {
    internal_array[i] = internal_array[i - 1];
  }
  internal_array[insert_idx] = InternalPair{key, new_page_id};
  internal_page->IncreaseSize(1);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::SplitInternal(InternalPage *old_page, InternalPage *new_page, InternalPair &inserting_pair)
    -> KeyType {
  // 结点被分为三个部分 [1,...,ceil((n+1) / 2) - 1] [ceil((n+1) / 2)] [ceil((n+1) / 2) + 1, .. end]
  int n{old_page->GetKeyNum()};  // 注意索引从 1 开始
  InternalPair overflow_pair;    // 准备一个溢出键值对，用以表示 array[n]
  int insert_idx{SearchInternal(inserting_pair.first, old_page)};
  insert_idx = (insert_idx == 0 ? n + 1 : insert_idx);
  InternalPair *old_array = old_page->GetArray();
  InternalPair *new_array = new_page->GetArray();
  // 将新元素插入到 old_page (准备了一个溢出变量，盛放溢出元素)
  if (insert_idx == n + 1) {
    overflow_pair = inserting_pair;
  } else {
    for (int i = n + 1; i > insert_idx; --i) {
      if (i == n + 1) {
        overflow_pair = old_array[i - 1];
      } else {
        old_array[i] = old_array[i - 1];
      }
    }
    old_array[insert_idx] = inserting_pair;
  }
  old_page->IncreaseSize(1);  // 虚假地插入了一个元素
  // 分裂结点，注意索引从 1 开始，但幸运的是，内部结点 key 的索引也从 1 开始
  int split_idx = std::ceil((old_page->GetMaxSize() + 1) / 2);
  KeyType return_key = old_array[split_idx].first;
  int move_num{n + 1 - split_idx};  // 不包括分裂结点，其右侧的结点需要被移动到 new_page
  // 拷贝一部分元素到新的内部结点[索引从1开始]
  for (int i = 1; i <= move_num; ++i) {
    int old_idx{split_idx + i};
    if (old_idx == n + 1) {
      new_array[i] = overflow_pair;
    } else {
      new_array[i] = old_array[old_idx];
    }
  }
  // 将分裂结点的指针移动到新内部结点最左侧 <k, v> 的 v 中
  new_array[0].second = old_array[split_idx].second;
  // 调整两个数组的大小
  old_page->IncreaseSize(-move_num - 1);
  new_page->IncreaseSize(move_num);  // 由于内部结点初始大小就是1，所以这里添加 key 的增加个数即可
  // 注意：你还不能返回，因为移动到新 page 的子结点的 parent 需要被重新设定
  for (int i = 0; i < new_page->GetSize(); ++i) {
    page_id_t child_page_id{new_page->ValueAt(i)};
    WritePageGuard child_guard{buffer_pool_manager_->FetchPageWrite(child_page_id)};
    BPlusTreePage *child_page{PageFromGuard<BPlusTreePage>(child_guard)};
    //! \bug child_page->SetPageId ？ 这只会导致赋值混乱！不可以！应该设置的是子结点双亲的 id
    if (child_page != nullptr) {
      child_page->SetParentPageId(new_page->GetPageId());
    }
  }
  return return_key;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::NewRootInternalPage(WritePageGuard &page_guard, page_id_t page_id) -> InternalPage * {
  InternalPage *new_root_page{PageFromGuard<InternalPage>(page_guard)};
  if (new_root_page == nullptr) {
    return nullptr;
  }
  new_root_page->Init(page_id, INVALID_PAGE_ID, internal_max_size_);  // 初始化新的 Page
  root_page_id_ = page_id;
  return new_root_page;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, page_id_t page_id, page_id_t parent_page_id)
    -> BPLUSTREE_TYPE::InsertStatus {
  WritePageGuard page_guard{};
  if (page_id != INVALID_PAGE_ID) {
    page_guard = buffer_pool_manager_->FetchPageWrite(page_id);
    guard_queue.push_back(&page_guard);  // 给每个 page_guard 记录一个指针，从而访问该对象
  } else {
    return InsertStatus::FAILED_INSERT;
  }
  // BufferPoolTracer(key);
  BPlusTreePage *page{PageFromGuard<BPlusTreePage>(page_guard)};
  if (page == nullptr) {
    return InsertStatus::FAILED_INSERT;
  }
  if (page->IsLeafPage()) {
    LeafPage *leaf_page{PageFromGuard<LeafPage>(page_guard)};
    int find_idx{SearchLeaf(key, leaf_page)};
    if (find_idx != -1) {
      return InsertStatus::FAILED_INSERT;  // key 已经存在了，无需插入
    }
    if (leaf_page == nullptr) {
      return InsertStatus::FAILED_INSERT;
    }
    if (!leaf_page->IsFull()) {  // 叶子结点未满，可以插入
      // 叶子结点未满，则前面所有的锁皆可释放(DROP是安全的，只会释放一次)
      GuardDrop(guard_queue);
      InsertLeaf(key, value, leaf_page);
      return InsertStatus::SUCCESS_INSERT;
    }
    // 一个很复杂的情况：叶子结点分裂
    //! \bug 这种做法是危险的，不合理的，将会导致数组越界从而 page_id 发生改变，进而。所以你需要后插入
    // 现在叶子结点溢出了！，你需要将前 ceil(n / 2) 个值放在原来的表中，剩余值放在新叶子结点
    //! \bug InsertLeaf(key, value, leaf_page); 这种写法会导致数组越界，从而使得 Page 的 page_id_ 被溢出数据覆盖写入
    // page_id_ 被覆盖写入的后果就是：这个页无法被正确释放，永远留在了缓冲池中，导致剩余的缓冲池更为拥挤，带来链式反映
    MappingType inserting_pair{key, value};
    page_id_t new_page_id{};  // 产生新的页
    WritePageGuard new_page_guard = buffer_pool_manager_->NewWritePageGuarded(&new_page_id);

    // BufferPoolTracer(key);

    LeafPage *new_leaf_page{PageFromGuard<LeafPage>(new_page_guard)};
    if (new_leaf_page == nullptr) {
      return InsertStatus::FAILED_INSERT;
    }
    new_leaf_page->Init(new_page_id, parent_page_id, leaf_max_size_);  // 初始化新的 Page
    SplitLeaf(leaf_page, new_leaf_page, inserting_pair);               // 修改后的代码：杜绝数组溢出
    if (leaf_page->IsRootPage()) {  // 一个特别情况，叶子结点是根，此时需要生成新的根
      //! \note 对于更新 ROOT 的情况，或许需要加锁保护
      // std::lock_guard<std::recursive_mutex> guard{latch_};
      // std::cout << "线程：" << std::this_thread::get_id() << "执行到了更新ROOT" << std::endl;
      page_id_t new_root_id{};  // 产生新的 ROOT 页
      WritePageGuard new_root_guard = buffer_pool_manager_->NewWritePageGuarded(&new_root_id);
      InternalPage *new_root_page{NewRootInternalPage(new_root_guard, new_root_id)};

      // BufferPoolTracer(key);

      if (new_root_page == nullptr) {
        return InsertStatus::FAILED_INSERT;
      }
      InsertInternalPage(leaf_page->GetPageId(), new_leaf_page->KeyAt(0), new_leaf_page->GetPageId(), new_root_page);
      // 两个子结点指向新的 root
      leaf_page->SetParentPageId(root_page_id_);
      new_leaf_page->SetParentPageId(root_page_id_);
      return InsertStatus::LEAF_SPLIT_INSERT;
    }
    // 把键值对传参传出去。第一个元素的 first key 值是没用的。
    // 即：传参应该是：InsertInternalPage(splitted_leafs_[0].second,
    //                                   splitted_leafs_[1].first, splitted_leafs_[1].second)
    splitted_.clear();
    splitted_.push_back({leaf_page->KeyAt(0), leaf_page->GetPageId()});
    splitted_.push_back({new_leaf_page->KeyAt(0), new_leaf_page->GetPageId()});
    return InsertStatus::LEAF_SPLIT_INSERT;
  }

  // 现在这个 page 是非叶子结点，所以需要查找合适的指向下一步的指针
  InternalPage *internal_page{PageFromGuard<InternalPage>(page_guard)};
  // 提示：需要及时将页和锁从缓存池清出。插入时，如果这个内部结点不满，则其祖先是安全的。
  if (internal_page == nullptr) {
    return InsertStatus::FAILED_INSERT;
  }
  if (!internal_page->IsFull()) {  // 释放所有祖先结点的锁，并 unpin 它们
    GuardDrop(guard_queue);
  }
  int target_idx{SearchInternalFind(key, internal_page)};

  // 中间：代码的递归调用
  InsertStatus status = Insert(key, value, internal_page->ValueAt(target_idx), page_id);
  if (status == InsertStatus::SUCCESS_INSERT || status == InsertStatus::FAILED_INSERT) {
    return status;
  }

  // 两个元素分别是 key & new_page_id
  InternalPair inserting_pair{splitted_[1].first, splitted_[1].second};
  if (!internal_page->IsFull()) {
    InsertInternalPage(std::nullopt, inserting_pair.first, inserting_pair.second, internal_page);
    return InsertStatus::SUCCESS_INSERT;
  }  // 其它情况：该内部结点已经满了，无法插入新的元素了，此时需要一分为三：拆分内部结点
  page_id_t new_internal_page_id{};
  WritePageGuard new_internal_guard{buffer_pool_manager_->NewWritePageGuarded(&new_internal_page_id)};
  InternalPage *new_internal_page{PageFromGuard<InternalPage>(new_internal_guard)};
  if (new_internal_page == nullptr) {
    return InsertStatus::FAILED_INSERT;
  }
  new_internal_page->Init(new_internal_page_id, parent_page_id, internal_max_size_);
  // 需要拿上去的结点
  KeyType splitted_key{SplitInternal(internal_page, new_internal_page, inserting_pair)};
  if (internal_page->IsRootPage()) {  // 这个内部结点是根结点，那么需要创建新根
    //! \note 对于更新 ROOT 的情况，或许需要加锁保护
    // std::lock_guard<std::recursive_mutex> guard{latch_};
    page_id_t new_root_id{};  // 产生新的 ROOT 页
    WritePageGuard new_root_guard = buffer_pool_manager_->NewWritePageGuarded(&new_root_id);
    InternalPage *new_root_page{NewRootInternalPage(new_root_guard, new_root_id)};
    InsertInternalPage(internal_page->GetPageId(), splitted_key, new_internal_page->GetPageId(), new_root_page);
    internal_page->SetParentPageId(root_page_id_);
    new_internal_page->SetParentPageId(root_page_id_);
    return InsertStatus::INTERNAL_SPLIT_INSERT;  // 事实上，如果内部结点是根，则到此就直接返回出去了，没有递归栈了
  }
  splitted_.clear();  // 每次用完后，给上一层递归传参之前，清空这个数组，因为该分裂的已经分裂了
  // 与叶子结点是类似的，第一个元素的 key (first) 没有用处
  splitted_.push_back({internal_page->KeyAt(0), internal_page->GetPageId()});
  splitted_.push_back({splitted_key, new_internal_page->GetPageId()});
  return InsertStatus::INTERNAL_SPLIT_INSERT;
}

/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  std::lock_guard<std::recursive_mutex> guard{latch_};
  if (root_page_id_ == INVALID_PAGE_ID) {
    {
      std::lock_guard<std::recursive_mutex> latch_guard(latch_);
      if (root_page_id_ == INVALID_PAGE_ID) {  // 两阶段检查保证初始化的线程安全
        WritePageGuard root_guard{InitializeRoot()};  // 初始化根结点为叶子结点，并将 page_id 赋值给 root_page_id_
        auto *page = root_guard.AsMut<LeafPage>();
        page->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);
      }
    }
  }
  // 如果这个 key 已经在 B+ 树里面了，则无需再插入。
  ReadPageGuard dummy_guard{};
  if (SearchBPlusTree(key, root_page_id_, dummy_guard).has_value()) {
    return false;
  }
  // BufferPoolTracer(key);
  latch_.lock();
  // std::cout <<  "线程：" << std::this_thread::get_id() << " 插入元素计数：" << count++ << std::endl;
  latch_.unlock();
  // 否则执行复杂的插入过程，从根结点出发，开始执行插入
  Insert(key, value, root_page_id_, INVALID_PAGE_ID);
  // std::cout <<  "线程：" << std::this_thread::get_id() << " 插入元素成功：" << (count - 1) << std::endl;
  // 清空 guard_queue，因为这些变量将不再被使用，访问未知内存会带来风险
  guard_queue.clear();
  return true;  // 一般而言，这样总能插入成功
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertOneElem(MappingType &elem, int index, LeafPage *leaf_page) -> bool {
  MappingType *array{leaf_page->GetArray()};
  int n{leaf_page->GetKeyNum()};
  if (!(index >= 0 && index <= n)) {
    return false;
  }
  for (int i = n; i > index; --i) {
    array[i] = array[i - 1];
  }
  array[index] = elem;
  leaf_page->IncreaseSize(1);
  return true;
}
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::RemoveOneElem(MappingType &elem, int index, LeafPage *leaf_page) -> bool {
  MappingType *array{leaf_page->GetArray()};
  int n{leaf_page->GetKeyNum()};
  if (!(index >= 0 && index < n)) {
    return false;
  }
  elem = array[index];  // 保存被删除的这个元素
  for (int i = index; i < n - 1; ++i) {
    array[i] = array[i + 1];
  }
  leaf_page->IncreaseSize(-1);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertOneInternalElem(InternalPair &elem, int index, InternalPage *internal_page) -> bool {
  InternalPair *array{internal_page->GetArray()};
  int n{internal_page->GetKeyNum()};
  if (!(index >= 1 && index <= n + 1)) {
    return false;
  }
  for (int i = n + 1; i > index; --i) {
    array[i] = array[i - 1];
  }
  array[index] = elem;
  internal_page->IncreaseSize(1);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::RemoveOneInternalElem(InternalPair &elem, int index, InternalPage *internal_page) -> bool {
  InternalPair *array{internal_page->GetArray()};
  int n{internal_page->GetKeyNum()};
  if (!(index >= 1 && index <= n)) {
    return false;
  }
  elem = array[index];  // 保存被删除的这个元素
  for (int i = index; i < n; ++i) {
    array[i] = array[i + 1];
  }
  internal_page->IncreaseSize(-1);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::RemoveOne(const KeyType &key, LeafPage *leaf_page) -> bool {
  int target_idx{SearchLeaf(key, leaf_page)};
  if (target_idx == -1) {
    return false;
  }
  MappingType removed_elem{};  // 占位符
  RemoveOneElem(removed_elem, target_idx, leaf_page);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::RemoveOneInternal(const KeyType &key, InternalPage *internal_page) -> bool {
  int target_idx{SearchInternalAccuracy(key, internal_page)};
  if (target_idx == -1) {
    return false;
  }
  InternalPair removed_elem{};  // 占位符
  RemoveOneInternalElem(removed_elem, target_idx, internal_page);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindTargetValue(InternalPage *page, page_id_t page_id) -> int {
  if (page == nullptr) {
    return -1;
  }
  int n{page->GetSize()};  // note 这个是键值对的个数，因为要遍历的是 value，所以下标从 0 开始
  InternalPair *parent_array{page->GetArray()};
  int target_idx{-1};
  for (int i = 0; i < n; ++i) {
    if (parent_array[i].second == page_id) {
      target_idx = i;
    }
  }
  return target_idx;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetSiblings(InternalPage *parent_page, page_id_t page_id) -> std::vector<page_id_t> {
  std::vector<page_id_t> siblings(2, INVALID_PAGE_ID);
  if (parent_page == nullptr) {
    return siblings;
  }
  int n{parent_page->GetSize()};  // note 这个是键值对的个数，因为要遍历的是 value，所以下标从 0 开始
  InternalPair *parent_array{parent_page->GetArray()};
  int target_idx{FindTargetValue(parent_page, page_id)};
  if (target_idx == -1) {
    return siblings;
  }
  if (target_idx == 0) {  // 最左侧，仅有右兄弟
    siblings[1] = parent_array[target_idx + 1].second;
  } else if (target_idx == n - 1) {  // 最右侧，仅有左兄弟
    siblings[0] = parent_array[target_idx - 1].second;
  } else {  // 其它一般情况
    siblings[0] = parent_array[target_idx - 1].second;
    siblings[1] = parent_array[target_idx + 1].second;
  }
  return siblings;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::LeafBorrow(LeafPage *cur_page, std::vector<page_id_t> &siblings) -> LeafBorrowResult {
  WritePageGuard left_guard{};
  WritePageGuard right_guard{};
  if (siblings[0] != INVALID_PAGE_ID) {
    left_guard = buffer_pool_manager_->FetchPageWrite(siblings[0]);
  }
  //! \bug 就是这里导致的死锁 left_guard = buffer_pool_manager_->FetchPageWrite(siblings[1]);
  //! \bug 思考：为什么 left_guard
  //! 重复赋值两次会导致上面第一个if的锁无法释放？提示：原来的移动构造写的不对，不能放弃原有资源
  if (siblings[1] != INVALID_PAGE_ID) {
    right_guard = buffer_pool_manager_->FetchPageWrite(siblings[1]);
  }
  // 获取左右兄弟结点
  LeafPage *left_page{siblings[0] == INVALID_PAGE_ID ? nullptr : PageFromGuard<LeafPage>(left_guard)};
  LeafPage *right_page{siblings[1] == INVALID_PAGE_ID ? nullptr : PageFromGuard<LeafPage>(right_guard)};
  // 一般而言，这种情况不会出现，必然会有兄弟结点。
  if (left_page == nullptr && right_page == nullptr) {
    return {LeafBorrowStatus::FAILED_BORROW, cur_page->KeyAt(0)};
  }
  if (left_page != nullptr && left_page->GtHalfFull()) {  // 左侧不空，且左侧可以借入，则借入
    // 从左侧尾部拷贝应该拷贝到右侧数组的头部！
    MappingType moving_elem{};
    RemoveOneElem(moving_elem, left_page->GetKeyNum() - 1, left_page);
    InsertOneElem(moving_elem, 0, cur_page);
    return {LeafBorrowStatus::BORROW_LEFT, moving_elem.first};
  }
  if (right_page != nullptr && right_page->GtHalfFull()) {
    // 从右侧数组的头部拷贝到左侧数组的尾部
    //! \bug 从右侧借用必须返回右侧结点的首个结点 key
    MappingType moving_elem{};
    RemoveOneElem(moving_elem, 0, right_page);
    InsertOneElem(moving_elem, cur_page->GetKeyNum(), cur_page);
    return {LeafBorrowStatus::BORROW_RIGHT, right_page->KeyAt(0)};
  }
  // 其它情况：两侧都不能借，那么就只能返回失败了
  return {LeafBorrowStatus::FAILED_BORROW, cur_page->KeyAt(0)};
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InternalBorrow(InternalPage *cur_page, InternalPage *parent_page, std::vector<page_id_t> &siblings)
    -> InternalBorrowStatus {
  if (cur_page == nullptr || parent_page == nullptr) {
    return InternalBorrowStatus::FAILED_BORROW;
  }
  WritePageGuard left_guard{};
  WritePageGuard right_guard{};
  if (siblings[0] != INVALID_PAGE_ID) {
    left_guard = buffer_pool_manager_->FetchPageWrite(siblings[0]);
  }
  if (siblings[1] != INVALID_PAGE_ID) {
    right_guard = buffer_pool_manager_->FetchPageWrite(siblings[1]);
  }
  // 获取左右兄弟结点
  InternalPage *left_page{siblings[0] == INVALID_PAGE_ID ? nullptr : PageFromGuard<InternalPage>(left_guard)};
  InternalPage *right_page{siblings[1] == INVALID_PAGE_ID ? nullptr : PageFromGuard<InternalPage>(right_guard)};
  // 一般而言，这种情况不会出现，必然会有兄弟结点。
  if (left_page == nullptr && right_page == nullptr) {
    return InternalBorrowStatus::FAILED_BORROW;
  }
  if (left_page != nullptr && left_page->GtHalfFull()) {  // 左侧不空，且左侧可以借入，则借入
    // 此时，你需要进行三方交换：将左侧末尾的 key 拿到 parent，parent 结点的 key 拿下来，指针要做相应移动
    InternalPair removing_elem{};
    RemoveOneInternalElem(removing_elem, left_page->GetKeyNum(), left_page);
    int parent_idx{FindTargetValue(parent_page, cur_page->GetPageId())};  // parent page 中需要替换 key 的 id 值
    KeyType parent_key{parent_page->KeyAt(parent_idx)};
    parent_page->SetKeyAt(parent_idx, removing_elem.first);
    // 下面就是子结点移动指针的操作了
    InternalPair inserting_elem{parent_key, cur_page->GetArray()[0].second};
    InsertOneInternalElem(inserting_elem, 1, cur_page);
    // 设置 cur_page(右侧内部结点) 的最左侧指针
    cur_page->GetArray()[0].second = removing_elem.second;
    // 这个 removing_elem 的 parent page 更换了！，所以你需要重新设置
    WritePageGuard child_guard{buffer_pool_manager_->FetchPageWrite(removing_elem.second)};
    BPlusTreePage *child_page{PageFromGuard<BPlusTreePage>(child_guard)};
    if (child_page != nullptr) {
      child_page->SetParentPageId(cur_page->GetPageId());
    }
    return InternalBorrowStatus::BORROW_LEFT;
  }
  if (right_page != nullptr && right_page->GtHalfFull()) {
    // 反向的三方交换：将右侧第一个 key 拿到 parent，parent 结点的 key 拿到左侧，指针要做相应的移动
    InternalPair removing_elem{};
    //! \bug left_page->GetKeyNum() ? 为什么要这么做，这不合理？
    RemoveOneInternalElem(removing_elem, 1, right_page);
    int parent_idx{FindTargetValue(parent_page, right_page->GetPageId())};  // parent page 中需要替换 key 的 id 值
    KeyType parent_key{parent_page->KeyAt(parent_idx)};
    parent_page->SetKeyAt(parent_idx, removing_elem.first);
    // 下面就是子结点移动指针的操作
    InternalPair inserting_elem{parent_key, right_page->GetArray()[0].second};
    InsertOneInternalElem(inserting_elem, cur_page->GetKeyNum() + 1, cur_page);
    right_page->GetArray()[0].second = removing_elem.second;
    // 子页易主
    WritePageGuard child_guard{buffer_pool_manager_->FetchPageWrite(inserting_elem.second)};
    BPlusTreePage *child_page{PageFromGuard<BPlusTreePage>(child_guard)};
    if (child_page != nullptr) {
      child_page->SetParentPageId(cur_page->GetPageId());
    }
    return InternalBorrowStatus::BORROW_RIGHT;
  }
  // 其它情况：两侧都不能借，那么就只能返回失败了
  return InternalBorrowStatus::FAILED_BORROW;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::LeafMerge(LeafPage *left_page, LeafPage *right_page) {
  if (left_page == nullptr || right_page == nullptr) {
    return;
  }
  MappingType *right_array{right_page->GetArray()};
  for (int i = 0; i < right_page->GetKeyNum(); ++i) {
    // 总是插入左侧数组的尾部
    InsertOneElem(right_array[i], left_page->GetKeyNum(), left_page);
  }
  // 维护叶子结点的单向链表结构
  left_page->SetNextPageId(right_page->GetNextPageId());
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InternalMerge(InternalPage *left_page, InternalPage *right_page, InternalPage *parent_page) {
  if (left_page == nullptr || right_page == nullptr || parent_page == nullptr) {
    return;
  }
  int parent_idx{FindTargetValue(parent_page, right_page->GetPageId())};
  KeyType parent_key{parent_page->KeyAt(parent_idx)};
  InternalPair inserting_elem{parent_key, right_page->GetArray()[0].second};
  // 将 parent 结点的 key + right_page 最左侧指针移动到 left_page 的新元素
  InsertOneInternalElem(inserting_elem, left_page->GetKeyNum() + 1, left_page);
  InternalPair removed_elem{};  // 移除 parent_page 中相应的结点
  RemoveOneInternalElem(removed_elem, parent_idx, parent_page);
  // 将 right_page 的结点插入左侧数组的尾部。从 1 开始。
  InternalPair *right_array{right_page->GetArray()};
  for (int i = 1; i <= right_page->GetKeyNum(); ++i) {
    InsertOneInternalElem(right_array[i], left_page->GetKeyNum() + 1, left_page);
  }
  // 最后将 right_page 相关的所有子 page_id 指针易主
  for (int i = 0; i < right_page->GetSize(); ++i) {
    WritePageGuard child_guard{buffer_pool_manager_->FetchPageWrite(right_array[i].second)};
    BPlusTreePage *child_page{PageFromGuard<BPlusTreePage>(child_guard)};
    if (child_page != nullptr) {
      child_page->SetParentPageId(left_page->GetPageId());
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Remove(const KeyType &key, page_id_t page_id, WritePageGuard &parent_guard)
    -> BPLUSTREE_TYPE::RemoveStatus {
  WritePageGuard page_guard{};
  if (page_id != INVALID_PAGE_ID) {  // 必须先检查 page_id 不存在的情况，以免造成奇怪的错误
    page_guard = buffer_pool_manager_->FetchPageWrite(page_id);
    remove_guard_queue.push_back(&page_guard);  // 让 page_guard 加入队列，便于及时释放
  }
  BPlusTreePage *page{PageFromGuard<BPlusTreePage>(page_guard)};
  if (page == nullptr) {
    return RemoveStatus::REMOVE_FAILED;
  }
  if (page->IsLeafPage()) {  // 叶子结点，则执行删除
    LeafPage *leaf_page{PageFromGuard<LeafPage>(page_guard)};
    // 简单情况：leaf 本身就是 root，则此时直接删除这个 key 即可
    // 其它简单情况：结点半满以上(不能等于半满)，无需进行其它操作
    if (leaf_page != nullptr && (leaf_page->IsRootPage() || leaf_page->GetKeyNum() > leaf_page->GetMinKeyNum())) {
      //! \note 如果叶子结点在半满以上，可以释放所有祖先结点的 page guard(释放锁 + unpin)
      if (leaf_page->GetKeyNum() > leaf_page->GetMinKeyNum()) {
        GuardDrop(remove_guard_queue);
      }
      bool if_success{RemoveOne(key, leaf_page)};
      //! \bug 特殊情况，如果根结点删除了最后一个 key，你应该清空 B+ 树
      if (leaf_page->IsRootPage() && leaf_page->GetKeyNum() == 0) {
        // std::lock_guard<std::recursive_mutex> latch_guard(latch_);  // 多个线程执行写入操作必须加锁
        root_page_id_ = INVALID_PAGE_ID;
      }
      return if_success ? RemoveStatus::SUCCESS_REMOVE : RemoveStatus::REMOVE_FAILED;
    }

    // 复杂情况：结点恰好半满，则此时需要进行复杂操作
    RemoveOne(key, leaf_page);  // 先把这个结点删去
    // 1. 先向左邻右舍借来结点，返回的应该是一个 key，这个 key 应该拿上去
    InternalPage *parent_page{PageFromGuard<InternalPage>(parent_guard)};
    if (parent_page == nullptr) {
      return RemoveStatus::REMOVE_FAILED;
    }
    std::vector<page_id_t> siblings{GetSiblings(parent_page, page_id)};
    InternalPair *parent_array{parent_page->GetArray()};  // 获得 parent 数组中的详细数据
    LeafBorrowResult borrow_result{LeafBorrow(leaf_page, siblings)};
    if (borrow_result.first != LeafBorrowStatus::FAILED_BORROW) {  // 借成功，则直接替换父结点然后返回即可
      if (borrow_result.first == LeafBorrowStatus::BORROW_LEFT) {  // 从左边借出，key 要放到当前 page_id 这个地方
        parent_array[FindTargetValue(parent_page, page_id)].first = borrow_result.second;
      } else {  // 从右侧借来的结点，则 page_id 的下一个结点的 key 要修改
        //! \bug 并且需要改成右侧结点的第一个元素
        parent_array[FindTargetValue(parent_page, page_id) + 1].first = borrow_result.second;
      }
      // 神奇的是，如果你能从左邻右舍接到元素，那么无需调整内部结点
      return RemoveStatus::LEAF_BORROWED;
    }

    // 2. 更复杂的情况：当你无法从左邻右舍借来元素的时候，你需要合并结点。
    // 优先和左侧结点合并，并且要[合并到左侧]，因为这样移动的元素个数较少
    if (siblings[0] != INVALID_PAGE_ID) {
      WritePageGuard left_guard{buffer_pool_manager_->FetchPageWrite(siblings[0])};
      LeafPage *left_page{PageFromGuard<LeafPage>(left_guard)};
      LeafMerge(left_page, leaf_page);
      InternalPair removed_elem{};
      RemoveOneInternalElem(removed_elem, FindTargetValue(parent_page, page_id), parent_page);
    } else {  // 左侧没有，只能和右侧合并
      WritePageGuard right_guard{buffer_pool_manager_->FetchPageWrite(siblings[1])};
      LeafPage *right_page{PageFromGuard<LeafPage>(right_guard)};
      if (right_page == nullptr) {
        return RemoveStatus::REMOVE_FAILED;
      }
      LeafMerge(leaf_page, right_page);
      InternalPair removed_elem{};
      RemoveOneInternalElem(removed_elem, FindTargetValue(parent_page, right_page->GetPageId()), parent_page);
    }
    return RemoveStatus::LEAF_MERGED;
  }

  // 其它情况：page_id 是内部结点
  InternalPage *internal_page{PageFromGuard<InternalPage>(page_guard)};
  //! \note 如果内部结点半满以上，则可以释放所有祖先结点
  if (internal_page != nullptr && internal_page->GtHalfFull()) {
    GuardDrop(remove_guard_queue);
  }
  int target_idx{SearchInternalFind(key, internal_page)};

  // 递归调用
  if (internal_page == nullptr) {
    return RemoveStatus::REMOVE_FAILED;
  }
  RemoveStatus status{Remove(key, internal_page->ValueAt(target_idx), page_guard)};
  if (status != RemoveStatus::LEAF_MERGED && status != RemoveStatus::INTERNAL_MERGED) {
    return RemoveStatus::SUCCESS_REMOVE;
  }

  // 叶子结点合并的情况[注意：该祖先的相应元素已经被删除了]
  // status == LEAF_MERGED || status == INTERNAL_MERGED
  if (internal_page->GetKeyNum() >= internal_page->GetMinKeyNum()) {
    return RemoveStatus::SUCCESS_REMOVE;
  }
  // 下面就是不满半数的情况
  //! \bug 对根结点的判断部分很可能存在问题
  if (internal_page->IsRootPage()) {
    if (internal_page->GetKeyNum() == 0) {  // 仅剩最左侧指针了
      //! \note 对于更新 ROOT 的情况，或许需要加锁保护
      // std::lock_guard<std::recursive_mutex> guard{latch_};
      root_page_id_ = internal_page->ValueAt(0);  // 此时新的根结点诞生
      // 新的根结点的 PARENT_ID 应该变为 0
      WritePageGuard new_root_guard{buffer_pool_manager_->FetchPageWrite(root_page_id_)};
      BPlusTreePage *new_root_page{PageFromGuard<BPlusTreePage>(new_root_guard)};
      if (new_root_page != nullptr) {
        new_root_page->SetParentPageId(INVALID_PAGE_ID);
      }
    }
    // 其它更简单的情况：根作为内部结点只要有一个 key，它就有存在的价值
    return RemoveStatus::SUCCESS_REMOVE;
  }
  // 普通的内部结点不满半数，先尝试向左邻右舍借结点
  InternalPage *parent_page{PageFromGuard<InternalPage>(parent_guard)};
  std::vector<page_id_t> siblings{GetSiblings(parent_page, page_id)};
  InternalBorrowStatus borrow_status{InternalBorrow(internal_page, parent_page, siblings)};
  if (borrow_status != InternalBorrowStatus::FAILED_BORROW) {
    return RemoveStatus::SUCCESS_REMOVE;
  }
  // 其它情况：需要将 parent + cur_page + 兄弟 page 合并[注意：合并则一定是将右侧合并到左侧]
  if (siblings[0] != INVALID_PAGE_ID) {
    WritePageGuard left_guard{buffer_pool_manager_->FetchPageWrite(siblings[0])};
    InternalPage *left_page{PageFromGuard<InternalPage>(left_guard)};
    InternalMerge(left_page, internal_page, parent_page);
  } else {
    WritePageGuard right_guard{buffer_pool_manager_->FetchPageWrite(siblings[1])};
    InternalPage *right_page{PageFromGuard<InternalPage>(right_guard)};
    InternalMerge(internal_page, right_page, parent_page);
  }
  return RemoveStatus::INTERNAL_MERGED;
}

/**
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 * @note 它只是一个 interface function，真实现功能还得靠楼上十分复杂的递归函数
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  std::lock_guard<std::recursive_mutex> guard{latch_};
  if (root_page_id_ == INVALID_PAGE_ID) {
    return;
  }  // 当前的树是空的，立即返回
  ReadPageGuard dummy_guard{};
  if (!SearchBPlusTree(key, root_page_id_, dummy_guard).has_value()) {
    return;
  }  // 什么也找不到，立即返回
  // 随后，进入十分复杂的删除操作
  WritePageGuard temp_guard{};
  Remove(key, root_page_id_, root_guard_);
  //! \note 每次用完 guard_queue_ 后一定要记得清空数组！
  remove_guard_queue.clear();
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/**
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  if (IsEmpty()) {
    return INDEXITERATOR_TYPE();
  }
  page_id_t page_id{root_page_id_};
  ReadPageGuard page_guard{buffer_pool_manager_->FetchPageRead(page_id)};
  BPlusTreePage *page{PageFromGuard<BPlusTreePage>(page_guard)};
  if (page == nullptr) {
    return INDEXITERATOR_TYPE();
  }
  while (!page->IsLeafPage()) {  // 不是叶子结点就一路向左，一路向下
    InternalPage *internal_page{PageFromGuard<InternalPage>(page_guard)};
    if (internal_page != nullptr) {
      page_id = internal_page->ValueAt(0);  // 最左侧指针
    }
    page_guard = buffer_pool_manager_->FetchPageRead(page_id);
    page = PageFromGuard<BPlusTreePage>(page_guard);
  }
  // 现在已经找到了最左侧的叶子结点，构造这个函数
  return INDEXITERATOR_TYPE(PageFromGuard<LeafPage>(page_guard), 0, buffer_pool_manager_);
}

/**
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @note 这里我们假设让游标(指针)恰好指向key。
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  if (IsEmpty()) {
    return INDEXITERATOR_TYPE();
  }
  ReadPageGuard dummy_guard{};
  std::optional<page_id_t> find_res{SearchTargetLeaf(key, root_page_id_, dummy_guard)};
  if (!find_res.has_value()) {
    return INDEXITERATOR_TYPE();
  }
  ReadPageGuard page_guard{buffer_pool_manager_->FetchPageRead(find_res.value())};
  LeafPage *leaf_page{PageFromGuard<LeafPage>(page_guard)};
  int traget_idx{SearchLeaf(key, leaf_page)};
  return INDEXITERATOR_TYPE(leaf_page, traget_idx, buffer_pool_manager_);
}

/**
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @note 一路向右下方走去，直到叶子结点
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return 0; }

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::BufferPoolTracer(const KeyType &key) {
  std::cout << "当前正在插入：[" << key << "]"
            << "当前缓冲池可用页的数目为：[" << buffer_pool_manager_->GetAvailableSize() << "]" << std::endl;
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/**
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  auto *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Draw an empty tree");
    return;
  }
  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
  out << "}" << std::endl;
  out.flush();
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  if (IsEmpty()) {
    LOG_WARN("Print an empty tree");
    return;
  }
  ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
}

/**
 * This method is used for debug only, You don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
