#pragma once

#include <cmath>
#include <fstream>
#include <iostream>
#include <queue>
#include <sstream>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "concurrency/transaction.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define BPLUSTREE_TYPE BPlusTree<KeyType, ValueType, KeyComparator>

/**
 * Main class providing the API for the Interactive B+ Tree.
 *
 * Implementation of simple b+ tree data structure where internal pages direct
 * the search and leaf pages contain actual data.
 * (1) We only support unique key
 * (2) support insert & remove
 * (3) The structure should shrink and grow dynamically
 * (4) Implement index iterator for range scan
 */
INDEX_TEMPLATE_ARGUMENTS
class BPlusTree {
  using InternalPage = BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>;
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;
  using InternalPair = std::pair<KeyType, page_id_t>;
  // using LeafBorrowResult = std::pair<BPlusTree::LeafBorrowStatus, KeyType>; // 返回叶子结点之间相借用的结果
 public:
  // SUCCESS_INSERT 指的是没有造成分裂的成功插入； FAILED_INSERT 表示 KEY 已经存在，无法插入；SPLIT_INSERT
  // 表示因为插入造成了分裂
  enum class InsertStatus { SUCCESS_INSERT, FAILED_INSERT, LEAF_SPLIT_INSERT, INTERNAL_SPLIT_INSERT };
  enum class RemoveStatus { SUCCESS_REMOVE, LEAF_MERGED, LEAF_BORROWED, INTERNAL_MERGED, REMOVE_FAILED };
  enum class LeafBorrowStatus {
    BORROW_LEFT,
    BORROW_RIGHT,
    FAILED_BORROW
  };  // 叶子结点从左兄弟借、从右兄弟借入，失败借入
  enum class InternalBorrowStatus {
    BORROW_LEFT,
    BORROW_RIGHT,
    FAILED_BORROW
  };  // 内部结点从左兄弟借入，从右兄弟借入，失败借入
  using LeafBorrowResult = std::pair<BPlusTree::LeafBorrowStatus, KeyType>;  // 返回叶子结点之间相借用的结果
  // using InternalBorrowResult = std::pair<BPlusTree::LeafBorrowStatus, KeyType>; // 返回内部结点之间借用的结果

  explicit BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                     int leaf_max_size = LEAF_PAGE_SIZE, int internal_max_size = INTERNAL_PAGE_SIZE);

  // Returns true if this B+ tree has no keys and values.
  auto IsEmpty() const -> bool;

  // Insert a key-value pair into this B+ tree.
  auto Insert(const KeyType &key, const ValueType &value, Transaction *transaction = nullptr) -> bool;

  // Remove a key and its value from this B+ tree.
  void Remove(const KeyType &key, Transaction *transaction = nullptr);

  // return the value associated with a given key
  auto GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction = nullptr) -> bool;

  // return the page id of the root node
  auto GetRootPageId() -> page_id_t;

  // index iterator
  auto Begin() -> INDEXITERATOR_TYPE;
  auto Begin(const KeyType &key) -> INDEXITERATOR_TYPE;
  auto End() -> INDEXITERATOR_TYPE;

  // print the B+ tree
  void Print(BufferPoolManager *bpm);

  // draw the B+ tree
  void Draw(BufferPoolManager *bpm, const std::string &outf);

  // read data from file and insert one by one
  void InsertFromFile(const std::string &file_name, Transaction *transaction = nullptr);

  // read data from file and remove one by one
  void RemoveFromFile(const std::string &file_name, Transaction *transaction = nullptr);

  /**
   * 初始化根结点 (Create Root Page，并且最开始根就是叶子结点)，但是不刷回磁盘
   */
  auto InitializeRoot() -> WritePageGuard;

  /**
   * 在叶子结点中搜索 key 对应的 index
   * @note 调用该函数之前应该确保这个 page pin + lock
   * @param key 要搜索的 key
   * @param page 结点指针
   * @return 如果查找成功，返回 index；没有找到，返回 -1
   */
  auto SearchLeaf(const KeyType &key, LeafPage *page) -> int;

  /**
   * 寻找这个 key 最合适的插入位置，即第一个 >= key 的下标
   * @return 适合插入的下标 比如原来的数组大小是 n，下标是 0...n - 1，那么返回的范围是 0 <= return <= n。
   * 等于 n 表示需要插在数组的末尾。
   * @note 如果 page == null，直接返回 -1
   */
  auto SearchLeafInsert(const KeyType &key, LeafPage *page) -> int;

  /**
   * 在非叶子结点中搜索 key, 寻找第一个 >= key 的 index
   * @note 调用该函数之前应该确保这个 page pin + lock
   * @note 该函数仅适用于寻找合适的插入位置，不适合查找合适的指针
   * @param key 要搜索的 key
   * @param page 结点指针
   * @return 如果查找成功，返回指针所在的 index；没有找到，返回 0
   */
  auto SearchInternal(const KeyType &key, InternalPage *page) -> int;

  /**
   * 寻找最后一个 <= key 的 index，用于在内部结点查找最合适的指针
   * 因为该index的指针范围 [val[index], val[index] + 1)，下一个结点 > key ，说明
   * key < val[index + 1]，另一方面，key >= val[index]
   * @return 如果查找成功，返回指针所在的 index；没有找到，返回 0
   */
  auto SearchInternalFind(const KeyType &key, InternalPage *page) -> int;

  /**
   * 寻找内部结点中哪个元素的 key == key，精准匹配其索引 index。
   * 如果查找失败，则返回 -1
   */
  auto SearchInternalAccuracy(const KeyType &key, InternalPage *page) -> int;

  /**
   * 从某个 internal_page/ page 出发(以它为根)，找到 key 对应的 value
   * @return 具体的数值 ValueType，一般指的是这个 索引项比如 id 所对应记录的磁盘地址。查找失败则返回 std::nullopt
   * @note 锁的释放策略：下一个结点获取到锁，就可以立即释放双亲结点的锁
   */
  auto SearchBPlusTree(const KeyType &key, page_id_t page, ReadPageGuard &parent_guard) -> std::optional<ValueType>;

  /**
   * 从某个 internal_page/ page 出发(以它为根)，找到 key 所在的叶子结点的 page_id
   * @return 这个 key 所在叶子结点对应的 page_id。查找失败则返回 std::nullopt
   * @note 锁的释放策略：下一个结点获取到锁，就可以立即释放双亲结点的锁
   * @note 为什么要设立这个函数？答：给后面的迭代器使用
   */
  auto SearchTargetLeaf(const KeyType &key, page_id_t page_id, ReadPageGuard &parent_guard) -> std::optional<page_id_t>;

  /**
   * 进行 B+ 树插入操作
   * @param page_id 它的作用是表示是从哪个结点开始向下查找合适位置并插入
   * @return 返回插入状态：成功插入，插入且分裂，插入失败(因为key已经在B+树里了)
   */
  auto Insert(const KeyType &key, const ValueType &value, page_id_t page_id, page_id_t parent_page_id) -> InsertStatus;

  /**
   * 在叶子结点中插入数值到合适位置[提示：即便 full 也可以插入，因为 leaf node 允许一个元素的溢出]
   */
  auto InsertLeaf(const KeyType &key, const ValueType &value, LeafPage *page) -> void;
  /**
   * 叶子结点的分裂
   * @note 千万不要使得数组越界，不要进行溢出插入，这会让你死的很惨！
   */
  auto SplitLeaf(LeafPage *old_page, LeafPage *new_page, MappingType &inserting_pair) -> void;

  /**
   * 将新的元素插入内部结点。如果结点已经满了就禁止插入
   * @param value0 其作用在于：如果这是一个新成立的内部结点，那么 value0 决定了其最左侧指针的指向
   * @param key 比如 id
   * @param value key 对应的 value，一般是 key 右侧的指针地址
   */
  auto InsertInternalPage(std::optional<page_id_t> old_page_id, const KeyType &key, const page_id_t &new_page_id,
                          InternalPage *internal_page) -> bool;

  /**
   * 将内部结点一分为二。两个结点分别作为参数。注意，这会导致一个元素减少[要么成立新根，要么拿上去]。
   * 此外，它还会更新对应子结点的 parent id
   * @note 拿上去的元素，或者说是在这俩里面减少的元素是按顺序排列的第 ceil((n + 1) / 2) 个元素(从1开始计数)
   * @return 返回处于分裂点元素的 key， 它的 value 应该是 new_page ，但是这逻辑在该函数外面处理即可
   */
  auto SplitInternal(InternalPage *old_page, InternalPage *new_page, InternalPair &inserting_pair) -> KeyType;

  /**
   * 产生一个新的 root page，它会将 root_page_id_ 设置为他自己，并完成根结点的初始化
   * @param page_id 新生成 root 的 page_id
   */
  auto NewRootInternalPage(WritePageGuard &page_guard, page_id_t page_id) -> InternalPage *;

  /**
   * 每次我们都尝试去左侧兄弟结点借入，如果左侧不足，就进行合并；第一个叶子结点只能和右侧兄弟结点借入
   * @note 从左侧借入情况，只需要移动左侧兄弟最后一个结点而无需
   * @param page_id 它的作用是表示是从哪个结点开始向下查找删除的合适位置
   * @param parent_guard 传入引用，使得子结点的栈中也能操作父结点
   */
  auto Remove(const KeyType &key, page_id_t page_id, WritePageGuard &parent_guard) -> BPLUSTREE_TYPE::RemoveStatus;

  /**
   * 从叶子结点中移除一个元素，如果移除成功，返回 true，移除失败(一般是没有找到key)，返回 false
   * @note 由于我们在外层函数检验了 key 的存在性，所以一般情况下没有移除失败的情况，key 一定存在
   */
  auto RemoveOne(const KeyType &key, LeafPage *leaf_page) -> bool;

  /**
   * 某个叶子结点试图向左右两侧的兄弟借入结点
   * @param siblings 这个 leaf_page 的兄弟结点的 page_id ，始终有两个，先左后右。如果一个结点只有单侧兄弟，
   * 那么另一侧会变为 INVALID_PAGE_ID
   * @return LeafBorrowResult 一个 pair，借用状态 --> 借用的 key
   */
  auto LeafBorrow(LeafPage *cur_page, std::vector<page_id_t> &siblings) -> LeafBorrowResult;

  /**
   * 某个内部结点试图向左右两侧兄弟借入结点，如果借入成功，会直接完成三方转换。
   * 如果借入失败，将返回失败结果，你需要手动合并三方内部结点。
   */
  auto InternalBorrow(InternalPage *cur_page, InternalPage *parent_page, std::vector<page_id_t> &siblings)
      -> InternalBorrowStatus;
  /**
   * 插入一个元素到某个叶子结点，插入位置恰好是 index
   * @note 该函数会对 index 的合法性进行检查，只允许 0 <= index <= leaf_page->GetKeyNum()。其它情况就直接返回 false
   * @return 插入成功返回 true；插入失败返回 false。同时，页的 size_ 属性也会相应调整
   */
  auto InsertOneElem(MappingType &elem, int index, LeafPage *leaf_page) -> bool;

  /**
   * 插入一个元素到某个内部结点，插入位置恰好是 index
   * @note 该函数会对 index 的合法性进行检查，只允许 1 <= index <= leaf_page->GetKeyNum() + 1。其它情况就直接返回 false
   * @return 插入成功返回 true；插入失败返回 false。同时，页的 size_ 属性也会相应调整
   */
  auto InsertOneInternalElem(InternalPair &elem, int index, InternalPage *internal_page) -> bool;

  /**
   * 删除叶子结点的一个元素，删除位置恰好是 index
   * @param[out] elem 输出参数，将移除的这个元素保存起来
   * @note 该函数会对 index 的合法性进行检查，只允许 0 <= index < leaf_page->GetKeyNum()。其它情况就直接返回 false
   * @return 删除成功返回 true；删除失败返回 false。同时，页的 size_ 属性也会相应调整
   */
  auto RemoveOneElem(MappingType &elem, int index, LeafPage *leaf_page) -> bool;

  /**
   * 删除内部结点的一个元素，删除位置恰好是 index
   * @param[out] elem 输出参数，将移除的这个元素保存起来
   * @note 该函数会对 index 的合法性进行检查，只允许 1 <= index <= internal_page->GetKeyNum()
   * @return 删除成功返回 true；删除失败返回 false。同时，页的 size_ 属性也会相应调整
   */
  auto RemoveOneInternalElem(InternalPair &elem, int index, InternalPage *internal_page) -> bool;

  /**
   * 在内部结点查找 page_id 在数组中对应的索引值
   * @param page_id 你要查询的叶子结点的 page_id
   * @return 查找成功，返回对应的索引值(从0开始的那个索引，数组下标)；查找失败，返回 -1
   */
  auto FindTargetValue(InternalPage *page, page_id_t page_id) -> int;

  /**
   * 获取某个叶子结点的兄弟结点
   * @param page_id 你要查询的叶子结点的 page_id
   * @return 个 leaf_page 的兄弟结点的 page_id ，始终有两个，先左后右。如果一个结点只有单侧兄弟，
   * 那么另一侧会变为 INVALID_PAGE_ID。如果两侧都没有，那么 vector 的两个元素都是 INVALID_PAGE_ID。
   * 当然，一般不会出现两侧兄弟都没有的情况。
   */
  auto GetSiblings(InternalPage *parent_page, page_id_t page_id) -> std::vector<page_id_t>;

  /**
   * 将右侧的叶子结点合并到左侧
   */
  void LeafMerge(LeafPage *left_page, LeafPage *right_page);

  /**
   * 移除内部结点中 remove 一个 key == key 的数组元素(注意：位置 0 的 key 无效)
   */
  auto RemoveOneInternal(const KeyType &key, InternalPage *internal_page) -> bool;

  /**
   * 内部结点的 merge ，将右侧 page 及 parent_page 关联的一个结点全部合并到左侧
   */
  void InternalMerge(InternalPage *left_page, InternalPage *right_page, InternalPage *parent_page);

  /**
   * 在插入时，如果发现某个结点未满，则调用此函数，让 guard_queue_ 除了最后一个元素以外都释放掉。
   * 在删除时，如果某个结点半满以上，则调用此函数，让 guard_queue_ 除了最后一个元素以外都释放掉。
   * @note 该方法同时也会把相应的指针元素移除数组
   */
  void GuardDrop(std::vector<WritePageGuard *> &guard_queue_);

  /**
   * 用于打印 B+ 树各类操作过程中缓冲池大小
   */
  void BufferPoolTracer(const KeyType &key);

  /**
   * 用于实现从 guard 到 page指针的转换。如果 page_guard 中的 page_ 是 null，返回 nullptr
   */
  template <typename T>
  auto PageFromGuard(WritePageGuard &page_guard) -> T * {
    if (page_guard.PageId() == INVALID_PAGE_ID) {
      return nullptr;
    }
    return page_guard.AsMut<T>();
  }
  /**
   * 用于实现从 guard 到 page指针的转换。如果 page_guard 中的 page_ 是 null，返回 nullptr
   */
  template <typename T>
  auto PageFromGuard(ReadPageGuard &page_guard) -> T * {
    if (page_guard.PageId() == INVALID_PAGE_ID) {
      return nullptr;
    }
    return page_guard.As<T>();
  }

 private:
  void UpdateRootPageId(int insert_record = 0);

  /* Debug Routines for FREE!! */
  void ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const;

  void ToString(BPlusTreePage *page, BufferPoolManager *bpm) const;

  // member variable [你需要将你的BufferPoolManagerInstance嵌入到这里，具体而言，使用动态类型转换]
  std::string index_name_;
  page_id_t root_page_id_;
  // BufferPoolManager *buffer_pool_manager_; 注意：这是原有的数据结构
  BufferPoolManagerInstance *buffer_pool_manager_;
  KeyComparator comparator_;
  int leaf_max_size_;
  int internal_max_size_;
  std::recursive_mutex empty_latch_;  // 用于初始化
  std::vector<InternalPair> splitted_;
  std::recursive_mutex latch_; 

  WritePageGuard root_guard_;
};
}  // namespace bustub
