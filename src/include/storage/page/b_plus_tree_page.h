#pragma once

#include <cassert>
#include <climits>
#include <cmath>
#include <cstdlib>
#include <string>
#include "buffer/buffer_pool_manager_instance.h"
// #include "buffer/buffer_pool_manager.h"
#include "storage/index/generic_key.h"

namespace bustub {

#define MappingType std::pair<KeyType, ValueType>

#define INDEX_TEMPLATE_ARGUMENTS template <typename KeyType, typename ValueType, typename KeyComparator>

// define page type enum
enum class IndexPageType { INVALID_INDEX_PAGE = 0, LEAF_PAGE, INTERNAL_PAGE };

/**
 * Both internal and leaf page are inherited from this page.
 *
 * It actually serves as a header part for each B+ tree page and
 * contains information shared by both leaf page and internal page.
 *
 * Header format (size in byte, 24 bytes in total):
 * ----------------------------------------------------------------------------
 * | PageType (4) | LSN (4) | CurrentSize (4) | MaxSize (4) |
 * ----------------------------------------------------------------------------
 * | ParentPageId (4) | PageId(4) |
 * ----------------------------------------------------------------------------
 */
class BPlusTreePage {
 public:
  virtual ~BPlusTreePage() = default;
  auto IsLeafPage() const -> bool;
  auto IsRootPage() const -> bool;
  void SetPageType(IndexPageType page_type);

  auto GetSize() const -> int;
  void SetSize(int size);
  void IncreaseSize(int amount);

  auto GetMaxSize() const -> int;
  void SetMaxSize(int max_size);
  auto GetMinSize() const -> int;

  auto GetParentPageId() const -> page_id_t;
  void SetParentPageId(page_id_t parent_page_id);

  auto GetPageId() const -> page_id_t;
  void SetPageId(page_id_t page_id);

  void SetLSN(lsn_t lsn = INVALID_LSN);

  // 补充：上面的函数过于混乱，我们以B+树的 key(不包括指针) 作为是否决定分裂的唯一标准
  /**
   * 获取叶子或者非叶子结点上 key 的个数 [注意：非叶子结点数组第一个 key 无效]。
   * key 的最小个数会保证 leaf 或者 internal 结点至少是半满的
   */
  auto GetKeyNum() -> int;
  /**
   * 获取叶子或者非叶子结点上最小 key 的个数，既一个结点 key 的个数应该 >= 这个数值
   */
  auto GetMinKeyNum() -> int;
  auto IsFull() -> bool;
  /**
   * 某个结点当前元素个数是否 > 半满。GetKeyNum() > GetMinKeyNum();
   * @note 这可以表明这个结点能否再借出一个结点。如果返回 true，表明“地主家还有余粮”
   * @note Gt 的意思是 greater than
   */
  auto GtHalfFull() -> bool;

 private:
  //! \note 这个 B+ 树允许叶子结点和内部结点的阶是不一样的。
  // member variable, attributes that both internal and leaf page share
  IndexPageType page_type_;
  lsn_t lsn_ __attribute__((__unused__));  // 项目4才会使用，不用管
  //! \note size_ 始终代表的是键值对的个数，插入一个键值对，你就应该给他 + 1，通过 GetKeyNum() 获得当前 key 的个数即可。
  int size_;  // 键值对之个数。叶子结点：size_ 是 key 的个数；非叶子结点 size_ 是 child 指针的个数[也是 key-value
              // 对，包含第一个空 key 情况下的个数]。
  // 在内部结点，第一个 key 会被忽略，因为是 n - 1 个 key，n 个指针结点(child))
  int max_size_;  // 最大键值对之个数。对于非叶子结点，它本身就是 B+ 树的结点阶数 n，key 最多存放 n - 1
                  // 个[叶子、非叶子一致，阶数都是 max_size_]
  page_id_t parent_page_id_;  // 双亲结点的 id(指针)，我们假设 -1 是无效 page_id
  page_id_t page_id_;         // 本结点自己的 id(标识或者指针)
};

}  // namespace bustub
