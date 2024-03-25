/**
 * 给 B+ 树写的迭代器，具体而言，是用来访问叶子结点的
 * 以链表的方式访底部的链表即可
 */
#pragma once
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;

 public:
  IndexIterator() = default;
  // you may define your own constructor based on your member variables
  IndexIterator(LeafPage *cur_page, int cursor, BufferPoolManagerInstance *bpm);
  ~IndexIterator();  // NOLINT

  /**
   * @note 指的是最后一个元素的下一个元素。如果当前指向最后一个元素，不算 End
   */
  auto IsEnd() -> bool;

  auto operator*() -> const MappingType &;

  /**
   * 提示：这是前置自增运算符 ++i
   */
  auto operator++() -> IndexIterator &;

  auto operator==(const IndexIterator &itr) const -> bool {
    return cur_page_ == itr.cur_page_ && cur_cursor_ == itr.cur_cursor_;
  }

  auto operator!=(const IndexIterator &itr) const -> bool {
    return !(cur_page_ == itr.cur_page_ && cur_cursor_ == itr.cur_cursor_);
  }

 private:
  //! \note 或许你需要使用 ReadPageGuard
  LeafPage *cur_page_;  // 一次只保留链表中的一个叶子结点，如果这个结点到末尾了，就把下一个页面读入作为 cur_page_
  int cur_cursor_;      // 在当前 leaf_page 中的第几个元素？
  BufferPoolManagerInstance *buffer_pool_manager_;
};

}  // namespace bustub
