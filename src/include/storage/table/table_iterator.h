#pragma once

#include <cassert>

#include "common/rid.h"
#include "concurrency/transaction.h"
#include "storage/table/tuple.h"

namespace bustub {

class TableHeap;

/**
 * TableIterator enables the sequential scan of a TableHeap.
 * @note TableHeap 就是一个 page 的 list[你可以将其抽象为磁盘上 table 具体内容本身]，为了使这个文件完整，
 * 每个文件都相当于是双向链表的一个结点。
 */
class TableIterator {
  friend class Cursor;

 public:
  TableIterator(TableHeap *table_heap, RID rid, Transaction *txn);

  TableIterator(const TableIterator &other)
      : table_heap_(other.table_heap_), tuple_(new Tuple(*other.tuple_)), txn_(other.txn_) {}

  ~TableIterator() { delete tuple_; }

  inline auto operator==(const TableIterator &itr) const -> bool {
    return tuple_->rid_.Get() == itr.tuple_->rid_.Get();
  }

  inline auto operator!=(const TableIterator &itr) const -> bool { return !(*this == itr); }

  auto operator*() -> const Tuple &;
  // 使用方法：重载的意义在于，-> 返回了一个对象，所以你调用 iterato r-> 操作符时，就像调用 tuple*
  // (返回对象)的各种方法一样。
  auto operator->() -> Tuple *;

  auto operator++() -> TableIterator &;  // 前置： ++i

  auto operator++(int) -> TableIterator;  // 后置： i++

  auto operator=(const TableIterator &other) -> TableIterator & {
    table_heap_ = other.table_heap_;
    *tuple_ = *other.tuple_;
    txn_ = other.txn_;
    return *this;
  }

 private:
  TableHeap *table_heap_;
  Tuple *tuple_;
  Transaction *txn_;
};

}  // namespace bustub
