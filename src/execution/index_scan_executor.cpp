#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_{plan},
      index_info_{exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid())},
      tree_{dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(index_info_->index_.get())},
      table_info_{exec_ctx_->GetCatalog()->GetTable(index_info_->table_name_)},
      table_heap_{table_info_->table_.get()} {}  // 提示:反复扩容数组可能导致性能瓶颈

void IndexScanExecutor::Init() {
  sorted_rids_.clear();  //! \bug 多次调用 init 一定要清空所有数据结构
  auto tree_iterator{tree_->GetBeginIterator()};
  while (tree_iterator != tree_->GetEndIterator()) {
    sorted_rids_.push_back((*tree_iterator).second);  // 所有 RID 加入 vector
    ++tree_iterator;
  }
  cursor_ = 0;  // 初始游标是 0
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // 再往后迭代没有元素了,则直接返回
  if (cursor_ == sorted_rids_.size()) {
    return false;
  }
  *rid = sorted_rids_[cursor_++];  // 获取目标的 rid, 然后游标向后移动
  // 直接将获取到的元素存入 tuple_
  table_heap_->GetTuple(*rid, tuple, exec_ctx_->GetTransaction());
  return true;
}
}  // namespace bustub
