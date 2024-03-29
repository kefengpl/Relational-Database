#pragma once

#include <vector>

#include "common/rid.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/index_scan_plan.h"
#include "storage/table/tuple.h"

namespace bustub {
/**
 * IndexScanExecutor executes an index scan over a table.
 */
class IndexScanExecutor : public AbstractExecutor {
 public:
  /**
   * Creates a new index scan executor.
   * @param exec_ctx the executor context
   * @param plan the index scan plan to be executed
   */
  IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan);

  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

  /**
   * 或许可以在初始化阶段对索引进行扫描,形成 std::vector<RID> sorted_rids_; 
  */
  void Init() override;
  /**
   * 或许是在索引上迭代，那么这个索引的 key 很可能就是 RID
   * @note 你需要先找到  key 构建的 B+ 树，叶子结点所谓记录的 “磁盘地址” 就可以是 "RID"
   * @note 一次只需要获取一个元组,因为 *tuple 就限定了,一次你只能存储一个元组并将它返回给外层的 PollExecutor
   * @note 提示: rid 包括 page_id + slot_num , 显然是在使用分槽的页结构
  */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

 private:
  // 成员变量表 提示: 你不需要迭代器
  /** The index scan plan node to be executed. */
  const IndexScanPlanNode *plan_;
  IndexInfo* index_info_;
  BPlusTreeIndexForOneIntegerColumn* tree_; // 提示:它内部的核心成员变量就是你写的 B+ 树
  TableInfo* table_info_; // 单表扫描。该变量储存了 table 的元信息
  TableHeap* table_heap_; 
  /** 按照某个 key 的先后顺序排列的 RID[可以反映其磁盘之地址] */
  std::vector<RID> sorted_rids_; 
  size_t cursor_;
};
}  // namespace bustub
