#pragma once

#include <memory>
#include <utility>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/delete_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * DeletedExecutor executes a delete on a table.
 * Deleted values are always pulled from a child.
 */
class DeleteExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new DeleteExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The delete plan to be executed
   * @param child_executor The child executor that feeds the delete
   */
  DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                 std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the delete */
  void Init() override;

  /**
   * Yield the number of rows deleted from the table.
   * @param[out] tuple The integer tuple indicating the number of rows deleted from the table
   * @param[out] rid The next tuple RID produced by the delete (ignore, not used)
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   *
   * NOTE: DeleteExecutor::Next() does not use the `rid` out-parameter.
   * NOTE: DeleteExecutor::Next() returns true with the number of deleted rows produced only once.
   * @note delete 会从 child 拿到一些元组，把它们删除即可。你依然要在这里写循环。
   * @note 它的写法和 insert 的结构基本是一致的，没什么难度。
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the delete */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

 private:
  /** The delete plan node to be executed */
  const DeletePlanNode *plan_;
  /** The child executor from which RIDs for deleted tuples are pulled */
  std::unique_ptr<AbstractExecutor> child_executor_;
  bool reentrant_;         // 重入标记，检查是否重入
  TableInfo *table_info_;  // 单表删除。该变量储存了 table 的元信息
  TableHeap *table_heap_;  // 由于要删除元素，所以需要这个东西
};
}  // namespace bustub
