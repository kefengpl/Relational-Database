#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/plans/nested_index_join_plan.h"
#include "storage/table/tmp_tuple.h"
#include "storage/table/tuple.h"
#include "type/value_factory.h"

namespace bustub {

/**
 * IndexJoinExecutor executes index join operations.
 */
class NestIndexJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Creates a new nested index join executor.
   * @param exec_ctx the context that the hash join should be performed in
   * @param plan the nested index join plan node
   * @param child_executor the outer table
   */
  NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                        std::unique_ptr<AbstractExecutor> &&child_executor);

  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

  void Init() override;
  /**
   * 提示：由于 B+ TREE 仅支持唯一索引，所以等值连接的时候应该都是唯一匹配的，不会出现重复值。
   * 所以你只需要 while 循环左侧表即可
  */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /**
   * 将符合条件的左表的元组和右侧表的元组连接起来。
   * 所谓连接，就是新的元组需要包含左侧元组的所有列，也会包含右侧元组的所有列
   * @param[out] tuple 它就是 Next 函数需要输出的元组。
   * @note 如果 right_tuple 是 null，会生成悬浮元组
  */
  void MakeJoinTuple(std::unique_ptr<Tuple>& left_tuple, std::unique_ptr<Tuple>& right_tuple, Tuple *tuple);

  /**
   * 左表的游标移动到下一个元组
   * @return 如果发现左侧下移后没有新元组，返回 false；其它情况返回 true
  */
  auto NextAndReset() -> bool;

 private:
  /** The nested index join plan node. */
  const NestedIndexJoinPlanNode *plan_;
  /** 提示：这里的孩子结点算子发动机仅包括左表。 */
  std::unique_ptr<AbstractExecutor> child_executor_;
  /** 索引的信息，可以用来召唤B+树 */
  IndexInfo* index_info_;
  /** 表本身的信息 */
  TableInfo* right_table_info_;
  /** 你需要用到的 B+ 树索引，其底层的数据结构正是 B+ 树 */
  BPlusTreeIndexForOneIntegerColumn* tree_;
  /** 当前正在使用的左侧数据表的元组 */
  std::unique_ptr<Tuple> left_tuple_; 
  /** 标志位：左侧元组是否悬浮？ */
  bool left_tuple_dangling_;
};
}  // namespace bustub
