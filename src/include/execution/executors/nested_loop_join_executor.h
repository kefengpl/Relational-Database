#pragma once

#include <memory>
#include <utility>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "storage/table/tuple.h"
#include "type/value_factory.h"

namespace bustub {

/**
 * NestedLoopJoinExecutor executes a nested-loop JOIN on two tables.
 */
class NestedLoopJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new NestedLoopJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The NestedLoop join plan to be executed
   * @param left_executor The child executor that produces tuple for the left side of join
   * @param right_executor The child executor that produces tuple for the right side of join
   */
  NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                         std::unique_ptr<AbstractExecutor> &&left_executor,
                         std::unique_ptr<AbstractExecutor> &&right_executor);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join
   * @param[out] rid The next tuple RID produced, not used by nested loop join.
   * @return `true` if a tuple was produced, `false` if there are no more tuples.
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the insert */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

  /**
   * 将符合条件的左表的元组和右侧表的元组连接起来。
   * 所谓连接，就是新的元组需要包含左侧元组的所有列，也会包含右侧元组的所有列
   * @param[out] tuple 它就是 Next 函数需要输出的元组。
   * @note 如果 right_tuple 是 null，会生成悬浮元组
   */
  void MakeJoinTuple(std::unique_ptr<Tuple> &left_tuple, std::unique_ptr<Tuple> &right_tuple, Tuple *tuple);

  /**
   * 左表移动到下一个元组，右侧表游标重置
   * @param[out] right_tuple 输出参数，重置后右侧的第一个元组。
   * @return 如果发现左侧下移后没有新元组，返回 false；如果发现右侧游标重新初始化之后还是没有元组，说明右侧表是空，返回
   * false。 其它情况都返回 true
   * @note right_tuple 只有在函数返回 true 时才能正常使用。
   */
  auto NextAndReset() -> bool;

 private:
  /** The NestedLoopJoin plan node to be executed. */
  const NestedLoopJoinPlanNode *plan_;
  /** 连接时，左侧表的执行器，用于从左侧表中吐出元组 */
  std::unique_ptr<AbstractExecutor> left_executor_;
  /** 连接时，右侧表的执行器，用于从右侧表中吐出元组 */
  std::unique_ptr<AbstractExecutor> right_executor_;
  /** 当前正在使用的左侧数据表的元组 */
  std::unique_ptr<Tuple> left_tuple_;
  /** 成员变量：正在使用的右侧数据表的元组 */
  std::unique_ptr<Tuple> right_tuple_;
  /** 标志位：左侧元组是否悬浮？ */
  bool left_tuple_dangling_;
  /** 右侧表是否是空？ */
  bool right_table_empty_;
};

}  // namespace bustub
