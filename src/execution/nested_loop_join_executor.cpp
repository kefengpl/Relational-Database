#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor{exec_ctx},
      plan_{plan},
      left_executor_{std::move(left_executor)},
      right_executor_{std::move(right_executor)},
      left_tuple_{std::make_unique<Tuple>()},
      right_tuple_{std::make_unique<Tuple>()},
      left_tuple_dangling_{true},
      right_table_empty_{false} {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    // throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  RID child_rid{};
  // 提示：指针 left_tuple_ 需要先初始化，否则 *left_tuple_ 将直接导致你访问未知内存，然后直接报错
  left_executor_->Next(left_tuple_.get(), &child_rid);
  // 先检测右侧表是否是空值？
  if (!right_executor_->Next(right_tuple_.get(), &child_rid)) {
    right_table_empty_ = true;
  }
  right_executor_->Init();
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (!left_tuple_->IsAllocated()) {  // 左侧再无剩余元组
    return false;
  }
  if (right_table_empty_ && plan_->GetJoinType() == JoinType::INNER) {  // 内连接并且右侧表是空的，直接返回 false
    return false;
  }
  while (true) {
    // 从右侧表吐出失败，说明右侧表遍历结束，或者右侧表是空，[对于左连接]直接生成悬浮元组
    if (!right_executor_->Next(right_tuple_.get(), rid)) {
      if (plan_->GetJoinType() == JoinType::LEFT) {
        if (left_tuple_dangling_) {
          std::unique_ptr<Tuple> empty_ptr{};
          MakeJoinTuple(left_tuple_, empty_ptr, tuple);  // 输出一个悬浮元组
          //! \bug 如果下面两行放在 if 外侧，会导致非悬浮的左元组被添加两次，因为它返回了 true
          NextAndReset();
          return true;
        }
        // 不生成悬浮元组的情况：左表下移，右侧表游标重置。
      }
      //! \bug 对于非悬浮元组，应该直接进行普通重置。
      // 提示：非左连接情况下，右侧表此时不可能是空，因为开头会判断这种情况
      if (!NextAndReset()) {
        return false;
      }
      continue;
    }

    // 提示：这个函数可以直接评估是否满足连接条件
    Value evaluate_value{plan_->Predicate().EvaluateJoin(left_tuple_.get(), left_executor_->GetOutputSchema(),
                                                         right_tuple_.get(), right_executor_->GetOutputSchema())};
    // 评估：发现可以连接
    if (!evaluate_value.IsNull() && evaluate_value.GetAs<bool>()) {
      left_tuple_dangling_ = false;
      MakeJoinTuple(left_tuple_, right_tuple_, tuple);
      return true;
    }
  }
}

auto NestedLoopJoinExecutor::NextAndReset() -> bool {
  // 内连接的情况：左侧元组下移，右侧游标 reset
  RID child_rid{};
  if (!left_executor_->Next(left_tuple_.get(), &child_rid)) {
    left_tuple_ = std::make_unique<Tuple>();
    return false;  // 左侧没有新元组，直接返回 false
  }
  left_tuple_dangling_ = true;  // 左侧元组下移，重新进入悬浮状态，直到和正经的右侧元组连接。
                                //! \bug 你的 seq_scan 初始化的时候没有将迭代器设置为表的初始位置
  right_executor_->Init();
  return true;
}

void NestedLoopJoinExecutor::MakeJoinTuple(std::unique_ptr<Tuple> &left_tuple, std::unique_ptr<Tuple> &right_tuple,
                                           Tuple *tuple) {
  uint32_t left_col_num{left_executor_->GetOutputSchema().GetColumnCount()};
  uint32_t right_col_num{right_executor_->GetOutputSchema().GetColumnCount()};
  std::vector<Value> joined_tuple{};
  joined_tuple.reserve(left_col_num + right_col_num);
  for (uint32_t i = 0; i < left_col_num; ++i) {  // 新元组左侧所有属性
    joined_tuple.push_back(left_tuple_->GetValue(&left_executor_->GetOutputSchema(), i));
  }
  for (uint32_t i = 0; i < right_col_num; ++i) {  // 新元组右侧所有属性
    if (right_tuple == nullptr) {                 // 你需要创造悬浮元组，即右侧皆为空值
      joined_tuple.push_back(
          ValueFactory::GetNullValueByType(right_executor_->GetOutputSchema().GetColumn(i).GetType()));
    } else {
      joined_tuple.push_back(right_tuple->GetValue(&right_executor_->GetOutputSchema(), i));
    }
  }
  *tuple = Tuple{joined_tuple, &GetOutputSchema()};
}

}  // namespace bustub
