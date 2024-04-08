#include "execution/executors/nested_index_join_executor.h"

namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_{plan},
      child_executor_{std::move(child_executor)},
      index_info_{exec_ctx->GetCatalog()->GetIndex(plan_->GetIndexOid())},
      right_table_info_{exec_ctx->GetCatalog()->GetTable(plan_->GetInnerTableOid())},
      tree_{dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(index_info_->index_.get())},
      left_tuple_{std::make_unique<Tuple>()},
      left_tuple_dangling_{true} {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    // throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestIndexJoinExecutor::Init() {
  child_executor_->Init();
  RID child_rid{};
  child_executor_->Next(left_tuple_.get(), &child_rid);
}

auto NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (!left_tuple_->IsAllocated()) {  // 左侧表根本没有元组，直接返回 false
    return false;
  }
  while (true) {
    Value raw_key{plan_->KeyPredicate()->Evaluate(left_tuple_.get(), child_executor_->GetOutputSchema())};
    Tuple key{std::vector<Value>{raw_key}, index_info_->index_->GetKeySchema()};
    std::vector<RID>
        result{};  // 一般是只能取到一个 RID，根据实现逻辑，只有查找成功才会加入动态数组，查找失败则数组是空
    tree_->ScanKey(key, &result, exec_ctx_->GetTransaction());
    if (result.empty()) {  // 如果很遗憾，发现没有匹配的元组，那么你需要判断是否是左连接，左连接需要生成悬浮元组
      if (plan_->GetJoinType() == JoinType::LEFT) {
        if (left_tuple_dangling_) {  // 生成悬浮元组
          std::unique_ptr<Tuple> empty_ptr{};
          MakeJoinTuple(left_tuple_, empty_ptr, tuple);
          NextAndReset();
          return true;
        }
      }
      // 对于某个不能匹配的左表元组，如果是内连接，那么需要向下迭代
      if (!NextAndReset()) {
        return false;
      }
      continue;
    }
    std::unique_ptr<Tuple> right_tuple{std::make_unique<Tuple>()};
    right_table_info_->table_->GetTuple(result[0], right_tuple.get(),
                                        exec_ctx_->GetTransaction());  // 获取唯一的右侧元组
    // 进行左右元组连接
    MakeJoinTuple(left_tuple_, right_tuple, tuple);
    // 成功的情况也要让左侧游标下移，因为右侧至多有一个匹配的元组。
    NextAndReset();
    return true;
  }
}

auto NestIndexJoinExecutor::NextAndReset() -> bool {
  // 内连接的情况：左侧元组下移，右侧游标 reset
  RID child_rid{};
  if (!child_executor_->Next(left_tuple_.get(), &child_rid)) {
    left_tuple_ = std::make_unique<Tuple>();
    return false;  // 左侧没有新元组，直接返回 false
  }
  left_tuple_dangling_ = true;  // 左侧元组下移，重新进入悬浮状态，直到和正经的右侧元组连接。
  return true;
}

void NestIndexJoinExecutor::MakeJoinTuple(std::unique_ptr<Tuple> &left_tuple, std::unique_ptr<Tuple> &right_tuple,
                                          Tuple *tuple) {
  uint32_t left_col_num{child_executor_->GetOutputSchema().GetColumnCount()};
  uint32_t right_col_num{right_table_info_->schema_.GetColumnCount()};
  std::vector<Value> joined_tuple{};
  joined_tuple.reserve(left_col_num + right_col_num);
  for (uint32_t i = 0; i < left_col_num; ++i) {  // 新元组左侧所有属性
    joined_tuple.push_back(left_tuple_->GetValue(&child_executor_->GetOutputSchema(), i));
  }
  for (uint32_t i = 0; i < right_col_num; ++i) {  // 新元组右侧所有属性
    if (right_tuple == nullptr) {                 // 你需要创造悬浮元组，即右侧皆为空值
      joined_tuple.push_back(ValueFactory::GetNullValueByType(right_table_info_->schema_.GetColumn(i).GetType()));
    } else {
      joined_tuple.push_back(right_tuple->GetValue(&right_table_info_->schema_, i));
    }
  }
  *tuple = Tuple{joined_tuple, &GetOutputSchema()};
}

}  // namespace bustub
