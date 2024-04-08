#include "execution/executors/values_executor.h"

namespace bustub {

ValuesExecutor::ValuesExecutor(ExecutorContext *exec_ctx, const ValuesPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan), dummy_schema_(Schema({})) {}

void ValuesExecutor::Init() { cursor_ = 0; }
/**
 * 提示：values 似乎不会更新 rid，所以你需要在外界手动更新 rid
 */
auto ValuesExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (cursor_ >= plan_->GetValues().size()) {
    return false;
  }

  std::vector<Value> values{};
  values.reserve(GetOutputSchema().GetColumnCount());

  const auto &row_expr = plan_->GetValues()[cursor_];  // 直接获取“行”表达式，用于解析 (1, 2, 'a') 这种元组
  for (const auto &col : row_expr) {                   // 对每个行表达式的每个元素进行解析
    values.push_back(col->Evaluate(nullptr, dummy_schema_));
  }

  *tuple = Tuple{values, &GetOutputSchema()};
  cursor_ += 1;

  return true;
}

}  // namespace bustub
