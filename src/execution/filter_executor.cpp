#include "execution/executors/filter_executor.h"
#include "common/exception.h"
#include "type/value_factory.h"

namespace bustub {

FilterExecutor::FilterExecutor(ExecutorContext *exec_ctx, const FilterPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void FilterExecutor::Init() {
  // Initialize the child executor
  child_executor_->Init();
}
// 提示：一次只能获取 1 ~ 0 个元组
auto FilterExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  auto filter_expr = plan_->GetPredicate(); // 获得谓词，所谓谓词就是 “条件”，比如 #0.0>1 (salary > 1 这种)

  while (true) {
    // Get the next tuple。 显然，FILTER 算法会采用全表扫描。逐个从子结点取出元组，然后逐个验证是否满足条件。
    const auto status = child_executor_->Next(tuple, rid); // 会将结果存入 tuple

    if (!status) { // 这是一个死循环，直到全表扫描结束，status == false，此时会返回 false，表示已经没有合适的元组了
      return false;
    }

    auto value = filter_expr->Evaluate(tuple, child_executor_->GetOutputSchema());
    if (!value.IsNull() && value.GetAs<bool>()) { 
      return true; // value != null && value == true 这说明找到了符合条件的元组，所以直接返回 true 即可。
    }
  }
}

}  // namespace bustub
