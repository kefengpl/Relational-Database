#include "execution/executors/projection_executor.h"
#include "storage/table/tuple.h"

namespace bustub {

ProjectionExecutor::ProjectionExecutor(ExecutorContext *exec_ctx, const ProjectionPlanNode *plan,
                                       std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void ProjectionExecutor::Init() {
  // Initialize the child executor
  child_executor_->Init();
}
// 从 child 中获取一个结果？
auto ProjectionExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  Tuple child_tuple{};

  // Get the next tuple
  const auto status = child_executor_->Next(&child_tuple, rid);
  if (!status) {
    return false;
  }

  // Compute expressions
  std::vector<Value> values{}; // 收集一个元组中的各种数据 [即便是 select 2 + 3 这种，也需要返回一个元组 (5)]
  values.reserve(GetOutputSchema().GetColumnCount()); // 返回这个算子输出的列数，从而设定 values (vector) 的大小(resize)
  for (const auto &expr : plan_->GetExpressions()) { // expr 转化为字符串比如 #0.0 (2 + 3) 这两种都算
    //! \note 它是输入是 子算子执行器返回的一个元组，以及这个子算子执行器返回元组对应的关系模式[子算子输出的关系模式]
    values.push_back(expr->Evaluate(&child_tuple, child_executor_->GetOutputSchema())); // 计算这个算子的表达式结果。
  }

  *tuple = Tuple{values, &GetOutputSchema()};

  return true;
}
}  // namespace bustub
