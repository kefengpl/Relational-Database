#include "optimizer/optimizer.h"
#include "execution/plans/limit_plan.h"
#include "execution/plans/sort_plan.h"
#include "execution/plans/topn_plan.h"
#include "execution/expressions/column_value_expression.h"

namespace bustub {

auto Optimizer::OptimizeSortLimitAsTopN(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children{};
  // 提示：在某些情况下，此时 projection + seqscan 已经被优化为 seqscan 了。
  for (const auto& child : plan->GetChildren()) { 
    children.emplace_back(OptimizeMergeProjection(child)); 
  }
  AbstractPlanNodeRef optimized_plan{plan->CloneWithChildren(std::move(children))}; // 重新克隆了头结点本身以及优化后的子树
  if (optimized_plan->GetType() != PlanType::Limit) {
    return optimized_plan;
  }
  // 核心逻辑：如果 plan 的最上层就是 limit
  const LimitPlanNode& limit_plan{dynamic_cast<const LimitPlanNode &>(*optimized_plan)};
  BUSTUB_ENSURE(optimized_plan->children_.size() == 1, "limit 下面理应只能有一个孩子。");
  const AbstractPlanNodeRef &child_plan{optimized_plan->children_[0]}; // 获得孩子的计划
  if (child_plan->GetType() != PlanType::Sort) { // limit 必须紧接着 sort 才能进行优化
    return optimized_plan;
  }
  const SortPlanNode &sort_plan{dynamic_cast<const SortPlanNode &>(*child_plan)};
  const auto &order_bys = sort_plan.GetOrderBy(); // 下面需要检查 order_bys 的表达式是否都是列表达式
  /*for (const auto& order_by : order_bys) {
    const auto& [order_type, expr] = order_by;
    // Order expression is a column value expression // 检查 #0.1 这样的列表达式是否有效
    //! \note 最主要的是 排除 2 + 3 这种算术表达式的情况
    const auto *column_value_expr = dynamic_cast<ColumnValueExpression *>(expr.get()); 
    if (column_value_expr == nullptr) {
      return optimized_plan;
    }
  }*/
  // 顶层结点 limit + sort 二合一
  return std::make_shared<TopNPlanNode>(optimized_plan->output_schema_, child_plan->children_[0], order_bys, limit_plan.GetLimit());
}

}  // namespace bustub
