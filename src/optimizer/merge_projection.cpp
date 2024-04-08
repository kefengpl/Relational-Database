#include <algorithm>
#include <memory>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeMergeProjection(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child :
       plan->GetChildren()) {  // 一个 plan 可以有多层结点，它是一颗树，children 就是最上层算子的[子树，一个或者多个]
    children.emplace_back(OptimizeMergeProjection(child));  // 递归对子结点进行优化
  }
  // 提示：每个 plan 仅需知道它的 child 是谁即可。
  auto optimized_plan =
      plan->CloneWithChildren(std::move(children));  // 可以看作是克隆了该plan + 该plan 的后代(优化后的子树)

  if (optimized_plan->GetType() == PlanType::Projection) {
    const auto &projection_plan = dynamic_cast<const ProjectionPlanNode &>(*optimized_plan);
    // Has exactly one child
    BUSTUB_ENSURE(optimized_plan->children_.size() == 1, "Projection with multiple children?? That's weird!");
    // If the schema is the same (except column name)。下面的代码比较了 projection 孩子结点的 schema 和 projection 本身
    // schema 的异同
    const auto &child_plan = optimized_plan->children_[0];
    const auto &child_schema = child_plan->OutputSchema();
    const auto &projection_schema = projection_plan.OutputSchema();
    const auto &child_columns = child_schema.GetColumns();
    const auto &projection_columns = projection_schema.GetColumns();
    if (std::equal(child_columns.begin(), child_columns.end(), projection_columns.begin(), projection_columns.end(),
                   [](auto &&child_col, auto &&proj_col) {
                     // TODO(chi): consider VARCHAR length
                     return child_col.GetType() == proj_col.GetType();
                   })) {
      const auto &exprs = projection_plan.GetExpressions();
      // If all items are column value expressions
      bool is_identical = true;
      for (size_t idx = 0; idx < exprs.size();
           idx++) {  // 这个 for 循环获取了 #0.1 这样的列表达式，似乎在检验它是否是列表达式而不是(3+2)这种东西
        auto column_value_expr = dynamic_cast<const ColumnValueExpression *>(exprs[idx].get());
        if (column_value_expr != nullptr) {
          if (column_value_expr->GetTupleIdx() == 0 && column_value_expr->GetColIdx() == idx) {
            continue;
          }
        }
        is_identical = false;
        break;
      }
      if (is_identical) {
        auto plan = child_plan->CloneWithChildren(child_plan->GetChildren());
        plan->output_schema_ = std::make_shared<Schema>(projection_schema);
        return plan;
      }
    }
  }
  return optimized_plan;
}

}  // namespace bustub
