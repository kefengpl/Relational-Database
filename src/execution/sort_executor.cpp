#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_{plan}, child_executor_{std::move(child_executor)}, tuple_list_{} {}

void SortExecutor::Init() { 
    child_executor_->Init();
    Tuple child_tuple{}; // 它本身没写移动构造,所以只有复制构造
    RID rid{};
    while (child_executor_->Next(&child_tuple, &rid)) {
        tuple_list_.push_back(child_tuple);
    } // 获取到了所有的 tuple

    // 举例: order_bys=[(Ascending, #0.0), (Descending, #0.1)]
    auto order_bys = plan_->GetOrderBy(); // 一个 vector, 储存了 每个字段名称和排序规则
    // pair <OrderByType, AbstractExpressionRef>
    for (const auto& order_pair : order_bys) {
        OrderByType order_by_type{order_pair.first};
        AbstractExpressionRef expr{order_pair.second};
        auto comparator{[&expr, &order_by_type, this](Tuple& tuple1, Tuple& tuple2) -> bool {
            Value value1{expr->Evaluate(&tuple1, this->GetOutputSchema())};
            Value value2{expr->Evaluate(&tuple2, this->GetOutputSchema())};
            if (order_by_type == OrderByType::DESC) {
                return value1.Min(value2).Subtract(value1).IsZero() ? false : true;
            }
            return value1.Min(value2).Subtract(value1).IsZero() ? true : false;
        }};
        // 按照上面指定的规则对元组进行排序
        std::sort(tuple_list_.begin(), tuple_list_.end(), comparator);
    }
    cursor_ = 0; // 设置初始游标是 0 
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
    if (cursor_ == tuple_list_.size()) {
        return false;
    }
    *tuple = tuple_list_[cursor_++];
    *rid = tuple->GetRid();
    return true;
}

}  // namespace bustub
