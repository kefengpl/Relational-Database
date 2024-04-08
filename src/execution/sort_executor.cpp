#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_{plan}, child_executor_{std::move(child_executor)}, tuple_list_{} {}

void SortExecutor::Init() { 
    child_executor_->Init();
    tuple_list_.clear();
    Tuple child_tuple{}; // 它本身没写移动构造,所以只有复制构造
    RID rid{};
    while (child_executor_->Next(&child_tuple, &rid)) {
        tuple_list_.push_back(child_tuple);
    } // 获取到了所有的 tuple

    // 举例: order_bys=[(Ascending, #0.0), (Descending, #0.1)]
    //! \bug 你必须写一个通用的函数，仅进行一次排序，而不是像 EXCEL 或者冒泡排序一样，先排一个，再排另一个。
    auto order_bys = plan_->GetOrderBy(); // 一个 vector, 储存了 每个字段名称和排序规则
    std::vector<OrderByType> order_by_types{};
    std::vector<AbstractExpressionRef> exprs{};
    order_by_types.reserve(order_bys.size());
    exprs.reserve(exprs.size());
    // pair <OrderByType, AbstractExpressionRef>
    for (const auto& order_pair : order_bys) {
        order_by_types.push_back(order_pair.first);
        exprs.push_back(order_pair.second);
    }
    auto comparator{[&exprs, &order_by_types, this](Tuple& tuple1, Tuple& tuple2) -> bool {
        for (size_t i = 0; i < exprs.size(); ++i) {
            AbstractExpressionRef expr{exprs[i]};
            OrderByType order_by_type{order_by_types[i]};
            Value value1{expr->Evaluate(&tuple1, this->GetOutputSchema())};
            Value value2{expr->Evaluate(&tuple2, this->GetOutputSchema())};
            if (value1.CompareEquals(value2) == CmpBool::CmpTrue) {
                // 如果两个值相等，就需要进入下一层循环去比较
                continue;
            }
            // 下面才是二者在某个属性上不相等的情况
            if (order_by_type == OrderByType::DESC) {
                return (value1.Min(value2).CompareEquals(value1) == CmpBool::CmpTrue) ? false : true;
            }
            return (value1.Min(value2).CompareEquals(value1) == CmpBool::CmpTrue) ? true : false;
        }
        // 发现两个元组所有待比较属性完全一致
        return true;
    }};
    // 按照上面指定的规则对元组进行排序
    std::sort(tuple_list_.begin(), tuple_list_.end(), comparator);
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
