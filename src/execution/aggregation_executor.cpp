#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor{exec_ctx}, plan_{plan}, child_{std::move(child)}, 
      aht_{plan_->aggregates_, plan_->agg_types_}, aht_iterator_{aht_.Begin()} {} 

void AggregationExecutor::Init() {
    child_->Init();
    aht_.Clear(); //! \bug 每次初始化都需要清空原有汇总表，防止汇总结果每调用一次 Init 就进行一次累加
    Tuple child_tuple{};
    RID child_tuple_id{};
    // 处理空表的情况
    if (!child_->Next(&child_tuple, &child_tuple_id)) {
        if (plan_->group_bys_.size() != 0) { // 有 group by 并且表是空的，那么直接返回
            return;
        }
        aht_.InsertCombine(AggregateKey{}, AggregateValue{});
        aht_iterator_ = aht_.Begin();
        return;
    }
    std::vector<AbstractExpressionRef> agg_exprs{plan_->aggregates_}; // 几个聚合函数应用的列分别是何者？
    std::vector<AbstractExpressionRef> group_bys{plan_->group_bys_}; // group_by_ 的几个字段分别是何者？          
    // std::vector<AggregationType> agg_functions{plan_->agg_types_}; // 一个 select 语句应用了哪些聚合函数？
    // 把对应列的值取出来，并加入到内存中的哈希表中
    // 提示：这里的 AggregateKey，AggregateValue 都可以包含多列，它们本质上都是 std::vector<Value>
    std::vector<Value> agg_keys(group_bys.size());
    std::vector<Value> agg_values(agg_exprs.size()); 
    do {
        // 先获得聚合 key，注意聚合 key 可能是0，表示 没有 group by
        for (size_t i = 0; i < group_bys.size(); ++i) {
            agg_keys[i] = group_bys[i]->Evaluate(&child_tuple, child_->GetOutputSchema());
        }
        // 然后获得聚合 value
        for (size_t i = 0; i < agg_exprs.size(); ++i) {
            agg_values[i] = agg_exprs[i]->Evaluate(&child_tuple, child_->GetOutputSchema());
        }
        AggregateValue aggregate_value{agg_values};
        AggregateKey aggregate_key{agg_keys};
        // 你需要处理从表格中蹦出来的元组。此时的 key(s) 就是 group_py 的 key，value(s) 就是所有聚合函数里面表达式在该元组的值。
        aht_.InsertCombine(aggregate_key, aggregate_value);
    } while (child_->Next(&child_tuple, &child_tuple_id));
    // 初始化迭代器
    aht_iterator_ = aht_.Begin();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
    if (aht_iterator_ == aht_.End()) {
        aht_.Clear(); // 如果游标已经移动到末尾了，就直接清空数组即可
        return false;
    }
    const std::vector<Value>& keys{aht_iterator_.Key().group_bys_};
    const std::vector<Value>& values{aht_iterator_.Val().aggregates_};
    std::vector<Value> result_values{};
    result_values.reserve(keys.size() + values.size()); // 预留空间，即 End() 指针不会移动到空间末尾
    result_values.insert(result_values.end(), keys.begin(), keys.end());
    result_values.insert(result_values.end(), values.begin(), values.end());
    //! \bug 是 result_values，不是 values ！
    *tuple = Tuple{result_values, &GetOutputSchema()};
    ++aht_iterator_;
    return true;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { 
    return child_.get(); 
}

}  // namespace bustub
