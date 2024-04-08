#include "execution/executors/topn_executor.h"

namespace bustub {
// 正式构造函数
TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    // 提示：委托构造函数与其它初始化列表禁止混合使用
    : AbstractExecutor{exec_ctx}, plan_{plan}, child_executor_{std::move(child_executor)} {  
        auto order_bys = plan_->GetOrderBy(); // 一个 vector, 储存了 每个字段名称和排序规则
        order_by_types_.reserve(order_bys.size());
        exprs_.reserve(exprs_.size());
        // pair <OrderByType, AbstractExpressionRef>
        for (const auto& order_pair : order_bys) {
            order_by_types_.push_back(order_pair.first);
            exprs_.push_back(order_pair.second);
        }
    }

void TopNExecutor::Init() { 
    child_executor_->Init();
    topn_elems_.clear();
    // 扫描全表，逐步构建起优先队列[top 是较小值]
    Tuple child_tuple{};
    RID child_rid{};
    while (child_executor_->Next(&child_tuple, &child_rid)) {
        if (topn_elems_.size() < plan_->GetN()) { // 优先队列未满
            push(child_tuple);
        } else { // 优先队列满了，根据条件剔除堆顶元素
            if (!Comparator(top(), child_tuple)) {
                pop();
                push(child_tuple);
            }
        }
    }
    // 你需要对 vector 进行排序[]注意：堆本身并不是有序的]
    std::sort(topn_elems_.begin(), topn_elems_.end(), [this](const Tuple& tuple1, const Tuple& tuple2){
        return this->Comparator(tuple1, tuple2);
    });
    // 然后再将元素排序后顺序喷出即可
    cursor_ = 0;
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
    if (cursor_ == topn_elems_.size()) {
        return false;
    }
    *tuple = topn_elems_[cursor_++];
    *rid = tuple->GetRid();
    return true;
}

auto TopNExecutor::Comparator(const Tuple& tuple1, const Tuple& tuple2) -> bool {
    for (size_t i = 0; i < exprs_.size(); ++i) {
        AbstractExpressionRef expr{exprs_[i]};
        OrderByType order_by_type{order_by_types_[i]};
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
}

void TopNExecutor::heapify(size_t idx) {
    size_t n{topn_elems_.size()};
    size_t lchild_idx{2 * idx + 1};
    size_t rchild_idx{2 * idx + 2};
    size_t largest{idx};
    // 先和左侧打擂台
    if (lchild_idx < n && Comparator(topn_elems_[largest], topn_elems_[lchild_idx])) {
        largest = lchild_idx;
    }
    // 再和右侧打擂台
    if (rchild_idx < n && Comparator(topn_elems_[largest], topn_elems_[rchild_idx])) {
        largest = rchild_idx;
    }
    if (largest != idx) {
        std::swap(topn_elems_[largest], topn_elems_[idx]);
        heapify(largest);
    }
}

void TopNExecutor::RefreshHeap() {
    size_t n{topn_elems_.size()};
    if (n <= 1) { return; }
    for (int i = n / 2 - 1; i >= 0; --i) {
        heapify(i);
    }
}

Tuple& TopNExecutor::top() {
    if (!topn_elems_.empty()) {
        return topn_elems_[0];
    }
    throw "堆已经是空的了";
}

void TopNExecutor::pop() {
    if (!topn_elems_.empty()) {
        topn_elems_.erase(topn_elems_.begin());
        RefreshHeap();
        return;
    }
    throw "堆已经是空的了";
}

void TopNExecutor::push(Tuple& tuple) {
    if (topn_elems_.size() == plan_->GetN()) {
        throw "堆已经满了";
    }
    topn_elems_.push_back(tuple);
    RefreshHeap();
}

}  // namespace bustub
