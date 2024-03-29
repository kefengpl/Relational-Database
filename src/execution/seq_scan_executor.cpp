#include "execution/executors/seq_scan_executor.h"

namespace bustub {

/**
 * 提示：指向 unique_ptr 的裸指针 -> 只能访问 unique_ptr 的 . 方法(比如 get())。
 * 但是 unique_ptr 重载的 -> (访问具体的函数) 你必须通过 裸指针->get()->func() 这种方法调用
*/
SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : 
    AbstractExecutor(exec_ctx), plan_{plan}, table_heap_ptr_{&(exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->table_)},
    table_iterator_{table_heap_ptr_->get()->Begin(exec_ctx_->GetTransaction())} {}

void SeqScanExecutor::Init() {}
// 这是一个单表顺序扫描的算子，暂时不用考虑表连接(join)的情况
auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
    // 没有元素了，直接返回 false 
    if (table_iterator_ == table_heap_ptr_->get()->End()) {
        return false;
    }

    // 其它情况需要返回一个元组
    *tuple = (*table_iterator_);
    *rid = table_iterator_->GetRid();
    ++table_iterator_;
    return true; 
}

}  // namespace bustub
