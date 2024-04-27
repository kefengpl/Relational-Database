#include "execution/executors/seq_scan_executor.h"

namespace bustub {

/**
 * 提示：指向 unique_ptr 的裸指针 -> 只能访问 unique_ptr 的 . 方法(比如 get())。
 * 但是 unique_ptr 重载的 -> (访问具体的函数) 你必须通过 裸指针->get()->func() 这种方法调用
 */
SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_{plan},
      table_heap_ptr_{&(exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->table_)},
      table_iterator_{table_heap_ptr_->get()->Begin(exec_ctx_->GetTransaction())},
      txn_{exec_ctx_->GetTransaction()},
      lock_manager_{exec_ctx_->GetLockManager()} {}
/**
 * 全表扫描或许只需要加表级别的锁，即加上 S 锁
 * @note 上面只是理论，事实上，你必须加 IS + S(行) 锁。否则会报错。
 * @note READ_UNCOMMITTED 隔离级别无需加任何锁。
 */
void SeqScanExecutor::Init() {
  if (txn_->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
    lock_manager_->LockTableWrapper(txn_, LockManager::LockMode::INTENTION_SHARED, plan_->GetTableOid());
  }
  table_iterator_ = table_heap_ptr_->get()->Begin(txn_);
}
// 这是一个单表顺序扫描的算子，暂时不用考虑表连接(join)的情况
auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // 没有元素了，直接返回 false
  if (table_iterator_ == table_heap_ptr_->get()->End()) {
    if (txn_->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
      lock_manager_->UnlockTableWrapper(txn_, plan_->GetTableOid());
    }
    return false;
  }

  // 其它情况需要返回一个元组
  *rid = table_iterator_->GetRid();
  if (txn_->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
    lock_manager_->LockRowWrapper(txn_, LockManager::LockMode::SHARED, plan_->GetTableOid(), *rid);
  }
  *tuple = (*table_iterator_);
  if (txn_->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
    lock_manager_->UnLockRowWrapper(txn_, plan_->GetTableOid(), *rid);
  }
  ++table_iterator_;
  return true;
}

}  // namespace bustub
