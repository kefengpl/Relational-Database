
#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_{plan},
      child_executor_{std::move(child_executor)},
      table_info_{exec_ctx_->GetCatalog()->GetTable(plan_->TableOid())},
      table_heap_{table_info_->table_.get()},
      insert_num_{Value{TypeId::INTEGER, 0}},
      reentrance_{false},
      txn_{exec_ctx_->GetTransaction()},
      lock_manager_{exec_ctx_->GetLockManager()} {}
/**
 * 无论何种隔离级别，都需要获取表级别的意向写锁
 * @note 由于意向锁 IX 需要在行锁没有的时候才能释放，因此它需要在最后释放，这里无需手动释放
 */
void InsertExecutor::Init() {
  child_executor_->Init();
  insert_num_ = Value{TypeId::INTEGER, 0};
  reentrance_ = false;
  lock_manager_->LockTableWrapper(txn_, LockManager::LockMode::INTENTION_EXCLUSIVE, plan_->TableOid());
}
/**
 * The planner will ensure values have the same schema as the table.
 * 这意味着你无需检查插入元组的 Schema 与 table 是否相符，因为 planner 已经替你检查过了
 * @note 这里面 rid 参数是没有任何作用的，除了传参传来传去图一乐以外
 */
auto InsertExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  Tuple child_tuple{};
  // 获取表上的索引。提示：BPlussTreeIndex(继承自 Index) 包含一个 BPlusTree。从而可以使用你写的 B+ 树
  std::vector<IndexInfo *> index_info_list{exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_)};
  // 循环获取 values 中的 tuple。事实上，采用顺序插入，直接插入到文件尾部即可。
  if (!child_executor_->Next(&child_tuple, rid)) {
    if (!reentrance_) {  // 没有元素，且不是重入，那么返回 0.因为此时确实没有可用元素
      *tuple = Tuple{std::vector<Value>{insert_num_}, &GetOutputSchema()};
      reentrance_ = true;
      return true;
    }
    // 没有元素，并且插入了元素，说明是重复进入，直接返回 false
    reentrance_ = true;
    return false;
  }
  do {
    //! \note 这里的 InsertTuple 的 rid 是输出参数，table_heap_ 会自动为新插入的元组分配 rid
    //! \note 提示：这个函数会自动维护 事务的 write_set()。你只需要手动维护 index_write_set();
    table_heap_->InsertTuple(child_tuple, rid, exec_ctx_->GetTransaction());
    //! \note 由于 IX 锁在上面，所以表级别的读取是无法读取到这个元素的，或许不需要对新插入的元组进行加锁。
    lock_manager_->LockRowWrapper(txn_, LockManager::LockMode::EXCLUSIVE, plan_->TableOid(), *rid);
    // 插入一个元素就立即对该表的所有索引进行更新
    for (IndexInfo *index_info : index_info_list) {
      //! \bug 你必须获得 (key) 这种字段！而不是整个元组...
      Tuple key{child_tuple.KeyFromTuple(child_executor_->GetOutputSchema(), *(index_info->index_->GetKeySchema()),
                                         index_info->index_->GetKeyAttrs())};
      index_info->index_->InsertEntry(key, *rid, exec_ctx_->GetTransaction());
      // 记录对索引的更新
      txn_->GetIndexWriteSet()->emplace_back(*rid, plan_->TableOid(), WType::INSERT, key, index_info->index_oid_,
                                             exec_ctx_->GetCatalog());
    }
    insert_num_ = insert_num_.Add(Value(TypeId::INTEGER, 1));
  } while (child_executor_->Next(&child_tuple, rid));
  // 写一个输出
  *tuple = Tuple{std::vector<Value>{insert_num_}, &GetOutputSchema()};
  reentrance_ = true;  // 下次再进入就是 reentrance_ 了
  return true;
}

}  // namespace bustub
