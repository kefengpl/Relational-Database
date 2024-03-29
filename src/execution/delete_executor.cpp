#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_{plan}, child_executor_{std::move(child_executor)},
      table_info_{exec_ctx_->GetCatalog()->GetTable(plan_->TableOid())}, 
      table_heap_{table_info_->table_.get()} {}

void DeleteExecutor::Init() { 
    child_executor_->Init(); 
    reentrant_ = false;
}

auto DeleteExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
    Tuple child_tuple{};
    Value delete_num{TypeId::INTEGER, 0};
    //! \note 此时的 rid 是有用的，可以被当作存储变量
    if (!child_executor_->Next(&child_tuple, rid)) {
        if (!reentrant_) {
            *tuple = Tuple{std::vector<Value>{delete_num}, &GetOutputSchema()};
            reentrant_ = true;
            return true;
        }
        reentrant_ = true; // 多余语句...
        return false;
    }
    std::vector<IndexInfo *> index_info_list{exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_)};
    do {
        // 删除元组并更新索引
        table_heap_->MarkDelete(*rid, exec_ctx_->GetTransaction());
        for (IndexInfo* index_info : index_info_list) {
            //! \bug 你必须获得 (key) 这种字段！而不是整个元组...
            Tuple key{child_tuple.KeyFromTuple(child_executor_->GetOutputSchema(), 
                                     *(index_info->index_->GetKeySchema()), index_info->index_->GetKeyAttrs())};
            index_info->index_->DeleteEntry(key, *rid, exec_ctx_->GetTransaction());
        }
        delete_num = delete_num.Add(Value(TypeId::INTEGER, 1));
    } while (child_executor_->Next(&child_tuple, rid));
    // 写一个输出
    *tuple = Tuple{std::vector<Value>{delete_num}, &GetOutputSchema()};
    reentrant_ = true;
    return true;
}

}  // namespace bustub
