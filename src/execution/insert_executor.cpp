
#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_{plan}, child_executor_{std::move(child_executor)},
      table_info_{exec_ctx_->GetCatalog()->GetTable(plan_->TableOid())}, 
      table_heap_{table_info_->table_.get()}, insert_num_{Value{TypeId::INTEGER, 0}}, reentrance_{false} {}

void InsertExecutor::Init() { 
    child_executor_->Init(); 
    insert_num_ = Value{TypeId::INTEGER, 0};
    reentrance_ = false;
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
        if (!reentrance_) { // 没有元素，且不是重入，那么返回 0.因为此时确实没有可用元素
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
        table_heap_->InsertTuple(child_tuple, rid, exec_ctx_->GetTransaction());
        // 插入一个元素就立即对该表的所有索引进行更新
        for (IndexInfo* index_info : index_info_list) {
            index_info->index_->InsertEntry(child_tuple, *rid, exec_ctx_->GetTransaction());
        }
        insert_num_ = insert_num_.Add(Value(TypeId::INTEGER, 1));
    } while (child_executor_->Next(&child_tuple, rid));
    // 写一个输出
    *tuple = Tuple{std::vector<Value>{insert_num_}, &GetOutputSchema()};
    reentrance_ = true; // 下次再进入就是 reentrance_ 了
    return true;
}

}  // namespace bustub
