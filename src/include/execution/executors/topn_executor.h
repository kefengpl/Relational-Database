#pragma once

#include <memory>
#include <queue>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/topn_plan.h"
#include "storage/table/tuple.h"

namespace bustub {
// using TupleHeap = std::priority_queue<Tuple, std::vector<Tuple>, std::function<bool(const Tuple&, const Tuple&)>>;
/**
 * The TopNExecutor executor executes a topn.
 */
class TopNExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new TopNExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The topn plan to be executed
   */
  TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan, std::unique_ptr<AbstractExecutor> &&child_executor);

  /**
   * 委托构造函数，其目的是为了正确初始化优先队列
   */
  TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan);

  /** Initialize the topn */
  void Init() override;

  /**
   * Yield the next tuple from the topn.
   * @param[out] tuple The next tuple produced by the topn
   * @param[out] rid The next tuple RID produced by the topn
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the topn */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

  /**
   * 调整，以 idx 为根结点，对堆进行调整。[注意是大顶堆]
   */
  void Heapify(size_t idx);
  /**
   * 我们需要构建大顶堆，以便于这个大顶能够及时被替换。(较小的元组才是先输出的元组)
   */
  void RefreshHeap();
  /**
   * 经典的堆方法
   */
  auto Top() -> Tuple &;
  void Pop();
  void Push(Tuple &tuple);
  auto Comparator(const Tuple &tuple1, const Tuple &tuple2) -> bool;

 private:
  /** The topn plan node to be executed */
  const TopNPlanNode *plan_;
  std::unique_ptr<AbstractExecutor> child_executor_;
  std::vector<OrderByType> order_by_types_;
  std::vector<AbstractExpressionRef> exprs_;
  // std::unique_ptr<TupleHeap> topn_elems_;
  /** 自定义比较器，需要运行时动态生成 lambda 表达式，直观理解：如果返回 true，则第一个 tuple 是 "较小"的 */
  // decltype([&exprs, &order_by_types, this](const Tuple& tuple1, const Tuple& tuple2) -> bool{return true;})
  // comparator_;
  /** 专门为优先队列准备的比较器，方向与 comparator_ 相反 */
  // std::function<bool(const Tuple&, const Tuple&)> rev_comparator_;
  std::vector<Tuple> topn_elems_;
  size_t cursor_;
};
}  // namespace bustub
