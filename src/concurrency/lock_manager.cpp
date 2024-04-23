#include "concurrency/lock_manager.h"

#include "common/config.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool { 
  if (txn == nullptr) { return false; }
  // 给这个事务加大锁，防止其一致性状态发生改变
  TxnLatchGuard txn_latch_guard{txn};
  LockIllegalCheck(txn, lock_mode, oid, LockRange::TABLE); // 检查加锁合法性，失败自然会抛异常的
  std::optional<LockMode> cur_lock_mode{GetLockLevel(txn, oid, LockRange::TABLE)};
  if (cur_lock_mode != std::nullopt) { 
    if (cur_lock_mode.value() == lock_mode) {
      return true;
    } 
    // 可能需要锁升级，只要进入下面的 if 里面，说明兼容性检查通过，允许升级
    if (LockCanUpdate(txn, lock_mode, cur_lock_mode.value())) {
      return UpgradeTableLock(txn, cur_lock_mode.value(), lock_mode, oid);
    }
  }
  // 其它情况：没有锁升级，直接获取锁[当然，可能会阻塞，因为它包含一些 wait() 函数]
  TryTableLock(txn, lock_mode, oid);
  if (txn->GetState() == TransactionState::ABORTED) { // 如果发现事务被抛弃，则返回 false
    return false;
  }
  return true; 
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool { 
  if (txn == nullptr) { return false; }
  // 给这个事务加大锁，防止其一致性状态发生改变
  TxnLatchGuard txn_latch_guard{txn};
  std::optional<LockMode> cur_lock_mode{GetLockLevel(txn, oid, LockRange::TABLE)};
  if (cur_lock_mode == std::nullopt) { // 根本不持有该资源的锁
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException{txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD};
  }
  if (RowLockExist(txn, oid)) { // 行级别的锁还存在，不允许解开表锁
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException{txn->GetTransactionId(), AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS};
  }
  // 释放事务上面的锁
  DropLock(txn, oid, LockRange::TABLE, cur_lock_mode.value());
  // 改变事务的状态，提示：似乎事务提交之后 SS2PL 会自动释放所有锁，所以这里或许不用手动释放锁
  ChangeTxnState(txn, cur_lock_mode.value());
  std::unique_lock<std::mutex> table_lock_map_guard{table_lock_map_latch_};
  std::shared_ptr<LockRequestQueue> lock_request_queue{table_lock_map_[oid]};
  std::unique_lock<std::mutex> request_queue_guard{lock_request_queue->latch_}; 
  table_lock_map_guard.unlock();
  //移除请求队列中的所有与这个事务相关的锁请求(大部分情况下就是一个事务只持有相同资源的一把锁！)
  lock_request_queue->request_queue_.remove_if([&txn](const std::unique_ptr<LockRequest>& ptr) -> bool {
    return ptr != nullptr && ptr->txn_id_ == txn->GetTransactionId();
  });
  lock_request_queue->cv_.notify_all(); // 让大家再去争夺锁资源
  return true;
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid) -> bool { return true; }


auto LockManager::LockResource(Transaction *txn, LockMode lock_mode, 
                               const table_oid_t &oid, LockRange lock_range, const RID rid) -> bool {
  // 给这个事务加大锁，防止其一致性状态发生改变
  TxnLatchGuard txn_latch_guard{txn};
  LockIllegalCheck(txn, lock_mode, oid, lock_range); // 检查加锁合法性，失败自然会抛异常的
  std::optional<LockMode> cur_lock_mode{GetLockLevel(txn, oid, lock_range)};
  if (cur_lock_mode != std::nullopt) { 
    if (cur_lock_mode.value() == lock_mode) {
      return true;
    } 
    // 可能需要锁升级，只要进入下面的 if 里面，说明兼容性检查通过，允许升级
    if (LockCanUpdate(txn, lock_mode, cur_lock_mode.value())) {
      return UpgradeTableLock(txn, cur_lock_mode.value(), lock_mode, oid);
    }
  }
  // 其它情况：没有锁升级，直接获取锁[当然，可能会阻塞，因为它包含一些 wait() 函数]
  TryTableLock(txn, lock_mode, oid);
  if (txn->GetState() == TransactionState::ABORTED) { // 如果发现事务被抛弃，则返回 false
    return false;
  }
  return true; 
}

auto LockManager::UpgradeTableLock(Transaction *txn, LockMode cur_lock_mode, LockMode lock_mode, const table_oid_t &oid) -> bool {
  TryTableLock(txn, lock_mode, oid, true);
  if (txn->GetState() == TransactionState::ABORTED) { // 如果发现事务被抛弃，则返回 false
    return false;
  }
  return true;
}

auto LockManager::LockIllegalCheck(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, LockRange lock_range) -> void {
  if (txn == nullptr) {
    throw std::runtime_error("txn 不允许是空");
  }
  // 1. 读未提交级别无需也不允许获取 S IS SIX 锁
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    if (lock_mode == LockMode::SHARED || 
        lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
      txn->SetState(TransactionState::ABORTED);
      //! \note 这不是 Java，你不需要 new 异常，直接把对象本身抛出即可
      throw TransactionAbortException{txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED};   
    } 
  }  

  // 2. 行级锁不允许出现意向锁，只有两种允许：EXCLUSIVE SHARED
  if (lock_range == LockRange::ROW) {
    if (!(lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::SHARED)) {
      throw TransactionAbortException{txn->GetTransactionId(), AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW};   
    }
  }
  // 3. 收缩阶段不允许获得任何写锁，如果是可重复读，则收缩阶段不允许获得任何锁
  if (txn->GetState() == TransactionState::SHRINKING) {
    // 3.1 收缩阶段，如果是 REPEATABLE_READ， 也不允许获得任何锁
    if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException{txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING};
    }
    // 3.2 收缩阶段不允许获得任何写锁，比如 X IX。适用于 READ_UNCOMMITTED & READ_COMMITTED
    if (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::INTENTION_EXCLUSIVE) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException{txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING};   
    }
  }

  // 4. 获取行级锁必须先获取对应的表级别锁
  //! \note 
  if (lock_range == LockRange::ROW) {
    bool X_holds{txn->IsTableExclusiveLocked(oid) || txn->IsTableIntentionExclusiveLocked(oid) || 
                 txn->IsTableSharedIntentionExclusiveLocked(oid)}; // 是否持有任何 X 类锁：X IX SIX ?
    bool S_holds{txn->IsTableSharedLocked(oid) || txn->IsTableIntentionSharedLocked(oid)}; // 是否持有任何 S 类锁：S IS ?
    if (!X_holds && lock_mode == LockMode::EXCLUSIVE) {
      throw TransactionAbortException{txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT};   
    }
    if (!(X_holds || S_holds) && lock_mode == LockMode::SHARED) {
      throw TransactionAbortException{txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT};   
    }
  } 
}

auto LockManager::LockCanUpdate(Transaction *txn, LockMode lock_mode, LockMode cur_lock_mode) -> bool {
  if (txn == nullptr) {
    throw std::runtime_error("txn 不允许是空");
  }
  if (upgrade_rules_.find(cur_lock_mode) == upgrade_rules_.end()) {
    throw TransactionAbortException{txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE};
  }
  if (upgrade_rules_[cur_lock_mode].find(lock_mode) == upgrade_rules_[cur_lock_mode].end()) {
    throw TransactionAbortException{txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE};
  }
  return true;
}

auto LockManager::GetLockLevel(Transaction *txn, const table_oid_t &oid, 
                               LockRange lock_range, const RID rid) -> std::optional<LockMode> {
  if (txn == nullptr) {
    throw std::runtime_error("txn 不允许是空");
  }
  if (lock_range == LockRange::TABLE) {
    if (txn->IsTableExclusiveLocked(oid)) {
      return std::optional{LockMode::EXCLUSIVE};
    }
    if (txn->IsTableIntentionExclusiveLocked(oid)) {
      return std::optional{LockMode::INTENTION_EXCLUSIVE};
    }
    if (txn->IsTableIntentionSharedLocked(oid)) {
      return std::optional{LockMode::INTENTION_SHARED};
    }
    if (txn->IsTableSharedIntentionExclusiveLocked(oid)) {
      return std::optional{LockMode::SHARED_INTENTION_EXCLUSIVE};
    }
    if (txn->IsTableSharedLocked(oid)) {
      return std::optional{LockMode::SHARED};
    }
  } else { // lock_range == LockRange::ROW
    if (txn->IsRowExclusiveLocked(oid, rid)) {
      return std::optional{LockMode::EXCLUSIVE};
    }
    if (txn->IsRowSharedLocked(oid, rid)) {
      return std::optional{LockMode::SHARED};
    }
  }
  return std::nullopt; 
}

auto LockManager::TxnTableLockSet(Transaction *txn, LockMode lock_mode) -> std::shared_ptr<std::unordered_set<table_oid_t>> {
  if (txn == nullptr) {
    throw std::runtime_error("txn 不允许是空");
  }  
  switch (lock_mode) {
    case LockMode::EXCLUSIVE:
      return txn->GetExclusiveTableLockSet();
    case LockMode::SHARED:
      return txn->GetSharedTableLockSet();
    case LockMode::INTENTION_EXCLUSIVE:
      return txn->GetIntentionExclusiveTableLockSet();
    case LockMode::INTENTION_SHARED:
      return txn->GetIntentionSharedTableLockSet();   
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      return txn->GetSharedIntentionExclusiveTableLockSet();       
  }
  return nullptr;
}

auto LockManager::TxnRowLockSet(Transaction *txn, LockMode lock_mode) -> std::shared_ptr<std::unordered_map<table_oid_t, std::unordered_set<RID>>> {
  if (txn == nullptr) {
    throw std::runtime_error("txn 不允许是空");
  }  
  switch (lock_mode) {
    case LockMode::SHARED:
      return txn->GetSharedRowLockSet();
    case LockMode::EXCLUSIVE:
      return txn->GetExclusiveRowLockSet();
    default:
      return nullptr;
  }  
}

auto LockManager::AddLock(Transaction *txn, const table_oid_t &oid, LockRange lock_range, LockMode lock_mode, const RID rid) -> void {
  if (txn == nullptr) {
    throw std::runtime_error("txn 不允许是空");
  }
  if (lock_range == LockRange::TABLE) {
    TxnTableLockSet(txn, lock_mode)->insert(oid);
  } else { // lock_range == LockRange::ROW
    (*TxnRowLockSet(txn, lock_mode))[oid].insert(rid);
  }  
}

auto LockManager::DropLock(Transaction *txn, const table_oid_t &oid, LockRange lock_range, LockMode lock_mode, const RID rid) -> void {
  if (txn == nullptr) {
    throw std::runtime_error("txn 不允许是空");
  }
  if (lock_range == LockRange::TABLE) {
    TxnTableLockSet(txn, lock_mode)->erase(oid);
  } else { // lock_range == LockRange::ROW
    (*TxnRowLockSet(txn, lock_mode))[oid].erase(rid);
  }   
}

auto LockManager::TryTableLock(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, bool upgrade) -> void {
  std::unique_lock<std::mutex> table_lock_map_guard{table_lock_map_latch_};
  // 1. 争做吃螃蟹的第一人，还没有任何事务在这个表上加锁。注意：为了线程安全，后面即便队列清空，也不会删除这个创建的队列了
  if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    table_lock_map_[oid] = std::make_shared<LockRequestQueue>();
    table_lock_map_[oid]->request_queue_.push_back(std::make_unique<LockRequest>(txn->GetTransactionId(), lock_mode, oid));
    table_lock_map_[oid]->request_queue_.back()->granted_ = true;
    AddLock(txn, oid, LockRange::TABLE, lock_mode);
    return;
  }
  // 2. 发现这个表的请求队列已经建立了，则需要监测有哪些既得利益者正在持有锁[注意：你监测到第一个不持有锁的即可]
  std::shared_ptr<LockRequestQueue> lock_request_queue{table_lock_map_[oid]};
  //! \note 这里或许可以释放 table_lock_map_guard 的锁了
  std::unique_lock<std::mutex> request_queue_guard{lock_request_queue->latch_};
  table_lock_map_guard.unlock();
  if (upgrade && lock_request_queue->upgrading_ != INVALID_TXN_ID) { // 只允许有一个事务进行升级
    throw TransactionAbortException{txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT};   
  }
  //! \bug 典型错误：只有升级的时候才需要 if 里面的代码 ！
  if (upgrade) { 
    lock_request_queue->upgrading_ = txn->GetTransactionId();
    // 删除事务里面对该资源的锁记录以及队列的锁请求
    DropLock(txn, oid, LockRange::TABLE, GetLockLevel(txn, oid, LockRange::TABLE).value());
    lock_request_queue->request_queue_.remove_if([&txn](const std::unique_ptr<LockRequest>& ptr) -> bool {
      return ptr != nullptr && ptr->txn_id_ == txn->GetTransactionId();
    });
  }

  LockRequest* this_request{lock_request_queue->InsertToRequestQueue(txn, lock_mode, oid, upgrade)};
  lock_request_queue->cv_.wait(request_queue_guard, [&]() -> bool {
    return (txn->GetState() == TransactionState::ABORTED || CanGrantLock(lock_request_queue, request_queue_guard, this_request));
  });
  // 由于事务中途被抛弃，获取锁宣告失败，生成的锁请求也需要取消[注意：我们在 PreviousLockReuqests 中考虑了中途 ABORTED 的情况]
  if (txn->GetState() == TransactionState::ABORTED) {
      lock_request_queue->request_queue_.remove_if([&txn](const std::unique_ptr<LockRequest>& ptr) -> bool {
      return ptr != nullptr && ptr->txn_id_ == txn->GetTransactionId();
    });
    return;
  }
  
  // 添加新的锁，因为是满足锁兼容性条件的。
  this_request->granted_ = true;
  AddLock(txn, oid, LockRange::TABLE, lock_mode);
  if (upgrade) { // 锁升级完成，你需要把升级标签改回去
    lock_request_queue->upgrading_ = INVALID_TXN_ID;
  }
}

auto LockManager::CanGrantLock(std::shared_ptr<LockRequestQueue>& lock_request_queue, 
                               std::unique_lock<std::mutex>& request_queue_guard, LockRequest* request_addr) -> bool {
  // 从前往后检查，遍历当前所有锁请求的类型[granted & non-granted]
  std::unordered_set<LockMode> cur_lock_requests{lock_request_queue->PreviousLockReuqests(request_addr)};
  for (LockMode cur_lock_mode : cur_lock_requests) {
    if (!ConflictChecker::CanCoexistence(cur_lock_mode, request_addr->lock_mode_)) {
      return false; // 该事务不可获取锁，因为与它前面的元素共存冲突
    }
  }  
  return true;
}

std::unordered_map<LockManager::LockMode, std::unordered_set<LockManager::LockMode>> 
LockManager::ConflictChecker::COEXISTENCE_MAP{
  {LockMode::INTENTION_SHARED, {LockMode::INTENTION_SHARED, LockMode::INTENTION_EXCLUSIVE, 
                                  LockMode::SHARED, LockMode::SHARED_INTENTION_EXCLUSIVE}}, 
  {LockMode::INTENTION_EXCLUSIVE, {LockMode::INTENTION_SHARED, LockMode::INTENTION_EXCLUSIVE}},
  {LockMode::SHARED, {LockMode::INTENTION_SHARED, LockMode::SHARED}},
  {LockMode::SHARED_INTENTION_EXCLUSIVE, {LockMode::INTENTION_SHARED}},
  {LockMode::EXCLUSIVE, {}},
};

auto LockManager::ConflictChecker::CanCoexistence(LockMode lock_mode1, LockMode lock_mode2) -> bool {
  return COEXISTENCE_MAP[lock_mode1].find(lock_mode2) != COEXISTENCE_MAP[lock_mode1].end();
}

auto LockManager::LockRequestQueue::PreviousLockReuqests(LockRequest* lock_request) -> std::unordered_set<LockMode> {
  std::unordered_set<LockMode> result{};
  for (std::unique_ptr<LockRequest>& cur_lock_request : request_queue_) {
    if (lock_request == cur_lock_request.get()) {
      break;
    }
    // 如果某个事务 ABORT 状态，那么这样的锁也是无效的，直接 continue
    if (TransactionManager::GetTransaction(cur_lock_request->txn_id_)->GetState() == TransactionState::ABORTED) {
      continue;
    }
    result.insert(cur_lock_request->lock_mode_);
  }
  return result;
}

auto LockManager::LockRequestQueue::InsertToRequestQueue(Transaction *txn, LockMode lock_mode, 
                                                         const table_oid_t &oid, bool upgrade) -> LockRequest* {
  if (!upgrade) {
    request_queue_.push_back(std::make_unique<LockRequest>(txn->GetTransactionId(), lock_mode, oid));
    return request_queue_.back().get();
  } 
  auto it{request_queue_.begin()};
  while (it != request_queue_.end() && (*it)->granted_) {
    std::advance(it, 1);
  }
  if (it == request_queue_.end()) {
    return InsertToRequestQueue(txn, lock_mode, oid);
  }
  LockRequest* request{new LockRequest{txn->GetTransactionId(), lock_mode, oid}};
  request_queue_.insert(it, std::unique_ptr<LockRequest>{request});
  return request;
}

auto LockManager::RowLockExist(Transaction *txn, const table_oid_t &oid) -> bool {
  auto x_row_lock_set{txn->GetExclusiveRowLockSet()};
  auto row_lock_set = x_row_lock_set->find(oid);
  if (row_lock_set == x_row_lock_set->end()) {
    return false;
  }
  return !row_lock_set->second.empty();
}

auto LockManager::ChangeTxnState(Transaction *txn, LockMode lock_mode) -> void {
  if (txn == nullptr) {
    throw std::runtime_error("txn 不允许是空");
  }
  if (txn->GetState() == TransactionState::COMMITTED || txn->GetState() == TransactionState::ABORTED) {
    return;
  }
  if (lock_mode == LockMode::EXCLUSIVE) {
    txn->SetState(TransactionState::SHRINKING);
    return;
  } 
  if (lock_mode == LockMode::SHARED || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
    if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
      txn->SetState(TransactionState::SHRINKING);
    }
  }
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool { return false; }

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);
  return edges;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {  // TODO(students): detect deadlock
    }
  }
}

}  // namespace bustub
