#include "concurrency/lock_manager.h"

#include "common/config.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

std::unordered_map<LockManager::LockMode, std::string> mode_map{
    {LockManager::LockMode::SHARED, "S"},
    {LockManager::LockMode::EXCLUSIVE, "X"},
    {LockManager::LockMode::INTENTION_SHARED, "IS"},
    {LockManager::LockMode::INTENTION_EXCLUSIVE, "IX"},
    {LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE, "SIX"}};

std::unordered_map<LockManager::LockRange, std::string> range_map{{LockManager::LockRange::ROW, "ROW"},
                                                                  {LockManager::LockRange::TABLE, "TABLE"}};

void LockManager::LockTableWrapper(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) {
  try {
    if (!this->LockTable(txn, lock_mode, oid)) {
      throw ExecutionException("LockTable failed");
    }
  } catch (TransactionAbortException &e) {
    throw ExecutionException("LockTable failed");
  }
}
void LockManager::UnlockTableWrapper(Transaction *txn, const table_oid_t &oid) {
  try {
    this->UnlockTable(txn, oid);
  } catch (TransactionAbortException &e) {
    throw ExecutionException("UnLockTable failed");
  }
}
void LockManager::LockRowWrapper(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) {
  try {
    if (!this->LockRow(txn, lock_mode, oid, rid)) {
      throw ExecutionException("LockRow failed");
    }
  } catch (TransactionAbortException &e) {
    throw ExecutionException("LockRow failed");
  }
}
void LockManager::UnLockRowWrapper(Transaction *txn, const table_oid_t &oid, const RID &rid) {
  try {
    this->UnlockRow(txn, oid, rid);
  } catch (TransactionAbortException &e) {
    throw ExecutionException("UnLockRow failed");
  }
}

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  return LockResource(txn, lock_mode, oid, LockRange::TABLE);
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  return UnLockResource(txn, oid, LockRange::TABLE);
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  return LockResource(txn, lock_mode, oid, LockRange::ROW, rid);
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid) -> bool {
  return UnLockResource(txn, oid, LockRange::ROW, rid);
}

auto LockManager::LockResource(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, LockRange lock_range,
                               RID rid) -> bool {
  if (txn == nullptr) {
    return false;
  }
  if (txn->GetState() == TransactionState::COMMITTED || txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  // 给这个事务加大锁，防止其一致性状态发生改变
  TxnLatchGuard txn_latch_guard{txn};
  LockIllegalCheck(txn, lock_mode, oid, lock_range);  // 检查加锁合法性，失败自然会抛异常的
  //! \bug 这里也忘记传参 RID 了，导致行锁无法升级
  std::optional<LockMode> cur_lock_mode{GetLockLevel(txn, oid, lock_range, rid)};
  if (cur_lock_mode != std::nullopt) {
    /* std::cout << "当前持有的锁的等级 = " << mode_map[cur_lock_mode.value()] << ", 锁住的对象 = " << oid
              << ", rid = " << rid << std::endl; */
    if (cur_lock_mode.value() == lock_mode) {
      return true;
    }
    // 可能需要锁升级，只要进入下面的 if 里面，说明兼容性检查通过，允许升级
    if (LockCanUpdate(txn, lock_mode, cur_lock_mode.value())) {
      return UpgradeLock(txn, cur_lock_mode.value(), lock_mode, oid, lock_range, rid);
    }
  }
  // 其它情况：没有锁升级，直接获取锁[当然，可能会阻塞，因为它包含一些 wait() 函数]
  TryLock(txn, lock_mode, oid, lock_range, rid);
  return txn->GetState() != TransactionState::ABORTED;
}

auto LockManager::UnLockResource(Transaction *txn, const table_oid_t &oid, LockRange lock_range, RID rid) -> bool {
  if (txn == nullptr) {
    return false;
  }
  // 给这个事务加大锁，防止其一致性状态发生改变
  TxnLatchGuard txn_latch_guard{txn};
  std::optional<LockMode> cur_lock_mode{GetLockLevel(txn, oid, lock_range, rid)};  //! \bug 这里忘记传参 rid 了！
  if (cur_lock_mode == std::nullopt) {                                             // 根本不持有该资源的锁
    AbortAndThrowException(txn, AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  if (lock_range == LockRange::TABLE && RowLockExist(txn, oid)) {  // 行级别的锁还存在，不允许解开表锁
    AbortAndThrowException(txn, AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
  }
  std::mutex &lock_map_latch{lock_range == LockRange::TABLE ? table_lock_map_latch_ : row_lock_map_latch_};
  std::unique_lock<std::mutex> lock_map_guard{lock_map_latch};

  std::shared_ptr<LockRequestQueue> lock_request_queue{lock_range == LockRange::TABLE ? table_lock_map_[oid]
                                                                                      : row_lock_map_[rid]};
  std::unique_lock<std::mutex> request_queue_guard{lock_request_queue->latch_};

  // lock_map_guard.unlock();
  // 移除请求队列中的所有与这个事务相关的锁请求(大部分情况下就是一个事务只持有相同资源的一把锁！)
  lock_request_queue->UnSafeRemove(txn);
  lock_request_queue->cv_.notify_all();  // 让大家再去争夺锁资源
  // request_queue_guard.unlock();
  // 释放事务上面的锁
  DropLock(txn, oid, lock_range, cur_lock_mode.value(), rid);
  // 改变事务的状态，提示：似乎事务提交之后 SS2PL 会自动释放所有锁，所以这里或许不用手动释放锁
  ChangeTxnState(txn, cur_lock_mode.value());
  return true;
}

auto LockManager::UpgradeLock(Transaction *txn, LockMode cur_lock_mode, LockMode lock_mode, const table_oid_t &oid,
                              LockRange lock_range, RID rid) -> bool {
  TryLock(txn, lock_mode, oid, lock_range, rid, true);
  return txn->GetState() != TransactionState::ABORTED;
}

auto LockManager::LockIllegalCheck(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, LockRange lock_range)
    -> void {
  if (txn == nullptr) {
    throw std::runtime_error("txn 不允许是空");
  }
  // 1. 读未提交级别无需也不允许获取 S IS SIX 锁
  //! \bug 你抄错了一些东西！把 INTENTION_SHARED 抄成了 SHARED_INTENTION_EXCLUSIVE
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    if (lock_mode == LockMode::SHARED || lock_mode == LockMode::INTENTION_SHARED ||
        lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
      AbortAndThrowException(txn, AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
    }
  }

  // 2. 行级锁不允许出现意向锁，只有两种允许：EXCLUSIVE SHARED
  if (lock_range == LockRange::ROW) {
    if (!(lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::SHARED)) {
      AbortAndThrowException(txn, AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
    }
  }
  // 3. 收缩阶段不允许获得任何写锁，如果是可重复读，则收缩阶段不允许获得任何锁
  if (txn->GetState() == TransactionState::SHRINKING) {
    // 3.1 收缩阶段，如果是 REPEATABLE_READ ， 也不允许获得任何锁
    if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
      AbortAndThrowException(txn, AbortReason::LOCK_ON_SHRINKING);
    }
    // 3.2 收缩阶段不允许获得任何写锁，比如 X IX。适用于 READ_UNCOMMITTED & READ_COMMITTED
    //! \note 提示：收缩阶段似乎 SIX 锁也不可以加
    if (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::INTENTION_EXCLUSIVE ||
        lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
      AbortAndThrowException(txn, AbortReason::LOCK_ON_SHRINKING);
    }
  }

  // 4. 获取行级锁必须先获取对应的表级别锁
  //! \note
  if (lock_range == LockRange::ROW) {
    bool x_holds{txn->IsTableExclusiveLocked(oid) || txn->IsTableIntentionExclusiveLocked(oid) ||
                 txn->IsTableSharedIntentionExclusiveLocked(oid)};  // 是否持有任何 X 类锁：X IX SIX ?
    bool s_holds{txn->IsTableSharedLocked(oid) ||
                 txn->IsTableIntentionSharedLocked(oid)};  // 是否持有任何 S 类锁：S IS ?
    if (!x_holds && lock_mode == LockMode::EXCLUSIVE) {
      AbortAndThrowException(txn, AbortReason::TABLE_LOCK_NOT_PRESENT);
    }
    if (!(x_holds || s_holds) && lock_mode == LockMode::SHARED) {
      AbortAndThrowException(txn, AbortReason::TABLE_LOCK_NOT_PRESENT);
    }
  }
}

auto LockManager::LockCanUpdate(Transaction *txn, LockMode lock_mode, LockMode cur_lock_mode) -> bool {
  if (txn == nullptr) {
    throw std::runtime_error("txn 不允许是空");
  }
  if (upgrade_rules_.find(cur_lock_mode) == upgrade_rules_.end()) {
    AbortAndThrowException(txn, AbortReason::INCOMPATIBLE_UPGRADE);
  }
  if (upgrade_rules_[cur_lock_mode].find(lock_mode) == upgrade_rules_[cur_lock_mode].end()) {
    AbortAndThrowException(txn, AbortReason::INCOMPATIBLE_UPGRADE);
  }
  return true;
}

auto LockManager::GetLockLevel(Transaction *txn, const table_oid_t &oid, LockRange lock_range, RID rid)
    -> std::optional<LockMode> {
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
  } else {  // lock_range == LockRange::ROW
    if (txn->IsRowExclusiveLocked(oid, rid)) {
      return std::optional{LockMode::EXCLUSIVE};
    }
    if (txn->IsRowSharedLocked(oid, rid)) {
      return std::optional{LockMode::SHARED};
    }
  }
  return std::nullopt;
}

auto LockManager::TxnTableLockSet(Transaction *txn, LockMode lock_mode)
    -> std::shared_ptr<std::unordered_set<table_oid_t>> {
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

auto LockManager::TxnRowLockSet(Transaction *txn, LockMode lock_mode)
    -> std::shared_ptr<std::unordered_map<table_oid_t, std::unordered_set<RID>>> {
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

auto LockManager::AddLock(Transaction *txn, const table_oid_t &oid, LockRange lock_range, LockMode lock_mode, RID rid)
    -> void {
  if (txn == nullptr) {
    throw std::runtime_error("txn 不允许是空");
  }
  if (lock_range == LockRange::TABLE) {
    TxnTableLockSet(txn, lock_mode)->insert(oid);
  } else {  // lock_range == LockRange::ROW
    (*TxnRowLockSet(txn, lock_mode))[oid].insert(rid);
  }
}

auto LockManager::DropLock(Transaction *txn, const table_oid_t &oid, LockRange lock_range, LockMode lock_mode, RID rid)
    -> void {
  if (txn == nullptr) {
    throw std::runtime_error("txn 不允许是空");
  }
  if (lock_range == LockRange::TABLE) {
    TxnTableLockSet(txn, lock_mode)->erase(oid);
  } else {  // lock_range == LockRange::ROW
    (*TxnRowLockSet(txn, lock_mode))[oid].erase(rid);
  }
}

auto LockManager::TryLock(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, LockRange lock_range, RID rid,
                          bool upgrade) -> void {
  std::mutex &lock_map_latch{lock_range == LockRange::TABLE ? table_lock_map_latch_ : row_lock_map_latch_};
  std::unique_lock<std::mutex> lock_map_guard{lock_map_latch};
  // 1.争做吃螃蟹第一人，还没有任何事务在这个资源上加锁。注意：为了线程安全，后面即便队列清空，也不会删除这个创建的队列了
  if (TryInsertNewBucket(txn, lock_mode, oid, lock_range, rid)) {
    return;
  }
  // 2. 发现这个表的请求队列已经建立了，则需要监测有哪些既得利益者正在持有锁[注意：你监测到第一个不持有锁的即可]
  std::shared_ptr<LockRequestQueue> lock_request_queue{lock_range == LockRange::TABLE ? table_lock_map_[oid]
                                                                                      : row_lock_map_[rid]};
  //! \note 这里或许可以释放 lock_map_guard 的锁了
  std::unique_lock<std::mutex> request_queue_guard{lock_request_queue->latch_};
  lock_map_guard.unlock();
  if (upgrade && lock_request_queue->upgrading_ != INVALID_TXN_ID) {  // 只允许有一个事务进行升级
    AbortAndThrowException(txn, AbortReason::UPGRADE_CONFLICT);
  }
  //! \bug 只有升级的时候才需要 if (upgrade) 里面的代码 ！
  if (upgrade) {
    lock_request_queue->upgrading_ = txn->GetTransactionId();
    // 删除事务里面对该资源的锁记录以及队列的锁请求
    DropLock(txn, oid, lock_range, GetLockLevel(txn, oid, lock_range, rid).value(), rid);
    lock_request_queue->UnSafeRemove(txn);
  }

  LockRequest *this_request{lock_request_queue->InsertToRequestQueue(txn, lock_mode, oid, rid, upgrade)};
  lock_request_queue->cv_.wait(request_queue_guard, [&]() -> bool {
    return (txn->GetState() == TransactionState::ABORTED || TryGrantLock(lock_request_queue, this_request));
  });
  // 由于事务中途被抛弃，获取锁宣告失败，生成的锁请求也需要取消
  // [注意：我们在 PreviousLockReuqests 中考虑了中途 ABORTED 的情况]
  if (txn->GetState() == TransactionState::ABORTED) {
    //! \note 这个或许也是不必要的，因为 Abort 函数会自动该事务的清空锁请求记录以及 txn 本身的锁记录
    lock_request_queue->UnSafeRemove(txn);
    if (upgrade && lock_request_queue->upgrading_ == txn->GetTransactionId()) {
      lock_request_queue->upgrading_ = INVALID_TXN_ID;
    }
    //! \bug 注意：你似乎需要通知其它变量，这个事务被抛弃了
    lock_request_queue->cv_.notify_all();
    return;
  }

  // 在事务本身添加新的锁，因为是满足锁兼容性条件的。
  this_request->granted_ = true;
  AddLock(txn, oid, lock_range, lock_mode, rid);
  if (upgrade) {  // 锁升级完成，你需要把升级标签改回去
    lock_request_queue->upgrading_ = INVALID_TXN_ID;
  }
}

auto LockManager::TryInsertNewBucket(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, LockRange lock_range,
                                     RID rid) -> bool {
  if (txn == nullptr) {
    throw std::runtime_error("txn 不允许是空");
  }
  // TABLE 的情况
  if (lock_range == LockRange::TABLE) {
    // 1. 争做吃螃蟹第一人，还没有任何事务在这个表上加锁。注意：为了线程安全，后面即便队列清空，也不会删除这个创建的队列了
    if (table_lock_map_.find(oid) == table_lock_map_.end()) {
      table_lock_map_[oid] = std::make_shared<LockRequestQueue>();
      table_lock_map_[oid]->request_queue_.push_back(
          std::make_unique<LockRequest>(txn->GetTransactionId(), lock_mode, oid));
      table_lock_map_[oid]->request_queue_.back()->granted_ = true;
      AddLock(txn, oid, LockRange::TABLE, lock_mode);
      return true;
    }
    return false;
  }
  // ROW 的情况
  if (row_lock_map_.find(rid) == row_lock_map_.end()) {
    row_lock_map_[rid] = std::make_shared<LockRequestQueue>();
    row_lock_map_[rid]->request_queue_.push_back(
        std::make_unique<LockRequest>(txn->GetTransactionId(), lock_mode, oid, rid));
    row_lock_map_[rid]->request_queue_.back()->granted_ = true;
    AddLock(txn, oid, LockRange::ROW, lock_mode, rid);
    return true;
  }
  return false;
}

auto LockManager::TryGrantLock(std::shared_ptr<LockRequestQueue> &lock_request_queue, LockRequest *request_addr)
    -> bool {
  if (request_addr->granted_) {
    return true;  // 已经授予则直接返回 true 即可
  }
  // 从前往后检查，遍历当前所有锁请求的类型[granted & non-granted but can grant]
  //! \bug 你需要检验两种类型的锁：一种是已经授予的锁，另一种是没有授予但是检测时发现可以被授予的锁
  // 如果它前面的锁中存在 non-granted && cannot grant 的情况，那么由于 FIFO ，应该直接返回 false
  //! \note 采用递归的方式，在这个函数体内部直接将队列的请求状态 granted_ = true
  std::unordered_set<LockMode> cur_lock_requests{};
  for (std::unique_ptr<LockRequest> &cur_lock_request : lock_request_queue->request_queue_) {
    if (request_addr == cur_lock_request.get()) {
      break;
    }
    // 如果某个事务 ABORT/COMMITTED 状态，这说明，那么这样的锁也是无效的，直接 continue
    TransactionState txn_state{TransactionManager::GetTransaction(cur_lock_request->txn_id_)->GetState()};
    if (txn_state == TransactionState::ABORTED || txn_state == TransactionState::COMMITTED) {
      continue;
    }
    // 如果某个请求 !granted && !TryGrantLock 那么应该返回 false，这是由 FIFO 决定的
    if (!cur_lock_request->granted_ && !TryGrantLock(lock_request_queue, cur_lock_request.get())) {
      return false;
    }
    cur_lock_requests.insert(cur_lock_request->lock_mode_); // 如果前面的请求能够授予，或者已经授予，都加入授予锁请求的集合 
  }
  bool can_coexistence{
      std::all_of(cur_lock_requests.begin(), cur_lock_requests.end(), [request_addr](LockMode cur_lock_mode) {
        return ConflictChecker::CanCoexistence(cur_lock_mode, request_addr->lock_mode_);
      })};  // all of 就是如果所有都是 true，才会返回 true；否则返回 false
  if (can_coexistence) {
    request_addr->granted_ = true;
    return true;
  }
  return false;
}

std::unordered_map<LockManager::LockMode, std::unordered_set<LockManager::LockMode>>
    LockManager::ConflictChecker::coexistence_map{
        {LockMode::INTENTION_SHARED,
         {LockMode::INTENTION_SHARED, LockMode::INTENTION_EXCLUSIVE, LockMode::SHARED,
          LockMode::SHARED_INTENTION_EXCLUSIVE}},
        {LockMode::INTENTION_EXCLUSIVE, {LockMode::INTENTION_SHARED, LockMode::INTENTION_EXCLUSIVE}},
        {LockMode::SHARED, {LockMode::INTENTION_SHARED, LockMode::SHARED}},
        {LockMode::SHARED_INTENTION_EXCLUSIVE, {LockMode::INTENTION_SHARED}},
        {LockMode::EXCLUSIVE, {}},
    };

auto LockManager::ConflictChecker::CanCoexistence(LockMode lock_mode1, LockMode lock_mode2) -> bool {
  return coexistence_map[lock_mode1].find(lock_mode2) != coexistence_map[lock_mode1].end();
}

auto LockManager::LockRequestQueue::PreviousLockReuqests(LockRequest *lock_request) -> std::unordered_set<LockMode> {
  std::unordered_set<LockMode> result{};
  for (std::unique_ptr<LockRequest> &cur_lock_request : request_queue_) {
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

auto LockManager::LockRequestQueue::InsertToRequestQueue(Transaction *txn, LockMode lock_mode, const table_oid_t &oid,
                                                         RID rid, bool upgrade) -> LockRequest * {
  if (!upgrade) {
    request_queue_.push_back(std::make_unique<LockRequest>(txn->GetTransactionId(), lock_mode, oid, rid));
    return request_queue_.back().get();
  }
  auto it{request_queue_.begin()};
  while (it != request_queue_.end() && (*it)->granted_) {
    std::advance(it, 1);
  }
  if (it == request_queue_.end()) {
    return InsertToRequestQueue(txn, lock_mode, oid, rid);
  }
  LockRequest *request{new LockRequest{txn->GetTransactionId(), lock_mode, oid, rid}};
  request_queue_.insert(it, std::unique_ptr<LockRequest>{request});
  return request;
}

auto LockManager::LockRequestQueue::UnSafeRemove(Transaction *txn) -> int {
  int remove_count{};  // 记录移除元素的个数
  this->request_queue_.remove_if([&txn, &remove_count](const std::unique_ptr<LockRequest> &ptr) -> bool {
    bool will_remove{ptr != nullptr && ptr->txn_id_ == txn->GetTransactionId()};
    if (will_remove) {
      ++remove_count;
    }
    return will_remove;
  });
  return remove_count;
}

auto LockManager::LockRequestQueue::UnSafeRemove(txn_id_t target_txn_id) -> int {
  int remove_count{};  // 记录移除元素的个数
  this->request_queue_.remove_if([&target_txn_id, &remove_count](const std::unique_ptr<LockRequest> &ptr) -> bool {
    bool will_remove{ptr != nullptr && ptr->txn_id_ == target_txn_id};
    if (will_remove) {
      ++remove_count;
    }
    return will_remove;
  });
  return remove_count;
}

auto LockManager::RowLockExist(Transaction *txn, const table_oid_t &oid) -> bool {
  std::unique_lock<std::mutex> row_lock_map_guard{row_lock_map_latch_};
  for (auto &pair : row_lock_map_) {
    std::shared_ptr<LockRequestQueue> request_queue{pair.second};
    std::unique_lock<std::mutex> request_queue_guard{request_queue->latch_};
    for (auto &request : request_queue->request_queue_) {
      if (request->oid_ == oid && txn->GetTransactionId() == request->txn_id_) {
        return true;
      }
    }
  }
  return false;
}

auto LockManager::ChangeTxnState(Transaction *txn, LockMode lock_mode) -> void {
  if (txn == nullptr) {
    throw std::runtime_error("txn 不允许是空");
  }
  // 事务的状态是不可逆的。
  if (txn->GetState() == TransactionState::COMMITTED || txn->GetState() == TransactionState::ABORTED) {
    return;
  }
  if (lock_mode == LockMode::EXCLUSIVE) {
    txn->SetState(TransactionState::SHRINKING);
    return;
  }
  if (lock_mode == LockMode::SHARED && txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    txn->SetState(TransactionState::SHRINKING);
  }
}

auto LockManager::AbortAndThrowException(Transaction *txn, AbortReason abort_reason) -> void {
  txn->SetState(TransactionState::ABORTED);
  throw TransactionAbortException{txn->GetTransactionId(), abort_reason};
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  std::vector<txn_id_t> &edges{waits_for_[t1]};  // 添加必然会创建新的记录
  if (std::find(edges.begin(), edges.end(), t2) != edges.end()) {
    return;
  }
  waits_for_[t1].push_back(t2);
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  if (waits_for_.find(t1) == waits_for_.end()) {
    return;  // 防止创建新的桶
  }
  std::vector<txn_id_t> &edges{waits_for_[t1]};
  auto it{std::find(edges.begin(), edges.end(), t2)};
  if (it == edges.end()) {
    return;
  }
  edges.erase(it);
}

auto LockManager::GetInDegree(std::vector<std::pair<txn_id_t, txn_id_t>> &edge_list)
    -> std::unordered_map<txn_id_t, int> {
  std::unordered_map<txn_id_t, int> in_degree_list{};
  for (auto &pair : waits_for_) {
    in_degree_list[pair.first] = 0;
  }
  for (auto &pair : edge_list) {
    in_degree_list[pair.second]++;
  }
  return in_degree_list;
}

void LockManager::DropOneNode(std::vector<std::pair<txn_id_t, txn_id_t>> &edge_list,
                              std::unordered_map<txn_id_t, int> &in_degree_list, txn_id_t node_id) {
  auto predicate{[&in_degree_list, &node_id](const std::pair<txn_id_t, txn_id_t> &pair) -> bool {
    bool is_target{pair.first == node_id};
    if (is_target) {
      in_degree_list[pair.second]--;
    }
    return is_target;
  }};
  auto new_end{std::remove_if(edge_list.begin(), edge_list.end(), predicate)};
  edge_list.erase(new_end, edge_list.end());
  in_degree_list.erase(node_id);  // 注意每次检测完毕后删去目标结点
}

auto LockManager::FindMAXVal(const std::unordered_set<txn_id_t> &this_set) -> txn_id_t {
  txn_id_t result{INVALID_TXN_ID};
  for (txn_id_t elem : this_set) {
    result = std::max(result, elem);
  }
  return result;
}

auto LockManager::DFS(txn_id_t start_txn_id, std::unordered_set<txn_id_t> &node_path,
                      std::unordered_set<txn_id_t> &visited_set, txn_id_t *txn_id) -> void {
  node_path.insert(start_txn_id);
  visited_set.insert(start_txn_id);
  for (const txn_id_t adj_txn_id : waits_for_[start_txn_id]) {
    // 这已经意味着存在 start_txn_id -> adj_txn_id 的路径了
    if (node_path.find(adj_txn_id) != node_path.end()) {
      *txn_id = FindMAXVal(node_path);
      return;
    }
    if (visited_set.find(adj_txn_id) != visited_set.end()) {
      continue;
    }
    DFS(adj_txn_id, node_path, visited_set, txn_id);
    if (*txn_id != INVALID_TXN_ID) {
      return;
    }
  }
  node_path.erase(start_txn_id);
}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool {
  *txn_id = INVALID_TXN_ID;  // 首先，将参数设置为无效值。
  std::vector<txn_id_t> nodes{GetNodeList()};
  std::sort(nodes.begin(), nodes.end());
  // 注意：需要对 waits_for_ 的邻接表进行排序，按照从小到大的顺序
  for (auto &pair : waits_for_) {
    auto &adj_vector{pair.second};
    std::sort(adj_vector.begin(), adj_vector.end());
  }
  // 逐个检测环
  std::unordered_set<txn_id_t> node_path{};
  std::unordered_set<txn_id_t> visited_set{};
  for (txn_id_t node : nodes) {
    if (visited_set.find(node) == visited_set.end()) {
      DFS(node, node_path, visited_set, txn_id);
      if (*txn_id != INVALID_TXN_ID) {
        return true;
      }
    }
  }
  return false;
}

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges{};
  for (auto &pair : waits_for_) {
    for (txn_id_t &txn_id : pair.second) {
      edges.emplace_back(pair.first, txn_id);
    }
  }
  return edges;
}

auto LockManager::GetNodeList() -> std::vector<txn_id_t> {
  std::unordered_set<txn_id_t> nodes{};
  for (auto &pair : waits_for_) {
    for (txn_id_t &txn_id : pair.second) {
      nodes.insert(pair.first);
      nodes.insert(txn_id);
    }
  }
  std::vector<txn_id_t> result{};
  result.reserve(nodes.size());
  for (txn_id_t elem : nodes) {
    result.push_back(elem);
  }
  return result;
}

void LockManager::ConstructGraphByOneQueue(std::shared_ptr<LockRequestQueue> &request_queue_ptr) {
  std::list<std::unique_ptr<LockRequest>> &request_queue{request_queue_ptr->request_queue_};
  std::unordered_set<LockRequest *> cur_granted_reuqests{};  // 记录遍历到当前的所有 granted_ 的请求
  for (std::unique_ptr<LockRequest> &lock_request : request_queue) {
    if (lock_request->granted_) {
      cur_granted_reuqests.insert(lock_request.get());
    } else {
      for (LockRequest *granted_request : cur_granted_reuqests) {
        if (!ConflictChecker::CanCoexistence(granted_request->lock_mode_, lock_request->lock_mode_)) {
          AddEdge(lock_request->txn_id_, granted_request->txn_id_);
        }
      }
    }
  }
  request_queue_ptr->cv_.notify_all();
}

void LockManager::GenerateWaitForGraph() {
  // 你需要构建一个 GRAPH，然后判断是否有环。t1 -> t2 表示 t1 waits for t2
  waits_for_.clear();
  for (auto &pair : table_lock_map_) {
    std::shared_ptr<LockRequestQueue> request_queue{pair.second};
    std::unique_lock<std::mutex> request_queue_guard{request_queue->latch_};
    ConstructGraphByOneQueue(request_queue);
  }
  for (auto &pair : row_lock_map_) {
    std::shared_ptr<LockRequestQueue> request_queue{pair.second};
    std::unique_lock<std::mutex> request_queue_guard{request_queue->latch_};
    ConstructGraphByOneQueue(request_queue);
  }
}

void LockManager::RemoveAndNotify(std::shared_ptr<LockRequestQueue> &request_queue_ptr, txn_id_t txn_id) {
  int remove_cnt{request_queue_ptr->UnSafeRemove(txn_id)};
  if (remove_cnt > 0) {
    if (request_queue_ptr->upgrading_ == txn_id) {
      request_queue_ptr->upgrading_ = INVALID_TXN_ID;
    }
  }
  request_queue_ptr->cv_.notify_all();  // 无论如何都尝试唤醒其它事务
}

void LockManager::RemoveLockRequestOf(txn_id_t txn_id) {
  for (auto &pair : table_lock_map_) {
    std::shared_ptr<LockRequestQueue> request_queue{pair.second};
    std::unique_lock<std::mutex> request_queue_guard{request_queue->latch_};
    RemoveAndNotify(request_queue, txn_id);
  }
  for (auto &pair : row_lock_map_) {
    std::shared_ptr<LockRequestQueue> request_queue{pair.second};
    std::unique_lock<std::mutex> request_queue_guard{request_queue->latch_};
    RemoveAndNotify(request_queue, txn_id);
  }
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {
      // 死锁检测的时候禁止其它线程访问 *_lock_map_，于是，所有的锁请求队列也被禁止访问了
      std::unique_lock<std::mutex> table_lock_map_guard{table_lock_map_latch_};
      std::unique_lock<std::mutex> row_lock_map_guard{row_lock_map_latch_};
      GenerateWaitForGraph();
      txn_id_t aborting_txn_id{};
      if (HasCycle(&aborting_txn_id)) {
        // 有环的情况下，你必须将某个事务的状态设为抛弃，卸下它在相关队列中的锁[当然你也可以不卸下锁]，然后通知其它小伙伴
        TransactionManager::GetTransaction(aborting_txn_id)->SetState(TransactionState::ABORTED);
        RemoveLockRequestOf(aborting_txn_id);
      }
    }
  }
}

}  // namespace bustub
