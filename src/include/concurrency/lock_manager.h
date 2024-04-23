#pragma once

#include <algorithm>
#include <condition_variable>  // NOLINT
#include <list>
#include <memory>
#include <mutex>  // NOLINT
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/rid.h"
#include "concurrency/transaction.h"

namespace bustub {

class TransactionManager;

/**
 * LockManager handles transactions asking for locks on records.
 */
class LockManager {
 public:
  enum class LockMode { SHARED, EXCLUSIVE, INTENTION_SHARED, INTENTION_EXCLUSIVE, SHARED_INTENTION_EXCLUSIVE };
  enum class LockRange { TABLE, ROW };

  /**
   * 把哪些锁能共存，哪些不能共存都写出来
  */
  class ConflictChecker {
  public:
    /**
     * 检验两个不同等级的锁能否共存。
     * @note 这两个锁必须管理的是相同资源
    */
    static auto CanCoexistence(LockMode lock_mode1, LockMode lock_mode2) -> bool;
  private:
    /** 锁的共存表 */
    static std::unordered_map<LockMode, std::unordered_set<LockMode>> COEXISTENCE_MAP;
  };

  /**
   * 实现一个事务自动锁管理器，同时提供了手动释放锁的接口
  */
  class TxnLatchGuard {
  public:
    TxnLatchGuard(Transaction *txn): txn_{txn}, holding_lock_{false} {
      if (txn_ == nullptr) { return; }
      txn_->LockTxn();
      holding_lock_ = true;
    }
    void UnLock() {
      if (holding_lock_) {
        txn_->UnlockTxn();
        holding_lock_ = false;
      }
    }
    ~TxnLatchGuard() {
      UnLock();
    }
  private:
    Transaction* txn_;
    bool holding_lock_;
  };

  /**
   * Structure to hold a lock request.
   * This could be a lock request on a table OR a row.
   * For table lock requests, the rid_ attribute would be unused.
   */
  class LockRequest {
   public:
    LockRequest(txn_id_t txn_id, LockMode lock_mode, table_oid_t oid) /** Table lock request */
        : txn_id_(txn_id), lock_mode_(lock_mode), oid_(oid) {}
    LockRequest(txn_id_t txn_id, LockMode lock_mode, table_oid_t oid, RID rid) /** Row lock request */
        : txn_id_(txn_id), lock_mode_(lock_mode), oid_(oid), rid_(rid) {}

    /** Txn_id of the txn requesting the lock */
    txn_id_t txn_id_;
    /** Locking mode of the requested lock */
    LockMode lock_mode_;
    /** Oid of the table for a table lock; oid of the table the row belong to for a row lock */
    table_oid_t oid_;
    /** Rid of the row for a row lock; unused for table locks */
    RID rid_;
    /** Whether the lock has been granted or not */
    bool granted_{false};
  };

  /**
   * 这个类用于维护对[同一个资源]的各种类型的锁的请求队列[注意：不是等待队列]。
   * 似乎是每次获加锁请求的时候，就把元素加入 LockRequestQueue
   * 也就是说，这个队列中，如果获得了锁，也会在这个队列中；如果没获得锁，也会在这个队列中[等待获得]。
   * 如果某个事务释放了锁，那么应该对这个队列进行处理。
   * 原则上，一个队列一定是这样的  [granted requests] | [blocked requests]，即 granted 的请求必定连续且在队列的前面
  */
  class LockRequestQueue {
   public:
    /** 
     * List of lock requests for the same resource (table or row)。
     * 记录了一些锁的请求信息，是锁请求的等待队列。杜绝使用裸指针后忘记释放锁的问题，所以直接使用独占指针
     */
    std::list<std::unique_ptr<LockRequest>> request_queue_;
    // std::list<LockRequest *> request_queue_;
    /** For notifying blocked transactions on this rid */
    std::condition_variable cv_;
    /** txn_id of an upgrading transaction (if any) */
    txn_id_t upgrading_ = INVALID_TXN_ID;
    /** coordination，我们认为它用于配合条件变量，并且，它需要保护请求队列 */
    std::mutex latch_;
    /**
     * 找出当前的请求队列中某个请求前面持有了哪些锁？
     * @param lock_request 一个锁请求地址，我们会获得队列中这个锁请求前面的所有锁请求的类型。
     * @return 该函数会返回一个 set，表示当前请求的前面加了哪些锁
     * @note 该函数本身内部不会加锁，非线程安全
     * @note 无论前面请求的锁是否被 granted，都应该检查兼容性，因为要遵循 FIFO 的原则，
     * 你同时会唤醒多个事务，而保证 FIFO 则需要检查这个请求前面的所有请求的兼容性。
     * @note 如果某个事务 ABORTED ，那么它的锁记录无效
    */
    auto PreviousLockReuqests(LockRequest* lock_request) -> std::unordered_set<LockMode>;
    /**
     * 表级锁的插入到锁请求队列，如果是普通请求，则添加到队列尾部；如果是锁升级，则添加到第一个 un_granted 的位置
     * @param upgrade 表示是否是锁升级，默认是 false
     * @return 新添加请求的地址
     * @note 该函数本身内部不会加锁，非线程安全
    */
    auto InsertToRequestQueue(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, bool upgrade = false) -> LockRequest*;
  };

  /**
   * Creates a new lock manager configured for the deadlock detection policy.
   */
  LockManager() {
    enable_cycle_detection_ = true;
    cycle_detection_thread_ = new std::thread(&LockManager::RunCycleDetection, this);
  }

  ~LockManager() {
    enable_cycle_detection_ = false;
    cycle_detection_thread_->join();
    delete cycle_detection_thread_;
  }

  /**
   * [LOCK_NOTE]
   *
   * GENERAL BEHAVIOUR:
   *    Both LockTable() and LockRow() are blocking methods; they should wait till the lock is granted and then return.
   *    If the transaction was aborted in the meantime[或许就是某次等待时被唤醒后], do not grant the lock and return false.
   *
   *
   * MULTIPLE TRANSACTIONS:
   *    LockManager should maintain a queue for each resource; locks should be granted to transactions in a FIFO manner.
   *    If there are multiple compatible lock requests(如果存在多个兼容的锁请求), all should be granted at the same time
   *    as long as FIFO is honoured.
   * 理解：比如事务 0 加了S锁，事务 1 等待添加X锁，那么事务2添加S锁的请求不能被允许，因为要满足 FIFO 的原则。
   *
   * SUPPORTED LOCK MODES:
   *    Table locking should support all lock modes.
   *    Row locking should not support Intention locks. Attempting this should set the TransactionState as
   *    ABORTED and throw a TransactionAbortException (ATTEMPTED_INTENTION_LOCK_ON_ROW)
   * 
   * ISOLATION LEVEL:
   *    Depending on the ISOLATION LEVEL, a transaction should attempt to take locks:
   *    - Only if required, AND
   *    - Only if allowed
   *
   *    For instance S/IS/SIX locks are not required under READ_UNCOMMITTED, and any such attempt should set the
   *    TransactionState as ABORTED and throw a TransactionAbortException (LOCK_SHARED_ON_READ_UNCOMMITTED).
   *    [提示：这些异常似乎是需要你自己抛出的]
   *
   *    Similarly, X/IX locks on rows are not allowed if the the Transaction State is SHRINKING, and any such attempt
   *    should set the TransactionState as ABORTED and throw a TransactionAbortException (LOCK_ON_SHRINKING).
   *
   *    REPEATABLE_READ:
   *        The transaction is required to take all locks.
   *        All locks are allowed in the GROWING state [所有锁都应该在增长阶段获取]
   *        No locks are allowed in the SHRINKING state [只要事务进入了SHRINKING状态，那么就不能持有任何锁，即锁要被一次性释放]
   *        [先释放所有锁，然后立即进入 SHRINKING STATE]
   *
   *    READ_COMMITTED:
   *        The transaction is required to take all locks. 
   *        All locks are allowed in the GROWING state [所有锁都应该在增长阶段获取]
   *        Only IS, S locks are allowed in the SHRINKING state [释放第一个X类锁之后，必须进入 SHRINKING 状态, SIX 被当作了X类锁]
   *                                                            [这种情况下依然允许获取 S 或者 IS 锁]
   *
   *    READ_UNCOMMITTED:
   *        The transaction is required to take only IX, X locks.
   *        X, IX locks are allowed in the GROWING state. [增长阶段只允许持有 X IX 锁]
   *        S, IS, SIX locks are never allowed [所有和读相关的锁都不用加]
   *
   *
   * MULTILEVEL LOCKING:
   *    While locking rows, Lock() should ensure that the transaction has an appropriate lock on the table which the row
   *    belongs to. For instance, if an exclusive lock is attempted on a row, the transaction must hold either
   *    X, IX, or SIX on the table. If such a lock does not exist on the table, Lock() should set the TransactionState
   *    as ABORTED and throw a TransactionAbortException (TABLE_LOCK_NOT_PRESENT)
   *    先加表级别的锁，再加行级别的锁。并且即便是锁住全表了，似乎也需要加行级锁。[一般X锁的话，无需添加行级别的X锁了]
   *    X IX SIX on table --> X on row
   *    S IS X IX SIX on table --> S on row [很宽泛了，只要持有任何表级别的锁，就可以加行级别的S锁]
   * 
   * LOCK UPGRADE:
   *    Calling Lock() on a resource that is already locked should have the following behaviour:
   *    - If requested lock mode is the same as that of the lock presently held,
   *      Lock() should return true since it already has the lock.
   *    - If requested lock mode is different, Lock() should upgrade the lock held by the transaction.
   *
   *    A lock request being upgraded should be prioritised over other waiting lock requests on the same resource.
   *    [所以你需要把这个请求进行“插队”，插到第一个没有 GRANTED 的元素那里]
   *
   *    While upgrading, only the following transitions should be allowed:
   *        IS -> [S, X, IX, SIX]
   *        S -> [X, SIX]
   *        IX -> [X, SIX]
   *        SIX -> [X]
   *    Any other upgrade is considered incompatible, and such an attempt should set the TransactionState as ABORTED
   *    and throw a TransactionAbortException (INCOMPATIBLE_UPGRADE)
   *
   *    Furthermore, only one transaction should be allowed to upgrade its lock on a given resource.
   *    Multiple concurrent lock upgrades on the same resource should set the TransactionState as
   *    ABORTED and throw a TransactionAbortException (UPGRADE_CONFLICT).
   *    只有一个事务能够升级锁，多个并发的锁升级则需要[或许是这些并发的事务]抛出异常。
   *
   *
   * BOOK KEEPING:
   *    If a lock is granted to a transaction, lock manager should update its
   *    lock sets appropriately (check transaction.h)
   */

  /**
   * [UNLOCK_NOTE]
   *
   * GENERAL BEHAVIOUR:
   *    Both UnlockTable() and UnlockRow() should release the lock on the resource and return.
   *    Both should ensure that the transaction currently holds a lock on the resource it is attempting to unlock.
   *    If not, LockManager should set the TransactionState as ABORTED and throw
   *    a TransactionAbortException (ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD)
   *    
   *    [解锁顺序：先解除行级锁，再解除意向锁]
   *    Additionally, unlocking a table should only be allowed if the transaction does not hold locks on any
   *    row on that table. If the transaction holds locks on rows of the table, Unlock should set the Transaction State
   *    as ABORTED and throw a TransactionAbortException (TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS).
   *    [解锁的时候需要提醒其它等待者加锁]
   *    Finally, unlocking a resource should also grant any new lock requests for the resource (if possible).
   *
   * TRANSACTION STATE UPDATE
   *    Unlock should update the transaction state appropriately (depending upon the ISOLATION LEVEL)
   *    Only unlocking S or X locks changes transaction state.
   *    [下面的东西就按照它说的来写吧]
   *    REPEATABLE_READ:
   *        Unlocking S/X locks should set the transaction state to SHRINKING [先释放锁，再进入 SHRINKING 状态]
   *
   *    READ_COMMITTED:
   *        Unlocking X locks should set the transaction state to SHRINKING. [释放了 X 锁之后必须进入 SHRINIKING 状态]
   *        Unlocking S locks does not affect transaction state. [释放 S 锁不影响状态]
   *
   *   READ_UNCOMMITTED:
   *        Unlocking X locks should set the transaction state to SHRINKING.
   *        S locks are not permitted under READ_UNCOMMITTED.
   *            The behaviour upon unlocking an S lock under this isolation level is undefined.
   *
   *
   * BOOK KEEPING:
   *    After a resource is unlocked, lock manager should update the transaction's lock sets
   *    appropriately (check transaction.h)
   */

  /**
   * Acquire a lock on table_oid_t in the given lock_mode.
   * If the transaction already holds a lock on the table, upgrade the lock
   * to the specified lock_mode (if possible).
   *
   * This method should abort the transaction and throw a
   * TransactionAbortException under certain circumstances.
   * See [LOCK_NOTE] in header file.
   *
   * @param txn the transaction requesting the lock upgrade
   * @param lock_mode the lock mode for the requested lock
   * @param oid the table_oid_t of the table to be locked in lock_mode
   * @return true if the upgrade is successful, false otherwise
   */
  auto LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) noexcept(false) -> bool;

  /**
   * Release the lock held on a table by the transaction.
   *
   * This method should abort the transaction and throw a
   * TransactionAbortException under certain circumstances.
   * See [UNLOCK_NOTE] in header file.
   *
   * @param txn the transaction releasing the lock
   * @param oid the table_oid_t of the table to be unlocked
   * @return true if the unlock is successful, false otherwise
   * @note 假设同一时刻同一事务仅持有一个该资源的锁，所以锁升级的时候应该先解锁，再等待获得锁
   */
  auto UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool;

  /**
   * Acquire a lock on rid in the given lock_mode.
   * If the transaction already holds a lock on the row, upgrade the lock
   * to the specified lock_mode (if possible).
   *
   * This method should abort the transaction and throw a
   * TransactionAbortException under certain circumstances.
   * See [LOCK_NOTE] in header file.
   *
   * @param txn the transaction requesting the lock upgrade
   * @param lock_mode the lock mode for the requested lock
   * @param oid the table_oid_t of the table the row belongs to
   * @param rid the RID of the row to be locked
   * @return true if the upgrade is successful, false otherwise
   */
  auto LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool;

  /**
   * Release the lock held on a row by the transaction.
   *
   * This method should abort the transaction and throw a
   * TransactionAbortException under certain circumstances.
   * See [UNLOCK_NOTE] in header file.
   *
   * @param txn the transaction releasing the lock
   * @param rid the RID that is locked by the transaction
   * @param oid the table_oid_t of the table the row belongs to
   * @param rid the RID of the row to be unlocked
   * @return true if the unlock is successful, false otherwise
   */
  auto UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid) -> bool;

  /*** Graph API ***/

  /**
   * Adds an edge from t1 -> t2 from waits for graph.
   * @param t1 transaction waiting for a lock
   * @param t2 transaction being waited for
   */
  auto AddEdge(txn_id_t t1, txn_id_t t2) -> void;

  /**
   * Removes an edge from t1 -> t2 from waits for graph.
   * @param t1 transaction waiting for a lock
   * @param t2 transaction being waited for
   */
  auto RemoveEdge(txn_id_t t1, txn_id_t t2) -> void;

  /**
   * Checks if the graph has a cycle, returning the newest transaction ID in the cycle if so.
   * @param[out] txn_id if the graph has a cycle, will contain the newest transaction ID
   * @return false if the graph has no cycle, otherwise stores the newest transaction ID in the cycle to txn_id
   */
  auto HasCycle(txn_id_t *txn_id) -> bool;

  /**
   * @return all edges in current waits_for graph
   */
  auto GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>>;

  /**
   * Runs cycle detection in the background.
   */
  auto RunCycleDetection() -> void;
  /**
   * 检查本次事务即将获取的锁是否合法，不合法则抛出异常。同时兼容行级锁和表锁。
   * @param lock_range 锁的范围，是行级别的锁还是表级别的锁
   * @return 如果不合法，会直接抛出异常，自然会处理。该函数的调用者也就不会向下执行了，故无需返回值
   * @note 该函数理论上应该在对事务加了锁的情况下调用
  */
  auto LockIllegalCheck(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, LockRange lock_range) -> void;

  /**
   * 考察是否持有与要加的锁相同的锁。
   * @param lock_range 锁的范围，是行级别的锁还是表级别的锁
   * @param rid 如果 lock_range 是 ROW ，这个参数才有作用，否则不要使用这个参数
   * @param cur_lock_mode 如果持有锁的话
   * @return 如果持有的锁与要加的锁相同[资源一致，锁的等级一致]，那么返回 true。如果不持有该资源的锁或者持有但是等级不一致，返回 false
   * @note 该函数理论上应该在对事务加了锁的情况下调用
  */
  //auto HoldingSameLock(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, LockMode cur_lock_mode,
  //                     LockRange lock_range, const RID rid = {}) -> bool;

  /**
   * 考察对同一个资源的锁能否升级，该函数同时兼容行级锁和表锁。
   * @param cur_lock_mode 如果持有该资源的锁的话，该事务当前持有该资源的锁的级别是什么？
   * @return 如果当前事务持有的该资源的锁满足升级条件，返回 true；其它情况会抛出异常的。
   * @throw 如果当前事务持有该资源的锁，但是不满足升级条件，则抛出 INCOMPATIBLE_UPGRADE 异常
   * @note 该函数理论上应该在对事务加了锁的情况下调用
   * @note 再提示：该函数必须在 HoldingSameLock 之后调用，排除持有的锁完全相同的情况。
  */
  auto LockCanUpdate(Transaction *txn, LockMode lock_mode, LockMode cur_lock_mode) -> bool;

  /**
   * 获取 txn 锁住某个资源的具体级别
   * @param lock_range 锁的范围，是行级别的锁还是表级别的锁
   * @param rid 如果 lock_range 是 ROW ，这个参数才有作用，否则不要使用这个参数
   * @return 如果确实锁住了这个资源，那么返回 std::optional<LockMode>，否则返回 std::nullopt
   * @note 该函数理论上应该在对事务加了锁的情况下调用
  */
  auto GetLockLevel(Transaction *txn, const table_oid_t &oid, LockRange lock_range, const RID rid = {}) -> std::optional<LockMode>;

  /**
   * 尝试获取表级别的锁
   * @param upgrade 表示是否是锁升级，默认是 false
   * @note 已经对获取锁的合法性进行了检查，并且不是锁升级，即 txn 当前的确不持有这个锁
   * @note 该函数理论上应该在对事务加了锁的情况下调用
  */
  auto TryTableLock(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, bool upgrade = false) -> void;

  /**
   * 判断能否授予锁。具体做法：检查当前锁请求之前的所有请求[包括 granted 和 non-granted]，考察兼容性
   * @param request_addr 当前锁请求对应请求对象的地址
   * @return 如果兼容，返回 true，可以授予；否则返回 false。
  */
  auto CanGrantLock(std::shared_ptr<LockRequestQueue>& lock_request_queue, std::unique_lock<std::mutex>& request_queue_guard, LockRequest* request_addr) -> bool;

  /**
   * 对某个事务进行加锁。这里指的是在 事务对象中 的相应数据结构进行记录，而不是操作锁管理器
   * @param lock_range 锁的范围，是行级别的锁还是表级别的锁
   * @param rid 如果 lock_range 是 ROW ，这个参数才有作用，否则不要使用这个参数
   * @note 该函数理论上应该在对事务加了锁的情况下调用
  */
  auto AddLock(Transaction *txn, const table_oid_t &oid, LockRange lock_range, LockMode lock_mode, const RID rid = {}) -> void;
  /**
   * 对某个事务在某个资源上的锁进行释放。这里指的是在 事务对象中 的相应数据结构进行记录，而不是操作锁管理器
   * @param lock_range 锁的范围，是行级别的锁还是表级别的锁
   * @param rid 如果 lock_range 是 ROW ，这个参数才有作用，否则不要使用这个参数
   * @note 该函数理论上应该在对事务加了锁的情况下调用。此外，该函数不会检查释放锁之前事务是否持有该锁
  */
  auto DropLock(Transaction *txn, const table_oid_t &oid, LockRange lock_range, LockMode lock_mode, const RID rid = {}) -> void;

  /**
   * 通过 lock_mode 匹配合适的 txn 中的锁的集合 [表级锁]
  */
  auto TxnTableLockSet(Transaction *txn, LockMode lock_mode) -> std::shared_ptr<std::unordered_set<table_oid_t>>;

  /**
   * 通过 lock_mode 匹配合适的 txn 中的锁的集合 [行级锁]
  */  
  auto TxnRowLockSet(Transaction *txn, LockMode lock_mode) -> std::shared_ptr<std::unordered_map<table_oid_t, std::unordered_set<RID>>>;
  /**
   * 升级表锁。
   * @param cur_lock_mode 当前这个事务持有该资源的锁的等级
   * @param lock_mode 要升级到的目标等级
   * @note 该函数理论上应该在对事务加了锁的情况下调用
   * @note 锁的升级或许应该先把相同资源的原来的锁请求卸掉(UNLOCK)，然后再重新获取锁
  */
  auto UpgradeTableLock(Transaction *txn, LockMode cur_lock_mode, LockMode lock_mode, const table_oid_t &oid) -> bool;
  /**
   * 在某个表 oid 上，txn 是否持有任何行级别的锁？该函数主要用于判断某个表级别的锁能否释放。
   * @note 该函数理论上应该在对事务加了锁的情况下调用
  */
  auto RowLockExist(Transaction *txn, const table_oid_t &oid) -> bool;

  /**
   * 释放锁之后，根据不同隔离级别而改变事务的状态。一般是转为 SHRINKING.
   * 它不会对事务持有的锁进行任何操作。
   * @note 该函数理论上应该在对事务加了锁的情况下调用
  */
  auto ChangeTxnState(Transaction *txn, LockMode lock_mode) -> void;

  /**
   * 为了同时兼容 LockTable 和 LockRow 而实现的公共接口
   * @param rid 带有默认值的参数，如果调用者是 LockTable，那么这个参数直接忽略即可
  */
  auto LockResource(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, LockRange lock_range, const RID rid = {}) -> bool;

 private:
  /** Fall 2022 */
  /** Structure that holds lock requests for a given table oid<它存放了所有等待获取锁的事务 */
  std::unordered_map<table_oid_t, std::shared_ptr<LockRequestQueue>> table_lock_map_;
  /** Coordination，这是用于保护 table_lock_map_ 的数据结构，这个数据结构修改时，你每次都需要加锁 */
  std::mutex table_lock_map_latch_;

  /** Structure that holds lock requests for a given RID，显然，一般而言，RID就可以直接定位到一个元组了，
   * 而无需 table_id 这种东西
   */
  std::unordered_map<RID, std::shared_ptr<LockRequestQueue>> row_lock_map_;
  /** Coordination */
  std::mutex row_lock_map_latch_;

  std::atomic<bool> enable_cycle_detection_;
  std::thread *cycle_detection_thread_;
  /** Waits-for graph representation. */
  std::unordered_map<txn_id_t, std::vector<txn_id_t>> waits_for_;
  std::mutex waits_for_latch_;

  /** 我们把锁的升级规则存入 map 中 */
  std::unordered_map<LockMode, std::unordered_set<LockMode>> upgrade_rules_{
    {LockMode::INTENTION_SHARED, {LockMode::SHARED, LockMode::EXCLUSIVE, 
                                  LockMode::INTENTION_EXCLUSIVE, LockMode::SHARED_INTENTION_EXCLUSIVE}}, 
    {LockMode::SHARED, {LockMode::EXCLUSIVE, LockMode::SHARED_INTENTION_EXCLUSIVE}},
    {LockMode::INTENTION_EXCLUSIVE, {LockMode::EXCLUSIVE, LockMode::SHARED_INTENTION_EXCLUSIVE}},
    {LockMode::SHARED_INTENTION_EXCLUSIVE, {LockMode::EXCLUSIVE}}
  };
};

}  // namespace bustub
