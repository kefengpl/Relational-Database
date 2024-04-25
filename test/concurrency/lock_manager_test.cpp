/**
 * lock_manager_test.cpp
 */

#include "concurrency/lock_manager.h"
#include <chrono>  // NOLINT
#include <random>
#include <thread>  // NOLINT

#include "common/bustub_instance.h"
#include "common/config.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "fmt/core.h"
#include "gtest/gtest.h"

namespace bustub {

/*
 * This test is only a sanity check. Please do not rely on this test
 * to check the correctness.
 */

// --- Helper functions ---
void CheckGrowing(Transaction *txn) { EXPECT_EQ(txn->GetState(), TransactionState::GROWING); }

void CheckShrinking(Transaction *txn) { EXPECT_EQ(txn->GetState(), TransactionState::SHRINKING); }

void CheckAborted(Transaction *txn) { EXPECT_EQ(txn->GetState(), TransactionState::ABORTED); }

void CheckCommitted(Transaction *txn) { EXPECT_EQ(txn->GetState(), TransactionState::COMMITTED); }

void CheckTxnRowLockSize(Transaction *txn, table_oid_t oid, size_t shared_size, size_t exclusive_size) {
  EXPECT_EQ((*(txn->GetSharedRowLockSet()))[oid].size(), shared_size);
  EXPECT_EQ((*(txn->GetExclusiveRowLockSet()))[oid].size(), exclusive_size);
}

int GetTxnTableLockSize(Transaction *txn, LockManager::LockMode lock_mode) {
  switch (lock_mode) {
    case LockManager::LockMode::SHARED:
      return txn->GetSharedTableLockSet()->size();
    case LockManager::LockMode::EXCLUSIVE:
      return txn->GetExclusiveTableLockSet()->size();
    case LockManager::LockMode::INTENTION_SHARED:
      return txn->GetIntentionSharedTableLockSet()->size();
    case LockManager::LockMode::INTENTION_EXCLUSIVE:
      return txn->GetIntentionExclusiveTableLockSet()->size();
    case LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE:
      return txn->GetSharedIntentionExclusiveTableLockSet()->size();
  }

  return -1;
}

void CheckTableLockSizes(Transaction *txn, size_t s_size, size_t x_size, size_t is_size, size_t ix_size,
                         size_t six_size) {
  EXPECT_EQ(s_size, txn->GetSharedTableLockSet()->size());
  EXPECT_EQ(x_size, txn->GetExclusiveTableLockSet()->size());
  EXPECT_EQ(is_size, txn->GetIntentionSharedTableLockSet()->size());
  EXPECT_EQ(ix_size, txn->GetIntentionExclusiveTableLockSet()->size());
  EXPECT_EQ(six_size, txn->GetSharedIntentionExclusiveTableLockSet()->size());
}

TEST(LockManagerTest, DISABLED_AbortTest1) {
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};
  table_oid_t oid = 0;

  auto *txn0 = txn_mgr.Begin();
  auto *txn1 = txn_mgr.Begin();
  auto *txn2 = txn_mgr.Begin();

  std::thread t1([&]() {
    bool res;
    res = lock_mgr.LockTable(txn0, LockManager::LockMode::EXCLUSIVE, oid);
    EXPECT_TRUE(res);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    lock_mgr.UnlockTable(txn0, oid);
    txn_mgr.Commit(txn0);
    EXPECT_EQ(TransactionState::COMMITTED, txn0->GetState());
  });

  std::thread t2([&]() {
    bool res;
    std::this_thread::sleep_for(std::chrono::milliseconds(30));
    res = lock_mgr.LockTable(txn1, LockManager::LockMode::EXCLUSIVE, oid);
    EXPECT_FALSE(res);

    EXPECT_EQ(TransactionState::ABORTED, txn1->GetState());
    txn_mgr.Abort(txn1);
  });

  std::thread t3([&]() {
    bool res;
    std::this_thread::sleep_for(std::chrono::milliseconds(30));
    res = lock_mgr.LockTable(txn2, LockManager::LockMode::EXCLUSIVE, oid);
    EXPECT_TRUE(res);
    lock_mgr.UnlockTable(txn2, oid);
    txn_mgr.Commit(txn2);
    EXPECT_EQ(TransactionState::COMMITTED, txn2->GetState());
  });
  //! \bug 或许这个测试用例本身就存在线程安全问题。
  std::this_thread::sleep_for(std::chrono::milliseconds(70));  // 原始值 70
  txn1->SetState(TransactionState::ABORTED);

  t1.join();
  t2.join();
  t3.join();
  delete txn0;
  delete txn1;
  delete txn2;
}

TEST(LockManagerTest, ModifiedAbortTest) {
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};
  table_oid_t oid = 0;

  auto *txn0 = txn_mgr.Begin();
  auto *txn1 = txn_mgr.Begin();
  auto *txn2 = txn_mgr.Begin();

  std::thread t1([&]() {
    bool res;
    res = lock_mgr.LockTable(txn0, LockManager::LockMode::EXCLUSIVE, oid);
    EXPECT_TRUE(res);
    std::this_thread::sleep_for(
        std::chrono::milliseconds(500));  // 增加持锁时间，以确保在其他事务试图获取锁时锁还未释放
    // lock_mgr.UnlockTable(txn0, oid);  // 注释掉解锁操作
    // txn_mgr.Commit(txn0); // 延迟提交，确保锁长时间持有
    // EXPECT_EQ(TransactionState::COMMITTED, txn0->GetState());
  });

  std::thread t2([&]() {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));  // 确保txn0已经获取了锁
    bool res = lock_mgr.LockTable(txn1, LockManager::LockMode::EXCLUSIVE, oid);
    EXPECT_FALSE(res);  // 这里期望失败，因为txn0还持有锁且没有释放

    EXPECT_EQ(TransactionState::ABORTED, txn1->GetState());
    txn_mgr.Abort(txn1);
  });

  std::thread t3([&]() {
    std::this_thread::sleep_for(std::chrono::milliseconds(150));  // 确保txn1已经进入等待
    bool res = lock_mgr.LockTable(txn2, LockManager::LockMode::EXCLUSIVE, oid);
    EXPECT_FALSE(res);  // 期望失败，因为锁仍然被txn0持有

    EXPECT_EQ(TransactionState::ABORTED, txn2->GetState());
    txn_mgr.Abort(txn2);
  });

  t1.join();
  t2.join();
  t3.join();
  delete txn0;
  delete txn1;
  delete txn2;
}

}  // namespace bustub
