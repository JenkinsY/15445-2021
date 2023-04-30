//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"
#include "concurrency/transaction_manager.h"

#include <utility>
#include <vector>

namespace bustub {

bool LockManager::LockShared(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> lk(latch_);
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  // 事务状态为SHRINKING时不能加锁
  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    return false;
  }
  // 事务类型为READ_UNCOMMITTED没有S锁
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCKSHARED_ON_READ_UNCOMMITTED);
    return false;
  }
  // 该tuple已经拥有锁
  if (txn->IsSharedLocked(rid) || txn->IsExclusiveLocked(rid)) {
    return true;
  }
  
  LOG_DEBUG("%d want to get share %d",(int)txn->GetTransactionId(), (int)rid.GetSlotNum());
  txn->SetState(TransactionState::GROWING);
  auto &request_queue = lock_table_[rid].request_queue_;
  auto &cv = lock_table_[rid].cv_;
  auto txn_id = txn->GetTransactionId();
  request_queue.emplace_back(txn_id, LockMode::SHARED);

  auto check_func = [&]() {
    bool is_continue = true;
    auto it = request_queue.begin();
    while (it != request_queue.end()) {
      if (it->txn_id_ == txn_id) {
        it->granted_ = is_continue;
        return is_continue;
      }
      if (it->lock_mode_ == LockMode::EXCLUSIVE) {
        // 杀死低优先级的所有锁
        if (it->txn_id_ > txn_id) {
          Transaction *tst = TransactionManager::GetTransaction(it->txn_id_);
          tst->SetState(TransactionState::ABORTED);
          tst->GetExclusiveLockSet()->erase(rid);
          tst->GetSharedLockSet()->erase(rid);
          it = lock_table_[rid].request_queue_.erase(it);
          LOG_DEBUG("SHARED: %d kill %d",(int)txn->GetTransactionId(), (int)it->txn_id_);
          cv.notify_all();
        } else {
          is_continue = false;
          ++it;
        }
      }
      else ++it;
    }
    return true;
  };

  while (!check_func() && txn->GetState() != TransactionState::ABORTED) {
    cv.wait(lk);
  }
  // 在check过程中可能被aborted
  if (txn->GetState() == TransactionState::ABORTED) {
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
    return false;
  }

  txn->GetSharedLockSet()->emplace(rid);
  return true;
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> lk(latch_);
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    return false;
  }
  // 该tuple已经有共享锁，升级锁
  if (txn->IsSharedLocked(rid)) {
    LockUpgrade(txn, rid);
  }
  if (txn->IsExclusiveLocked(rid)) {
    return true;
  }
  
  LOG_DEBUG("%d want to get exclusive %d",(int)txn->GetTransactionId(), (int)rid.GetSlotNum());
  txn->SetState(TransactionState::GROWING);
  auto &request_queue = lock_table_[rid].request_queue_;
  auto &cv = lock_table_[rid].cv_;
  auto txn_id = txn->GetTransactionId();
  request_queue.emplace_back(txn_id, LockMode::EXCLUSIVE);

  auto check_func = [&]() {
    auto it = request_queue.begin();
    while (it != request_queue.end()) {
      if (it->txn_id_ == txn_id) {
        it->granted_ = true;
        return true;
      }
      // 杀死低优先级的所有锁
      if (it->txn_id_ > txn_id) {
        Transaction *tst = TransactionManager::GetTransaction(it->txn_id_);
        tst->SetState(TransactionState::ABORTED);
        tst->GetExclusiveLockSet()->erase(rid);
        tst->GetSharedLockSet()->erase(rid);
        it = lock_table_[rid].request_queue_.erase(it);
        LOG_DEBUG("SHARED: %d kill %d",(int)txn->GetTransactionId(), (int)it->txn_id_);
        cv.notify_all();
      } else {
        // 继续wait
        return false;
      }
    }
    return true;
  };

  while (!check_func() && txn->GetState() != TransactionState::ABORTED) {
    cv.wait(lk);
  }
  // 在check过程中可能被aborted
  if (txn->GetState() == TransactionState::ABORTED) {
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
    return false;
  }
  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> lk(latch_);
  // 检查事务当前没有被终止
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  // 事务状态为SHRINKING时不能升级锁
  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    return false;
  }
  // 已经有互斥锁了
  if (txn->IsExclusiveLocked(rid)) {
    return true;
  }
  // 该事务已经提交过更新锁了
  if (lock_table_[rid].upgrading_ != INVALID_TXN_ID) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  lock_table_[rid].upgrading_ = txn->GetTransactionId();

  // 把锁请求清除
  auto &request_queue = lock_table_[rid].request_queue_;
  auto &cv = lock_table_[rid].cv_;
  auto txn_id = txn->GetTransactionId();
  auto it = request_queue.begin();
  while (it != request_queue.end()) {
    if (it->txn_id_ == txn_id) {
      break;
    }
    ++it;
  }
  request_queue.erase(it);
  txn->GetSharedLockSet()->erase(rid);

  cv.notify_all();

  // 重新加互斥锁
  LOG_DEBUG("%d want to get exclusive %d",(int)txn->GetTransactionId(), (int)rid.GetSlotNum());
  request_queue.emplace_back(txn_id, LockMode::EXCLUSIVE);

  auto check_func = [&]() {
    auto it = request_queue.begin();
    while (it != request_queue.end()) {
      if (it->txn_id_ == txn_id) {
        it->granted_ = true;
        return true;
      }
      // 杀死低优先级的所有锁
      if (it->txn_id_ > txn_id) {
        Transaction *tst = TransactionManager::GetTransaction(it->txn_id_);
        tst->SetState(TransactionState::ABORTED);
        tst->GetExclusiveLockSet()->erase(rid);
        tst->GetSharedLockSet()->erase(rid);
        it = lock_table_[rid].request_queue_.erase(it);
        LOG_DEBUG("SHARED: %d kill %d",(int)txn->GetTransactionId(), (int)it->txn_id_);
        cv.notify_all();
      } else {
        // 继续wait
        return false;
      }
    }
    return true;
  };

  while (!check_func() && txn->GetState() != TransactionState::ABORTED) {
    cv.wait(lk);
  }
  // 在check过程中可能被aborted
  if (txn->GetState() == TransactionState::ABORTED) {
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
    return false;
  }

  lock_table_[rid].upgrading_ = INVALID_TXN_ID;

  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> lk(latch_);
  // 只有REPEATABLE_READ遵守2PL，应该改状态
  if (txn->GetState() == TransactionState::GROWING && txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    txn->SetState(TransactionState::SHRINKING);
  }
  // 把锁请求清除
  auto &request_queue = lock_table_[rid].request_queue_;
  auto &cv = lock_table_[rid].cv_;
  auto txn_id = txn->GetTransactionId();
  auto it = request_queue.begin();
  while (it != request_queue.end()) {
    if (it->txn_id_ == txn_id) {
      break;
    }
    ++it;
  }
  request_queue.erase(it);
  cv.notify_all();
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->erase(rid);
  return true;
}

}  // namespace bustub
