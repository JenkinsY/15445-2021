//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  table_heap_ = table_info_->table_.get();
}

void InsertExecutor::Init() {
  if (child_executor_ != nullptr) {
    child_executor_->Init();
  }
}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  // 为什么不用 Tuple* 和 RID* --- 未初始化 有空指针风险
  Tuple insert_tuple;
  RID insert_rid;
  // 先判断有没有子计划，如果没有的话直接插入即可
  // 有的话先执行子计划，仿照ExecutionEngine即可
  if (plan_->IsRawInsert()) {
    for (const auto &row_values : plan_->RawValues()) {
      insert_tuple = Tuple(row_values, &(table_info_->schema_));
      InsertIntoTableWithIndex(&insert_tuple);
    }
  } else {
    while (child_executor_->Next(&insert_tuple, &insert_rid)) {
      InsertIntoTableWithIndex(&insert_tuple);
    }
  }
  return false;
}

void InsertExecutor::InsertIntoTableWithIndex(Tuple *tuple) {
  // 调用table_heap，插入记录
  RID cur_rid;
  table_heap_->InsertTuple(*tuple, &cur_rid, exec_ctx_->GetTransaction());
  // 加锁
  Transaction *transaction = GetExecutorContext()->GetTransaction();
  LockManager *lock_mgr = GetExecutorContext()->GetLockManager();
  if (lock_mgr != nullptr) {
    if (transaction->IsSharedLocked(cur_rid)) {
      lock_mgr->LockUpgrade(transaction, cur_rid);
    } else if (!transaction->IsExclusiveLocked(cur_rid)) {
      lock_mgr->LockExclusive(transaction, cur_rid);
    }
  }
  // 更新索引
  auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  for (const auto &index : indexes) {
    auto key_tuple =
        tuple->KeyFromTuple(table_info_->schema_, *index->index_->GetKeySchema(), index->index_->GetKeyAttrs());
    index->index_->InsertEntry(key_tuple, cur_rid, exec_ctx_->GetTransaction());
    // 在事务中记录下变更
    transaction->GetIndexWriteSet()->emplace_back(IndexWriteRecord(
        cur_rid, table_info_->oid_, WType::INSERT, *tuple, index->index_oid_, exec_ctx_->GetCatalog()));
  }
  // 解锁
  if (transaction->GetIsolationLevel() == IsolationLevel::READ_COMMITTED && lock_mgr != nullptr) {
    lock_mgr->Unlock(transaction, cur_rid);
  }
}

}  // namespace bustub
