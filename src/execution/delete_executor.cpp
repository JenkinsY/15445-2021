//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  if (child_executor_ != nullptr) {
    child_executor_->Init();
  }
}

bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  auto table_heap = table_info->table_.get();
  Transaction *transaction = GetExecutorContext()->GetTransaction();
  LockManager *lock_mgr = GetExecutorContext()->GetLockManager();
  Tuple del_tuple;
  RID del_rid;

  /* 如何找到需要删除的tuple：根据 DeletePlanNode->SeqScanPlanNode */
  // child_executor_会指向一个查询器（SeqScanExecutor）
  // del_tuple和del_rid的值会从查询器的Next()函数返回
  while (child_executor_->Next(&del_tuple, &del_rid)) {
    // 加锁
    if (lock_mgr != nullptr) {
      if (transaction->IsSharedLocked(del_rid)) {
        lock_mgr->LockUpgrade(transaction, del_rid);
      } else if (!transaction->IsExclusiveLocked(del_rid)) {
        lock_mgr->LockExclusive(transaction, del_rid);
      }
    }
    // 调用TableHeap标记删除状态
    table_heap->MarkDelete(del_rid, exec_ctx_->GetTransaction());
    // 更新索引
    auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);
    for (const auto &index : indexes) {
      auto key_tuple =
          del_tuple.KeyFromTuple(table_info->schema_, *index->index_->GetKeySchema(), index->index_->GetKeyAttrs());
      index->index_->DeleteEntry(key_tuple, del_rid, exec_ctx_->GetTransaction());
      // 在事务中记录下变更
      transaction->GetIndexWriteSet()->emplace_back(IndexWriteRecord(
          del_rid, table_info->oid_, WType::DELETE, del_tuple, index->index_oid_, exec_ctx_->GetCatalog()));
    }
    // 解锁
    if (transaction->GetIsolationLevel() == IsolationLevel::READ_COMMITTED && lock_mgr != nullptr) {
      lock_mgr->Unlock(transaction, del_rid);
    }
  }
  return false;
}

}  // namespace bustub
