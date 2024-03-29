//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      table_info_(exec_ctx_->GetCatalog()->GetTable(plan_->TableOid())),
      child_executor_(std::move(child_executor)) {}

void UpdateExecutor::Init() {
  if (child_executor_ != nullptr) {
    child_executor_->Init();
  }
}

bool UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  Tuple old_tuple;
  Tuple new_tuple;
  RID tuple_rid;
  Transaction *transaction = GetExecutorContext()->GetTransaction();
  LockManager *lock_mgr = GetExecutorContext()->GetLockManager();
  // 执行子查询
  while (child_executor_->Next(&old_tuple, &tuple_rid)) {
    // 加锁
    if (lock_mgr != nullptr) {
      if (transaction->IsSharedLocked(tuple_rid)) {
        lock_mgr->LockUpgrade(transaction, tuple_rid);
      } else if (!transaction->IsExclusiveLocked(tuple_rid)) {
        lock_mgr->LockExclusive(transaction, tuple_rid);
      }
    }
    new_tuple = GenerateUpdatedTuple(old_tuple);
    table_info_->table_->UpdateTuple(new_tuple, tuple_rid, exec_ctx_->GetTransaction());

    auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
    for (const auto &index : indexes) {
      auto key_tuple =
          old_tuple.KeyFromTuple(table_info_->schema_, *index->index_->GetKeySchema(), index->index_->GetKeyAttrs());
      index->index_->DeleteEntry(key_tuple, tuple_rid, exec_ctx_->GetTransaction());
      auto new_key_tuple =
          new_tuple.KeyFromTuple(table_info_->schema_, *index->index_->GetKeySchema(), index->index_->GetKeyAttrs());
      index->index_->InsertEntry(new_key_tuple, tuple_rid, exec_ctx_->GetTransaction());
      // 在事务中记录下变更
      IndexWriteRecord write_record(tuple_rid, table_info_->oid_, WType::DELETE, new_tuple, index->index_oid_,
                                    exec_ctx_->GetCatalog());
      write_record.old_tuple_ = old_tuple;
      transaction->GetIndexWriteSet()->emplace_back(write_record);
    }
    // 解锁
    if (transaction->GetIsolationLevel() == IsolationLevel::READ_COMMITTED && lock_mgr != nullptr) {
      lock_mgr->Unlock(transaction, tuple_rid);
    }
  }
  return false;
}

Tuple UpdateExecutor::GenerateUpdatedTuple(const Tuple &src_tuple) {
  const auto &update_attrs = plan_->GetUpdateAttr();
  Schema schema = table_info_->schema_;
  uint32_t col_count = schema.GetColumnCount();
  std::vector<Value> values;
  for (uint32_t idx = 0; idx < col_count; idx++) {
    if (update_attrs.find(idx) == update_attrs.cend()) {
      values.emplace_back(src_tuple.GetValue(&schema, idx));
    } else {
      const UpdateInfo info = update_attrs.at(idx);
      Value val = src_tuple.GetValue(&schema, idx);
      switch (info.type_) {
        case UpdateType::Add:
          values.emplace_back(val.Add(ValueFactory::GetIntegerValue(info.update_val_)));
          break;
        case UpdateType::Set:
          values.emplace_back(ValueFactory::GetIntegerValue(info.update_val_));
          break;
      }
    }
  }
  return Tuple{values, &schema};
}

}  // namespace bustub
