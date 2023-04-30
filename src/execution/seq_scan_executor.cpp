//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      table_info_(nullptr),
      table_heap_(nullptr),
      iter_(nullptr, RID(), nullptr) {}

void SeqScanExecutor::Init() {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  table_heap_ = table_info_->table_.get();
  iter_ = table_heap_->Begin(exec_ctx_->GetTransaction());
}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  // 表的所有列和想要输出的列
  Schema table_schema = table_info_->schema_;
  const Schema *out_schema = this->GetOutputSchema();

  while (iter_ != table_heap_->End()) {
    auto table_tuple = *iter_;
    RID origin_rid = iter_->GetRid();

    // 加锁
    LockManager *lock_mgr = GetExecutorContext()->GetLockManager();
    Transaction *txn = GetExecutorContext()->GetTransaction();
    if (lock_mgr != nullptr) {
      if (txn->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
        if (!txn->IsSharedLocked(origin_rid) && !txn->IsExclusiveLocked(origin_rid)) {
          lock_mgr->LockShared(txn, origin_rid);
        }
      }
    }

    std::vector<Value> res;

    // 遍历输出的列，把该行的数据提出来（跳过不需要的列对应的单元格数据）
    for (const auto &col : out_schema->GetColumns()) {
      Value value = col.GetExpr()->Evaluate(&table_tuple, &table_schema);
      res.emplace_back(value);
    }

    // 解锁
    if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED && lock_mgr != nullptr) {
      lock_mgr->Unlock(txn, origin_rid);
    }

    // 迭代器+1
    ++iter_;

    // 构建新行，看看该行符不符合条件，符合则返回，不符合就继续找下一行
    Tuple temp_tuple(res, out_schema);
    auto predicate = plan_->GetPredicate();

    // 不存在谓词或符合谓词，输出tuple
    if (predicate == nullptr || predicate->Evaluate(&temp_tuple, out_schema).GetAs<bool>()) {
      *tuple = temp_tuple;
      *rid = origin_rid;
      return true;
    }
  }
  return false;
}

}  // namespace bustub
