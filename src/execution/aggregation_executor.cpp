//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      ht_(plan->GetAggregates(), plan->GetAggregateTypes()),
      iter_(ht_.Begin()) {}

void AggregationExecutor::Init() {
  child_->Init();

  Tuple tuple;
  RID rid;
  while (child_->Next(&tuple, &rid)) {
    ht_.InsertCombine(MakeAggregateKey(&tuple), MakeAggregateValue(&tuple));
  }
  iter_ = ht_.Begin();
}

bool AggregationExecutor::Next(Tuple *tuple, RID *rid) {
  const Schema *out_schema = this->GetOutputSchema();
  while (iter_ != ht_.End()) {
    const auto &key = iter_.Key();
    const auto &val = iter_.Val();
    ++iter_;

    auto *having = plan_->GetHaving();
    if (having == nullptr || having->EvaluateAggregate(key.group_bys_, val.aggregates_).GetAs<bool>()) {
      std::vector<Value> res;
      for (const auto &col : out_schema->GetColumns()) {
        Value value = col.GetExpr()->EvaluateAggregate(key.group_bys_, val.aggregates_);
        res.emplace_back(value);
      }
      *tuple = Tuple(res, out_schema);
      *rid = tuple->GetRid();
      return true;
    }
  }
  return false;
}

const AbstractExecutor *AggregationExecutor::GetChildExecutor() const { return child_.get(); }

}  // namespace bustub
