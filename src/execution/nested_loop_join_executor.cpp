//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {}

void NestedLoopJoinExecutor::Init() {
  // 一次性将满足连接条件的 tuple 全放入 buffer
  buffer_.clear();
  const Schema *out_schema = this->GetOutputSchema();
  const Schema *left_schema = left_executor_->GetOutputSchema();
  const Schema *right_schema = right_executor_->GetOutputSchema();

  Tuple left_tuple;
  Tuple right_tuple;
  RID temp_rid;
  // 分别循环执行查询
  left_executor_->Init();
  while (left_executor_->Next(&left_tuple, &temp_rid)) {
    right_executor_->Init();
    while (right_executor_->Next(&right_tuple, &temp_rid)) {
      // 计算连接条件
      auto predicate = plan_->Predicate();
      if (predicate == nullptr ||
          predicate->EvaluateJoin(&left_tuple, left_schema, &right_tuple, right_schema).GetAs<bool>()) {
        // 计算要输出的列
        std::vector<Value> res;
        for (const auto &col : out_schema->GetColumns()) {
          Value value = col.GetExpr()->EvaluateJoin(&left_tuple, left_schema, &right_tuple, right_schema);
          res.emplace_back(value);
        }
        buffer_.emplace_back(Tuple(res, out_schema));
      }
    }
  }
}

bool NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) {
  // 每次 next() 取出一个tuple
  if (!buffer_.empty()) {
    *tuple = buffer_.back();
    buffer_.pop_back();
    *rid = tuple->GetRid();
    return true;
  }
  return false;
}

}  // namespace bustub
