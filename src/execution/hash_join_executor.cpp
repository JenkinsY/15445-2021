//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
// #include "common/logger.h"
namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_child_(std::move(left_child)),
      right_child_(std::move(right_child)) {}

void HashJoinExecutor::Init() {
  buffer_.clear();
  map_.clear();
  const Schema *out_schema = this->GetOutputSchema();
  const Schema *left_schema = left_child_->GetOutputSchema();
  const Schema *right_schema = right_child_->GetOutputSchema();

  // 通过左侧查询构造哈希
  Tuple left_tuple;
  RID temp_rid;
  left_child_->Init();
  while (left_child_->Next(&left_tuple, &temp_rid)) {
    HashJoinKey left_key;
    left_key.column_value_ = plan_->LeftJoinKeyExpression()->Evaluate(&left_tuple, left_schema);
    // LOG_DEBUG("left_key_value : %s", left_key.column_value_.ToString().c_str());
    if (map_.count(left_key) != 0) {
      map_[left_key].emplace_back(left_tuple);
    } else {
      map_[left_key] = std::vector<Tuple>{left_tuple};
    }
  }

  // 遍历右侧查询，得到查询结果
  Tuple right_tuple;
  right_child_->Init();
  while (right_child_->Next(&right_tuple, &temp_rid)) {
    HashJoinKey right_key;
    right_key.column_value_ = plan_->RightJoinKeyExpression()->Evaluate(&right_tuple, right_schema);
    // 遍历每一个对应的左侧查询
    if (map_.count(right_key) != 0) {
      for (const auto &ltuple : map_[right_key]) {
        std::vector<Value> res;
        for (const auto &col : out_schema->GetColumns()) {
          Value value = col.GetExpr()->EvaluateJoin(&ltuple, left_schema, &right_tuple, right_schema);
          res.emplace_back(value);
          // LOG_DEBUG("right_key_value : %s", value.ToString().c_str());
        }
        buffer_.emplace_back(Tuple(res, out_schema));
      }
    }
  }
}

bool HashJoinExecutor::Next(Tuple *tuple, RID *rid) {
  if (!buffer_.empty()) {
    *tuple = buffer_.back();
    buffer_.pop_back();
    *rid = tuple->GetRid();
    return true;
  }
  return false;
}

}  // namespace bustub
