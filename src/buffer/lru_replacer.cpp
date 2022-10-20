//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) {
  capacity_ = num_pages;
  lru_list_.clear();
  lru_hash_.clear();
}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::scoped_lock lock{mutex_};

  if (lru_list_.empty()) {
    return false;
  }
  auto last = lru_list_.back();
  lru_list_.pop_back();
  lru_hash_.erase(last);
  *frame_id = last;
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  std::scoped_lock lock{mutex_};

  if (lru_hash_.find(frame_id) == lru_hash_.end()) {
    return;
  }
  lru_list_.remove(frame_id);
  lru_hash_.erase(frame_id);
  return;
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::scoped_lock lock{mutex_};

  if (lru_hash_.find(frame_id) != lru_hash_.end()) {
    return;
  }
  if (capacity_ == lru_list_.size()) {
    auto last = lru_list_.back();
    lru_list_.pop_back();
    lru_hash_.erase(last);
  }
  lru_list_.push_front(frame_id);
  lru_hash_[frame_id] = lru_list_.begin();
}

size_t LRUReplacer::Size() { return lru_list_.size(); }

}  // namespace bustub
