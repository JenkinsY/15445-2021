//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_header_page.cpp
//
// Identification: src/storage/page/hash_table_header_page.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/hash_table_directory_page.h"
#include <algorithm>
#include <unordered_map>
#include "common/logger.h"

namespace bustub {
page_id_t HashTableDirectoryPage::GetPageId() const { return page_id_; }

void HashTableDirectoryPage::SetPageId(bustub::page_id_t page_id) { page_id_ = page_id; }

lsn_t HashTableDirectoryPage::GetLSN() const { return lsn_; }

void HashTableDirectoryPage::SetLSN(lsn_t lsn) { lsn_ = lsn; }

uint32_t HashTableDirectoryPage::GetGlobalDepth() { return global_depth_; }

// For example, global depth 3 corresponds to 0x00000007 in a 32-bit representation.
// depth为 3 表示前面29位0，末尾3位1，用移位实现
uint32_t HashTableDirectoryPage::GetGlobalDepthMask() { return (1 << global_depth_) - 1; }

uint32_t HashTableDirectoryPage::GetLocalDepthMask(uint32_t bucket_idx) {
  uint8_t depth = local_depths_[bucket_idx];
  return (1 << depth) - 1;
}

// 将目录复制一份放到末尾，没有进行数据迁移，两个bucket对应一个page
void HashTableDirectoryPage::IncrGlobalDepth() {
  for (int i = 0, new_idx = Size(); i < Size(); i++, new_idx++) {
    bucket_page_ids_[new_idx] = bucket_page_ids_[i];
    local_depths_[new_idx] = local_depths_[i];
  }
  global_depth_++;
}

void HashTableDirectoryPage::DecrGlobalDepth() { global_depth_--; }

page_id_t HashTableDirectoryPage::GetBucketPageId(uint32_t bucket_idx) { return bucket_page_ids_[bucket_idx]; }

void HashTableDirectoryPage::SetBucketPageId(uint32_t bucket_idx, page_id_t bucket_page_id) {
  bucket_page_ids_[bucket_idx] = bucket_page_id;
}

uint32_t HashTableDirectoryPage::Size() { return 1 << global_depth_; }

// 整个Directory能不能收缩取决于每个localdepth是否都比globaldepth小
bool HashTableDirectoryPage::CanShrink() {
  for (uint32_t i = 0; i < Size(); i++) {
    if (local_depths_[i] == global_depth_) {
      return false;
    }
  }
  return true;
}

uint32_t HashTableDirectoryPage::GetLocalDepth(uint32_t bucket_idx) { return local_depths_[bucket_idx]; }

void HashTableDirectoryPage::SetLocalDepth(uint32_t bucket_idx, uint8_t local_depth) {
  assert(local_depth <= global_depth_);
  local_depths_[bucket_idx] = local_depth;
}

void HashTableDirectoryPage::IncrLocalDepth(uint32_t bucket_idx) {
  assert(local_depths_[bucket_idx] < global_depth_);
  local_depths_[bucket_idx]++;
}

void HashTableDirectoryPage::DecrLocalDepth(uint32_t bucket_idx) { local_depths_[bucket_idx]--; }

// 这个函数实现一个翻转最高位为输出编号的功能。
// GetSplitImageIndex(101) = 001; GetSplitImageIndex(001) = 101
uint32_t HashTableDirectoryPage::GetSplitImageIndex(uint32_t bucket_idx) {
  // Example: 当对应的localdepth是3时：
  // 1<<(3-1) = 0000....00100
  return bucket_idx ^ (1 << (local_depths_[bucket_idx] - 1));
}

uint32_t HashTableDirectoryPage::GetLocalHighBit(uint32_t bucket_idx) {
  size_t tmp = global_depth_ - local_depths_[bucket_idx];
  return bucket_idx >> tmp << tmp;
}

/**
 * VerifyIntegrity - Use this for debugging but **DO NOT CHANGE**
 *
 * If you want to make changes to this, make a new function and extend it.
 *
 * Verify the following invariants:
 * (1) All LD <= GD.
 * (2) Each bucket has precisely 2^(GD - LD) pointers pointing to it.
 * (3) The LD is the same at each index with the same bucket_page_id
 */
void HashTableDirectoryPage::VerifyIntegrity() {
  //  build maps of {bucket_page_id : pointer_count} and {bucket_page_id : local_depth}
  std::unordered_map<page_id_t, uint32_t> page_id_to_count = std::unordered_map<page_id_t, uint32_t>();
  std::unordered_map<page_id_t, uint32_t> page_id_to_ld = std::unordered_map<page_id_t, uint32_t>();

  //  verify for each bucket_page_id, pointer
  for (uint32_t curr_idx = 0; curr_idx < Size(); curr_idx++) {
    page_id_t curr_page_id = bucket_page_ids_[curr_idx];
    uint32_t curr_ld = local_depths_[curr_idx];
    assert(curr_ld <= global_depth_);

    ++page_id_to_count[curr_page_id];

    if (page_id_to_ld.count(curr_page_id) > 0 && curr_ld != page_id_to_ld[curr_page_id]) {
      uint32_t old_ld = page_id_to_ld[curr_page_id];
      LOG_WARN("Verify Integrity: curr_local_depth: %u, old_local_depth %u, for page_id: %u", curr_ld, old_ld,
               curr_page_id);
      PrintDirectory();
      assert(curr_ld == page_id_to_ld[curr_page_id]);
    } else {
      page_id_to_ld[curr_page_id] = curr_ld;
    }
  }

  auto it = page_id_to_count.begin();

  while (it != page_id_to_count.end()) {
    page_id_t curr_page_id = it->first;
    uint32_t curr_count = it->second;
    uint32_t curr_ld = page_id_to_ld[curr_page_id];
    uint32_t required_count = 0x1 << (global_depth_ - curr_ld);
    // if global_depth_ 为 3, local_depth_ 为 1, 这个 bucket 应该有 4 个目录指向它 [1 << (3 - 1) == 4]
    if (curr_count != required_count) {
      LOG_WARN("Verify Integrity: curr_count: %u, required_count %u, for page_id: %u", curr_ld, required_count,
               curr_page_id);
      PrintDirectory();
      assert(curr_count == required_count);
    }
    it++;
  }
}

void HashTableDirectoryPage::PrintDirectory() {
  LOG_DEBUG("======== DIRECTORY (global_depth_: %u) ========", global_depth_);
  LOG_DEBUG("| bucket_idx | page_id | local_depth |");
  for (uint32_t idx = 0; idx < static_cast<uint32_t>(0x1 << global_depth_); idx++) {
    LOG_DEBUG("|      %u     |     %u     |     %u     |", idx, bucket_page_ids_[idx], local_depths_[idx]);
  }
  LOG_DEBUG("================ END DIRECTORY ================");
}

}  // namespace bustub
