//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "container/hash/extendible_hash_table.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::ExtendibleHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                     const KeyComparator &comparator, HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
  //  implement me!
  HashTableDirectoryPage *dir_page =
      reinterpret_cast<HashTableDirectoryPage *>(buffer_pool_manager_->NewPage(&directory_page_id_));
  dir_page->SetPageId(directory_page_id_);
  page_id_t new_bucket_id;
  buffer_pool_manager_->NewPage(&new_bucket_id);
  dir_page->SetBucketPageId(0, new_bucket_id);
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, true, nullptr));
  assert(buffer_pool_manager_->UnpinPage(new_bucket_id, true, nullptr));
}

/*****************************************************************************
 * HELPERS
 *****************************************************************************/
/**
 * Hash - simple helper to downcast MurmurHash's 64-bit hash to 32-bit
 * for extendible hashing.
 *
 * @param key the key to hash
 * @return the downcasted 32-bit hash
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::Hash(KeyType key) {
  return static_cast<uint32_t>(hash_fn_.GetHash(key));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToDirectoryIndex(KeyType key, HashTableDirectoryPage *dir_page) {
  uint32_t hash_key = Hash(key);
  uint32_t mask = dir_page->GetGlobalDepthMask();
  return hash_key & mask;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) {
  uint32_t dir_index = KeyToDirectoryIndex(key, dir_page);
  return dir_page->GetBucketPageId(dir_index);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HashTableDirectoryPage *HASH_TABLE_TYPE::FetchDirectoryPage() {
  return reinterpret_cast<HashTableDirectoryPage *>(buffer_pool_manager_->FetchPage(directory_page_id_));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_BUCKET_TYPE *HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) {
  return reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(buffer_pool_manager_->FetchPage(bucket_page_id));
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  table_latch_.RLock();
  page_id_t bucket_page_id = KeyToPageId(key, dir_page);
  HASH_TABLE_BUCKET_TYPE *bucket = FetchBucketPage(bucket_page_id);
  Page *p = reinterpret_cast<Page *>(bucket);
  p->RLatch();
  // 读取数据
  bool res = bucket->GetValue(key, comparator_, result);
  p->RUnlatch();
  table_latch_.RUnlock();
  // Unpin
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  assert(buffer_pool_manager_->UnpinPage(bucket_page_id, false, nullptr));
  return res;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  table_latch_.RLock();
  page_id_t bucket_page_id = KeyToPageId(key, dir_page);
  HASH_TABLE_BUCKET_TYPE *bucket = FetchBucketPage(bucket_page_id);
  Page *p = reinterpret_cast<Page *>(bucket);
  p->RLatch();
  if (!bucket->IsFull()) {
    bool res = bucket->Insert(key, value, comparator_);
    p->RUnlatch();
    table_latch_.RUnlock();
    assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
    assert(buffer_pool_manager_->UnpinPage(bucket_page_id, true, nullptr));
    return res;
  }
  p->RUnlatch();
  table_latch_.RUnlock();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  assert(buffer_pool_manager_->UnpinPage(bucket_page_id, false, nullptr));
  return SplitInsert(transaction, key, value);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  table_latch_.WLock();
  uint32_t split_bucket_index = KeyToDirectoryIndex(key, dir_page);
  uint32_t split_bucket_depth = dir_page->GetLocalDepth(split_bucket_index);

  // 看看Directory需不需要扩容
  if (split_bucket_depth == dir_page->GetGlobalDepth()) {
    dir_page->IncrGlobalDepth();
  }

  // 增加local depth
  dir_page->IncrLocalDepth(split_bucket_index);

  // 获取当前bucket，先将数据保存下来，然后重新初始化它
  page_id_t split_bucket_page_id = KeyToPageId(key, dir_page);
  HASH_TABLE_BUCKET_TYPE *split_bucket = FetchBucketPage(split_bucket_page_id);
  Page *split_page = reinterpret_cast<Page *>(split_bucket);
  split_page->WLatch();

  uint32_t origin_array_size = split_bucket->NumReadable();
  MappingType *origin_array = split_bucket->GetArrayCopy();
  split_bucket->Clear();

  // 创建一个image bucket，并初始化该image bucket
  page_id_t image_bucket_page_id;
  buffer_pool_manager_->NewPage(&image_bucket_page_id);
  HASH_TABLE_BUCKET_TYPE *image_bucket = FetchBucketPage(image_bucket_page_id);
  Page *image_page = reinterpret_cast<Page *>(image_bucket);
  image_page->WLatch();

  uint32_t split_image_bucket_index = dir_page->GetSplitImageIndex(split_bucket_index);
  dir_page->SetLocalDepth(split_image_bucket_index, dir_page->GetLocalDepth(split_bucket_index));
  dir_page->SetBucketPageId(split_image_bucket_index, image_bucket_page_id);

  // 将所有同一级对应相同bucket的目录项设置为相同的local depth和page
  /**
   * 如果 globaldepth = 3, localdepth = 1 的 bucket 分裂为两个，localdepth = 2.
   * 每个bucket应该对应两个目录项，上一步只给split_image设置了一个对应目录项
   */
  uint32_t diff = 1 << dir_page->GetLocalDepth(split_bucket_index);
  for (uint32_t i = split_bucket_index; i >= diff; i -= diff) {
    dir_page->SetLocalDepth(i, dir_page->GetLocalDepth(split_bucket_index));
    dir_page->SetBucketPageId(i, split_bucket_page_id);
  }
  for (uint32_t i = split_bucket_index; i < dir_page->Size(); i += diff) {
    dir_page->SetLocalDepth(i, dir_page->GetLocalDepth(split_bucket_index));
    dir_page->SetBucketPageId(i, split_bucket_page_id);
  }
  for (uint32_t i = split_image_bucket_index; i >= diff; i -= diff) {
    dir_page->SetLocalDepth(i, dir_page->GetLocalDepth(split_bucket_index));
    dir_page->SetBucketPageId(i, image_bucket_page_id);
  }
  for (uint32_t i = split_image_bucket_index; i < dir_page->Size(); i += diff) {
    dir_page->SetLocalDepth(i, dir_page->GetLocalDepth(split_bucket_index));
    dir_page->SetBucketPageId(i, image_bucket_page_id);
  }

  // 重新插入数据
  for (uint32_t i = 0; i < origin_array_size; i++) {
    page_id_t target_bucket_page_id = KeyToPageId(origin_array[i].first, dir_page);
    assert(target_bucket_page_id == split_bucket_page_id || target_bucket_page_id == image_bucket_page_id);
    // 这里根据新计算的hash结果决定插入哪个bucket
    if (target_bucket_page_id == split_bucket_page_id) {
      split_bucket->Insert(origin_array[i].first, origin_array[i].second, comparator_);
    } else {
      image_bucket->Insert(origin_array[i].first, origin_array[i].second, comparator_);
    }
  }
  delete[] origin_array;

  // 解除table，split，image的写锁
  table_latch_.WUnlock();
  split_page->WUnlatch();
  image_page->WUnlatch();

  // Unpin
  assert(buffer_pool_manager_->UnpinPage(split_bucket_page_id, true, nullptr));
  assert(buffer_pool_manager_->UnpinPage(image_bucket_page_id, true, nullptr));
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, true, nullptr));

  // 最后尝试再次插入
  return Insert(transaction, key, value);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  table_latch_.RLock();
  page_id_t bucket_page_id = KeyToPageId(key, dir_page);
  HASH_TABLE_BUCKET_TYPE *bucket = FetchBucketPage(bucket_page_id);
  Page *p = reinterpret_cast<Page *>(bucket);
  p->WLatch();
  // 删除Key-value
  bool res = bucket->Remove(key, value, comparator_);
  // bucket为空则合并
  if (bucket->IsEmpty()) {
    // Unpin
    p->WUnlatch();
    table_latch_.RUnlock();
    assert(buffer_pool_manager_->UnpinPage(directory_page_id_, true, nullptr));
    assert(buffer_pool_manager_->UnpinPage(bucket_page_id, false, nullptr));
    Merge(transaction, key, value);
    return res;
  }
  p->WUnlatch();
  table_latch_.RUnlock();
  // Unpin
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  assert(buffer_pool_manager_->UnpinPage(bucket_page_id, true, nullptr));
  return res;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  table_latch_.WLock();
  uint32_t target_bucket_index = KeyToDirectoryIndex(key, dir_page);
  uint32_t image_bucket_index = dir_page->GetSplitImageIndex(target_bucket_index);
  page_id_t target_bucket_page_id = KeyToPageId(key, dir_page);

  uint32_t local_depth = dir_page->GetLocalDepth(target_bucket_index);
  // local depth为0说明已经最小了，不收缩
  // 如果该bucket与其split image深度不同，也不收缩
  if (local_depth == 0 || local_depth != dir_page->GetLocalDepth(image_bucket_index)) {
    table_latch_.WUnlock();
    assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
    return;
  }

  // 如果target bucket不为空，则不收缩
  HASH_TABLE_BUCKET_TYPE *target_bucket = FetchBucketPage(target_bucket_page_id);
  Page *target_page = reinterpret_cast<Page *>(target_bucket);
  target_page->RLatch();
  if (!target_bucket->IsEmpty()) {
    target_page->RUnlatch();
    table_latch_.WUnlock();
    assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
    assert(buffer_pool_manager_->UnpinPage(target_bucket_page_id, false, nullptr));
    return;
  }
  target_page->RUnlatch();

  // 删除target bucket，此时该bucket已经为空
  assert(buffer_pool_manager_->UnpinPage(target_bucket_page_id, false, nullptr));
  assert(buffer_pool_manager_->DeletePage(target_bucket_page_id, nullptr));

  // 设置target bucket的page为split image的page，即合并target和split
  page_id_t image_bucket_page_id = dir_page->GetBucketPageId(image_bucket_index);
  dir_page->SetBucketPageId(target_bucket_index, image_bucket_page_id);
  dir_page->DecrLocalDepth(target_bucket_index);
  dir_page->DecrLocalDepth(image_bucket_index);
  assert(dir_page->GetLocalDepth(target_bucket_index) == dir_page->GetLocalDepth(image_bucket_index));

  // 遍历整个directory，将所有指向target bucket page的bucket全部重新指向split image bucket的page
  for (uint32_t i = 0; i < dir_page->Size(); i++) {
    if (dir_page->GetBucketPageId(i) == target_bucket_page_id || dir_page->GetBucketPageId(i) == image_bucket_page_id) {
      dir_page->SetBucketPageId(i, image_bucket_page_id);
      dir_page->SetLocalDepth(i, dir_page->GetLocalDepth(image_bucket_index));
    }
  }

  // 尝试收缩Directory
  // 这里要循环，不能只收缩一次
  while (dir_page->CanShrink()) {
    dir_page->DecrGlobalDepth();
  }

  table_latch_.WUnlock();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, true, nullptr));
}

/*****************************************************************************
 * GETGLOBALDEPTH - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::GetGlobalDepth() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t global_depth = dir_page->GetGlobalDepth();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
  return global_depth;
}

/*****************************************************************************
 * VERIFY INTEGRITY - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::VerifyIntegrity() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  dir_page->VerifyIntegrity();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
}

/*****************************************************************************
 * TEMPLATE DEFINITIONS - DO NOT TOUCH
 *****************************************************************************/
template class ExtendibleHashTable<int, int, IntComparator>;

template class ExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class ExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class ExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class ExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class ExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
