//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_bucket_page.cpp
//
// Identification: src/storage/page/hash_table_bucket_page.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/hash_table_bucket_page.h"
#include "common/logger.h"
#include "common/util/hash_util.h"
#include "storage/index/generic_key.h"
#include "storage/index/hash_comparator.h"
#include "storage/table/tmp_tuple.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::GetValue(KeyType key, KeyComparator cmp, std::vector<ValueType> *result) {
  bool ret = false;
  for (size_t i = 0; i < BUCKET_ARRAY_SIZE; i++) {
    if (!IsOccupied(i)) {
      break;
    }
    if (IsReadable(i) && cmp(key, KeyAt(i)) == 0) {
      result->push_back(array_[i].second);
      ret = true;
    }
  }
  return ret;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::Insert(KeyType key, ValueType value, KeyComparator cmp) {
  int free_idx = -1;
  for (size_t i = 0; i < BUCKET_ARRAY_SIZE; i++) {
    // 在末尾插入或者在中间被删除过的位置插入，保证IsOccupied为1的位置连续
    if (free_idx == -1 && (!IsReadable(i) || !IsOccupied(i))) {
      free_idx = i;
    }
    // 如果是在末尾有空位直接跳出，否则判断后面是否有相同的值
    if (!IsOccupied(i)) {
      break;
    }
    if (IsReadable(i) && cmp(key, KeyAt(i)) == 0 && ValueAt(i) == value) {
      return false;
    }
  }

  if (free_idx == -1) {
    return false;
  }

  array_[free_idx] = MappingType(key, value);
  SetOccupied(free_idx);
  SetReadable(free_idx);
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::Remove(KeyType key, ValueType value, KeyComparator cmp) {
  for (size_t i = 0; i < BLOCK_ARRAY_SIZE; i++) {
    if (!IsOccupied(i)) {
      break;
    }
    if (IsReadable(i) && cmp(key, array_[i].first) == 0 && array_[i].second == value) {
      RemoveAt(i);
      return true;
    }
  }
  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
KeyType HASH_TABLE_BUCKET_TYPE::KeyAt(uint32_t bucket_idx) const {
  return array_[bucket_idx].first;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
ValueType HASH_TABLE_BUCKET_TYPE::ValueAt(uint32_t bucket_idx) const {
  return array_[bucket_idx].second;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::RemoveAt(uint32_t bucket_idx) {
  // 将其位置的readable设置为0即可
  readable_[bucket_idx / 8] &= ~(1 << (bucket_idx % 8));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsOccupied(uint32_t bucket_idx) const {
  // 使用位运算，判断对应位是否为1
  return (occupied_[bucket_idx / 8] & (1 << (bucket_idx % 8))) > 0;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetOccupied(uint32_t bucket_idx) {
  // 将occupied对应位设置为1
  occupied_[bucket_idx / 8] |= 1 << (bucket_idx % 8);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsReadable(uint32_t bucket_idx) const {
  return (readable_[bucket_idx / 8] & (1 << (bucket_idx % 8))) > 0;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetReadable(uint32_t bucket_idx) {
  readable_[bucket_idx / 8] |= 1 << (bucket_idx % 8);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsFull() {
  return NumReadable() == BUCKET_ARRAY_SIZE;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_BUCKET_TYPE::NumReadable() {
  uint32_t cnt = 0;
  for (size_t i = 0; i < BUCKET_ARRAY_SIZE; i++) {
    if (!IsOccupied(i)) {
      break;
    }
    if (IsReadable(i)) {
      cnt++;
    }
  }
  return cnt;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsEmpty() {
  return NumReadable() == 0;
}

// 复制备份bucket的元素，在分裂时调用
template <typename KeyType, typename ValueType, typename KeyComparator>
MappingType *HASH_TABLE_BUCKET_TYPE::GetArrayCopy() {
  uint32_t num = NumReadable();
  MappingType *copy = new MappingType[num];
  for (size_t i = 0, idx = 0; i < BUCKET_ARRAY_SIZE; i++) {
    if (!IsOccupied(i)) {
      break;
    }
    if (IsReadable(i)) {
      copy[idx++] = array_[i];
    }
  }
  return copy;
}

// 清空当前bucket，在分裂时调用
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::Clear() {
  memset(occupied_, 0, sizeof(occupied_));
  memset(readable_, 0, sizeof(readable_));
  memset(array_, 0, sizeof(array_));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::PrintBucket() {
  uint32_t size = 0;
  uint32_t taken = 0;
  uint32_t free = 0;
  for (size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++) {
    if (!IsOccupied(bucket_idx)) {
      break;
    }

    size++;

    if (IsReadable(bucket_idx)) {
      taken++;
    } else {
      free++;
    }
  }

  LOG_INFO("Bucket Capacity: %lu, Size: %u, Taken: %u, Free: %u", BUCKET_ARRAY_SIZE, size, taken, free);
}

// DO NOT REMOVE ANYTHING BELOW THIS LINE
template class HashTableBucketPage<int, int, IntComparator>;

template class HashTableBucketPage<GenericKey<4>, RID, GenericComparator<4>>;
template class HashTableBucketPage<GenericKey<8>, RID, GenericComparator<8>>;
template class HashTableBucketPage<GenericKey<16>, RID, GenericComparator<16>>;
template class HashTableBucketPage<GenericKey<32>, RID, GenericComparator<32>>;
template class HashTableBucketPage<GenericKey<64>, RID, GenericComparator<64>>;

// template class HashTableBucketPage<hash_t, TmpTuple, HashComparator>;

}  // namespace bustub
