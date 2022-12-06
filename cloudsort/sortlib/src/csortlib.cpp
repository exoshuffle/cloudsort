#include "csortlib.h"

#include <algorithm>
#include <array>
#include <cassert>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <iostream>
#include <limits>
#include <memory>
#include <queue>
#include <thread>
#include <vector>

namespace csortlib {

template <typename T> size_t _TotalSize(const std::vector<T> &parts) {
  size_t ret = 0;
  for (const auto &part : parts) {
    ret += part.size;
  }
  return ret;
}

std::vector<Partition> SortAndPartition(const Array<Record> &record_array,
                                        const std::vector<Key> &boundaries) {
  Record *const records = record_array.ptr;
  const size_t num_records = record_array.size;
  std::sort(records, records + num_records, HeaderComparator<Record>());

  std::vector<Partition> ret;
  ret.reserve(boundaries.size());
  auto bound_it = boundaries.begin();
  size_t off = 0;
  size_t prev_off = 0;
  while (off < num_records && bound_it != boundaries.end()) {
    const Key bound = *bound_it;
    while (off < num_records && records[off].key() < bound) {
      ++off;
    }
    const size_t size = off - prev_off;
    if (!ret.empty()) {
      ret.back().size = size;
    }
    ret.emplace_back(Partition{off, 0});
    ++bound_it;
    prev_off = off;
  }
  if (!ret.empty()) {
    ret.back().size = num_records - prev_off;
  }
  assert(ret.size() == boundaries.size());
  assert(_TotalSize(ret) == num_records);
  return ret;
}

std::vector<Key> GetBoundaries(size_t num_partitions) {
  std::vector<Key> ret;
  ret.reserve(num_partitions);
  const Key min = 0;
  const Key max = std::numeric_limits<Key>::max();
  const Key step = ceil(max / num_partitions);
  Key boundary = min;
  for (size_t i = 0; i < num_partitions; ++i) {
    ret.emplace_back(boundary);
    boundary += step;
  }
  return ret;
}

struct SortData {
  const Record *record;
  PartitionId part_id;
  size_t index;
};

struct SortDataComparator {
  inline bool operator()(const SortData &a, const SortData &b) {
    return memcmp(a.record->header, b.record->header, HEADER_SIZE) > 0;
  }
};

class Merger::Impl {
public:
  std::vector<ConstArray<Record>> parts_;
  std::priority_queue<SortData, std::vector<SortData>, SortDataComparator>
      heap_;
  bool ask_for_refills_;
  std::vector<Key> boundaries_;

  size_t bound_i_ = 0;
  bool past_last_bound_ = false;

  Impl(const std::vector<ConstArray<Record>> &parts, bool ask_for_refills,
       const std::vector<Key> &boundaries)
      : parts_(parts), ask_for_refills_(ask_for_refills),
        boundaries_(boundaries) {
    for (size_t i = 0; i < parts_.size(); ++i) {
      _PushFirstItem(parts_[i], i);
    }
    _IncBound();
  }

  void _IncBound() {
    bound_i_++;
    past_last_bound_ = bound_i_ >= boundaries_.size();
  }

  inline void _PushFirstItem(const ConstArray<Record> &part,
                             PartitionId part_id) {
    if (part.size > 0) {
      heap_.push({part.ptr, part_id, 0});
    }
  }

  void Refill(const ConstArray<Record> &part, PartitionId part_id) {
    assert(part_id < parts_.size());
    parts_[part_id] = part;
    _PushFirstItem(part, part_id);
  }

  GetBatchRetVal GetBatch(Record *const &ret, size_t max_num_records) {
    size_t cnt = 0;
    auto cur = ret;
    Key bound = past_last_bound_ ? 0 : boundaries_[bound_i_];
    while (!heap_.empty()) {
      if (cnt >= max_num_records) {
        return std::make_pair(cnt, -1);
      }
      const SortData top = heap_.top();
      if (!past_last_bound_ && top.record->key() >= bound) {
        _IncBound();
        return std::make_pair(cnt, -1);
      }
      heap_.pop();
      const PartitionId i = top.part_id;
      const size_t j = top.index;
      // Copy record to output array
      *cur++ = *top.record;
      ++cnt;
      if (j + 1 < parts_[i].size) {
        heap_.push({top.record + 1, i, j + 1});
      } else if (ask_for_refills_) {
        return std::make_pair(cnt, i);
      }
    }
    return std::make_pair(cnt, -1);
  }
}; // namespace csortlib

Merger::Merger(const std::vector<ConstArray<Record>> &parts,
               bool ask_for_refills, const std::vector<Key> &boundaries)
    : impl_(std::make_unique<Impl>(parts, ask_for_refills, boundaries)) {}

GetBatchRetVal Merger::GetBatch(Record *const &ret, size_t max_num_records) {
  return impl_->GetBatch(ret, max_num_records);
}

void Merger::Refill(const ConstArray<Record> &part, PartitionId part_id) {
  return impl_->Refill(part, part_id);
}

Array<Record> MergePartitions(const std::vector<ConstArray<Record>> &parts,
                              bool ask_for_refills,
                              const std::vector<Key> &boundaries) {
  const size_t num_records = _TotalSize(parts);
  if (num_records == 0) {
    return {nullptr, 0};
  }
  Record *const ret = new Record[num_records]; // need to manually delete
  Merger merger(parts, ask_for_refills, boundaries);
  merger.GetBatch(ret, num_records);
  return {ret, num_records};
}

} // namespace csortlib
