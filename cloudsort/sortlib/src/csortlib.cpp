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

#ifdef CSORTLIB_USE_ALT_MEMORY_LAYOUT

struct SortItem {
  // 10 bytes.
  uint8_t header[HEADER_SIZE];
  // 2 bytes for alignment.
  // uint8_t _unused[2];
  // 4 bytes.
  uint32_t offset;
};

struct SortItemComparator {
  inline bool operator()(const SortItem &a, const SortItem &b) {
    return memcmp(a.header, b.header, HEADER_SIZE) < 0;
  }
};

std::unique_ptr<std::vector<SortItem>>
_MakeSortItems(const Array<Record> &record_array) {
  const Record *const records = record_array.ptr;
  const size_t num_records = record_array.size;
  auto ret = std::make_unique<std::vector<SortItem>>();
  ret->reserve(num_records);
  SortItem item = {};
  for (uint32_t &offset = item.offset; offset < num_records; ++offset) {
    memcpy(item.header, (records + offset)->header, HEADER_SIZE);
    ret->emplace_back(item);
  }
  return ret;
}

void _ApplyOrder(const Array<Record> &record_array,
                 const std::vector<SortItem> &sort_items) {
  Record *const records = record_array.ptr;
  const size_t num_records = record_array.size;
  std::vector<uint32_t> positions(num_records);
  for (size_t i = 0; i < num_records; ++i) {
    positions[sort_items[i].offset] = i;
  }
  size_t i = 0;
  while (i < num_records) {
    const uint32_t pos = positions[i];
    if (pos != i) {
      std::swap(records[i], records[pos]);
      std::swap(positions[i], positions[pos]);
    } else {
      ++i;
    }
  }
}

void _ApplyOrderByCopying(const Array<Record> &record_array,
                          const std::vector<SortItem> &sort_items) {
  Record *const records = record_array.ptr;
  const size_t num_records = record_array.size;
  Record *buffer = new Record[num_records];
  Record *cur = buffer;
  for (const auto &item : sort_items) {
    memcpy(cur++, records + item.offset, sizeof(Record));
  }
  memcpy(records, buffer, sizeof(Record) * num_records);
  delete[] buffer;
}

#endif

std::vector<Partition> SortAndPartition(const Array<Record> &record_array,
                                        const std::vector<Key> &boundaries) {
  Record *const records = record_array.ptr;
  const size_t num_records = record_array.size;
#ifdef CSORTLIB_TIMEIT
  const auto start_time = std::chrono::high_resolution_clock::now();
#endif
#ifdef CSORTLIB_USE_ALT_MEMORY_LAYOUT
  auto sort_items = _MakeSortItems(record_array);
#ifdef CSORTLIB_TIMEIT
  const auto make_sort_items_stop = std::chrono::high_resolution_clock::now();
  printf("MakeSortItems,%ld\n",
         std::chrono::duration_cast<std::chrono::milliseconds>(
             make_sort_items_stop - start_time)
             .count());
#endif

  std::sort(sort_items->begin(), sort_items->end(),
            HeaderComparator<SortItem>());
#ifdef CSORTLIB_TIMEIT
  const auto partial_sort_stop = std::chrono::high_resolution_clock::now();
  printf("Sort,%ld\n", std::chrono::duration_cast<std::chrono::milliseconds>(
                           partial_sort_stop - make_sort_items_stop)
                           .count());
#endif
  _ApplyOrder(record_array, *sort_items);
#ifdef CSORTLIB_TIMEIT
  const auto sort_stop = std::chrono::high_resolution_clock::now();
  printf("ApplyOrder,%ld\n",
         std::chrono::duration_cast<std::chrono::milliseconds>(
             sort_stop - partial_sort_stop)
             .count());
#endif
#else
  std::sort(records, records + num_records, HeaderComparator<Record>());
#ifdef CSORTLIB_TIMEIT
  const auto sort_stop = std::chrono::high_resolution_clock::now();
  printf("Sort,%ld\n", std::chrono::duration_cast<std::chrono::milliseconds>(
                           sort_stop - start_time)
                           .count());
#endif
#endif

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
#ifdef CSORTLIB_TIMEIT
  const auto partition_stop = std::chrono::high_resolution_clock::now();
  printf("Partition,%ld\n",
         std::chrono::duration_cast<std::chrono::milliseconds>(partition_stop -
                                                               sort_stop)
             .count());
#endif
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

void _ReadPartition(const std::string &filename, size_t offset, char *buffer,
                    size_t buffer_size, ConstArray<Record> *ret) {
  printf("Read %s offset=%lu\n", filename.c_str(), offset);
  std::ifstream fin(filename, std::ios::in | std::ios::binary);
  fin.seekg(offset); // TODO: what bits does this set, if any?
  fin.read(buffer, buffer_size);
  const auto bytes_read = fin.gcount();
  assert(bytes_read % RECORD_SIZE == 0);
  ret->ptr = (Record *)buffer;
  ret->size = bytes_read / RECORD_SIZE;
}

FileMerger::FileMerger(const std::vector<std::string> &input_files,
                       const std::string &output_file, size_t input_batch_bytes,
                       size_t output_batch_records)
    : input_files_(input_files), output_file_(output_file),
      input_batch_bytes_(input_batch_bytes),
      output_batch_records_(output_batch_records),
      num_inputs_(input_files.size()) {}

size_t FileMerger::Run() {
  // Allocate buffers.
  std::vector<std::unique_ptr<char[]>> input_buffers;
  std::vector<ConstArray<Record>> input_record_arrays;
  input_buffers.reserve(num_inputs_);
  input_record_arrays.reserve(num_inputs_);
  for (size_t i = 0; i < num_inputs_; ++i) {
    input_buffers.emplace_back(std::make_unique<char[]>(input_batch_bytes_));
    input_record_arrays.emplace_back(ConstArray<Record>());
  }
  auto output_buffer =
      std::make_unique<char[]>(output_batch_records_ * RECORD_SIZE);

  // Load initial partitions.
  std::vector<std::thread> input_threads;
  input_threads.reserve(num_inputs_);
  for (size_t i = 0; i < num_inputs_; ++i) {
    input_threads.emplace_back(
        std::thread(_ReadPartition, input_files_[i], 0, input_buffers[i].get(),
                    input_batch_bytes_, &input_record_arrays[i]));
  }
  for (auto &thread : input_threads) {
    thread.join();
  }

  // initialize merger with these partitions
  // keep track of file offsets
  // launch file write thread
  // Merger merger;
  return 0;
}

} // namespace csortlib
