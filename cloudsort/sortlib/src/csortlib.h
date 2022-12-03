#ifndef __CSORTLIB_H__
#define __CSORTLIB_H__

// #define CSORTLIB_TIMEIT 1
// #define CSORTLIB_USE_ALT_MEMORY_LAYOUT 1

#include <cstring>
#include <memory>
#include <queue>
#include <string>
#include <vector>

namespace csortlib {

const size_t HEADER_SIZE = 10;
const size_t RECORD_SIZE = 100;

// We consider the first 8 bytes of the header as a 64-bit unsigned integer
// "key". The key is used to partition records.
typedef uint64_t Key;
const size_t KEY_SIZE = sizeof(Key);

struct Record {
  uint8_t header[HEADER_SIZE];
  uint8_t body[RECORD_SIZE - HEADER_SIZE];

  // Assuming current architecture is little endian.
  inline Key key() const { return __builtin_bswap64(*(Key *)header); }
};

template <typename T> struct HeaderComparator {
  inline bool operator()(const T &a, const T &b) {
    return memcmp(a.header, b.header, HEADER_SIZE) < 0;
  }
};

template <typename T> struct Array {
  T *ptr;
  size_t size;
};

template <typename T> struct ConstArray {
  const T *ptr;
  size_t size;
};

struct Partition {
  size_t offset;
  size_t size;
};

inline bool operator==(const Partition &a, const Partition &b) {
  return a.offset == b.offset && a.size == b.size;
}

// Sort the data in-place, then return a list of partitions. A partition
// is represented by an offset and a size. If the i-th partition is empty,
// then ret[i].offset == ret[i + 1].offset, and ret[i].size == 0.
// TODO: this seems to be not true when some range does not exist.
//
// Invariants:
// - ret[0].offset === 0
// - ret[i] < num_records for all i
//
// CPU cost: O(Pm * log(Pm))
// Memory cost: 0
// where Pm == len(records)
std::vector<Partition> SortAndPartition(const Array<Record> &record_array,
                                        const std::vector<Key> &boundaries);

// Compute the boundaries by partitioning the key space into partitions.
// Return the boundary integers.
// E.g. the keys (first 8 bytes) of all records in the i-th partition
// must be in the half-open interval [ boundaries[i], boundaries[i + 1] ).
// TODO: this will be more complicated for skewed distribution.
std::vector<Key> GetBoundaries(size_t num_partitions);

using PartitionId = int16_t;
using GetBatchRetVal = std::pair<size_t, PartitionId>;

// Responsible for merging M sorted partitions and producing the output
// in blocks.
//
// CPU cost: O(Pr * log(M))
// where Pr == sum(len(p) for p in partitions), M == len(partitions)
class Merger {
public:
  Merger(const std::vector<ConstArray<Record>> &partitions,
         bool ask_for_refills, const std::vector<Key> &boundaries);

  // Returns (count, part_id) where
  // - count is the actual number of records written into `ret`, and
  // - part_id is the partition that has been depleted (-1 if none).
  GetBatchRetVal GetBatch(Record *const &ret, size_t max_num_records);

  void Refill(const ConstArray<Record> &part, PartitionId part_id);

private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

// A functional version of Merger that handles memory allocation.
Array<Record> MergePartitions(const std::vector<ConstArray<Record>> &partitions,
                              bool ask_for_refills = false,
                              const std::vector<Key> &boundaries = {});

class FileMerger {
public:
  FileMerger(const std::vector<std::string> &input_files,
             const std::string &output_file, size_t input_batch_bytes,
             size_t output_batch_records);

  // Start merging and return the number of total bytes merged.
  size_t Run();

private:
  const std::vector<std::string> input_files_;
  const std::string output_file_;
  const size_t input_batch_bytes_;
  const size_t output_batch_records_;
  const size_t num_inputs_;
};

} // namespace csortlib

#endif
