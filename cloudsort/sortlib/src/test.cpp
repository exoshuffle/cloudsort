#include <cassert>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <vector>

#include "csortlib.h"

using namespace csortlib;

void PrintRecord(const Record &rec) {
  for (size_t i = 0; i < HEADER_SIZE; ++i) {
    printf("%02x ", rec.header[i]);
  }
  printf("\n");
}

void AssertSorted(const Array<Record> &array) {
  for (size_t i = 0; i < array.size - 1; ++i) {
    const auto &a = array.ptr[i];
    const auto &b = array.ptr[i + 1];
    assert(std::memcmp(a.header, b.header, HEADER_SIZE) <= 0);
  }
}

std::vector<ConstArray<Record>>
MakeConstRecordArrays(Record *const records,
                      const std::vector<Partition> &parts) {
  std::vector<ConstArray<Record>> ret;
  ret.reserve(parts.size());
  for (const auto &part : parts) {
    ret.emplace_back(ConstArray<Record>{records + part.offset, part.size});
  }
  return ret;
}

void test_file_merger() {
  FileMerger merger(
      {"/mnt/data0/tmp/temp/temp-00000000", "/mnt/data0/tmp/temp/temp-00000001",
       "/mnt/data0/tmp/temp/temp-00000002",
       "/mnt/data0/tmp/temp/temp-00000003"},
      "/mnt/data0/tmp/temp/test-output", 100 * 1024 * 1024, 1 * 1024 * 1024);
  merger.Run();
}

int main() {
  printf("Hello, world!\n");
#ifdef CSORTLIB_USE_ALT_MEMORY_LAYOUT
  printf("Using alt memory layout.\n");
#endif

  // test_file_merger();
  // exit(0);

  const size_t num_reducers = 1000;
  const auto &boundaries = GetBoundaries(num_reducers);

  const size_t num_records = 1000 * 1000 * 20;
  Record *records = new Record[num_records];

  FILE *fin;
  size_t file_size = 0;
  fin = fopen("/tmp/part-0000", "r");
  if (fin == NULL) {
    perror("Failed to open file");
  } else {
    file_size = fread(records, RECORD_SIZE, num_records, fin);
    printf("Read %lu bytes.\n", file_size);
    fclose(fin);
  }

  const int num_trials = 3;
  for (int i = 0; i < num_trials; i++) {
    Record *records_copy = new Record[num_records];
    std::memcpy(records_copy, records, num_records * RECORD_SIZE);

    const auto start1 = std::chrono::high_resolution_clock::now();
    SortAndPartition({records_copy, num_records}, boundaries);
    const auto stop1 = std::chrono::high_resolution_clock::now();
    printf("SortAndPartition,%ld\n\n",
           std::chrono::duration_cast<std::chrono::milliseconds>(stop1 - start1)
               .count());
  }
  // const auto& record_arrays = MakeConstRecordArrays(records, parts);
  // const auto start2 = std::chrono::high_resolution_clock::now();
  // const auto output = MergePartitions(record_arrays);
  // const auto stop2 = std::chrono::high_resolution_clock::now();

  // FILE* fout;
  // fout = fopen("data1g-output", "w");
  // if (fout == NULL) {
  //     perror("Failed to open file");
  // } else {
  //     size_t writecount = fwrite(output.ptr, RECORD_SIZE, output.size, fout);
  //     printf("Wrote %lu bytes.\n", writecount);
  //     fclose(fout);
  // }
  // printf("Execution time (ms):\n");
  // printf("SortAndPartition,%ld\n",
  //        std::chrono::duration_cast<std::chrono::milliseconds>(stop1 -
  //        start1)
  //            .count());
  // printf("MergePartitions,%ld\n",
  //        std::chrono::duration_cast<std::chrono::milliseconds>(stop2 -
  //        start2)
  //            .count());
  // printf("Total,%ld\n",
  //        std::chrono::duration_cast<std::chrono::milliseconds>(stop2 -
  //        start1)
  //            .count());

  return 0;
}
