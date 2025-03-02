# distutils: language = c++
# distutils: sources = src/csortlib.cpp

from libc.stdint cimport uint8_t, uint64_t
from libcpp cimport bool
from libcpp.pair cimport pair
from libcpp.vector cimport vector

import io
from typing import Callable, Iterable, List, Tuple, Union

import numpy as np


cdef extern from "src/csortlib.h" namespace "csortlib":
    const size_t HEADER_SIZE
    const size_t RECORD_SIZE
    ctypedef uint64_t Key
    ctypedef struct Record:
        uint8_t header[HEADER_SIZE]
        uint8_t body[RECORD_SIZE - HEADER_SIZE]
    ctypedef struct Partition:
        size_t offset
        size_t size
    cdef cppclass Array[T]:
        T* ptr
        size_t size
    cdef cppclass ConstArray[T]:
        const T* ptr
        size_t size
    cdef vector[Key] GetBoundaries(size_t num_partitions) nogil
    cdef vector[Partition] SortAndPartition(const Array[Record]& record_array, const vector[Key]& boundaries) nogil
    cdef cppclass Merger:
        Merger(const vector[ConstArray[Record]]& parts, bool ask_for_refills, const vector[Key]& boundaries) nogil
        pair[size_t, int] GetBatch(Record* const& ptr, size_t max_num_records) nogil
        void Refill(const ConstArray[Record]& part, int part_id) nogil


KeyT = np.uint64
HeaderT = np.dtype((np.uint8, HEADER_SIZE))
PayloadT = np.dtype((np.uint8, RECORD_SIZE - HEADER_SIZE))
RecordT = np.dtype([("header", HeaderT), ("body", PayloadT)])
BlockInfo = Tuple[int, int]


def get_boundaries(n: int) -> List[int]:
    return GetBoundaries(n)


cdef Array[Record] _to_record_array(buf):
    cdef Array[Record] ret
    cdef uint8_t[:] mv = buf
    ret.ptr = <Record *>&mv[0]
    ret.size = int(len(buf) / RECORD_SIZE)
    return ret


cdef ConstArray[Record] _to_const_record_array(buf):
    cdef ConstArray[Record] ret
    if buf is None:
        ret.ptr = NULL
        ret.size = 0
        return ret
    cdef const uint8_t[:] mv = buf
    ret.ptr = <const Record*>&mv[0]
    ret.size = int(len(buf) / RECORD_SIZE)
    return ret


def sort_and_partition(part: np.ndarray, boundaries: List[int]) -> List[BlockInfo]:
    arr = _to_record_array(part)
    blocks = SortAndPartition(arr, boundaries)
    return [(c.offset * RECORD_SIZE, c.size * RECORD_SIZE) for c in blocks]


def merge_partitions(
    num_blocks: int,
    get_block: Callable[[int, int], np.ndarray],
    batch_num_records: int = 1_000_000,
    ask_for_refills: bool = False,
    boundaries: List[int] = [],
) -> Iterable[np.ndarray]:
    """
    An iterator that returns merged blocks for upload.
    """
    # The blocks array is necessary for Python reference counting.
    blocks = [get_block(i, 0) for i in range(num_blocks)]
    block_indexes = np.zeros(num_blocks, dtype=int)

    cdef vector[ConstArray[Record]] record_arrays
    record_arrays.reserve(num_blocks)
    for block in blocks:
        record_arrays.push_back(_to_const_record_array(block))

    merger = new Merger(record_arrays, ask_for_refills, boundaries)
    buffer = np.empty(batch_num_records * RECORD_SIZE, dtype=np.uint8)
    cdef uint8_t[:] mv = buffer
    cdef Record* ptr = <Record*>&mv[0]
    cdef size_t max_num_records = batch_num_records
    while True:
        with nogil:
            ret = merger.GetBatch(ptr, max_num_records)
        cnt, part_id = ret
        if cnt == 0:
            return
        actual_bytes = cnt * RECORD_SIZE
        yield buffer[:actual_bytes]
        if part_id != -1:
            block_indexes[part_id] += 1
            block = get_block(part_id, block_indexes[part_id])
            if block is None or block.size == 0:
                continue
            blocks[part_id] = block
            merger.Refill(_to_const_record_array(block), part_id)
