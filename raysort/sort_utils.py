import csv
import logging
import os
import subprocess
import tempfile
from typing import Iterable, Optional

import numpy as np
import ray

from raysort import (
    constants,
    logging_utils,
    ray_utils,
    s3_utils,
    tracing_utils,
)
from raysort.config import AppConfig
from raysort.typing import PartId, PartInfo, Path, RecordCount

# ------------------------------------------------------------
#     Loading and Saving Partitions
# ------------------------------------------------------------


def get_manifest_file(_cfg: AppConfig, kind: str = "input") -> Path:
    return constants.MANIFEST_FMT.format(kind=kind, suffix="cloud")


def load_manifest(cfg: AppConfig, kind: str = "input") -> list[PartInfo]:
    with open(get_manifest_file(cfg, kind=kind)) as fin:
        reader = csv.reader(fin)
        return [PartInfo.from_csv_row(row) for row in reader]


def load_partitions(cfg: AppConfig, pinfolist: list[PartInfo]) -> np.ndarray:
    return s3_utils.download(
        pinfolist[0],
        size=cfg.input_part_size,
        max_concurrency=cfg.map_io_parallelism,
    )


def save_partition(
    cfg: AppConfig, pinfo: PartInfo, merger: Iterable[np.ndarray]
) -> list[PartInfo]:
    return s3_utils.multipart_upload(cfg, pinfo, merger)


@ray.remote
def make_data_dirs(_cfg: AppConfig):
    os.makedirs(constants.TMPFS_PATH, exist_ok=True)


def init(cfg: AppConfig):
    ray.get(ray_utils.run_on_all_workers(cfg, make_data_dirs, include_current=True))


def part_info(
    cfg: AppConfig,
    part_id: PartId,
    *,
    kind: str = "input",
) -> PartInfo:
    shard = hash(str(part_id)) & constants.S3_SHARD_MASK
    path = _get_part_path(part_id, shard=shard, kind=kind)
    bucket = cfg.s3_buckets[shard % len(cfg.s3_buckets)]
    return PartInfo(part_id, None, bucket, path, 0, None)


def _get_part_path(
    part_id: PartId,
    *,
    prefix: Path = "",
    shard: Optional[int] = None,
    kind: str = "input",
) -> Path:
    filename_fmt = constants.FILENAME_FMT[kind]
    filename = filename_fmt.format(part_id=part_id)
    parts = [prefix]
    if shard is not None:
        parts.append(constants.SHARD_FMT.format(shard=shard))
    parts.append(filename)
    return os.path.join(*parts)


def _run_gensort(offset: int, size: int, path: str, buf: bool = False) -> str:
    # Add `,buf` to use buffered I/O instead of direct I/O (for tmpfs).
    if buf:
        path += ",buf"
    proc = subprocess.run(
        f"{constants.GENSORT_PATH} -c -b{offset} {size} {path}",
        shell=True,
        check=True,
        stderr=subprocess.PIPE,
        text=True,
    )
    return proc.stderr.strip()


@ray.remote
def generate_part(
    cfg: AppConfig,
    part_id: PartId,
    size: RecordCount,
    offset: RecordCount,
) -> PartInfo:
    with tracing_utils.timeit("generate_part"):
        logging_utils.init()
        pinfo = part_info(cfg, part_id)
        path = os.path.join(constants.TMPFS_PATH, f"{part_id:010x}")
        pinfo.size = size * constants.RECORD_SIZE
        pinfo.checksum = _run_gensort(offset, size, path, cfg.cloud_storage)
        s3_utils.upload(
            path,
            pinfo,
            max_concurrency=cfg.map_io_parallelism,
        )
        logging.info("Generated input %s", pinfo)
        return pinfo


def generate_input(cfg: AppConfig):
    total_size = constants.bytes_to_records(cfg.total_data_size)
    size = constants.bytes_to_records(cfg.input_shard_size)
    offset = 0
    tasks = []
    for m in range(cfg.num_mappers_per_worker):
        for w in range(cfg.num_workers):
            for i in range(cfg.num_shards_per_mapper):
                if offset >= total_size:
                    break
                part_id = constants.merge_part_ids(w, m, i)
                tasks.append(
                    generate_part.options(**ray_utils.node_i(cfg, w)).remote(
                        cfg, part_id, min(size, total_size - offset), offset
                    )
                )
                offset += size
    logging.info("Generating %d partitions", len(tasks))
    parts = ray.get(tasks)
    with open(get_manifest_file(cfg), "w") as fout:
        writer = csv.writer(fout)
        for pinfo in parts:
            writer.writerow(pinfo.to_csv_row())


def _run_valsort(argstr: str) -> str:
    proc = subprocess.run(
        f"{constants.VALSORT_PATH} {argstr}",
        check=True,
        shell=True,
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        logging.critical("\n%s", proc.stderr.decode("ascii"))
        raise RuntimeError(f"Validation failed: {argstr}")
    return proc.stderr


def _validate_part_impl(pinfo: PartInfo, path: Path, buf: bool = False) -> bytes:
    filesize = os.path.getsize(path)
    assert filesize == pinfo.size, (pinfo, filesize)
    sum_path = path + ".sum"
    argstr = f"-o {sum_path} {path}"
    if buf:
        argstr += ",buf"
    _run_valsort(argstr)
    with open(sum_path, "rb") as fin:
        return fin.read()


@ray.remote
def validate_part(cfg: AppConfig, pinfo: PartInfo) -> bytes:
    logging_utils.init()
    with tracing_utils.timeit("validate_part"):
        tmp_path = os.path.join(constants.TMPFS_PATH, os.path.basename(pinfo.path))
        s3_utils.download(
            pinfo,
            filename=tmp_path,
            max_concurrency=cfg.reduce_io_parallelism,
        )
        ret = _validate_part_impl(pinfo, tmp_path, buf=True)
        os.remove(tmp_path)
        logging.info("Validated output %s", pinfo)
        return ret


def compare_checksums(input_checksums: list[int], output_summary: str) -> None:
    assert "Checksum: " in output_summary, output_summary
    checksum_line = output_summary.split("Checksum: ")[1]
    output_checksum = checksum_line.split()[0]
    input_checksum = sum(input_checksums)
    input_checksum = str(hex(input_checksum))[2:]
    input_checksum = input_checksum[-len(output_checksum) :]
    assert (
        input_checksum == output_checksum
    ), f"Mismatched checksums: {input_checksum} {output_checksum} ||| {str(hex(sum(input_checksums)))} ||| {output_summary}"


def validate_output(cfg: AppConfig):
    if cfg.skip_sorting or cfg.skip_output:
        return
    parts = load_manifest(cfg, kind="output")
    total_bytes = sum(p.size for p in parts)
    assert total_bytes == cfg.total_data_size, total_bytes - cfg.total_data_size

    results = []
    for pinfo in parts:
        opt = {
            "resources": {constants.WORKER_RESOURCE: 1e-3},
            "num_cpus": int(np.ceil(cfg.output_part_size / 2_000_000_000)),
        }
        results.append(validate_part.options(**opt).remote(cfg, pinfo))
    logging.info("Validating %d partitions", len(results))
    results = ray.get(results)

    all_checksum = b"".join(results)
    with open(get_manifest_file(cfg), "r") as fin:
        reader = csv.reader(fin)
        input_checksums = [int(row[-1], 16) for row in reader]

    with tempfile.NamedTemporaryFile() as fout:
        fout.write(all_checksum)
        fout.flush()
        output_summary = _run_valsort(f"-s {fout.name}")
        compare_checksums(input_checksums, output_summary)

    logging.info("All OK!")
