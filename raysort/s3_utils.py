import io
import os
import queue
import threading
from typing import Iterable, List, Optional

import boto3
import botocore
import numpy as np
from boto3.s3 import transfer

from raysort import constants, s3_custom
from raysort.config import AppConfig
from raysort.typing import PartInfo, Path

MULTIPART_CHUNKSIZE = 16 * 1024 * 1024


def client() -> botocore.client.BaseClient:
    return boto3.session.Session().client(
        "s3",
        config=botocore.config.Config(
            max_pool_connections=100,
            parameter_validation=False,
            retries={
                "max_attempts": 10,
                "mode": "adaptive",
            },
        ),
    )


def get_transfer_config(**kwargs) -> transfer.TransferConfig:
    if "multipart_chunksize" not in kwargs:
        kwargs["multipart_chunksize"] = MULTIPART_CHUNKSIZE
    return transfer.TransferConfig(**kwargs)


def upload(src: Path, pinfo: PartInfo, *, delete_src: bool = True, **kwargs) -> None:
    config = get_transfer_config(**kwargs)
    try:
        client().upload_file(src, pinfo.bucket, pinfo.path, Config=config)
    finally:
        if delete_src:
            os.remove(src)


def download(
    pinfo: PartInfo,
    filename: Optional[Path] = None,
    buf: Optional[io.BytesIO] = None,
    size: Optional[int] = None,
    **kwargs,
) -> np.ndarray:
    config = get_transfer_config(**kwargs)
    if filename:
        client().download_file(pinfo.bucket, pinfo.path, filename, Config=config)
        return np.empty(0, dtype=np.uint8)
    if buf is None:
        buf = io.BytesIO()
    if size:
        s3_custom.download_fileobj(
            client(), pinfo.bucket, pinfo.path, buf, size, Config=config
        )
    else:
        client().download_fileobj(pinfo.bucket, pinfo.path, buf, Config=config)
    return np.frombuffer(buf.getbuffer(), dtype=np.uint8)


def multipart_upload(
    cfg: AppConfig, pinfo: PartInfo, merger: Iterable[np.ndarray]
) -> List[PartInfo]:
    parallelism = cfg.reduce_io_parallelism
    s3_client = client()
    mpu = s3_client.create_multipart_upload(Bucket=pinfo.bucket, Key=pinfo.path)
    upload_threads = []
    mpu_queue = queue.PriorityQueue()
    mpu_part_id = 1
    bytes_count = 0

    def _upload(**kwargs):
        resp = s3_client.upload_part(**kwargs)
        mpu_queue.put((kwargs["PartNumber"], resp))

    def _upload_part(data):
        nonlocal mpu_part_id, bytes_count
        if len(upload_threads) >= parallelism > 0:
            upload_threads.pop(0).join()
        kwargs = dict(
            Body=data,
            Bucket=pinfo.bucket,
            Key=pinfo.path,
            PartNumber=mpu_part_id,
            UploadId=mpu["UploadId"],
        )
        if parallelism > 0:
            thd = threading.Thread(target=_upload, kwargs=kwargs)
            thd.start()
            upload_threads.append(thd)
        else:
            _upload(**kwargs)
        mpu_part_id += 1
        bytes_count += len(data)

    # The merger produces a bunch of small chunks towards the end, which
    # we need to fuse into one chunk before uploading to S3.
    tail = io.BytesIO()
    for datachunk in merger:
        if datachunk.size >= constants.S3_MIN_CHUNK_SIZE:
            # There should never be large chunks once we start seeing
            # small chunks towards the end.
            assert tail.getbuffer().nbytes == 0
            _upload_part(datachunk.tobytes())  # copying is necessary
        else:
            tail.write(datachunk)

    if tail.getbuffer().nbytes > 0:
        _upload_part(tail.getvalue())

    # Wait for all upload tasks to complete.
    for thd in upload_threads:
        thd.join()

    mpu_parts = []
    while not mpu_queue.empty():
        mpu_id, resp = mpu_queue.get()
        mpu_parts.append({"ETag": resp["ETag"], "PartNumber": mpu_id})

    s3_client.complete_multipart_upload(
        Bucket=pinfo.bucket,
        Key=pinfo.path,
        MultipartUpload={"Parts": mpu_parts},
        UploadId=mpu["UploadId"],
    )
    pinfo.size = bytes_count
    return [pinfo]
