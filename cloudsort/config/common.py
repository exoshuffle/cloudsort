# pylint: disable=too-many-instance-attributes
import enum
import math
import os
from dataclasses import InitVar, dataclass, field
from typing import Dict, List, Optional

import ray

from cloudsort.typing import AppStep, InstanceLifetime, SpillingMode

AZURE_CONTAINER = os.getenv("AZURE_CONTAINER")
S3_BUCKET = os.getenv("S3_BUCKET")

CONFIG_NAME_ENV_VAR = "CONFIG"
APP_STEPS_ENV_VAR = "STEPS"

KiB = 1024
MiB = KiB * 1024
GiB = MiB * 1024
KB = 1000
MB = KB * 1000
GB = MB * 1000


def get_s3_buckets(count: int = 10) -> List[str]:
    assert S3_BUCKET
    return [f"{S3_BUCKET}-{i:03d}" for i in range(count)]


class Cloud(enum.Enum):
    AWS = "aws"
    AZURE = "azure"


@dataclass
class InstanceType:
    name: str
    cpu: int
    memory_gib: float
    cloud: Cloud = Cloud.AWS
    memory_bytes: int = field(init=False)
    disk_count: int = 0
    disk_device_offset: int = 1
    hdd: bool = False

    def __post_init__(self):
        self.memory_bytes = int(self.memory_gib * GiB)


@dataclass
class ClusterConfig:
    instance_count: int
    instance_type: InstanceType
    instance_lifetime: InstanceLifetime = InstanceLifetime.DEDICATED
    instance_disk_gb: int = 40
    ebs: bool = False
    local: bool = False
    name: str = ""

    def __post_init__(self):
        if self.ebs:
            self.instance_type.disk_count += 1
        if self.name == "":
            self.name = f"{self.instance_type.name}-{self.instance_count}"
            if self.instance_type.cloud == Cloud.AZURE:
                self.name = self.name.replace("_", "-")


@dataclass
class SystemConfig:
    _cluster: InitVar[ClusterConfig]
    max_fused_object_count: int = 2000
    object_spilling_threshold: float = 0.8
    # How much system memory to allocate for the object store.
    object_store_memory_percent: float = 0.6
    object_store_memory_bytes: int = field(init=False)
    # How much larger should /dev/shm be compared to the object store.
    shared_memory_multiplier: float = 1.001
    shared_memory_bytes: int = field(init=False)
    ray_storage: Optional[str] = f"s3://{S3_BUCKET}" if S3_BUCKET else None
    s3_spill: int = 0

    def __post_init__(self, cluster: ClusterConfig):
        self.object_store_memory_bytes = int(
            cluster.instance_type.memory_bytes * self.object_store_memory_percent
        )
        self.shared_memory_bytes = int(
            self.object_store_memory_bytes * self.shared_memory_multiplier
        )


@dataclass
class AppConfig:
    _cluster: InitVar[ClusterConfig]

    total_gb: InitVar[float]
    input_part_gb: InitVar[float]
    output_part_gb: InitVar[Optional[float]] = None
    total_data_size: int = field(init=False)
    input_part_size: int = field(init=False)
    output_part_size: int = field(init=False)

    num_workers: int = field(init=False)
    num_mappers: int = field(init=False)
    num_shards_per_mapper: int = 1
    num_shards: int = field(init=False)
    input_shard_size: int = field(init=False)
    num_mappers_per_worker: int = field(init=False)
    num_mergers_per_worker: int = field(init=False)
    num_reducers: int = field(init=False)
    num_reducers_per_worker: int = field(init=False)

    num_concurrent_rounds: int = 2
    merge_factor: int = 2
    map_parallelism_multiplier: InitVar[float] = 0.5
    reduce_parallelism_multiplier: InitVar[float] = 0.5
    map_parallelism: int = field(init=False)
    merge_parallelism: int = field(init=False)
    reduce_parallelism: int = field(init=False)

    io_size: int = 256 * KiB
    map_io_parallelism: int = 10
    reduce_io_parallelism: int = 20
    merge_io_parallelism: int = field(init=False)

    shuffle_wait_percentile: float = 0.75
    shuffle_wait_timeout: float = 5.0

    skip_sorting: bool = False
    skip_input: bool = False
    skip_output: bool = False
    skip_first_stage: bool = False
    skip_final_reduce: bool = False

    spilling: SpillingMode = SpillingMode.RAY

    dataloader_mode: str = ""

    record_object_refs: bool = False

    native_scheduling: bool = False
    use_put: bool = False
    use_yield: bool = False

    simple_shuffle: bool = False
    riffle: bool = False
    magnet: bool = False

    s3_buckets: List[str] = field(default_factory=list)
    azure_containers: str = ""
    cloud_storage: bool = field(init=False)

    fail_node: Optional[str] = None
    fail_time: int = 45

    generate_input: bool = False
    sort: bool = False
    validate_output: bool = False

    # Runtime Context
    worker_ips: List[str] = field(default_factory=list)
    worker_ids: List[ray.NodeID] = field(default_factory=list)
    worker_ip_to_id: Dict[str, ray.NodeID] = field(default_factory=dict)
    data_dirs: List[str] = field(default_factory=list)
    is_local_cluster: bool = False

    def __post_init__(
        self,
        cluster: ClusterConfig,
        total_gb: float,
        input_part_gb: float,
        output_part_gb: Optional[float],
        map_parallelism_multiplier: float,
        reduce_parallelism_multiplier: float,
    ):
        self.is_local_cluster = cluster.local
        self.total_data_size = int(total_gb * GB)
        self.input_part_size = int(input_part_gb * GB)
        self.output_part_size = int((output_part_gb or input_part_gb) * GB)
        self.map_parallelism = int(
            map_parallelism_multiplier * cluster.instance_type.cpu
        )
        self.reduce_parallelism = int(
            reduce_parallelism_multiplier * cluster.instance_type.cpu
        )

        self.num_workers = cluster.instance_count
        self.num_mappers = int(math.ceil(self.total_data_size / self.input_part_size))
        self.num_shards = self.num_mappers * self.num_shards_per_mapper
        self.input_shard_size = self.input_part_size // self.num_shards_per_mapper

        self.num_mappers_per_worker = int(
            math.ceil(self.num_mappers / self.num_workers)
        )
        if self.riffle:
            assert self.merge_factor % self.map_parallelism == 0, (
                self.merge_factor,
                self.map_parallelism,
            )
            self.merge_parallelism = 1
        else:
            assert self.map_parallelism % self.merge_factor == 0, (
                self.map_parallelism,
                self.merge_factor,
            )
            self.merge_parallelism = self.map_parallelism // self.merge_factor
            self.merge_parallelism = self.map_parallelism // self.merge_factor
        if self.skip_first_stage:
            self.skip_input = True
        self.num_rounds = int(
            math.ceil(self.num_mappers / self.num_workers / self.map_parallelism)
        )
        self.num_mergers_per_worker = self.num_rounds * self.merge_parallelism
        self.merge_io_parallelism = self.map_io_parallelism

        self.num_reducers = int(math.ceil(self.total_data_size / self.output_part_size))
        self.num_reducers_per_worker = int(
            math.ceil(self.num_reducers / self.num_workers)
        )

        self.cloud_storage = bool(self.s3_buckets or self.azure_containers)


@dataclass
class JobConfig:
    name: str
    cluster: ClusterConfig
    system: SystemConfig
    app: AppConfig

    def __init__(self, name: str, cluster: Dict, system: Dict, app: Dict):
        self.name = name
        self.cluster = ClusterConfig(**cluster)
        self.system = SystemConfig(**system, _cluster=self.cluster)
        self.app = AppConfig(**app, _cluster=self.cluster)


def get_steps(steps: Optional[List[AppStep]] = None) -> Dict:
    """
    Return a dictionary of steps to run for AppConfig.
    """
    if not steps:
        steps_str = os.getenv(APP_STEPS_ENV_VAR)
        if steps_str:
            steps = [AppStep(step) for step in steps_str.split(",")]
        if not steps:
            steps = [AppStep.GENERATE_INPUT, AppStep.SORT, AppStep.VALIDATE_OUTPUT]
    return {step.value: True for step in steps}
