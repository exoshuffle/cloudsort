import logging

import ray

from raysort import constants
from raysort.config import AppConfig, JobConfig

KiB = 1024
MiB = KiB * 1024
GiB = MiB * 1024


def run_on_all_workers(
    cfg: AppConfig,
    fn: ray.remote_function.RemoteFunction,
    include_current: bool = False,
) -> list[ray.ObjectRef]:
    opts = [node_aff(node) for node in cfg.worker_ids]
    if include_current:
        opts.append(current_node_aff())
    return [fn.options(**opt).remote(cfg) for opt in opts]


def current_node_aff() -> dict:
    return node_aff(ray.get_runtime_context().node_id)


def node_aff(node_id: ray.NodeID, *, soft: bool = False) -> dict:
    return {
        "scheduling_strategy": ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy(
            node_id=node_id,
            soft=soft,
        )
    }


def node_i(cfg: AppConfig, node_idx: int) -> dict:
    return node_aff(cfg.worker_ids[node_idx % cfg.num_workers])


@ray.remote
def get_node_id() -> ray.NodeID:
    return ray.get_runtime_context().node_id


def _init_runtime_context(cfg: AppConfig):
    resources = ray.cluster_resources()
    logging.info("Cluster resources: %s", resources)
    assert (
        constants.WORKER_RESOURCE in resources
    ), "Ray cluster is not set up correctly: no worker resources. Did you forget `--local`?"
    cfg.num_workers = int(resources[constants.WORKER_RESOURCE])
    head_node_str = "node:" + ray.util.get_node_ip_address()
    cfg.worker_ips = [
        r.split(":")[1]
        for r in resources
        if r.startswith("node:") and r != head_node_str
    ]
    assert cfg.num_workers == len(cfg.worker_ips), cfg
    cfg.worker_ids = ray.get(
        [
            get_node_id.options(resources={f"node:{node_ip}": 1e-3}).remote()
            for node_ip in cfg.worker_ips
        ]
    )
    cfg.worker_ip_to_id = dict(zip(cfg.worker_ips, cfg.worker_ids))


def init(job_cfg: JobConfig):
    ray.init(address="auto")
    _init_runtime_context(job_cfg.app)
