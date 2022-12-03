# pylint: disable=use-dict-literal
import os

from cloudsort.config.common import (
    InstanceType,
    JobConfig,
    SpillingMode,
    get_s3_buckets,
    get_steps,
)

local_cluster = dict(
    instance_count=min(os.cpu_count() or 16, 16),
    instance_type=InstanceType(
        name="local",
        cpu=2,
        memory_gib=0,  # not used
    ),
    local=True,
)

local_base_app_config = dict(
    **get_steps(),
    map_parallelism_multiplier=1,
    reduce_parallelism_multiplier=1,
)

local_mini_app_config = dict(
    **local_base_app_config,
    total_gb=0.16,
    input_part_gb=0.01,
)

local_app_config = dict(
    **local_base_app_config,
    total_gb=0.512,
    input_part_gb=0.002,
)

configs = [
    # ------------------------------------------------------------
    #     Local experiments
    # ------------------------------------------------------------
    JobConfig(
        name="LocalSimple",
        cluster=local_cluster,
        system=dict(),
        app=dict(
            **local_app_config,
            simple_shuffle=True,
        ),
    ),
    JobConfig(
        name="LocalManualSpillingDisk",
        cluster=local_cluster,
        system=dict(),
        app=dict(
            **local_app_config,
            spilling=SpillingMode.DISK,
        ),
    ),
    JobConfig(
        name="LocalManualSpillingDiskParallel",
        cluster=local_cluster,
        system=dict(),
        app=dict(
            **local_app_config,
            spilling=SpillingMode.DISK,
        ),
    ),
    JobConfig(
        name="LocalNative",
        cluster=local_cluster,
        system=dict(),
        app=dict(**local_app_config),
    ),
    JobConfig(
        name="LocalNativePut",
        cluster=local_cluster,
        system=dict(),
        app=dict(
            **local_app_config,
            use_put=True,
        ),
    ),
    JobConfig(
        name="LocalNativeYield",
        cluster=local_cluster,
        system=dict(),
        app=dict(
            **local_app_config,
            use_yield=True,
        ),
    ),
    JobConfig(
        name="LocalMagnet",
        cluster=local_cluster,
        system=dict(),
        app=dict(
            **local_app_config,
            magnet=True,
        ),
    ),
    JobConfig(
        name="LocalRiffle",
        cluster=local_cluster,
        system=dict(),
        app=dict(
            **local_app_config,
            riffle=True,
            merge_factor=8,
        ),
    ),
    JobConfig(
        name="LocalNativeReduceOnly",
        cluster=local_cluster,
        system=dict(),
        app=dict(
            **local_app_config,
            skip_first_stage=True,
        ),
    ),
    JobConfig(
        name="LocalSchedulingDebug",
        cluster=local_cluster,
        system=dict(),
        app=dict(
            **local_app_config,
            simple_shuffle=True,
        ),
    ),
    # ------------------------------------------------------------
    #     Local fault tolerance experiments
    # ------------------------------------------------------------
    JobConfig(
        name="LocalSimpleFT",
        cluster=local_cluster,
        system=dict(),
        app=dict(
            **local_app_config,
            simple_shuffle=True,
            skip_input=True,
            fail_node=0,
        ),
    ),
    JobConfig(
        name="LocalNativeFT",
        cluster=local_cluster,
        system=dict(),
        app=dict(
            **local_app_config,
            skip_input=True,
            fail_node=0,
        ),
    ),
    JobConfig(
        name="LocalNativePutFT",
        cluster=local_cluster,
        system=dict(),
        app=dict(
            **local_app_config,
            use_put=True,
            skip_input=True,
            fail_node=0,
        ),
    ),
    JobConfig(
        name="LocalMagnetFT",
        cluster=local_cluster,
        system=dict(),
        app=dict(
            **local_app_config,
            magnet=True,
            skip_input=True,
            fail_node=0,
        ),
    ),
    JobConfig(
        name="LocalRiffleFT",
        cluster=local_cluster,
        system=dict(),
        app=dict(
            **local_app_config,
            riffle=True,
            merge_factor=8,
            skip_input=True,
            fail_node=0,
        ),
    ),
    # ------------------------------------------------------------
    #     Local S3 spilling experiments
    # ------------------------------------------------------------
    JobConfig(
        name="LocalS3Spilling",
        cluster=local_cluster,
        system=dict(
            s3_spill=4,
        ),
        app=dict(
            **local_mini_app_config,
        ),
    ),
    JobConfig(
        name="LocalS3IO",
        cluster=local_cluster,
        system=dict(),
        app=dict(
            **local_mini_app_config,
            s3_buckets=get_s3_buckets(1),
        ),
    ),
    JobConfig(
        name="LocalS3IOMultiShard",
        cluster=local_cluster,
        system=dict(),
        app=dict(
            **local_mini_app_config,
            s3_buckets=get_s3_buckets(1),
            num_shards_per_mapper=2,
        ),
    ),
    JobConfig(
        name="LocalS3IOAndSpilling",
        cluster=local_cluster,
        system=dict(
            s3_spill=4,
        ),
        app=dict(
            **local_mini_app_config,
            s3_buckets=get_s3_buckets(1),
        ),
    ),
    JobConfig(
        name="LocalS3IOManualSpillingS3",
        cluster=local_cluster,
        system=dict(),
        app=dict(
            **local_mini_app_config,
            s3_buckets=get_s3_buckets(1),
            spilling=SpillingMode.S3,
        ),
    ),
    JobConfig(
        name="LocalS3IOManualSpillingS3Parallel",
        cluster=local_cluster,
        system=dict(),
        app=dict(
            **local_mini_app_config,
            s3_buckets=get_s3_buckets(1),
            spilling=SpillingMode.S3,
        ),
    ),
    # ------------------------------------------------------------
    #     Local data loader experiments
    # ------------------------------------------------------------
    JobConfig(
        name="LocalNoStreamingDL",
        cluster=local_cluster,
        system=dict(),
        app=dict(
            **local_app_config,
            skip_input=True,
        ),
    ),
    JobConfig(
        name="LocalPartialStreamingDL",
        cluster=local_cluster,
        system=dict(),
        app=dict(
            **local_app_config,
            skip_input=True,
            dataloader_mode="partial",
        ),
    ),
    JobConfig(
        name="LocalFullStreamingDL",
        cluster=local_cluster,
        system=dict(),
        app=dict(
            **local_app_config,
            skip_input=True,
            dataloader_mode="streaming",
        ),
    ),
]
