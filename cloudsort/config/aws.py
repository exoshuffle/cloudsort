# pylint: disable=use-dict-literal
from cloudsort.config.common import (
    InstanceLifetime,
    InstanceType,
    JobConfig,
    SpillingMode,
    get_s3_buckets,
    get_steps,
)

# ------------------------------------------------------------
#     VM Types
# ------------------------------------------------------------

d3_xl = InstanceType(
    name="d3.xlarge",
    cpu=4,
    memory_gib=32,
    disk_count=3,
    hdd=True,
)

d3_2xl = InstanceType(
    name="d3.2xlarge",
    cpu=8,
    memory_gib=61.8,
    disk_count=6,
    hdd=True,
)

i3_2xl = InstanceType(
    name="i3.2xlarge",
    cpu=8,
    memory_gib=61.8,
    disk_count=1,
    disk_device_offset=0,
)

i4i_2xl = InstanceType(
    name="i4i.2xlarge",
    cpu=8,
    memory_gib=61.8,
    disk_count=1,
)

i4i_4xl = InstanceType(
    name="i4i.4xlarge",
    cpu=16,
    memory_gib=123.6,
    disk_count=1,
)

i4i_8xl = InstanceType(
    name="i4i.8xlarge",
    cpu=32,
    memory_gib=247.2,
    disk_count=2,
)

r6i_2xl = InstanceType(
    name="r6i.2xlarge",
    cpu=8,
    memory_gib=61.8,
)

t3_2xl = InstanceType(
    name="t3.2xlarge",
    cpu=2,
    memory_gib=8,
    disk_device_offset=0,
)

m6i_xl = InstanceType(
    name="m6i.xlarge",
    cpu=4,
    memory_gib=16,
)

configs = [
    # ------------------------------------------------------------
    #     t3.2xl 10 nodes scheduling policy debugging
    # ------------------------------------------------------------
    JobConfig(
        name="1tb-2gb-t3",
        cluster=dict(
            instance_count=10,
            instance_type=t3_2xl,
            local=False,
        ),
        system=dict(),
        app=dict(
            **get_steps(),
            total_gb=1000,
            input_part_gb=2,
            map_parallelism_multiplier=1,
            reduce_parallelism_multiplier=1,
            native_scheduling=True,
        ),
    ),
    # ------------------------------------------------------------
    #     i3.2xl 10 nodes 1TB NSDI '23
    # ------------------------------------------------------------
    JobConfig(
        # 571s, https://wandb.ai/raysort/raysort/runs/2ib2wl1l
        name="1tb-2gb-i3-simple",
        cluster=dict(
            instance_count=10,
            instance_type=i3_2xl,
            local=False,
        ),
        system=dict(),
        app=dict(
            **get_steps(),
            total_gb=1000,
            input_part_gb=2,
            reduce_parallelism_multiplier=1,
            simple_shuffle=True,
        ),
    ),
    JobConfig(
        # 675s, https://wandb.ai/raysort/raysort/runs/15gi7d6y
        name="1tb-1gb-i3-simple",
        cluster=dict(
            instance_count=10,
            instance_type=i3_2xl,
            local=False,
        ),
        system=dict(),
        app=dict(
            **get_steps(),
            total_gb=1000,
            input_part_gb=1,
            reduce_parallelism_multiplier=1,
            simple_shuffle=True,
        ),
    ),
    JobConfig(
        # 1038s, https://wandb.ai/raysort/raysort/runs/217qjb59
        name="1tb-.5gb-i3-simple",
        cluster=dict(
            instance_count=10,
            instance_type=i3_2xl,
            local=False,
        ),
        system=dict(),
        app=dict(
            **get_steps(),
            total_gb=1000,
            input_part_gb=0.5,
            reduce_parallelism_multiplier=1,
            simple_shuffle=True,
        ),
    ),
    JobConfig(
        # 804s, https://wandb.ai/raysort/raysort/runs/2kuh08o8
        name="1tb-2gb-i3-riffle",
        cluster=dict(
            instance_count=10,
            instance_type=i3_2xl,
            local=False,
        ),
        system=dict(),
        app=dict(
            **get_steps(),
            total_gb=1000,
            input_part_gb=2,
            reduce_parallelism_multiplier=1,
            riffle=True,
            merge_factor=8,
        ),
    ),
    JobConfig(
        # 705s, https://wandb.ai/raysort/raysort/runs/16gj5var
        name="1tb-1gb-i3-riffle",
        cluster=dict(
            instance_count=10,
            instance_type=i3_2xl,
            local=False,
        ),
        system=dict(),
        app=dict(
            **get_steps(),
            total_gb=1000,
            input_part_gb=1,
            reduce_parallelism_multiplier=1,
            riffle=True,
            merge_factor=8,
        ),
    ),
    JobConfig(
        # 770s, https://wandb.ai/raysort/raysort/runs/wp9zs9qb
        name="1tb-.5gb-i3-riffle",
        cluster=dict(
            instance_count=10,
            instance_type=i3_2xl,
            local=False,
        ),
        system=dict(),
        app=dict(
            **get_steps(),
            total_gb=1000,
            input_part_gb=0.5,
            reduce_parallelism_multiplier=1,
            riffle=True,
            merge_factor=8,
        ),
    ),
    JobConfig(
        # 623s, https://wandb.ai/raysort/raysort/runs/1x8b6ggk
        name="1tb-2gb-i3-magnet",
        cluster=dict(
            instance_count=10,
            instance_type=i3_2xl,
            local=False,
        ),
        system=dict(),
        app=dict(
            **get_steps(),
            total_gb=1000,
            input_part_gb=2,
            reduce_parallelism_multiplier=1,
            magnet=True,
        ),
    ),
    JobConfig(
        # 608s, https://wandb.ai/raysort/raysort/runs/2qn6b7e7
        name="1tb-1gb-i3-magnet",
        cluster=dict(
            instance_count=10,
            instance_type=i3_2xl,
            local=False,
        ),
        system=dict(),
        app=dict(
            **get_steps(),
            total_gb=1000,
            input_part_gb=1,
            reduce_parallelism_multiplier=1,
            magnet=True,
        ),
    ),
    JobConfig(
        # 630s, https://wandb.ai/raysort/raysort/runs/3c71pxms
        name="1tb-.5gb-i3-magnet",
        cluster=dict(
            instance_count=10,
            instance_type=i3_2xl,
            local=False,
        ),
        system=dict(),
        app=dict(
            **get_steps(),
            total_gb=1000,
            input_part_gb=0.5,
            reduce_parallelism_multiplier=1,
            magnet=True,
        ),
    ),
    JobConfig(
        # 597s, https://wandb.ai/raysort/raysort/runs/2ot7wyr4
        name="1tb-2gb-i3-cosco",
        cluster=dict(
            instance_count=10,
            instance_type=i3_2xl,
            local=False,
        ),
        system=dict(),
        app=dict(
            **get_steps(),
            total_gb=1000,
            input_part_gb=2,
            reduce_parallelism_multiplier=1,
        ),
    ),
    JobConfig(
        # 612s, https://wandb.ai/raysort/raysort/runs/1fkhss8g
        name="1tb-1gb-i3-cosco",
        cluster=dict(
            instance_count=10,
            instance_type=i3_2xl,
            local=False,
        ),
        system=dict(),
        app=dict(
            **get_steps(),
            total_gb=1000,
            input_part_gb=1,
            reduce_parallelism_multiplier=1,
        ),
    ),
    JobConfig(
        # 651s, https://wandb.ai/raysort/raysort/runs/1iatcdc6
        name="1tb-.5gb-i3-cosco",
        cluster=dict(
            instance_count=10,
            instance_type=i3_2xl,
            local=False,
        ),
        system=dict(),
        app=dict(
            **get_steps(),
            total_gb=1000,
            input_part_gb=0.5,
            reduce_parallelism_multiplier=1,
        ),
    ),
    # ------------------------------------------------------------
    #     d3.2xl 10 nodes 1TB (NSDI '22)
    # ------------------------------------------------------------
    JobConfig(
        # currently slow due to https://github.com/ray-project/ray/issues/24667
        name="1tb-2gb-d3-cosco",
        cluster=dict(
            instance_count=10,
            instance_type=d3_2xl,
        ),
        system=dict(),
        app=dict(
            **get_steps(),
            total_gb=1000,
            input_part_gb=2,
        ),
    ),
    # ------------------------------------------------------------
    #     i3.2xl 10 nodes 1TB (NSDI '22)
    # ------------------------------------------------------------
    JobConfig(
        # 584s, https://wandb.ai/raysort/raysort/runs/ky90ojwr
        name="1tb-2gb-i3-cosco",
        cluster=dict(
            instance_count=10,
            instance_type=i3_2xl,
        ),
        system=dict(),
        app=dict(
            **get_steps(),
            total_gb=1000,
            input_part_gb=2,
        ),
    ),
    # ------------------------------------------------------------
    #     i4i.2xl 10, 20 nodes
    # ------------------------------------------------------------
    JobConfig(
        # 361s, https://wandb.ai/raysort/raysort/runs/1hdz0pqi
        name="1tb-2gb-i4i",
        cluster=dict(
            instance_count=10,
            instance_type=i4i_2xl,
        ),
        system=dict(),
        app=dict(
            **get_steps(),
            total_gb=1000,
            input_part_gb=2,
            reduce_parallelism_multiplier=1,
        ),
    ),
    # ------------------------------------------------------------
    #     i4i.2xl 100 nodes
    # ------------------------------------------------------------
    JobConfig(
        # 607s, https://wandb.ai/raysort/raysort/runs/3b6bjy93
        # https://raysort.grafana.net/dashboard/snapshot/ODuYv9zKDbFnZc9GSS71mzyYC5MYdolK
        name="10tb-2gb-i4i",
        cluster=dict(
            instance_count=100,
            instance_type=i4i_2xl,
        ),
        system=dict(),
        app=dict(
            **get_steps(),
            total_gb=10000,
            input_part_gb=2,
            reduce_parallelism_multiplier=1,
        ),
    ),
    JobConfig(
        # 3089s, https://wandb.ai/raysort/raysort/runs/35zd12xu
        # https://raysort.grafana.net/dashboard/snapshot/D47iMJ63Vl2eskBynzE472E17DhQqRs0
        name="50tb-2gb-i4i",
        cluster=dict(
            instance_count=100,
            instance_type=i4i_2xl,
        ),
        system=dict(),
        app=dict(
            **get_steps(),
            total_gb=50000,
            input_part_gb=2,
            reduce_parallelism_multiplier=1,
        ),
    ),
    # ------------------------------------------------------------
    #     S3 + i4i.2xl 10 nodes
    # ------------------------------------------------------------
    JobConfig(
        # 423s, https://wandb.ai/raysort/raysort/runs/p1ygq4c6
        name="1tb-2gb-i4i-native-s3",
        cluster=dict(
            instance_count=10,
            instance_type=i4i_2xl,
        ),
        system=dict(),
        app=dict(
            **get_steps(),
            total_gb=1000,
            input_part_gb=2,
            s3_buckets=get_s3_buckets(),
            map_parallelism_multiplier=1,
            reduce_parallelism_multiplier=1,
            merge_factor=1,
        ),
    ),
    JobConfig(
        # TODO
        name="1tb-2gb-i4i4x-s3",
        cluster=dict(
            instance_count=10,
            instance_type=i4i_4xl,
        ),
        system=dict(),
        app=dict(
            **get_steps(),
            total_gb=1000,
            input_part_gb=2,
            s3_buckets=get_s3_buckets(),
            map_parallelism_multiplier=1,
            reduce_parallelism_multiplier=1,
            merge_factor=1,
        ),
    ),
    # ------------------------------------------------------------
    #     S3 + i4i.2xl 20 nodes
    # ------------------------------------------------------------
    JobConfig(
        # 466s, https://wandb.ai/raysort/raysort/runs/jtrapg8i
        name="2tb-2gb-i4i-native-s3",
        cluster=dict(
            instance_count=20,
            instance_type=i4i_2xl,
        ),
        system=dict(),
        app=dict(
            **get_steps(),
            total_gb=2000,
            input_part_gb=2,
            s3_buckets=get_s3_buckets(),
            map_parallelism_multiplier=1,
            reduce_parallelism_multiplier=1,
            merge_factor=1,
        ),
    ),
    JobConfig(
        # 402s, https://wandb.ai/raysort/raysort/runs/jpegf7l2
        name="2tb-2gb-i4i4x-s3",
        cluster=dict(
            instance_count=10,
            instance_type=i4i_4xl,
        ),
        system=dict(),
        app=dict(
            **get_steps(),
            total_gb=2000,
            input_part_gb=2,
            # output_part_gb=4,
            s3_buckets=get_s3_buckets(),
            map_parallelism_multiplier=12 / 16,
            reduce_parallelism_multiplier=12 / 16,
            merge_factor=1,
        ),
    ),
    JobConfig(
        # 1550s
        name="8tb-2gb-10-i4i4x-s3",
        cluster=dict(
            instance_count=10,
            instance_type=i4i_4xl,
        ),
        system=dict(),
        app=dict(
            **get_steps(),
            total_gb=8000,
            input_part_gb=2,
            output_part_gb=4,
            s3_buckets=get_s3_buckets(),
            map_parallelism_multiplier=0.75,
            reduce_parallelism_multiplier=0.75,
            merge_factor=1,
        ),
    ),
    JobConfig(
        # 433s * 8
        name="16tb-2gb-i4i4x-s3",
        cluster=dict(
            instance_count=10,
            instance_type=i4i_4xl,
        ),
        system=dict(),
        app=dict(
            **get_steps(),
            total_gb=16000,
            input_part_gb=2,
            s3_buckets=get_s3_buckets(),
            map_parallelism_multiplier=0.75,
            reduce_parallelism_multiplier=0.75,
            merge_factor=1,
        ),
    ),
    # ------------------------------------------------------------
    #     S3 + i4i.2xl 40 nodes
    # ------------------------------------------------------------
    JobConfig(
        # 536s, https://wandb.ai/raysort/raysort/runs/14xr10t2
        name="4tb-2gb-i4i-native-s3",
        cluster=dict(
            instance_count=40,
            instance_type=i4i_2xl,
        ),
        system=dict(),
        app=dict(
            **get_steps(),
            total_gb=4000,
            input_part_gb=2,
            s3_buckets=get_s3_buckets(),
            reduce_parallelism_multiplier=1,
        ),
    ),
    JobConfig(
        # 418s, https://wandb.ai/raysort/raysort/runs/3n4runhx
        name="4tb-2gb-i4i4x-s3",
        cluster=dict(
            instance_count=20,
            instance_type=i4i_4xl,
        ),
        system=dict(),
        app=dict(
            **get_steps(),
            total_gb=4000,
            input_part_gb=2,
            s3_buckets=get_s3_buckets(),
            map_parallelism_multiplier=0.75,
            reduce_parallelism_multiplier=0.75,
            merge_factor=1,
            num_shards_per_mapper=2,
        ),
    ),
    JobConfig(
        # 2901s, https://wandb.ai/raysort/raysort/runs/q0w17xxi
        name="20tb-2gb-i4i-native-s3",
        cluster=dict(
            instance_count=40,
            instance_type=i4i_2xl,
        ),
        system=dict(),
        app=dict(
            **get_steps(),
            total_gb=20000,
            input_part_gb=2,
            s3_buckets=get_s3_buckets(),
            reduce_parallelism_multiplier=1,
        ),
    ),
    JobConfig(
        # 1266s; 1100s if perfect scaling
        name="10tb-2gb-20-i4i4x-s3",
        cluster=dict(
            instance_count=20,
            instance_type=i4i_4xl,
        ),
        system=dict(),
        app=dict(
            **get_steps(),
            total_gb=10000,
            input_part_gb=2,
            s3_buckets=get_s3_buckets(),
            map_parallelism_multiplier=1,
            reduce_parallelism_multiplier=1,
            merge_factor=1,
        ),
    ),
    # ------------------------------------------------------------
    #     S3 + i4i.2xl 60 nodes
    # ------------------------------------------------------------
    JobConfig(
        # 454s, https://wandb.ai/raysort/raysort/runs/3jpwrmic
        name="6tb-2gb-i4i4x-s3",
        cluster=dict(
            instance_count=30,
            instance_type=i4i_4xl,
        ),
        system=dict(),
        app=dict(
            **get_steps(),
            total_gb=6000,
            input_part_gb=2,
            s3_buckets=get_s3_buckets(30),
            map_parallelism_multiplier=0.75,
            reduce_parallelism_multiplier=0.75,
            merge_factor=1,
        ),
    ),
    JobConfig(
        # 5748s with a long tail.
        name="60tb-2gb-i4i4x-s3",
        cluster=dict(
            instance_count=36,
            instance_type=i4i_4xl,
        ),
        system=dict(),
        app=dict(
            **get_steps(),
            total_gb=60000,
            input_part_gb=2,
            s3_buckets=get_s3_buckets(20),
            map_parallelism_multiplier=1,
            reduce_parallelism_multiplier=1,
            merge_factor=1,
        ),
    ),
    # ------------------------------------------------------------
    #     S3 + i4i.2xl 100 nodes
    # ------------------------------------------------------------
    JobConfig(
        # 681s, https://wandb.ai/raysort/raysort/runs/39gvukz0
        # 795s with multi upload
        name="10tb-2gb-i4i-native-s3",
        cluster=dict(
            instance_count=100,
            instance_type=i4i_2xl,
        ),
        system=dict(),
        app=dict(
            **get_steps(),
            total_gb=10000,
            input_part_gb=2,
            s3_buckets=get_s3_buckets(),
            reduce_parallelism_multiplier=1,
            use_yield=True,
        ),
    ),
    JobConfig(
        # 4153s, https://wandb.ai/raysort/raysort/runs/qcw9riog (multi upload)
        # https://raysort.grafana.net/dashboard/snapshot/41UCIyP11JsWOawGx3S0KMiyfmgBEjkt
        # 4028s, https://wandb.ai/raysort/raysort/runs/g03tgbgz (single upload)
        name="50tb-2gb-i4i-native-s3",
        cluster=dict(
            instance_count=100,
            instance_type=i4i_2xl,
        ),
        system=dict(),
        app=dict(
            **get_steps(),
            total_gb=50000,
            input_part_gb=2,
            s3_buckets=get_s3_buckets(),
            reduce_parallelism_multiplier=1,
        ),
    ),
    # ------------------------------------------------------------
    #     S3 + i4i.4xl 40 nodes
    # ------------------------------------------------------------
    JobConfig(
        # 460s, https://wandb.ai/raysort/raysort/runs/17mqvhde
        # 456s, https://wandb.ai/raysort/raysort/runs/6emlt2u1
        name="8tb-2gb-i4i4x-s3",
        cluster=dict(
            instance_count=40,
            instance_type=i4i_4xl,
        ),
        system=dict(),
        app=dict(
            **get_steps(),
            total_gb=8000,
            input_part_gb=2,
            output_part_gb=4,
            s3_buckets=get_s3_buckets(40),
            map_parallelism_multiplier=0.75,
            reduce_parallelism_multiplier=0.75,
            merge_factor=1,
        ),
    ),
    JobConfig(
        # 2243s, https://wandb.ai/raysort/raysort/runs/3i916jv3
        # 2187s, https://wandb.ai/raysort/raysort/runs/grm4o1p7
        name="40tb-2gb-i4i4x-s3",
        cluster=dict(
            instance_count=40,
            instance_type=i4i_4xl,
        ),
        system=dict(),
        app=dict(
            **get_steps(),
            total_gb=40000,
            input_part_gb=2,
            output_part_gb=4,
            s3_buckets=get_s3_buckets(40),
            map_parallelism_multiplier=0.75,
            reduce_parallelism_multiplier=0.75,
            merge_factor=1,
        ),
    ),
    JobConfig(
        # 5361s, https://wandb.ai/raysort/raysort/runs/sfycimry
        name="100tb-2gb-i4i4x-s3",
        cluster=dict(
            instance_count=40,
            instance_type=i4i_4xl,
        ),
        system=dict(),
        app=dict(
            **get_steps(),
            total_gb=100000,
            input_part_gb=2,
            output_part_gb=4,
            s3_buckets=get_s3_buckets(40),
            map_parallelism_multiplier=0.75,
            reduce_parallelism_multiplier=0.75,
            merge_factor=1,
        ),
    ),
    # ------------------------------------------------------------
    #     S3 10 nodes 1TB
    # ------------------------------------------------------------
    JobConfig(
        # 570s, https://wandb.ai/raysort/raysort/runs/2n652zza
        name="1tb-2gb-s3-native-s3",
        cluster=dict(
            instance_count=10,
            instance_type=r6i_2xl,
        ),
        system=dict(
            s3_spill=16,
        ),
        app=dict(
            **get_steps(),
            total_gb=1000,
            input_part_gb=2,
            s3_buckets=get_s3_buckets(),
        ),
    ),
    JobConfig(
        # 575s, https://wandb.ai/raysort/raysort/runs/3vk1b0aa
        name="1tb-1gb-s3-native-s3",
        cluster=dict(
            instance_count=10,
            instance_type=r6i_2xl,
        ),
        system=dict(
            s3_spill=16,
        ),
        app=dict(
            **get_steps(),
            total_gb=1000,
            input_part_gb=1,
            s3_buckets=get_s3_buckets(),
        ),
    ),
    JobConfig(
        # 650s, https://wandb.ai/raysort/raysort/runs/2d7d9ysa
        name="1tb-2gb-s3-manual-s3",
        cluster=dict(
            instance_count=10,
            instance_type=r6i_2xl,
        ),
        system=dict(
            object_spilling_threshold=1,
        ),
        app=dict(
            **get_steps(),
            total_gb=1000,
            input_part_gb=2,
            s3_buckets=get_s3_buckets(),
            spilling=SpillingMode.S3,
        ),
    ),
    # ------------------------------------------------------------
    #     S3 20 nodes
    # ------------------------------------------------------------
    JobConfig(
        # 650s, https://wandb.ai/raysort/raysort/runs/30rszs7y
        # 580s, https://wandb.ai/raysort/raysort/runs/3e7h09lt (cannot reproduce)
        name="2tb-2gb-s3-native-s3",
        cluster=dict(
            instance_count=20,
            instance_type=r6i_2xl,
        ),
        system=dict(
            max_fused_object_count=3,
            s3_spill=16,
        ),
        app=dict(
            **get_steps(),
            total_gb=2000,
            input_part_gb=2,
            s3_buckets=get_s3_buckets(),
        ),
    ),
    JobConfig(
        # 2906s, https://wandb.ai/raysort/raysort/runs/1r83qp4x
        name="10tb-2gb-s3-native-s3",
        cluster=dict(
            instance_count=20,
            instance_type=r6i_2xl,
        ),
        system=dict(
            max_fused_object_count=3,
            s3_spill=16,
        ),
        app=dict(
            **get_steps(),
            total_gb=10000,
            input_part_gb=2,
            s3_buckets=get_s3_buckets(),
        ),
    ),
    JobConfig(
        # 730s, https://wandb.ai/raysort/raysort/runs/2tlqlqpo
        name="2tb-2gb-s3-manual-s3",
        cluster=dict(
            instance_count=20,
            instance_type=r6i_2xl,
        ),
        system=dict(
            object_spilling_threshold=1,
        ),
        app=dict(
            **get_steps(),
            total_gb=2000,
            input_part_gb=2,
            s3_buckets=get_s3_buckets(),
            spilling=SpillingMode.S3,
        ),
    ),
    # ------------------------------------------------------------
    #     S3 40 nodes
    # ------------------------------------------------------------
    JobConfig(
        # 707s, https://wandb.ai/raysort/raysort/runs/2zekqq6m
        name="4tb-2gb-s3-manual-s3",
        cluster=dict(
            instance_count=40,
            instance_type=r6i_2xl,
        ),
        system=dict(
            object_spilling_threshold=1,
        ),
        app=dict(
            **get_steps(),
            total_gb=4000,
            input_part_gb=2,
            s3_buckets=get_s3_buckets(),
            spilling=SpillingMode.S3,
        ),
    ),
    JobConfig(
        # TODO(@lsf)
        name="20tb-2gb-s3-manual-s3",
        cluster=dict(
            instance_count=40,
            instance_type=r6i_2xl,
        ),
        system=dict(
            object_spilling_threshold=1,
        ),
        app=dict(
            **get_steps(),
            total_gb=20000,
            input_part_gb=2,
            s3_buckets=get_s3_buckets(),
            spilling=SpillingMode.S3,
        ),
    ),
    # ------------------------------------------------------------
    #     Spot instances 20 nodes
    # ------------------------------------------------------------
    JobConfig(
        name="600gb-1gb-spot-s3",
        cluster=dict(
            instance_count=20,
            instance_type=r6i_2xl,
            instance_lifetime=InstanceLifetime.SPOT,
        ),
        system=dict(
            max_fused_object_count=3,
            s3_spill=16,
        ),
        app=dict(
            **get_steps(),
            total_gb=600,
            input_part_gb=1,
            s3_buckets=get_s3_buckets(),
        ),
    ),
    # ------------------------------------------------------------
    #     Spot version of i3.2xl 10 nodes 1TB
    # ------------------------------------------------------------
    JobConfig(
        # 584s, https://wandb.ai/raysort/raysort/runs/ky90ojwr
        name="1tb-2gb-i3-spot",
        cluster=dict(
            instance_count=10,
            instance_type=i3_2xl,
            instance_lifetime=InstanceLifetime.SPOT,
        ),
        system=dict(),
        app=dict(
            **get_steps(),
            total_gb=1000,
            input_part_gb=2,
        ),
    ),
    # ------------------------------------------------------------
    #     MapReduce Online Test Cluster
    # ------------------------------------------------------------
    JobConfig(
        name="mpo",
        cluster=dict(
            instance_count=10,
            instance_type=r6i_2xl,
            instance_lifetime=InstanceLifetime.SPOT,
        ),
        system=dict(),
        app=dict(
            **get_steps(),
            total_gb=1000,
            input_part_gb=1,
        ),
    ),
]
