"""Cluster management toolkit."""
import json
import os
import pathlib
import shutil
import signal
import socket
import string
import subprocess
from typing import Dict, List, Optional, Tuple, Union

import boto3
import click
import psutil
import ray
import shell_utils
import yaml

from cloudsort import config
from cloudsort.typing import InstanceLifetime

cfg = config.get()

MNT_PATH_PATTERN = "/mnt/data*"
MNT_PATH_FMT = "/mnt/data{i}"
PARALLELISM = (os.cpu_count() or 1) * 4
SCRIPT_DIR = pathlib.Path(os.path.dirname(__file__))

ANSIBLE_DIR = SCRIPT_DIR / "config" / "ansible"
HADOOP_TEMPLATE_DIR = SCRIPT_DIR / "config" / "hadoop"
TERRAFORM_DIR = SCRIPT_DIR / "config" / "terraform"
RAY_SYSTEM_CONFIG_FILE_PATH = SCRIPT_DIR.parent / "_ray_config.yml"

GRAFANA_SERVER_PORT = 3000
PROMETHEUS_SERVER_PORT = 9090
PROMETHEUS_NODE_EXPORTER_PORT = 8091
RAY_METRICS_EXPORT_PORT = 8090
RAY_OBJECT_MANAGER_PORT = 8076

KiB = 1024
MiB = KiB * 1024


# ------------------------------------------------------------
#     Terraform and AWS
# ------------------------------------------------------------


def get_tf_template_dir() -> str:
    if cfg.cluster.instance_type.cloud == config.Cloud.AZURE:
        return "azure-template"
    if cfg.cluster.instance_lifetime == InstanceLifetime.DEDICATED:
        return "aws-template"
    return "aws-spot-template"


def get_tf_dir(cluster_name: str) -> pathlib.Path:
    return TERRAFORM_DIR / f"_{cluster_name}"


def get_instances(filters: Dict) -> List[Dict]:
    ec2 = boto3.client("ec2")
    paginator = ec2.get_paginator("describe_instances")
    ret = []
    for page in paginator.paginate(
        Filters=[{"Name": k, "Values": v} for k, v in filters.items()]
    ):
        ret.extend(page["Reservations"])
    return [item["Instances"][0] for item in ret]


def check_cluster_existence(cluster_name: str, raise_if_exists: bool = False) -> bool:
    # TODO(@lsf): check azure
    instances = get_instances(
        {
            "tag:ClusterName": [cluster_name],
            # Excluding "shutting-down" (0x20) and "terminated" (0x30).
            # https://docs.aws.amazon.com/cli/latest/reference/ec2/describe-instances.html
            "instance-state-code": [str(code) for code in [0x00, 0x10, 0x40, 0x50]],
        }
    )
    cnt = len(instances)
    ret = cnt > 0
    if raise_if_exists and ret:
        shell_utils.error(f"{cluster_name} must not exist (found {cnt} instances)")
    return ret


def get_terraform_vars(**kwargs) -> str:
    return "".join([f' -var="{k}={v}"' for k, v in kwargs.items()])


def get_or_create_tf_dir(cluster_name: str, must_exist: bool = False) -> pathlib.Path:
    tf_dir = get_tf_dir(cluster_name)
    if os.path.exists(tf_dir):
        if not must_exist:
            click.echo(f"Found existing configuration for {cluster_name}")
        return tf_dir
    if must_exist:
        raise FileNotFoundError(f"Cluster configuration does not exist {tf_dir}")

    template_dir = TERRAFORM_DIR / get_tf_template_dir()
    assert not os.path.exists(tf_dir), f"{tf_dir} must not exist"
    assert os.path.exists(template_dir), f"{template_dir} must exist"
    shutil.copytree(template_dir, tf_dir)
    click.echo(f"Created configuration directory for {cluster_name}")
    click.echo(f"You can manually edit {tf_dir}/main.tf and then run this launcher")
    return tf_dir


def terraform_provision(cluster_name: str) -> None:
    tf_dir = get_or_create_tf_dir(cluster_name)
    shell_utils.run("terraform init", cwd=tf_dir)
    cmd = "terraform apply -auto-approve" + get_terraform_vars(
        cluster_name=cluster_name,
        instance_count=cfg.cluster.instance_count,
        instance_type=cfg.cluster.instance_type.name,
        instance_disk_gb=cfg.cluster.instance_disk_gb,
    )
    shell_utils.run(cmd, cwd=tf_dir)


def get_tf_output(
    cluster_name: str, key: Union[str, List[str]]
) -> Union[List[str], List[List[str]]]:
    tf_dir = get_or_create_tf_dir(cluster_name, must_exist=True)
    p = shell_utils.run("terraform output -json", cwd=tf_dir, stdout=subprocess.PIPE)
    data = json.loads(p.stdout.decode("ascii"))
    if isinstance(key, list):
        return [data[k]["value"] for k in key]
    return data[key]["value"]


def aws_action(cluster_name: str, method: str, verb: str) -> None:
    ids = get_tf_output(cluster_name, "instance_ids")
    ec2 = boto3.client("ec2")
    fn = getattr(ec2, method)
    fn(InstanceIds=ids)
    click.secho(f"{verb} {cluster_name} ({len(ids)} instances)", fg="green")


# ------------------------------------------------------------
#     Ansible
# ------------------------------------------------------------


def get_ansible_inventory_content(node_ips: List[str]) -> str:
    def get_item(ip):
        host = "node_" + ip.replace(".", "_")
        return host, {"ansible_host": ip}

    hosts = [get_item(ip) for ip in node_ips]
    ret = {
        "all": {
            "hosts": dict(hosts),
        },
    }
    return yaml.dump(ret)


def get_or_create_ansible_inventory(
    cluster_name: str, ips: Optional[List[str]] = None
) -> pathlib.Path:
    path = ANSIBLE_DIR / f"_{cluster_name}.yml"
    if not ips:
        if os.path.exists(path):
            return path
        raise ValueError("No hosts provided to Ansible")
    with open(path, "w") as fout:
        fout.write(get_ansible_inventory_content(ips))
    click.secho(f"Created {path}", fg="green")
    return path


def get_ssh_key() -> str:
    if cfg.cluster.instance_type.cloud == config.Cloud.AWS:
        return "/home/ubuntu/.aws/login-us-west-2.pem"
    if cfg.cluster.instance_type.cloud == config.Cloud.AZURE:
        return "/home/azureuser/.ssh/lsf-azure-aa.pem"
    return ""


def run_ansible_playbook(
    inventory_path: pathlib.Path,
    playbook: str,
    *,
    ev: Optional[Dict] = None,
    retries: int = 1,
    time_between_retries: float = 10,
) -> subprocess.CompletedProcess:
    if not playbook.endswith(".yml"):
        playbook += ".yml"
    playbook_path = ANSIBLE_DIR / playbook
    cmd = "ansible-playbook"
    cmd += " --key-file " + get_ssh_key()
    cmd += f" -f {PARALLELISM} -i {inventory_path} {playbook_path}"
    if ev:
        cmd += f" --extra-vars '{json.dumps(ev)}'"
    return shell_utils.run(
        cmd,
        cwd=ANSIBLE_DIR,
        retries=retries,
        time_between_retries=time_between_retries,
    )


def get_data_disks() -> List[str]:
    offset = cfg.cluster.instance_type.disk_device_offset
    return [
        f"/dev/nvme{i + offset}n1" for i in range(cfg.cluster.instance_type.disk_count)
    ]


def get_mnt_paths() -> List[str]:
    return [
        MNT_PATH_FMT.format(i=i) for i in range(cfg.cluster.instance_type.disk_count)
    ]


def get_ansible_vars() -> Dict:
    ret = {}
    if cfg.cluster.instance_type.disk_count == 0:
        ret["mnt_prefix"] = ""
    ret["data_disks"] = get_data_disks()
    return ret


# ------------------------------------------------------------
#     YARN
# ------------------------------------------------------------


def update_hosts_file(ips: List[str]) -> None:
    PATH = "/etc/hosts"
    MARKER = "### HADOOP_YARN_HOSTS ###\n"
    with open(PATH) as fin:
        content = fin.read()

    marker_idx = content.find(MARKER)
    content = content if marker_idx < 0 else content[:marker_idx]
    content += "\n" + MARKER

    for i, ip in enumerate(ips):
        content += f"{ip} dn{i + 1}\n"

    shell_utils.run(f"sudo cp {PATH} {PATH}.backup")
    shell_utils.run(f"sudo echo <<EOF\n{content}\nEOF > {PATH}")
    click.secho(f"Updated {PATH}", fg="green")


def update_workers_file(ips: List[str]) -> None:
    PATH = os.path.join(os.getenv("HADOOP_HOME"), "etc/hadoop/workers")
    shell_utils.run(f"cp {PATH} {PATH}.backup")
    with open(PATH, "w") as fout:
        fout.write("\n".join(ips))
    click.secho(f"Updated {PATH}", fg="green")


def update_hadoop_xml(
    filename: str, head_ip: str, mnt_paths: List[str], is_hdd: bool
) -> None:
    with open(HADOOP_TEMPLATE_DIR / (filename + ".template")) as fin:
        template = string.Template(fin.read())
    content = template.substitute(
        DEFAULT_FS=f"hdfs://{head_ip}:9000",
        HEAD_IP=head_ip,
        IO_BUFFER_SIZE=128 * KiB if is_hdd else 4 * KiB,
        DATA_DIRS=",".join(os.path.join(p, "hadoop/dfs/data") for p in mnt_paths),
        LOCAL_DIRS=",".join(os.path.join(p, "hadoop/yarn/local") for p in mnt_paths),
    )
    output_path = os.path.join(os.getenv("HADOOP_HOME"), "etc/hadoop", filename)
    with open(output_path, "w") as fout:
        fout.write(content)
    click.secho(f"Updated {output_path}", fg="green")


def update_hadoop_config(head_ip: str, mnt_paths: List[str], is_hdd: bool) -> None:
    for filename in ["core-site.xml", "hdfs-site.xml", "yarn-site.xml"]:
        update_hadoop_xml(filename, head_ip, mnt_paths, is_hdd)


# ------------------------------------------------------------
#     Prometheus
# ------------------------------------------------------------


def free_port(port: int):
    for proc in psutil.process_iter():
        try:
            for conns in proc.connections(kind="inet"):
                if conns.laddr.port == port:
                    proc.send_signal(signal.SIGTERM)
        except (PermissionError, psutil.AccessDenied):
            continue


def get_prometheus_sd_content(head_ip: str, ips: List[str]) -> str:
    def get_addrs(port, include_head=True):
        return [f"{ip}:{port}" for ip in (ips if not include_head else ips + [head_ip])]

    return json.dumps(
        [
            {
                "labels": {"job": "ray"},
                "targets": get_addrs(RAY_METRICS_EXPORT_PORT),
            },
            {
                "labels": {"job": "node"},
                "targets": get_addrs(PROMETHEUS_NODE_EXPORTER_PORT, include_head=False),
            },
        ]
    )


def setup_prometheus(head_ip: str, ips: List[str]) -> None:
    prometheus_data_path = "/tmp/prometheus"
    os.makedirs(prometheus_data_path, exist_ok=True)
    with open("/tmp/prometheus/service_discovery.json", "w") as fout:
        fout.write(get_prometheus_sd_content(head_ip, ips))
    free_port(PROMETHEUS_SERVER_PORT)
    cmd = str(SCRIPT_DIR.parent / "cloudsort/bin/prometheus/prometheus")
    cmd += " --web.enable-admin-api"
    cmd += " --config.file=" + str(SCRIPT_DIR / "config/prometheus/prometheus.yml")
    cmd += f" --storage.tsdb.path={prometheus_data_path}"
    cmd += " --storage.tsdb.retention.size=4GB"
    subprocess.Popen(cmd, shell=True)  # pylint: disable=consider-using-with


def setup_grafana() -> None:
    cwd = str(SCRIPT_DIR.parent / "cloudsort/bin/grafana")
    cmd = f"{cwd}/bin/grafana-server"
    free_port(GRAFANA_SERVER_PORT)
    subprocess.Popen(cmd, cwd=cwd, shell=True)  # pylint: disable=consider-using-with


# ------------------------------------------------------------
#     Interface Methods
# ------------------------------------------------------------


def get_cluster_name() -> str:
    user = os.getenv("USER")
    assert user, "$USER is not set"
    return f"{cfg.cluster.name}-{user}"


def get_current_ip() -> str:
    if cfg.cluster.instance_type.cloud == config.Cloud.AWS:
        return shell_utils.run_output("ec2metadata --local-ipv4")
    return socket.gethostbyname(socket.gethostname())


def common_setup(cluster_name: str, cluster_exists: bool) -> pathlib.Path:
    head_ip = get_current_ip()
    ips = get_tf_output(cluster_name, "instance_ips")
    inventory_path = get_or_create_ansible_inventory(cluster_name, ips=ips)
    if not os.environ.get("HADOOP_HOME"):
        click.secho("$HADOOP_HOME not set, skipping Hadoop setup", color="yellow")
    else:
        update_hosts_file(ips)
        update_workers_file(ips)
        update_hadoop_config(head_ip, get_mnt_paths(), cfg.cluster.instance_type.hdd)
    # TODO: use boto3 to wait for describe_instance_status to be "ok" for all
    if cfg.cluster.instance_type.cloud == config.Cloud.AWS and not cluster_exists:
        shell_utils.sleep(60, "worker nodes starting up")
    ev = get_ansible_vars()
    run_ansible_playbook(inventory_path, "setup", ev=ev, retries=10)
    setup_prometheus(head_ip, ips)
    setup_grafana()
    return inventory_path


def json_dump_no_space(data) -> str:
    return json.dumps(data, separators=(",", ":"))


def get_ray_start_cmd() -> Tuple[str, Dict, Dict]:
    system_config = {
        "local_fs_capacity_threshold": 0.999,
        "memory_usage_threshold_fraction": 1.0,
        "max_fused_object_count": cfg.system.max_fused_object_count,
        "object_spilling_threshold": cfg.system.object_spilling_threshold,
        "verbose_spill_logs": 0,
    }
    if cfg.system.s3_spill > 0:
        system_config.update(
            **{
                "max_io_workers": cfg.system.s3_spill,
                "object_spilling_config": json_dump_no_space(
                    {
                        "type": "ray_storage",
                        "params": {"buffer_size": 16 * MiB},
                    }
                ),
            }
        )
    else:
        mnt_paths = get_mnt_paths()
        if len(mnt_paths) == 0:
            click.echo("No data disks mounted. Using /tmp.")
            mnt_paths = ["/tmp"]
        system_config.update(
            **{
                "max_io_workers": max(4, len(mnt_paths) * 2),
                "object_spilling_config": json_dump_no_space(
                    {
                        "type": "filesystem",
                        "params": {
                            "directory_path": [f"{mnt}/ray" for mnt in mnt_paths],
                            "buffer_size": 10 * MiB
                            if cfg.cluster.instance_type.hdd
                            else -1,
                        },
                    },
                ),
            }
        )
    system_config_str = json_dump_no_space(system_config)
    resources = json_dump_no_space({"head": 1})
    cmd = "ray start --head"
    cmd += f" --metrics-export-port={RAY_METRICS_EXPORT_PORT}"
    cmd += f" --object-manager-port={RAY_OBJECT_MANAGER_PORT}"
    cmd += f" --system-config='{system_config_str}'"
    cmd += f" --resources='{resources}'"
    env = {
        "RAY_STORAGE": cfg.system.ray_storage,
    }
    env = {k: str(v) for k, v in env.items()}
    return cmd, env, system_config


def write_ray_system_config(conf: Dict, path: pathlib.Path) -> None:
    with open(path, "w") as fout:
        yaml.dump(conf, fout)


def restart_ray(
    inventory_path: pathlib.Path,
    clear_data_dir: bool,
) -> None:
    # Clear all mounts in case the previous setup has more mounts than we have.
    shell_utils.run(f"sudo rm -rf {MNT_PATH_PATTERN}")
    for mnt in get_mnt_paths():
        shell_utils.run(f"sudo mkdir -m 777 -p {mnt}")
    shell_utils.run(f"rsync -a {SCRIPT_DIR.parent}/ray-patch/ {ray.__path__[0]}")
    shell_utils.run("ray stop -f")
    ray_cmd, ray_env, ray_system_config = get_ray_start_cmd()
    shell_utils.run(ray_cmd, env=dict(os.environ, **ray_env))
    head_ip = get_current_ip()
    # TODO(@lsf): pass ray_env to ansible
    ev = {
        "head_ip": head_ip,
        "clear_data_dir": clear_data_dir,
        "ray_object_manager_port": RAY_OBJECT_MANAGER_PORT,
        "ray_merics_export_port": RAY_METRICS_EXPORT_PORT,
        "ray_object_store_memory": cfg.system.object_store_memory_bytes,
        "shared_memory_bytes": cfg.system.shared_memory_bytes,
        "mnt_paths": get_mnt_paths(),
    }
    run_ansible_playbook(inventory_path, "ray", ev=ev)
    shell_utils.sleep(3, "waiting for Ray nodes to connect")
    shell_utils.run("ray status")
    write_ray_system_config(ray_system_config, RAY_SYSTEM_CONFIG_FILE_PATH)


def restart_yarn(inventory_path: pathlib.Path) -> None:
    mnt_paths = get_mnt_paths()
    env = dict(
        os.environ,
        HADOOP_SSH_OPTS=f"-i {get_ssh_key()} -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR",
        HADOOP_OPTIONAL_TOOLS="hadoop-aws",
    )
    HADOOP_HOME = os.getenv("HADOOP_HOME")
    SPARK_HOME = os.getenv("SPARK_HOME")
    SPARK_EVENTS_DIR = "hdfs:///spark-events"
    shell_utils.run(f"{SPARK_HOME}/sbin/stop-history-server.sh")
    shell_utils.run(f"{HADOOP_HOME}/sbin/stop-yarn.sh", env=env)
    shell_utils.run(f"{HADOOP_HOME}/sbin/stop-dfs.sh", env=env)
    shell_utils.run(f"{HADOOP_HOME}/bin/hdfs namenode -format -force")
    run_ansible_playbook(inventory_path, "yarn", ev={"mnt_paths": mnt_paths})
    shell_utils.run(f"{HADOOP_HOME}/sbin/start-dfs.sh", env=env)
    shell_utils.run(f"{HADOOP_HOME}/sbin/start-yarn.sh", env=env)
    shell_utils.run(f"{HADOOP_HOME}/bin/hdfs dfs -mkdir {SPARK_EVENTS_DIR}")
    shell_utils.run(
        f"{SPARK_HOME}/sbin/start-history-server.sh",
        env=dict(
            env,
            SPARK_HISTORY_OPTS=f"-Dspark.history.fs.logDirectory={SPARK_EVENTS_DIR}",
        ),
    )


def print_after_setup(cluster_name: str) -> None:
    success_msg = f"Cluster {cluster_name} is up and running."
    click.secho("\n" + "-" * len(success_msg), fg="green")
    click.secho(success_msg, fg="green")
    click.echo(f"  Terraform config directory: {get_tf_dir(cluster_name)}")
    click.echo(f"  Ray system config written to: {RAY_SYSTEM_CONFIG_FILE_PATH}")


def pip_install_upgrade() -> None:
    requirement_dir = SCRIPT_DIR.parent / "requirements"
    requirements = " ".join(
        f"-r {requirement_dir}/{requirement_file}"
        for requirement_file in ["dev.txt", "worker.txt"]
    )
    shell_utils.run(f"pip install --no-cache-dir --upgrade {requirements}")


# ------------------------------------------------------------
#     CLI Interface
# ------------------------------------------------------------


def setup_command_options(cli_fn):
    decorators = [
        cli.command(),
        click.option(
            "--ray",
            default=False,
            is_flag=True,
            help="start a Ray cluster",
        ),
        click.option(
            "--yarn",
            default=False,
            is_flag=True,
            help="start a YARN cluster",
        ),
        click.option(
            "--clear_data_dir",
            default=False,
            is_flag=True,
            help="whether to remove input data directory",
        ),
    ]
    ret = cli_fn
    for dec in decorators:
        ret = dec(ret)
    return ret


@click.group()
def cli():
    pass


@setup_command_options
def up(
    ray: bool,  # pylint: disable=redefined-outer-name
    yarn: bool,
    clear_data_dir: bool,
):
    pip_install_upgrade()
    cluster_name = get_cluster_name()
    cluster_exists = check_cluster_existence(cluster_name)
    config_exists = os.path.exists(get_tf_dir(cluster_name))
    if cluster_exists and not config_exists:
        shell_utils.error(
            f"{cluster_name} exists on the cloud but nothing is found locally"
        )
    terraform_provision(cluster_name)
    inventory_path = common_setup(cluster_name, cluster_exists)
    if ray:
        restart_ray(
            inventory_path,
            clear_data_dir,
        )
    if yarn:
        restart_yarn(inventory_path)
    print_after_setup(cluster_name)


@setup_command_options
@click.option(
    "--no-common",
    default=False,
    is_flag=True,
    help="whether to skip common setup (file sync, mounts, etc)",
)
def setup(
    ray: bool,  # pylint: disable=redefined-outer-name
    yarn: bool,
    clear_data_dir: bool,
    no_common: bool,
):
    cluster_name = get_cluster_name()
    if no_common:
        inventory_path = get_or_create_ansible_inventory(cluster_name)
    else:
        inventory_path = common_setup(cluster_name, True)
    if ray:
        restart_ray(
            inventory_path,
            clear_data_dir,
        )
    if yarn:
        restart_yarn(inventory_path)
    print_after_setup(cluster_name)


@cli.command()
def down():
    cluster_name = get_cluster_name()
    tf_dir = get_or_create_tf_dir(cluster_name, must_exist=True)
    cmd = "terraform destroy -auto-approve" + get_terraform_vars(
        cluster_name=cluster_name
    )
    shell_utils.run(cmd, cwd=tf_dir)
    check_cluster_existence(cluster_name, raise_if_exists=True)


@cli.command()
def start():
    aws_action(get_cluster_name(), "start_instances", "Started")


@cli.command()
def stop():
    aws_action(get_cluster_name(), "stop_instances", "Stopped")


@cli.command()
def reboot():
    aws_action(get_cluster_name(), "reboot_instances", "Rebooted")


@cli.command()
@click.argument("worker_id_or_ip", type=str, default="0")
def ssh(worker_id_or_ip: str):
    try:
        idx = int(worker_id_or_ip)
        ips = get_tf_output(get_cluster_name(), "instance_ips")
        click.echo(f"worker_ips = {ips}")
        ip = ips[idx]
    except ValueError:
        ip = worker_id_or_ip
    shell_utils.run(
        f"ssh -i {get_ssh_key()} -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null {ip}"
    )


if __name__ == "__main__":
    cli()
