# Exoshuffle-CloudSort

This repository is a snapshot of [Exoshuffle-CloudSort](http://sortbenchmark.org/ExoshuffleCloudSort2022.pdf), the winning entry of the [2022 CloudSort Benchmark](http://sortbenchmark.org/) in the Indy category.

## Prerequisites

To run Exoshuffle-CloudSort, you will need:

* AWS credentials with access to EC2 and S3
* A head node of size `r6i.2xlarge`
* 40 empty Amazon S3 buckets (you can use the [Terraform template](https://github.com/exoshuffle/cloudsort/tree/master/scripts/config/terraform/aws-s3-template) to create them)

The easiest way to setup the head node is to launch it with the provide image [raysort-worker-20230108](https://us-west-2.console.aws.amazon.com/ec2/v2/home?region=us-west-2#ImageDetails:imageId=ami-0da946239520bf5d7). Alternatively, install Python 3.9.13 with Anaconda, then run:

```bash
pip install -Ur requirements/dev.txt
pip install -Ur requirements/worker.txt
pip install -e .
pushd cloudsort/sortlib && python setup.py build_ext --inplace && popd
scripts/installers/install_binaries.sh
```

Edit `.envrc` and change `USER` and `S3_BUCKET` to your own. Set up [direnv](https://direnv.net/) so that the `.envrc` files are sourced automatically when you `cd` into a directory. Otherwise, manually `source .envrc`.

## Starting up a Cluster

The easiest way to start up a cluster of worker nodes is by using the [cls.py](https://github.com/exoshuffle/cloudsort/blob/master/scripts/cls.py) script, which launches VMs using Terraform and sets them up using Ansible. Some values are hardcoded for our experiments, but generally it should run with few changes. If something does not work, file an issue.

1. Install Terraform: `scripts/installers/install_terraform.sh`
2. Run `export CONFIG=2tb-2gb-i4i4x-s3 && python scripts/cls.py up --ray` to launch a Ray cluster
3. Run a test run on the cluster: `python cloudsort/main.py 2>&1 | tee main.log`

The `2tb-2gb-i4i4x-s3` config launches 10 `i4i.4xlarge` nodes, and runs a 1TB sort with 2GB partitions using 10 S3 buckets for I/O. The expected sorting time is around 400 seconds.

## Running the 100TB Benchmark

To run the 100TB CloudSort benchmark, use the following command:

```bash
export STEPS= && export CONFIG=100tb-2gb-i4i4x-s3 && python scripts/cls.py up --ray && python cloudsort/main.py 2>&1 | tee main.log
```

If `STEPS` is empty, the program will run all three steps: generate input, sort, and validate output. You can also specify the steps to run, e.g. `STEPS=sort,validate_output`. The expected sorting time is around 5400 seconds.

You can get runtime metrics using Prometheus and Grafana.

## Cluster Management

`scripts/cls.py` is the centralized place for cluster management code.

- `python scripts/cls.py up` launches a cluster via Terraform and configures it via Ansible. Add `--ray` or `--yarn` to start a Ray or a YARN cluster.
- `python scripts/cls.py setup` skips Terraform and only runs Ansible for software setup. Add `--ray` or `--yarn` to start a Ray or a YARN cluster.
- `python scripts/cls.py down` terminates the cluster via Terraform. Tip: when you're done for the day, run `python scripts/cls.py down && sudo shutdown -h now` to terminate the cluster and stop your head node.
- `python scripts/cls.py start/stop/reboot` calls the AWS CLI tool to start/stop/reboot all your machines in the cluster. Useful when you want to stop the cluster but not terminate the machines.
