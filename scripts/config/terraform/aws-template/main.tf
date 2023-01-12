terraform {
  required_version = ">= 1.1"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 3.0"
    }
  }
}

provider "aws" {
  region = "us-west-2"
}

resource "aws_instance" "cluster" {
  count = var.instance_count

  ami                         = "ami-0c5cce1d70efb41f5" # raysort-worker-20230111
  instance_type               = var.instance_type
  key_name                    = "login-us-west-2"
  associate_public_ip_address = true
  # subnet_id                   = "subnet-084e54bf496121333" # exoshuffle-subnet-public1-us-west-2a
  # subnet_id                   = "subnet-0af4497f8b2bffeaa" # exoshuffle-subnet-public1-us-west-2b
  subnet_id = "subnet-033a4bd05bcc8c21e" # exoshuffle-subnet-public1-us-west-2c
  # subnet_id                   = "subnet-05b20e1a041f5fd44" # exoshuffle-subnet-public2-us-west-2d

  root_block_device {
    volume_size = var.instance_disk_gb
  }

  tags = {
    ClusterName = var.cluster_name
    Name        = "${var.cluster_name}-${format("%03d", count.index)}"
  }
}
