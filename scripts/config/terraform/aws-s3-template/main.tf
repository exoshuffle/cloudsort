terraform {
  required_version = ">= 1.1"
  required_providers {
    azurerm = {
      source  = "hashicorp/aws"
      version = "~> 3.0"
    }
  }
}

provider "aws" {
  region = "us-west-2"
}

resource "aws_s3_bucket" "bucket" {
  count         = var.bucket_count
  bucket        = "${var.bucket_prefix}-${format("%03d", count.index)}"
  force_destroy = true
}

resource "aws_s3_bucket_lifecycle_configuration" "example" {
  count  = var.bucket_count
  bucket = aws_s3_bucket.bucket[count.index].id

  rule {
    id = "Delete objects after 1 day"
    expiration {
      days = 1
    }
    status = "Enabled"
  }
}