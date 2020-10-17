import os

__DIR__ = os.path.dirname(os.path.abspath(__file__))

# Basics
RECORD_SIZE = 100

# Each record is 100 bytes. Official requirement is 10^12 records (100 TiB).
TOTAL_NUM_RECORDS = 1000 * 10  # 1 MiB
# TOTAL_NUM_RECORDS = 1000 * 1000 * 10  # 1 GiB

# Cluster config
NUM_MAPPERS = 1
NUM_REDUCERS = 1

# Executable locations
GENSORT_PATH = os.path.join(__DIR__, "../gensort/64/gensort")
VALSORT_PATH = os.path.join(__DIR__, "../gensort/64/valsort")
DATA_DIR = {"input": "/var/tmp/raysort/input/", "output": "/var/tmp/raysort/output/"}
FILENAME_FMT = {"input": "input-{part_id}", "output": "output-{part_id}"}

# AWS S3 config
USE_S3 = True
S3_REGION = "us-west-2"
S3_BUCKET = "raysort-debug"
OBJECT_KEY_FMT = {"input": "input-{part_id}", "output": "output-{part_id}"}
