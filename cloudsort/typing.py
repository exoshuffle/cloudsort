import dataclasses
import enum
from typing import List, Optional, Tuple  # pylint: disable=import-self

ByteCount = int
PartId = int
Path = str
RecordCount = int

BlockInfo = Tuple[int, int]


class AppStep(enum.Enum):
    GENERATE_INPUT = "generate_input"
    SORT = "sort"
    VALIDATE_OUTPUT = "validate_output"


@dataclasses.dataclass
class PartInfo:
    part_id: int
    node: Optional[str]
    bucket: Optional[str]
    path: Path
    size: ByteCount
    checksum: Optional[str]

    def __repr__(self):
        ret = ""
        if self.node:
            ret += f"{self.node}:"
        if self.bucket:
            ret += f"{self.bucket}:"
        ret += self.path
        ret += f"(size={self.size},checksum={self.checksum})"
        return ret

    def to_csv_row(self) -> List[str]:
        return [
            f"{self.part_id:010x}",
            self.node or "",
            self.bucket or "",
            self.path,
            str(self.size),
            self.checksum or "",
        ]

    @classmethod
    def from_csv_row(cls, row: List[str]) -> "PartInfo":
        return cls(int(row[0], 16), row[1], row[2], row[3], int(row[4]), row[5])


class SpillingMode(enum.Enum):
    RAY = "ray"
    DISK = "disk"
    S3 = "s3"


class InstanceLifetime(enum.Enum):
    DEDICATED = "dedicated"
    SPOT = "spot"
