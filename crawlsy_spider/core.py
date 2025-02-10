from enum import Enum


class Status(Enum):
    RUNNING = "RUNNING"
    FAILED = "FAILED"
    SUCCEED = "succeed"
    PENDING = "pending"