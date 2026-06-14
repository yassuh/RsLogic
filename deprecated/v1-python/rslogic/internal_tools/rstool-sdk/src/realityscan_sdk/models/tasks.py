
from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass
class TaskHandle:
    taskID: str

    @classmethod
    def from_json(cls, d: Dict[str, Any]) -> "TaskHandle":
        return cls(taskID=str(d["taskID"]))


@dataclass
class TaskStatus:
    taskID: str
    timeStart: Optional[int]
    timeEnd: Optional[int]
    state: str
    errorCode: int
    errorMessage: str

    @classmethod
    def from_json(cls, d: Dict[str, Any]) -> "TaskStatus":
        return cls(
            taskID=str(d.get("taskID", "")),
            timeStart=d.get("timeStart"),
            timeEnd=d.get("timeEnd"),
            state=str(d.get("state", "")),
            errorCode=int(d.get("errorCode", 0)),
            errorMessage=str(d.get("errorMessage", "")),
        )
