
from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass
class RSProjectStatus:
    restarted: bool
    progress: float
    timeTotal: float
    timeEstimation: float
    errorCode: int
    changeCounter: int
    processID: int

    @classmethod
    def from_json(cls, d: Dict[str, Any]) -> "RSProjectStatus":
        return cls(
            restarted=bool(d.get("restarted", False)),
            progress=float(d.get("progress", 0.0)),
            timeTotal=float(d.get("timeTotal", 0.0)),
            timeEstimation=float(d.get("timeEstimation", 0.0)),
            errorCode=int(d.get("errorCode", 0)),
            changeCounter=int(d.get("changeCounter", 0)),
            processID=int(d.get("processID", 0)),
        )
