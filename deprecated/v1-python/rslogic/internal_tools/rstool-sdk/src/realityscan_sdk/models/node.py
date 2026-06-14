from dataclasses import dataclass
from typing import Any

@dataclass(frozen=True)
class RSNodeConnectionInfo:
    protocol: str
    hostAddress: str
    port: int
    authToken: str
    pairingPage: str
    landingPage: str
    allAddresses: list[str]

    @classmethod

    def from_json(cls, data: dict[str, Any]) -> "RSNodeConnectionInfo":
        return cls(
            protocol=str(data.get("protocol", "")),
            hostAddress=str(data.get("hostAddress", "")),
            port=int(data.get("port", 0)),
            authToken=str(data.get("authToken", "")),
            pairingPage=str(data.get("pairingPage", "")),
            landingPage=str(data.get("landingPage", "")),
            allAddresses=list(data.get("allAddresses", [])),
        )
    @property
    def base_url(self) -> str:
        return f"{self.protocol}://{self.hostAddress}:{self.port}"
@dataclass(frozen=True)
class RSProjectInformation:
    name: str
    guid: str
    timeStamp: str
    @classmethod
    def from_json(cls, data: dict[str, Any]) -> "RSProjectInformation":
        return cls(
            name=str(data.get("name", "")),
            guid=str(data.get("guid", "")),
            timeStamp=str(data.get("timeStamp", 0)),
        )
@dataclass(frozen=True)
class RSNodeStatus:
    status: str
    apiVersion: str
    activeSessions: int
    maxSessions: int
    sessionIds: list[str]
    @classmethod
    def from_json(cls, data: dict[str, Any]) -> "RSNodeStatus":
        return cls(
            status=str(data.get("status", "")),
            apiVersion=str(data.get("apiVersion", "")),
            activeSessions=int(data.get("activeSessions", 0)),
            maxSessions=int(data.get("maxSessions", 0)),
            sessionIds=list(data.get("sessionIds", [])),
        )
    def __str__(self) -> str:
        return (
            f"RSNodeStatus(status={self.status}, apiVersion={self.apiVersion}, "
            f"activeSessions={self.activeSessions}, maxSessions={self.maxSessions}, "
            f"  sessionIds={self.sessionIds})"
        )