from typing import TYPE_CHECKING, List

from ..models.node import RSNodeConnectionInfo, RSProjectInformation, RSNodeStatus

if TYPE_CHECKING:
    from ..client import RealityScanClient


class NodeAPI:
    def __init__(self, client: "RealityScanClient") -> None:
        self._c = client

    def connect_user(self) -> RSNodeConnectionInfo:
        """
        GET /node/connectuser
        Bootstraps auth via /node/connection (unauthenticated, localhost-only),
        stores the token, then reserves a session slot via /node/connectuser.
        """
        conn_data = self._c._request("GET", "/node/connection", require_session=False)
        conn_info = RSNodeConnectionInfo.from_json(conn_data)
        if conn_info.authToken:
            self._c.auth_token = conn_info.authToken
        self._c._request("GET", "/node/connectuser", require_session=False)
        return conn_info

    # Back-compat for older naming
    connectuser = connect_user

    def connection(self) -> RSNodeConnectionInfo:
        """GET /node/connection"""
        data = self._c._request("GET", "/node/connection", require_session=False)
        return RSNodeConnectionInfo.from_json(data)

    def disconnect_user(self) -> None:
        """GET /node/disconnectuser"""
        self._c._request("GET", "/node/disconnectuser", require_session=False)

    def projects(self) -> List[RSProjectInformation]:
        """GET /node/projects"""
        data = self._c._request("GET", "/node/projects", require_session=False)
        return [RSProjectInformation.from_json(x) for x in data]

    def status(self) -> RSNodeStatus:
        """GET /node/status"""
        data = self._c._request("GET", "/node/status", require_session=False)
        print(data)
        return RSNodeStatus.from_json(data)
