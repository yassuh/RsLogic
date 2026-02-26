from dataclasses import dataclass
from typing import Any, Optional, Union, Iterable, Dict
import httpx
from .resources.node import NodeAPI
from .resources.project import ProjectAPI

Headers = Dict[str, str]
Params = Dict[str, Any]

@dataclass
class ClientConfig:
    base_url: str
    client_id: str
    app_token: str
    timeout_s: float = 30.0
    verify_tls: bool = True
    user_agent: str = "RealityScanSDK"

class RealityScanClient:
    def __init__(self, base_url: str, auth_token: str, client_id: str, app_token: str, timeout_s: float = 30.0, session: Optional[str] = None,
                 verify_tls: bool = True, user_agent: str = "RealityScanSDK", http: Optional[httpx.Client] = None) -> None:
        self.config = ClientConfig(
            base_url=base_url,
            client_id=client_id,
            app_token=app_token,
            timeout_s=timeout_s,
            verify_tls=verify_tls,
            user_agent=user_agent
        )
        self.auth_token = auth_token
        self.session: Optional[str] = session
        self.http = http or httpx.Client(
            base_url=self.config.base_url,
            timeout=self.config.timeout_s,
            verify=self.config.verify_tls,
            headers={
                "User-Agent": self.config.user_agent,
            }
        )
        self._owns_http = http is None

        # Resource Groups
        self.project = ProjectAPI(self)
        self.node = NodeAPI(self)

    def close(self) -> None:
        if self._owns_http:
            self.http.close()
    def __enter__(self) -> "RealityScanClient":
        return self
    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def _base_headers(self, *, require_session: bool) -> Headers:
        h: Headers = {
            "clientId": self.config.client_id,
            "appToken": self.config.app_token,
            "Authorization": f"Bearer {self.auth_token}",
        }
        if require_session:
            if not self.session:
                raise ValueError("This call requires a Session header, but client.session is not set."
                                 "Call client.project.create or client.project.open first.")
            h["Session"] = self.session
        return h
    
    def _request(self, method: str, path: str, *, require_session: bool = True,
                 params: Optional[dict] = None, json: Optional[dict] = None,
                 content: Optional[Union[bytes, str]] = None, extra_headers: Optional[Headers] = None,
                 stream: bool = False) -> Any:
        headers = self._base_headers(require_session=require_session)
        if extra_headers:
            headers.update(extra_headers)
        try:
            response = self.http.request(
                method,
                path,
                headers=headers,
                params=params,
                json=json,
                content=content,
                #stream=stream
            )
        except httpx.HTTPError as e:
            raise RuntimeError(f"HTTP request failed: {e}") from e
        
        if response.status_code >= 400:
            # Try to get error message from body
            msg = f"HTTP {response.status_code} {response.reason_phrase}"
            try:
                if "application/json" in (response.headers.get("Content-Type") or ""):
                    data = response.json()
                    msg += f": {data}"
                else:
                    text = response.text
                    if text:
                        msg += f": {text[:1000]}"
            except Exception:
                pass
            raise RuntimeError(msg)

        session_header = response.headers.get("Session")
        if session_header:
            self.session = session_header
        
        if 200 <= response.status_code < 300:
            ctype = (response.headers.get("Content-Type") or "").lower()
            if "application/json" in ctype:
                return response.json()
            if "text/" in ctype:
                return response.text
            return response.content
    @staticmethod

    def _array_params(key: str, values: Optional[Iterable[str]]) -> Params:
        """
        RealityScan docs show query params like taskIds=array[UUID].
        httpx will encode list values as repeated keys: ?taskIds=a&taskIds=b
        """
        if not values:
            return {}
        return {key: list(values)}

#from realityscan_sdk import RealityScanClient
