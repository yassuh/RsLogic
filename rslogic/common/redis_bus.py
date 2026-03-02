"""Redis queue helper for orchestrator commands and client results."""

from __future__ import annotations

import logging
import json
import time
from typing import Any

import redis

_LOGGER = logging.getLogger("rslogic.common.redis_bus")


class RedisBus:
    def __init__(self, url: str, command_queue_key: str, result_queue_key: str) -> None:
        self._redis = redis.from_url(url, decode_responses=True)
        self._command_queue = command_queue_key
        self._result_queue = result_queue_key
        self._heartbeat_prefix = "rslogic:clients"

    def _command_key(self, client_id: str) -> str:
        if "{client_id}" in self._command_queue:
            return self._command_queue.format(client_id=client_id)
        return f"{self._command_queue}:{client_id}:jobs"

    def _result_keys(self, client_id: str | None = None) -> list[str]:
        keys = [self._result_queue]
        if client_id:
            if "{client_id}" in self._result_queue:
                keys.append(self._result_queue.format(client_id=client_id))
            else:
                keys.append(f"{self._result_queue}:{client_id}")
            return list(dict.fromkeys([k for k in keys if k]))

        if "{client_id}" in self._result_queue:
            template = self._result_queue.replace("{client_id}", "*")
            keys.extend(self._redis.keys(template))
        # de-duplicate while preserving order
        return list(dict.fromkeys([k for k in keys if k]))

    def _heartbeat_key(self, client_id: str) -> str:
        return f"{self._heartbeat_prefix}:{client_id}:heartbeat"

    @staticmethod
    def _parse_client_from_heartbeat_key(key: str) -> str | None:
        pieces = key.split(":")
        if len(pieces) < 3:
            return None
        if pieces[0] != "rslogic" or pieces[1] != "clients":
            return None
        return pieces[2]

    def list_active_clients(self) -> list[str]:
        keys = self._redis.keys(f"{self._heartbeat_prefix}:*:heartbeat")
        clients = [cid for raw in keys if (cid := self._parse_client_from_heartbeat_key(raw))]
        clients.sort()
        return clients

    def publish_command(self, client_id: str, payload: dict[str, Any]) -> None:
        _LOGGER.debug("publish_command client_id=%s queue=%s payload=%s", client_id, self._command_key(client_id), payload)
        self._redis.lpush(self._command_key(client_id), json.dumps(payload, default=str))

    def pop_command(self, client_id: str, timeout_s: int) -> dict[str, Any] | None:
        _LOGGER.debug("polling command client_id=%s timeout=%s queue=%s", client_id, timeout_s, self._command_key(client_id))
        value = self._redis.brpop([self._command_key(client_id)], timeout=timeout_s)
        if not value:
            return None
        _, raw = value
        if not raw:
            return None
        _LOGGER.debug("command received for client_id=%s raw=%s", client_id, raw)
        return json.loads(raw)

    def publish_result(self, client_id: str, payload: dict[str, Any]) -> None:
        _LOGGER.debug("publish_result client_id=%s payload=%s", client_id, payload)
        payload = {
            "client_id": client_id,
            "timestamp": time.time(),
            **payload,
        }
        result = json.dumps(payload, default=str)
        for key in self._result_keys(client_id):
            self._redis.lpush(key, result)

    def pop_result(self, timeout_s: int = 1) -> dict[str, Any] | None:
        _LOGGER.debug("polling results timeout=%s keys=%s", timeout_s, self._result_keys(None))
        value = self._redis.brpop(self._result_keys(None), timeout=timeout_s)
        if not value:
            return None
        _, raw = value
        if not raw:
            return None
        return json.loads(raw)

    def heartbeat(self, client_id: str, payload: dict[str, Any]) -> None:
        data = {"ts": time.time(), **payload}
        key = self._heartbeat_key(client_id)
        _LOGGER.debug("heartbeat client_id=%s key=%s", client_id, key)
        self._redis.set(key, json.dumps(data, default=str), ex=20)
