"""Execute workflow steps for a job."""

from __future__ import annotations

import logging
import re
import uuid
from pathlib import Path
from typing import Any, TYPE_CHECKING, Callable
from dataclasses import dataclass

if TYPE_CHECKING:
    from realityscan_sdk.client import RealityScanClient

from rslogic.client.file_ops import FileExecutor
from rslogic.common.schemas import Step

_LOGGER = logging.getLogger("rslogic.client.executor")


SDK_SESSION_ACTIONS = frozenset({"sdk_project_create", "sdk_project_open"})


@dataclass(frozen=True)
class StepExecutionResult:
    """Typed step execution result used by the runtime."""

    value: Any
    task_ids: list[str]


def _candidates_for_method(method_name: str) -> list[str]:
    stripped = method_name.strip().lower()
    camel = "".join(part.title() for part in stripped.split("_"))
    candidates = [stripped]
    if camel:
        candidates.append(camel[0].lower() + camel[1:])
    if "_" in stripped:
        compact = stripped.replace("_", "")
        if compact:
            candidates.append(compact)
    return list(dict.fromkeys(candidates))


def _sdk_method(client: RealityScanClient, action: str):
    mapping = {
        "sdk_node_connect_user": lambda **_: client.node.connect_user(),
        "sdk_node_disconnect_user": lambda **_: client.node.disconnect_user(),
        "sdk_project_create": lambda **_: client.project.create(),
        "sdk_project_open": lambda **kwargs: client.project.open(kwargs.get("guid"), name=kwargs.get("name")),
        "sdk_project_close": lambda **_: client.project.close(),
        "sdk_project_disconnect": lambda **_: client.project.disconnect(),
        "sdk_project_save": lambda **kwargs: client.project.save(kwargs.get("path")),
        "sdk_project_command": lambda **kwargs: client.project.command(
            kwargs.get("name", ""),
            params=kwargs.get("params"),
            conditional_tag=kwargs.get("conditional_tag"),
            use_post=bool(kwargs.get("use_post", False)),
            encoded=kwargs.get("encoded"),
            post_body=kwargs.get("post_body"),
        ),
        "sdk_project_command_group": lambda **kwargs: client.project.command_group(kwargs.get("command_calls", {})),
        "sdk_project_new_scene": lambda **_: client.project.new_scene(),
        "sdk_project_status": lambda **_: client.project.status(),
    }
    if action in mapping:
        return mapping[action]
    if not action.startswith("sdk_"):
        return None
    body = action[4:]
    if body.startswith("project_"):
        method = body[len("project_"):]
        for candidate in _candidates_for_method(method):
            if hasattr(client.project, candidate):
                return getattr(client.project, candidate)
        return None
    if body.startswith("node_"):
        method = body[len("node_"):]
        for candidate in _candidates_for_method(method):
            if hasattr(client.node, candidate):
                return getattr(client.node, candidate)
        return None
    return None


def _normalize_sdk_params(method: Any, params: dict[str, Any]) -> dict[str, Any]:
    return dict(params)


def _relative_target_path(base_dir: Path, relative_dir: Any) -> Path:
    if relative_dir is None:
        return base_dir
    text = str(relative_dir).strip()
    if not text:
        return base_dir
    target = Path(text)
    if target.is_absolute():
        raise RuntimeError("relative_dir must be a relative path beneath the session data root")
    if any(part == ".." for part in target.parts):
        raise RuntimeError("relative_dir must not escape the session data root")
    return base_dir / target


class StepExecutor:
    def __init__(
        self,
        sdk_client: object | None,
        file_executor: FileExecutor | None = None,
        *,
        initial_session: str | None = None,
        on_session_update: Callable[[str | None], None] | None = None,
    ):
        self.sdk_client = sdk_client
        self.file_executor = file_executor
        self._session: str | None = None
        self._on_session_update = on_session_update
        self._staging_dir: Path | None = None
        self._context: dict[str, str] = {}
        if initial_session:
            self._set_context_session(initial_session)

    def begin_job(self, job_id: str, *, group_id: str | None = None) -> None:
        self._staging_dir = None
        self._context = {
            "job_id": job_id,
        }
        self._sync_session_context()
        if group_id:
            self._context["group_id"] = group_id
        if self.file_executor is not None:
            self._context.update(
                {
                    "working_root": str(self.file_executor.working_root),
                    "staging_root": str(self.file_executor.staging_root),
                }
            )

    def _set_context_session(self, session: str | None) -> None:
        if self._on_session_update is not None:
            self._on_session_update(session)
        self._session = session
        if not session:
            self._context.pop("session", None)
            self._context.pop("session_data_dir", None)
            return
        self._context["session"] = session
        if self.file_executor is not None:
            self._context["session_data_dir"] = str(
                self.file_executor.working_root / "sessions" / session / "_data"
            )

    def _sync_session_context(self) -> None:
        if not self._session:
            self._context.pop("session", None)
            self._context.pop("session_data_dir", None)
            return
        self._context["session"] = self._session
        if self.file_executor is not None:
            self._context["session_data_dir"] = str(
                self.file_executor.working_root / "sessions" / self._session / "_data"
            )

    @staticmethod
    def _normalize_sdk_params(method: Any, params: dict[str, Any]) -> dict[str, Any]:
        return _normalize_sdk_params(method, params)

    @staticmethod
    def is_session_action(action: str) -> bool:
        return action in SDK_SESSION_ACTIONS

    @staticmethod
    def _render_text_template(value: str, context: dict[str, str]) -> str:
        def replace(match: re.Match[str]) -> str:
            key = match.group(1)
            if key in context:
                return str(context[key])
            return match.group(0)

        return re.sub(r"{([a-zA-Z0-9_]+)}", replace, value)

    def _render(self, value: Any) -> Any:
        if isinstance(value, str):
            return self._render_text_template(value, self._context)
        if isinstance(value, list):
            return [self._render(item) for item in value]
        if isinstance(value, tuple):
            return tuple(self._render(item) for item in value)
        if isinstance(value, dict):
            return {key: self._render(item) for key, item in value.items()}
        return value

    def end_job(self, job_id: str) -> None:
        self._staging_dir = None
        self._context = {"job_id": job_id}
        self._sync_session_context()

    def execute(self, step: Step, *, job_id: str, group_id: str | None = None) -> StepExecutionResult:
        action = step.action
        kind = step.kind
        params = self._render(dict(step.params or {}))
        _LOGGER.debug("execute_step kind=%s action=%s job_id=%s group_id=%s params=%s", kind, action, job_id, group_id, params)
        if group_id:
            self._context["group_id"] = group_id

        if kind == "file":
            if self.file_executor is None:
                raise RuntimeError("file executor not configured")

            if action == "stage":
                if not group_id:
                    raise RuntimeError("group_id required for stage action")
                stage_dir = self.file_executor.stage_group(group_id, job_id)
                _LOGGER.info("stage complete job_id=%s group_id=%s staging_dir=%s", job_id, group_id, stage_dir)
                self._staging_dir = stage_dir
                self._context["staging_dir"] = str(stage_dir)
                return StepExecutionResult(value=str(stage_dir), task_ids=[])

            if action == "file_write_manifest":
                if not group_id:
                    raise RuntimeError("group_id required for file_write_manifest")
                staging = self._staging_dir
                if staging is None:
                    raise RuntimeError("staging directory not known; run stage before file_write_manifest")
                manifest = self.file_executor.write_manifest(job_id, staging, group_id)
                _LOGGER.info("file_write_manifest complete job_id=%s staging_dir=%s manifest=%s", job_id, staging, manifest)
                return StepExecutionResult(value=str(manifest), task_ids=[])

            if action == "file_copy_staging_to_session":
                if not group_id:
                    raise RuntimeError("group_id required for file_copy_staging_to_session")
                staging = self._staging_dir
                if staging is None:
                    raise RuntimeError("staging directory not known; run stage before file_copy_staging_to_session")
                if not staging.exists():
                    raise RuntimeError(f"staging directory does not exist: {staging}")
                base_dir = self._context.get("session_data_dir")
                if not base_dir:
                    raise RuntimeError("session data directory not known; run sdk_project_create before copying staged files")
                session_dir = _relative_target_path(Path(base_dir), params.get("relative_dir"))
                result = str(self.file_executor.copy_staging_to_session(job_id, staging, session_dir, group_id))
                _LOGGER.info(
                    "file_copy_staging_to_session complete job_id=%s staging=%s destination=%s",
                    job_id,
                    staging,
                    session_dir,
                )
                return StepExecutionResult(value=result, task_ids=[])

            raise RuntimeError(f"unsupported file action {action}")

        if kind != "sdk":
            raise RuntimeError(f"unsupported kind {kind}")

        if self.sdk_client is None:
            raise RuntimeError(
                "SDK command requested but realityscan_sdk is not available in this runtime. "
                "Install the sdk package or remove sdk steps from this job."
            )

        method = _sdk_method(self.sdk_client, action)
        if method is None:
            raise RuntimeError(f"unsupported action {action}")

        params = self._normalize_sdk_params(method, params)
        _LOGGER.debug("sdk call action=%s method=%s params=%s", action, getattr(method, "__name__", str(method)), params)
        try:
            result = method(**params)
            task_ids = self._extract_task_ids(result)
            _LOGGER.info(
                "sdk call complete action=%s result=%s task_ids=%s",
                action,
                self._short_repr(result),
                task_ids,
            )
            if action in {"sdk_project_create", "sdk_project_open"} and isinstance(result, str):
                self._set_context_session(result)
            if action in {"sdk_project_close", "sdk_project_disconnect", "sdk_project_delete"}:
                self._set_context_session(None)
            return StepExecutionResult(value=result, task_ids=task_ids)
        except TypeError:
            if params:
                _LOGGER.exception("sdk call type error action=%s params=%s", action, params)
                raise
            result = method()
            task_ids = self._extract_task_ids(result)
            _LOGGER.info(
                "sdk call complete action=%s result=%s task_ids=%s",
                action,
                self._short_repr(result),
                task_ids,
            )
            if action in {"sdk_project_create", "sdk_project_open"} and isinstance(result, str):
                self._set_context_session(result)
            if action in {"sdk_project_close", "sdk_project_disconnect", "sdk_project_delete"}:
                self._set_context_session(None)
            return StepExecutionResult(value=result, task_ids=task_ids)

    @staticmethod
    def _extract_task_ids(result: Any) -> list[str]:
        task_ids: list[str] = []

        def add(candidate: Any) -> None:
            if candidate is None:
                return

            task_id = candidate
            if isinstance(candidate, dict):
                task_id = candidate.get("taskID") or candidate.get("taskId") or candidate.get("id")
            else:
                if hasattr(candidate, "taskID"):
                    task_id = getattr(candidate, "taskID")
                elif hasattr(candidate, "taskId"):
                    task_id = getattr(candidate, "taskId")
            if task_id is None:
                return

            task_id_text = str(task_id).strip()
            if not task_id_text:
                return

            try:
                parsed = uuid.UUID(task_id_text)
            except ValueError:
                return

            normalized = str(parsed)
            if normalized and normalized not in task_ids:
                task_ids.append(normalized)

        if isinstance(result, (list, tuple, set)):
            for item in result:
                add(item)
            return task_ids
        if isinstance(result, dict):
            add(result)
            return task_ids
        if isinstance(result, str):
            _LOGGER.debug("ignoring scalar string result for task extraction action may be session/result text: %s", result)
            return task_ids
        add(result)
        return task_ids

    @staticmethod
    def _short_repr(value: Any, *, max_len: int = 400) -> str:
        text = repr(value)
        if len(text) <= max_len:
            return text
        return f"{text[:max_len]}…(+{len(text)-max_len} chars)"

    def current_session(self) -> str | None:
        return self._session

    def context(self) -> dict[str, str]:
        return dict(self._context)
