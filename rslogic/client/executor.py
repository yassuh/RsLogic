"""Execute workflow steps for a job."""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from realityscan_sdk.client import RealityScanClient

from rslogic.client.file_ops import FileExecutor
from rslogic.common.schemas import Step

_LOGGER = logging.getLogger("rslogic.client.executor")


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
        "sdk_project_commandgroup": lambda **kwargs: client.project.command_group(kwargs.get("command_calls", {})),
        "sdk_project_new_scene": lambda **_: client.project.new_scene(),
        "sdk_new_scene": lambda **_: client.project.new_scene(),
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
    if body.startswith("new_scene"):
        return client.project.new_scene
    return None


def _normalize_sdk_params(method: Any, params: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(params)
    if method.__name__ == "add_folder" and "path" in normalized and "folder_path" not in normalized:
        normalized["folder_path"] = normalized.pop("path")
    return normalized


class StepExecutor:
    def __init__(self, sdk_client: object | None, file_executor: FileExecutor | None = None):
        self.sdk_client = sdk_client
        self.file_executor = file_executor
        self._staging_dir: Path | None = None
        self._context: dict[str, str] = {}

    def begin_job(self, job_id: str, *, group_id: str | None = None) -> None:
        self._staging_dir = None
        self._context = {
            "job_id": job_id,
        }
        if group_id:
            self._context["group_id"] = group_id
        if self.file_executor is not None:
            self._context.update(
                {
                    "working_root": str(self.file_executor.working_root),
                    "working_projects_root": str(self.file_executor.working_projects_root),
                    "staging_root": str(self.file_executor.staging_root),
                }
            )

    def _set_context_session(self, session: str | None) -> None:
        if not session:
            return
        self._context["session"] = session
        if self.file_executor is not None:
            self._context["session_data_dir"] = str(
                self.file_executor.working_root / "sessions" / session / "_data"
            )

    @staticmethod
    def _normalize_sdk_params(method: Any, params: dict[str, Any]) -> dict[str, Any]:
        return _normalize_sdk_params(method, params)

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

    def execute(self, step: Step, *, job_id: str, group_id: str | None = None) -> str:
        action = step.action
        kind = step.kind
        params = self._render(dict(step.params or {}))
        _LOGGER.debug("execute_step kind=%s action=%s job_id=%s group_id=%s params=%s", kind, action, job_id, group_id, params)
        if group_id:
            self._context["group_id"] = group_id

        if kind == "file":
            if self.file_executor is None:
                raise RuntimeError("file executor not configured")

            if action in {"stage", "file_stage", "file_stage_group"}:
                if not group_id:
                    raise RuntimeError("group_id required for file_stage action")
                stage_dir = self.file_executor.stage_group(group_id, job_id)
                _LOGGER.info("file_stage complete job_id=%s group_id=%s staging_dir=%s", job_id, group_id, stage_dir)
                self._staging_dir = stage_dir
                self._context["staging_dir"] = str(stage_dir)
                return str(stage_dir)

            if action == "file_write_manifest":
                if not group_id:
                    raise RuntimeError("group_id required for file_write_manifest")
                staging = self._staging_dir
                if staging is None:
                    staging = self.file_executor.stage_group(group_id, job_id)
                    self._staging_dir = staging
                manifest = self.file_executor.write_manifest(job_id, staging, group_id)
                _LOGGER.info("file_write_manifest complete job_id=%s staging_dir=%s manifest=%s", job_id, staging, manifest)
                return str(manifest)

            if action in {"file_move_staging_to_working", "file_move_to_working", "file_import_to_working"}:
                staging = self._staging_dir
                if staging is None:
                    staging = self.file_executor.staging_root
                if not staging.exists():
                    raise RuntimeError(f"staging directory does not exist: {staging}")
                working_dir = params.get("working_dir")
                if working_dir is None:
                    working_dir = self.file_executor.working_projects_root / str(job_id)
                else:
                    working_dir = Path(str(working_dir))
                result = str(self.file_executor.move_staging_to_working(job_id, staging, working_dir))
                _LOGGER.info(
                    "file_move_staging_to_working complete job_id=%s staging=%s destination=%s",
                    job_id,
                    staging,
                    working_dir,
                )
                return result

            if action in {"file_move_to_session_imagery", "file_move_staging_to_session_imagery", "file_move_to_session_folder"}:
                staging = self._staging_dir
                if staging is None:
                    staging = self.file_executor.staging_root
                if not staging.exists():
                    raise RuntimeError(f"staging directory does not exist: {staging}")

                session = self._context.get("session")
                if not session:
                    raise RuntimeError("session is not available; run sdk_project_create before moving files into session imagery")

                working_dir = params.get("working_dir")
                if working_dir is None:
                    base_dir = self._context.get("session_data_dir")
                    if not base_dir:
                        raise RuntimeError("session data directory not known")
                    working_dir = Path(base_dir) / "Imagery"
                else:
                    working_dir = Path(str(working_dir))
                result = str(self.file_executor.move_staging_to_working(job_id, staging, working_dir))
                _LOGGER.info(
                    "file_move_to_session_imagery complete job_id=%s group_id=%s session=%s destination=%s",
                    job_id,
                    group_id,
                    session,
                    working_dir,
                )
                return result

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
            _LOGGER.info("sdk call complete action=%s result=%s", action, result)
            if action in {"sdk_project_create", "sdk_project_open"} and isinstance(result, str):
                self._set_context_session(result)
            if action in {"sdk_project_close", "sdk_project_disconnect", "sdk_project_delete"}:
                self._context.pop("session", None)
                self._context.pop("session_data_dir", None)
            return str(result)
        except TypeError:
            if params:
                _LOGGER.exception("sdk call type error action=%s params=%s", action, params)
                raise
            return str(method())
