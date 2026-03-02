"""Execute workflow steps for a job."""

from __future__ import annotations

from pathlib import Path

from realityscan_sdk.client import RealityScanClient

from rslogic.client.file_ops import FileExecutor
from rslogic.common.schemas import Step


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


class StepExecutor:
    def __init__(self, sdk_client: RealityScanClient, file_executor: FileExecutor | None = None):
        self.sdk_client = sdk_client
        self.file_executor = file_executor
        self._staging_dir: Path | None = None

    def begin_job(self, job_id: str) -> None:
        self._staging_dir = None

    def end_job(self, job_id: str) -> None:
        self._staging_dir = None

    def execute(self, step: Step, *, job_id: str, group_id: str | None = None) -> str:
        action = step.action
        kind = step.kind
        params = dict(step.params or {})

        if kind == "file":
            if self.file_executor is None:
                raise RuntimeError("file executor not configured")

            if action in {"stage", "file_stage", "file_stage_group"}:
                if not group_id:
                    raise RuntimeError("group_id required for file_stage action")
                stage_dir = self.file_executor.stage_group(group_id, job_id)
                self._staging_dir = stage_dir
                return str(stage_dir)

            if action == "file_write_manifest":
                if not group_id:
                    raise RuntimeError("group_id required for file_write_manifest")
                staging = self._staging_dir
                if staging is None:
                    staging = self.file_executor.stage_group(group_id, job_id)
                    self._staging_dir = staging
                manifest = self.file_executor.write_manifest(job_id, staging, group_id)
                return str(manifest)

            if action in {"file_move_staging_to_working", "file_move_to_working", "file_import_to_working"}:
                staging = self._staging_dir
                if staging is None:
                    staging = self.file_executor.staging_root / str(job_id)
                if not staging.exists():
                    raise RuntimeError(f"staging directory does not exist: {staging}")
                working_dir = params.get("working_dir")
                if working_dir is not None:
                    working_dir = Path(str(working_dir))
                return str(self.file_executor.move_staging_to_working(job_id, staging, working_dir))

            raise RuntimeError(f"unsupported file action {action}")

        if kind != "sdk":
            raise RuntimeError(f"unsupported kind {kind}")

        method = _sdk_method(self.sdk_client, action)
        if method is None:
            raise RuntimeError(f"unsupported action {action}")
        try:
            return str(method(**params))
        except TypeError:
            if params:
                raise
            return str(method())
