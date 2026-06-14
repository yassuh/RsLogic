"""RealityScan job-draft helpers used by the operator TUI."""

from __future__ import annotations

from dataclasses import dataclass, field
import inspect
import json
from pathlib import Path
from typing import Any

from rslogic.common.schemas import JobRequest, Step


DEFAULT_REALITYSCAN_SESSION_STEPS: list[dict[str, Any]] = [
    {"kind": "file", "action": "stage", "params": {}},
    {"kind": "sdk", "action": "sdk_node_connect_user", "params": {}},
    {"kind": "sdk", "action": "sdk_project_create", "params": {}},
    {"kind": "sdk", "action": "sdk_project_new_scene", "params": {}},
    {"kind": "file", "action": "file_copy_staging_to_session", "params": {"relative_dir": "Imagery"}},
]

ALIGN_REALITYSCAN_STEPS: list[dict[str, Any]] = [
    *DEFAULT_REALITYSCAN_SESSION_STEPS,
    {"kind": "sdk", "action": "sdk_project_add_folder", "params": {"folder_path": "Imagery"}},
    {"kind": "sdk", "action": "sdk_project_command", "params": {"name": "align"}, "timeout_s": 0},
    {"kind": "sdk", "action": "sdk_project_save", "params": {"path": "realityscan-job.rspj"}},
]

SAVE_PROJECT_STEPS: list[dict[str, Any]] = [
    {"kind": "sdk", "action": "sdk_project_save", "params": {"path": "realityscan-job.rspj"}},
]

PROJECT_STATUS_STEPS: list[dict[str, Any]] = [
    {"kind": "sdk", "action": "sdk_project_status", "params": {}},
]

ALIGN_ONLY_STEPS: list[dict[str, Any]] = [
    {"kind": "sdk", "action": "sdk_project_add_folder", "params": {"folder_path": "Imagery"}},
    {"kind": "sdk", "action": "sdk_project_command", "params": {"name": "align"}, "timeout_s": 0},
]

_ACTION_MAP = json.loads((Path(__file__).resolve().parents[2] / "job-action-map.json").read_text(encoding="utf-8"))


def _annotation_label(annotation: Any) -> str:
    if annotation is inspect.Signature.empty:
        return "value"
    if isinstance(annotation, type):
        return annotation.__name__
    return str(annotation).replace("typing.", "")


def _sdk_action_entry(method: Any) -> dict[str, Any]:
    signature = inspect.signature(method)
    params: dict[str, str] = {}
    required: list[str] = []
    optional: list[str] = []

    for parameter in signature.parameters.values():
        if parameter.name == "self":
            continue
        if parameter.kind in {inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD}:
            continue
        params[parameter.name] = _annotation_label(parameter.annotation)
        if parameter.default is inspect.Signature.empty:
            required.append(parameter.name)
        else:
            optional.append(parameter.name)

    entry: dict[str, Any] = {}
    doc = (inspect.getdoc(method) or "").strip().splitlines()
    if doc:
        entry["description"] = doc[0].strip()
    if params:
        entry["params"] = params
    if required:
        entry["required_params"] = required
    if optional:
        entry["optional_params"] = optional
    return entry


def _sdk_resource_entries() -> dict[str, dict[str, Any]]:
    try:
        from realityscan_sdk.resources.node import NodeAPI
        from realityscan_sdk.resources.project import ProjectAPI
    except ImportError:
        return {}

    entries: dict[str, dict[str, Any]] = {}
    for prefix, resource in (("sdk_node_", NodeAPI), ("sdk_project_", ProjectAPI)):
        seen: set[int] = set()
        for name, method in inspect.getmembers(resource, inspect.isfunction):
            if name.startswith("_") or name == "__init__":
                continue
            identity = id(method)
            if identity in seen:
                continue
            seen.add(identity)
            entries[f"{prefix}{name}"] = _sdk_action_entry(method)
    return entries


def _merged_action_entries(kind: str) -> dict[str, dict[str, Any]]:
    normalized = kind.strip().lower()
    if normalized == "file":
        return dict(_ACTION_MAP.get("file_steps", {}))

    merged = _sdk_resource_entries()
    merged.update(dict(_ACTION_MAP.get("sdk_steps", {})))
    return merged


@dataclass(frozen=True)
class JobFragment:
    key: str
    label: str
    description: str
    steps: tuple[dict[str, Any], ...]


PREBUILT_JOB_FRAGMENTS: tuple[JobFragment, ...] = (
    JobFragment(
        key="basic_session",
        label="Basic session chain",
        description="Stage imagery, connect the node user, create a project, create a scene, and copy imagery into the session.",
        steps=tuple(DEFAULT_REALITYSCAN_SESSION_STEPS),
    ),
    JobFragment(
        key="align_full",
        label="Full align workflow",
        description="Create a full session, add the Imagery folder, run align, and save the project.",
        steps=tuple(ALIGN_REALITYSCAN_STEPS),
    ),
    JobFragment(
        key="align_only",
        label="Align tail",
        description="Append the add-folder and align commands to an existing open project/session.",
        steps=tuple(ALIGN_ONLY_STEPS),
    ),
    JobFragment(
        key="save_project",
        label="Save project",
        description="Save the current project to `realityscan-job.rspj`.",
        steps=tuple(SAVE_PROJECT_STEPS),
    ),
    JobFragment(
        key="project_status",
        label="Project status",
        description="Fetch the current project status as a single SDK step.",
        steps=tuple(PROJECT_STATUS_STEPS),
    ),
)

_FRAGMENTS_BY_KEY = {fragment.key: fragment for fragment in PREBUILT_JOB_FRAGMENTS}


def _clone_steps(steps: list[dict[str, Any]]) -> list[dict[str, Any]]:
    cloned: list[dict[str, Any]] = []
    for step in steps:
        payload = dict(step)
        params = payload.get("params")
        payload["params"] = dict(params) if isinstance(params, dict) else {}
        cloned.append(payload)
    return cloned


def _parse_json_object(raw: str, *, empty_default: dict[str, Any] | None = None) -> dict[str, Any]:
    text = raw.strip()
    if not text:
        return dict(empty_default or {})
    parsed = json.loads(text)
    if not isinstance(parsed, dict):
        raise ValueError("JSON value must be an object")
    return parsed


def read_workflow_path_or_inline(raw: str) -> list[dict[str, Any]]:
    text = raw.strip().strip('"').strip("'")
    if not text:
        return _clone_steps(DEFAULT_REALITYSCAN_SESSION_STEPS)
    path = Path(text)
    if path.exists():
        payload = json.loads(path.read_text(encoding="utf-8"))
    else:
        payload = json.loads(text)

    if isinstance(payload, dict):
        if "steps" in payload and isinstance(payload["steps"], list):
            return payload["steps"]
        if "steps" in payload:
            raise ValueError("steps must be a list")
        raise ValueError("workflow JSON must be a list of steps or an object with a 'steps' field")
    if not isinstance(payload, list):
        raise ValueError("workflow JSON must be a list")
    return payload


def build_step_payload(
    *,
    kind: str,
    action: str,
    params_raw: str,
    timeout_raw: str,
    display_name: str | None = None,
) -> dict[str, Any]:
    params = _parse_json_object(params_raw, empty_default={})
    timeout_s = 600 if not timeout_raw.strip() else int(timeout_raw.strip())
    step = Step(
        kind=kind,
        action=action,
        params=params,
        timeout_s=timeout_s,
        display_name=(display_name or "").strip() or None,
    )
    return step.model_dump(exclude_none=True)


def fragment_options() -> list[tuple[str, str]]:
    return [(fragment.label, fragment.key) for fragment in PREBUILT_JOB_FRAGMENTS]


def fragment_steps(key: str) -> list[dict[str, Any]]:
    fragment = _FRAGMENTS_BY_KEY.get(key)
    if fragment is None:
        raise KeyError(f"unknown job fragment: {key}")
    return _clone_steps(list(fragment.steps))


def fragment_details(key: str) -> list[str]:
    fragment = _FRAGMENTS_BY_KEY.get(key)
    if fragment is None:
        return [f"fragment={key}", "unknown fragment"]
    actions = ", ".join(str(step.get("action", "-")) for step in fragment.steps)
    return [
        f"fragment={fragment.label}",
        fragment.description,
        f"steps={len(fragment.steps)}",
        f"actions={actions}",
    ]


def _action_entries(kind: str) -> dict[str, dict[str, Any]]:
    return _merged_action_entries(kind)


def action_options(kind: str) -> list[tuple[str, str]]:
    options: list[tuple[str, str]] = []
    for action, entry in sorted(_action_entries(kind).items()):
        detail = str(entry.get("description") or entry.get("method") or "").strip()
        label = action if not detail else f"{action} | {detail}"
        if len(label) > 120:
            label = f"{label[:117]}..."
        options.append((label, action))
    return options


def action_details(kind: str, action: str) -> list[str]:
    normalized_kind = kind.strip().lower() or "sdk"
    normalized_action = action.strip()
    if not normalized_action:
        return [f"kind={normalized_kind}", "action=<blank>"]

    entry = _action_entries(normalized_kind).get(normalized_action)
    if entry is None:
        return [
            f"kind={normalized_kind}",
            f"action={normalized_action}",
            "catalog=custom or dynamic action",
        ]

    lines = [
        f"kind={normalized_kind}",
        f"action={normalized_action}",
    ]
    detail = str(entry.get("description") or entry.get("method") or "").strip()
    if detail:
        lines.append(detail)
    params = entry.get("params")
    if isinstance(params, dict) and params:
        lines.append(f"params={json.dumps(params, sort_keys=True)}")
    required = entry.get("required_params") or entry.get("requires_params")
    if isinstance(required, list) and required:
        lines.append(f"required={', '.join(str(value) for value in required)}")
    optional = entry.get("optional_params")
    if isinstance(optional, list) and optional:
        lines.append(f"optional={', '.join(str(value) for value in optional)}")
    return lines


def fragment_catalog() -> list[dict[str, Any]]:
    return [
        {
            "key": fragment.key,
            "label": fragment.label,
            "description": fragment.description,
            "steps": _clone_steps(list(fragment.steps)),
        }
        for fragment in PREBUILT_JOB_FRAGMENTS
    ]


def action_catalog() -> dict[str, Any]:
    return {
        "file_steps": dict(_ACTION_MAP.get("file_steps", {})),
        "sdk_steps": _merged_action_entries("sdk"),
        "dynamic_rules": list(_ACTION_MAP.get("dynamic_rules", [])),
    }


@dataclass
class RealityScanJobDraft:
    job_name: str | None = None
    auto_assign: bool = True
    target_client: str | None = None
    client_id: str | None = None
    group_id: str | None = None
    group_name: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    steps: list[dict[str, Any]] = field(default_factory=lambda: _clone_steps(DEFAULT_REALITYSCAN_SESSION_STEPS))

    @classmethod
    def basic(cls) -> "RealityScanJobDraft":
        return cls(steps=_clone_steps(DEFAULT_REALITYSCAN_SESSION_STEPS))

    @classmethod
    def align(cls) -> "RealityScanJobDraft":
        return cls(
            job_name="align-imagery",
            steps=_clone_steps(ALIGN_REALITYSCAN_STEPS),
        )

    def replace_steps(self, steps: list[dict[str, Any]]) -> None:
        normalized = [Step.model_validate(step).model_dump(exclude_none=True) for step in steps]
        self.steps = normalized

    def load_workflow(self, raw: str) -> None:
        self.replace_steps(read_workflow_path_or_inline(raw))

    def add_step(self, step: dict[str, Any]) -> None:
        self.steps.append(Step.model_validate(step).model_dump(exclude_none=True))

    def append_steps(self, steps: list[dict[str, Any]]) -> None:
        for step in steps:
            self.add_step(step)

    def insert_step(self, index: int, step: dict[str, Any]) -> None:
        self.steps.insert(self._normalize_insert_index(index), Step.model_validate(step).model_dump(exclude_none=True))

    def insert_steps(self, index: int, steps: list[dict[str, Any]]) -> None:
        insert_at = self._normalize_insert_index(index)
        normalized = [Step.model_validate(step).model_dump(exclude_none=True) for step in steps]
        self.steps[insert_at:insert_at] = normalized

    def update_step(self, index: int, step: dict[str, Any]) -> None:
        self.steps[self._normalize_index(index)] = Step.model_validate(step).model_dump(exclude_none=True)

    def remove_step(self, index: int) -> dict[str, Any]:
        return self.steps.pop(self._normalize_index(index))

    def move_step(self, index: int, offset: int) -> int:
        current = self._normalize_index(index)
        target = current + offset
        if target < 0 or target >= len(self.steps):
            raise IndexError(f"target step out of range: {index} -> {index + offset}")
        step = self.steps.pop(current)
        self.steps.insert(target, step)
        return target + 1

    def step_at(self, index: int) -> dict[str, Any]:
        return dict(self.steps[self._normalize_index(index)])

    def _normalize_index(self, index: int) -> int:
        if index < 1 or index > len(self.steps):
            raise IndexError(f"step index out of range: {index}")
        return index - 1

    def _normalize_insert_index(self, index: int) -> int:
        if index < 1 or index > len(self.steps) + 1:
            raise IndexError(f"insert step index out of range: {index}")
        return index - 1

    def build_request(self) -> JobRequest:
        request = JobRequest(
            job_name=(self.job_name or "").strip() or None,
            client_id=(self.client_id or "").strip() or None,
            target_client=(self.target_client or "").strip() or None,
            auto_assign=bool(self.auto_assign),
            group_id=(self.group_id or "").strip() or None,
            group_name=(self.group_name or "").strip() or None,
            metadata=dict(self.metadata),
            steps=[Step.model_validate(step) for step in self.steps],
        )
        if not request.steps:
            raise ValueError("job must contain at least one step")
        if not request.auto_assign and not request.requested_client:
            raise ValueError("target_client or client_id is required when auto_assign is false")
        if self.uses_stage_step() and not (request.group_id or request.group_name):
            raise ValueError("stage workflows require group_id or group_name")
        return request

    def uses_stage_step(self) -> bool:
        return any(
            str(step.get("kind", "")).strip().lower() == "file"
            and str(step.get("action", "")).strip().lower() == "stage"
            for step in self.steps
        )

    def preview_lines(self) -> list[str]:
        lines = [
            f"job_name={self.job_name or '-'} auto_assign={self.auto_assign} steps={len(self.steps)}",
            f"group={self.group_id or self.group_name or '-'} target={self.client_id or self.target_client or '-'}",
            f"metadata={json.dumps(self.metadata, sort_keys=True)}",
        ]
        for index, step in enumerate(self.steps, start=1):
            params = step.get("params", {})
            params_preview = json.dumps(params, sort_keys=True)
            if len(params_preview) > 120:
                params_preview = f"{params_preview[:120]}..."
            display_name = step.get("display_name")
            lines.append(
                f"{index}. {step.get('kind')}:{step.get('action')} "
                f"timeout={step.get('timeout_s', 600)} "
                f"display={display_name or '-'} params={params_preview}"
            )
        return lines
