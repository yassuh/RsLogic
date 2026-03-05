"""Shared text rendering helpers for client heartbeat status panels."""

from __future__ import annotations

from typing import Any


def coerce_progress(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        text = value.strip().rstrip("%")
        if not text:
            return None
        try:
            return float(text)
        except ValueError:
            return None
    return None


def render_task_state(task_state: Any) -> str:
    if isinstance(task_state, dict):
        tasks = task_state.get("tasks")
        if isinstance(tasks, list):
            return f"tasks={len(tasks)}"
    if isinstance(task_state, list):
        return f"tasks={len(task_state)}"
    return "tasks=0"


def render_project_status(project_status: Any) -> str:
    if not isinstance(project_status, dict) or not project_status:
        return "no data"
    progress = project_status.get("progress")
    if progress is None:
        return "available"
    return f"progress={progress} elapsed={project_status.get('timeTotal', '-')} est={project_status.get('timeEstimation', '-')}"


def render_running_task_bars(task_state: Any, project_status: Any = None) -> str:
    tasks = task_state.get("tasks") if isinstance(task_state, dict) else task_state
    if not isinstance(tasks, list) or not tasks:
        project_progress = coerce_progress(project_status.get("progress")) if isinstance(project_status, dict) else None
        if project_progress is None:
            return "running tasks: 0"
        return f"running tasks: 0 | project={project_progress:.0f}%"

    lines = [f"running tasks: {len(tasks)}"]
    for task in tasks:
        if not isinstance(task, dict):
            continue
        task_id = task.get("taskID") or task.get("taskId") or task.get("id") or "-"
        task_progress = coerce_progress(task.get("progress"))
        state = task.get("state") or task.get("status") or "-"
        percent = "n/a" if task_progress is None else f"{task_progress:.0f}%"
        lines.append(f"{task_id}: {state} {percent}")
    return "\n".join(lines)
