from __future__ import annotations

from rslogic.client.status_render import render_project_status, render_running_task_bars, render_task_state


def test_render_task_state_counts_tasks() -> None:
    assert render_task_state({"tasks": [{"taskID": "1"}, {"taskID": "2"}]}) == "tasks=2"


def test_render_project_status_formats_progress_line() -> None:
    assert render_project_status({"progress": 42, "timeTotal": 12, "timeEstimation": 8}) == "progress=42 elapsed=12 est=8"


def test_render_running_task_bars_formats_each_task() -> None:
    rendered = render_running_task_bars(
        [{"taskID": "t-1", "state": "running", "progress": 35}],
        {"progress": 42},
    )

    assert "running tasks: 1" in rendered
    assert "t-1: running 35%" in rendered
