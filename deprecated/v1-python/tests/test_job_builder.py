from __future__ import annotations

import json

import pytest

from rslogic.tui.job_builder import (
    RealityScanJobDraft,
    action_catalog,
    action_options,
    build_step_payload,
    fragment_steps,
    read_workflow_path_or_inline,
)


def test_build_step_payload_normalizes_step() -> None:
    payload = build_step_payload(
        kind="SDK",
        action=" sdk_project_command ",
        params_raw='{"name":"align"}',
        timeout_raw="0",
        display_name="Align",
    )

    assert payload["kind"] == "sdk"
    assert payload["action"] == "sdk_project_command"
    assert payload["params"] == {"name": "align"}
    assert payload["timeout_s"] == 0
    assert payload["display_name"] == "Align"


def test_draft_requires_group_for_stage_workflow() -> None:
    draft = RealityScanJobDraft.basic()
    draft.auto_assign = True

    with pytest.raises(ValueError, match="group_id or group_name"):
        draft.build_request()


def test_draft_build_request_includes_job_name_and_steps() -> None:
    draft = RealityScanJobDraft.align()
    draft.group_name = "flight-a"
    request = draft.build_request()

    assert request.job_name == "align-imagery"
    assert request.group_name == "flight-a"
    assert request.steps[-1].action == "sdk_project_save"
    assert request.steps[4].action == "file_copy_staging_to_session"
    assert request.steps[4].params["relative_dir"] == "Imagery"


def test_read_workflow_path_or_inline_accepts_object_with_steps(tmp_path) -> None:
    workflow_path = tmp_path / "workflow.json"
    workflow_path.write_text(
        json.dumps(
            {
                "steps": [
                    {"kind": "sdk", "action": "sdk_node_connect_user", "params": {}},
                ]
            }
        ),
        encoding="utf-8",
    )

    steps = read_workflow_path_or_inline(str(workflow_path))

    assert steps == [{"kind": "sdk", "action": "sdk_node_connect_user", "params": {}}]


def test_fragment_steps_are_cloneable() -> None:
    steps = fragment_steps("save_project")
    steps[0]["params"]["path"] = "changed.rspj"

    fresh = fragment_steps("save_project")

    assert fresh[0]["params"]["path"] == "realityscan-job.rspj"


def test_action_options_include_known_sdk_actions() -> None:
    options = action_options("sdk")

    assert any(value == "sdk_project_command" for _, value in options)
    assert any(value == "sdk_project_add_folder" for _, value in options)


def test_action_catalog_includes_sdk_surface_entries() -> None:
    catalog = action_catalog()

    assert "sdk_project_add_folder" in catalog["sdk_steps"]
    assert catalog["sdk_steps"]["sdk_project_add_folder"]["params"]["folder_path"] == "str"
    assert "sdk_node_status" in catalog["sdk_steps"]
    assert "sdk_new_scene" not in catalog["sdk_steps"]
    assert "sdk_project_commandgroup" not in catalog["sdk_steps"]
    assert "file_copy_staging_to_session_imagery" not in catalog["file_steps"]
    assert catalog["file_steps"]["file_copy_staging_to_session"]["optional_params"] == ["relative_dir"]


def test_draft_insert_and_move_steps() -> None:
    draft = RealityScanJobDraft.basic()
    draft.group_name = "flight-a"
    draft.insert_steps(1, [{"kind": "sdk", "action": "sdk_project_status", "params": {}}])

    assert draft.steps[0]["action"] == "sdk_project_status"

    new_index = draft.move_step(1, 1)

    assert new_index == 2
    assert draft.steps[1]["action"] == "sdk_project_status"
