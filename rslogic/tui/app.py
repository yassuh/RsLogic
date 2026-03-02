"""Textual TUI for upload, ingest, and orchestrator submission workflows."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import requests
from textual.app import App, ComposeResult
from textual.containers import Horizontal, Vertical
from textual.widgets import Button, Footer, Header, Input, RichLog, Rule, Static, Tree

from config import CONFIG
from rslogic.ingest import IngestService
from rslogic.upload_service import FolderUploader


DEFAULT_WORKFLOW_STEPS: list[dict[str, Any]] = [
    {"kind": "file", "action": "stage", "params": {}},
    {"kind": "sdk", "action": "sdk_node_connect_user"},
    {"kind": "sdk", "action": "sdk_project_create"},
    {"kind": "sdk", "action": "sdk_new_scene"},
    {"kind": "file", "action": "file_move_staging_to_working"},
]
UPLOAD_DIR_MAX_DEPTH = 4


def _read_json_path_or_inline(raw: str) -> list[dict[str, Any]]:
    text = raw.strip().strip('"').strip("'")
    if not text:
        return DEFAULT_WORKFLOW_STEPS
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
        raise ValueError("json payload should be a list of steps or object with a 'steps' field")
    if not isinstance(payload, list):
        raise ValueError("workflow JSON must be a list")
    return payload


class RsLogicTUI(App):
    CSS = """
    Screen {
        layout: vertical;
    }
    #top_row {
        height: 1fr;
    }
    #two_columns {
        height: 1fr;
    }
    #left_menu {
        width: 28%;
        min-width: 26;
    }
    .card {
        padding: 1;
        height: auto;
    }
    #right_panel {
        width: 72%;
    }
    .function-panel {
        display: none;
    }
    .function-panel.active {
        display: block;
    }
    .row {
        width: 100%;
        padding: 0 1;
    }
    .muted {
        color: $text-muted;
    }
    #activity {
        height: 5;
        max-height: 5;
    }
    #log_panel {
        margin-top: 1;
        height: 7;
    }
    #left_menu,
    #right_panel,
    #log_panel {
        border: none;
    }
    #col_sep {
        color: white;
        width: 1;
        border: none;
    }
    #sep_bottom {
        color: white;
        height: 1;
        border: none;
    }
    """

    BINDINGS = [("q", "quit", "Quit")]

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        yield Static("[b]RsLogic TUI[/b]", classes="row")
        yield Static(
            f"Waiting bucket: {CONFIG.s3.bucket_name} | Processed bucket: {CONFIG.s3.processed_bucket_name}",
            classes="row muted",
        )

        with Vertical(id="top_row"):
            with Horizontal(id="two_columns"):
                with Vertical(id="left_menu", classes="card"):
                    yield Static("[b]Functions[/b]")
                    yield Button("Upload folder", id="menu_upload", variant="default")
                    yield Button("Ingest waiting", id="menu_ingest", variant="default")
                    yield Button("Submit job", id="menu_job", variant="default")
                    yield Button("Check status", id="menu_status", variant="default")
                yield Rule(orientation="vertical", id="col_sep", line_style="solid")

                with Vertical(id="right_panel", classes="card"):
                    with Vertical(id="panel_upload", classes="function-panel"):
                        yield Static("[b]Upload images + sidecars[/b]")
                        yield Static("Select upload directory:", classes="row")
                        yield Tree("Upload folders", id="upload_dir_tree")
                        yield Static("No folder selected", id="upload_selected_path", classes="row muted")
                        yield Input(placeholder="Optional group name", id="upload_group")
                        yield Button("Upload", id="action_upload", variant="primary")

                    with Vertical(id="panel_ingest", classes="function-panel"):
                        yield Static("[b]Ingest waiting bucket[/b]")
                        yield Input(placeholder="Optional group name", id="ingest_group")
                        yield Input(placeholder="Optional limit", id="ingest_limit")
                        yield Button("Ingest", id="action_ingest", variant="primary")

                    with Vertical(id="panel_job", classes="function-panel"):
                        yield Static("[b]Submit job to orchestrator[/b]")
                        yield Input(value="true", placeholder="auto_assign: true|false", id="job_auto_assign")
                        yield Input(placeholder="target_client (required when auto_assign=false)", id="job_target_client")
                        yield Input(placeholder="group_id (optional)", id="job_group_id")
                        yield Input(placeholder="group_name (optional)", id="job_group_name")
                        yield Input(placeholder="client_id (optional)", id="job_client_id")
                        yield Input(
                            placeholder='Workflow JSON or file path (blank = default pipeline)',
                            id="job_workflow",
                        )
                        yield Button("Submit job", id="action_job", variant="primary")

                    with Vertical(id="panel_status", classes="function-panel"):
                        yield Static("[b]Job status[/b]")
                        yield Input(placeholder="Job ID", id="job_status_id")
                        with Horizontal():
                            yield Button("Refresh", id="action_job_status", variant="default")
                            yield Button("Exit", id="action_exit", variant="warning")

            yield Rule(id="sep_bottom", line_style="solid")

        with Vertical(id="log_panel", classes="card"):
            yield Static("[b]Activity (5 rows max)[/b]", classes="row")
            yield RichLog(highlight=True, id="activity", wrap=True, max_lines=5)
        yield Footer()

    def on_mount(self) -> None:
        self._log("RsLogic TUI ready")
        self._build_upload_directory_tree()
        self._show_panel("upload")

    def _show_panel(self, name: str) -> None:
        for panel_name in ("upload", "ingest", "job", "status"):
            panel = self.query_one(f"#panel_{panel_name}")
            is_active = panel_name == name
            panel.visible = is_active
            panel.set_class(is_active, "active")

    def _log(self, message: str) -> None:
        self.query_one(RichLog).write(message)

    def _input_value(self, widget_id: str) -> str:
        input_widget = self.query_one(f"#{widget_id}", expect_type=Input)
        return (input_widget.value or "").strip()

    def _upload(self) -> None:
        folder = self._selected_upload_path()
        if not folder:
            self._log("Upload failed: folder path required")
            return
        group = self._input_value("upload_group") or None
        uploader = FolderUploader()
        records = uploader.run(Path(folder).expanduser())
        self._log(f"Uploaded {len(records)} objects")
        self._log(f"Group hint: {group or '<none>'}")

    def _selected_upload_path(self) -> str | None:
        label = self.query_one("#upload_selected_path", expect_type=Static)
        text = label.renderable
        if text is None:
            return None
        text_value = str(text)
        if text_value.startswith("Selected: "):
            path_text = text_value.removeprefix("Selected: ")
            return path_text if path_text and path_text != "No folder selected" else None
        return None

    def _build_upload_directory_tree(self) -> None:
        tree = self.query_one("#upload_dir_tree", expect_type=Tree)
        tree.clear()
        root_path = Path.cwd().resolve()
        self._populate_directory_node(tree.root, root_path)
        tree.root.expand()

    def _has_subdirs(self, folder: Path) -> bool:
        try:
            for child in folder.iterdir():
                if child.is_dir():
                    return True
        except OSError:
            return False
        return False

    def _populate_directory_node(self, parent_node: object, parent_path: Path, level: int = 0, max_level: int = 4) -> None:
        if level >= UPLOAD_DIR_MAX_DEPTH:
            return
        try:
            entries = sorted((p for p in parent_path.iterdir() if p.is_dir()), key=lambda p: p.name.lower())
        except OSError:
            return

        for entry in entries:
            label = entry.name
            has_children = self._has_subdirs(entry)
            child = parent_node.add(label, data=str(entry), allow_expand=has_children)
            if has_children and level + 1 < UPLOAD_DIR_MAX_DEPTH:
                self._populate_directory_node(child, entry, level + 1)

    def on_tree_node_selected(self, event: Tree.NodeSelected) -> None:
        if event.control.id != "upload_dir_tree":
            return
        data = event.node.data
        if not isinstance(data, str):
            return
        self.query_one("#upload_selected_path", expect_type=Static).update(f"Selected: {data}")

    def _ingest(self) -> None:
        group = self._input_value("ingest_group") or None
        limit_value = self._input_value("ingest_limit")
        limit = int(limit_value) if limit_value.isdigit() else None
        service = IngestService()
        items = service.run(group_name=group, limit=limit)
        self._log(f"Ingested {len(items)} images")

    def _submit_job(self) -> None:
        auto_assign = self._input_value("job_auto_assign").lower() != "false"
        target_client = self._input_value("job_target_client") or None
        group_id = self._input_value("job_group_id") or None
        group_name = self._input_value("job_group_name") or None
        client_id = self._input_value("job_client_id") or None
        workflow_raw = self._input_value("job_workflow")

        if not auto_assign and not target_client:
            self._log("Submit job failed: target_client is required when auto_assign=false")
            return

        steps = _read_json_path_or_inline(workflow_raw)
        uses_file_stage = any(
            step.get("kind", "").strip().lower() == "file"
            and step.get("action", "").strip().lower() in {"stage", "file_stage", "file_stage_group"}
            for step in steps
        )
        if uses_file_stage and not (group_id or group_name):
            self._log("Submit job failed: file stage workflow requires group_id or group_name")
            return

        payload: dict[str, Any] = {
            "auto_assign": auto_assign,
            "steps": steps,
        }
        if target_client:
            payload["target_client"] = target_client
        if client_id:
            payload["client_id"] = client_id
        if group_id:
            payload["group_id"] = group_id
        if group_name:
            payload["group_name"] = group_name

        response = requests.post(f"{CONFIG.api.base_url}/jobs", json=payload, timeout=30)
        response.raise_for_status()
        job = response.json()
        self._log(f"Dispatched job {job.get('job_id')} -> client {job.get('client_id')}")

    def _status(self) -> None:
        job_id = self._input_value("job_status_id")
        if not job_id:
            self._log("Job status failed: job_id required")
            return
        response = requests.get(f"{CONFIG.api.base_url}/jobs/{job_id}", timeout=30)
        if response.status_code == 404:
            self._log("Job status: not found")
            return
        response.raise_for_status()
        payload = response.json()
        self._log(json.dumps(payload, indent=2))

    def _run_command(self, action: str) -> None:
        try:
            if action.startswith("menu_"):
                if action == "menu_upload":
                    self._show_panel("upload")
                elif action == "menu_ingest":
                    self._show_panel("ingest")
                elif action == "menu_job":
                    self._show_panel("job")
                elif action == "menu_status":
                    self._show_panel("status")
                return

            if action == "action_upload":
                self._upload()
            elif action == "action_ingest":
                self._ingest()
            elif action == "action_job":
                self._submit_job()
            elif action == "action_job_status":
                self._status()
            elif action == "action_exit":
                self.exit()
        except Exception as exc:
            self._log(f"Error: {exc}")

    def on_button_pressed(self, event: Button.Pressed) -> None:
        self._run_command(event.button.id or "")


def main() -> None:
    RsLogicTUI().run()


if __name__ == "__main__":
    main()
