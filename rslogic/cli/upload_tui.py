"""Textual-powered interactive upload wizard with multi-page flow."""

from __future__ import annotations

import json
import threading
from datetime import datetime
import time
from pathlib import Path
from uuid import uuid4
from typing import Any, Callable, Dict, Iterable, List, Sequence
from urllib import error as urllib_error
from urllib import request as urllib_request
import ast

from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical
from textual.widgets import (
    Button,
    ContentSwitcher,
    DirectoryTree,
    Footer,
    Header,
    Input,
    Label,
    ProgressBar,
    Tree,
    Static,
)

from config import AppConfig
from rslogic.jobs.command_channel import (
    COMMAND_TYPE_RSTOOL_COMMAND,
    COMMAND_TYPE_RSTOOL_DISCOVER,
    ProcessingCommand,
    ProcessingCommandResult,
    RESULT_STATUS_ACCEPTED,
    RESULT_STATUS_ERROR,
    RESULT_STATUS_OK,
    RESULT_STATUS_PROGRESS,
    RedisCommandBus,
)

UploadReporter = Callable[[str], None]
UploadProgress = Callable[[int, int, int, int], None]
UploadHandler = Callable[[Sequence[str], str | None, bool, UploadReporter, UploadProgress], None]


class FastDirectoryTree(DirectoryTree):
    """Directory tree tuned for large repos and image folders."""

    MAX_ENTRIES_PER_DIRECTORY = 500
    SKIP_NAMES = {
        ".git",
        ".venv",
        "__pycache__",
        ".mypy_cache",
        ".pytest_cache",
        ".ruff_cache",
        "node_modules",
    }

    def filter_paths(self, paths: Iterable[Path]) -> Iterable[Path]:
        count = 0
        for path in paths:
            name = path.name
            if name in self.SKIP_NAMES:
                continue
            # Folder-only explorer view to avoid huge file lists.
            if not self._safe_is_dir(path):
                continue
            if count >= self.MAX_ENTRIES_PER_DIRECTORY:
                break
            yield path
            count += 1


class UploadWizardApp(App[None]):
    """Interactive upload UI with setup, upload, and result pages."""

    TITLE = "RsLogic Upload Wizard"
    SUB_TITLE = "S3 upload with explorer"
    BINDINGS = [
        Binding("n", "next_page", "Next"),
        Binding("b", "previous_page", "Back"),
        Binding("u", "start_upload", "Upload"),
        Binding("q", "cancel", "Close"),
        Binding("ctrl+d", "clear_selection", "Clear Paths"),
    ]

    PAGE_SETUP = "page-setup"
    PAGE_UPLOAD = "page-upload"
    PAGE_RESULT = "page-result"
    PAGE_START = "page-start"
    PAGE_INGEST = "page-ingest"
    PAGE_JOB = "page-job"
    PAGE_COMMAND = "page-command"
    LOG_ROW_COUNT = 3
    SDK_RESOURCE_ROOT = Path(__file__).resolve().parents[2] / "internal_tools" / "rstool-sdk" / "src" / "realityscan_sdk" / "resources"
    COMMAND_TREE_NODE_ID = "command-tree"

    DEFAULT_CSS = """
    Screen {
        layout: vertical;
        background: #0b1220;
        color: #e2e8f0;
    }

    #page-title {
        margin: 0 1;
        border: round #334155;
        padding: 0 1;
        height: 3;
        content-align: left middle;
        color: #cbd5e1;
    }

    #pages {
        height: 1fr;
    }

    .page {
        height: 1fr;
    }

    #setup-top-row {
        layout: horizontal;
        height: 1fr;
    }

    .panel {
        border: round #334155;
        margin: 0 1;
        padding: 0 1;
        background: #111827;
    }

    .panel-title {
        text-style: bold;
        color: #93c5fd;
        margin-bottom: 1;
    }

    #explorer-panel {
        width: 2fr;
    }

    #defaults-panel {
        width: 1fr;
    }

    #directory-tree {
        height: 1fr;
        border: round #1d4ed8;
    }

    #explorer-help {
        color: #94a3b8;
        margin-top: 1;
        height: auto;
    }

    #defaults-static {
        border: round #475569;
        background: #0f172a;
        color: #e2e8f0;
        padding: 0 1;
        height: auto;
    }

    #group-input {
        margin-top: 1;
        border: round #475569;
        background: #0f172a;
        color: #e2e8f0;
    }

    #group-help {
        margin-top: 1;
        color: #94a3b8;
        height: auto;
    }

    #selected-panel {
        height: 12;
        margin-top: 1;
    }

    #selected-output {
        border: round #475569;
        height: 1fr;
        background: #0f172a;
        color: #e2e8f0;
    }

    #path-input {
        margin-top: 1;
        border: round #475569;
        background: #0f172a;
        color: #e2e8f0;
    }

    #selection-actions {
        margin-top: 1;
        height: auto;
    }

    #setup-action-row, #upload-action-row, #result-action-row, #start-action-row, #ingest-action-row, #job-action-row,
    #command-action-row {
        height: auto;
        margin: 1;
        align: right middle;
    }

    #start-options-panel {
        margin-top: 1;
        height: 1fr;
    }

    #start-options {
        border: round #475569;
        background: #0f172a;
        color: #cbd5e1;
        padding: 0 1;
        height: 1fr;
    }

    #command-config-panel,
    #command-result-panel {
        margin-top: 1;
    }

    #command-page-top {
        height: 1fr;
    }

    #command-tree-panel {
        width: 1fr;
        margin-right: 1;
    }

    #command-config-panel {
        width: 2fr;
    }

    #command-tree {
        border: round #475569;
        background: #0f172a;
        color: #e2e8f0;
        padding: 0 1;
        height: 1fr;
    }

    #command-config,
    #command-result {
        border: round #475569;
        background: #0f172a;
        color: #cbd5e1;
        padding: 0 1;
        height: auto;
    }

    #command-history {
        border: round #475569;
        background: #0f172a;
        color: #cbd5e1;
        padding: 0 1;
        height: 1fr;
    }

    #ingest-config-panel {
        margin-top: 1;
        height: auto;
    }

    #ingest-config {
        border: round #475569;
        background: #0f172a;
        color: #cbd5e1;
        padding: 0 1;
        height: 8;
    }

    #ingest-result-panel {
        margin-top: 1;
        height: 1fr;
    }

    #ingest-result {
        border: round #475569;
        background: #0f172a;
        color: #cbd5e1;
        padding: 0 1;
        height: 1fr;
    }

    #ingest-group-input, #ingest-prefix-input, #ingest-limit-input, #ingest-concurrency-input {
        margin-top: 1;
        border: round #475569;
        background: #0f172a;
        color: #e2e8f0;
    }

    #job-config-panel {
        margin-top: 1;
        height: auto;
    }

    #job-config {
        border: round #475569;
        background: #0f172a;
        color: #cbd5e1;
        padding: 0 1;
        height: 20;
    }

    #job-result-panel {
        margin-top: 1;
        height: 1fr;
    }

    #job-result {
        border: round #475569;
        background: #0f172a;
        color: #cbd5e1;
        padding: 0 1;
        height: 1fr;
    }

    #job-group-input, #job-drone-input, #job-max-images-input, #job-sdk-folder-input,
    #job-sdk-project-input, #job-sdk-detector-input, #job-sdk-acc-xyz-input, #job-sdk-acc-ypr-input,
    #job-sdk-include-subdirs-input, #job-sdk-run-align-input, #job-sdk-run-normal-input,
    #job-sdk-run-ortho-input, #job-sdk-timeout-input, #job-id-input {
        margin-top: 1;
        border: round #475569;
        background: #0f172a;
        color: #e2e8f0;
    }

    #command-type-input, #command-target-input, #command-method-input, #command-target-object-input,
    #command-session-input, #command-project-guid-input, #command-project-name-input,
    #command-args-input, #command-kwargs-input, #command-timeout-input, #command-id-input,
    #command-custom-payload-input {
        margin-top: 1;
        border: round #475569;
        background: #0f172a;
        color: #e2e8f0;
    }

    #upload-plan-panel {
        height: auto;
        margin-top: 1;
    }

    #upload-plan {
        border: round #475569;
        background: #0f172a;
        color: #cbd5e1;
        padding: 0 1;
        height: 10;
    }

    #progress-panel {
        height: 1fr;
        margin-top: 1;
    }

    #upload-progress {
        border: round #475569;
        background: #0f172a;
        height: auto;
        padding: 0 1;
    }

    #progress-detail {
        color: #cbd5e1;
        margin-top: 1;
    }

    #result-summary {
        border: round #475569;
        background: #0f172a;
        color: #cbd5e1;
        padding: 0 1;
        height: 1fr;
    }

    #status {
        height: 3;
        margin: 0 1 1 1;
        border: round #334155;
        color: #cbd5e1;
        content-align: left middle;
        padding: 0 1;
    }

    #status.ok {
        border: round #16a34a;
        color: #86efac;
    }

    #status.error {
        border: round #dc2626;
        color: #fca5a5;
    }

    #log-bar {
        height: 5;
        margin: 0 1 1 1;
        border: round #334155;
        color: #cbd5e1;
        padding: 0 1;
        content-align: left top;
    }

    Button {
        margin-right: 1;
    }
    """

    def __init__(
        self,
        config: AppConfig,
        upload_handler: UploadHandler,
        root_path: Path | None = None,
        initial_group_name: str | None = None,
    ) -> None:
        super().__init__()
        self._config = config
        self._upload_handler = upload_handler
        self._root_path = root_path or Path.cwd()
        self._initial_group_name = (initial_group_name or "").strip()
        self._selected_paths: List[Path] = []
        self._uploading = False
        self._ingesting = False
        self._job_running = False
        self._command_running = False
        self._last_job_id: str | None = None
        self._last_command_id: str | None = None
        self._last_completed = 0
        self._last_total = 0
        self._last_total_bytes = 0
        self._last_target_bytes = 1
        self._last_group_name: str | None = None
        self._override_existing_upload = False
        self._override_existing_ingest = False
        self._command_bus: RedisCommandBus | None = None
        self._command_events: List[str] = []
        self._log_lines: List[str] = []
        self._command_catalog = self._load_command_catalog()

    def compose(self) -> ComposeResult:
        yield Header(show_clock=False)
        yield Static("", id="page-title")
        with ContentSwitcher(initial=self.PAGE_START, id="pages"):
            with Vertical(id=self.PAGE_START, classes="page"):
                with Vertical(id="start-options-panel", classes="panel"):
                    yield Label("Workflows", classes="panel-title")
                    yield Static(
                        "\n".join(
                            [
                                "1) Upload Imagery",
                                "   Upload local imagery to the locked waiting bucket only.",
                                "",
                                "2) Ingest Waiting Metadata",
                                "   Call the server to ingest S3 object metadata from the waiting bucket",
                                "   into image_assets.metadata in the database.",
                                "",
                                "3) Create Processing Job",
                                "   Run the RealityScan SDK sequence used in example.ipynb:",
                                "   newScene -> set -> addFolder -> align/model/ortho -> save.",
                                "",
                                "4) Command Console",
                                "   Send Redis control commands directly and view replies.",
                            ]
                        ),
                        id="start-options",
                    )

                with Horizontal(id="start-action-row"):
                    yield Button("Upload Imagery", variant="primary", id="start-upload-button")
                    yield Button("Ingest Waiting Metadata", variant="success", id="start-ingest-button")
                    yield Button("Create Processing Job", variant="warning", id="start-job-button")
                    yield Button("Command Console", variant="primary", id="start-command-button")
                    yield Button("Close", variant="error", id="start-close-button")

            with Vertical(id=self.PAGE_SETUP, classes="page"):
                with Horizontal(id="setup-top-row"):
                    with Vertical(id="explorer-panel", classes="panel"):
                        yield Label("Folder Explorer", classes="panel-title")
                        yield FastDirectoryTree(str(self._root_path), id="directory-tree")
                        yield Static(
                            "Browse folders only. Enter adds folder. Large folders are capped for responsiveness. "
                            "Video files are ignored.",
                            id="explorer-help",
                        )
                    with Vertical(id="defaults-panel", classes="panel"):
                        yield Label("Upload Defaults", classes="panel-title")
                        yield Static(
                            "\n".join(
                                [
                                    f"Bucket: {self._config.s3.bucket_name} (locked)",
                                    f"Prefix: {self._config.s3.scratchpad_prefix}",
                                    f"Concurrency: {max(self._config.s3.multipart_concurrency, 1)}",
                                    f"Part size MB: {max(self._config.s3.multipart_part_size // (1024 * 1024), 5)}",
                                    f"Resume: {self._config.s3.resume_uploads}",
                                ]
                            ),
                            id="defaults-static",
                        )
                        yield Label("Group (optional)", classes="panel-title")
                        yield Input(
                            value=self._initial_group_name,
                            placeholder="image group name",
                            id="group-input",
                        )
                        yield Button("", id="upload-override-toggle")
                        yield Static(
                            "Stored in S3 user metadata as group_name.",
                            id="group-help",
                        )

                with Vertical(id="selected-panel", classes="panel"):
                    yield Label("Selected Paths", classes="panel-title")
                    yield Static("", id="selected-output")
                    yield Input(
                        placeholder="Type a path and press Enter to add (faster than browsing huge folders)",
                        id="path-input",
                    )
                    with Horizontal(id="selection-actions"):
                        yield Button("Add Path", id="add-path")
                        yield Button("Undo Last", id="undo-path")
                        yield Button("Clear Paths", id="clear-paths")

                with Horizontal(id="setup-action-row"):
                    yield Button("Back", id="setup-back-button")
                    yield Button("Next", variant="primary", id="setup-next-button")
                    yield Button("Close", variant="error", id="setup-close-button")

            with Vertical(id=self.PAGE_UPLOAD, classes="page"):
                with Vertical(id="upload-plan-panel", classes="panel"):
                    yield Label("Upload Plan", classes="panel-title")
                    yield Static("", id="upload-plan")

                with Vertical(id="progress-panel", classes="panel"):
                    yield Label("Upload Progress", classes="panel-title")
                    yield ProgressBar(total=1, show_eta=False, id="upload-progress")
                    yield Static("Waiting for upload...", id="progress-detail")

                with Horizontal(id="upload-action-row"):
                    yield Button("Back", id="upload-back-button")
                    yield Button("Start Upload", variant="success", id="upload-start-button")
                    yield Button("Close", variant="error", id="upload-close-button")

            with Vertical(id=self.PAGE_RESULT, classes="page"):
                with Vertical(classes="panel"):
                    yield Label("Result", classes="panel-title")
                    yield Static("", id="result-summary")

                with Horizontal(id="result-action-row"):
                    yield Button("New Upload", variant="primary", id="result-new-button")
                    yield Button("Home", id="result-home-button")
                    yield Button("Close", variant="error", id="result-close-button")

            with Vertical(id=self.PAGE_INGEST, classes="page"):
                with Vertical(id="ingest-config-panel", classes="panel"):
                    yield Label("Ingest Configuration", classes="panel-title")
                    yield Input(
                        placeholder="Group override (optional, otherwise use S3 metadata/default)",
                        id="ingest-group-input",
                    )
                    yield Input(
                        placeholder="Prefix filter (optional)",
                        id="ingest-prefix-input",
                    )
                    yield Input(value="1000", placeholder="Object limit", id="ingest-limit-input")
                    yield Input(value="24", placeholder="Concurrency", id="ingest-concurrency-input")
                    yield Button("", id="ingest-override-toggle")
                    yield Static("", id="ingest-config")

                with Vertical(id="ingest-result-panel", classes="panel"):
                    yield Label("Ingest Result", classes="panel-title")
                    yield Static("No ingest run yet.", id="ingest-result")

                with Horizontal(id="ingest-action-row"):
                    yield Button("Back", id="ingest-back-button")
                    yield Button("Run Ingest", variant="success", id="ingest-run-button")
                    yield Button("Close", variant="error", id="ingest-close-button")

            with Vertical(id=self.PAGE_JOB, classes="page"):
                with Vertical(id="job-config-panel", classes="panel"):
                    yield Label("Job Configuration", classes="panel-title")
                    yield Input(
                        placeholder="Group name (optional; API default if omitted)",
                        id="job-group-input",
                    )
                    yield Input(
                        placeholder="Drone type filter (optional)",
                        id="job-drone-input",
                    )
                    yield Input(
                        placeholder="Max images (optional integer)",
                        id="job-max-images-input",
                    )
                    yield Input(
                        value="Imagery",
                        placeholder="RealityScan addFolder path (relative/absolute path visible to node)",
                        id="job-sdk-folder-input",
                    )
                    yield Input(
                        placeholder="Project save path/name (optional, e.g. test_auto.rspj)",
                        id="job-sdk-project-input",
                    )
                    yield Input(
                        value="Ultra",
                        placeholder="Detector sensitivity (e.g. Ultra)",
                        id="job-sdk-detector-input",
                    )
                    yield Input(
                        value="0.1",
                        placeholder="Camera prior accuracy X/Y/Z",
                        id="job-sdk-acc-xyz-input",
                    )
                    yield Input(
                        value="1.0",
                        placeholder="Camera prior accuracy Yaw/Pitch/Roll",
                        id="job-sdk-acc-ypr-input",
                    )
                    yield Input(
                        value="true",
                        placeholder="Include subdirectories (true/false)",
                        id="job-sdk-include-subdirs-input",
                    )
                    yield Input(
                        value="true",
                        placeholder="Run align stage (true/false)",
                        id="job-sdk-run-align-input",
                    )
                    yield Input(
                        value="true",
                        placeholder="Run normal model stage (true/false)",
                        id="job-sdk-run-normal-input",
                    )
                    yield Input(
                        value="true",
                        placeholder="Run ortho projection stage (true/false)",
                        id="job-sdk-run-ortho-input",
                    )
                    yield Input(
                        value="7200",
                        placeholder="Per-task timeout seconds",
                        id="job-sdk-timeout-input",
                    )
                    yield Input(
                        placeholder="Job ID for status/cancel (auto-filled after create)",
                        id="job-id-input",
                    )
                    yield Static("", id="job-config")

                with Vertical(id="job-result-panel", classes="panel"):
                    yield Label("Job Result", classes="panel-title")
                    yield Static("No job run yet.", id="job-result")

                with Horizontal(id="job-action-row"):
                    yield Button("Back", id="job-back-button")
                    yield Button("Create Job", variant="success", id="job-create-button")
                    yield Button("Refresh Status", id="job-refresh-button")
                    yield Button("Cancel Job", variant="warning", id="job-cancel-button")
                    yield Button("Close", variant="error", id="job-close-button")

            with Vertical(id=self.PAGE_COMMAND, classes="page"):
                with Horizontal(id="command-page-top", classes="command-page-top"):
                    with Vertical(id="command-tree-panel", classes="panel"):
                        yield Label("RSTool Commands", classes="panel-title")
                        yield Tree("RSTool API", id=self.COMMAND_TREE_NODE_ID)
                    with Vertical(id="command-config-panel", classes="panel"):
                        yield Label("Redis Command Console", classes="panel-title")
                        yield Input(
                            value=COMMAND_TYPE_RSTOOL_DISCOVER,
                            id="command-type-input",
                        )
                        yield Input(
                            value="node",
                            placeholder="command target (node/project/client) for rstool commands",
                            id="command-target-input",
                        )
                        yield Input(
                            placeholder="command target_object (optional, e.g. connect_user)",
                            id="command-target-object-input",
                        )
                        yield Input(
                            placeholder="method (e.g. connect_user / newScene / add_folder)",
                            id="command-method-input",
                        )
                        yield Input(
                            placeholder="session/action (optional)",
                            id="command-session-input",
                        )
                        yield Input(
                            placeholder="project guid (for session_action=open)",
                            id="command-project-guid-input",
                        )
                        yield Input(
                            placeholder="project name (for session_action=open)",
                            id="command-project-name-input",
                        )
                        yield Input(
                            value='[]',
                            placeholder="args (JSON array)",
                            id="command-args-input",
                        )
                        yield Input(
                            value='{}',
                            placeholder="kwargs (JSON object)",
                            id="command-kwargs-input",
                        )
                        yield Input(
                            value=str(self._config.control.request_timeout_seconds),
                            placeholder="response wait timeout seconds",
                            id="command-timeout-input",
                        )
                        yield Input(
                            placeholder="command id (optional)",
                            id="command-id-input",
                        )
                        yield Input(
                            placeholder="Custom raw payload JSON (optional override)",
                            id="command-custom-payload-input",
                        )
                        yield Static("", id="command-config")

                with Vertical(id="command-result-panel", classes="panel"):
                    yield Label("Latest Command Result", classes="panel-title")
                    yield Static("No command run yet.", id="command-result")
                    yield Static("", id="command-history")

                with Horizontal(id="command-action-row"):
                    yield Button("Back", id="command-back-button")
                    yield Button("Discover SDK", variant="primary", id="command-discover-button")
                    yield Button("Send Command", variant="success", id="command-send-button")
                    yield Button("Clear Response", id="command-clear-response-button")
                    yield Button("Close", variant="error", id="command-close-button")

        yield Static("", id="status")
        yield Static("", id="log-bar")
        yield Footer()

    def on_mount(self) -> None:
        self._refresh_log_bar()
        self._set_upload_override_button_label()
        self._set_ingest_override_button_label()
        self._refresh_selected_paths()
        self._refresh_upload_plan()
        self._refresh_ingest_plan()
        self._refresh_job_plan()
        self._refresh_command_config()
        self._build_command_tree()
        self._set_progress_state(0, 1, 0, 1)
        self._set_result_summary("No uploads run yet.")
        self._switch_page(self.PAGE_START)
        self._set_status("Choose a workflow to begin.", error=False)

    def _load_command_catalog(self) -> Dict[str, Dict[str, Dict[str, str]]]:
        catalog: Dict[str, Dict[str, Dict[str, str]]] = {"node": {}, "project": {}}
        source_map: Dict[str, str] = {
            "node": str(self.SDK_RESOURCE_ROOT / "node.py"),
            "project": str(self.SDK_RESOURCE_ROOT / "project.py"),
        }
        for target, source_path in source_map.items():
            catalog[target] = self._load_command_source_catalog(target, source_path)
            if not catalog[target]:
                catalog[target] = self._fallback_command_catalog(target)
        return catalog

    def _load_command_source_catalog(self, target: str, source_path: str) -> Dict[str, Dict[str, str]]:
        source_file = Path(source_path)
        if not source_file.exists():
            return {}
        try:
            source_text = source_file.read_text(encoding="utf-8")
            source_tree = ast.parse(source_text)
        except Exception as exc:
            self._append_log(f"WARN unable to parse SDK {target} source: {exc}")
            return {}

        class_name = "NodeAPI" if target == "node" else "ProjectAPI"
        methods: Dict[str, Dict[str, str]] = {}
        for node in source_tree.body:
            if isinstance(node, ast.ClassDef) and node.name == class_name:
                for member in node.body:
                    if isinstance(member, (ast.FunctionDef, ast.AsyncFunctionDef)):
                        if member.name.startswith("_"):
                            continue
                        try:
                            signature = f"({ast.unparse(member.args)})"
                        except Exception:
                            signature = "(...)"
                        methods[member.name] = {
                            "signature": signature,
                            "doc": (ast.get_docstring(member) or "").strip(),
                        }
                break
        return methods

    def _fallback_command_catalog(self, target: str) -> Dict[str, Dict[str, str]]:
        if target == "node":
            return {
                "connect_user": {"signature": "(self)", "doc": "Connect user to the node."},
                "connection": {"signature": "(self)", "doc": "Get node connection."},
                "disconnect_user": {"signature": "(self)", "doc": "Disconnect user from the node."},
                "projects": {"signature": "(self)", "doc": "List known projects."},
                "status": {"signature": "(self)", "doc": "Get node status."},
            }
        if target == "project":
            return {
                "create": {"signature": "(self)", "doc": "Create/open a new project session."},
                "open": {"signature": "(self, guid, name=None)", "doc": "Open an existing project."},
                "close": {"signature": "(self)", "doc": "Close current project."},
                "disconnect": {"signature": "(self)", "doc": "Disconnect from current project."},
                "delete": {"signature": "(self, guid)", "doc": "Delete project by GUID."},
                "status": {"signature": "(self)", "doc": "Get project status."},
                "command": {"signature": "(self, name, params=None, ...)", "doc": "Run a low-level command."},
                "new_scene": {"signature": "(self)", "doc": "Start a new scene."},
                "add_folder": {"signature": "(self, folder_path)", "doc": "Import folder images."},
            }
        return {}

    def _build_command_tree(self) -> None:
        try:
            tree = self.query_one(f"#{self.COMMAND_TREE_NODE_ID}", Tree)
        except Exception:
            return
        tree.clear()
        tree.root.label = "RSTool API"
        for target in ("node", "project"):
            methods = self._command_catalog.get(target, {})
            if not methods:
                continue
            target_node = tree.root.add(target.capitalize())
            grouped: Dict[str, List[str]] = {}
            for name in methods:
                category = self._command_group_name(target, name)
                grouped.setdefault(category, []).append(name)
            for category in sorted(grouped):
                category_node = target_node.add(category)
                for method_name in sorted(grouped[category]):
                    method_meta = methods[method_name]
                    label = f"{method_name}{method_meta.get('signature', '')}"
                    node_data = {
                        "type": "command",
                        "target": target,
                        "method": method_name,
                        "signature": method_meta.get("signature", "()"),
                        "doc": method_meta.get("doc", ""),
                    }
                    node = category_node.add(label, data=node_data)

    def _command_group_name(self, target: str, method_name: str) -> str:
        if target == "node":
            return "Node API"
        if method_name in {"create", "open", "close", "disconnect", "delete"}:
            return "Lifecycle"
        if method_name in {"connection", "status", "projects"}:
            return "Node Info"
        if method_name.startswith("test_") or method_name.startswith("clear_") or method_name == "tags":
            return "Metadata"
        if method_name.startswith("task") or method_name.startswith("clear_task"):
            return "Tasks"
        if method_name.startswith("command") or method_name.startswith("cond_") or method_name.startswith("exec_"):
            return "Command"
        if method_name.startswith("import_") or method_name.startswith("add") or method_name.startswith("add_") or method_name.startswith("load") or method_name.startswith("save") or method_name.startswith("new_") or method_name.startswith("align"):
            return "Project IO"
        if method_name.startswith("select") or method_name.startswith("deselect") or method_name.startswith("invert"):
            return "Selection"
        if method_name.startswith("set_") or method_name.startswith("enable_") or method_name.startswith("lock_") or method_name.startswith("get_status") or method_name.startswith("pause_") or method_name.startswith("unpause_") or method_name.startswith("abort_"):
            return "Project Settings"
        if method_name in {"headless", "hide_ui", "show_ui", "start", "quit", "acknowledge_restart"}:
            return "Project Control"
        return "Project Commands"

    async def on_directory_tree_file_selected(self, event: DirectoryTree.FileSelected) -> None:
        await self._add_path(event.path)

    async def on_directory_tree_directory_selected(self, event: DirectoryTree.DirectorySelected) -> None:
        await self._add_path(event.path)

    def on_tree_node_selected(self, event: Tree.NodeSelected) -> None:
        data = getattr(event.node, "data", None)
        if not isinstance(data, dict):
            return
        if data.get("type") != "command":
            return
        self._fill_command_fields_from_tree(data)

    async def on_input_submitted(self, event: Input.Submitted) -> None:
        if event.input.id == "path-input":
            self._add_path_from_input()
            return
        if event.input.id in {
            "command-type-input",
            "command-target-input",
            "command-target-object-input",
            "command-method-input",
            "command-session-input",
            "command-project-guid-input",
            "command-project-name-input",
            "command-args-input",
            "command-kwargs-input",
            "command-timeout-input",
            "command-id-input",
            "command-custom-payload-input",
        }:
            self._refresh_command_config()
            return
        if event.input.id in {"ingest-group-input", "ingest-prefix-input", "ingest-limit-input", "ingest-concurrency-input"}:
            self._refresh_ingest_plan()
            return
        if event.input.id in {
            "job-group-input",
            "job-drone-input",
            "job-max-images-input",
            "job-sdk-folder-input",
            "job-sdk-project-input",
            "job-sdk-detector-input",
            "job-sdk-acc-xyz-input",
            "job-sdk-acc-ypr-input",
            "job-sdk-include-subdirs-input",
            "job-sdk-run-align-input",
            "job-sdk-run-normal-input",
            "job-sdk-run-ortho-input",
            "job-sdk-timeout-input",
            "job-id-input",
        }:
            self._refresh_job_plan()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        button_id = event.button.id
        if button_id == "start-upload-button":
            self._switch_page(self.PAGE_SETUP)
            self._set_status("Upload workflow: Step 1 select folders and click Next.", error=False)
            return
        if button_id == "start-ingest-button":
            self._switch_page(self.PAGE_INGEST)
            self._set_status("Ingest workflow: configure and run server-side ingest.", error=False)
            return
        if button_id == "start-job-button":
            self._switch_page(self.PAGE_JOB)
            self._set_status("Job workflow: configure SDK command sequence and create a processing job.", error=False)
            return
        if button_id == "start-command-button":
            self._switch_page(self.PAGE_COMMAND)
            self._set_status("Command console: build a payload and send to the Redis command queue.", error=False)
            return
        if button_id == "start-close-button":
            self.action_cancel()
            return
        if button_id == "setup-back-button":
            self._switch_page(self.PAGE_START)
            self._set_status("Choose a workflow to begin.", error=False)
            return
        if button_id == "setup-next-button":
            self.action_next_page()
            return
        if button_id == "setup-close-button":
            self.action_cancel()
            return
        if button_id == "add-path":
            self._add_path_from_input()
            return
        if button_id == "undo-path":
            self._undo_last_path()
            return
        if button_id == "clear-paths":
            self.action_clear_selection()
            return
        if button_id == "upload-override-toggle":
            self._toggle_upload_override()
            return
        if button_id == "upload-back-button":
            self.action_previous_page()
            return
        if button_id == "upload-start-button":
            self.action_start_upload()
            return
        if button_id == "upload-close-button":
            self.action_cancel()
            return
        if button_id == "result-new-button":
            self._switch_page(self.PAGE_SETUP)
            self._set_status("Step 1: select folders and click Next.", error=False)
            return
        if button_id == "result-home-button":
            self._switch_page(self.PAGE_START)
            self._set_status("Choose a workflow to begin.", error=False)
            return
        if button_id == "result-close-button":
            self.action_cancel()
            return
        if button_id == "ingest-back-button":
            if self._ingesting:
                self._set_status("Ingest is running. Wait for completion before navigating.", error=True)
                return
            self._switch_page(self.PAGE_START)
            self._set_status("Choose a workflow to begin.", error=False)
            return
        if button_id == "ingest-run-button":
            self._start_ingest_workflow()
            return
        if button_id == "ingest-override-toggle":
            self._toggle_ingest_override()
            return
        if button_id == "ingest-close-button":
            self.action_cancel()
            return
        if button_id == "job-back-button":
            if self._job_running:
                self._set_status("Job request is running. Wait for completion before navigating.", error=True)
                return
            self._switch_page(self.PAGE_START)
            self._set_status("Choose a workflow to begin.", error=False)
            return
        if button_id == "job-create-button":
            self._start_job_create_workflow()
            return
        if button_id == "job-refresh-button":
            self._start_job_refresh_workflow()
            return
        if button_id == "job-cancel-button":
            self._start_job_cancel_workflow()
            return
        if button_id == "job-close-button":
            self.action_cancel()
            return
        if button_id == "command-back-button":
            if self._command_running:
                self._set_status("Command in progress. Wait for completion before navigating.", error=True)
                return
            self._switch_page(self.PAGE_START)
            self._set_status("Choose a workflow to begin.", error=False)
            return
        if button_id == "command-discover-button":
            self._start_command_discover_workflow()
            return
        if button_id == "command-send-button":
            self._start_command_send_workflow()
            return
        if button_id == "command-clear-response-button":
            self._clear_command_results()
            self._set_status("Cleared command response history.", error=False)
            return
        if button_id == "command-close-button":
            self.action_cancel()
            return

    def action_cancel(self) -> None:
        if self._uploading:
            self._set_status("Upload in progress. Wait for completion before closing.", error=True)
            return
        if self._ingesting:
            self._set_status("Ingest in progress. Wait for completion before closing.", error=True)
            return
        if self._job_running:
            self._set_status("Job request in progress. Wait for completion before closing.", error=True)
            return
        if self._command_running:
            self._set_status("Command in progress. Wait for completion before closing.", error=True)
            return
        self._close_command_bus()
        self.exit(None)

    def action_next_page(self) -> None:
        current = self._current_page()
        if current == self.PAGE_START:
            self._switch_page(self.PAGE_SETUP)
            self._set_status("Upload workflow: Step 1 select folders and click Next.", error=False)
            return
        if current == self.PAGE_SETUP:
            self._go_to_upload_page()
            return
        if current == self.PAGE_UPLOAD:
            self.action_start_upload()
            return

    def action_previous_page(self) -> None:
        if self._uploading or self._ingesting or self._job_running or self._command_running:
            self._set_status("Cannot navigate while a workflow is running.", error=True)
            return
        current = self._current_page()
        if current == self.PAGE_SETUP:
            self._switch_page(self.PAGE_START)
            self._set_status("Choose a workflow to begin.", error=False)
            return
        if current == self.PAGE_INGEST:
            self._switch_page(self.PAGE_START)
            self._set_status("Choose a workflow to begin.", error=False)
            return
        if current == self.PAGE_JOB:
            self._switch_page(self.PAGE_START)
            self._set_status("Choose a workflow to begin.", error=False)
            return
        if current == self.PAGE_UPLOAD:
            self._switch_page(self.PAGE_SETUP)
            self._set_status("Step 1: update selection or group, then Next.", error=False)
            return
        if current == self.PAGE_RESULT:
            self._switch_page(self.PAGE_UPLOAD)
            self._set_status("Review upload results or start another upload.", error=False)
            return
        if current == self.PAGE_COMMAND:
            self._switch_page(self.PAGE_START)
            self._set_status("Choose a workflow to begin.", error=False)
            return

    def action_start_upload(self) -> None:
        current = self._current_page()
        if current == self.PAGE_SETUP:
            self._go_to_upload_page()
            if self._current_page() != self.PAGE_UPLOAD:
                return
        if self._current_page() != self.PAGE_UPLOAD:
            return
        self._start_upload()

    def action_clear_selection(self) -> None:
        if self._uploading or self._ingesting or self._job_running:
            self._set_status("Cannot change selection while a workflow is running.", error=True)
            return
        self._selected_paths.clear()
        self._refresh_selected_paths()
        self._refresh_upload_plan()
        self._set_status("Cleared selected paths.", error=False)

    def _current_page(self) -> str:
        switcher = self.query_one("#pages", ContentSwitcher)
        return str(switcher.current)

    def _switch_page(self, page_id: str) -> None:
        switcher = self.query_one("#pages", ContentSwitcher)
        switcher.current = page_id
        title = self.query_one("#page-title", Static)

        if page_id == self.PAGE_START:
            title.update("Workflow Start")
            self.query_one("#start-upload-button", Button).focus()
            return
        if page_id == self.PAGE_SETUP:
            title.update("Step 1 of 3 - Setup")
            self.query_one("#directory-tree", DirectoryTree).focus()
            return
        if page_id == self.PAGE_UPLOAD:
            self._refresh_upload_plan()
            title.update("Step 2 of 3 - Upload")
            self.query_one("#upload-start-button", Button).focus()
            return
        if page_id == self.PAGE_RESULT:
            title.update("Step 3 of 3 - Result")
            self.query_one("#result-new-button", Button).focus()
            return
        if page_id == self.PAGE_INGEST:
            self._refresh_ingest_plan()
            title.update("Ingest Waiting Metadata")
            self.query_one("#ingest-run-button", Button).focus()
            return
        if page_id == self.PAGE_JOB:
            self._refresh_job_plan()
            title.update("Create Processing Job")
            self.query_one("#job-create-button", Button).focus()
            return
        if page_id == self.PAGE_COMMAND:
            self._refresh_command_config()
            self._build_command_tree()
            self._set_command_controls(disabled=False)
            title.update("Command Console")
            self._refresh_command_result()
            self.query_one("#command-type-input", Input).focus()

    def _go_to_upload_page(self) -> None:
        if not self._selected_paths:
            self._set_status("Select at least one folder before continuing.", error=True)
            return
        self._refresh_upload_plan()
        self._switch_page(self.PAGE_UPLOAD)
        self._set_status("Step 2: review plan then Start Upload.", error=False)

    def _set_setup_controls(self, *, disabled: bool) -> None:
        self.query_one("#setup-back-button", Button).disabled = disabled
        self.query_one("#setup-next-button", Button).disabled = disabled
        self.query_one("#add-path", Button).disabled = disabled
        self.query_one("#undo-path", Button).disabled = disabled
        self.query_one("#clear-paths", Button).disabled = disabled
        self.query_one("#upload-override-toggle", Button).disabled = disabled
        self.query_one("#group-input", Input).disabled = disabled
        self.query_one("#path-input", Input).disabled = disabled

    def _set_upload_controls(self, *, disabled: bool) -> None:
        self.query_one("#upload-back-button", Button).disabled = disabled
        self.query_one("#upload-start-button", Button).disabled = disabled

    def _start_upload(self) -> None:
        if self._uploading:
            self._set_status("Upload already in progress.", error=True)
            return
        if not self._selected_paths:
            self._set_status("Select at least one folder before uploading.", error=True)
            return

        group_name = self._read_group_name()
        selected_paths = [str(path) for path in self._selected_paths]
        self._uploading = True
        self._last_group_name = group_name
        self._set_setup_controls(disabled=True)
        self._set_upload_controls(disabled=True)
        self._set_status("Uploading... progress bar is updating live.", error=False)
        self._set_progress_state(0, max(len(selected_paths), 1), 0, 1)
        self._set_progress_detail(
            f"Starting upload for {len(selected_paths)} selected path(s), "
            f"group={group_name or '(none)'}, override_existing={self._override_existing_upload}"
        )

        thread = threading.Thread(
            target=self._run_upload_worker,
            args=(selected_paths, group_name, self._override_existing_upload),
            daemon=True,
        )
        thread.start()

    def _run_upload_worker(
        self,
        selected_paths: Sequence[str],
        group_name: str | None,
        override_existing: bool,
    ) -> None:
        def reporter(message: str) -> None:
            self.call_from_thread(self._set_progress_detail, message)

        def progress(completed: int, total: int, uploaded_bytes: int, total_bytes: int) -> None:
            self.call_from_thread(self._set_progress_state, completed, total, uploaded_bytes, total_bytes)

        try:
            self._upload_handler(selected_paths, group_name, override_existing, reporter, progress)
        except Exception as exc:  # pragma: no cover - runtime path
            self.call_from_thread(self._finish_upload, False, str(exc))
            return

        self.call_from_thread(self._finish_upload, True, None)

    def _finish_upload(self, success: bool, error_message: str | None) -> None:
        self._uploading = False
        self._set_setup_controls(disabled=False)
        self._set_upload_controls(disabled=False)

        if success:
            self._set_status("Upload complete. Review results.", error=False)
            self._set_progress_detail("Upload complete.")
            self._set_result_summary(
                "\n".join(
                    [
                        "Status: SUCCESS",
                        f"Files completed: {self._last_completed}/{self._last_total}",
                        f"Bytes uploaded: {self._last_total_bytes}",
                        f"Group: {self._last_group_name or '(none)'}",
                        f"Override existing: {self._override_existing_upload}",
                    ]
                )
            )
        else:
            self._set_status("Upload failed. Review results.", error=True)
            self._set_progress_detail(f"Upload failed: {error_message}")
            self._set_result_summary(
                "\n".join(
                    [
                        "Status: FAILED",
                        f"Files completed: {self._last_completed}/{self._last_total}",
                        f"Bytes uploaded: {self._last_total_bytes}",
                        f"Group: {self._last_group_name or '(none)'}",
                        f"Override existing: {self._override_existing_upload}",
                        f"Error: {error_message or 'unknown'}",
                    ]
                )
            )

        self._switch_page(self.PAGE_RESULT)

    def _set_ingest_controls(self, *, disabled: bool) -> None:
        self.query_one("#ingest-back-button", Button).disabled = disabled
        self.query_one("#ingest-run-button", Button).disabled = disabled
        self.query_one("#ingest-override-toggle", Button).disabled = disabled
        self.query_one("#ingest-group-input", Input).disabled = disabled
        self.query_one("#ingest-prefix-input", Input).disabled = disabled
        self.query_one("#ingest-limit-input", Input).disabled = disabled
        self.query_one("#ingest-concurrency-input", Input).disabled = disabled

    def _start_ingest_workflow(self) -> None:
        if self._ingesting:
            self._set_status("Ingest already running.", error=True)
            return
        try:
            payload = self._build_ingest_request_payload()
        except ValueError as exc:
            self._set_status(str(exc), error=True)
            return

        self._ingesting = True
        self._set_ingest_controls(disabled=True)
        self._set_status("Running server-side ingest of waiting-bucket metadata...", error=False)
        self._set_ingest_result("Ingest running...")

        thread = threading.Thread(
            target=self._run_ingest_worker,
            args=(payload,),
            daemon=True,
        )
        thread.start()

    def _run_ingest_worker(self, payload: Dict[str, Any]) -> None:
        endpoint = f"{self._config.api.base_url}/images/ingest/waiting"
        try:
            data = self._request_json(
                method="POST",
                endpoint=endpoint,
                payload=payload,
                timeout=600,
            )
        except Exception as exc:  # pragma: no cover - network/runtime path
            self.call_from_thread(self._finish_ingest, False, None, str(exc))
            return

        self.call_from_thread(self._finish_ingest, True, data, None)

    def _finish_ingest(
        self,
        success: bool,
        response_payload: Dict[str, Any] | None,
        error_message: str | None,
    ) -> None:
        self._ingesting = False
        self._set_ingest_controls(disabled=False)
        if success and response_payload is not None:
            self._set_status("Ingest complete. Waiting-bucket metadata persisted.", error=False)
            self._set_ingest_result(json.dumps(response_payload, indent=2))
            return
        self._set_status("Ingest failed. Check server/API configuration.", error=True)
        self._set_ingest_result(f"Ingest failed: {error_message or 'unknown error'}")

    def on_unmount(self) -> None:
        self._close_command_bus()

    def _close_command_bus(self) -> None:
        if self._command_bus is None:
            return
        bus = self._command_bus
        self._command_bus = None
        try:
            bus.close()
        except Exception:
            self._append_log("WARN Failed to close command Redis bus cleanly.")

    def _build_command_bus(self) -> RedisCommandBus:
        if self._command_bus is None:
            self._command_bus = RedisCommandBus(self._config.queue.redis_url)
            try:
                self._command_bus.ping()
            except Exception as exc:
                self._close_command_bus()
                raise RuntimeError(f"Redis unavailable at {self._config.queue.redis_url}: {exc}") from exc
        return self._command_bus

    def _set_command_controls(self, *, disabled: bool) -> None:
        self.query_one("#command-back-button", Button).disabled = disabled
        self.query_one("#command-discover-button", Button).disabled = disabled
        self.query_one("#command-send-button", Button).disabled = disabled
        self.query_one("#command-clear-response-button", Button).disabled = disabled
        self.query_one(f"#{self.COMMAND_TREE_NODE_ID}", Tree).disabled = disabled
        self.query_one("#command-type-input", Input).disabled = disabled
        self.query_one("#command-target-input", Input).disabled = disabled
        self.query_one("#command-target-object-input", Input).disabled = disabled
        self.query_one("#command-method-input", Input).disabled = disabled
        self.query_one("#command-session-input", Input).disabled = disabled
        self.query_one("#command-project-guid-input", Input).disabled = disabled
        self.query_one("#command-project-name-input", Input).disabled = disabled
        self.query_one("#command-args-input", Input).disabled = disabled
        self.query_one("#command-kwargs-input", Input).disabled = disabled
        self.query_one("#command-timeout-input", Input).disabled = disabled
        self.query_one("#command-id-input", Input).disabled = disabled
        self.query_one("#command-custom-payload-input", Input).disabled = disabled

    def _start_command_discover_workflow(self) -> None:
        if self._command_running:
            self._set_status("A command is already running.", error=True)
            return
        try:
            self.query_one("#command-type-input", Input).value = COMMAND_TYPE_RSTOOL_DISCOVER
            payload = self._build_discover_payload()
            command_id = self._read_command_id()
            timeout_seconds = self._resolve_command_timeout()
        except ValueError as exc:
            self._set_status(str(exc), error=True)
            return

        self._command_running = True
        self._last_command_id = command_id
        self._set_command_controls(disabled=True)
        self._clear_command_results()
        self._set_command_result("Sending command discover request...")
        self._append_command_event(f"command_id={command_id} sending type={COMMAND_TYPE_RSTOOL_DISCOVER}")

        thread = threading.Thread(
            target=self._run_command_worker,
            args=(COMMAND_TYPE_RSTOOL_DISCOVER, command_id, payload, timeout_seconds),
            daemon=True,
        )
        thread.start()

    def _start_command_send_workflow(self) -> None:
        if self._command_running:
            self._set_status("A command is already running.", error=True)
            return
        try:
            command_type = self._read_command_type()
            if command_type != COMMAND_TYPE_RSTOOL_COMMAND:
                raise ValueError(f"command type must be {COMMAND_TYPE_RSTOOL_COMMAND} for send")
            payload = self._build_send_payload()
            command_id = self._read_command_id()
            timeout_seconds = self._resolve_command_timeout()
        except ValueError as exc:
            self._set_status(str(exc), error=True)
            return

        self._command_running = True
        self._last_command_id = command_id
        self._set_command_controls(disabled=True)
        self._clear_command_results()
        self._set_command_result("Sending rstool command request...")
        self._append_command_event(f"command_id={command_id} sending type={COMMAND_TYPE_RSTOOL_COMMAND}")

        thread = threading.Thread(
            target=self._run_command_worker,
            args=(command_type, command_id, payload, timeout_seconds),
            daemon=True,
        )
        thread.start()

    def _fill_command_fields_from_tree(self, data: Dict[str, Any]) -> None:
        target = data.get("target", "").strip().lower()
        method = data.get("method", "").strip()
        if not target or not method:
            return

        if self._command_running:
            self._set_status("Stop current workflow? Command is running; wait for completion before editing.", error=True)

        self.query_one("#command-type-input", Input).value = COMMAND_TYPE_RSTOOL_COMMAND
        self.query_one("#command-target-input", Input).value = target
        self.query_one("#command-method-input", Input).value = method
        self.query_one("#command-target-object-input", Input).value = ""
        self.query_one("#command-args-input", Input).value = self._default_args_json_from_signature(
            str(data.get("signature", "()"))
        )
        self.query_one("#command-kwargs-input", Input).value = "{}"
        doc = str(data.get("doc", "")).strip()
        signature = str(data.get("signature", "()")).strip()
        parts = [f"Selected: {target}.{method}{signature}"]
        if doc:
            parts.append(doc)
        self._set_command_result("\n".join(parts))
        self._refresh_command_config()
        self._append_command_event(f"Prefilled command target={target} method={method}")
        self._set_status(f"Selected command: {target}.{method}", error=False)

    def _default_args_json_from_signature(self, signature: str) -> str:
        required_args = self._parse_required_args_from_signature(signature)
        if not required_args:
            return "[]"
        return json.dumps(required_args)

    def _parse_required_args_from_signature(self, signature: str) -> List[str]:
        if not signature:
            return []
        text = signature.strip()
        if not (text.startswith("(") and text.endswith(")")):
            return []
        body = text[1:-1].strip()
        if not body:
            return []

        args: List[str] = []
        depth = 0
        current = []
        chunks = []
        for char in body:
            if char in {"(", "[", "{", "<"}:
                depth += 1
            elif char in {")", "]", "}", ">"}:
                depth -= 1
            if char == "," and depth == 0:
                chunks.append("".join(current).strip())
                current = []
                continue
            current.append(char)
        if current:
            chunks.append("".join(current).strip())

        for chunk in chunks:
            if not chunk or chunk.startswith("*") or chunk.startswith("**") or chunk.startswith("/"):
                continue
            if "=" in chunk or chunk == "self":
                continue
            if ":" in chunk:
                name = chunk.split(":", 1)[0].strip()
            else:
                name = chunk
            if name and name != "self":
                args.append(f"<{name}>")
        return args

    def _read_command_id(self) -> str:
        command_id = self.query_one("#command-id-input", Input).value.strip()
        if command_id:
            return command_id
        generated = str(uuid4())
        self.query_one("#command-id-input", Input).value = generated
        return generated

    def _read_command_type(self) -> str:
        command_type = self.query_one("#command-type-input", Input).value.strip()
        if not command_type:
            return COMMAND_TYPE_RSTOOL_COMMAND
        return command_type

    def _read_stripped(self, widget_id: str) -> str:
        return self.query_one(f"#{widget_id}", Input).value.strip()

    def _resolve_command_timeout(self) -> int:
        raw = self._read_stripped("command-timeout-input")
        if not raw:
            return max(self._config.control.request_timeout_seconds, 1)
        try:
            value = int(raw)
        except ValueError as exc:
            raise ValueError("command timeout must be an integer in seconds") from exc
        if value < 1:
            raise ValueError("command timeout must be at least 1 second")
        return value

    def _parse_json_object(self, raw: str, *, field: str) -> Dict[str, Any]:
        if not raw:
            return {}
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ValueError(f"invalid {field}: not valid JSON") from exc
        if not isinstance(parsed, dict):
            raise ValueError(f"invalid {field}: must be a JSON object")
        return parsed

    def _parse_json_array(self, raw: str, *, field: str) -> list[Any]:
        if not raw:
            return []
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ValueError(f"invalid {field}: not valid JSON") from exc
        if not isinstance(parsed, list):
            raise ValueError(f"invalid {field}: must be a JSON array")
        return parsed

    def _custom_payload(self) -> Dict[str, Any]:
        custom_raw = self._read_stripped("command-custom-payload-input")
        return self._parse_json_object(custom_raw, field="command-custom-payload")

    def _build_discover_payload(self) -> Dict[str, Any]:
        payload: Dict[str, Any] = {}
        target = self._read_stripped("command-target-input")
        if target:
            payload["target"] = target
        payload.update(self._custom_payload())
        return payload

    def _build_send_payload(self) -> Dict[str, Any]:
        command_type = self._read_stripped("command-type-input")
        if not command_type:
            command_type = COMMAND_TYPE_RSTOOL_COMMAND
        target = self._read_stripped("command-target-input") or "node"
        method = self._read_stripped("command-method-input")
        if command_type == COMMAND_TYPE_RSTOOL_COMMAND and not method:
            raise ValueError("method is required for rstool commands")

        payload: Dict[str, Any] = {
            "target": target,
        }
        if target in {"node", "project", "client"}:
            pass
        else:
            raise ValueError("target must be one of node, project, or client")

        if method:
            payload["method"] = method
        args = self._read_stripped("command-args-input")
        kwargs = self._read_stripped("command-kwargs-input")
        payload["args"] = self._parse_json_array(args, field="command-args")
        payload["kwargs"] = self._parse_json_object(kwargs, field="command-kwargs")

        target_object = self._read_stripped("command-target-object-input")
        if target_object:
            payload["target_object"] = target_object

        session = self._read_stripped("command-session-input")
        if session:
            payload["session"] = session

        project_guid = self._read_stripped("command-project-guid-input")
        project_name = self._read_stripped("command-project-name-input")
        if project_guid:
            payload["project_guid"] = project_guid
        if project_name:
            payload["project_name"] = project_name
        if method == "open" and project_guid:
            payload["session_action"] = "open"
        if method:
            payload["method"] = method

        custom = self._custom_payload()
        if custom:
            payload.update(custom)
        return payload

    def _run_command_worker(
        self,
        command_type: str,
        command_id: str,
        payload: Dict[str, Any],
        timeout_seconds: int,
    ) -> None:
        reply_to = f"{self._config.control.result_queue_key}:reply:{command_id}"
        command = ProcessingCommand(
            command_id=command_id,
            command_type=command_type,
            payload=payload,
            reply_to=reply_to,
        )
        try:
            bus = self._build_command_bus()
            bus.push(
                self._config.control.command_queue_key,
                command.to_payload(),
                expire_seconds=self._config.control.result_ttl_seconds,
            )
        except Exception as exc:
            self.call_from_thread(
                self._finish_command_workflow,
                False,
                command_id,
                {"status": "error", "message": "Failed to publish Redis command", "error": str(exc)},
            )
            return

        self.call_from_thread(self._append_command_event, f"Published command {command_id} -> queue={self._config.control.command_queue_key}")
        self.call_from_thread(
            self._set_status,
            f"Command sent (id={command_id}); waiting up to {timeout_seconds}s for responses.",
            error=False,
        )

        deadline = time.monotonic() + timeout_seconds
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                self.call_from_thread(
                    self._finish_command_workflow,
                    False,
                    command_id,
                    {"status": "error", "message": "Timeout waiting for command response"},
                )
                return
            bus = self._build_command_bus()
            try:
                raw = bus.pop(reply_to, timeout_seconds=max(1, min(10, int(remaining))))
            except Exception as exc:  # pragma: no cover - redis runtime edge case
                self.call_from_thread(
                    self._append_command_event,
                    f"ERROR during pop reply queue={reply_to}: {exc}",
                )
                continue
            if raw is None:
                self.call_from_thread(
                    self._append_command_event,
                    f"waiting for response... remaining={remaining:.1f}s",
                )
                continue
            try:
                event = ProcessingCommandResult.parse(raw)
            except Exception as exc:
                self.call_from_thread(
                    self._append_command_event,
                    f"Discarding invalid result payload for command_id={command_id}: {exc}",
                )
                continue
            if event.command_id != command_id:
                continue

            self.call_from_thread(self._append_command_event, self._format_command_event(event))
            event_payload = json.dumps(event.to_payload(), indent=2)
            self.call_from_thread(self._set_command_result, event_payload)

            status = (event.status or "").lower()
            if status in {RESULT_STATUS_ACCEPTED, RESULT_STATUS_PROGRESS}:
                continue
            if status == RESULT_STATUS_OK:
                if command_type == COMMAND_TYPE_RSTOOL_DISCOVER:
                    self.call_from_thread(self._apply_discovered_command_catalog, event.to_payload())
                self.call_from_thread(self._finish_command_workflow, True, command_id, event.to_payload())
                return
            self.call_from_thread(
                self._finish_command_workflow,
                False,
                command_id,
                event.to_payload(),
            )
            return

    def _format_command_event(self, event: ProcessingCommandResult) -> str:
        parts = [
            f"command_id={event.command_id}",
            f"status={event.status}",
            f"type={event.command_type}",
        ]
        if event.message:
            parts.append(f"message={event.message}")
        if event.progress is not None:
            parts.append(f"progress={event.progress}")
        if event.error:
            parts.append(f"error={event.error}")
        if event.data:
            parts.append(f"data_keys={','.join(sorted(map(str, event.data.keys())))}")
        return " | ".join(parts)

    def _finish_command_workflow(
        self,
        success: bool,
        command_id: str,
        response: Dict[str, Any] | None,
    ) -> None:
        self._command_running = False
        self._set_command_controls(disabled=False)
        if success:
            self._set_status(
                f"Command {command_id} completed.",
                error=False,
            )
            if response is not None:
                self._set_command_result(json.dumps(response, indent=2))
            return
        message = "Command request failed."
        if response:
            if response.get("error"):
                message = str(response["error"])
            elif response.get("message"):
                message = str(response["message"])
            self._set_command_result(json.dumps(response, indent=2))
        self._set_status(message, error=True)

    def _refresh_command_config(self) -> None:
        command_type = self._read_stripped("command-type-input") or COMMAND_TYPE_RSTOOL_DISCOVER
        target = self._read_stripped("command-target-input") or "node"
        target_object = self._read_stripped("command-target-object-input")
        method = self._read_stripped("command-method-input")
        timeout = self._resolve_command_timeout()
        command_id = self._read_stripped("command-id-input") or "(auto)"
        args_raw = self._read_stripped("command-args-input") or "[]"
        kwargs_raw = self._read_stripped("command-kwargs-input") or "{}"
        custom_raw = self._read_stripped("command-custom-payload-input")

        lines = [
            f"Command Type: {command_type}",
            f"Queue: {self._config.control.command_queue_key}",
            f"Result Queue: {self._config.control.result_queue_key}",
            f"Reply suffix: :reply:{command_id}",
            f"Target: {target}",
            f"Target Object: {target_object or '(none)'}",
            f"Method: {method or '(none)'}",
            f"Session: {self._read_stripped('command-session-input') or '(none)'}",
            f"Project GUID: {self._read_stripped('command-project-guid-input') or '(none)'}",
            f"Project Name: {self._read_stripped('command-project-name-input') or '(none)'}",
            f"timeout: {timeout}s",
            f"args example: {args_raw}",
            f"kwargs example: {kwargs_raw}",
            f"custom payload: {custom_raw or '(none)'}",
        ]
        self.query_one("#command-config", Static).update("\n".join(lines))

    def _clear_command_results(self) -> None:
        self._command_events = []
        self._set_command_history("")
        self._set_command_result("No command run yet.")

    def _set_command_result(self, text: str) -> None:
        self.query_one("#command-result", Static).update(text)

    def _refresh_command_result(self) -> None:
        if self._command_events:
            self._set_command_history("\n".join(self._command_events[-80:]))
        else:
            self._set_command_history("")

    def _set_command_history(self, text: str) -> None:
        self.query_one("#command-history", Static).update(text)

    def _append_command_event(self, text: str) -> None:
        self._command_events.append(text)
        if len(self._command_events) > 120:
            self._command_events = self._command_events[-120:]
        self._set_command_history("\n".join(self._command_events))

    def _apply_discovered_command_catalog(self, event_payload: Dict[str, Any]) -> None:
        data = event_payload.get("data")
        if not isinstance(data, dict):
            return
        available = data.get("available")
        if not isinstance(available, dict):
            return
        updated = False
        for target in ("node", "project"):
            methods = available.get(target)
            if not isinstance(methods, dict):
                continue
            catalog: Dict[str, Dict[str, str]] = {}
            for method_name, method_info in methods.items():
                if not isinstance(method_name, str):
                    continue
                if not isinstance(method_info, dict):
                    continue
                signature = str(method_info.get("signature") or "()").strip()
                doc = str(method_info.get("doc") or "").strip()
                catalog[method_name] = {"signature": signature, "doc": doc}
            if catalog:
                self._command_catalog[target] = catalog
                updated = True
        if updated:
            self.call_from_thread(self._build_command_tree)
            self.call_from_thread(
                self._append_command_event,
                "Updated command tree from discover response.",
            )

    def _set_job_controls(self, *, disabled: bool) -> None:
        self.query_one("#job-back-button", Button).disabled = disabled
        self.query_one("#job-create-button", Button).disabled = disabled
        self.query_one("#job-refresh-button", Button).disabled = disabled
        self.query_one("#job-cancel-button", Button).disabled = disabled
        self.query_one("#job-group-input", Input).disabled = disabled
        self.query_one("#job-drone-input", Input).disabled = disabled
        self.query_one("#job-max-images-input", Input).disabled = disabled
        self.query_one("#job-sdk-folder-input", Input).disabled = disabled
        self.query_one("#job-sdk-project-input", Input).disabled = disabled
        self.query_one("#job-sdk-detector-input", Input).disabled = disabled
        self.query_one("#job-sdk-acc-xyz-input", Input).disabled = disabled
        self.query_one("#job-sdk-acc-ypr-input", Input).disabled = disabled
        self.query_one("#job-sdk-include-subdirs-input", Input).disabled = disabled
        self.query_one("#job-sdk-run-align-input", Input).disabled = disabled
        self.query_one("#job-sdk-run-normal-input", Input).disabled = disabled
        self.query_one("#job-sdk-run-ortho-input", Input).disabled = disabled
        self.query_one("#job-sdk-timeout-input", Input).disabled = disabled
        self.query_one("#job-id-input", Input).disabled = disabled

    def _start_job_create_workflow(self) -> None:
        if self._job_running:
            self._set_status("Job request already in progress.", error=True)
            return
        try:
            payload = self._build_job_create_payload()
        except ValueError as exc:
            self._set_status(str(exc), error=True)
            return

        self._job_running = True
        self._set_job_controls(disabled=True)
        self._set_status("Creating processing job via API...", error=False)
        self._set_job_result("Submitting job create request...")

        thread = threading.Thread(
            target=self._run_job_create_worker,
            args=(payload,),
            daemon=True,
        )
        thread.start()

    def _start_job_refresh_workflow(self) -> None:
        if self._job_running:
            self._set_status("Job request already in progress.", error=True)
            return
        job_id = self._read_job_id_for_action()
        if not job_id:
            self._set_status("Set a Job ID first (or create a job).", error=True)
            return

        self._job_running = True
        self._set_job_controls(disabled=True)
        self._set_status(f"Refreshing status for job {job_id}...", error=False)
        self._set_job_result("Fetching job status...")

        thread = threading.Thread(
            target=self._run_job_refresh_worker,
            args=(job_id,),
            daemon=True,
        )
        thread.start()

    def _start_job_cancel_workflow(self) -> None:
        if self._job_running:
            self._set_status("Job request already in progress.", error=True)
            return
        job_id = self._read_job_id_for_action()
        if not job_id:
            self._set_status("Set a Job ID first (or create a job).", error=True)
            return

        self._job_running = True
        self._set_job_controls(disabled=True)
        self._set_status(f"Cancelling job {job_id}...", error=False)
        self._set_job_result("Submitting cancellation request...")

        thread = threading.Thread(
            target=self._run_job_cancel_worker,
            args=(job_id,),
            daemon=True,
        )
        thread.start()

    def _run_job_create_worker(self, payload: Dict[str, Any]) -> None:
        endpoint = f"{self._config.api.base_url}/jobs"
        try:
            data = self._request_json(method="POST", endpoint=endpoint, payload=payload, timeout=120)
        except Exception as exc:
            self.call_from_thread(self._finish_job_action, False, None, str(exc))
            return
        self.call_from_thread(self._finish_job_action, True, data, None)

    def _run_job_refresh_worker(self, job_id: str) -> None:
        endpoint = f"{self._config.api.base_url}/jobs/{job_id}"
        try:
            data = self._request_json(method="GET", endpoint=endpoint, payload=None, timeout=120)
        except Exception as exc:
            self.call_from_thread(self._finish_job_action, False, None, str(exc))
            return
        self.call_from_thread(self._finish_job_action, True, data, None)

    def _run_job_cancel_worker(self, job_id: str) -> None:
        endpoint = f"{self._config.api.base_url}/jobs/{job_id}/cancel"
        try:
            data = self._request_json(method="POST", endpoint=endpoint, payload=None, timeout=120)
        except Exception as exc:
            self.call_from_thread(self._finish_job_action, False, None, str(exc))
            return
        self.call_from_thread(self._finish_job_action, True, data, None)

    def _finish_job_action(
        self,
        success: bool,
        response_payload: Dict[str, Any] | None,
        error_message: str | None,
    ) -> None:
        self._job_running = False
        self._set_job_controls(disabled=False)

        if success and response_payload is not None:
            maybe_job_id = str(response_payload.get("id") or response_payload.get("job_id") or "").strip() or None
            if maybe_job_id:
                self._last_job_id = maybe_job_id
                self.query_one("#job-id-input", Input).value = maybe_job_id
            self._refresh_job_plan()
            self._set_status("Job workflow request complete.", error=False)
            self._set_job_result(json.dumps(response_payload, indent=2))
            return

        self._set_status("Job workflow request failed. Check API/server logs.", error=True)
        self._set_job_result(f"Job request failed: {error_message or 'unknown error'}")

    def _build_job_create_payload(self) -> Dict[str, Any]:
        group_raw = self.query_one("#job-group-input", Input).value.strip()
        drone_raw = self.query_one("#job-drone-input", Input).value.strip()
        max_images_raw = self.query_one("#job-max-images-input", Input).value.strip()
        sdk_folder_raw = self.query_one("#job-sdk-folder-input", Input).value.strip()
        sdk_project_raw = self.query_one("#job-sdk-project-input", Input).value.strip()
        sdk_detector_raw = self.query_one("#job-sdk-detector-input", Input).value.strip()
        sdk_acc_xyz_raw = self.query_one("#job-sdk-acc-xyz-input", Input).value.strip()
        sdk_acc_ypr_raw = self.query_one("#job-sdk-acc-ypr-input", Input).value.strip()
        sdk_include_subdirs_raw = self.query_one("#job-sdk-include-subdirs-input", Input).value.strip()
        sdk_run_align_raw = self.query_one("#job-sdk-run-align-input", Input).value.strip()
        sdk_run_normal_raw = self.query_one("#job-sdk-run-normal-input", Input).value.strip()
        sdk_run_ortho_raw = self.query_one("#job-sdk-run-ortho-input", Input).value.strip()
        sdk_timeout_raw = self.query_one("#job-sdk-timeout-input", Input).value.strip()

        if not sdk_folder_raw:
            raise ValueError("sdk_imagery_folder is required (path visible to RealityScan node)")

        payload: Dict[str, Any] = {}
        if group_raw:
            payload["group_name"] = group_raw
        if drone_raw:
            payload["drone_type"] = drone_raw

        max_images = self._parse_optional_positive_int(max_images_raw, field_name="max_images")
        if max_images is not None:
            payload["max_images"] = max_images

        sdk_include_subdirs = self._parse_bool(sdk_include_subdirs_raw, field_name="sdk_include_subdirs")
        sdk_run_align = self._parse_bool(sdk_run_align_raw, field_name="sdk_run_align")
        sdk_run_normal_model = self._parse_bool(sdk_run_normal_raw, field_name="sdk_run_normal_model")
        sdk_run_ortho_projection = self._parse_bool(sdk_run_ortho_raw, field_name="sdk_run_ortho_projection")

        if not (sdk_run_align or sdk_run_normal_model or sdk_run_ortho_projection):
            raise ValueError("At least one SDK stage must be enabled: align, normal model, or ortho projection")

        payload["sdk_imagery_folder"] = sdk_folder_raw
        if sdk_project_raw:
            payload["sdk_project_path"] = sdk_project_raw
        if sdk_detector_raw:
            payload["sdk_detector_sensitivity"] = sdk_detector_raw

        sdk_acc_xyz = self._parse_optional_float(sdk_acc_xyz_raw, field_name="sdk_camera_prior_accuracy_xyz")
        if sdk_acc_xyz is not None:
            payload["sdk_camera_prior_accuracy_xyz"] = sdk_acc_xyz

        sdk_acc_ypr = self._parse_optional_float(
            sdk_acc_ypr_raw,
            field_name="sdk_camera_prior_accuracy_yaw_pitch_roll",
        )
        if sdk_acc_ypr is not None:
            payload["sdk_camera_prior_accuracy_yaw_pitch_roll"] = sdk_acc_ypr

        payload["sdk_include_subdirs"] = sdk_include_subdirs
        payload["sdk_run_align"] = sdk_run_align
        payload["sdk_run_normal_model"] = sdk_run_normal_model
        payload["sdk_run_ortho_projection"] = sdk_run_ortho_projection

        sdk_timeout = self._parse_optional_positive_int(
            sdk_timeout_raw,
            field_name="sdk_task_timeout_seconds",
        )
        if sdk_timeout is not None:
            payload["sdk_task_timeout_seconds"] = sdk_timeout
        return payload

    def _read_job_id_for_action(self) -> str | None:
        raw = self.query_one("#job-id-input", Input).value.strip()
        if raw:
            return raw
        return self._last_job_id

    @staticmethod
    def _parse_optional_positive_int(raw: str, *, field_name: str) -> int | None:
        if not raw:
            return None
        value = UploadWizardApp._parse_positive_int(raw, field_name=field_name)
        return value

    @staticmethod
    def _parse_optional_float(raw: str, *, field_name: str) -> float | None:
        if not raw:
            return None
        try:
            return float(raw)
        except ValueError as exc:
            raise ValueError(f"{field_name} must be a number") from exc

    @staticmethod
    def _parse_bool(raw: str, *, field_name: str) -> bool:
        if not raw:
            raise ValueError(f"{field_name} must be true or false")
        rendered = raw.strip().lower()
        if rendered in {"1", "true", "t", "yes", "y", "on"}:
            return True
        if rendered in {"0", "false", "f", "no", "n", "off"}:
            return False
        raise ValueError(f"{field_name} must be true or false")

    def _refresh_job_plan(self) -> None:
        group_raw = self.query_one("#job-group-input", Input).value.strip()
        drone_raw = self.query_one("#job-drone-input", Input).value.strip()
        max_images_raw = self.query_one("#job-max-images-input", Input).value.strip()
        sdk_folder_raw = self.query_one("#job-sdk-folder-input", Input).value.strip()
        sdk_project_raw = self.query_one("#job-sdk-project-input", Input).value.strip()
        sdk_detector_raw = self.query_one("#job-sdk-detector-input", Input).value.strip()
        sdk_acc_xyz_raw = self.query_one("#job-sdk-acc-xyz-input", Input).value.strip()
        sdk_acc_ypr_raw = self.query_one("#job-sdk-acc-ypr-input", Input).value.strip()
        sdk_include_subdirs_raw = self.query_one("#job-sdk-include-subdirs-input", Input).value.strip()
        sdk_run_align_raw = self.query_one("#job-sdk-run-align-input", Input).value.strip()
        sdk_run_normal_raw = self.query_one("#job-sdk-run-normal-input", Input).value.strip()
        sdk_run_ortho_raw = self.query_one("#job-sdk-run-ortho-input", Input).value.strip()
        sdk_timeout_raw = self.query_one("#job-sdk-timeout-input", Input).value.strip()
        job_id_raw = self.query_one("#job-id-input", Input).value.strip() or (self._last_job_id or "")

        rstools_mode = (self._config.rstools.mode or "stub").strip().lower()
        sdk_ready = all(
            (
                (self._config.rstools.sdk_base_url or "").strip(),
                (self._config.rstools.sdk_client_id or "").strip(),
                (self._config.rstools.sdk_app_token or "").strip(),
                (self._config.rstools.sdk_auth_token or "").strip(),
            )
        )

        lines = [
            f"Server URL: {self._config.api.base_url}",
            f"Runner mode: {rstools_mode}",
            f"SDK credentials complete: {sdk_ready}",
            "SDK sequence: newScene -> set -> addFolder -> align/model/ortho -> save",
            f"Group: {group_raw or '(API default group)'}",
            f"Drone type: {drone_raw or '(any)'}",
            f"Max images: {max_images_raw or '(no limit)'}",
            f"addFolder path (node-visible): {sdk_folder_raw or '(required)'}",
            f"Save project path: {sdk_project_raw or '(auto: <job_id>.rspj)'}",
            f"Detector sensitivity: {sdk_detector_raw or '(Ultra)'}",
            f"Camera prior accuracy X/Y/Z: {sdk_acc_xyz_raw or '(0.1)'}",
            f"Camera prior accuracy Yaw/Pitch/Roll: {sdk_acc_ypr_raw or '(1.0)'}",
            f"Include subdirectories: {sdk_include_subdirs_raw or '(true)'}",
            f"Run align: {sdk_run_align_raw or '(true)'}",
            f"Run normal model: {sdk_run_normal_raw or '(true)'}",
            f"Run ortho projection: {sdk_run_ortho_raw or '(true)'}",
            f"Task timeout seconds: {sdk_timeout_raw or '(7200)'}",
            f"Active job id: {job_id_raw or '(none)'}",
        ]
        self.query_one("#job-config", Static).update("\n".join(lines))

    def _set_job_result(self, text: str) -> None:
        self.query_one("#job-result", Static).update(text)

    def _request_json(
        self,
        *,
        method: str,
        endpoint: str,
        payload: Dict[str, Any] | None,
        timeout: int,
    ) -> Dict[str, Any]:
        encoded_payload = json.dumps(payload).encode("utf-8") if payload is not None else None
        headers = {"Content-Type": "application/json"} if payload is not None else {}
        request = urllib_request.Request(
            endpoint,
            data=encoded_payload,
            headers=headers,
            method=method,
        )
        try:
            with urllib_request.urlopen(request, timeout=timeout) as response:
                body = response.read().decode("utf-8").strip()
        except urllib_error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace").strip()
            message = detail or exc.reason or "HTTP error"
            raise RuntimeError(f"{exc.code} {message}") from exc

        if not body:
            return {}
        parsed = json.loads(body)
        if not isinstance(parsed, dict):
            return {"data": parsed}
        return parsed

    def _build_ingest_request_payload(self) -> Dict[str, Any]:
        group_raw = self.query_one("#ingest-group-input", Input).value.strip()
        prefix_raw = self.query_one("#ingest-prefix-input", Input).value.strip()
        limit_raw = self.query_one("#ingest-limit-input", Input).value.strip() or "1000"
        concurrency_raw = self.query_one("#ingest-concurrency-input", Input).value.strip() or "24"

        limit = self._parse_positive_int(limit_raw, field_name="limit")
        concurrency = self._parse_positive_int(concurrency_raw, field_name="concurrency")

        payload: Dict[str, Any] = {
            "limit": limit,
            "concurrency": concurrency,
            "override_existing": self._override_existing_ingest,
        }
        if group_raw:
            payload["group_name"] = group_raw
        if prefix_raw:
            payload["prefix"] = prefix_raw
        return payload

    @staticmethod
    def _parse_positive_int(raw: str, *, field_name: str) -> int:
        try:
            value = int(raw)
        except ValueError as exc:
            raise ValueError(f"{field_name} must be an integer") from exc
        if value < 1:
            raise ValueError(f"{field_name} must be at least 1")
        return value

    def _refresh_ingest_plan(self) -> None:
        group_raw = self.query_one("#ingest-group-input", Input).value.strip()
        prefix_raw = self.query_one("#ingest-prefix-input", Input).value.strip()
        limit_raw = self.query_one("#ingest-limit-input", Input).value.strip() or "1000"
        concurrency_raw = self.query_one("#ingest-concurrency-input", Input).value.strip() or "24"

        lines = [
            f"Server URL: {self._config.api.base_url}",
            f"Bucket: {self._config.s3.bucket_name}",
            f"Group override: {group_raw or '(use metadata/default)'}",
            f"Prefix: {prefix_raw or '(all waiting objects)'}",
            f"Limit: {limit_raw}",
            f"Concurrency: {concurrency_raw}",
            f"Override existing: {self._override_existing_ingest}",
        ]
        self.query_one("#ingest-config", Static).update("\n".join(lines))

    def _set_ingest_result(self, text: str) -> None:
        self.query_one("#ingest-result", Static).update(text)

    def _set_progress_state(self, completed: int, total: int, uploaded_bytes: int, total_bytes: int) -> None:
        safe_total = max(total, 1)
        bounded_completed = min(max(completed, 0), safe_total)
        safe_target_bytes = max(total_bytes, 1)
        bounded_uploaded_bytes = min(max(uploaded_bytes, 0), safe_target_bytes)
        effective_progress_bytes = max(
            bounded_uploaded_bytes,
            int((bounded_completed / safe_total) * safe_target_bytes),
        )
        effective_progress_bytes = min(effective_progress_bytes, safe_target_bytes)
        self._last_completed = bounded_completed
        self._last_total = safe_total
        self._last_total_bytes = bounded_uploaded_bytes
        self._last_target_bytes = safe_target_bytes
        bar = self.query_one("#upload-progress", ProgressBar)
        bar.update(total=float(safe_target_bytes), progress=float(effective_progress_bytes))
        percentage = (effective_progress_bytes / safe_target_bytes) * 100 if safe_target_bytes > 0 else 0.0
        self._set_progress_detail(
            " | ".join(
                [
                    f"Files {bounded_completed}/{safe_total}",
                    f"Bytes {bounded_uploaded_bytes}/{safe_target_bytes}",
                    f"{percentage:.1f}%",
                ]
            )
        )

    def _set_progress_detail(self, message: str) -> None:
        self.query_one("#progress-detail", Static).update(message)

    def _read_group_name(self) -> str | None:
        raw = self.query_one("#group-input", Input).value.strip()
        return raw or None

    def _refresh_upload_plan(self) -> None:
        summary = self.query_one("#upload-plan", Static)
        group = self._read_group_name() or "(none)"
        lines = [
            f"Bucket: {self._config.s3.bucket_name} (locked)",
            f"Prefix: {self._config.s3.scratchpad_prefix}",
            f"Selected paths: {len(self._selected_paths)}",
            f"Group: {group}",
            f"Override existing: {self._override_existing_upload}",
            "",
            "Paths:",
        ]
        if not self._selected_paths:
            lines.append("- none")
        else:
            preview = self._selected_paths[:6]
            lines.extend(f"- {path}" for path in preview)
            remaining = len(self._selected_paths) - len(preview)
            if remaining > 0:
                lines.append(f"- ... and {remaining} more")
        summary.update("\n".join(lines))

    def _set_result_summary(self, text: str) -> None:
        self.query_one("#result-summary", Static).update(text)

    def _add_path_from_input(self) -> None:
        if self._uploading or self._ingesting or self._job_running:
            self._set_status("Cannot change selection while a workflow is running.", error=True)
            return
        input_widget = self.query_one("#path-input", Input)
        raw = input_widget.value.strip()
        if not raw:
            self._set_status("Enter a file or folder path first.", error=True)
            return
        candidate = Path(raw).expanduser()
        if not candidate.is_absolute():
            candidate = (self._root_path / candidate).resolve()
        else:
            candidate = candidate.resolve()
        if not candidate.exists():
            self._set_status(f"Path not found: {candidate}", error=True)
            return
        if candidate in self._selected_paths:
            self._set_status(f"Already selected: {candidate}", error=False)
            return
        self._selected_paths.append(candidate)
        input_widget.value = ""
        self._refresh_selected_paths()
        self._refresh_upload_plan()
        self._set_status(f"Added: {candidate}", error=False)

    async def _add_path(self, path: Path) -> None:
        if self._uploading or self._ingesting or self._job_running:
            self._set_status("Cannot change selection while a workflow is running.", error=True)
            return
        normalized = path.resolve()
        if normalized in self._selected_paths:
            self._set_status(f"Already selected: {normalized}", error=False)
            return

        self._selected_paths.append(normalized)
        self._refresh_selected_paths()
        self._refresh_upload_plan()
        self._set_status(f"Added: {normalized}", error=False)

    def _undo_last_path(self) -> None:
        if self._uploading or self._ingesting or self._job_running:
            self._set_status("Cannot change selection while a workflow is running.", error=True)
            return
        if not self._selected_paths:
            self._set_status("No selected paths to remove.", error=True)
            return
        removed = self._selected_paths.pop()
        self._refresh_selected_paths()
        self._refresh_upload_plan()
        self._set_status(f"Removed: {removed}", error=False)

    def _refresh_selected_paths(self) -> None:
        output = self.query_one("#selected-output", Static)
        if not self._selected_paths:
            output.update("No folders selected.")
            return
        lines = [f"{index + 1}. {path}" for index, path in enumerate(self._selected_paths)]
        output.update("\n".join(lines))

    def _set_status(self, message: str, *, error: bool) -> None:
        status = self.query_one("#status", Static)
        status.remove_class("ok")
        status.remove_class("error")
        status.add_class("error" if error else "ok")
        status.update(message)
        level = "ERROR" if error else "INFO"
        self._append_log(f"{level} {message}")

    def _append_log(self, message: str) -> None:
        timestamp = datetime.now().strftime("%H:%M:%S")
        lines = [line.strip() for line in message.splitlines() if line.strip()]
        if not lines:
            lines = [message.strip()]
        for line in lines:
            if not line:
                continue
            self._log_lines.append(f"{timestamp} {line}")
        if len(self._log_lines) > self.LOG_ROW_COUNT:
            self._log_lines = self._log_lines[-self.LOG_ROW_COUNT :]
        self._refresh_log_bar()

    def _refresh_log_bar(self) -> None:
        log_bar = self.query_one("#log-bar", Static)
        lines = list(self._log_lines[-self.LOG_ROW_COUNT :])
        if len(lines) < self.LOG_ROW_COUNT:
            lines = ([""] * (self.LOG_ROW_COUNT - len(lines))) + lines
        log_bar.update("\n".join(lines))

    def _set_upload_override_button_label(self) -> None:
        label = "Override Existing Uploads: ON" if self._override_existing_upload else "Override Existing Uploads: OFF"
        self.query_one("#upload-override-toggle", Button).label = label

    def _set_ingest_override_button_label(self) -> None:
        label = "Override Existing Ingest: ON" if self._override_existing_ingest else "Override Existing Ingest: OFF"
        self.query_one("#ingest-override-toggle", Button).label = label

    def _toggle_upload_override(self) -> None:
        if self._uploading:
            self._set_status("Cannot change upload override while upload is running.", error=True)
            return
        self._override_existing_upload = not self._override_existing_upload
        self._set_upload_override_button_label()
        self._refresh_upload_plan()
        self._set_status(f"Upload override set to {self._override_existing_upload}.", error=False)

    def _toggle_ingest_override(self) -> None:
        if self._ingesting:
            self._set_status("Cannot change ingest override while ingest is running.", error=True)
            return
        self._override_existing_ingest = not self._override_existing_ingest
        self._set_ingest_override_button_label()
        self._refresh_ingest_plan()
        self._set_status(f"Ingest override set to {self._override_existing_ingest}.", error=False)


def run_upload_wizard(
    config: AppConfig,
    upload_handler: UploadHandler,
    initial_group_name: str | None = None,
) -> None:
    """Run the TUI wizard and keep the UI open until user closes it."""
    UploadWizardApp(config, upload_handler, initial_group_name=initial_group_name).run()
