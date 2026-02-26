"""Textual-powered interactive upload wizard with multi-page flow."""

from __future__ import annotations

import json
import threading
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Sequence
from urllib import error as urllib_error
from urllib import request as urllib_request

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
    Static,
)

from config import AppConfig

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
    LOG_ROW_COUNT = 3

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

    #setup-action-row, #upload-action-row, #result-action-row, #start-action-row, #ingest-action-row, #job-action-row {
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
        self._last_job_id: str | None = None
        self._last_completed = 0
        self._last_total = 0
        self._last_total_bytes = 0
        self._last_target_bytes = 1
        self._last_group_name: str | None = None
        self._override_existing_upload = False
        self._override_existing_ingest = False
        self._log_lines: List[str] = []

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
                            ]
                        ),
                        id="start-options",
                    )

                with Horizontal(id="start-action-row"):
                    yield Button("Upload Imagery", variant="primary", id="start-upload-button")
                    yield Button("Ingest Waiting Metadata", variant="success", id="start-ingest-button")
                    yield Button("Create Processing Job", variant="warning", id="start-job-button")
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
        self._set_progress_state(0, 1, 0, 1)
        self._set_result_summary("No uploads run yet.")
        self._switch_page(self.PAGE_START)
        self._set_status("Choose a workflow to begin.", error=False)

    async def on_directory_tree_file_selected(self, event: DirectoryTree.FileSelected) -> None:
        await self._add_path(event.path)

    async def on_directory_tree_directory_selected(self, event: DirectoryTree.DirectorySelected) -> None:
        await self._add_path(event.path)

    async def on_input_submitted(self, event: Input.Submitted) -> None:
        if event.input.id == "path-input":
            self._add_path_from_input()
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
        if self._uploading or self._ingesting or self._job_running:
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
