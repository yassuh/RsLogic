RsLogic execution architecture

## Top-level packages

- `rslogic/api/server.py` is the orchestrator control plane entrypoint (`rslogic-api`).
  - Results consumer now normalizes and merges `task_state` / `project_status` fields from client payloads into `result_summary` by message timestamp, so `/jobs` and `/jobs/{job_id}` expose cumulative task snapshots and latest project state.
- `rslogic/cli/upload.py` is the CLI upload entrypoint (`rslogic-upload`).
- `rslogic/ingest.py` ingests objects from `drone-imagery-waiting` into `drone-imagery` and writes rows into `studio-db` (`rslogic-ingest`).
  - `rslogic/client/runtime.py` is the standalone client worker (`rslogic-client`, `rslogic-worker`).
  - Added per-step heartbeat result messages (default every 3s) while a long-running step is executing so Redis users and operators can see active progress with `step_index`, `step_action`, and elapsed time.
  - Added structured runtime logs to stdout/stderr (job_id, step index/action, params preview, and per-step durations) for high-frequency local diagnosis.
  - Step result payloads are now surfaced in Redis progress events (`result_summary`) so `sdk_project_status`, `sdk_project_command`, `align`, etc. return visible output in consumer logs without needing to inspect logs separately.
  - Runtime now tracks SDK task IDs returned by `TaskHandle` and performs periodic `project.tasks` polling while a step heartbeat is active.
  - Runtime now waits for the requested task IDs to become terminal (instead of requiring the poll payload length to match exactly), so polling responses that include unrelated tasks no longer block completion.
  - SDK task completion checks now tolerate mixed payload shapes (`taskID` or `task_id`) and only fail when any requested task reports terminal error/errorCode.
  - Progress/task visibility (`running_tasks`) is scoped to tasks whose task state is `started`, so the progress bar is tied to currently-running work rather than completed/failed snapshots.
  - Scene creation commands (`sdk_new_scene`, `sdk_project_new_scene`) and `sdk_project_command` align are treated as long-running and are not bounded by the default 600s wait.
  - Task snapshots and project status are attached to progress messages (`task_state`, `running_tasks`, `completed_tasks`, `project_status`) so TUI/operators can observe long-running reconstruction progress before a step finishes.
  - Runtime heartbeat payload now also mirrors active job/task/project progress (`active_job_id`, `task_state`, `project_status`) every heartbeat interval so client supervision can read progress without consuming result queue traffic.
  - `_report_progress` publishes explicit task/project payloads (`task_state`, `project_status`) in addition to `result_summary` to make progress consumers (including DB orchestration) resilient to schema changes.
  - SDK availability is now optional at import time: the runtime starts even if `realityscan_sdk` is absent, and only fails SDK jobs with a clear error, allowing file-only jobs to execute.
  - `file_stage` and other file steps no longer generate fake task IDs for plain path strings; task IDs are now derived only from UUID-like task handle values, so filesystem paths do not trigger long task waits.
  - `StepExecutor.execute` now returns typed `StepExecutionResult` objects with explicit task IDs, preventing return-shape inference from deciding control flow.
- `rslogic/client/rsnode_client.py` is the runtime bootstrap entrypoint used by the control TUI and CLI.
  - now auto-discovers the repo root at startup and injects it into `sys.path`/`PYTHONPATH` when executed directly, so `python <repo>/rslogic/client/rsnode_client.py` works even when the venv interpreter has broken package discovery.
- `rslogic/client/control_tui.py` is the new Python/Textual client control app (`rslogic-clientctl`):
  - start/stop/restart/status management for `rslogic.client.rsnode_client`.
  - auto-bootstrap mode: creates `.venv` with `uv venv` (fallback to stdlib `venv`) and installs `-e .` if missing/broken.
  - `start` now verifies the spawned runtime remains alive after launch; if it exits immediately, the command returns a clear failure with captured startup stderr/stdout tail.
  - bootstrap import verification is strict; runtime check now validates only the required base config modules and then launches from a deterministic root (`Path(__file__).resolve().parents[2]`) with explicit marker checks.
  - live status cards for client process, rsnode process presence, heartbeat age/state, and per-client redis queue depth.
  - live log tail panels from `logs/client/rslogic-client-stdout.log` and `logs/client/rslogic-client-stderr.log`.
  - status grid displays live task/project heartbeat state (`task_state`, `project_status`, `active_job_id`) for operators while long-running SDK jobs are running.
  - task display now renders per-task loading bars only for currently-started tasks, using heartbeat `running_tasks` and project progress fallback so active task progress is visible continuously.
  - command actions: `tui` (default), `start`, `stop`, `restart`, `status` (for scripting/automation).
  - determines repo root deterministically from script location (`Path(__file__).resolve().parents[2]`) and fails fast if expected markers are missing, rather than scanning alternate directories.
  - client env contract is loaded with `python-dotenv` from `client.env` at repo root only (hardcoded location).
  - `client.env` is the source of truth for all client settings: values are hydrated into `os.environ` before any client config import to ensure runtime/control paths are consistent.
  - `RSLOGIC_CLIENT_ENV_FILE` is not used by the client runtime path anymore; the root `client.env` file is the single source of truth.
  - runtime heartbeat includes `pid` and `host`, and control status now uses that heartbeat payload to recover and display the runtime PID when the local pid file is absent.
  - status now clears a stale `logs/client/rslogic-client.pid` when the process does not exist and immediately relaunches a fresh client process, replacing the stale PID in the pid file.
  - status output now includes `orphaned_pid_recovered`, `orphaned_pid`, and `auto_restarted` for visibility into this pid self-heal path.
  - CLI `start`/`restart` run detached and close parent-side subprocess handles to avoid deallocator warnings.
- `rslogic_clientctl.py` is the top-level launcher used by `rslogic-clientctl` script:
  - resolves repo root from local script location (`Path(__file__).resolve().parent`) and inserts it into `PYTHONPATH` before importing package modules.
  - fails fast with explicit errors when markers are missing instead of trying alternate source-layout fallbacks.
  - imports `rslogic.client.control_tui` by fully-qualified module name and no longer depends on `from rslogic.client import control_tui` to avoid `ImportError` on nested installs.
- Added canonical config ownership in `rslogic/config.py` (full config logic now lives in-package) and a tiny compatibility top-level `config.py` shim so legacy imports still work.
- bootstrap checks in `rslogic.client.control_tui` focus on core runtime dependencies (`rslogic.config` and optionally `textual`) and can run without `RSLOGIC_ROOT`.
- `rslogic/tui/app.py` provides the operator UX path (`rslogic-tui`).
  - Implemented with `textual` for interactive terminal controls.
  - Added status-panel action `Clear queued jobs` that clears Redis command/result queue entries for a configured client id directly through `RedisBus`, without calling orchestrator API endpoints.
- Upload workflow uses a directory tree widget so operators select folders (directories only), avoiding large in-folder file listings.
- `installer.bat` now boots a local uv + python3.14t virtual environment, installs this repo in editable mode, and points operators to `rslogic-clientctl` for process control.
- Job contract artifacts:
  - `job-contract.schema.json` documents the JSON payload expected by `POST /jobs` for `auto_assign`, routing, group fields, and step objects.
  - `job-action-map.json` documents executable file and sdk actions and how `rslogic.client.executor.StepExecutor` dispatches them.
  - Upload directory picker is initialized at the current workspace directory (`Path.cwd()`), which is typically the launched repository root.
- `rslogic/common/*` contains shared Redis, S3, DB, and workflow schemas used by orchestrator and client.
- `rslogic/client/executor.py` translates step actions into realityscan-sdk calls and file operations.
  - New context-aware behavior now tracks `session` after `sdk_project_create/open` and supports placeholder expansion in step params (`{session}`, `{session_data_dir}`, `{job_id}`, `{staging_dir}`, etc.).
  - Added file action for session imagery placement (`file_move_to_session_imagery`, `file_move_staging_to_session_imagery`, `file_move_to_session_folder`) to copy staged assets into `<working_root>/sessions/<session>/_data/Imagery` before project import.
  - SDK parameter compatibility now normalizes `path` → `folder_path` for `add_folder`-style commands, so jobs using legacy job JSON keys continue to execute instead of failing on unexpected keyword arguments.
  - SDK execution now fails fast with a clear message when SDK actions are submitted without the SDK dependency installed.
  - `StepExecutor.execute` now returns `StepExecutionResult` for explicit step typing (`value` + `task_ids`), making it impossible for file path returns to be mistaken as tasks.
- `rslogic/client/file_ops.py` handles staging/working directory movement for job-local assets.
  - Client `file_stage` is image-only; it stages only image assets referenced in DB rows and does not download/pull sidecar objects to the local client.
  - `file_stage` writes staged files directly into `staging_root` (no per-job/job-group subfolders), using DB asset IDs for stable unique filenames.
  - `file_stage` now downloads assets in parallel with a thread pool (`CONFIG.s3.multipart_concurrency`, defaulting to CPU count) for faster staging throughput.
  - staging is cache-first: existing files in `staging_root` are reused across jobs and skipped if already present; `file_stage` only fetches missing assets.
  - `file_move_*` operations now copy from `stage-map.json` (if present) so only files touched by the most recent stage action are copied into working directories, while shared cache files remain in staging for future jobs.
  - File copy steps (`file_move_staging_to_working`, `file_move_to_working`, `file_import_to_working`, `file_move_to_session_imagery`, and session imagery variants) default to `staging_root` when `working_dir` is not explicitly provided, and never to `staging_root/<job_id>`.
- `rslogic/client/process_guard.py` keeps the local RealityScan process running when configured.

Auto-assignment:
- `POST /jobs` accepts `target_client` or `client_id` for explicit routing.
- If `auto_assign=true` and no explicit client is set, orchestrator selects the first active heartbeat client.
- `GET /clients` lists heartbeat-active clients via `rslogic:clients:*:heartbeat`.
- `rslogic/tui/app.py` can also dispatch workflow JSON directly to `POST /jobs`.
- Ingest UX behavior:
  - The TUI ingest action now logs a useful preflight state when no assets are inserted:
    - number of ready images found in `drone-imagery-waiting`,
    - number of unmatched objects in the bucket.

## In/out contracts

### Upload
- Input: local folder path.
- Output: uploads only image files and sidecar files (`.xmp`, `.xml`, `.json`) to `CONFIG.s3.bucket_name` (`drone-imagery-waiting`).
- Keying:
  - image keys are `hash.extension`, where hash is the SHA-256 of image bytes.
  - sidecar keys are the same base hash as its paired image, with the sidecar extension.
  - if no matching sidecar file is found, the uploader generates a synthetic `*.json` sidecar from parsed image metadata so ingest still receives sidecar-style payloads.
  - there is a single upload record per image so image and its sidecars are always uploaded together; duplicates were removed to avoid malformed payloads that dropped the whole upload batch.
- No folder-like keys (no `/` path segments) are written to S3; organization happens in Postgres (`image_groups`, links, metadata).
- Behavior:
  - multi-threaded upload with configurable workers (defaults to `os.cpu_count()`),
  - callback hook (`on_progress`) to drive UI loading bars,
  - artifact pairing is directory + stem based,
  - sidecar stem matching accepts both `image.ext` and `image.ext.sidecar` style file names (e.g. `image.jpg.xmp`) so these are treated as the same asset group and uploaded as sidecars.
  - manifest file is written to `CONFIG.s3.manifest_dir`.

### Ingest
- Input: objects currently in waiting bucket.
- Pairing: directory + stem anchor is used to match image↔sidecar.
- If an image has no sidecar object, ingest synthesizes a `*.json` sidecar payload from parsed image EXIF/XMP so `metadata['sidecars']` is still populated.
- Execution: multi-threaded image ingest workers (from `CONFIG.s3.multipart_concurrency`), with optional per-run `on_progress` callback.
- Observability: ingest now emits explicit preflight/status messages (`Scanning waiting bucket...`, pair counts, worker count, and queue phase) so UI can show why startup is paused before the first rows appear.
- Output:
  - downloads each matched image and parses EXIF/sidecar,
  - EXIF parser normalizes non-JSON values (for example IFDRational) into JSON-safe primitives and strips null bytes from text before writing metadata,
  - complete EXIF is preserved under `metadata['exif']`, with:
    - camera/make/model/focal length/size/software fields extracted into `RsLogicImageAsset` columns (`drone_model`, `camera_make`, `camera_model`, `focal_length_mm`, `image_width`, `image_height`, `software`, `captured_at`),
    - GPS EXIF fields are extracted during ingest and mapped into `RsLogicImageAsset` geospatial columns:
    - `latitude`
    - `longitude`
    - `altitude_m`
    - `location` (`POINT` geometry, SRID 4326)
  - creates `image_assets` rows and attaches optional `image_group`,
  - moves images and sidecars into `drone-imagery` bucket using flat keys (no folder paths),
  - each image is moved independently as soon as its worker completes, so assets appear in `drone-imagery` incrementally instead of waiting for batch completion,
  - writes source/parsed metadata into the row metadata.

### Job orchestration
- Orchestrator receives `JobRequest`:
  - `client_id` or `target_client` route explicitly,
  - `auto_assign` optional routing fallback to active clients,
  - `group_id` or `group_name` attach images by group.
- Each job is persisted in `RsLogicProcessingJob`, serialized, and pushed to target Redis command queue.
- `GET /jobs`, `GET /jobs/{job_id}`, and `GET /clients` expose progress/state.

### Client runtime
- Reads `RSLOGIC_CLIENT_ID` and waits for job envelopes from Redis.
- Loads client environment from `client.env` at repo root by default; if `RSLOGIC_CLIENT_ENV_FILE` is set, that path is used instead.
- Client startup is env-file authoritative: values are injected into process env before import-time config resolution, so shell-level env overrides are intentionally ignored for client semantics.
  - Creates/maintains `RealityScanClient` and executes ordered steps:
  - `kind=file` staging/mapping/move operations,
  - `kind=sdk` sdk calls such as `sdk_node_connect_user`, `sdk_project_create`, `sdk_new_scene`, and command/project methods.
  - publishes command result text from each completed step in redis `result_summary` (`result`, `result_type`, `result_preview`) so operators can see what each SDK call returned.
- `sdk_project_create` and `sdk_project_open` are treated as session-establishing steps: when no task IDs are returned, runtime now requires a non-empty session string (from result or current executor context) before advancing, and uses that as the completion signal for the step.
- The session-establishing contract is explicit in `rslogic/client/executor.py` via `SDK_SESSION_ACTIONS`, so completion semantics are endpoint-driven instead of inferred from return payload shape.
- Runtime task extraction now only polls for explicit task identifiers (task handles / task payload objects); scalar UUID-like strings are no longer treated as task IDs so `project_create` session IDs are not mistaken for task IDs.
- Added step-level logging for async-vs-sync completion decisions and timeout warnings in `rslogic/client/runtime.py` so a step that returns no task IDs is clearly marked as synchronous in logs.
- Task completion is now compared using canonical UUID normalization so finished tasks are recognized regardless of taskID casing/format variations (e.g. uppercase UUID strings from SDK task payloads are treated the same as canonical lowercased IDs).
  - when an SDK step returns a `TaskHandle`, runtime keeps an in-memory task registry keyed by `job_id` and keeps it updated by polling `project.tasks`; task + project status are included in heartbeat and completion payloads.
- SDK task failure criteria now ignore `errorCode == 1` as a terminal non-fatal code; task failure is treated as:
  - explicit terminal error states (`failed`, `error`, `aborted`, `canceled`, `cancelled`), or
  - terminal task state with `errorCode` not in `{0,1}`.
- Client SDK identity is normalized per runtime: if `RSLOGIC_RSTOOLS_SDK_CLIENT_ID` is missing or not a UUID, the client derives a stable UUID (`uuid5`) from it to satisfy RealityScan node client-id authorization.
- Job `group_id` input is normalized before DB writes in the client: UUID-like IDs are used as-is, otherwise the value is treated as a group name and auto-created in `image_groups`.
- Publishes heartbeat and result updates.
- Rejects work when already busy with another job and reports `rejected`.

## Data objects

- `JobRequest` (`rslogic/common/schemas.py`):
  - optional `client_id` / `target_client`,
  - optional `auto_assign`,
  - optional `group_id` or `group_name`,
  - ordered `steps` (`action`, `kind`, `params`, `timeout_s`).
- `Step` validates action/kind strings and is validated before execution.

## Process boundaries

- Upload + ingest remain operator-driven entry points from TUI/CLI.
- Orchestrator handles only queueing, routing, and status recording.
- Client is responsible for filesystem + RealityScan SDK execution and reporting.

## Operational notes

- `config.py` labels:
  - waiting bucket: `LOCKED_WAITING_BUCKET_NAME = "drone-imagery-waiting"`
  - processed bucket: `LOCKED_PROCESSED_BUCKET_NAME = "drone-imagery"`
  - label DB path is resolved from the installed `studio_db` package and points at that package root.
- `studio-db` is consumed as an installed dependency (`studio-db>=0.1.0`), so DB models are imported via the `studio_db` module instead of repository file-path probing.
- Heartbeats are written to redis key `rslogic:clients:{client_id}:heartbeat`.
- `rslogic-clientctl` is the preferred control entrypoint on remote clients and replaces direct `start-rslogic-client.bat`/`stop-rslogic-client.bat` usage.
- Client shutdown path:
  - signal handlers in `ClientRuntime`,
  - best-effort stop for tracked rsnode process,
  - active loop exits when `stop_event` is set.
