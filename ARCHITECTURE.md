RsLogic execution architecture

## Top-level packages

- `rslogic/api/server.py` is the orchestrator control plane entrypoint (`rslogic-api`).
- `rslogic/cli/upload.py` is the CLI upload entrypoint (`rslogic-upload`).
- `rslogic/ingest.py` ingests objects from `drone-imagery-waiting` into `drone-imagery` and writes rows into `studio-db` (`rslogic-ingest`).
- `rslogic/client/runtime.py` is the standalone client worker (`rslogic-client`, `rslogic-worker`).
  - Added per-step heartbeat result messages (default every 3s) while a long-running step is executing so Redis users and operators can see active progress with `step_index`, `step_action`, and elapsed time.
  - Added structured runtime logs to stdout/stderr (job_id, step index/action, params preview, and per-step durations) for high-frequency local diagnosis.
- `rslogic/client/control_tui.py` is the new Python/Textual client control app (`rslogic-clientctl`):
  - start/stop/restart/status management for `rslogic.client.rsnode_client`.
  - auto-bootstrap mode: creates `.venv` with `uv venv` (fallback to stdlib `venv`) and installs `-e .` if missing/broken.
  - bootstrap import verification is strict; runtime check now validates only the required base config modules and then launches with deterministic root detection.
  - live status cards for client process, rsnode process presence, heartbeat age/state, and per-client redis queue depth.
  - live log tail panels from `logs/client/rslogic-client-stdout.log` and `logs/client/rslogic-client-stderr.log`.
  - command actions: `tui` (default), `start`, `stop`, `restart`, `status` (for scripting/automation).
  - auto-discovers the real repo root even in nested installs (for example `C:\\ProgramData\\RsLogic\\RsLogic`) and always starts runtime from the discovered `<root>\\rslogic\\client\\rsnode_client.py`.
  - CLI `start`/`restart` run detached and close parent-side subprocess handles to avoid deallocator warnings.
- `rslogic_clientctl.py` is the top-level launcher used by `rslogic-clientctl` script:
  - resolves repo root from `RSLOGIC_ROOT`/cwd and inserts it into `PYTHONPATH` before importing package modules.
  - auto-discovers nested `RsLogic`/repo layouts and can continue by running `rslogic/client/control_tui.py` from source if package import fails.
- bootstrap checks in `rslogic.client.control_tui` focus on core runtime dependencies (`config` and optionally `textual`) rather than requiring `import rslogic`, so the TUI can start even when package resolution is partially broken in a local environment.
- `rslogic/tui/app.py` provides the operator UX path (`rslogic-tui`).
  - Implemented with `textual` for interactive terminal controls.
- Upload workflow uses a directory tree widget so operators select folders (directories only), avoiding large in-folder file listings.
- `installer.bat` now boots a local uv + python3.14t virtual environment, installs this repo in editable mode, and points operators to `rslogic-clientctl` for process control.
- Job contract artifacts:
  - `job-contract.schema.json` documents the JSON payload expected by `POST /jobs` for `auto_assign`, routing, group fields, and step objects.
  - `job-action-map.json` documents executable file and sdk actions and how `rslogic.client.executor.StepExecutor` dispatches them.
  - Upload directory picker is initialized at the current workspace directory (`Path.cwd()`), which is typically the launched repository root.
- `rslogic/common/*` contains shared Redis, S3, DB, and workflow schemas used by orchestrator and client.
- `rslogic/client/executor.py` translates step actions into realityscan-sdk calls and file operations.
  - New context-aware behavior now tracks `session` after `sdk_project_create/open` and supports placeholder expansion in step params (`{session}`, `{session_data_dir}`, `{job_id}`, `{staging_dir}`, etc.).
  - Added file action for session imagery placement (`file_move_to_session_imagery`, `file_move_staging_to_session_imagery`, `file_move_to_session_folder`) to move staged assets into `<working_root>/sessions/<session>/_data/Imagery` before project import.
  - SDK parameter compatibility now normalizes `path` → `folder_path` for `add_folder`-style commands, so jobs using legacy job JSON keys continue to execute instead of failing on unexpected keyword arguments.
  - `rslogic/client/file_ops.py` handles staging/working directory movement for job-local assets.
  - Client `file_stage` is image-only; it stages only image assets referenced in DB rows and does not download/pull sidecar objects to the local client.
  - `file_stage` writes staged files directly into `staging_root` (no per-job/job-group subfolders), using DB asset IDs for stable unique filenames.
  - File move steps (`file_move_staging_to_working`, `file_move_to_working`, `file_import_to_working`, and session imagery variants) default to `staging_root` when `working_dir` is not explicitly provided, and never to `staging_root/<job_id>`.
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
- Loads client environment from `RSLOGIC_CLIENT_ENV_FILE` or `client.env` automatically before config resolution.
- Creates/maintains `RealityScanClient` and executes ordered steps:
  - `kind=file` staging/mapping/move operations,
  - `kind=sdk` sdk calls such as `sdk_node_connect_user`, `sdk_project_create`, `sdk_new_scene`, and command/project methods.
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
- Label-db models are loaded from `rslogic/internal_tools/label-db/studio-db/models.py` by default.
- Heartbeats are written to redis key `rslogic:clients:{client_id}:heartbeat`.
- `rslogic-clientctl` is the preferred control entrypoint on remote clients and replaces direct `start-rslogic-client.bat`/`stop-rslogic-client.bat` usage.
- Client shutdown path:
  - signal handlers in `ClientRuntime`,
  - best-effort stop for tracked rsnode process,
  - active loop exits when `stop_event` is set.
