RsLogic execution architecture

## Top-level packages

- `rslogic/api/server.py` is the orchestrator control plane entrypoint (`rslogic-api`).
- `rslogic/cli/upload.py` is the CLI upload entrypoint (`rslogic-upload`).
- `rslogic/ingest.py` ingests objects from `drone-imagery-waiting` into `drone-imagery` and writes rows into `studio-db` (`rslogic-ingest`).
- `rslogic/client/runtime.py` is the standalone client worker (`rslogic-client`, `rslogic-worker`).
- `rslogic/tui/app.py` provides the operator UX path (`rslogic-tui`).
  - Implemented with `textual` for interactive terminal controls.
- Upload workflow uses a directory tree widget so operators select folders (directories only), avoiding large in-folder file listings.
- `installer.bat` boots a local uv + python3.14t virtual environment, installs this repo in editable mode, and lays down `client.env` from `client.env.template` plus a `start-rslogic-client.bat` launcher.
- Job contract artifacts:
  - `job-contract.schema.json` documents the JSON payload expected by `POST /jobs` for `auto_assign`, routing, group fields, and step objects.
  - `job-action-map.json` documents executable file and sdk actions and how `rslogic.client.executor.StepExecutor` dispatches them.
  - Upload directory picker is initialized at the current workspace directory (`Path.cwd()`), which is typically the launched repository root.
- `rslogic/common/*` contains shared Redis, S3, DB, and workflow schemas used by orchestrator and client.
- `rslogic/client/executor.py` translates step actions into realityscan-sdk calls and file operations.
- `rslogic/client/file_ops.py` handles staging/working directory movement for job-local assets.
- `rslogic/client/process_guard.py` keeps the local RealityScan process running when configured.

Auto-assignment:
- `POST /jobs` accepts `target_client` or `client_id` for explicit routing.
- If `auto_assign=true` and no explicit client is set, orchestrator selects the first active heartbeat client.
- `GET /clients` lists heartbeat-active clients via `rslogic:clients:*:heartbeat`.
- `rslogic/tui/app.py` can also dispatch workflow JSON directly to `POST /jobs`.

## In/out contracts

### Upload
- Input: local folder path.
- Output: uploads only image files and sidecar files (`.xmp`, `.xml`, `.json`) to `CONFIG.s3.bucket_name` (`drone-imagery-waiting`).
- Behavior:
  - multi-threaded upload with configurable workers,
  - artifact pairing is directory + stem based,
  - manifest file is written to `CONFIG.s3.manifest_dir`.

### Ingest
- Input: objects currently in waiting bucket.
- Pairing: directory + stem anchor is used to match image↔sidecar.
- Output:
  - downloads each matched image and parses EXIF/sidecar,
  - creates `image_assets` rows and attaches optional `image_group`,
  - moves images and sidecars into `drone-imagery` bucket,
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
- Client shutdown path:
  - signal handlers in `ClientRuntime`,
  - best-effort stop for tracked rsnode process,
  - active loop exits when `stop_event` is set.
