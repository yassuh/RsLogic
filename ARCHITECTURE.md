# ARCHITECTURE

## Operational Network Map

- Local service defaults: `host:localhost → API(rslogic api:8000), DB(pgbouncer/postgres via config:5432/9000), Redis(redis:6379/9002)`
- Container stack (`internal_tools/label-db/studio-db/docker-compose.yaml`): `postgis:5432@9000`, `pgbouncer:5432@9001`, `redis:6379@9002` on `studio-network`
- RSNode deployment target (validated working defaults): `192.168.193.59:8000` (API), `192.168.193.56:9002` (Redis)
- RSNode client launch/runtime defaults: `C:\ProgramData\RsLogic\RsLogic` checkout + `C:\Program Files\Epic Games\RealityScan_2.1\RSNode.exe`
- Integration/test references (validate before hardcoding): `192.168.193.59:7878`, `192.168.193.59` as previously used for manual API contact

## Core Concepts

- Shared schema lives in `label-db` and is managed by Alembic.
  - Vendored in `internal_tools/label-db` (canonical source from `github.com/yassuh/label-db`).
- `RsLogic` adds ingestion/job orchestration logic and calls into the same database.
- In-house tooling is stored under `internal_tools/`:
  - `internal_tools/label-db/studio-db` (Alembic + shared DB model/migrations)
  - `internal_tools/rstool-sdk` (RealityScan SDK from `github.com/bossdown123/RsTool`)

## Runtime Configuration

`config.py` is the single source for settings:

- `RsToolsConfig` includes execution mode (`stub`, `cli`, `sdk`, `remote`) and credentials.
- `LabelDbConfig` contains the shared Alembic root, alembic.ini path, and database URL.
- `ApiConfig` contains `RSLOGIC_API_BASE_URL` used by local tools (TUI ingest workflow) to call the server.
- `RSLOGIC_DEFAULT_GROUP_NAME` controls fallback group name when clients omit `group_name`.
- `RSLOGIC_LOG_LEVEL` and `RSLOGIC_LOG_FORMAT` control server log verbosity/format (`DEBUG` is recommended during upload tuning).
- Queue/Redis settings:
  - `RSLOGIC_QUEUE_BACKEND` supports `redis` (default) or `memory`.
  - `RSLOGIC_QUEUE_START_LOCAL_WORKERS` controls whether API process consumes queue jobs locally.
  - Redis connection resolves from `RSLOGIC_REDIS_URL` / `REDIS_URL` or host/port env vars (`REDIS_HOST`, `REDIS_PORT`, `REDIS_DB`, `REDIS_PASSWORD`).
  - Queue base key is `RSLOGIC_REDIS_QUEUE_KEY` (default `rslogic:jobs:queue`) and is split internally into `:processing` and `:upload`.
  - Remote RSNode worker control bus keys:
    - `RSLOGIC_CONTROL_COMMAND_QUEUE` (default `rslogic:control:commands`)
    - `RSLOGIC_CONTROL_RESULT_QUEUE` (default `rslogic:control:results`)
    - `RSLOGIC_CONTROL_BLOCK_TIMEOUT_SECONDS`, `RSLOGIC_CONTROL_RESULT_TTL_SECONDS`, `RSLOGIC_CONTROL_REQUEST_TIMEOUT_SECONDS`
  - Dedicated client package defaults in `rslogic/client`:
    - `rslogic.client.rsnode_client` consumes `RSLOGIC_CLIENT_*` settings and publishes `rslogic:clients:<client_id>:status`.
    - `RSLOGIC_CLIENT_REPO_URL` and `RSLOGIC_CLIENT_INSTALL_ROOT` drive `rslogic/client/install.bat` bootstrap behavior.
- `config.py` loads `.env` with override disabled (`load_dotenv(override=False)`), so runtime-injected environment values (for example from orchestrator startup) are preserved unless the caller sets them explicitly.
- Postgres DSN derivation URL-encodes credentials and defaults to `postgis:5432` when explicit DB URL env vars are not provided.
- S3 bucket routing:
  - Waiting uploads are locked to `drone-imagery-waiting` (`S3Config.bucket_name`).
  - Post-ingest storage is locked to `drone-imagery` (`S3Config.processed_bucket_name`; env override `RSLOGIC_S3_PROCESSED_BUCKET_NAME` / `S3_PROCESSED_BUCKET_NAME`).

## Service Ports

- API runtime:
  - Runtime entrypoint `rslogic.api.server.main` defaults to `host=0.0.0.0`, `port=8000`.
  - Config default base URL is `RSLOGIC_API_BASE_URL=http://localhost:8000` (trimmed).
  - SDK fallback for the RSNode client default is `http://{server_host}:8000`.
- Queue/runtime services:
  - Redis default connection in `config.py` is `REDIS_HOST: REDIS_PORT` (`6379`) with DB `RSLOGIC_REDIS_DB` default `0`.
- Orchestrator queue connection resolution follows environment overrides first (`RSLOGIC_REDIS_HOST` / `REDIS_HOST`, `RSLOGIC_REDIS_PORT` / `REDIS_PORT`), then command-line flags.
  - Control bus keys default to:
    - `RSLOGIC_CONTROL_COMMAND_QUEUE=rslogic:control:commands`
    - `RSLOGIC_CONTROL_RESULT_QUEUE=rslogic:control:results`
    - Queue backend key base: `RSLOGIC_REDIS_QUEUE_KEY=rslogic:jobs:queue`
- Processing queue workers:
  - `rslogic-worker` reads and writes Redis queues.
  - `rslogic-worker` does not expose an HTTP port; it consumes `redis` queues (`:processing`, `:upload`).
- RSNode orchestration runtime:
  - `scripts/rslogic_rsnode_client.py` starts with defaults:
    - `--repo-root C:\ProgramData\RsLogic\RsLogic`
    - `--node-executable "C:\Program Files\Epic Games\RealityScan_2.1\RSNode.exe"`
    - `--node-data-root "%LOCALAPPDATA%\\Epic Games\\RealityScan\\RSNodeData"`
    - `--node-data-root-argument -dataRoot`
  - Internal API endpoint calls by the SDK client target `http://{server_host}:8000` when `server_host` is non-localhost.
- DB services:
  - `config.py` defaults `POSTGRES_HOST=postgis`, `POSTGRES_PORT=5432`.
  - Docker-compose stack maps `9000` to `5432` for `postgis`, `9001` to `5432` for `pgbouncer` (optional), and `9002` to `6379` for `redis`.
- Script/runtime CLI references:
  - `start_rslogic_rsnode_client.bat` default launcher path: `%ProgramData%\RsLogic\RsLogic`.
  - `scripts/rslogic_rsnode_client.py` can be pointed directly to custom ports/hosts with `--redis-host`, `--redis-port`, `--server-host`, `--sdk-base-url`.

## Machines, IPs, and Network Topology

- Known defaults in code:
  - `192.168.193.59` (defined as `DEFAULT_SERVER_HOST` in `scripts/rslogic_rsnode_client.py`).
  - User-facing startup defaults in the same script assume:
    - RS API: `http://192.168.193.59:8000`
    - Redis: `192.168.193.56:9002` only when `--redis-port` or `RSLOGIC_REDIS_PORT` is explicitly set that way.
  - Label: **configuration default / deployment target**, not an auto-discovered runtime value.
- Known user-reported node reference:
  - `192.168.193.59` is the active test/integration endpoint for direct RSNode verification.
  - Label: **test/integration reference**, validate whether this is still active before hardcoding.
- Active container network (in-repo docker model):
  - `internal_tools/label-db/studio-db/docker-compose.yaml`
    - Network name: `studio-network` (bridge).
    - Services: `postgis`, `pgbouncer`, `redis`.
    - All run in `studio-network` and expose host ports (`9000`, `9001`, `9002`) for local access.
- Data/config hostnames (current repo `.env`):
  - `POSTGRES_HOST=postgis`, `POSTGRES_PORT=5432` (docker service name / internal network context).
  - `REDIS_HOST=redis`, `REDIS_PORT=6379`.
  - For direct remote host deployments, overrides can point `RSLOGIC_REDIS_HOST`, `RSLOGIC_REDIS_PORT`, `POSTGRES_HOST`, `POSTGRES_PORT` to host network values.

## Database

- `StorageRepository` persists `ImageAsset` and `ProcessingJob` entities for RsLogic workflows.
  - The repository consumes ORM classes from `internal_tools/label-db/studio-db/models.py` to avoid duplicated model definitions.
  - SQLAlchemy sessions are configured with `expire_on_commit=False` so returned ORM objects keep scalar fields (for example `id`) accessible after commit in async/threaded ingest paths.
- The repository does not auto-create schema locally; all schema lifecycle is handled in the shared Alembic project:
  - `rslogic.storage.repository` reads models from `label-db` and only issues ORM operations against that shared DB.
  - `rslogic.db.migrate` wrapper executes `alembic` in label-db project context.
- `internal_tools/label-db/studio-db` now includes RsLogic tables:
  - `image_assets` (metadata extracted on upload, canonical URI in `uri`, plus JSON `metadata` field persisted from S3/user metadata)
    - `file_size` is stored as `BIGINT` to support large media objects (>2 GiB)
  - `image_groups` and `image_group_items` (image grouping model)
  - `processing_jobs` (queue/job lifecycle with `image_group_id`)
- Upload behavior for CLI is locked to `drone-imagery-waiting` and always uses SHA-256 file-hash object keys.
- `S3MultipartUploader` now extracts image metadata before upload, maps selected fields into S3 user-defined object metadata, and always uses SHA-256 file-hash object keys.
  - Full flattened metadata JSON is stored directly on the image object metadata in `metadata_json` (`metadata_json_format=json-flat-v1`).
  - Payload includes full parsed image metadata (including EXIF/XMP) plus parsed sidecar telemetry metadata.
  - Upload metadata now also includes EXIF-derived fields (`exif_*`) when available, so waiting-bucket ingest can persist EXIF context into `image_assets.metadata`.
  - Matching image sidecars (`.XMP`, `.MRK`) are parsed and folded into object metadata before upload.
  - Multipart resume state now handles stale `completed` manifests safely by resetting to a fresh multipart session instead of failing assertions, and completed manifests are removed after successful completion.
- Video assets are currently unsupported and ignored by upload workflows (`.mp4/.mov/.m4v/.avi/.mkv` are skipped).
- `rslogic-upload` CLI now provides:
  - `upload` for files/folders using defaults from `config.py` (bucket must already exist; CLI does not create/delete buckets)
  - `upload --group <name>` stores `group_name` in S3 user metadata for each uploaded object
  - `upload --override-existing` forces replacing existing S3 objects with the same hash-key; without this flag existing objects are skipped
  - Upload prefix/concurrency/part-size/resume are fixed to `config.py` defaults and are not user-editable via CLI/TUI
  - `groups list` / `groups create` wired to `image_groups` in label-db
  - `ingest` preview command that scans `drone-imagery-waiting`, fetches object `head_object` metadata, parses primitive types, and returns JSON metadata payloads
  - `interactive` wizard for guided upload/ingest flows (also the default mode when no subcommand is provided)
  - Interactive startup mode can be controlled explicitly with `--tui` (force Textual UI) or `--prompt` (force prompt mode)
  - Interactive mode is powered by `textual` and starts on a workflow selection page
  - Workflow 1: upload imagery to the locked waiting bucket (`drone-imagery-waiting`)
  - Workflow 2: run server-side waiting-bucket metadata ingest from TUI
  - Workflow 3: create/inspect/cancel processing jobs via API (`POST /jobs`, `GET /jobs/{id}`, `POST /jobs/{id}/cancel`)
  - Workflow 4: Redis Command Console for direct `rstool_sdk.discover` and `rstool_sdk.command` traffic to connected RSNode workers.
    - Send commands through `rslogic:control:commands`, inspect replies from `rslogic:control:results:reply:<command_id>`, and view command history in the same page.
  - Upload workflow uses a multi-page flow: setup page (selection/group), upload page (plan + live progress), and result page (final status)
  - Job workflow is SDK-oriented and mirrors `example.ipynb`: `newScene -> set(...) -> addFolder -> align/calculateNormalModel/calculateOrthoProjection -> save`
  - Job workflow inputs include `group_name`, optional `drone_type`/`max_images`, plus SDK controls (`sdk_imagery_folder`, detector/accuracy settings, per-stage toggles, timeout), and `stage_only` for test/staging runs.
  - `stage_only=true` performs a metadata-filtered image pull/staging workflow only (`pull_s3_images`, `s3_staging_root`, `s3_prefix`, etc.) and skips SDK scene/align/orthophoto execution.
  - `sdk_imagery_folder` is a path visible to the RealityScan node host (not a local browser path)
  - Local filesystem browsing remains in the client-side TUI; server-side APIs do not control an end-user OS file explorer
  - Job workflow plan panel surfaces current RsTools mode and SDK readiness derived from `config.rstools`
  - CLI temporarily disables Python logging while the Textual wizard is active to prevent terminal log overdraw artifacts
  - Upload setup includes an `Override Existing Uploads` toggle that controls whether hash-matching objects are overwritten or skipped
  - Ingest setup includes an `Override Existing Ingest` toggle that controls whether existing `image_assets` rows are updated or skipped
  - TUI explorer uses a `FastDirectoryTree` wrapper (skips common heavy/cache directories, caps visible entries per directory, and only renders folders) and includes a direct path input to keep keypress/navigation responsive on large trees
  - Interactive mode performs uploads inside the TUI worker thread, updates a live loading bar as files complete, and remains open after completion until the user explicitly closes it
  - TUI now includes a bottom log bar showing the latest 3 status log lines (rolling buffer) for quick in-app diagnostics
  - Upload progress now emits byte-level transfer callbacks (including multipart part completion) and the TUI loading bar tracks bytes transferred for visible movement during large uploads
  - `Manage Groups` panel in `RsTerminal` now reads group summaries from `label_db` and renders live group-level metadata and image counts (images, bucket diversity, first/last capture).
  - Batch upload no longer aborts on a single-file failure: per-file errors are reported in-line and remaining files continue uploading
- `rslogic-client` CLI runs the remote RSNode worker:
  - `run` / `worker`: runs a processing worker process that consumes `processing_job.execute` commands from Redis and executes them through `realityscan_sdk`.
  - Additional control command types:
    - `rstool_sdk.discover`: returns discoverable methods for `node` and `project` targets from `realityscan_sdk.resources.node.NodeAPI` and `.project.ProjectAPI`.
      - Discovery currently enumerates public methods from the imported SDK classes and falls back to AST parsing of the local `internal_tools/rstool-sdk` sources when classes cannot be imported.
      - Current repo snapshot exports 6 `node` methods and 72 `project` methods (including documented aliases).
      - Include aliases like `connectuser` as discoverable command keys.
    - `rstool_sdk.command`: invokes arbitrary SDK call paths via Redis control queue (not hardcoded to a fixed allow-list).
      - `target` is optional and defaults to `node`.
      - `target` supports `node`, `project`, or `client`.
      - `target_object` is an optional dotted path resolved from the target root (for example `project` or `project.command` when `target` is `client`).
      - `method` is the final dotted method path (for example `create`, `status`, `command`, or `project.command`).
      - A private-name guard blocks any segment that starts with `_`.
  - `rstool_sdk.discover` payload examples:
    - `{"target": "node"}` to list node APIs.
    - `{"target": "project"}` to list project APIs.
    - `{}` to return both.
  - `rstool_sdk.command` payload example:
    - `{"target":"node","method":"connect_user","args":[],"kwargs":{}}` for `connectuser` equivalent.
    - `{"target":"project","method":"create","args":[],"kwargs":{}}` creates/opens project session in current client.
    - `{"target":"client","method":"node.status","args":[],"kwargs":{}}` executes a nested method on the raw client object.
    - `{"target":"client","target_object":"project","method":"command","args":["save",{"param1":"value"}],"kwargs":{}}` executes project command helpers with arbitrary kwargs.
  - Worker results are published as progress/error/complete events to the control result queue and the per-command reply queue.
  - This client is intended for the RSNode host (or any machine with access to the RSNode API).
  - Startup behavior must provide a stable session location through `--dataRoot` (SDK docs show `dataRoot` with default `%LOCALAPPDATA%\Epic Games\RealityScan\RSNodeData`).
  - Recommended startup shape on this installer: `"C:\Program Files\Epic Games\RealityScan_2.1\RSNode.exe" -dataRoot "<path>"` with automatic fallback retry using `--dataRoot` when needed.
  - On startup, this worker pings Redis before entering the consume loop and fails fast if control Redis is unreachable.
  - On startup, this worker emits masked startup diagnostics for SDK environment variables (`RSLOGIC_RSTOOLS_SDK_BASE_URL`, `RSLOGIC_RSTOOLS_SDK_CLIENT_ID`, `RSLOGIC_RSTOOLS_SDK_APP_TOKEN`, `RSLOGIC_RSTOOLS_SDK_AUTH_TOKEN`) but does not require them to be present at process startup.
  - Presence heartbeat is published continuously to Redis at `<control_command_queue>:presence:<host>:<pid>` as a JSON key with TTL.
    - Default interval: 5s, TTL: 15s.
    - Value includes `status`, `worker_id`, `command_queue`, `result_queue`, `workers`, `pid`, and `last_seen`.
    - Override with `RSLOGIC_CLIENT_HEARTBEAT_INTERVAL_SECONDS` and `RSLOGIC_CLIENT_HEARTBEAT_TTL_SECONDS`.
    - The worker updates `status=online` periodically and writes `status=stopped` once shutdown begins.
    - A consumer can treat key expiry as liveness loss.
    - Orchestrator startup heartbeat policy: wait for `RSLOGIC_CLIENT_HEARTBEAT_INTERVAL_SECONDS * 4` (minimum 30 seconds) for missing/invalid heartbeat status before applying restart/backoff.
  - `scripts/rslogic_rsnode_client.py` now checks presence directly from Redis for `clientHeartbeat` in status output.
  - `clientRedis` and `clientHeartbeat` status fields are now explicitly set to deterministic states (`connected`, `disconnected`, `booting`, `absent`, etc.) without requiring manual `redis-cli` inspection.
  - Orchestrator probes client bootstrap log lines for actual Redis URL and published presence key to avoid false negatives when startup args differ from launcher defaults.
- `rslogic-client` startup diagnostics:
  - Missing SDK env values are now reported as startup warnings with the exact missing variable names.
  - Client startup is allowed to proceed without credentials so `rstool_sdk` discovery/command flows can bootstrap token acquisition path.
  - Redis failures report as `Redis ping failed for <url>` immediately before worker creation.
  - `rslogic.client.rsnode_client` performs a startup bootstrap: applies available SDK overrides, calls `node.connect_user()` (best effort), then calls `node.connection()` and updates in-memory `sdk_base_url`/`auth_token` when a connection payload is returned.
  - The same bootstrap is attempted before `rstool_sdk.command` execution, and connection-shaped command results are folded back into in-memory SDK config.
  - The orchestrator now reads `RSLOGIC_RSTOOLS_SDK_BASE_URL`, `RSLOGIC_RSTOOLS_SDK_CLIENT_ID`,
    `RSLOGIC_RSTOOLS_SDK_APP_TOKEN`, and `RSLOGIC_RSTOOLS_SDK_AUTH_TOKEN` from the active repo `.env`
    and process environment when CLI arguments are not supplied, but treats missing values as a warning.
  - STATUS output now includes `clientError=...` to expose startup bootstrap failures observed in child logs.
- `scripts/rslogic_rsnode_client.py` is the single orchestrator for RSNode hosts:
  - Clones or reuses the local checkout at `C:\ProgramData\RsLogic\RsLogic` by default.
  - Performs `git fetch/checkout` and explicit ahead/behind reconciliation against `main` during startup and periodic checks.
  - Default reconciliation policy is `hard-reset` (`--git-sync-strategy` supports `rebase` and `ff-only` as alternatives).
  - Creates or reuses `.venv`, installs `rslogic` in editable mode, and writes `.env.rsnode-worker`.
  - Enforces single-instance behavior with an OS file lock and proactively cleans orphaned `RSNode.exe` + `rslogic.client.rsnode_client` processes from prior runs before starting tracked services.
  - Before launching the client, probes the virtual environment for a minimum runtime module set (`dotenv`, `sqlalchemy`, `redis`, `requests`, `httpx`, etc.) to avoid stale/partial installs, then reinstall/re-writes the head marker when modules are missing.
  - If existing checkout is missing or invalid, it is backed up (or replaced when bootstrap is needed) and then re-cloned automatically.
  - On startup, dependency install is skipped when the current git commit was already installed and no dependency refresh is needed.
  - Starts and monitors both `RSNode.exe` and `rslogic.client.rsnode_client` in a single long-running loop.
  - Heartbeat lookup strategy:
    - Orchestrator uses a priority chain: explicit key from client logs, then `presence:<hostname>:<pid>` for active client pid, then `<control_command_queue>:presence:*` scan.
    - Before returning a heartbeat result, orchestrator performs a Redis ping and parses the JSON payload (`status`, `last_seen`) from matched keys.
    - Redis heartbeat discovery uses direct key checks first, then `scan_iter` fallback to avoid requiring `KEYS` permissions.
  - Client launch environment forces unbuffered Python output (`PYTHONUNBUFFERED=1`, `PYTHONIOENCODING=utf-8`) so bootstrap logs are always written before status polling.
  - Keeps dedicated launch logs for RSNode/client (`rsnode-stdout.log`, `rsnode-stderr.log`, `rslogic-client-stdout.log`, `rslogic-client-stderr.log`) and includes recent process failure output in status logs.
  - On unexpected stop, status now emits `stopped/<reason>` so `node`/`client` health checks show explicit stop reason (exit code or termination state).
  - Detects repository updates and refreshes dependencies before restarting managed processes.
  - Supports custom RSNode startup args through `--node-arguments` and `--node-data-root-argument`.
  - `--node-authtoken` and `--sdk-auth-token` now default to `85DBDE55-3FFF-4228-9F06-CBED4003BBB8` when unset.
  - `--sdk-app-token` now defaults to `123` when unset.
  - Uses active host defaults `192.168.193.59`, Redis `192.168.193.56:9002`, and API base `http://192.168.193.59:8000`.
- `scripts/start_rslogic_rsnode_client.bat` is the one-click launcher:
  - Starts the Python orchestrator in a persistent console and keeps the window open while the orchestrator runs.
  - If the orchestrator script is missing locally, it bootstraps a fresh checkout to `%ProgramData%\RsLogic\RsLogic` before launch.
  - Forces `--git-sync-strategy hard-reset` by default to recover from branch divergence automatically.
  - Arguments passed to the batch file are forwarded to the Python orchestrator.
  - Uses direct Python invocation (`py -3` when present, otherwise `python`) instead of `call` to avoid batch tokenization artifacts.
- The launcher sets `--node-data-root-argument=-dataRoot` by default and writes status/error output to the console while running.
  - A desktop shortcut can be created with `scripts/create_rslogic_rsnode_client_shortcut.bat` (targeting `start_rslogic_rsnode_client.bat`).
- `scripts/create_rslogic_rsnode_client_shortcut.bat` creates `RsLogic RSNode Client.lnk` on the current user desktop (default target: `start_rslogic_rsnode_client.bat`).
- The shortcut launcher opens a persistent console and keeps logs visible while the RSNode orchestrator runs.
- `scripts/reconstruct_via_redis.py` is a dispatch-only runner for `processing_job.execute`:
  - Builds a processing command payload and enqueues it on `rslogic:control:commands`.
  - Does not execute SDK method sequences locally.
  - Supports:
    - `--job-id`, `--working-directory`, `--image-key`
    - `--set-filter` and `--filter-json` for arbitrary processing filters
    - S3 pull options (`--pull-s3-images`, `--s3-bucket`, `--s3-prefix`, `--s3-region`, `--s3-endpoint-url`, etc.)
    - `--wait` / `--no-wait` and optional `--reply-queue`
    - `--stage-only` (forwarded to `filters.stage_only`, handled by the RS client runner)
  - Defaults to:
    - Redis: `redis://192.168.193.56:9002/0`
    - RS API: `http://192.168.193.59:8000`
    - `app_token=123`
    - `auth_token=85DBDE55-3FFF-4228-9F06-CBED4003BBB8`
  - Result handling:
    - In wait mode, reads the final result from reply queue and exits non-zero on timeout/error.
    - In fire-and-forget mode, exits immediately after publishing.
- `scripts/sync_rslogic_repo.ps1` performs explicit branch synchronization:
  - fetches `origin/main` and prints ahead/behind comparison.
  - resolves behind-only state via fast-forward.
  - handles divergence with `-Strategy rebase` or `-Strategy hard-reset` (default: `hard-reset`).
  - for diverged history this helper performs one of:
    - `.\scripts\sync_rslogic_repo.ps1 -Strategy rebase`
    - `.\scripts\sync_rslogic_repo.ps1 -Strategy hard-reset`
- S3 uploads are routed through the server-configured path:
  - Bucket: locked to `drone-imagery-waiting` (not client-configurable)
  - Prefix: `RSLOGIC_S3_SCRATCHPAD_PREFIX` / `S3_SCRATCHPAD_PREFIX` (default `scratchpad`)
  - S3 client uses explicit connection tuning for high-throughput uploads (`max_pool_connections`, connect/read timeouts, adaptive retries)
- Ingestion persists key metadata and canonical `uri` (for S3 uploads `s3://bucket/key`) in `image_assets`.
  - `image_assets.location` is derived from metadata `latitude`/`longitude` as PostGIS `POINT(longitude latitude)` during image create/update.
- API also provides `POST /images/ingest/waiting`:
  - Server lists waiting-bucket objects from S3
  - Server reads object user metadata via `head_object`
  - Server decodes full flattened metadata JSON from object metadata key `metadata_json`
  - Parsed S3 metadata is written into `image_assets.metadata` (ORM field `metadata_json`)
  - Standard image fields (`captured_at`, lat/lon, camera/drone, size, sha) are mapped from that metadata when present
  - Each successfully handled waiting object is moved to `drone-imagery` and deleted from `drone-imagery-waiting`
  - DB `image_assets.uri` / `bucket_name` are updated to the destination bucket location (`s3://drone-imagery/<key>`)
  - Writes are idempotent per `uri` via repository upsert behavior
- API now also provides `POST /images/upload`:
  - Supports optional `POST /images/upload/prepare` to pre-create an upload job
  - Accepts multipart upload from clients
  - Supports single-file and multi-file uploads
  - Supports folder uploads from browser form data (folder names are ignored)
  - Uploads file bytes to configured S3 scratchpad location server-side
  - Always generates SHA-256-based object keys for uploaded files, then persists metadata and `uri` in `image_assets`
  - Parses metadata on the server and persists `image_assets` rows
  - Enqueues each upload as a job and returns batch job `id` immediately so the request does not block on parse/ingest
  - Upload batch jobs are tracked in `processing_jobs` and can be queried at `GET /images/upload/{batch_id}`
  - Live job progress can be streamed via WebSocket at `WS /ws/jobs/{job_id}`
  - Clients may also generate `batch_id` locally and pass it to `POST /images/upload`, avoiding a separate prepare call
- Non-REST upload transport is available at `WS /ws/upload`:
  - Client sends JSON control messages (`start`, `file_start`, `file_end`, `complete`) and binary chunk frames
  - Server assembles temp files, then enqueues the same upload+ingest batch pipeline and returns `queued` with `job_id`
  - `group_name` is the API field mapped to `image_groups.name` in DB
- API provides group management endpoints:
  - `POST /groups` creates/returns an `image_groups` row
  - `GET /groups` lists existing groups
- DB migrations run against `internal_tools/label-db/studio-db` using the configured Postgres host/port variables.
  - `load_config()` resolves `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`.
  - `rslogic.db.migrate` escapes `%` in `DATABASE_URL` before invoking Alembic to avoid `configparser` interpolation errors with URL-encoded passwords.
  - For container-hosted DBs, common values are `postgis:5432`; for local host access, use your local forwarding port.

## Job execution

- `rslogic.jobs.service.JobOrchestrator` selects images by metadata filters and calls an `RsToolsRunner`.
- Job orchestration supports two execution backends:
  - `memory`: in-process `ThreadPoolExecutor` (legacy behavior).
  - `redis`: durable Redis list queue with worker consumers.
- `rslogic.api.app` keeps API contracts unchanged while queueing jobs through orchestrators.
  - In `redis` mode with `RSLOGIC_QUEUE_START_LOCAL_WORKERS=true`, API process also runs queue consumers.
  - For dedicated workers, set `RSLOGIC_QUEUE_START_LOCAL_WORKERS=false` on API and run `rslogic-worker`.
  - In `redis` mode, orchestrators ping Redis at startup and fail fast if Redis is unreachable (no implicit fallback).
  - API startup logs report effective orchestrator backends (`processing_backend` and `upload_backend`).
- Runner implementations currently include:
  - `StubRsToolsRunner`
  - `SubprocessRsToolsRunner`
  - `RsToolsSdkRunner` (optional, loaded from `realityscan_sdk`)
  - `RsToolsRemoteRunner` (dispatches jobs through Redis to the remote `rslogic-client`)
- `RsToolsSdkRunner` executes the same sequence as the notebook-based flow:
  - creates a session, runs `newScene`
  - applies `set` values (`appIncSubdirs`, `sfmCameraPriorAccuracy*`, `sfmDetectorSensitivity`)
  - calls `addFolder(<sdk_imagery_folder>)`
  - optionally runs `align`, `calculateNormalModel`, `calculateOrthoProjection`
  - optionally runs `save(<sdk_project_path>)`
  - polls task state with timeout (`sdk_task_timeout_seconds`) and returns command + node/project status in job summary
- The active runner is selected from `RSLOGIC_RSTOOLS_MODE` in config.
  - `remote` (or alias `rsnode_client`/`client`) dispatches work through Redis to an external RSNode worker.
  - If `RSLOGIC_RSTOOLS_MODE` is unset or `stub` and all SDK credentials are present
    (`RSLOGIC_RSTOOLS_SDK_BASE_URL`, `RSLOGIC_RSTOOLS_SDK_CLIENT_ID`,
    `RSLOGIC_RSTOOLS_SDK_APP_TOKEN`, `RSLOGIC_RSTOOLS_SDK_AUTH_TOKEN`),
    the server auto-selects `RsToolsRemoteRunner` instead of dry-run `StubRsToolsRunner`.

## File upload and ingestion

- `rslogic.storage.uploader.S3MultipartUploader` handles multipart/resume-capable uploads.
- `rslogic.services.ingestion.ImageIngestionService` parses uploaded image metadata server-side and persists records.
- `ImageIngestionService.upload_and_ingest_files` now processes per-file upload+ingest in a worker pool so parsing can start as each upload finishes, rather than waiting for all uploads first.
  - The batch ingest path filters out non-image files and ignores video assets.
- `ImageIngestionService.ingest_waiting_bucket_metadata` ingests waiting-bucket S3 user metadata directly (without downloading object bytes) and upserts rows in `image_assets`.
  - Waiting ingest now supports `override_existing`; when disabled existing records are returned as `skipped_existing`.
- `ImageUploadOrchestrator` is used by `POST /images/upload` to enqueue upload batches as background jobs; progress and final image IDs are persisted in job `result_summary`.

## Local tester

- `scratchpad/index.html` is a no-style HTML app for exercising local API endpoints during integration testing.
- `scratchpad/README.md` contains run instructions and endpoint checklist for health checks, ingest, image listing, and job APIs.

## RSNode server
 - Windows application running on a server that the `rstools-sdk` will make api requests to. 
 
