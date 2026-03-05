# RsLogic Codebase Architecture

## 1. System purpose

RsLogic is a photogrammetry workflow stack built around five persistent boundaries:

1. Local operator filesystem
2. S3-compatible object storage
3. Postgres/PostGIS (`studio-db`)
4. Redis command/result queues
5. RealityScan / RSNode HTTP API

The codebase turns a local image folder into:

1. Flat-keyed S3 objects in the waiting bucket
2. Parsed image records in Postgres
3. Image groups that can be targeted by jobs
4. Redis-dispatched workflow jobs
5. Client-side RealityScan execution with status fed back into Postgres and the FastAPI web UI at `/ui` plus client-local `rslogic-clientctl`

The repo is split into three code layers:

- `rslogic/*`: the application layer
- `rslogic/internal_tools/label-db/studio-db/*`: the database package RsLogic installs and imports as `studio_db`
- `rslogic/internal_tools/rstool-sdk/*`: the RealityScan SDK wrapper RsLogic installs and imports as `realityscan_sdk`

## 2. End-to-end pipelines

### 2.1 Upload pipeline

1. `rslogic.cli.upload` or `POST /ui/api/upload` in `rslogic.api.server` starts an `rslogic.api.web_ops.OperationRegistry` upload job.
2. `OperationRegistry` runs `rslogic.upload_service.FolderUploader`.
3. `FolderUploader` scans a local folder, groups images with sidecars by anchor/stem, hashes image bytes, and creates one `UploadRecord` per image.
4. Images are uploaded to the waiting bucket with flat keys of the form `sha256.ext`.
5. Matching sidecars are uploaded with the same hash stem.
6. If no sidecar exists, `rslogic.sidecar_parser.parse_exif` is used to synthesize a JSON sidecar payload and upload that instead.
7. A local upload manifest is written under `CONFIG.s3.manifest_dir`.

### 2.2 Ingest pipeline

1. `rslogic.ingest.IngestService` or `POST /ui/api/ingest` in `rslogic.api.server` starts ingest work.
2. `IngestService` lists objects in the waiting bucket with `rslogic.common.s3.s3_object_keys`.
3. It pairs flat image keys and flat sidecar keys into `IngestItem` objects.
4. Each image is downloaded to a temp file.
5. `rslogic.sidecar_parser.parse_exif`, `parse_sidecar`, and `extract_gps_from_exif` build the metadata payload.
6. `rslogic.common.db.LabelDbStore.create_image_asset` inserts an `RsLogicImageAsset` row.
7. If a group name was supplied, `LabelDbStore.get_or_create_group` and `attach_asset_to_group` connect the asset to an `ImageGroup`.
8. The image and any sidecars are moved from the waiting bucket to the processed bucket.
9. `LabelDbStore.update_asset_state` records move/completion metadata in the asset's `extra` JSON.

Important current behavior:

- S3 object keys stay flat. Grouping happens in Postgres, not in bucket prefixes.
- Ingest computes camera-derived values and stores them inside `metadata_json["derived"]`.
- The `RsLogicImageAsset` table has dedicated camera columns, but current ingest code does not populate those columns directly.

### 2.3 Job dispatch pipeline

1. `rslogic.api.server` exposes `POST /jobs`.
2. The request body is validated as `rslogic.common.schemas.JobRequest`.
3. If `group_name` is provided, the API resolves or creates the corresponding `ImageGroup`.
4. The API persists a `RsLogicRealityScanJob` row with status `queued`.
5. The API publishes a Redis job envelope through `rslogic.common.redis_bus.RedisBus`.
6. A target client pulls the job from its Redis command queue.

### 2.4 Client execution pipeline

1. `rslogic.client.runtime.ClientRuntime` loads `client.env` at import time and becomes the long-running worker.
2. It ensures RSNode is running through `rslogic.client.process_guard.RsNodeProcess`.
3. It creates:
   - a `RedisBus`
   - a `LabelDbStore`
   - a `FileExecutor`
   - optionally a `RealityScanClient`
4. For each incoming job it builds a `StepExecutor`.
5. `StepExecutor` executes each `Step`:
   - `kind="file"` routes to `FileExecutor`
   - `kind="sdk"` routes to `RealityScanClient.node` or `RealityScanClient.project`
6. If an SDK step returns task handles, `ClientRuntime` polls `project.tasks()` and `project.status()` until the requested task IDs are terminal.
7. Progress, task state, project state, and final results are published back to Redis.
8. The client also updates the `RsLogicRealityScanJob` row in Postgres.

### 2.5 Result consumption and operator visibility

1. `rslogic.api.server` runs a background Redis result consumer.
2. Incoming client updates are merged into `RsLogicRealityScanJob.result_summary`.
3. `GET /jobs` and `GET /jobs/{job_id}` expose merged task/project state.
4. `rslogic.api.server` also serves `/ui`, the static web assets, upload/ingest operation endpoints, job-builder metadata/import endpoints, and client queue/status endpoints consumed by the browser UI.
5. The browser web UI is the primary operator surface for upload, ingest, job building, job status, and orchestrator-side client monitoring.
6. The job builder intentionally gives the `CURRENT STEPS` rail more horizontal space than the step editor so longer chained workflows remain readable without collapsing into the preview pane.
7. `rslogic.client.control_tui.ClientControlTUI` remains a client-local supervisor by reading:
   - process PID/log files
   - Redis heartbeat
   - client queue depth

## 3. Runtime boundaries and owned state

| Boundary | Owner modules | State stored there |
| --- | --- | --- |
| Local upload folder | `rslogic.upload_service`, `rslogic.api.server`, `rslogic.api.web/*` | Source images and optional sidecars |
| Waiting bucket | `rslogic.upload_service`, `rslogic.ingest` | Raw uploaded image and sidecar objects |
| Processed bucket | `rslogic.ingest`, `rslogic.client.file_ops` | Ingested image payloads used for staging |
| Postgres/PostGIS | `rslogic.common.db`, `studio_db.models` | Image assets, groups, RealityScan jobs, plus labeling domain tables |
| Redis | `rslogic.common.redis_bus`, `rslogic.api.server`, `rslogic.client.runtime`, `rslogic.client.control_tui` | Job command queues, result queues, client heartbeats |
| Client working root | `rslogic.client.file_ops`, `rslogic.client.runtime` | `staging/`, `working/`, `sessions/<session>/_data/`, session state JSON |
| RealityScan/RSNode HTTP API | `realityscan_sdk` | Project session lifecycle, async task execution, project progress |

## 4. Repository map

### 4.1 Top-level files

| Path | Role |
| --- | --- |
| `pyproject.toml` | Package definition, editable internal dependencies, CLI entry points |
| `config.py` | Backward-compatible shim exporting `rslogic.config.CONFIG` |
| `rslogic_clientctl.py` | Top-level bootstrap launcher for the client control TUI |
| `ARCHITECTURE.md` | This document |
| `installer.bat` | Windows bootstrap/install helper for client machines |
| `job-contract.schema.json` | JSON Schema for external job payloads |
| `job-action-map.json` | Human-readable map of supported job step names/actions |
| `job-example.json` | Minimal sample job |
| `full_job_restarted.json` | Larger end-to-end example job |

### 4.2 Application packages

| Path | Role |
| --- | --- |
| `rslogic/config.py` | Canonical runtime configuration loader |
| `rslogic/common/schemas.py` | Shared workflow/job models |
| `rslogic/common/db.py` | Postgres access wrapper around `studio_db` models |
| `rslogic/common/redis_bus.py` | Redis queue, heartbeat, and client queue-depth abstraction |
| `rslogic/common/s3.py` | Thin S3 helpers used by upload/ingest/staging |
| `rslogic/sidecar_parser.py` | EXIF/XMP/XML/JSON metadata parsing |
| `rslogic/upload_service.py` | Local-folder-to-waiting-bucket uploader |
| `rslogic/ingest.py` | Waiting-bucket-to-DB-and-processed-bucket ingester |
| `rslogic/api/server.py` | FastAPI orchestrator, `/ui` web app, static asset host, upload/ingest op API, job/client API, and Redis result consumer |
| `rslogic/api/web_models.py` | Pydantic request models for the web UI upload/ingest/workflow-import endpoints |
| `rslogic/api/web_ops.py` | Background upload/ingest operation tracking for the web UI |
| `rslogic/api/web/index.html` | Operator web UI document shell |
| `rslogic/api/web/static/css/*` | Web UI visual system, layout, and dense tile styling |
| `rslogic/api/web/static/js/*` | Browser-side upload, ingest, jobs, clients, and job-builder behavior |
| `rslogic/client/executor.py` | Per-step dispatcher for file and SDK steps |
| `rslogic/client/file_ops.py` | Group staging and file copy operations |
| `rslogic/client/process_guard.py` | RSNode process supervision |
| `rslogic/client/runtime.py` | Long-running client worker |
| `rslogic/client/control_tui.py` | Client supervisor TUI and non-interactive commands |
| `rslogic/client/status_render.py` | Shared task/project/heartbeat render helpers used by client control surfaces |
| `rslogic/client/rsnode_client.py` | Runtime bootstrap entry point |
| `rslogic/tui/app.py` | Deprecated shim that forwards legacy orchestrator TUI entry paths to the web launcher |
| `rslogic/tui/launcher.py` | `rslogic-tui` bootstrap that re-execs with `PYTHON_GIL=0` / `-X gil=0` and launches the FastAPI web UI |
| `rslogic/tui/job_builder.py` | RealityScan job-draft presets, validation, and step editing helpers |
| `rslogic/cli/upload.py` | CLI wrapper around `FolderUploader` |
| `rslogic/db/migrate.py` | Alembic wrapper for `studio-db` migrations |
| `rslogic/jobs/worker.py` | Alias entry point to the client runtime |

### 4.3 Internal editable dependencies

| Path | Role |
| --- | --- |
| `rslogic/internal_tools/label-db/studio-db/*` | SQLAlchemy/PostGIS schema package imported as `studio_db` |
| `rslogic/internal_tools/rstool-sdk/*` | HTTP SDK package imported as `realityscan_sdk` |

### 4.4 Tests

| Path | Role |
| --- | --- |
| `tests/test_upload_service.py` | Upload keying, sidecar pairing, progress callback, synthetic sidecar coverage |
| `tests/test_ingest_service.py` | Ingest progress callback and S3 move/DB wiring coverage |
| `tests/test_sidecar_parser.py` | EXIF/XMP normalization and GPS extraction coverage |
| `tests/test_api_web.py` | FastAPI `/ui` shell, workflow import, directory listing, and client endpoint coverage |
| `tests/test_web_ops.py` | `OperationRegistry` success/error state transition coverage |
| `tests/test_status_render.py` | Client task/project status formatting coverage |
| `tests/test_redis_bus.py` | RedisBus heartbeat and queue-depth helper coverage |
| `tests/test_tui_launcher.py` | `rslogic-tui` GIL bootstrap and web-launch delegation coverage |
| `rslogic/internal_tools/rstool-sdk/tests/*` | SDK public API and resource exposure smoke tests |

## 5. Application class catalog (`rslogic/*`)

### 5.1 Configuration classes (`rslogic/config.py`)

| Class | Type | Role | Consumed by |
| --- | --- | --- | --- |
| `S3Config` | dataclass | Waiting/processed bucket names, endpoint, concurrency, manifest settings | Upload, ingest, file staging |
| `QueueConfig` | dataclass | Redis queue config and polling defaults | API, runtime, client control |
| `ControlConfig` | dataclass | Command/result queue keys and timeouts | API, runtime, client control |
| `RsToolsConfig` | dataclass | RSNode executable and RealityScan SDK connection settings | Runtime, client control |
| `LabelDbConfig` | dataclass | DB URL and Alembic location for `studio_db` | API, ingest, runtime, migrate |
| `ApiConfig` | dataclass | Base URL for local web/API callers | Web UI launcher, API callers |
| `LogConfig` | dataclass | Shared log formatting/level defaults | Service startup |
| `AppConfig` | dataclass | Typed aggregate of all config sections | Entire application via `CONFIG` |

### 5.2 Shared contract and infrastructure classes

| File | Class | Type | Role | Main callers |
| --- | --- | --- | --- | --- |
| `rslogic/common/schemas.py` | `Step` | Pydantic model | Normalized workflow step (`kind`, `action`, `params`, `timeout_s`) | API, runtime |
| `rslogic/common/schemas.py` | `JobRequest` | Pydantic model | External job submission contract | API, web job-builder submit flow |
| `rslogic/common/schemas.py` | `JobProgress` | dataclass | Lightweight progress container; not central to current orchestration path | Shared utilities / future progress handling |
| `rslogic/api/web_models.py` | `UploadStartRequest` | Pydantic model | Validates `POST /ui/api/upload` input | Web upload flow |
| `rslogic/api/web_models.py` | `IngestStartRequest` | Pydantic model | Validates `POST /ui/api/ingest` input | Web ingest flow |
| `rslogic/api/web_models.py` | `WorkflowImportRequest` | Pydantic model | Validates server-side workflow import input | Web job-builder import flow |
| `rslogic/common/db.py` | `LabelDbStore` | dataclass-backed service object | Imports `studio_db`, opens SQLAlchemy sessions, and encapsulates RsLogic DB operations | API, ingest, file staging, runtime |
| `rslogic/common/redis_bus.py` | `RedisBus` | service object | Publishes/pops command and result payloads, heartbeats, and client queue-depth lookups | API, runtime, client control |

`LabelDbStore` is the main seam between `rslogic/*` and the database package. Its explicit RsLogic operations are:

- `get_or_create_group`
- `create_image_asset`
- `attach_asset_to_group`
- `update_asset_state`
- `upsert_processing_job`
- `image_assets_for_group`

### 5.3 Upload and ingest classes

| File | Class | Type | Role | Upstream -> downstream |
| --- | --- | --- | --- | --- |
| `rslogic/upload_service.py` | `UploadRecord` | dataclass | One image plus its resolved sidecars and target S3 keys | `FolderUploader` -> S3 uploads |
| `rslogic/upload_service.py` | `FolderUploader` | service object | Scans folders, hashes images, pairs sidecars, uploads files, writes manifest | Web UI/CLI -> waiting bucket |
| `rslogic/ingest.py` | `IngestItem` | dataclass | One waiting-bucket image plus its matched sidecar keys | `_pair_objects` -> `_ingest_one` |
| `rslogic/ingest.py` | `IngestService` | service object | Pairs waiting objects, parses metadata, inserts DB rows, moves objects to processed bucket | Web UI/CLI -> DB + processed bucket |
| `rslogic/tui/job_builder.py` | `RealityScanJobDraft` | dataclass | In-memory RealityScan job builder with chainable workflow fragments, action catalog helpers, step editing, request validation, and preview formatting | Web job-builder inputs -> API job payload |

Important helper modules that these classes depend on:

- `rslogic.common.s3`: `make_client`, `s3_object_keys`, `copy_object`, `move_object`
- `rslogic.sidecar_parser`: `_to_json_value`, `parse_exif`, `parse_sidecar`, `extract_gps_from_exif`

### 5.4 Orchestrator and client execution classes

| File | Class | Type | Role | Main connections |
| --- | --- | --- | --- | --- |
| `rslogic/client/executor.py` | `StepExecutionResult` | dataclass | Typed step result with raw value plus extracted task IDs | `StepExecutor` -> `ClientRuntime` |
| `rslogic/client/executor.py` | `StepExecutor` | service object | Dispatches one `Step` to file or SDK handlers and maintains session-aware string templating context | `ClientRuntime` -> `FileExecutor` / `RealityScanClient` |
| `rslogic/client/file_ops.py` | `FileExecutor` | service object | Downloads group assets from processed storage into staging and copies staged files into working/session directories | `StepExecutor` -> filesystem + S3 + DB |
| `rslogic/client/process_guard.py` | `RsNodeProcess` | service object | Ensures RSNode is running, reuses external process if present, stops managed process on shutdown | `ClientRuntime` |
| `rslogic/client/runtime.py` | `ClientRuntime` | service object | Long-running client: queue polling, job locking, step execution, task polling, heartbeat publishing, DB/result updates | Redis + DB + filesystem + RSNode + RealityScan SDK |

How these classes are piped together:

1. `ClientRuntime` receives a Redis job envelope.
2. `ClientRuntime` builds `Step` objects.
3. `ClientRuntime` creates one `StepExecutor`.
4. `StepExecutor` calls `FileExecutor` for `kind="file"` work.
5. `StepExecutor` calls `RealityScanClient.node` or `.project` for `kind="sdk"` work.
6. `ClientRuntime` interprets `StepExecutionResult.task_ids` and polls async SDK tasks until terminal.

### 5.5 Web UI and process-control classes

| File | Class | Type | Role | Talks to |
| --- | --- | --- | --- | --- |
| `rslogic/api/web_ops.py` | `OperationState` | dataclass | In-memory status record for one background upload/ingest request | `OperationRegistry` -> `/ui/api/operations/*` |
| `rslogic/api/web_ops.py` | `OperationRegistry` | service object | Starts upload/ingest background threads and exposes recent operation snapshots to the web UI | `FolderUploader`, `IngestService`, `rslogic.api.server` |
| `rslogic/tui/app.py` | module shim | compatibility module | Deprecated legacy orchestrator entry path that forwards to the web launcher | `rslogic.tui.launcher` |
| `rslogic/tui/launcher.py` | module functions | bootstrap module | Re-execs the web launcher process with `PYTHON_GIL=0` so SQLAlchemy C extensions do not silently re-enable the GIL on free-threaded Python | `rslogic-tui` entry point |
| `rslogic/client/control_tui.py` | `_LogTailer` | helper class | Incremental log-file tail reader for client stdout/stderr panels | Client log files |
| `rslogic/client/control_tui.py` | `ClientProcessManager` | service object | Starts/stops/restarts the runtime, tracks PID file, reads Redis heartbeat, inspects queue depth and logs | Runtime process, Redis, local logs |
| `rslogic/client/control_tui.py` | `ClientControlTUI` | Textual app | Supervisory UI for one client machine | `ClientProcessManager` |

### 5.6 Modules without classes but with important ownership

| File | Main role |
| --- | --- |
| `rslogic/api/server.py` | FastAPI app, `/ui` shell, static asset serving, upload/ingest operation endpoints, job-builder metadata/import endpoints, job/client routes, and background result consumer |
| `rslogic/client/rsnode_client.py` | Ensures repo root on `sys.path`, then calls runtime `main()` |
| `rslogic/cli/upload.py` | Minimal argparse upload wrapper |
| `rslogic/db/migrate.py` | Minimal Alembic wrapper |
| `rslogic/jobs/worker.py` | Alias worker entry point to runtime `main()` |
| `config.py` | Compatibility export for `CONFIG` |
| `rslogic_clientctl.py` | Standalone launcher that imports `rslogic.client.control_tui` |

## 6. Database package model catalog (`studio_db`)

### 6.1 Package shape

- `rslogic/internal_tools/label-db/studio-db/models.py` contains the schema.
- `rslogic/internal_tools/label-db/studio-db/studio_db.py` re-exports `models.py` so the installed package can be imported as `studio_db`.
- `rslogic/internal_tools/label-db/studio-db/migrations/*` contains Alembic history.
- Clean-db bootstrap relies on the migrations themselves, not the current ORM state: `b7912b7a0663_create_model_tables.py` creates the shared DPC model tables but intentionally leaves `realityscan_jobs` to later revisions, and `a2d4f6e9b8c1_add_rslogic_job_and_image_tables.py` explicitly creates historical `processing_jobs` plus the legacy `image_assets.dataset_id` column expected by follow-on revisions.
- `models.py` also exposes compatibility alias `ImageAsset = RsLogicImageAsset`.

### 6.2 Base classes and enums

| Class | Kind | Role |
| --- | --- | --- |
| `Base` | SQLAlchemy declarative base | Root base class for all ORM models |
| `TimestampMixin` | mixin | Shared `created_at` and `updated_at` columns |
| `LayerSelectionMode` | enum | Project-layer selection policy |
| `LayerSourceType` | enum | Raster/layer source origin |
| `WorkOrderStatus` | enum | Lifecycle state for work orders |
| `WorkOrderSubmissionStatus` | enum | Lifecycle state for work-order submissions |
| `LabelGeometryStyle` | enum | Label geometry representation type |
| `CurveType` | enum | Edge curve interpolation type |
| `CoordinateSpace` | enum | Distinguishes geographic vs pixel-space AOI/geometry |

### 6.3 Studio domain models

These are the generic labeling/project models that exist alongside RsLogic's image/job tables.

| Model | Role | Key relationships |
| --- | --- | --- |
| `Account` | Top-level owner/account record | Has many `Profile`, `Project`, `ProjectLayer`, `WorkOrder`, `Label`, `WorkOrderSubmission` |
| `Profile` | User/profile inside an account | Belongs to `Account`; referenced as creator/submitted-by on other models |
| `Project` | Labeling/annotation project | Belongs to `Account`; optional creator `Profile`; has many `ProjectLayer`, `WorkOrder`, `Label` |
| `ProjectLayer` | A visual/source layer inside a project | Belongs to `Project` and `Account`; optional `created_by_profile`; optional FK to `RsLogicImageAsset` through `image_asset_id`; has many `WorkOrder` and `Label` |
| `WorkOrder` | Work unit tied to a project tile/layer | Belongs to `Project`, `ProjectLayer`, `Account`; has many `WorkOrderSubmission` and `WorkOrderLabel` |
| `WorkOrderSubmission` | One submission attempt for a work order | Belongs to `WorkOrder`, `Project`, `ProjectLayer`, `Account`; has many `Label` via `origin_submission` |
| `Label` | Label geometry record | Belongs to `Project`, `ProjectLayer`, `Account`; optional `origin_submission`; has many `LabelNode`, `LabelEdge`, `WorkOrderLabel` |
| `LabelNode` | Node/point inside a label geometry graph | Belongs to `Label` |
| `LabelEdge` | Edge between two `LabelNode` records | Belongs to `Label`; links `from_node` and `to_node` |
| `WorkOrderLabel` | Join table between `WorkOrder` and `Label` with overlap metadata | Belongs to `WorkOrder` and `Label` |

Current RsLogic usage of the studio domain models:

- `ProjectLayer` is the only generic studio model that directly references an RsLogic table (`RsLogicImageAsset`).
- The main RsLogic runtime does not currently create or update `Account`, `Profile`, `Project`, `WorkOrder`, `WorkOrderSubmission`, `Label`, `LabelNode`, `LabelEdge`, or `WorkOrderLabel`.
- Those models are present because `studio_db` is a broader labeling schema package that RsLogic shares.

### 6.4 RsLogic-specific database models

| Model | Role | Key relationships | Explicit RsLogic callers |
| --- | --- | --- | --- |
| `RsLogicImageAsset` | Canonical ingested image record | Has many `ProjectLayer` and `ImageGroupItem`; stores object locator, metadata JSON, geo columns, and extra state | `LabelDbStore.create_image_asset`, `update_asset_state`; `FileExecutor.stage_group`; ingest pipeline |
| `ImageGroup` | Named collection of image assets used as workflow input | Has many `ImageGroupItem` and `RsLogicRealityScanJob` | `LabelDbStore.get_or_create_group`; API job creation; ingest; client runtime group normalization |
| `ImageGroupItem` | Join table between `ImageGroup` and `RsLogicImageAsset` | Belongs to one group and one image | `LabelDbStore.attach_asset_to_group`; `image_assets_for_group`; file staging |
| `RsLogicRealityScanJob` | Persisted RealityScan workflow/job status record | Optional FK to `ImageGroup`; stores `job_name`, explicit `job_definition`, status, progress, message, and result summary | API create/list/status/result consumer; client runtime status updates |

How the RsLogic-specific models are piped:

1. `IngestService` inserts `RsLogicImageAsset`.
2. Optional ingest grouping creates/looks up `ImageGroup`.
3. `attach_asset_to_group` inserts `ImageGroupItem`.
4. API submission creates `RsLogicRealityScanJob`.
5. Runtime and API result-consumer continuously update `RsLogicRealityScanJob`.
6. `FileExecutor.stage_group` reads `ImageGroupItem` -> `RsLogicImageAsset` to download assets for job execution.

## 7. RealityScan SDK package catalog (`realityscan_sdk`)

### 7.1 Package shape

- Package-root public export is only `RealityScanClient`.
- `realityscan_sdk/client.py` contains the HTTP client object.
- `realityscan_sdk/resources/node.py` and `resources/project.py` expose the two API groups RsLogic uses.
- `realityscan_sdk/models/*` contains the parsed response dataclasses.

### 7.2 Classes

| Class | Kind | Role | Used by RsLogic |
| --- | --- | --- | --- |
| `ClientConfig` | dataclass | Stores base URL, client ID, app token, timeout, TLS, user agent | Built inside `RealityScanClient` |
| `RealityScanClient` | service object | Owns `httpx.Client`, session header management, request helper, and resource groups | Instantiated by `ClientRuntime`; called through `StepExecutor` |
| `NodeAPI` | resource object | Wraps `/node/*` endpoints such as `connect_user`, `disconnect_user`, `status`, and `projects` | `StepExecutor` resolves `sdk_node_*` actions here |
| `ProjectAPI` | resource object | Wraps `/project/*` lifecycle, status, tasks, generic command calls, file endpoints, and many convenience methods | `StepExecutor` resolves `sdk_project_*` and `sdk_*` actions here |
| `RSNodeConnectionInfo` | dataclass | Parsed `/node/connection` payload | Available to node calls; not central to current runtime loop |
| `RSProjectInformation` | dataclass | Parsed project listing entry | Available to node/project discovery |
| `RSNodeStatus` | dataclass | Parsed node status payload | Available to status calls |
| `RSProjectStatus` | dataclass | Parsed project progress payload | Polled by `ClientRuntime` for heartbeats and task waits |
| `TaskHandle` | dataclass | Minimal async task handle containing `taskID` | Returned by many `ProjectAPI` command methods; converted into task polling |
| `TaskStatus` | dataclass | Parsed task status payload | Polled by `ClientRuntime` through `project.tasks()` |

### 7.3 What RsLogic actually uses from the SDK

The RsLogic runtime mainly depends on these `ProjectAPI` and `NodeAPI` capabilities:

- `node.connect_user()`
- `node.disconnect_user()`
- `project.create()`
- `project.open()`
- `project.close()`
- `project.disconnect()`
- `project.save()`
- `project.new_scene()`
- `project.status()`
- `project.tasks()`
- `project.command(...)`
- dynamic method resolution for other `sdk_project_*` commands such as `sdk_project_add_folder`

`ProjectAPI` exposes many more command wrappers than RsLogic currently hard-codes. `StepExecutor` intentionally supports dynamic `sdk_project_*` and `sdk_node_*` action names so new SDK methods can be reached without adding a new dispatcher branch for each one.

## 8. Wiring and dependency tree

### 8.1 Upstream-to-downstream ownership

| Upstream | Downstream | Why |
| --- | --- | --- |
| Browser web UI at `/ui` | `rslogic.api.server` | Drive upload, ingest, job building, job status, and orchestrator-side client monitoring |
| `rslogic.api.server` | `OperationRegistry` | Run background upload and ingest work for the web UI |
| `OperationRegistry` | `FolderUploader` | Upload local folders into the waiting bucket |
| `OperationRegistry` | `IngestService` | Move waiting-bucket objects into processed storage and DB |
| `rslogic.api.server` | `LabelDbStore` | Persist `RsLogicRealityScanJob` records and resolve groups |
| `rslogic.api.server` | `RedisBus` | Dispatch job envelopes and consume result updates |
| `ClientRuntime` | `RsNodeProcess` | Ensure RSNode is available before/while executing jobs |
| `ClientRuntime` | `StepExecutor` | Execute validated workflow steps |
| `StepExecutor` | `FileExecutor` | Resolve/stage/copy image files for file steps |
| `StepExecutor` | `RealityScanClient` | Execute SDK/node/project commands |
| `FileExecutor` | `LabelDbStore.image_assets_for_group` | Resolve group assets to download |
| `FileExecutor` | `common.s3.make_client` | Download processed objects to local staging |
| `IngestService` | `sidecar_parser` | Build parsed metadata payloads |
| `ClientControlTUI` | `ClientProcessManager` | Process lifecycle, heartbeat, logs, queue visibility |

### 8.2 Storage flow summary

```text
local folder
  -> FolderUploader
  -> waiting bucket
  -> IngestService
  -> RsLogicImageAsset / ImageGroup / ImageGroupItem
  -> processed bucket
  -> API job submission
  -> Redis command queue
  -> ClientRuntime
  -> FileExecutor staging + RealityScan SDK execution
  -> Redis result queue + Redis heartbeat
  -> API result consumer
  -> RsLogicRealityScanJob.result_summary
  -> FastAPI `/ui` web UI / client control TUI / API clients
```

## 9. Entry points and scripts

| Script name | Target | Purpose |
| --- | --- | --- |
| `rslogic-upload` | `rslogic.cli.upload:main` | Upload a local folder to the waiting bucket |
| `rslogic-ingest` | `rslogic.ingest:main` | Ingest waiting-bucket objects into DB and processed bucket |
| `rslogic-api` | `rslogic.api.server:main` | Run the orchestrator API and `/ui` web app |
| `rslogic-client` | `rslogic.client.rsnode_client:main` | Run the client runtime |
| `rslogic-worker` | `rslogic.jobs.worker:main` | Alias for the client runtime |
| `rslogic-clientctl` | `rslogic_clientctl:main` | Run the client control TUI or client process commands |
| `rslogic-tui` | `rslogic.tui.launcher:main` | Run the FastAPI-backed operator web launcher with `PYTHON_GIL=0`; serves `/ui` |
| `rslogic-migrate` | `rslogic.db.migrate:main` | Apply `studio_db` migrations |

## 10. Test-only helper classes

These are not production architecture, but they are part of the repo and explain the test scaffolding.

| File | Class | Purpose |
| --- | --- | --- |
| `tests/test_upload_service.py` | `_FakeS3` | Captures upload calls for upload-service tests |
| `tests/test_ingest_service.py` | `_FakeS3` | Captures temp downloads for ingest tests |
| `tests/test_ingest_service.py` | `_FakeDb` | Minimal DB stub for ingest tests |
| `tests/test_sidecar_parser.py` | `_FakeImage` | Simulates EXIF payloads for parser normalization tests |
| `tests/test_sidecar_parser.py` | `_FakeImageForIngest` | Simulates image EXIF for ingest payload tests |
| `tests/test_sidecar_parser.py` | `_FakeImageForIngestGps` | Simulates GPS EXIF payloads for geodata tests |

## 11. Current architectural center of gravity

If you need to understand the codebase quickly, these are the most important files in dependency order:

1. `rslogic/config.py`
2. `rslogic/common/schemas.py`
3. `rslogic/common/db.py`
4. `rslogic/upload_service.py`
5. `rslogic/ingest.py`
6. `rslogic/api/server.py`
7. `rslogic/client/runtime.py`
8. `rslogic/client/executor.py`
9. `rslogic/client/file_ops.py`
10. `rslogic/internal_tools/label-db/studio-db/models.py`
11. `rslogic/internal_tools/rstool-sdk/src/realityscan_sdk/client.py`
12. `rslogic/internal_tools/rstool-sdk/src/realityscan_sdk/resources/project.py`

That chain is the actual pipe:

```text
config -> contracts -> DB/Redis adapters -> upload/ingest -> API dispatch
-> client runtime -> step executor -> file executor / RealityScan SDK
-> DB + Redis status -> FastAPI /ui web UI + client-local control TUI
```
