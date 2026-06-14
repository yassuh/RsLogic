# RsLogic v2

RsLogic v2 is being rebuilt as a Rust-first client orchestration and RealityScan pipeline system.

The old Python implementation has been moved under `deprecated/v1-python/` for reference.

- Architecture plan: `docs/RSLOGIC_V2_RUST_PLAN.md`
- Current implementation status: `docs/IMPLEMENTATION_STATUS.md`
- Acceptance audit: `docs/ACCEPTANCE_AUDIT.md`

## Workspace

```text
crates/rslogic-protocol     shared API, auth, job, telemetry, and websocket contracts
crates/rslogic-server       management server for enrollment, auth, desired state, and websocket control
crates/rslogic-agent        always-on local watchdog/client agent
crates/rslogic-worker       local pipeline worker for signed imagery jobs
crates/rslogic-realityscan  container adapter for RealityScan execution
```

## Local Development

```sh
cargo test --workspace
cargo run -p rslogic-server -- --bind 127.0.0.1:8080
cargo run -p rslogic-agent -- --management-url http://127.0.0.1:8080 --state-dir ./data/agent
```

The agent is designed to connect outbound to the management server. Runtime control should not rely on inbound SSH or a static client IP.

Use the durable Docker dev stack when testing jobs, clients, or dashboard state:

```sh
docker compose -f compose.dev.yml up -d postgres
$env:RSLOGIC_DATABASE_URL="postgres://rslogic:rslogic@127.0.0.1:54329/rslogic"
cargo run -p rslogic-server -- --bind 127.0.0.1:8080
```

Or run the API in Docker against the same database:

```sh
docker compose -f compose.dev.yml --profile api up api
```

Without `RSLOGIC_DATABASE_URL`, the server uses in-memory state. That path is useful for unit tests and throwaway smoke checks only; it will lose clients, jobs, events, and artifacts on restart.

For CloudFront signed input manifests, configure the management server with:

```sh
RSLOGIC_CLOUDFRONT_DOMAIN=d15n2niw0v0y8k.cloudfront.net
RSLOGIC_CLOUDFRONT_KEY_PAIR_ID=<cloudfront-public-key-id>
RSLOGIC_CLOUDFRONT_PRIVATE_KEY_FILE=/run/secrets/rslogic-cloudfront-private-key.pem
```

`RSLOGIC_CLOUDFRONT_PRIVATE_KEY_PEM` is also supported for development, but file/secret injection is the preferred deployment shape.

To resolve image assets from Studio API before signing CloudFront URLs, configure:

```sh
RSLOGIC_STUDIO_API_URL=https://<studio-api-host>
RSLOGIC_STUDIO_API_TOKEN=<server-side-token>
RSLOGIC_STUDIO_ASSET_PATH_TEMPLATE=/api/image-assets/{asset_id}
RSLOGIC_STUDIO_JOB_EVENT_PATH_TEMPLATE=/api/rslogic/jobs/{job_id}/events
RSLOGIC_STUDIO_ARTIFACT_PATH_TEMPLATE=/api/rslogic/jobs/{job_id}/artifacts
```

Then call `POST /api/admin/jobs/studio-manifest` with a `job_id`, expiry or TTL, and `asset_ids`. The server resolves asset metadata through Studio API and returns a signed worker input manifest.

When workers report job events, the management server stores the event locally and, if Studio API is configured, posts the lifecycle update back to Studio.

When workers report successful uploads, the management server stores the artifact metadata locally and, if Studio API is configured, posts `job_id`, `artifact_id`, `filename`, `storage_uri`, `content_type`, `sha256`, and `size_bytes` back to Studio.

For presigned output uploads, configure:

```sh
RSLOGIC_OUTPUT_BUCKET=<s3-output-bucket>
RSLOGIC_OUTPUT_PREFIX=rslogic/outputs
AWS_REGION=us-east-1
```

The management server uses normal AWS SDK credential resolution. The agent and worker still do not need AWS credentials.

Worker upload targets are explicit. Before artifact discovery, the worker packages each immediate directory under `outputs/` into a sibling `.zip` file, so a RealityScan project directory such as `outputs/preview-ortho/` becomes `outputs/preview-ortho.zip`. If a job includes output targets, each target `filename` must exist directly under `outputs/`; missing files fail the job instead of being skipped. The worker records discovered output metadata in `state.json`.

## NixOS

This repository exposes a Nix flake package for client hosts:

```sh
nix build .#rslogic-client
```

`rslogic-client` installs both:

```text
bin/rslogic-agent
bin/rslogic-worker
```

Service modules live in the sibling `yassuh-nixos` repo:

```text
modules/services/rslogic-agent.nix
modules/services/rslogic-worker.nix
```

They are imported by `hosts/yassuh-1/profile.nix` and remain disabled until the final package/binary deployment path and management URL are set.

Expected deployment shape after branch `v2` is pushed:

```nix
inputs.rslogic.url = "github:yassuh/RsLogic/v2";

yassuh.services.rslogicAgent.package = inputs.rslogic.packages.${pkgs.system}.rslogic-client;
yassuh.services.rslogicWorker.package = inputs.rslogic.packages.${pkgs.system}.rslogic-client;
```
