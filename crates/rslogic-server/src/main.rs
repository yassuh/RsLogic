mod cloudfront;
mod s3_upload;
mod store;
mod studio_api;

use std::{
    collections::{BTreeSet, HashMap},
    net::SocketAddr,
    path::PathBuf,
    sync::Arc,
    time::Duration as StdDuration,
};

use anyhow::Context;
use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        DefaultBodyLimit, Path, Query, State,
    },
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use chrono::Duration as ChronoDuration;
use clap::Parser;
use futures_util::{SinkExt, StreamExt};
use rslogic_protocol::{
    new_id, now, verify_challenge_signature, CameraIntrinsics, Challenge, ClientEvent,
    CloudfrontInput, DesiredState, EnrollmentApproval, EnrollmentRejection, EnrollmentRequest,
    EnrollmentRequestRecord, JobEvent, JobInputManifest, OrthoRenderMethod, OutputUploadTarget,
    PipelineJob, RealityScanAlignmentSettings, RealityScanPipeline, RealityScanStage,
    ServerCommand, SessionRequest, SessionToken, UploadedArtifact, PROTOCOL_VERSION,
    YASSUH_IMAGERY_CLOUDFRONT_DOMAIN,
};
use serde::{Deserialize, Serialize};
use store::{
    AdminClientRecord, ClientRecord, InMemoryStore, JobRecord, PostgresStore, QueuedCommand, Store,
    StoreError,
};
use tokio::{
    sync::{broadcast, RwLock},
    time,
};
use tower_http::trace::TraceLayer;
use tracing::{debug, error, info, warn};
use tracing_subscriber::{fmt, EnvFilter};

const ADMIN_BODY_LIMIT_BYTES: usize = 64 * 1024 * 1024;
const DEFAULT_IMAGERY_ASSET_LIMIT: usize = 1_000;
const MAX_IMAGERY_ASSET_LIMIT: usize = 5_000;

#[derive(Debug, Parser)]
struct Args {
    #[arg(long, env = "RSLOGIC_SERVER_BIND", default_value = "127.0.0.1:8080")]
    bind: SocketAddr,
    #[arg(long, env = "RSLOGIC_DATABASE_URL")]
    database_url: Option<String>,
    #[arg(
        long,
        env = "RSLOGIC_CLOUDFRONT_DOMAIN",
        default_value = YASSUH_IMAGERY_CLOUDFRONT_DOMAIN
    )]
    cloudfront_domain: String,
    #[arg(long, env = "RSLOGIC_CLOUDFRONT_KEY_PAIR_ID")]
    cloudfront_key_pair_id: Option<String>,
    #[arg(long, env = "RSLOGIC_CLOUDFRONT_PRIVATE_KEY_FILE")]
    cloudfront_private_key_file: Option<PathBuf>,
    #[arg(long, env = "RSLOGIC_CLOUDFRONT_PRIVATE_KEY_PEM")]
    cloudfront_private_key_pem: Option<String>,
    #[arg(long, env = "RSLOGIC_OUTPUT_BUCKET")]
    output_bucket: Option<String>,
    #[arg(long, env = "RSLOGIC_OUTPUT_PREFIX", default_value = "rslogic/outputs")]
    output_prefix: String,
    #[arg(
        long,
        env = "RSLOGIC_STUDIO_API_URL",
        default_value = "https://studio.yassuh.dev"
    )]
    studio_api_url: String,
    #[arg(long, env = "RSLOGIC_STUDIO_API_TOKEN")]
    studio_api_token: Option<String>,
    #[arg(
        long,
        env = "RSLOGIC_STUDIO_LOGIN_PATH",
        default_value = "/api/v1/auth/login"
    )]
    studio_login_path: String,
    #[arg(long, env = "RSLOGIC_STUDIO_API_EMAIL")]
    studio_api_email: Option<String>,
    #[arg(long, env = "RSLOGIC_STUDIO_API_PASSWORD")]
    studio_api_password: Option<String>,
    #[arg(
        long,
        env = "RSLOGIC_STUDIO_IMAGE_ASSETS_PATH",
        default_value = "/api/v1/image-assets"
    )]
    studio_image_assets_path: String,
    #[arg(
        long,
        env = "RSLOGIC_STUDIO_ASSET_PATH_TEMPLATE",
        default_value = "/api/v1/image-assets/{asset_id}"
    )]
    studio_asset_path_template: String,
    #[arg(
        long,
        env = "RSLOGIC_STUDIO_CAMERA_INTRINSICS_PATH_TEMPLATE",
        default_value = "/api/v1/camera-intrinsics/{camera_name}"
    )]
    studio_camera_intrinsics_path_template: String,
    #[arg(
        long,
        env = "RSLOGIC_STUDIO_JOB_EVENT_PATH_TEMPLATE",
        default_value = "/api/rslogic/jobs/{job_id}/events"
    )]
    studio_job_event_path_template: String,
    #[arg(
        long,
        env = "RSLOGIC_STUDIO_ARTIFACT_PATH_TEMPLATE",
        default_value = "/api/rslogic/jobs/{job_id}/artifacts"
    )]
    studio_artifact_path_template: String,
    #[arg(
        long,
        env = "RSLOGIC_STUDIO_IMAGE_CACHE_TTL_SECONDS",
        default_value_t = 60
    )]
    studio_image_cache_ttl_seconds: i64,
}

#[derive(Clone)]
struct AppState {
    store: Arc<dyn Store>,
    cloudfront: Option<Arc<cloudfront::CloudFrontUrlSigner>>,
    s3_output: Option<Arc<s3_upload::S3OutputPresigner>>,
    studio: Option<Arc<studio_api::StudioApiClient>>,
    cloudfront_domain: String,
    admin_events: broadcast::Sender<AdminStreamMessage>,
    studio_assets_cache: Arc<RwLock<Option<StudioAssetSnapshot>>>,
    studio_assets_cache_ttl: ChronoDuration,
}

impl AppState {
    fn new(
        store: Arc<dyn Store>,
        cloudfront: Option<Arc<cloudfront::CloudFrontUrlSigner>>,
        s3_output: Option<Arc<s3_upload::S3OutputPresigner>>,
        studio: Option<Arc<studio_api::StudioApiClient>>,
        cloudfront_domain: String,
        studio_assets_cache_ttl: ChronoDuration,
    ) -> Self {
        Self {
            store,
            cloudfront,
            s3_output,
            studio,
            cloudfront_domain,
            admin_events: broadcast::channel(256).0,
            studio_assets_cache: Arc::new(RwLock::new(None)),
            studio_assets_cache_ttl,
        }
    }

    #[cfg(test)]
    fn in_memory() -> Self {
        Self::new(
            Arc::new(InMemoryStore::default()),
            None,
            None,
            None,
            YASSUH_IMAGERY_CLOUDFRONT_DOMAIN.to_string(),
            ChronoDuration::seconds(60),
        )
    }
}

#[derive(Debug, Clone)]
struct StudioAssetSnapshot {
    loaded_at: chrono::DateTime<chrono::Utc>,
    assets: Arc<Vec<studio_api::StudioImageAsset>>,
}

#[derive(Debug, Deserialize)]
struct WsAuthQuery {
    token: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct CloudFrontManifestRequest {
    job_id: String,
    expires_at: Option<chrono::DateTime<chrono::Utc>>,
    ttl_seconds: Option<i64>,
    inputs: Vec<CloudFrontManifestInputRequest>,
}

#[derive(Debug, Deserialize, Serialize)]
struct CloudFrontManifestInputRequest {
    asset_id: String,
    filename: String,
    cloudfront_path: String,
    sha256: Option<String>,
    size_bytes: Option<u64>,
    #[serde(default, alias = "cameraIntrinsics", alias = "intrinsics")]
    camera_intrinsics: Option<CameraIntrinsics>,
}

#[derive(Debug, Deserialize, Serialize)]
struct StudioManifestRequest {
    job_id: String,
    expires_at: Option<chrono::DateTime<chrono::Utc>>,
    ttl_seconds: Option<i64>,
    asset_ids: Vec<String>,
}

#[derive(Debug, Deserialize, Serialize)]
struct OutputUploadTargetsRequest {
    job_id: String,
    ttl_seconds: Option<u64>,
    artifacts: Vec<OutputArtifactUploadRequest>,
}

#[derive(Debug, Deserialize, Serialize)]
struct OutputArtifactUploadRequest {
    artifact_id: String,
    filename: String,
    content_type: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct JobTemplate {
    template_id: String,
    name: String,
    description: String,
    stages: Vec<RealityScanStage>,
    project_filename: String,
    #[serde(default)]
    resume_source_job_id: Option<String>,
    #[serde(default)]
    resume_project_filename: Option<String>,
    orthomosaic_filename: Option<String>,
    #[serde(default)]
    ortho_pixel_size_meters: Option<f64>,
    #[serde(default)]
    ortho_render_method: Option<OrthoRenderMethod>,
    #[serde(default)]
    ortho_projection_params_xml: Option<String>,
    #[serde(default)]
    alignment_settings: Option<RealityScanAlignmentSettings>,
    #[serde(default)]
    print_progress_interval_seconds: Option<u32>,
}

#[derive(Debug, Deserialize, Serialize)]
struct BuildJobRequest {
    client_id: String,
    template_id: String,
    #[serde(default)]
    custom_template: Option<JobTemplate>,
    job_name: Option<String>,
    source: JobImageSelection,
    ttl_seconds: Option<i64>,
    realityscan_image: Option<String>,
    dry_run: Option<bool>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
enum JobImageSelection {
    GroupName { group_name: String },
    Polygon { coordinates: Vec<[f64; 2]> },
}

#[derive(Debug, Serialize)]
struct BuildJobResponse {
    dry_run: bool,
    selected_assets: Vec<SelectedJobAsset>,
    job: Option<PipelineJob>,
    queued_command: Option<QueuedCommand>,
    warnings: Vec<String>,
}

#[derive(Debug, Serialize)]
struct SelectedJobAsset {
    asset_id: String,
    filename: String,
    group_name: Option<String>,
    latitude: Option<f64>,
    longitude: Option<f64>,
    size_bytes: Option<u64>,
}

#[derive(Debug, Deserialize, Serialize)]
struct ClientRevocation {
    client_id: String,
    revoked_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Deserialize)]
struct ListJobEventsQuery {
    job_id: Option<String>,
    limit: Option<u32>,
}

#[derive(Debug, Deserialize)]
struct ListImageryQuery {
    limit: Option<usize>,
    offset: Option<usize>,
    group_name: Option<String>,
    bbox: Option<String>,
    q: Option<String>,
    geocoded: Option<bool>,
    refresh: Option<bool>,
}

#[derive(Debug, Serialize)]
struct AdminImageryAssetsResponse {
    image_assets: Vec<studio_api::StudioImageAsset>,
    total_assets: usize,
    matched_assets: usize,
    returned_assets: usize,
    offset: usize,
    limit: usize,
    next_offset: Option<usize>,
    refreshed_at: chrono::DateTime<chrono::Utc>,
    cache_age_seconds: i64,
}

#[derive(Debug, Serialize)]
struct AdminImageryGroupsResponse {
    total_assets: usize,
    geocoded_assets: usize,
    group_count: usize,
    groups: Vec<AdminImageryGroupSummary>,
    refreshed_at: chrono::DateTime<chrono::Utc>,
    cache_age_seconds: i64,
}

#[derive(Debug, Serialize)]
struct AdminImageryGroupSummary {
    key: String,
    label: String,
    source: String,
    group_name: Option<String>,
    asset_count: usize,
    geocoded_count: usize,
    size_bytes: Option<u64>,
    captured_start: Option<String>,
    captured_end: Option<String>,
    camera_summary: Option<String>,
    bounds: Option<AdminImageryBounds>,
}

#[derive(Debug, Clone, Copy, Serialize)]
struct AdminImageryBounds {
    min_latitude: f64,
    max_latitude: f64,
    min_longitude: f64,
    max_longitude: f64,
}

#[derive(Debug, Clone)]
struct ImageryGroupIdentity {
    key: String,
    label: String,
    source: String,
    group_name: Option<String>,
}

#[derive(Debug)]
struct ImageryGroupAccumulator {
    identity: ImageryGroupIdentity,
    asset_count: usize,
    geocoded_count: usize,
    size_bytes: u64,
    captured_start: Option<String>,
    captured_end: Option<String>,
    cameras: BTreeSet<String>,
    bounds: Option<AdminImageryBounds>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum AdminStreamMessage {
    Snapshot {
        reason: String,
        observed_at: chrono::DateTime<chrono::Utc>,
        clients: Vec<AdminClientRecord>,
        jobs: Vec<JobRecord>,
        job_events: Vec<JobEvent>,
    },
}

#[derive(Debug, Serialize)]
struct HealthResponse {
    status: &'static str,
    version: &'static str,
    protocol_version: &'static str,
    build_sha: Option<&'static str>,
    capabilities: HealthCapabilities,
}

#[derive(Debug, Serialize)]
struct HealthCapabilities {
    admin_job_events: bool,
    admin_websocket_job_events: bool,
    artifacts: bool,
    cloudfront_manifests: bool,
    postgres_store: bool,
    studio_api: bool,
    worker_status: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    init_tracing();

    let store: Arc<dyn Store> = match args.database_url.as_deref() {
        Some(database_url) => {
            info!("connecting rslogic management server to Postgres");
            Arc::new(
                PostgresStore::connect(database_url)
                    .await
                    .context("initializing Postgres store")?,
            )
        }
        None => {
            warn!("RSLOGIC_DATABASE_URL is not set; using in-memory management state");
            Arc::new(InMemoryStore::default())
        }
    };

    let cloudfront = cloudfront_signer_from_args(&args).await?;
    let s3_output = s3_output_from_args(&args).await?;
    let studio = studio_from_args(&args);
    let app = router(AppState::new(
        store,
        cloudfront,
        s3_output,
        studio,
        args.cloudfront_domain.clone(),
        ChronoDuration::seconds(args.studio_image_cache_ttl_seconds.max(0)),
    ));
    let listener = tokio::net::TcpListener::bind(args.bind)
        .await
        .with_context(|| format!("binding {}", args.bind))?;
    info!(
        "rslogic management server listening on http://{}",
        args.bind
    );
    axum::serve(listener, app).await?;
    Ok(())
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    fmt().with_env_filter(filter).init();
}

fn router(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route(
            "/api/client-enrollment/request",
            post(create_enrollment_request),
        )
        .route(
            "/api/client-enrollment/request/:request_id",
            get(get_enrollment_request),
        )
        .route(
            "/api/admin/client-enrollment/requests",
            get(list_enrollment_requests),
        )
        .route(
            "/api/admin/client-enrollment/requests/:request_id/approve",
            post(approve_enrollment_request),
        )
        .route(
            "/api/admin/client-enrollment/requests/:request_id/reject",
            post(reject_enrollment_request),
        )
        .route("/api/clients/:client_id/challenge", post(create_challenge))
        .route("/api/clients/:client_id/session", post(create_session))
        .route(
            "/api/clients/:client_id/desired-state",
            get(get_desired_state),
        )
        .route("/api/admin/clients", get(list_clients))
        .route("/api/admin/events", get(admin_events_websocket))
        .route("/api/admin/imagery/groups", get(list_imagery_groups))
        .route("/api/admin/imagery/assets", get(list_imagery_assets))
        .route("/api/admin/clients/:client_id/revoke", post(revoke_client))
        .route(
            "/api/admin/clients/:client_id/commands",
            post(enqueue_client_command),
        )
        .route(
            "/api/admin/clients/:client_id/jobs",
            post(enqueue_client_job),
        )
        .route(
            "/api/admin/jobs/cloudfront-manifest",
            post(create_cloudfront_manifest),
        )
        .route(
            "/api/admin/jobs/studio-manifest",
            post(create_studio_manifest),
        )
        .route(
            "/api/admin/jobs/output-targets",
            post(create_output_upload_targets),
        )
        .route("/api/admin/job-templates", get(list_job_templates))
        .route("/api/admin/jobs/build", post(build_job_from_imagery))
        .route("/api/admin/jobs", get(list_jobs))
        .route("/api/admin/job-events", get(list_job_events))
        .route("/api/admin/artifacts", get(list_artifacts))
        .route("/api/clients/:client_id/connect", get(client_websocket))
        .layer(DefaultBodyLimit::max(ADMIN_BODY_LIMIT_BYTES))
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

async fn healthz() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok",
        version: env!("CARGO_PKG_VERSION"),
        protocol_version: PROTOCOL_VERSION,
        build_sha: option_env!("RSLOGIC_BUILD_SHA"),
        capabilities: HealthCapabilities {
            admin_job_events: true,
            admin_websocket_job_events: true,
            artifacts: true,
            cloudfront_manifests: true,
            postgres_store: true,
            studio_api: true,
            worker_status: true,
        },
    })
}

async fn create_enrollment_request(
    State(state): State<AppState>,
    Json(request): Json<EnrollmentRequest>,
) -> Result<Json<EnrollmentRequestRecord>, ApiError> {
    let record = state
        .store
        .create_enrollment_request(request)
        .await
        .map_err(ApiError::from_store)?;
    info!(
        request_id = record.request_id,
        hostname = record.request.hostname,
        machine_id = record.request.machine_id,
        "created enrollment request"
    );
    Ok(Json(record))
}

async fn get_enrollment_request(
    State(state): State<AppState>,
    Path(request_id): Path<String>,
) -> Result<Json<EnrollmentRequestRecord>, ApiError> {
    let record = state
        .store
        .get_enrollment_request(&request_id)
        .await
        .map_err(ApiError::from_store)?
        .ok_or_else(|| ApiError::not_found("enrollment request not found"))?;
    Ok(Json(record))
}

async fn list_enrollment_requests(
    State(state): State<AppState>,
) -> Result<Json<Vec<EnrollmentRequestRecord>>, ApiError> {
    let records = state
        .store
        .list_enrollment_requests()
        .await
        .map_err(ApiError::from_store)?;
    Ok(Json(records))
}

async fn approve_enrollment_request(
    State(state): State<AppState>,
    Path(request_id): Path<String>,
) -> Result<Json<EnrollmentApproval>, ApiError> {
    let approval = state
        .store
        .approve_enrollment_request(&request_id)
        .await
        .map_err(ApiError::from_store)?;
    info!(
        request_id = approval.request_id,
        client_id = approval.client_id,
        "approved enrollment request"
    );
    publish_admin_snapshot(&state, "client_approved").await;
    Ok(Json(approval))
}

async fn reject_enrollment_request(
    State(state): State<AppState>,
    Path(request_id): Path<String>,
    Json(rejection): Json<EnrollmentRejection>,
) -> Result<Json<EnrollmentRequestRecord>, ApiError> {
    let record = state
        .store
        .reject_enrollment_request(&request_id, rejection.reason)
        .await
        .map_err(ApiError::from_store)?;
    info!(request_id, "rejected enrollment request");
    publish_admin_snapshot(&state, "enrollment_rejected").await;
    Ok(Json(record))
}

async fn create_challenge(
    State(state): State<AppState>,
    Path(client_id): Path<String>,
) -> Result<Json<Challenge>, ApiError> {
    active_client(&state, &client_id).await?;
    let challenge = state
        .store
        .create_challenge(&client_id)
        .await
        .map_err(ApiError::from_store)?
        .ok_or_else(|| ApiError::not_found("client not found"))?;
    Ok(Json(challenge))
}

async fn create_session(
    State(state): State<AppState>,
    Path(client_id): Path<String>,
    Json(request): Json<SessionRequest>,
) -> Result<Json<SessionToken>, ApiError> {
    let client = state
        .store
        .get_client(&client_id)
        .await
        .map_err(ApiError::from_store)?
        .ok_or_else(|| ApiError::not_found("client not found"))?;
    if client.revoked_at.is_some() {
        return Err(ApiError::unauthorized("client revoked"));
    }
    let challenge = state
        .store
        .consume_challenge(&request.challenge_id)
        .await
        .map_err(ApiError::from_store)?
        .ok_or_else(|| ApiError::bad_request("challenge not found"))?;
    if challenge.client_id != client_id {
        return Err(ApiError::unauthorized(
            "challenge belongs to another client",
        ));
    }
    if challenge.expires_at < now() {
        return Err(ApiError::unauthorized("challenge expired"));
    }
    verify_challenge_signature(&client.public_key, &challenge, &request.signature)
        .map_err(|_| ApiError::unauthorized("challenge signature invalid"))?;

    let token = SessionToken {
        client_id: client_id.clone(),
        token: new_id(),
        expires_at: now() + ChronoDuration::hours(12),
    };
    let token = state
        .store
        .create_session(token)
        .await
        .map_err(ApiError::from_store)?;
    info!(client_id, "created client session");
    Ok(Json(token))
}

async fn get_desired_state(
    State(state): State<AppState>,
    Path(client_id): Path<String>,
) -> Result<Json<DesiredState>, ApiError> {
    let client = active_client(&state, &client_id).await?;
    debug!(
        client_id = client.client_id,
        approved_at = %client.approved_at,
        "desired state requested"
    );
    Ok(Json(client.desired_state))
}

async fn list_clients(
    State(state): State<AppState>,
) -> Result<Json<Vec<AdminClientRecord>>, ApiError> {
    let clients = state
        .store
        .list_clients()
        .await
        .map_err(ApiError::from_store)?;
    Ok(Json(clients))
}

async fn list_imagery_assets(
    State(state): State<AppState>,
    Query(query): Query<ListImageryQuery>,
) -> Result<Json<AdminImageryAssetsResponse>, ApiError> {
    let snapshot = load_studio_assets(&state, query.refresh.unwrap_or(false)).await?;
    let bbox = query.bbox.as_deref().map(parse_bbox).transpose()?;
    let group_name = query
        .group_name
        .as_deref()
        .and_then(nonempty_trimmed)
        .map(str::to_string);
    let search = query
        .q
        .as_deref()
        .and_then(nonempty_trimmed)
        .map(|value| value.to_lowercase());
    let geocoded_only = query.geocoded.unwrap_or(false);
    let limit = query
        .limit
        .unwrap_or(DEFAULT_IMAGERY_ASSET_LIMIT)
        .clamp(1, MAX_IMAGERY_ASSET_LIMIT);
    let offset = query.offset.unwrap_or(0);
    let matched_assets = snapshot
        .assets
        .iter()
        .filter(|asset| {
            if geocoded_only && !asset_has_location(asset) {
                return false;
            }
            if let Some(group_name) = group_name.as_deref() {
                if !asset_matches_group(asset, group_name) {
                    return false;
                }
            }
            if let Some(bounds) = bbox {
                if !asset_in_bounds(asset, bounds) {
                    return false;
                }
            }
            if let Some(search) = search.as_deref() {
                if !asset_matches_search(asset, search) {
                    return false;
                }
            }
            true
        })
        .collect::<Vec<_>>();
    let page = matched_assets
        .iter()
        .skip(offset)
        .take(limit)
        .map(|asset| (*asset).clone())
        .collect::<Vec<_>>();
    let next_offset = (offset + page.len() < matched_assets.len()).then_some(offset + page.len());
    let cache_age_seconds = (now() - snapshot.loaded_at).num_seconds().max(0);

    Ok(Json(AdminImageryAssetsResponse {
        total_assets: snapshot.assets.len(),
        matched_assets: matched_assets.len(),
        returned_assets: page.len(),
        offset,
        limit,
        next_offset,
        refreshed_at: snapshot.loaded_at,
        cache_age_seconds,
        image_assets: page,
    }))
}

async fn list_imagery_groups(
    State(state): State<AppState>,
    Query(query): Query<ListImageryQuery>,
) -> Result<Json<AdminImageryGroupsResponse>, ApiError> {
    let snapshot = load_studio_assets(&state, query.refresh.unwrap_or(false)).await?;
    let mut groups = HashMap::<String, ImageryGroupAccumulator>::new();
    let mut geocoded_assets = 0usize;

    for asset in snapshot.assets.iter() {
        if asset_has_location(asset) {
            geocoded_assets += 1;
        }
        let identity = asset_group_identity(asset);
        groups
            .entry(identity.key.clone())
            .and_modify(|group| group.add_asset(asset))
            .or_insert_with(|| {
                let mut group = ImageryGroupAccumulator::new(identity);
                group.add_asset(asset);
                group
            });
    }

    let mut summaries = groups
        .into_values()
        .map(ImageryGroupAccumulator::into_summary)
        .collect::<Vec<_>>();
    summaries.sort_by(|left, right| {
        right
            .asset_count
            .cmp(&left.asset_count)
            .then_with(|| left.label.cmp(&right.label))
    });

    let cache_age_seconds = (now() - snapshot.loaded_at).num_seconds().max(0);
    Ok(Json(AdminImageryGroupsResponse {
        total_assets: snapshot.assets.len(),
        geocoded_assets,
        group_count: summaries.len(),
        groups: summaries,
        refreshed_at: snapshot.loaded_at,
        cache_age_seconds,
    }))
}

async fn revoke_client(
    State(state): State<AppState>,
    Path(client_id): Path<String>,
) -> Result<Json<ClientRevocation>, ApiError> {
    let client = state
        .store
        .revoke_client(&client_id)
        .await
        .map_err(ApiError::from_store)?
        .ok_or_else(|| ApiError::not_found("client not found"))?;
    let revoked_at = client
        .revoked_at
        .ok_or_else(|| ApiError::internal("client revocation timestamp missing"))?;
    info!(client_id, %revoked_at, "revoked client");
    publish_admin_snapshot(&state, "client_revoked").await;
    Ok(Json(ClientRevocation {
        client_id,
        revoked_at,
    }))
}

async fn enqueue_client_command(
    State(state): State<AppState>,
    Path(client_id): Path<String>,
    Json(command): Json<ServerCommand>,
) -> Result<Json<QueuedCommand>, ApiError> {
    active_client(&state, &client_id).await?;
    let queued = state
        .store
        .enqueue_command(&client_id, command)
        .await
        .map_err(ApiError::from_store)?
        .ok_or_else(|| ApiError::not_found("client not found"))?;
    info!(
        client_id = queued.client_id,
        command_id = queued.command_id,
        "queued client command"
    );
    publish_admin_snapshot(&state, "command_queued").await;
    Ok(Json(queued))
}

async fn enqueue_client_job(
    State(state): State<AppState>,
    Path(client_id): Path<String>,
    Json(job): Json<PipelineJob>,
) -> Result<Json<QueuedCommand>, ApiError> {
    active_client(&state, &client_id).await?;
    state
        .store
        .record_job_assignment(&client_id, job.clone())
        .await
        .map_err(ApiError::from_store)?;
    let queued = state
        .store
        .enqueue_command(&client_id, ServerCommand::AssignJob { job })
        .await
        .map_err(ApiError::from_store)?
        .ok_or_else(|| ApiError::not_found("client not found"))?;
    info!(
        client_id = queued.client_id,
        command_id = queued.command_id,
        "queued client job"
    );
    publish_admin_snapshot(&state, "job_queued").await;
    Ok(Json(queued))
}

async fn list_jobs(State(state): State<AppState>) -> Result<Json<Vec<JobRecord>>, ApiError> {
    let jobs = state
        .store
        .list_jobs()
        .await
        .map_err(ApiError::from_store)?;
    Ok(Json(jobs))
}

async fn list_job_events(
    State(state): State<AppState>,
    Query(query): Query<ListJobEventsQuery>,
) -> Result<Json<Vec<JobEvent>>, ApiError> {
    let events = state
        .store
        .list_job_events(query.job_id.as_deref(), query.limit.unwrap_or(200))
        .await
        .map_err(ApiError::from_store)?;
    Ok(Json(events))
}

async fn list_artifacts(
    State(state): State<AppState>,
) -> Result<Json<Vec<UploadedArtifact>>, ApiError> {
    let artifacts = state
        .store
        .list_uploaded_artifacts()
        .await
        .map_err(ApiError::from_store)?;
    Ok(Json(artifacts))
}

async fn list_job_templates() -> Json<Vec<JobTemplate>> {
    Json(job_templates())
}

async fn build_job_from_imagery(
    State(state): State<AppState>,
    Json(request): Json<BuildJobRequest>,
) -> Result<Json<BuildJobResponse>, ApiError> {
    active_client(&state, &request.client_id).await?;
    let studio = state
        .studio
        .as_ref()
        .ok_or_else(|| ApiError::service_unavailable("Studio API client is not configured"))?;
    let template = resolve_job_template(&request)?;
    let all_assets = load_studio_assets(&state, false).await?;
    let mut warnings = Vec::new();
    let mut selected_assets =
        select_job_assets(all_assets.assets.as_ref().as_slice(), &request.source)?;
    selected_assets.sort_by(|left, right| {
        left.captured_at
            .cmp(&right.captured_at)
            .then_with(|| left.filename().cmp(&right.filename()))
    });
    if selected_assets.is_empty() {
        return Err(ApiError::bad_request(
            "image selection did not match any Studio assets",
        ));
    }

    let selected_summary = selected_assets
        .iter()
        .map(selected_job_asset)
        .collect::<Vec<_>>();
    let dry_run = request.dry_run.unwrap_or(false);
    if dry_run {
        return Ok(Json(BuildJobResponse {
            dry_run,
            selected_assets: selected_summary,
            job: None,
            queued_command: None,
            warnings,
        }));
    }

    let signer = state
        .cloudfront
        .as_ref()
        .ok_or_else(|| ApiError::service_unavailable("CloudFront signer is not configured"))?;
    let job_id = new_id();
    let expires_at =
        now() + ChronoDuration::seconds(request.ttl_seconds.unwrap_or(6 * 60 * 60).max(60));
    let mut manifest_assets = selected_assets;
    warnings.extend(resolve_camera_intrinsics_for_assets(studio, &mut manifest_assets).await);
    let manifest = manifest_from_studio_assets(
        &state.cloudfront_domain,
        signer,
        job_id.clone(),
        expires_at,
        manifest_assets,
    )?;
    let job = PipelineJob {
        job_id,
        job_name: Some(
            request
                .job_name
                .filter(|value| !value.trim().is_empty())
                .unwrap_or_else(|| template.name.clone()),
        ),
        manifest,
        output_targets: Vec::new(),
        realityscan_image: request
            .realityscan_image
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| DesiredState::default().realityscan_image),
        pipeline: RealityScanPipeline {
            template_id: template.template_id,
            stages: template.stages,
            project_filename: template.project_filename,
            resume_source_job_id: template.resume_source_job_id,
            resume_project_filename: template.resume_project_filename,
            orthomosaic_filename: template.orthomosaic_filename,
            ortho_pixel_size_meters: template.ortho_pixel_size_meters,
            ortho_render_method: template.ortho_render_method,
            ortho_projection_params_xml: template.ortho_projection_params_xml,
            alignment_settings: template.alignment_settings,
            print_progress_interval_seconds: template.print_progress_interval_seconds,
        },
    };

    state
        .store
        .record_job_assignment(&request.client_id, job.clone())
        .await
        .map_err(ApiError::from_store)?;
    let queued_command = state
        .store
        .enqueue_command(
            &request.client_id,
            ServerCommand::AssignJob { job: job.clone() },
        )
        .await
        .map_err(ApiError::from_store)?
        .ok_or_else(|| ApiError::not_found("client not found"))?;
    publish_admin_snapshot(&state, "job_queued").await;

    Ok(Json(BuildJobResponse {
        dry_run,
        selected_assets: selected_summary,
        job: Some(job),
        queued_command: Some(queued_command),
        warnings,
    }))
}

fn job_templates() -> Vec<JobTemplate> {
    vec![
        JobTemplate {
            template_id: "align_preview_ortho".to_string(),
            name: "align / preview / ortho".to_string(),
            description:
                "Align images, reconstruct preview mesh, texture, calculate ortho projection, save project."
                    .to_string(),
            stages: vec![
                RealityScanStage::SetIntrinsics,
                RealityScanStage::Align,
                RealityScanStage::SelectMaximalComponent,
                RealityScanStage::SetReconstructionRegionAuto,
                RealityScanStage::CalculatePreviewModel,
                RealityScanStage::CalculateTexture,
                RealityScanStage::CalculateOrthoProjection,
                RealityScanStage::SaveProject,
            ],
            project_filename: "preview-ortho.rsproj".to_string(),
            resume_source_job_id: None,
            resume_project_filename: None,
            orthomosaic_filename: None,
            ortho_pixel_size_meters: None,
            ortho_render_method: None,
            ortho_projection_params_xml: None,
            alignment_settings: None,
            print_progress_interval_seconds: None,
        },
        JobTemplate {
            template_id: "align_normal_orthomosaic".to_string(),
            name: "align / normal / orthomosaic".to_string(),
            description:
                "Align images, run normal-quality reconstruction, texture, calculate and export orthomosaic."
                    .to_string(),
            stages: vec![
                RealityScanStage::SetIntrinsics,
                RealityScanStage::Align,
                RealityScanStage::SelectMaximalComponent,
                RealityScanStage::SetReconstructionRegionAuto,
                RealityScanStage::CalculateNormalModel,
                RealityScanStage::CalculateTexture,
                RealityScanStage::CalculateOrthoProjection,
                RealityScanStage::ExportOrthoProjection,
                RealityScanStage::SaveProject,
            ],
            project_filename: "normal-orthomosaic.rsproj".to_string(),
            resume_source_job_id: None,
            resume_project_filename: None,
            orthomosaic_filename: Some("orthomosaic.tif".to_string()),
            ortho_pixel_size_meters: None,
            ortho_render_method: None,
            ortho_projection_params_xml: None,
            alignment_settings: None,
            print_progress_interval_seconds: None,
        },
        JobTemplate {
            template_id: "align_only".to_string(),
            name: "align only".to_string(),
            description: "Align images, select largest component, and save the project.".to_string(),
            stages: vec![
                RealityScanStage::SetIntrinsics,
                RealityScanStage::Align,
                RealityScanStage::SelectMaximalComponent,
                RealityScanStage::SaveProject,
            ],
            project_filename: "aligned.rsproj".to_string(),
            resume_source_job_id: None,
            resume_project_filename: None,
            orthomosaic_filename: None,
            ortho_pixel_size_meters: None,
            ortho_render_method: None,
            ortho_projection_params_xml: None,
            alignment_settings: None,
            print_progress_interval_seconds: None,
        },
    ]
}

fn resolve_job_template(request: &BuildJobRequest) -> Result<JobTemplate, ApiError> {
    if let Some(template) = &request.custom_template {
        validate_custom_job_template(template)?;
        if template.template_id != request.template_id {
            return Err(ApiError::bad_request(
                "custom template id must match template_id",
            ));
        }
        return Ok(template.clone());
    }

    job_templates()
        .into_iter()
        .find(|template| template.template_id == request.template_id)
        .ok_or_else(|| ApiError::bad_request("unknown job template"))
}

fn validate_custom_job_template(template: &JobTemplate) -> Result<(), ApiError> {
    if template.template_id.trim().is_empty() {
        return Err(ApiError::bad_request("custom template_id cannot be empty"));
    }
    if template.name.trim().is_empty() {
        return Err(ApiError::bad_request(
            "custom template name cannot be empty",
        ));
    }
    if template.stages.is_empty() {
        return Err(ApiError::bad_request(
            "custom template must include at least one stage",
        ));
    }
    if template.project_filename.trim().is_empty() {
        return Err(ApiError::bad_request(
            "custom template project_filename cannot be empty",
        ));
    }
    if template
        .orthomosaic_filename
        .as_deref()
        .is_some_and(|value| value.trim().is_empty())
    {
        return Err(ApiError::bad_request(
            "custom template orthomosaic_filename cannot be empty",
        ));
    }
    if template
        .ortho_pixel_size_meters
        .is_some_and(|value| !value.is_finite() || value <= 0.0)
    {
        return Err(ApiError::bad_request(
            "custom template ortho_pixel_size_meters must be greater than zero",
        ));
    }
    Ok(())
}

async fn load_studio_assets(
    state: &AppState,
    refresh: bool,
) -> Result<StudioAssetSnapshot, ApiError> {
    let now_at = now();
    if !refresh {
        if let Some(snapshot) = state.studio_assets_cache.read().await.clone() {
            if now_at - snapshot.loaded_at <= state.studio_assets_cache_ttl {
                return Ok(snapshot);
            }
        }
    }

    let studio = state
        .studio
        .as_ref()
        .ok_or_else(|| ApiError::service_unavailable("Studio API client is not configured"))?;
    let stale_snapshot = state.studio_assets_cache.read().await.clone();
    match studio.list_image_assets().await {
        Ok(assets) => {
            let snapshot = StudioAssetSnapshot {
                loaded_at: now(),
                assets: Arc::new(assets),
            };
            *state.studio_assets_cache.write().await = Some(snapshot.clone());
            Ok(snapshot)
        }
        Err(error) => {
            if let Some(snapshot) = stale_snapshot {
                warn!(
                    %error,
                    cached_at = %snapshot.loaded_at,
                    "failed to refresh Studio image assets; serving stale cache"
                );
                Ok(snapshot)
            } else {
                warn!(%error, "failed to list image assets from Studio API");
                Err(ApiError::bad_gateway(
                    "failed to list image assets from Studio API",
                ))
            }
        }
    }
}

fn select_job_assets(
    assets: &[studio_api::StudioImageAsset],
    selection: &JobImageSelection,
) -> Result<Vec<studio_api::StudioImageAsset>, ApiError> {
    match selection {
        JobImageSelection::GroupName { group_name } => {
            let needle = group_name.trim();
            if needle.is_empty() {
                return Err(ApiError::bad_request("group_name cannot be empty"));
            }
            Ok(assets
                .iter()
                .filter(|asset| {
                    asset_group_name(asset).is_some_and(|value| value.eq_ignore_ascii_case(needle))
                })
                .cloned()
                .collect())
        }
        JobImageSelection::Polygon { coordinates } => {
            validate_polygon(coordinates)?;
            Ok(assets
                .iter()
                .filter(|asset| {
                    let (Some(latitude), Some(longitude)) = (asset.latitude, asset.longitude)
                    else {
                        return false;
                    };
                    point_in_polygon(longitude, latitude, coordinates)
                })
                .cloned()
                .collect())
        }
    }
}

fn selected_job_asset(asset: &studio_api::StudioImageAsset) -> SelectedJobAsset {
    SelectedJobAsset {
        asset_id: asset.asset_id.clone(),
        filename: asset.filename(),
        group_name: asset_group_name(asset).map(str::to_string),
        latitude: asset.latitude,
        longitude: asset.longitude,
        size_bytes: asset.size_bytes,
    }
}

fn asset_group_name(asset: &studio_api::StudioImageAsset) -> Option<&str> {
    asset
        .group_name
        .as_deref()
        .or(asset.image_group_name.as_deref())
        .filter(|value| !value.trim().is_empty())
}

fn asset_group_identity(asset: &studio_api::StudioImageAsset) -> ImageryGroupIdentity {
    if let Some(group_name) = asset_group_name(asset) {
        return ImageryGroupIdentity {
            key: format!("group_name:{group_name}"),
            label: group_name.to_string(),
            source: "group_name".to_string(),
            group_name: Some(group_name.to_string()),
        };
    }

    for candidate in [
        group_candidate(
            "image_group",
            asset.image_group_id.as_ref().and_then(json_scalar_text),
            asset
                .image_group_name
                .as_deref()
                .and_then(nonempty_trimmed)
                .map(str::to_string),
        ),
        group_candidate(
            "group",
            asset.group_id.as_ref().and_then(json_scalar_text),
            asset
                .group_name
                .as_deref()
                .and_then(nonempty_trimmed)
                .map(str::to_string),
        ),
        group_candidate(
            "imagery_source",
            asset.imagery_source_id.as_ref().and_then(json_scalar_text),
            asset
                .imagery_source_name
                .as_deref()
                .and_then(nonempty_trimmed)
                .map(str::to_string),
        ),
        group_candidate(
            "source",
            asset.source_id.as_ref().and_then(json_scalar_text),
            asset
                .source_name
                .as_deref()
                .and_then(nonempty_trimmed)
                .map(str::to_string),
        ),
        group_candidate(
            "source_version",
            asset.source_version_id.as_ref().and_then(json_scalar_text),
            asset
                .source_version_name
                .as_deref()
                .and_then(nonempty_trimmed)
                .map(str::to_string),
        ),
        group_candidate(
            "project",
            asset.project_id.as_ref().and_then(json_scalar_text),
            asset
                .project_name
                .as_deref()
                .and_then(nonempty_trimmed)
                .map(str::to_string),
        ),
        group_candidate(
            "batch",
            asset.batch_id.as_ref().and_then(json_scalar_text),
            asset
                .batch_name
                .as_deref()
                .and_then(nonempty_trimmed)
                .map(str::to_string),
        ),
        nested_group_candidate("image_group", asset.image_group.as_ref()),
    ]
    .into_iter()
    .flatten()
    {
        return candidate;
    }

    let capture_day = asset
        .captured_at
        .as_deref()
        .and_then(capture_date_key)
        .unwrap_or("unknown_date");
    let camera = asset_camera_label(asset).unwrap_or_else(|| "unknown camera".to_string());
    let account = asset
        .account_id
        .map(|account_id| format!("account {account_id}"))
        .unwrap_or_else(|| "unknown account".to_string());
    let label = if capture_day == "unknown_date" {
        format!("{account} / ungrouped")
    } else {
        format!("{capture_day} / {camera}")
    };

    ImageryGroupIdentity {
        key: format!("derived:{account}:{capture_day}:{camera}"),
        label,
        source: "derived".to_string(),
        group_name: None,
    }
}

fn group_candidate(
    prefix: &str,
    id: Option<String>,
    name: Option<String>,
) -> Option<ImageryGroupIdentity> {
    if id.is_none() && name.is_none() {
        return None;
    }
    let label = name.clone().unwrap_or_else(|| {
        format!(
            "{} {}",
            prefix.replace('_', " "),
            id.as_deref().unwrap_or("-")
        )
    });
    Some(ImageryGroupIdentity {
        key: format!("{}:{}", prefix, id.as_deref().unwrap_or(&label)),
        label,
        source: prefix.replace('_', " "),
        group_name: name,
    })
}

fn nested_group_candidate(
    prefix: &str,
    value: Option<&serde_json::Value>,
) -> Option<ImageryGroupIdentity> {
    let object = value?.as_object()?;
    let id = object
        .get("id")
        .and_then(json_scalar_text)
        .or_else(|| object.get("group_id").and_then(json_scalar_text));
    let name = ["name", "title", "label", "group_name"]
        .into_iter()
        .find_map(|key| object.get(key).and_then(json_scalar_text));
    group_candidate(prefix, id, name)
}

fn json_scalar_text(value: &serde_json::Value) -> Option<String> {
    match value {
        serde_json::Value::String(value) => nonempty_trimmed(value).map(str::to_string),
        serde_json::Value::Number(value) => Some(value.to_string()),
        _ => None,
    }
}

fn capture_date_key(value: &str) -> Option<&str> {
    value
        .get(0..10)
        .filter(|prefix| prefix.chars().filter(|character| *character == '-').count() == 2)
}

fn asset_has_location(asset: &studio_api::StudioImageAsset) -> bool {
    asset.latitude.is_some_and(f64::is_finite) && asset.longitude.is_some_and(f64::is_finite)
}

fn asset_in_bounds(asset: &studio_api::StudioImageAsset, bounds: AdminImageryBounds) -> bool {
    let (Some(latitude), Some(longitude)) = (asset.latitude, asset.longitude) else {
        return false;
    };
    latitude >= bounds.min_latitude
        && latitude <= bounds.max_latitude
        && longitude >= bounds.min_longitude
        && longitude <= bounds.max_longitude
}

fn asset_matches_group(asset: &studio_api::StudioImageAsset, needle: &str) -> bool {
    let needle = needle.trim();
    if needle.is_empty() {
        return true;
    }
    let identity = asset_group_identity(asset);
    asset_group_name(asset).is_some_and(|value| value.eq_ignore_ascii_case(needle))
        || identity.label.eq_ignore_ascii_case(needle)
        || identity.key.eq_ignore_ascii_case(needle)
}

fn asset_matches_search(asset: &studio_api::StudioImageAsset, search: &str) -> bool {
    [
        Some(asset.asset_id.as_str()),
        asset.filename.as_deref(),
        asset.cloudfront_path.as_deref(),
        asset.object_key.as_deref(),
        asset.uri.as_deref(),
        asset.camera_make.as_deref(),
        asset.camera_model.as_deref(),
        asset.drone_model.as_deref(),
    ]
    .into_iter()
    .flatten()
    .any(|value| value.to_lowercase().contains(search))
}

fn parse_bbox(value: &str) -> Result<AdminImageryBounds, ApiError> {
    let parts = value
        .split(',')
        .map(|part| part.trim().parse::<f64>())
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| ApiError::bad_request("bbox must be min_lon,min_lat,max_lon,max_lat"))?;
    let [min_longitude, min_latitude, max_longitude, max_latitude] = parts.as_slice() else {
        return Err(ApiError::bad_request(
            "bbox must be min_lon,min_lat,max_lon,max_lat",
        ));
    };
    if !min_longitude.is_finite()
        || !min_latitude.is_finite()
        || !max_longitude.is_finite()
        || !max_latitude.is_finite()
        || min_longitude > max_longitude
        || min_latitude > max_latitude
    {
        return Err(ApiError::bad_request(
            "bbox bounds must be finite and ordered min_lon,min_lat,max_lon,max_lat",
        ));
    }
    Ok(AdminImageryBounds {
        min_latitude: *min_latitude,
        max_latitude: *max_latitude,
        min_longitude: *min_longitude,
        max_longitude: *max_longitude,
    })
}

fn asset_camera_label(asset: &studio_api::StudioImageAsset) -> Option<String> {
    let make = asset.camera_make.as_deref().and_then(nonempty_trimmed);
    let model = asset.camera_model.as_deref().and_then(nonempty_trimmed);
    match (make, model) {
        (Some(make), Some(model)) => Some(format!("{make} {model}")),
        (Some(make), None) => Some(make.to_string()),
        (None, Some(model)) => Some(model.to_string()),
        (None, None) => None,
    }
}

impl ImageryGroupAccumulator {
    fn new(identity: ImageryGroupIdentity) -> Self {
        Self {
            identity,
            asset_count: 0,
            geocoded_count: 0,
            size_bytes: 0,
            captured_start: None,
            captured_end: None,
            cameras: BTreeSet::new(),
            bounds: None,
        }
    }

    fn add_asset(&mut self, asset: &studio_api::StudioImageAsset) {
        self.asset_count += 1;
        self.size_bytes = self
            .size_bytes
            .saturating_add(asset.size_bytes.unwrap_or(0));
        if let Some(captured_at) = asset.captured_at.as_deref().and_then(nonempty_trimmed) {
            if self
                .captured_start
                .as_ref()
                .is_none_or(|current| captured_at < current.as_str())
            {
                self.captured_start = Some(captured_at.to_string());
            }
            if self
                .captured_end
                .as_ref()
                .is_none_or(|current| captured_at > current.as_str())
            {
                self.captured_end = Some(captured_at.to_string());
            }
        }
        if let Some(camera) = asset_camera_label(asset) {
            self.cameras.insert(camera);
        }
        if let (Some(latitude), Some(longitude)) = (asset.latitude, asset.longitude) {
            if latitude.is_finite() && longitude.is_finite() {
                self.geocoded_count += 1;
                self.bounds = Some(match self.bounds {
                    Some(bounds) => AdminImageryBounds {
                        min_latitude: bounds.min_latitude.min(latitude),
                        max_latitude: bounds.max_latitude.max(latitude),
                        min_longitude: bounds.min_longitude.min(longitude),
                        max_longitude: bounds.max_longitude.max(longitude),
                    },
                    None => AdminImageryBounds {
                        min_latitude: latitude,
                        max_latitude: latitude,
                        min_longitude: longitude,
                        max_longitude: longitude,
                    },
                });
            }
        }
    }

    fn into_summary(self) -> AdminImageryGroupSummary {
        let camera_summary = match self.cameras.len() {
            0 => None,
            1 => self.cameras.iter().next().cloned(),
            count => Some(format!("{count} cameras")),
        };
        AdminImageryGroupSummary {
            key: self.identity.key,
            label: self.identity.label,
            source: self.identity.source,
            group_name: self.identity.group_name,
            asset_count: self.asset_count,
            geocoded_count: self.geocoded_count,
            size_bytes: (self.size_bytes > 0).then_some(self.size_bytes),
            captured_start: self.captured_start,
            captured_end: self.captured_end,
            camera_summary,
            bounds: self.bounds,
        }
    }
}

fn validate_polygon(coordinates: &[[f64; 2]]) -> Result<(), ApiError> {
    if coordinates.len() < 3 {
        return Err(ApiError::bad_request(
            "polygon selection requires at least 3 coordinates",
        ));
    }
    if coordinates
        .iter()
        .any(|point| !point[0].is_finite() || !point[1].is_finite())
    {
        return Err(ApiError::bad_request(
            "polygon coordinates must be finite lon/lat values",
        ));
    }
    Ok(())
}

fn point_in_polygon(longitude: f64, latitude: f64, polygon: &[[f64; 2]]) -> bool {
    let mut inside = false;
    let mut previous = polygon.len() - 1;
    for current in 0..polygon.len() {
        let [current_lon, current_lat] = polygon[current];
        let [previous_lon, previous_lat] = polygon[previous];
        let crosses = (current_lat > latitude) != (previous_lat > latitude);
        if crosses {
            let intersection_lon = (previous_lon - current_lon) * (latitude - current_lat)
                / (previous_lat - current_lat)
                + current_lon;
            if longitude < intersection_lon {
                inside = !inside;
            }
        }
        previous = current;
    }
    inside
}

async fn resolve_camera_intrinsics_for_assets(
    studio: &studio_api::StudioApiClient,
    assets: &mut [studio_api::StudioImageAsset],
) -> Vec<String> {
    let mut warnings = Vec::new();
    let mut cache: HashMap<String, Option<CameraIntrinsics>> = HashMap::new();
    for asset in assets {
        if asset.camera_intrinsics.is_some() {
            continue;
        }
        let Some(camera_name) = asset_camera_intrinsics_name(asset) else {
            continue;
        };
        if !cache.contains_key(&camera_name) {
            let intrinsics = match studio.get_camera_intrinsics(&camera_name).await {
                Ok(intrinsics) => intrinsics,
                Err(error) => {
                    warnings.push(format!(
                        "camera intrinsics lookup failed for {camera_name}: {error}"
                    ));
                    None
                }
            };
            cache.insert(camera_name.clone(), intrinsics);
        }
        if let Some(Some(intrinsics)) = cache.get(&camera_name) {
            asset.camera_intrinsics = Some(intrinsics.clone());
        }
    }
    warnings
}

fn asset_camera_intrinsics_name(asset: &studio_api::StudioImageAsset) -> Option<String> {
    let make = asset.camera_make.as_deref().and_then(nonempty_trimmed);
    let model = asset.camera_model.as_deref().and_then(nonempty_trimmed);
    match (make, model) {
        (Some(make), Some(model)) if model.to_lowercase().contains(&make.to_lowercase()) => {
            Some(model.to_string())
        }
        (Some(make), Some(model)) => Some(format!("{make} {model}")),
        (None, Some(model)) => Some(model.to_string()),
        (Some(make), None) => asset
            .drone_model
            .as_deref()
            .and_then(nonempty_trimmed)
            .map(|model| format!("{make} {model}")),
        (None, None) => None,
    }
}

fn nonempty_trimmed(value: &str) -> Option<&str> {
    let trimmed = value.trim();
    (!trimmed.is_empty()).then_some(trimmed)
}

async fn create_cloudfront_manifest(
    State(state): State<AppState>,
    Json(request): Json<CloudFrontManifestRequest>,
) -> Result<Json<JobInputManifest>, ApiError> {
    let signer = state
        .cloudfront
        .as_ref()
        .ok_or_else(|| ApiError::service_unavailable("CloudFront signer is not configured"))?;
    let expires_at = request
        .expires_at
        .unwrap_or_else(|| now() + ChronoDuration::seconds(request.ttl_seconds.unwrap_or(3600)));
    if expires_at <= now() {
        return Err(ApiError::bad_request(
            "manifest expiry must be in the future",
        ));
    }

    let mut inputs = Vec::with_capacity(request.inputs.len());
    for input in request.inputs {
        let resource_url =
            cloudfront::resource_url(&state.cloudfront_domain, &input.cloudfront_path);
        let url = signer
            .sign_url(&resource_url, expires_at)
            .map_err(|error| {
                error!(%error, "failed to sign CloudFront input URL");
                ApiError::internal("failed to sign CloudFront input URL")
            })?;
        inputs.push(CloudfrontInput {
            asset_id: input.asset_id,
            filename: input.filename,
            url,
            sha256: input.sha256,
            size_bytes: input.size_bytes,
            camera_intrinsics: input.camera_intrinsics,
        });
    }

    Ok(Json(JobInputManifest {
        job_id: request.job_id,
        expires_at,
        inputs,
    }))
}

async fn create_studio_manifest(
    State(state): State<AppState>,
    Json(request): Json<StudioManifestRequest>,
) -> Result<Json<JobInputManifest>, ApiError> {
    let studio = state
        .studio
        .as_ref()
        .ok_or_else(|| ApiError::service_unavailable("Studio API client is not configured"))?;
    let signer = state
        .cloudfront
        .as_ref()
        .ok_or_else(|| ApiError::service_unavailable("CloudFront signer is not configured"))?;
    let expires_at = request
        .expires_at
        .unwrap_or_else(|| now() + ChronoDuration::seconds(request.ttl_seconds.unwrap_or(3600)));
    if expires_at <= now() {
        return Err(ApiError::bad_request(
            "manifest expiry must be in the future",
        ));
    }

    let mut assets = Vec::with_capacity(request.asset_ids.len());
    for asset_id in &request.asset_ids {
        assets.push(studio.get_image_asset(asset_id).await.map_err(|error| {
            error!(asset_id, %error, "failed to resolve image asset from Studio API");
            ApiError::bad_gateway("failed to resolve image asset from Studio API")
        })?);
    }
    for warning in resolve_camera_intrinsics_for_assets(studio, &mut assets).await {
        warn!(%warning, "camera intrinsics lookup skipped for Studio manifest");
    }

    let manifest = manifest_from_studio_assets(
        &state.cloudfront_domain,
        signer,
        request.job_id,
        expires_at,
        assets,
    )?;
    Ok(Json(manifest))
}

fn manifest_from_studio_assets(
    cloudfront_domain: &str,
    signer: &cloudfront::CloudFrontUrlSigner,
    job_id: String,
    expires_at: chrono::DateTime<chrono::Utc>,
    assets: Vec<studio_api::StudioImageAsset>,
) -> Result<JobInputManifest, ApiError> {
    let mut inputs = Vec::with_capacity(assets.len());
    for asset in assets {
        let imagery_path = asset
            .imagery_path()
            .ok_or_else(|| ApiError::bad_gateway("Studio image asset missing object key"))?;
        let resource_url = cloudfront::resource_url(cloudfront_domain, imagery_path);
        let url = signer
            .sign_url(&resource_url, expires_at)
            .map_err(|error| {
                error!(%error, "failed to sign Studio image asset URL");
                ApiError::internal("failed to sign Studio image asset URL")
            })?;
        let filename = asset.filename();
        inputs.push(CloudfrontInput {
            asset_id: asset.asset_id,
            filename,
            url,
            sha256: asset.sha256,
            size_bytes: asset.size_bytes,
            camera_intrinsics: asset.camera_intrinsics,
        });
    }
    Ok(JobInputManifest {
        job_id,
        expires_at,
        inputs,
    })
}

async fn create_output_upload_targets(
    State(state): State<AppState>,
    Json(request): Json<OutputUploadTargetsRequest>,
) -> Result<Json<Vec<OutputUploadTarget>>, ApiError> {
    let presigner = state
        .s3_output
        .as_ref()
        .ok_or_else(|| ApiError::service_unavailable("S3 output presigner is not configured"))?;
    let ttl_seconds = request.ttl_seconds.unwrap_or(3600);
    if ttl_seconds == 0 || ttl_seconds > 604_800 {
        return Err(ApiError::bad_request(
            "ttl_seconds must be between 1 and 604800",
        ));
    }

    let mut targets = Vec::with_capacity(request.artifacts.len());
    for artifact in request.artifacts {
        validate_output_filename(&artifact.filename)?;
        let target = presigner
            .presign_put(
                &request.job_id,
                &artifact.artifact_id,
                &artifact.filename,
                artifact.content_type.as_deref(),
                StdDuration::from_secs(ttl_seconds),
            )
            .await
            .map_err(|error| {
                error!(%error, "failed to presign S3 output target");
                ApiError::internal("failed to presign S3 output target")
            })?;
        targets.push(target);
    }

    Ok(Json(targets))
}

fn validate_output_filename(filename: &str) -> Result<(), ApiError> {
    if filename.trim().is_empty()
        || filename == "."
        || filename == ".."
        || filename.contains('/')
        || filename.contains('\\')
    {
        return Err(ApiError::bad_request("invalid output filename"));
    }
    Ok(())
}

async fn client_websocket(
    State(state): State<AppState>,
    Path(client_id): Path<String>,
    Query(query): Query<WsAuthQuery>,
    ws: WebSocketUpgrade,
) -> Result<Response, ApiError> {
    let token = state
        .store
        .get_session(&query.token)
        .await
        .map_err(ApiError::from_store)?
        .ok_or_else(|| ApiError::unauthorized("session token not found"))?;
    if token.client_id != client_id || token.expires_at < now() {
        return Err(ApiError::unauthorized("session token invalid"));
    }
    active_client(&state, &client_id).await?;

    Ok(ws
        .on_upgrade(move |socket| websocket_loop(state, client_id, socket))
        .into_response())
}

async fn admin_events_websocket(State(state): State<AppState>, ws: WebSocketUpgrade) -> Response {
    ws.on_upgrade(move |socket| admin_events_loop(state, socket))
        .into_response()
}

async fn active_client(state: &AppState, client_id: &str) -> Result<ClientRecord, ApiError> {
    let client = state
        .store
        .get_client(client_id)
        .await
        .map_err(ApiError::from_store)?
        .ok_or_else(|| ApiError::not_found("client not found"))?;
    if client.revoked_at.is_some() {
        return Err(ApiError::unauthorized("client revoked"));
    }
    Ok(client)
}

async fn websocket_loop(state: AppState, client_id: String, socket: WebSocket) {
    let desired_state = match state.store.get_client(&client_id).await {
        Ok(Some(client)) => client.desired_state,
        Ok(None) => DesiredState::default(),
        Err(error) => {
            error!(client_id, %error, "failed to load desired state for websocket");
            DesiredState::default()
        }
    };
    let (mut sender, mut receiver) = socket.split();
    let hello = ServerCommand::DesiredStateUpdated { desired_state };
    let raw = match serde_json::to_string(&hello) {
        Ok(raw) => raw,
        Err(error) => {
            error!(%error, "failed to encode desired state");
            return;
        }
    };
    if sender.send(Message::Text(raw)).await.is_err() {
        return;
    }
    if !deliver_pending_commands(&state, &client_id, &mut sender).await {
        return;
    }

    info!(client_id, "client websocket connected");
    publish_admin_snapshot(&state, "client_connected").await;
    let mut command_poll = time::interval(StdDuration::from_secs(2));
    loop {
        tokio::select! {
            _ = command_poll.tick() => {
                if !deliver_pending_commands(&state, &client_id, &mut sender).await {
                    break;
                }
            }
            message = receiver.next() => {
                match message {
                    Some(Ok(Message::Text(raw))) => match serde_json::from_str::<ClientEvent>(&raw) {
                        Ok(event) => {
                            persist_client_event(&state, &client_id, &event).await;
                            debug!(client_id, ?event, "client event");
                        }
                        Err(error) => warn!(client_id, %error, raw, "invalid client event"),
                    },
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Ok(_)) => {}
                    Some(Err(error)) => {
                        warn!(client_id, %error, "websocket receive failed");
                        break;
                    }
                }
            }
        }
    }
    info!(client_id, "client websocket disconnected");
    publish_admin_snapshot(&state, "client_disconnected").await;
}

async fn persist_client_event(state: &AppState, client_id: &str, event: &ClientEvent) {
    let reason = admin_reason_for_client_event(event);
    let result = match event {
        ClientEvent::Heartbeat {
            observed_at,
            telemetry,
            ..
        } => {
            state
                .store
                .record_client_heartbeat(client_id, *observed_at, telemetry.clone())
                .await
        }
        ClientEvent::AgentStatus { status } => {
            state
                .store
                .record_agent_status(client_id, status.clone())
                .await
        }
        ClientEvent::WorkerStatus { status } => {
            state
                .store
                .record_worker_status(client_id, status.clone())
                .await
        }
        ClientEvent::JobEvent { event } => persist_job_event(state, client_id, event).await,
        ClientEvent::ArtifactUploaded { artifact } => {
            persist_uploaded_artifact(state, client_id, artifact).await
        }
        _ => Ok(()),
    };
    if let Err(error) = result {
        warn!(client_id, %error, "failed to persist client event");
    } else {
        publish_admin_snapshot(state, reason).await;
    }
}

fn admin_reason_for_client_event(event: &ClientEvent) -> &'static str {
    match event {
        ClientEvent::Hello { .. } => "client_hello",
        ClientEvent::Heartbeat { .. } => "heartbeat",
        ClientEvent::AgentStatus { .. } => "agent_status",
        ClientEvent::WorkerStatus { .. } => "worker_status",
        ClientEvent::JobEvent { .. } => "job_event",
        ClientEvent::LogChunk { .. } => "log_chunk",
        ClientEvent::ArtifactUploaded { .. } => "artifact_uploaded",
        ClientEvent::ErrorReport { .. } => "error_report",
    }
}

async fn admin_snapshot(
    state: &AppState,
    reason: impl Into<String>,
) -> Result<AdminStreamMessage, StoreError> {
    Ok(AdminStreamMessage::Snapshot {
        reason: reason.into(),
        observed_at: now(),
        clients: state.store.list_clients().await?,
        jobs: state.store.list_jobs().await?,
        job_events: state.store.list_job_events(None, 200).await?,
    })
}

async fn publish_admin_snapshot(state: &AppState, reason: &'static str) {
    match admin_snapshot(state, reason).await {
        Ok(message) => {
            let _ = state.admin_events.send(message);
        }
        Err(error) => warn!(%error, "failed to publish admin snapshot"),
    }
}

async fn admin_events_loop(state: AppState, socket: WebSocket) {
    let (mut sender, mut receiver) = socket.split();
    if let Ok(snapshot) = admin_snapshot(&state, "snapshot").await {
        if !send_admin_message(&mut sender, &snapshot).await {
            return;
        }
    }

    info!("admin websocket connected");
    let mut snapshots = state.admin_events.subscribe();
    let mut heartbeat = time::interval(StdDuration::from_secs(15));
    loop {
        tokio::select! {
            message = snapshots.recv() => {
                match message {
                    Ok(message) => {
                        if !send_admin_message(&mut sender, &message).await {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        match admin_snapshot(&state, "admin_stream_lagged").await {
                            Ok(snapshot) => {
                                if !send_admin_message(&mut sender, &snapshot).await {
                                    break;
                                }
                            }
                            Err(error) => warn!(%error, "failed to rebuild lagged admin snapshot"),
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
            _ = heartbeat.tick() => {
                match admin_snapshot(&state, "admin_heartbeat").await {
                    Ok(snapshot) => {
                        if !send_admin_message(&mut sender, &snapshot).await {
                            break;
                        }
                    }
                    Err(error) => warn!(%error, "failed to build admin heartbeat snapshot"),
                }
            }
            message = receiver.next() => {
                match message {
                    Some(Ok(Message::Text(raw))) if raw.trim().eq_ignore_ascii_case("refresh") => {
                        match admin_snapshot(&state, "admin_refresh").await {
                            Ok(snapshot) => {
                                if !send_admin_message(&mut sender, &snapshot).await {
                                    break;
                                }
                            }
                            Err(error) => warn!(%error, "failed to build requested admin snapshot"),
                        }
                    }
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Ok(_)) => {}
                    Some(Err(error)) => {
                        warn!(%error, "admin websocket receive failed");
                        break;
                    }
                }
            }
        }
    }
    info!("admin websocket disconnected");
}

async fn send_admin_message(
    sender: &mut futures_util::stream::SplitSink<WebSocket, Message>,
    message: &AdminStreamMessage,
) -> bool {
    let raw = match serde_json::to_string(message) {
        Ok(raw) => raw,
        Err(error) => {
            error!(%error, "failed to encode admin stream message");
            return true;
        }
    };
    sender.send(Message::Text(raw)).await.is_ok()
}

async fn persist_job_event(
    state: &AppState,
    client_id: &str,
    event: &rslogic_protocol::JobEvent,
) -> Result<(), StoreError> {
    state
        .store
        .record_job_event(client_id, event.clone())
        .await?;
    if let Some(studio) = state.studio.clone() {
        let client_id = client_id.to_string();
        let event = event.clone();
        tokio::spawn(async move {
            if let Err(error) = studio.record_job_event(&client_id, &event).await {
                warn!(
                    client_id,
                    job_id = %event.job_id,
                    state = ?event.state,
                    %error,
                    "failed to write job event to Studio API"
                );
            }
        });
    }
    Ok(())
}

async fn persist_uploaded_artifact(
    state: &AppState,
    client_id: &str,
    artifact: &UploadedArtifact,
) -> Result<(), StoreError> {
    state
        .store
        .record_uploaded_artifact(client_id, artifact.clone())
        .await?;
    if let Some(studio) = state.studio.clone() {
        let client_id = client_id.to_string();
        let artifact = artifact.clone();
        tokio::spawn(async move {
            if let Err(error) = studio.record_uploaded_artifact(&client_id, &artifact).await {
                warn!(
                    client_id,
                    job_id = %artifact.job_id,
                    artifact_id = %artifact.artifact_id,
                    %error,
                    "failed to write uploaded artifact to Studio API"
                );
            }
        });
    }
    Ok(())
}

async fn deliver_pending_commands(
    state: &AppState,
    client_id: &str,
    sender: &mut futures_util::stream::SplitSink<WebSocket, Message>,
) -> bool {
    let commands = match state.store.list_pending_commands(client_id, 50).await {
        Ok(commands) => commands,
        Err(error) => {
            error!(client_id, %error, "failed to load pending commands");
            return true;
        }
    };

    for queued in commands {
        let raw = match serde_json::to_string(&queued.command) {
            Ok(raw) => raw,
            Err(error) => {
                error!(
                    client_id,
                    command_id = queued.command_id,
                    %error,
                    "failed to encode queued command"
                );
                continue;
            }
        };
        if sender.send(Message::Text(raw)).await.is_err() {
            return false;
        }
        if let Err(error) = state.store.mark_command_delivered(&queued.command_id).await {
            error!(
                client_id,
                command_id = queued.command_id,
                %error,
                "failed to mark command delivered"
            );
        } else {
            info!(
                client_id,
                command_id = queued.command_id,
                "delivered queued command"
            );
        }
    }
    true
}

#[derive(Debug)]
struct ApiError {
    status: StatusCode,
    message: String,
}

impl ApiError {
    fn not_found(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            message: message.into(),
        }
    }

    fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: message.into(),
        }
    }

    fn unauthorized(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::UNAUTHORIZED,
            message: message.into(),
        }
    }

    fn internal(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: message.into(),
        }
    }

    fn bad_gateway(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_GATEWAY,
            message: message.into(),
        }
    }

    fn service_unavailable(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::SERVICE_UNAVAILABLE,
            message: message.into(),
        }
    }

    fn from_store(error: StoreError) -> Self {
        match error {
            StoreError::NotFound(message) => Self::not_found(message),
            StoreError::InvalidState(message) => Self::bad_request(message),
            other => {
                error!(%other, "storage operation failed");
                Self::internal("storage operation failed")
            }
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let body = Json(serde_json::json!({ "error": self.message }));
        (self.status, body).into_response()
    }
}

async fn cloudfront_signer_from_args(
    args: &Args,
) -> anyhow::Result<Option<Arc<cloudfront::CloudFrontUrlSigner>>> {
    let Some(key_pair_id) = args.cloudfront_key_pair_id.clone() else {
        if args.cloudfront_private_key_file.is_some() || args.cloudfront_private_key_pem.is_some() {
            warn!("CloudFront private key was supplied without RSLOGIC_CLOUDFRONT_KEY_PAIR_ID");
        }
        return Ok(None);
    };

    let private_key_pem = match (
        args.cloudfront_private_key_pem.as_ref(),
        args.cloudfront_private_key_file.as_ref(),
    ) {
        (Some(pem), _) => pem.clone(),
        (None, Some(path)) => tokio::fs::read_to_string(path)
            .await
            .with_context(|| format!("reading CloudFront private key {}", path.display()))?,
        (None, None) => {
            warn!("RSLOGIC_CLOUDFRONT_KEY_PAIR_ID is set but no private key was supplied");
            return Ok(None);
        }
    };

    let signer = cloudfront::CloudFrontUrlSigner::from_pem(key_pair_id, &private_key_pem)
        .context("loading CloudFront signer")?;
    info!(
        domain = args.cloudfront_domain,
        "configured CloudFront signed URL generator"
    );
    Ok(Some(Arc::new(signer)))
}

async fn s3_output_from_args(
    args: &Args,
) -> anyhow::Result<Option<Arc<s3_upload::S3OutputPresigner>>> {
    let Some(bucket) = args.output_bucket.clone() else {
        return Ok(None);
    };
    let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .load()
        .await;
    info!(
        bucket = %bucket,
        prefix = %args.output_prefix,
        "configured S3 presigned output target generator"
    );
    Ok(Some(Arc::new(s3_upload::S3OutputPresigner::new(
        aws_sdk_s3::Client::new(&config),
        bucket,
        args.output_prefix.clone(),
    ))))
}

fn studio_from_args(args: &Args) -> Option<Arc<studio_api::StudioApiClient>> {
    info!("configured Studio API image asset resolver and artifact writeback");
    Some(Arc::new(studio_api::StudioApiClient::new(
        args.studio_api_url.clone(),
        args.studio_api_token.clone(),
        args.studio_login_path.clone(),
        args.studio_api_email.clone(),
        args.studio_api_password.clone(),
        args.studio_image_assets_path.clone(),
        args.studio_asset_path_template.clone(),
        args.studio_camera_intrinsics_path_template.clone(),
        args.studio_job_event_path_template.clone(),
        args.studio_artifact_path_template.clone(),
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::{to_bytes, Body},
        http::{Method, Request},
    };
    use chrono::{TimeZone, Utc};
    use rsa::{
        pkcs8::{EncodePrivateKey, LineEnding},
        RsaPrivateKey,
    };
    use rslogic_protocol::{
        sign_challenge, AgentStatus, ClientKeypair, EnrollmentStatus, HardwareSummary, JobState,
        MachineTelemetry, WorkerProcessState,
    };
    use tower::ServiceExt;

    #[tokio::test]
    async fn enrollment_approval_and_signed_session_work() {
        let app = router(AppState::in_memory());
        let keypair = ClientKeypair::generate();
        let enrollment = test_enrollment(&keypair);

        let response = app
            .clone()
            .oneshot(json_request(
                Method::POST,
                "/api/client-enrollment/request",
                &enrollment,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let record: EnrollmentRequestRecord = response_json(response).await;
        assert_eq!(record.status, EnrollmentStatus::Pending);

        let response = app
            .clone()
            .oneshot(empty_request(
                Method::POST,
                &format!(
                    "/api/admin/client-enrollment/requests/{}/approve",
                    record.request_id
                ),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let approval: EnrollmentApproval = response_json(response).await;

        let response = app
            .clone()
            .oneshot(empty_request(
                Method::POST,
                &format!("/api/clients/{}/challenge", approval.client_id),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let challenge: Challenge = response_json(response).await;
        let signature = sign_challenge(&keypair.private_key, &challenge).unwrap();

        let response = app
            .clone()
            .oneshot(json_request(
                Method::POST,
                &format!("/api/clients/{}/session", approval.client_id),
                &SessionRequest {
                    challenge_id: challenge.challenge_id.clone(),
                    signature,
                },
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let session: SessionToken = response_json(response).await;
        assert_eq!(session.client_id, approval.client_id);

        let replay_response = app
            .oneshot(json_request(
                Method::POST,
                &format!("/api/clients/{}/session", approval.client_id),
                &SessionRequest {
                    challenge_id: challenge.challenge_id,
                    signature: "replay".to_string(),
                },
            ))
            .await
            .unwrap();
        assert_eq!(replay_response.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn job_asset_selection_filters_by_group_name() {
        let assets = vec![
            studio_api::StudioImageAsset {
                asset_id: "asset-1".to_string(),
                filename: Some("a.jpg".to_string()),
                group_name: Some("test-group-1".to_string()),
                ..studio_api::StudioImageAsset::default()
            },
            studio_api::StudioImageAsset {
                asset_id: "asset-2".to_string(),
                filename: Some("b.jpg".to_string()),
                group_name: Some("other".to_string()),
                ..studio_api::StudioImageAsset::default()
            },
        ];

        let selected = select_job_assets(
            &assets,
            &JobImageSelection::GroupName {
                group_name: "TEST-GROUP-1".to_string(),
            },
        )
        .unwrap();

        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].asset_id, "asset-1");
    }

    #[test]
    fn job_asset_selection_filters_by_polygon() {
        let assets = vec![
            studio_api::StudioImageAsset {
                asset_id: "inside".to_string(),
                latitude: Some(10.0),
                longitude: Some(10.0),
                ..studio_api::StudioImageAsset::default()
            },
            studio_api::StudioImageAsset {
                asset_id: "outside".to_string(),
                latitude: Some(30.0),
                longitude: Some(30.0),
                ..studio_api::StudioImageAsset::default()
            },
        ];

        let selected = select_job_assets(
            &assets,
            &JobImageSelection::Polygon {
                coordinates: vec![[0.0, 0.0], [20.0, 0.0], [20.0, 20.0], [0.0, 20.0]],
            },
        )
        .unwrap();

        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].asset_id, "inside");
    }

    #[tokio::test]
    async fn rejected_enrollment_cannot_be_approved() {
        let app = router(AppState::in_memory());
        let keypair = ClientKeypair::generate();
        let enrollment = test_enrollment(&keypair);

        let response = app
            .clone()
            .oneshot(json_request(
                Method::POST,
                "/api/client-enrollment/request",
                &enrollment,
            ))
            .await
            .unwrap();
        let record: EnrollmentRequestRecord = response_json(response).await;

        let response = app
            .clone()
            .oneshot(json_request(
                Method::POST,
                &format!(
                    "/api/admin/client-enrollment/requests/{}/reject",
                    record.request_id
                ),
                &EnrollmentRejection {
                    reason: Some("not this host".to_string()),
                },
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let response = app
            .oneshot(empty_request(
                Method::POST,
                &format!(
                    "/api/admin/client-enrollment/requests/{}/approve",
                    record.request_id
                ),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn command_can_be_queued_for_approved_client() {
        let app = router(AppState::in_memory());
        let keypair = ClientKeypair::generate();
        let approval = enroll_and_approve(&app, &keypair).await;

        let response = app
            .clone()
            .oneshot(json_request(
                Method::POST,
                &format!("/api/admin/clients/{}/commands", approval.client_id),
                &ServerCommand::StartWorker,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let queued: QueuedCommand = response_json(response).await;
        assert_eq!(queued.client_id, approval.client_id);
        assert!(matches!(queued.command, ServerCommand::StartWorker));
    }

    #[tokio::test]
    async fn admin_clients_endpoint_lists_approved_clients() {
        let app = router(AppState::in_memory());
        let keypair = ClientKeypair::generate();
        let approval = enroll_and_approve(&app, &keypair).await;

        let response = app
            .oneshot(empty_request(Method::GET, "/api/admin/clients"))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let clients: Vec<AdminClientRecord> = response_json(response).await;
        assert_eq!(clients.len(), 1);
        assert_eq!(clients[0].client_id, approval.client_id);
        assert_eq!(
            clients[0]
                .enrollment
                .as_ref()
                .map(|enrollment| enrollment.hardware.hostname.as_str()),
            Some("yassuh-1")
        );
        assert!(clients[0].desired_state.enabled);
    }

    #[tokio::test]
    async fn admin_clients_endpoint_includes_latest_runtime_telemetry() {
        let state = AppState::in_memory();
        let app = router(state.clone());
        let keypair = ClientKeypair::generate();
        let approval = enroll_and_approve(&app, &keypair).await;
        let telemetry = test_telemetry();
        state
            .store
            .record_agent_status(
                &approval.client_id,
                AgentStatus {
                    client_id: Some(approval.client_id.clone()),
                    agent_version: "0.1.0".to_string(),
                    connected: true,
                    worker_state: WorkerProcessState::Stopped,
                    telemetry: telemetry.clone(),
                },
            )
            .await
            .unwrap();

        let response = app
            .oneshot(empty_request(Method::GET, "/api/admin/clients"))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let clients: Vec<AdminClientRecord> = response_json(response).await;
        assert_eq!(
            clients[0]
                .latest_telemetry
                .as_ref()
                .and_then(|telemetry| telemetry.used_memory_bytes),
            telemetry.used_memory_bytes
        );
        assert_eq!(
            clients[0]
                .latest_status
                .as_ref()
                .map(|status| &status.worker_state),
            Some(&WorkerProcessState::Stopped)
        );
    }

    #[tokio::test]
    async fn admin_snapshot_includes_runtime_clients() {
        let state = AppState::in_memory();
        let app = router(state.clone());
        let keypair = ClientKeypair::generate();
        let approval = enroll_and_approve(&app, &keypair).await;

        state
            .store
            .record_agent_status(
                &approval.client_id,
                AgentStatus {
                    client_id: Some(approval.client_id.clone()),
                    agent_version: "0.1.0".to_string(),
                    connected: true,
                    worker_state: WorkerProcessState::Running,
                    telemetry: test_telemetry(),
                },
            )
            .await
            .unwrap();

        let snapshot = admin_snapshot(&state, "test").await.unwrap();
        let AdminStreamMessage::Snapshot {
            reason,
            clients,
            jobs,
            ..
        } = snapshot;
        assert_eq!(reason, "test");
        assert_eq!(clients.len(), 1);
        assert_eq!(clients[0].client_id, approval.client_id);
        assert_eq!(
            clients[0]
                .latest_telemetry
                .as_ref()
                .and_then(|telemetry| telemetry.cpu_count),
            Some(64)
        );
        assert!(jobs.is_empty());
    }

    #[tokio::test]
    async fn job_enqueue_records_assignment() {
        let app = router(AppState::in_memory());
        let keypair = ClientKeypair::generate();
        let approval = enroll_and_approve(&app, &keypair).await;
        let job = test_pipeline_job("job-1");

        let response = app
            .clone()
            .oneshot(json_request(
                Method::POST,
                &format!("/api/admin/clients/{}/jobs", approval.client_id),
                &job,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let response = app
            .oneshot(empty_request(Method::GET, "/api/admin/jobs"))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let jobs: Vec<JobRecord> = response_json(response).await;
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].job_id, "job-1");
        assert_eq!(jobs[0].client_id, approval.client_id);
        assert_eq!(jobs[0].state, JobState::Assigned);
    }

    #[tokio::test]
    async fn revoked_client_cannot_authenticate_or_receive_commands() {
        let app = router(AppState::in_memory());
        let keypair = ClientKeypair::generate();
        let approval = enroll_and_approve(&app, &keypair).await;

        let response = app
            .clone()
            .oneshot(empty_request(
                Method::POST,
                &format!("/api/admin/clients/{}/revoke", approval.client_id),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let revocation: ClientRevocation = response_json(response).await;
        assert_eq!(revocation.client_id, approval.client_id);

        let response = app
            .clone()
            .oneshot(empty_request(
                Method::POST,
                &format!("/api/clients/{}/challenge", approval.client_id),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

        let response = app
            .oneshot(json_request(
                Method::POST,
                &format!("/api/admin/clients/{}/commands", approval.client_id),
                &ServerCommand::StartWorker,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn cloudfront_manifest_endpoint_signs_inputs() {
        let app = router(AppState::new(
            Arc::new(InMemoryStore::default()),
            Some(Arc::new(test_cloudfront_signer())),
            None,
            None,
            "d15n2niw0v0y8k.cloudfront.net".to_string(),
            ChronoDuration::seconds(60),
        ));
        let request = CloudFrontManifestRequest {
            job_id: "job-1".to_string(),
            expires_at: Some(Utc.timestamp_opt(1_800_000_000, 0).unwrap()),
            ttl_seconds: None,
            inputs: vec![CloudFrontManifestInputRequest {
                asset_id: "asset-1".to_string(),
                filename: "image.jpg".to_string(),
                cloudfront_path: "/imagery/image.jpg".to_string(),
                sha256: Some("abc123".to_string()),
                size_bytes: Some(123),
                camera_intrinsics: None,
            }],
        };

        let response = app
            .oneshot(json_request(
                Method::POST,
                "/api/admin/jobs/cloudfront-manifest",
                &request,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let manifest: JobInputManifest = response_json(response).await;
        assert_eq!(manifest.job_id, "job-1");
        assert_eq!(manifest.inputs.len(), 1);
        assert!(manifest.inputs[0]
            .url
            .starts_with("https://d15n2niw0v0y8k.cloudfront.net/imagery/image.jpg?"));
        assert!(manifest.inputs[0].url.contains("Policy="));
        assert!(manifest.inputs[0].url.contains("Signature="));
        assert!(manifest.inputs[0].url.contains("Key-Pair-Id=KTEST"));
        assert!(manifest.inputs[0].url.contains("Hash-Algorithm=SHA256"));
    }

    #[tokio::test]
    async fn studio_assets_map_to_signed_manifest_inputs() {
        let signer = test_cloudfront_signer();
        let expires_at = Utc.timestamp_opt(1_800_000_000, 0).unwrap();
        let manifest = manifest_from_studio_assets(
            "d15n2niw0v0y8k.cloudfront.net",
            &signer,
            "job-1".to_string(),
            expires_at,
            vec![studio_api::StudioImageAsset {
                asset_id: "asset-1".to_string(),
                filename: None,
                cloudfront_path: Some("/imagery/nested/image.jpg".to_string()),
                sha256: Some("abc123".to_string()),
                size_bytes: Some(123),
                ..studio_api::StudioImageAsset::default()
            }],
        )
        .unwrap();

        assert_eq!(manifest.job_id, "job-1");
        assert_eq!(manifest.inputs.len(), 1);
        assert_eq!(manifest.inputs[0].asset_id, "asset-1");
        assert_eq!(manifest.inputs[0].filename, "image.jpg");
        assert_eq!(manifest.inputs[0].sha256.as_deref(), Some("abc123"));
        assert_eq!(manifest.inputs[0].size_bytes, Some(123));
        assert!(manifest.inputs[0]
            .url
            .starts_with("https://d15n2niw0v0y8k.cloudfront.net/imagery/nested/image.jpg?"));
        assert!(manifest.inputs[0].url.contains("Key-Pair-Id=KTEST"));
    }

    fn test_cloudfront_signer() -> cloudfront::CloudFrontUrlSigner {
        let mut rng = rand_core::OsRng;
        let private_key = RsaPrivateKey::new(&mut rng, 2048).unwrap();
        let pem = private_key.to_pkcs8_pem(LineEnding::LF).unwrap();
        cloudfront::CloudFrontUrlSigner::from_pem("KTEST", pem.as_str()).unwrap()
    }

    async fn enroll_and_approve(app: &Router, keypair: &ClientKeypair) -> EnrollmentApproval {
        let response = app
            .clone()
            .oneshot(json_request(
                Method::POST,
                "/api/client-enrollment/request",
                &test_enrollment(keypair),
            ))
            .await
            .unwrap();
        let record: EnrollmentRequestRecord = response_json(response).await;

        let response = app
            .clone()
            .oneshot(empty_request(
                Method::POST,
                &format!(
                    "/api/admin/client-enrollment/requests/{}/approve",
                    record.request_id
                ),
            ))
            .await
            .unwrap();
        response_json(response).await
    }

    fn test_enrollment(keypair: &ClientKeypair) -> EnrollmentRequest {
        EnrollmentRequest {
            hostname: "yassuh-1".to_string(),
            machine_id: "machine-1".to_string(),
            public_key: keypair.public_key.clone(),
            hardware: HardwareSummary {
                hostname: "yassuh-1".to_string(),
                machine_id: "machine-1".to_string(),
                os: "nixos".to_string(),
                arch: "x86_64".to_string(),
                cpu_count: Some(32),
                total_memory_bytes: None,
                gpu: Some("NVIDIA GeForce RTX 5090".to_string()),
                container_runtime: Some("docker".to_string()),
            },
            agent_version: "0.1.0".to_string(),
        }
    }

    fn test_telemetry() -> MachineTelemetry {
        MachineTelemetry {
            hostname: "yassuh-1".to_string(),
            cpu_count: Some(64),
            cpu_core_usage_percent: Some(vec![12.0, 34.0, 56.0, 78.0]),
            uptime_seconds: Some(123),
            load_average_1m: Some(0.5),
            load_average_5m: Some(0.4),
            load_average_15m: Some(0.3),
            total_memory_bytes: Some(100),
            available_memory_bytes: Some(40),
            used_memory_bytes: Some(60),
            total_disk_bytes: Some(1_000),
            free_disk_bytes: Some(500),
            gpu: Some("NVIDIA GeForce RTX 5090".to_string()),
            gpu_utilization_percent: Some(12.0),
            gpu_memory_total_bytes: Some(32 * 1024 * 1024),
            gpu_memory_used_bytes: Some(4 * 1024 * 1024),
            container_runtime: Some("docker".to_string()),
            observed_at: now(),
        }
    }

    fn test_pipeline_job(job_id: &str) -> PipelineJob {
        PipelineJob {
            job_id: job_id.to_string(),
            job_name: Some("test job".to_string()),
            manifest: JobInputManifest {
                job_id: job_id.to_string(),
                expires_at: Utc.timestamp_opt(1_800_000_000, 0).unwrap(),
                inputs: Vec::new(),
            },
            output_targets: Vec::new(),
            realityscan_image: "alpine:latest".to_string(),
            pipeline: RealityScanPipeline::default(),
        }
    }

    fn json_request<T: serde::Serialize>(method: Method, uri: &str, value: &T) -> Request<Body> {
        Request::builder()
            .method(method)
            .uri(uri)
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(value).unwrap()))
            .unwrap()
    }

    fn empty_request(method: Method, uri: &str) -> Request<Body> {
        Request::builder()
            .method(method)
            .uri(uri)
            .body(Body::empty())
            .unwrap()
    }

    async fn response_json<T: serde::de::DeserializeOwned>(response: Response) -> T {
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        serde_json::from_slice(&body).unwrap()
    }
}
