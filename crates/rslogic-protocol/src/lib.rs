use base64::{engine::general_purpose::STANDARD, Engine as _};
use chrono::{DateTime, Utc};
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use rand_core::OsRng;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use uuid::Uuid;

pub const DEFAULT_MANAGEMENT_URL: &str = "http://127.0.0.1:8080";
pub const DEFAULT_WEBSOCKET_PATH: &str = "/api/clients/{client_id}/connect";
pub const DEFAULT_AGENT_STATE_DIR: &str = "/var/lib/rslogic-agent";
pub const DEFAULT_WORKER_STATE_DIR: &str = "/var/lib/rslogic-worker";
pub const PROTOCOL_VERSION: &str = "rslogic-v2";
pub const YASSUH_IMAGERY_CLOUDFRONT_DOMAIN: &str = "d15n2niw0v0y8k.cloudfront.net";
pub const YASSUH_IMAGERY_BUCKET: &str = "yassuh-imagery-749174759245-us-east-1";

#[derive(Debug, Error)]
pub enum CryptoError {
    #[error("invalid base64: {0}")]
    Base64(#[from] base64::DecodeError),
    #[error("invalid key material")]
    Key,
    #[error("invalid signature")]
    Signature,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClientKeypair {
    pub public_key: String,
    pub private_key: String,
}

impl ClientKeypair {
    pub fn generate() -> Self {
        let signing_key = SigningKey::generate(&mut OsRng);
        let verifying_key = signing_key.verifying_key();
        Self {
            public_key: STANDARD.encode(verifying_key.to_bytes()),
            private_key: STANDARD.encode(signing_key.to_bytes()),
        }
    }

    pub fn signing_key(&self) -> Result<SigningKey, CryptoError> {
        let bytes = STANDARD.decode(&self.private_key)?;
        let bytes: [u8; 32] = bytes.try_into().map_err(|_| CryptoError::Key)?;
        Ok(SigningKey::from_bytes(&bytes))
    }
}

pub fn sign_challenge(private_key_b64: &str, challenge: &Challenge) -> Result<String, CryptoError> {
    let bytes = STANDARD.decode(private_key_b64)?;
    let bytes: [u8; 32] = bytes.try_into().map_err(|_| CryptoError::Key)?;
    let key = SigningKey::from_bytes(&bytes);
    let signature = key.sign(challenge.signing_payload().as_bytes());
    Ok(STANDARD.encode(signature.to_bytes()))
}

pub fn verify_challenge_signature(
    public_key_b64: &str,
    challenge: &Challenge,
    signature_b64: &str,
) -> Result<(), CryptoError> {
    let public = STANDARD.decode(public_key_b64)?;
    let public: [u8; 32] = public.try_into().map_err(|_| CryptoError::Key)?;
    let verifying_key = VerifyingKey::from_bytes(&public).map_err(|_| CryptoError::Key)?;

    let signature = STANDARD.decode(signature_b64)?;
    let signature = Signature::from_slice(&signature).map_err(|_| CryptoError::Signature)?;
    verifying_key
        .verify(challenge.signing_payload().as_bytes(), &signature)
        .map_err(|_| CryptoError::Signature)
}

pub fn machine_id_from_material(material: &str) -> String {
    let digest = Sha256::digest(material.as_bytes());
    hex::encode(&digest[..16])
}

pub fn now() -> DateTime<Utc> {
    Utc::now()
}

pub fn new_id() -> String {
    Uuid::new_v4().to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EnrollmentStatus {
    Pending,
    Approved,
    Rejected,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct HardwareSummary {
    pub hostname: String,
    pub machine_id: String,
    pub os: String,
    pub arch: String,
    pub cpu_count: Option<u32>,
    pub total_memory_bytes: Option<u64>,
    pub gpu: Option<String>,
    pub container_runtime: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EnrollmentRequest {
    pub hostname: String,
    pub machine_id: String,
    pub public_key: String,
    pub hardware: HardwareSummary,
    pub agent_version: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EnrollmentRequestRecord {
    pub request_id: String,
    pub status: EnrollmentStatus,
    pub client_id: Option<String>,
    pub request: EnrollmentRequest,
    pub created_at: DateTime<Utc>,
    pub decided_at: Option<DateTime<Utc>>,
    pub rejection_reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EnrollmentApproval {
    pub request_id: String,
    pub client_id: String,
    pub approved_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EnrollmentRejection {
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Challenge {
    pub challenge_id: String,
    pub client_id: String,
    pub nonce: String,
    pub expires_at: DateTime<Utc>,
}

impl Challenge {
    pub fn signing_payload(&self) -> String {
        format!(
            "rslogic-auth-v1:{}:{}:{}",
            self.client_id, self.challenge_id, self.nonce
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SessionRequest {
    pub challenge_id: String,
    pub signature: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SessionToken {
    pub client_id: String,
    pub token: String,
    pub expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DesiredState {
    pub enabled: bool,
    pub accept_jobs: bool,
    pub agent_version: String,
    pub worker_version: String,
    pub realityscan_image: String,
    pub max_concurrent_jobs: u16,
}

impl Default for DesiredState {
    fn default() -> Self {
        Self {
            enabled: true,
            accept_jobs: true,
            agent_version: env!("CARGO_PKG_VERSION").to_string(),
            worker_version: env!("CARGO_PKG_VERSION").to_string(),
            realityscan_image: "yassuh/realityscan:local".to_string(),
            max_concurrent_jobs: 1,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MachineTelemetry {
    pub hostname: String,
    pub cpu_count: Option<u32>,
    #[serde(default)]
    pub cpu_core_usage_percent: Option<Vec<f64>>,
    pub uptime_seconds: Option<u64>,
    pub load_average_1m: Option<f64>,
    pub load_average_5m: Option<f64>,
    pub load_average_15m: Option<f64>,
    pub total_memory_bytes: Option<u64>,
    pub available_memory_bytes: Option<u64>,
    pub used_memory_bytes: Option<u64>,
    pub total_disk_bytes: Option<u64>,
    pub free_disk_bytes: Option<u64>,
    pub gpu: Option<String>,
    pub gpu_utilization_percent: Option<f64>,
    pub gpu_memory_total_bytes: Option<u64>,
    pub gpu_memory_used_bytes: Option<u64>,
    pub container_runtime: Option<String>,
    pub observed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WorkerProcessState {
    Unknown,
    Stopped,
    Starting,
    Running,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AgentStatus {
    pub client_id: Option<String>,
    pub agent_version: String,
    pub connected: bool,
    pub worker_state: WorkerProcessState,
    pub telemetry: MachineTelemetry,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WorkerStatus {
    pub worker_version: String,
    pub process_state: WorkerProcessState,
    pub active_job_id: Option<String>,
    #[serde(default)]
    pub supports_job_events_jsonl: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct CameraIntrinsics {
    #[serde(default, alias = "cameraId", alias = "camera_key", alias = "cameraKey")]
    pub camera_id: Option<String>,
    #[serde(default, alias = "calibrationGroup")]
    pub calibration_group: Option<i32>,
    #[serde(default, alias = "calibrationPrior")]
    pub calibration_prior: Option<u8>,
    #[serde(
        default,
        alias = "focalLength35mm",
        alias = "focal_length_35_mm",
        alias = "focal_35mm",
        alias = "focal35mm"
    )]
    pub focal_length_35mm: Option<f64>,
    #[serde(
        default,
        alias = "principalPointXmm",
        alias = "principal_point_x",
        alias = "ppx_mm",
        alias = "ppx"
    )]
    pub principal_point_x_mm: Option<f64>,
    #[serde(
        default,
        alias = "principalPointYmm",
        alias = "principal_point_y",
        alias = "ppy_mm",
        alias = "ppy"
    )]
    pub principal_point_y_mm: Option<f64>,
    #[serde(default)]
    pub skew: Option<f64>,
    #[serde(default, alias = "aspectRatio")]
    pub aspect_ratio: Option<f64>,
    #[serde(default, alias = "lensGroup")]
    pub lens_group: Option<i32>,
    #[serde(default, alias = "distortionPrior")]
    pub distortion_prior: Option<u8>,
    #[serde(default, alias = "distortionModel")]
    pub distortion_model: Option<u8>,
    #[serde(default, alias = "radial1", alias = "k1")]
    pub radial_1: Option<f64>,
    #[serde(default, alias = "radial2", alias = "k2")]
    pub radial_2: Option<f64>,
    #[serde(default, alias = "radial3", alias = "k3")]
    pub radial_3: Option<f64>,
    #[serde(default, alias = "radial4", alias = "k4")]
    pub radial_4: Option<f64>,
    #[serde(default, alias = "tangential1", alias = "p1")]
    pub tangential_1: Option<f64>,
    #[serde(default, alias = "tangential2", alias = "p2")]
    pub tangential_2: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CloudfrontInput {
    pub asset_id: String,
    pub filename: String,
    pub url: String,
    pub sha256: Option<String>,
    pub size_bytes: Option<u64>,
    #[serde(default, alias = "cameraIntrinsics", alias = "intrinsics")]
    pub camera_intrinsics: Option<CameraIntrinsics>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct JobInputManifest {
    pub job_id: String,
    pub expires_at: DateTime<Utc>,
    pub inputs: Vec<CloudfrontInput>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UploadHeader {
    pub name: String,
    pub value: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OutputUploadTarget {
    pub artifact_id: String,
    pub filename: String,
    pub method: String,
    pub url: String,
    #[serde(default)]
    pub storage_uri: Option<String>,
    pub content_type: Option<String>,
    #[serde(default)]
    pub headers: Vec<UploadHeader>,
    pub expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PipelineJob {
    pub job_id: String,
    pub job_name: Option<String>,
    pub manifest: JobInputManifest,
    pub output_targets: Vec<OutputUploadTarget>,
    pub realityscan_image: String,
    #[serde(default)]
    pub pipeline: RealityScanPipeline,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RealityScanPipeline {
    pub template_id: String,
    pub stages: Vec<RealityScanStage>,
    pub project_filename: String,
    #[serde(default)]
    pub orthomosaic_filename: Option<String>,
    #[serde(default)]
    pub ortho_pixel_size_meters: Option<f64>,
    #[serde(default)]
    pub ortho_render_method: Option<OrthoRenderMethod>,
    #[serde(default)]
    pub ortho_projection_params_xml: Option<String>,
    #[serde(default)]
    pub alignment_settings: Option<RealityScanAlignmentSettings>,
}

impl Default for RealityScanPipeline {
    fn default() -> Self {
        Self {
            template_id: "align_preview_ortho".to_string(),
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
            orthomosaic_filename: None,
            ortho_pixel_size_meters: None,
            ortho_render_method: None,
            ortho_projection_params_xml: None,
            alignment_settings: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct RealityScanAlignmentSettings {
    #[serde(default)]
    pub feature_detection_quality: Option<String>,
    #[serde(default)]
    pub max_features_per_mpx: Option<u32>,
    #[serde(default)]
    pub max_features_per_image: Option<u32>,
    #[serde(default)]
    pub images_overlap: Option<String>,
    #[serde(default)]
    pub image_downscale_factor: Option<u32>,
    #[serde(default)]
    pub max_feature_reprojection_error: Option<f64>,
    #[serde(default)]
    pub detector_sensitivity: Option<String>,
    #[serde(default)]
    pub preselector_features: Option<u32>,
    #[serde(default)]
    pub force_component_rematch: Option<bool>,
    #[serde(default)]
    pub merge_georeferenced_components: Option<bool>,
    #[serde(default)]
    pub enable_camera_prior: Option<bool>,
    #[serde(default)]
    pub camera_prior_accuracy_x: Option<f64>,
    #[serde(default)]
    pub camera_prior_accuracy_y: Option<f64>,
    #[serde(default)]
    pub camera_prior_accuracy_z: Option<f64>,
    #[serde(default)]
    pub camera_prior_weight: Option<f64>,
    #[serde(default)]
    pub camera_prior_accuracy_yaw: Option<f64>,
    #[serde(default)]
    pub camera_prior_accuracy_pitch: Option<f64>,
    #[serde(default)]
    pub camera_prior_accuracy_roll: Option<f64>,
    #[serde(default)]
    pub camera_prior_weight_orientation: Option<f64>,
    #[serde(default)]
    pub input_relative_pose: Option<u8>,
    #[serde(default)]
    pub input_absolute_pose: Option<u8>,
    #[serde(default)]
    pub input_prior_accuracy_source: Option<u8>,
    #[serde(default)]
    pub input_position_accuracy_x: Option<f64>,
    #[serde(default)]
    pub input_position_accuracy_y: Option<f64>,
    #[serde(default)]
    pub input_position_accuracy_z: Option<f64>,
    #[serde(default)]
    pub input_yaw_accuracy: Option<f64>,
    #[serde(default)]
    pub input_pitch_accuracy: Option<f64>,
    #[serde(default)]
    pub input_roll_accuracy: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OrthoRenderMethod {
    TrueOrthoTexturing,
    TrueOrthoColoring,
    ImageMosaicingGeneral,
    ImageMosaicingAerial,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RealityScanStage {
    SetIntrinsics,
    Align,
    SelectMaximalComponent,
    SetReconstructionRegionAuto,
    CalculatePreviewModel,
    CalculateNormalModel,
    CalculateHighModel,
    CalculateTexture,
    CalculateOrthoProjection,
    ExportOrthoProjection,
    SaveProject,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum JobState {
    Assigned,
    Accepted,
    ResolvingInputs,
    Downloading,
    Verifying,
    Staging,
    RunningRealityscan,
    CollectingOutputs,
    UploadingOutputs,
    Completed,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum JobEventKind {
    Lifecycle,
    RealityScanCommand,
    RealityScanStatus,
    RealityScanHeartbeat,
    RealityScanFatal,
    Artifact,
    Cache,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct JobEventDetails {
    pub kind: JobEventKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stage_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase_index: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase_count: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status_progress: Option<f32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runtime_seconds: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub eta_seconds: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub raw_status: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stdout_log_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stderr_log_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fatal_pattern: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct JobEvent {
    pub job_id: String,
    pub state: JobState,
    pub message: String,
    pub progress: f32,
    pub observed_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub details: Option<JobEventDetails>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UploadedArtifact {
    pub job_id: String,
    pub artifact_id: String,
    pub filename: String,
    #[serde(default)]
    pub storage_uri: Option<String>,
    #[serde(default)]
    pub content_type: Option<String>,
    pub sha256: Option<String>,
    pub size_bytes: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerCommand {
    DesiredStateUpdated { desired_state: DesiredState },
    StartWorker,
    StopWorker,
    RestartWorker,
    AssignJob { job: PipelineJob },
    CancelJob { job_id: String },
    RequestLogs { lines: u32 },
    RotateKey,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClientEvent {
    Hello {
        client_id: String,
        agent_version: String,
        hostname: String,
    },
    Heartbeat {
        client_id: String,
        observed_at: DateTime<Utc>,
        telemetry: MachineTelemetry,
    },
    AgentStatus {
        status: AgentStatus,
    },
    WorkerStatus {
        status: WorkerStatus,
    },
    JobEvent {
        event: JobEvent,
    },
    LogChunk {
        job_id: Option<String>,
        stream: String,
        lines: Vec<String>,
    },
    ArtifactUploaded {
        artifact: UploadedArtifact,
    },
    ErrorReport {
        client_id: Option<String>,
        message: String,
        recoverable: bool,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;

    #[test]
    fn keypair_can_sign_and_verify_challenge() {
        let keypair = ClientKeypair::generate();
        let challenge = Challenge {
            challenge_id: "challenge-1".to_string(),
            client_id: "client-1".to_string(),
            nonce: "nonce".to_string(),
            expires_at: now() + Duration::minutes(5),
        };

        let signature = sign_challenge(&keypair.private_key, &challenge).unwrap();

        verify_challenge_signature(&keypair.public_key, &challenge, &signature).unwrap();
    }

    #[test]
    fn websocket_messages_are_tagged() {
        let command = ServerCommand::StartWorker;
        let value = serde_json::to_value(command).unwrap();
        assert_eq!(value["type"], "start_worker");
    }
}
