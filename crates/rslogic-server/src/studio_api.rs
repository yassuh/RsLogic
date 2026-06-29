use std::{collections::HashMap, sync::Arc};

use anyhow::Context;
use reqwest::{Client, StatusCode};
use rslogic_protocol::{CameraIntrinsics, JobEvent, UploadedArtifact};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

#[derive(Debug, Clone)]
pub struct StudioApiClient {
    http: Client,
    base_url: String,
    bearer_token: Option<String>,
    session_token: Arc<RwLock<Option<String>>>,
    login_path: String,
    login_email: Option<String>,
    login_password: Option<String>,
    image_assets_path: String,
    asset_path_template: String,
    camera_intrinsics_path_template: String,
    job_event_path_template: String,
    artifact_path_template: String,
}

impl StudioApiClient {
    pub fn new(
        base_url: impl Into<String>,
        bearer_token: Option<String>,
        login_path: impl Into<String>,
        login_email: Option<String>,
        login_password: Option<String>,
        image_assets_path: impl Into<String>,
        asset_path_template: impl Into<String>,
        camera_intrinsics_path_template: impl Into<String>,
        job_event_path_template: impl Into<String>,
        artifact_path_template: impl Into<String>,
    ) -> Self {
        Self {
            http: Client::new(),
            base_url: base_url.into().trim_end_matches('/').to_string(),
            bearer_token,
            session_token: Arc::new(RwLock::new(None)),
            login_path: login_path.into(),
            login_email,
            login_password,
            image_assets_path: image_assets_path.into(),
            asset_path_template: asset_path_template.into(),
            camera_intrinsics_path_template: camera_intrinsics_path_template.into(),
            job_event_path_template: job_event_path_template.into(),
            artifact_path_template: artifact_path_template.into(),
        }
    }

    pub async fn list_image_assets(&self) -> anyhow::Result<Vec<StudioImageAsset>> {
        let url = self.template_url(&self.image_assets_path, &[]);
        let request = self.with_auth(self.http.get(url)).await?;
        let response = request
            .send()
            .await
            .context("requesting Studio API image assets")?
            .error_for_status()
            .context("Studio API image assets request failed")?
            .json::<StudioImageAssetsResponse>()
            .await
            .context("decoding Studio API image assets response")?;
        Ok(response.image_assets)
    }

    pub async fn get_image_asset(&self, asset_id: &str) -> anyhow::Result<StudioImageAsset> {
        let url = self.template_url(&self.asset_path_template, &[("asset_id", asset_id)]);
        let request = self.with_auth(self.http.get(url)).await?;
        let asset = request
            .send()
            .await
            .context("requesting Studio API image asset")?
            .error_for_status()
            .context("Studio API image asset request failed")?
            .json::<StudioImageAsset>()
            .await
            .context("decoding Studio API image asset response")?;
        Ok(asset.with_fallback_asset_id(asset_id))
    }

    pub async fn get_camera_intrinsics(
        &self,
        camera_name: &str,
    ) -> anyhow::Result<Option<CameraIntrinsics>> {
        let url = self.template_url(
            &self.camera_intrinsics_path_template,
            &[("camera_name", camera_name)],
        );
        let request = self.with_auth(self.http.get(url)).await?;
        let response = request
            .send()
            .await
            .context("requesting Studio API camera intrinsics")?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        let value = response
            .error_for_status()
            .context("Studio API camera intrinsics request failed")?
            .json::<serde_json::Value>()
            .await
            .context("decoding Studio API camera intrinsics response")?;
        let row_value = value.get("camera_intrinsics").cloned().unwrap_or(value);
        let row: StudioCameraIntrinsicsRow = serde_json::from_value(row_value)
            .context("decoding Studio API camera intrinsics row")?;
        Ok(Some(row.into_camera_intrinsics()))
    }

    pub async fn record_job_event(&self, client_id: &str, event: &JobEvent) -> anyhow::Result<()> {
        let url = self.template_url(&self.job_event_path_template, &[("job_id", &event.job_id)]);
        let payload = StudioJobEvent {
            client_id: client_id.to_string(),
            job_id: event.job_id.clone(),
            state: event.state.clone(),
            message: event.message.clone(),
            progress: event.progress,
            observed_at: event.observed_at,
        };
        let request = self.with_auth(self.http.post(url).json(&payload)).await?;
        request
            .send()
            .await
            .context("posting Studio API job event")?
            .error_for_status()
            .context("Studio API job event request failed")?;
        Ok(())
    }

    pub async fn record_uploaded_artifact(
        &self,
        client_id: &str,
        artifact: &UploadedArtifact,
    ) -> anyhow::Result<()> {
        let url = self.template_url(
            &self.artifact_path_template,
            &[
                ("job_id", &artifact.job_id),
                ("artifact_id", &artifact.artifact_id),
            ],
        );
        let payload = StudioUploadedArtifact {
            client_id: client_id.to_string(),
            job_id: artifact.job_id.clone(),
            artifact_id: artifact.artifact_id.clone(),
            filename: artifact.filename.clone(),
            storage_uri: artifact.storage_uri.clone(),
            content_type: artifact.content_type.clone(),
            sha256: artifact.sha256.clone(),
            size_bytes: artifact.size_bytes,
        };
        let request = self.with_auth(self.http.post(url).json(&payload)).await?;
        request
            .send()
            .await
            .context("posting Studio API uploaded artifact")?
            .error_for_status()
            .context("Studio API uploaded artifact request failed")?;
        Ok(())
    }

    async fn with_auth(
        &self,
        request: reqwest::RequestBuilder,
    ) -> anyhow::Result<reqwest::RequestBuilder> {
        let Some(token) = self.auth_token().await? else {
            return Ok(request);
        };
        Ok(request.bearer_auth(token))
    }

    async fn auth_token(&self) -> anyhow::Result<Option<String>> {
        if let Some(token) = &self.bearer_token {
            return Ok(Some(token.clone()));
        }

        if let Some(token) = self.session_token.read().await.clone() {
            return Ok(Some(token));
        }

        let Some(email) = self.login_email.as_ref() else {
            return Ok(None);
        };
        let Some(password) = self.login_password.as_ref() else {
            return Ok(None);
        };

        let token = self.login(email, password).await?;
        *self.session_token.write().await = Some(token.clone());
        Ok(Some(token))
    }

    async fn login(&self, email: &str, password: &str) -> anyhow::Result<String> {
        let url = self.template_url(&self.login_path, &[]);
        let response = self
            .http
            .post(url)
            .json(&StudioLoginRequest { email, password })
            .send()
            .await
            .context("requesting Studio API login")?
            .error_for_status()
            .context("Studio API login failed")?
            .json::<StudioLoginResponse>()
            .await
            .context("decoding Studio API login response")?;
        Ok(response.token)
    }

    fn template_url(&self, template: &str, values: &[(&str, &str)]) -> String {
        let mut path = template.to_string();
        for (name, value) in values {
            path = path.replace(&format!("{{{name}}}"), &url_encode_path_segment(value));
        }
        if path.starts_with('/') {
            format!("{}{}", self.base_url, path)
        } else {
            format!("{}/{}", self.base_url, path)
        }
    }
}

#[derive(Debug, Serialize)]
struct StudioLoginRequest<'a> {
    email: &'a str,
    password: &'a str,
}

#[derive(Debug, Deserialize)]
struct StudioLoginResponse {
    #[serde(
        alias = "access_token",
        alias = "session_token",
        alias = "bearer_token"
    )]
    token: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StudioImageAssetsResponse {
    pub image_assets: Vec<StudioImageAsset>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct StudioCameraIntrinsicsRow {
    #[serde(default, alias = "cameraName", alias = "name")]
    pub camera_name: Option<String>,
    #[serde(default, alias = "calibrationPriorState", alias = "calibration_prior")]
    pub calibration_prior_state: Option<u8>,
    #[serde(default, alias = "lensPriorState", alias = "distortion_prior")]
    pub lens_prior_state: Option<u8>,
    #[serde(
        default,
        alias = "focalLength35mm",
        alias = "focal_length_35mm",
        alias = "focal_35mm"
    )]
    pub focal_length_35mm: Option<f64>,
    #[serde(
        default,
        alias = "principalPointXmm",
        alias = "principal_point_x_mm",
        alias = "principal_point_x",
        alias = "ppx"
    )]
    pub principal_point_x_mm: Option<f64>,
    #[serde(
        default,
        alias = "principalPointYmm",
        alias = "principal_point_y_mm",
        alias = "principal_point_y",
        alias = "ppy"
    )]
    pub principal_point_y_mm: Option<f64>,
    #[serde(default)]
    pub skew: Option<f64>,
    #[serde(default, alias = "aspectRatio")]
    pub aspect_ratio: Option<f64>,
    #[serde(default, alias = "distortionModel")]
    pub distortion_model: Option<u8>,
    #[serde(default, alias = "radialDistortion1", alias = "radial1", alias = "k1")]
    pub radial_distortion_1: Option<f64>,
    #[serde(default, alias = "radialDistortion2", alias = "radial2", alias = "k2")]
    pub radial_distortion_2: Option<f64>,
    #[serde(default, alias = "radialDistortion3", alias = "radial3", alias = "k3")]
    pub radial_distortion_3: Option<f64>,
    #[serde(default, alias = "radialDistortion4", alias = "radial4", alias = "k4")]
    pub radial_distortion_4: Option<f64>,
    #[serde(
        default,
        alias = "tangentialDistortion1",
        alias = "tangential1",
        alias = "p1"
    )]
    pub tangential_distortion_1: Option<f64>,
    #[serde(
        default,
        alias = "tangentialDistortion2",
        alias = "tangential2",
        alias = "p2"
    )]
    pub tangential_distortion_2: Option<f64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct StudioImageAsset {
    #[serde(default, alias = "id", alias = "assetId")]
    pub asset_id: String,
    #[serde(default, alias = "accountId")]
    pub account_id: Option<i64>,
    #[serde(default)]
    pub uri: Option<String>,
    #[serde(default, alias = "name", alias = "fileName", alias = "objectName")]
    pub filename: Option<String>,
    #[serde(
        default,
        alias = "cloudfrontPath",
        alias = "cloudfront_path",
        alias = "s3Key",
        alias = "s3_key",
        alias = "path",
        alias = "key"
    )]
    pub cloudfront_path: Option<String>,
    #[serde(default, alias = "objectKey")]
    pub object_key: Option<String>,
    #[serde(default, alias = "sha256Hex", alias = "sha256_hex", alias = "checksum")]
    pub sha256: Option<String>,
    #[serde(
        default,
        alias = "file_size",
        alias = "fileSize",
        alias = "sizeBytes",
        alias = "bytes"
    )]
    pub size_bytes: Option<u64>,
    #[serde(default, alias = "capturedAt")]
    pub captured_at: Option<String>,
    #[serde(default)]
    pub latitude: Option<f64>,
    #[serde(default)]
    pub longitude: Option<f64>,
    #[serde(default, alias = "imageWidth")]
    pub image_width: Option<i64>,
    #[serde(default, alias = "imageHeight")]
    pub image_height: Option<i64>,
    #[serde(default, alias = "bucketName")]
    pub bucket_name: Option<String>,
    #[serde(default, alias = "droneModel")]
    pub drone_model: Option<String>,
    #[serde(default, alias = "cameraMake")]
    pub camera_make: Option<String>,
    #[serde(default, alias = "cameraModel")]
    pub camera_model: Option<String>,
    #[serde(
        default,
        alias = "cameraIntrinsics",
        alias = "intrinsics",
        alias = "calibration",
        alias = "cameraCalibration"
    )]
    pub camera_intrinsics: Option<CameraIntrinsics>,
    #[serde(default, alias = "imageGroupId", alias = "image_group_uuid")]
    pub image_group_id: Option<serde_json::Value>,
    #[serde(default, alias = "imageGroupName")]
    pub image_group_name: Option<String>,
    #[serde(default, alias = "imageGroup")]
    pub image_group: Option<serde_json::Value>,
    #[serde(default, alias = "groupId")]
    pub group_id: Option<serde_json::Value>,
    #[serde(default, alias = "groupName")]
    pub group_name: Option<String>,
    #[serde(default, alias = "imagerySourceId")]
    pub imagery_source_id: Option<serde_json::Value>,
    #[serde(default, alias = "imagerySourceName")]
    pub imagery_source_name: Option<String>,
    #[serde(default, alias = "sourceId")]
    pub source_id: Option<serde_json::Value>,
    #[serde(default, alias = "sourceName")]
    pub source_name: Option<String>,
    #[serde(default, alias = "sourceVersionId")]
    pub source_version_id: Option<serde_json::Value>,
    #[serde(default, alias = "sourceVersionName")]
    pub source_version_name: Option<String>,
    #[serde(default, alias = "projectId")]
    pub project_id: Option<serde_json::Value>,
    #[serde(default, alias = "projectName")]
    pub project_name: Option<String>,
    #[serde(
        default,
        alias = "batchId",
        alias = "upload_batch_id",
        alias = "uploadBatchId"
    )]
    pub batch_id: Option<serde_json::Value>,
    #[serde(
        default,
        alias = "batchName",
        alias = "upload_batch_name",
        alias = "uploadBatchName"
    )]
    pub batch_name: Option<String>,
    #[serde(default)]
    pub metadata: Option<serde_json::Value>,
    #[serde(default, alias = "s3Tags", alias = "tags")]
    pub s3_tags: Option<serde_json::Value>,
    #[serde(default, flatten)]
    pub extra_fields: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StudioUploadedArtifact {
    pub client_id: String,
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
pub struct StudioJobEvent {
    pub client_id: String,
    pub job_id: String,
    pub state: rslogic_protocol::JobState,
    pub message: String,
    pub progress: f32,
    pub observed_at: chrono::DateTime<chrono::Utc>,
}

impl StudioImageAsset {
    fn with_fallback_asset_id(mut self, asset_id: &str) -> Self {
        if self.asset_id.trim().is_empty() {
            self.asset_id = asset_id.to_string();
        }
        self
    }

    pub fn filename(&self) -> String {
        self.filename
            .clone()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| filename_from_path(self.best_path().unwrap_or_default()))
    }

    pub fn imagery_path(&self) -> Option<&str> {
        self.cloudfront_path
            .as_deref()
            .or(self.object_key.as_deref())
            .filter(|value| !value.trim().is_empty())
    }

    fn best_path(&self) -> Option<&str> {
        self.imagery_path()
            .or(self.uri.as_deref())
            .filter(|value| !value.trim().is_empty())
    }
}

impl StudioCameraIntrinsicsRow {
    pub fn into_camera_intrinsics(self) -> CameraIntrinsics {
        let has_calibration = self.focal_length_35mm.is_some()
            || self.principal_point_x_mm.is_some()
            || self.principal_point_y_mm.is_some()
            || self.skew.is_some()
            || self.aspect_ratio.is_some();
        let has_distortion = self.distortion_model.is_some()
            || self.radial_distortion_1.is_some()
            || self.radial_distortion_2.is_some()
            || self.radial_distortion_3.is_some()
            || self.radial_distortion_4.is_some()
            || self.tangential_distortion_1.is_some()
            || self.tangential_distortion_2.is_some();
        let distortion_model = self
            .distortion_model
            .or_else(|| inferred_distortion_model(&self));
        CameraIntrinsics {
            camera_id: self.camera_name,
            calibration_group: None,
            calibration_prior: nonzero_prior_or_approximate(
                self.calibration_prior_state,
                has_calibration,
            ),
            focal_length_35mm: self.focal_length_35mm,
            principal_point_x_mm: self.principal_point_x_mm,
            principal_point_y_mm: self.principal_point_y_mm,
            skew: self.skew,
            aspect_ratio: self.aspect_ratio,
            lens_group: None,
            distortion_prior: nonzero_prior_or_approximate(self.lens_prior_state, has_distortion),
            distortion_model,
            radial_1: self.radial_distortion_1,
            radial_2: self.radial_distortion_2,
            radial_3: self.radial_distortion_3,
            radial_4: self.radial_distortion_4,
            tangential_1: self.tangential_distortion_1,
            tangential_2: self.tangential_distortion_2,
        }
    }
}

fn nonzero_prior_or_approximate(value: Option<u8>, has_values: bool) -> Option<u8> {
    value
        .filter(|prior| *prior != 0)
        .or_else(|| has_values.then_some(1))
}

fn inferred_distortion_model(row: &StudioCameraIntrinsicsRow) -> Option<u8> {
    let has_tangential =
        row.tangential_distortion_1.is_some() || row.tangential_distortion_2.is_some();
    let has_radial_4 = row.radial_distortion_4.is_some();
    let has_radial = row.radial_distortion_1.is_some()
        || row.radial_distortion_2.is_some()
        || row.radial_distortion_3.is_some()
        || has_radial_4;
    match (has_radial, has_radial_4, has_tangential) {
        (_, true, true) => Some(5),
        (_, false, true) => Some(4),
        (_, true, false) => Some(3),
        (true, false, false) => Some(2),
        _ => None,
    }
}

fn filename_from_path(path: &str) -> String {
    path.trim_end_matches('/')
        .rsplit('/')
        .next()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or("image")
        .to_string()
}

fn url_encode_path_segment(value: &str) -> String {
    let mut encoded = String::new();
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                encoded.push(byte as char);
            }
            other => encoded.push_str(&format!("%{other:02X}")),
        }
    }
    encoded
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn image_asset_accepts_expected_api_aliases() {
        let raw = serde_json::json!({
            "id": "asset-1",
            "fileName": "image.jpg",
            "s3Key": "imagery/image.jpg",
            "sha256Hex": "abc",
            "sizeBytes": 42
        });

        let asset: StudioImageAsset = serde_json::from_value(raw).unwrap();

        assert_eq!(asset.asset_id, "asset-1");
        assert_eq!(asset.filename(), "image.jpg");
        assert_eq!(asset.imagery_path(), Some("imagery/image.jpg"));
        assert_eq!(asset.sha256.as_deref(), Some("abc"));
        assert_eq!(asset.size_bytes, Some(42));
    }

    #[test]
    fn image_asset_derives_filename_from_path() {
        let asset = StudioImageAsset {
            asset_id: "asset-1".to_string(),
            filename: None,
            cloudfront_path: Some("/imagery/a/b/photo.tif".to_string()),
            ..StudioImageAsset::default()
        };

        assert_eq!(asset.filename(), "photo.tif");
    }

    #[test]
    fn image_asset_accepts_studio_cloud_schema() {
        let raw = serde_json::json!({
            "id": "asset-1",
            "account_id": 7,
            "uri": "s3://bucket/imagery/photo.jpg",
            "filename": "photo.jpg",
            "file_size": 1024,
            "captured_at": "2026-06-13T12:00:00Z",
            "latitude": 39.7392,
            "longitude": -104.9903,
            "image_width": 8192,
            "image_height": 5464,
            "bucket_name": "bucket",
            "object_key": "imagery/photo.jpg",
            "drone_model": "Mavic 3",
            "camera_make": "DJI",
            "camera_model": "FC3582",
            "camera_intrinsics": {
                "focal_length_35mm": 24.0,
                "principal_point_x_mm": 0.1,
                "principal_point_y_mm": -0.2,
                "distortion_model": 2,
                "radial_1": -0.01
            },
            "s3_tags": {
                "Aircraft Model": "DJI M4E"
            },
            "tag_aircraft_model": "DJI M4E"
        });

        let asset: StudioImageAsset = serde_json::from_value(raw).unwrap();

        assert_eq!(asset.account_id, Some(7));
        assert_eq!(asset.imagery_path(), Some("imagery/photo.jpg"));
        assert_eq!(asset.size_bytes, Some(1024));
        assert_eq!(asset.latitude, Some(39.7392));
        assert_eq!(asset.longitude, Some(-104.9903));
        assert_eq!(asset.image_width, Some(8192));
        assert_eq!(asset.image_height, Some(5464));
        assert_eq!(asset.drone_model.as_deref(), Some("Mavic 3"));
        assert_eq!(
            asset
                .camera_intrinsics
                .as_ref()
                .and_then(|intrinsics| intrinsics.focal_length_35mm),
            Some(24.0)
        );
        assert!(asset.captured_at.is_some());
        assert_eq!(
            asset
                .s3_tags
                .as_ref()
                .and_then(|tags| tags.get("Aircraft Model"))
                .and_then(serde_json::Value::as_str),
            Some("DJI M4E")
        );
        assert_eq!(
            asset
                .extra_fields
                .get("tag_aircraft_model")
                .and_then(serde_json::Value::as_str),
            Some("DJI M4E")
        );
    }

    #[test]
    fn template_url_encodes_path_values() {
        let studio = StudioApiClient::new(
            "https://studio.example.test/",
            None,
            "/api/v1/auth/login",
            None,
            None,
            "/api/v1/image-assets",
            "/api/image-assets/{asset_id}",
            "/api/v1/camera-intrinsics/{camera_name}",
            "/api/jobs/{job_id}/events",
            "/api/jobs/{job_id}/artifacts/{artifact_id}",
        );

        assert_eq!(
            studio.template_url(
                "api/jobs/{job_id}/artifacts/{artifact_id}",
                &[("job_id", "job 1"), ("artifact_id", "a/b")]
            ),
            "https://studio.example.test/api/jobs/job%201/artifacts/a%2Fb"
        );
    }

    #[test]
    fn camera_intrinsics_row_maps_dji_m4e_radial_distortion() {
        let raw = serde_json::json!({
            "camera_name": "DJI M4E",
            "calibration_prior_state": 0,
            "lens_prior_state": 0,
            "radial_distortion_1": -0.036013,
            "radial_distortion_2": -0.004848,
            "radial_distortion_3": 0.000481
        });

        let row: StudioCameraIntrinsicsRow = serde_json::from_value(raw).unwrap();
        let intrinsics = row.into_camera_intrinsics();

        assert_eq!(intrinsics.camera_id.as_deref(), Some("DJI M4E"));
        assert_eq!(intrinsics.distortion_prior, Some(1));
        assert_eq!(intrinsics.distortion_model, Some(2));
        assert_eq!(intrinsics.radial_1, Some(-0.036013));
        assert_eq!(intrinsics.radial_2, Some(-0.004848));
        assert_eq!(intrinsics.radial_3, Some(0.000481));
    }
}
