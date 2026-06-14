use std::time::Duration;

use anyhow::{anyhow, Context};
use aws_sdk_s3::{presigning::PresigningConfig, Client};
use chrono::Utc;
use rslogic_protocol::{OutputUploadTarget, UploadHeader};

#[derive(Debug, Clone)]
pub struct S3OutputPresigner {
    client: Client,
    bucket: String,
    prefix: String,
}

impl S3OutputPresigner {
    pub fn new(client: Client, bucket: impl Into<String>, prefix: impl Into<String>) -> Self {
        Self {
            client,
            bucket: bucket.into(),
            prefix: prefix.into(),
        }
    }

    pub async fn presign_put(
        &self,
        job_id: &str,
        artifact_id: &str,
        filename: &str,
        content_type: Option<&str>,
        expires_in: Duration,
    ) -> anyhow::Result<OutputUploadTarget> {
        let key = self.object_key(job_id, filename);
        let storage_uri = format!("s3://{}/{}", self.bucket, key);
        let mut request = self.client.put_object().bucket(&self.bucket).key(key);
        if let Some(content_type) = content_type {
            request = request.content_type(content_type);
        }
        let presigned = request
            .presigned(PresigningConfig::expires_in(expires_in)?)
            .await
            .context("presigning S3 PUT object request")?;
        let expires_at = Utc::now()
            + chrono::Duration::from_std(expires_in)
                .map_err(|_| anyhow!("presigned URL expiry is out of range"))?;
        let headers = presigned
            .headers()
            .filter_map(|(name, value)| {
                if name.eq_ignore_ascii_case("content-type") {
                    return None;
                }
                Some(UploadHeader {
                    name: name.to_string(),
                    value: value.to_string(),
                })
            })
            .collect();

        Ok(OutputUploadTarget {
            artifact_id: artifact_id.to_string(),
            filename: filename.to_string(),
            method: "PUT".to_string(),
            url: presigned.uri().to_string(),
            storage_uri: Some(storage_uri),
            content_type: content_type.map(str::to_string),
            headers,
            expires_at,
        })
    }

    fn object_key(&self, job_id: &str, filename: &str) -> String {
        let prefix = self.prefix.trim_matches('/');
        if prefix.is_empty() {
            format!("{job_id}/{filename}")
        } else {
            format!("{prefix}/{job_id}/{filename}")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_sdk_s3::{
        config::{BehaviorVersion, Credentials, Region},
        Client, Config,
    };

    #[tokio::test]
    async fn presign_put_returns_upload_target() {
        let config = Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new("us-east-1"))
            .credentials_provider(Credentials::new(
                "AKIATEST",
                "test-secret",
                None,
                None,
                "unit-test",
            ))
            .build();
        let presigner = S3OutputPresigner::new(Client::from_conf(config), "test-bucket", "outputs");

        let target = presigner
            .presign_put(
                "job-1",
                "artifact-1",
                "summary.txt",
                Some("text/plain"),
                Duration::from_secs(300),
            )
            .await
            .unwrap();

        assert_eq!(target.artifact_id, "artifact-1");
        assert_eq!(target.filename, "summary.txt");
        assert_eq!(target.method, "PUT");
        assert_eq!(
            target.storage_uri.as_deref(),
            Some("s3://test-bucket/outputs/job-1/summary.txt")
        );
        assert_eq!(target.content_type.as_deref(), Some("text/plain"));
        assert!(target.url.contains("X-Amz-Signature="));
        assert!(target.url.contains("outputs/job-1/summary.txt"));
    }
}
