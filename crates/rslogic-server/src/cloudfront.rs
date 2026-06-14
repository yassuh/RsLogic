use base64::{engine::general_purpose::STANDARD, Engine as _};
use chrono::{DateTime, Utc};
use rsa::{
    pkcs1::DecodeRsaPrivateKey,
    pkcs1v15::SigningKey,
    pkcs8::DecodePrivateKey,
    signature::{SignatureEncoding, Signer},
    RsaPrivateKey,
};
use serde::Serialize;
use sha2::Sha256;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum CloudFrontSignError {
    #[error("invalid RSA private key PEM")]
    PrivateKey,
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

#[derive(Debug, Clone)]
pub struct CloudFrontUrlSigner {
    key_pair_id: String,
    private_key: RsaPrivateKey,
}

impl CloudFrontUrlSigner {
    pub fn from_pem(
        key_pair_id: impl Into<String>,
        private_key_pem: &str,
    ) -> Result<Self, CloudFrontSignError> {
        let private_key = RsaPrivateKey::from_pkcs8_pem(private_key_pem)
            .or_else(|_| RsaPrivateKey::from_pkcs1_pem(private_key_pem))
            .map_err(|_| CloudFrontSignError::PrivateKey)?;
        Ok(Self {
            key_pair_id: key_pair_id.into(),
            private_key,
        })
    }

    pub fn sign_url(
        &self,
        resource_url: &str,
        expires_at: DateTime<Utc>,
    ) -> Result<String, CloudFrontSignError> {
        let policy = CloudFrontPolicy {
            statement: vec![CloudFrontStatement {
                resource: resource_url,
                condition: CloudFrontCondition {
                    date_less_than: CloudFrontEpochTime {
                        epoch_time: expires_at.timestamp(),
                    },
                },
            }],
        };
        let policy_json = serde_json::to_string(&policy)?;
        let signing_key = SigningKey::<Sha256>::new(self.private_key.clone());
        let signature = signing_key.sign(policy_json.as_bytes());
        let separator = query_separator(resource_url);
        Ok(format!(
            "{resource_url}{separator}Policy={}&Signature={}&Key-Pair-Id={}&Hash-Algorithm=SHA256",
            cloudfront_base64(policy_json.as_bytes()),
            cloudfront_base64(&signature.to_vec()),
            self.key_pair_id,
        ))
    }
}

#[derive(Debug, Serialize)]
struct CloudFrontPolicy<'a> {
    #[serde(rename = "Statement")]
    statement: Vec<CloudFrontStatement<'a>>,
}

#[derive(Debug, Serialize)]
struct CloudFrontStatement<'a> {
    #[serde(rename = "Resource")]
    resource: &'a str,
    #[serde(rename = "Condition")]
    condition: CloudFrontCondition,
}

#[derive(Debug, Serialize)]
struct CloudFrontCondition {
    #[serde(rename = "DateLessThan")]
    date_less_than: CloudFrontEpochTime,
}

#[derive(Debug, Serialize)]
struct CloudFrontEpochTime {
    #[serde(rename = "AWS:EpochTime")]
    epoch_time: i64,
}

pub fn resource_url(domain: &str, path: &str) -> String {
    let domain = domain
        .trim()
        .trim_start_matches("https://")
        .trim_start_matches("http://")
        .trim_end_matches('/');
    let path = path.trim_start_matches('/');
    format!("https://{domain}/{path}")
}

fn query_separator(url: &str) -> &'static str {
    if url.contains('?') {
        if url.ends_with('?') || url.ends_with('&') {
            ""
        } else {
            "&"
        }
    } else {
        "?"
    }
}

fn cloudfront_base64(value: &[u8]) -> String {
    STANDARD
        .encode(value)
        .replace('+', "-")
        .replace('=', "_")
        .replace('/', "~")
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use rsa::pkcs8::{EncodePrivateKey, LineEnding};

    #[test]
    fn signed_url_uses_custom_policy_sha256_parameters() {
        let mut rng = rand_core::OsRng;
        let private_key = RsaPrivateKey::new(&mut rng, 2048).unwrap();
        let pem = private_key.to_pkcs8_pem(LineEnding::LF).unwrap();
        let signer = CloudFrontUrlSigner::from_pem("KTEST", pem.as_str()).unwrap();
        let expires_at = Utc.timestamp_opt(1_800_000_000, 0).unwrap();

        let signed = signer
            .sign_url(
                "https://d15n2niw0v0y8k.cloudfront.net/imagery/test.jpg",
                expires_at,
            )
            .unwrap();

        assert!(signed.contains("Policy="));
        assert!(signed.contains("Signature="));
        assert!(signed.contains("Key-Pair-Id=KTEST"));
        assert!(signed.contains("Hash-Algorithm=SHA256"));
        let query = signed.split_once('?').unwrap().1;
        assert!(!query.contains('+'));
        assert!(!query.contains('/'));
    }

    #[test]
    fn resource_url_normalizes_domain_and_path() {
        assert_eq!(
            resource_url("https://example.cloudfront.net/", "/imagery/a.jpg"),
            "https://example.cloudfront.net/imagery/a.jpg"
        );
    }
}
