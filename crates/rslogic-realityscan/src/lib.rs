use std::{
    path::{Path, PathBuf},
    process::Stdio,
};

use anyhow::{anyhow, Context};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::{fs, process::Command};
use tracing::info;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ContainerRuntime {
    Docker,
    Podman,
}

impl ContainerRuntime {
    pub fn binary(&self) -> &'static str {
        match self {
            Self::Docker => "docker",
            Self::Podman => "podman",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RealityScanRunConfig {
    pub runtime: ContainerRuntime,
    pub image: String,
    pub job_dir: PathBuf,
    pub command: Vec<String>,
    pub gpu: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RealityScanRunResult {
    pub exit_code: i32,
    pub stdout_path: PathBuf,
    pub stderr_path: PathBuf,
}

#[async_trait]
pub trait RealityScanRunner: Send + Sync {
    async fn run(&self, config: RealityScanRunConfig) -> anyhow::Result<RealityScanRunResult>;
}

#[derive(Debug, Default)]
pub struct ContainerRealityScanRunner;

#[async_trait]
impl RealityScanRunner for ContainerRealityScanRunner {
    async fn run(&self, config: RealityScanRunConfig) -> anyhow::Result<RealityScanRunResult> {
        fs::create_dir_all(config.job_dir.join("logs")).await?;
        fs::create_dir_all(config.job_dir.join("outputs")).await?;
        let stdout_path = config.job_dir.join("logs/realityscan.stdout.log");
        let stderr_path = config.job_dir.join("logs/realityscan.stderr.log");

        let stdout = std::fs::File::create(&stdout_path)?;
        let stderr = std::fs::File::create(&stderr_path)?;

        let mut cmd = Command::new(config.runtime.binary());
        cmd.arg("run")
            .arg("--rm")
            .arg("--name")
            .arg(format!(
                "rslogic-job-{}",
                safe_container_suffix(&config.job_dir)
            ))
            .arg("-v")
            .arg(format!("{}:/job", config.job_dir.display()))
            .arg("-w")
            .arg("/job");
        if config.gpu {
            cmd.arg("--device").arg("nvidia.com/gpu=all");
            if Path::new("/dev/dri").exists() {
                cmd.arg("--device").arg("/dev/dri");
            }
            cmd.arg("-e").arg("NVIDIA_DRIVER_CAPABILITIES=all");
            cmd.arg("-e")
                .arg("VK_ICD_FILENAMES=/run/opengl-driver/share/vulkan/icd.d/nvidia_icd.json");
            cmd.arg("-e").arg("LD_LIBRARY_PATH=/run/opengl-driver/lib");
            if Path::new("/run/opengl-driver").exists() {
                cmd.arg("-v")
                    .arg("/run/opengl-driver:/run/opengl-driver:ro");
            }
            if Path::new("/nix/store").exists() {
                cmd.arg("-v").arg("/nix/store:/nix/store:ro");
            }
        }
        cmd.arg("-e").arg("XDG_RUNTIME_DIR=/tmp/runtime-rslogic");
        cmd.arg("-e").arg("WINEDEBUG=-all");
        cmd.arg(&config.image);
        cmd.args(&config.command);
        cmd.stdout(Stdio::from(stdout));
        cmd.stderr(Stdio::from(stderr));

        info!(
            runtime = config.runtime.binary(),
            image = config.image,
            job_dir = %config.job_dir.display(),
            "starting RealityScan container"
        );
        let status = cmd
            .status()
            .await
            .with_context(|| format!("running {}", config.runtime.binary()))?;
        let exit_code = status.code().unwrap_or(-1);
        if !status.success() {
            return Err(anyhow!(
                "RealityScan container exited with status {exit_code}; stderr={}",
                stderr_path.display()
            ));
        }
        Ok(RealityScanRunResult {
            exit_code,
            stdout_path,
            stderr_path,
        })
    }
}

fn safe_container_suffix(path: &PathBuf) -> String {
    path.file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("unknown")
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric() || *ch == '-')
        .collect()
}
