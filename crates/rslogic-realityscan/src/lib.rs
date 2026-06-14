use std::{
    path::{Path, PathBuf},
    process::Stdio,
    time::{Duration, Instant},
};

use anyhow::{anyhow, Context};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::{fs, process::Command, time::sleep};
use tracing::{info, warn};

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
    #[serde(default)]
    pub log_prefix: Option<String>,
    #[serde(default)]
    pub max_runtime_secs: Option<u64>,
    #[serde(default)]
    pub liveness_check_interval_secs: Option<u64>,
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
        let log_prefix = config
            .log_prefix
            .as_deref()
            .map(safe_name_fragment)
            .unwrap_or_else(|| "realityscan".to_string());
        let stdout_path = config
            .job_dir
            .join("logs")
            .join(format!("{log_prefix}.stdout.log"));
        let stderr_path = config
            .job_dir
            .join("logs")
            .join(format!("{log_prefix}.stderr.log"));
        let container_name = container_name(&config.job_dir, config.log_prefix.as_deref());

        let stdout = std::fs::File::create(&stdout_path)?;
        let stderr = std::fs::File::create(&stderr_path)?;

        remove_container(&config.runtime, &container_name)
            .await
            .ok();

        let mut cmd = Command::new(config.runtime.binary());
        cmd.arg("run")
            .arg("--rm")
            .arg("--name")
            .arg(&container_name)
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
            container_name,
            log_prefix,
            "starting RealityScan container"
        );
        let mut child = cmd
            .spawn()
            .with_context(|| format!("starting {}", config.runtime.binary()))?;
        let started_at = Instant::now();
        let check_interval =
            Duration::from_secs(config.liveness_check_interval_secs.unwrap_or(30).max(1));
        let max_runtime = config.max_runtime_secs.map(Duration::from_secs);

        let status = loop {
            if let Some(status) = child
                .try_wait()
                .with_context(|| format!("polling {}", config.runtime.binary()))?
            {
                break status;
            }
            if let Some(max_runtime) = max_runtime {
                if started_at.elapsed() > max_runtime {
                    warn!(
                        container_name,
                        max_runtime_secs = max_runtime.as_secs(),
                        "RealityScan container exceeded runtime limit"
                    );
                    remove_container(&config.runtime, &container_name)
                        .await
                        .ok();
                    child.wait().await.ok();
                    return Err(anyhow!(
                        "RealityScan container exceeded runtime limit of {} seconds; stdout={}; stderr={}",
                        max_runtime.as_secs(),
                        stdout_path.display(),
                        stderr_path.display()
                    ));
                }
            }
            if container_has_defunct_realityscan(&config.runtime, &container_name).await? {
                warn!(
                    container_name,
                    "RealityScan.exe became defunct; removing container"
                );
                remove_container(&config.runtime, &container_name)
                    .await
                    .ok();
                child.wait().await.ok();
                return Err(anyhow!(
                    "RealityScan.exe became defunct in container {container_name}; stdout={}; stderr={}",
                    stdout_path.display(),
                    stderr_path.display()
                ));
            }
            sleep(check_interval).await;
        };

        let exit_code = status.code().unwrap_or(-1);
        if !status.success() {
            return Err(anyhow!(
                "RealityScan container exited with status {exit_code}; stdout={}; stderr={}",
                stdout_path.display(),
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

async fn container_has_defunct_realityscan(
    runtime: &ContainerRuntime,
    container_name: &str,
) -> anyhow::Result<bool> {
    let output = Command::new(runtime.binary())
        .arg("top")
        .arg(container_name)
        .arg("-eo")
        .arg("pid,stat,comm,args")
        .output()
        .await
        .with_context(|| format!("checking RealityScan process state in {container_name}"))?;
    if !output.status.success() {
        return Ok(false);
    }
    Ok(top_output_has_defunct_realityscan(
        &String::from_utf8_lossy(&output.stdout),
    ))
}

async fn remove_container(runtime: &ContainerRuntime, container_name: &str) -> anyhow::Result<()> {
    let output = Command::new(runtime.binary())
        .arg("rm")
        .arg("-f")
        .arg(container_name)
        .output()
        .await
        .with_context(|| format!("removing RealityScan container {container_name}"))?;
    if output.status.success() {
        return Ok(());
    }
    let stderr = String::from_utf8_lossy(&output.stderr);
    if stderr.contains("No such container") || stderr.contains("no such container") {
        return Ok(());
    }
    Err(anyhow!(
        "failed to remove RealityScan container {container_name}: {stderr}"
    ))
}

fn top_output_has_defunct_realityscan(output: &str) -> bool {
    output.lines().any(|line| {
        if !line.contains("RealityScan.exe") {
            return false;
        }
        if line.contains("<defunct>") {
            return true;
        }
        line.split_whitespace()
            .nth(1)
            .is_some_and(|stat| stat.starts_with('Z'))
    })
}

fn container_name(path: &PathBuf, phase: Option<&str>) -> String {
    match phase {
        Some(phase) => format!(
            "rslogic-job-{}-{}",
            safe_container_suffix(path),
            safe_name_fragment(phase)
        ),
        None => format!("rslogic-job-{}", safe_container_suffix(path)),
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

fn safe_name_fragment(value: &str) -> String {
    let cleaned: String = value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' {
                ch
            } else {
                '-'
            }
        })
        .collect();
    let cleaned = cleaned.trim_matches('-');
    if cleaned.is_empty() {
        "default".to_string()
    } else {
        cleaned.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_defunct_realityscan_from_docker_top() {
        let output = "\
PID STAT COMMAND COMMAND
508927 Sl winewrapper.exe /opt/realityscan/lib/wine/winewrapper.exe
509011 Zsl RealityScan.exe [RealityScan.exe] <defunct>
";

        assert!(top_output_has_defunct_realityscan(output));
    }

    #[test]
    fn ignores_live_realityscan_from_docker_top() {
        let output = "\
PID STAT COMMAND COMMAND
509011 Sl RealityScan.exe RealityScan.exe -headless
";

        assert!(!top_output_has_defunct_realityscan(output));
    }

    #[test]
    fn phase_container_name_is_stable_and_safe() {
        let path = PathBuf::from("/tmp/jobs/abc-123");

        assert_eq!(
            container_name(&path, Some("00 align/save")),
            "rslogic-job-abc-123-00-align-save"
        );
    }
}
