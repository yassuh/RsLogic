use std::{
    path::{Path, PathBuf},
    process::Stdio,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{anyhow, Context};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::{
    fs,
    io::{AsyncBufReadExt, AsyncRead, AsyncWriteExt, BufReader},
    process::Command,
    sync::mpsc,
    task::JoinHandle,
    time::sleep,
};
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
    pub extra_mounts: Vec<ContainerBindMount>,
    #[serde(default)]
    pub log_prefix: Option<String>,
    #[serde(default)]
    pub max_runtime_secs: Option<u64>,
    #[serde(default)]
    pub liveness_check_interval_secs: Option<u64>,
    #[serde(default)]
    pub status_poll_interval_secs: Option<u64>,
    #[serde(default)]
    pub realityscan_instance_name: Option<String>,
    #[serde(default)]
    pub fatal_output_patterns: Vec<String>,
    #[serde(skip)]
    pub stdout_line_tx: Option<mpsc::UnboundedSender<String>>,
    #[serde(skip)]
    pub stderr_line_tx: Option<mpsc::UnboundedSender<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ContainerBindMount {
    pub host_path: PathBuf,
    pub container_path: String,
    #[serde(default)]
    pub read_only: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RealityScanRunResult {
    pub exit_code: i32,
    pub stdout_path: PathBuf,
    pub stderr_path: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RealityScanStatusSample {
    pub progress_id: String,
    pub progress_percent: f32,
    pub runtime_seconds: Option<f64>,
    pub eta_seconds: Option<f64>,
    pub raw_status: String,
}

#[derive(Debug)]
struct FatalOutputLine {
    stream: &'static str,
    pattern: String,
    line: String,
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

        remove_container(&config.runtime, &container_name)
            .await
            .ok();

        let mut cmd = Command::new(config.runtime.binary());
        cmd.arg("run").arg("--rm");
        for arg in runtime_run_limit_args(&config.runtime) {
            cmd.arg(arg);
        }
        cmd.arg("--name")
            .arg(&container_name)
            .arg("-v")
            .arg(format!("{}:/job", config.job_dir.display()))
            .arg("-w")
            .arg("/job");
        for mount in &config.extra_mounts {
            fs::create_dir_all(&mount.host_path)
                .await
                .with_context(|| format!("creating bind mount {}", mount.host_path.display()))?;
            cmd.arg("-v").arg(container_bind_mount_arg(mount));
        }
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
        cmd.stdout(Stdio::piped());
        cmd.stderr(Stdio::piped());

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
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| anyhow!("{} stdout was not piped", config.runtime.binary()))?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| anyhow!("{} stderr was not piped", config.runtime.binary()))?;
        let fatal_patterns = Arc::new(config.fatal_output_patterns.clone());
        let (fatal_line_tx, mut fatal_line_rx) = mpsc::unbounded_channel();
        let stdout_task = tokio::spawn(stream_to_log_file(
            "stdout",
            stdout,
            stdout_path.clone(),
            config.stdout_line_tx.clone(),
            fatal_line_tx.clone(),
            fatal_patterns.clone(),
        ));
        let stderr_task = tokio::spawn(stream_to_log_file(
            "stderr",
            stderr,
            stderr_path.clone(),
            config.stderr_line_tx.clone(),
            fatal_line_tx,
            fatal_patterns,
        ));
        let started_at = Instant::now();
        let check_interval =
            Duration::from_secs(config.liveness_check_interval_secs.unwrap_or(30).max(1));
        let max_runtime = config.max_runtime_secs.map(Duration::from_secs);
        let status_poll_interval = config
            .status_poll_interval_secs
            .map(|seconds| Duration::from_secs(seconds.max(1)));
        let mut next_status_poll = status_poll_interval.map(|interval| Instant::now() + interval);

        let status = loop {
            if let Some(status) = child
                .try_wait()
                .with_context(|| format!("polling {}", config.runtime.binary()))?
            {
                break status;
            }
            if let Ok(fatal) = fatal_line_rx.try_recv() {
                warn!(
                    container_name,
                    stream = fatal.stream,
                    pattern = fatal.pattern,
                    line = fatal.line,
                    "RealityScan emitted fatal output; removing container"
                );
                remove_container(&config.runtime, &container_name)
                    .await
                    .ok();
                child.wait().await.ok();
                await_log_tasks(stdout_task, stderr_task).await.ok();
                return Err(fatal_output_error(&fatal, &stdout_path, &stderr_path));
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
                    await_log_tasks(stdout_task, stderr_task).await.ok();
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
                    "RealityScan.exe became defunct; waiting for container process to exit"
                );
                sleep(Duration::from_secs(15)).await;
                if let Some(status) = child
                    .try_wait()
                    .with_context(|| format!("polling {}", config.runtime.binary()))?
                {
                    break status;
                }
                remove_container(&config.runtime, &container_name)
                    .await
                    .ok();
                child.wait().await.ok();
                await_log_tasks(stdout_task, stderr_task).await.ok();
                return Err(anyhow!(
                    "RealityScan.exe became defunct in container {container_name}; stdout={}; stderr={}",
                    stdout_path.display(),
                    stderr_path.display()
                ));
            }
            if let (Some(instance_name), Some(interval), Some(next_poll)) = (
                config.realityscan_instance_name.as_deref(),
                status_poll_interval,
                next_status_poll,
            ) {
                if Instant::now() >= next_poll {
                    if let Some(status_line) =
                        poll_realityscan_status(&config.runtime, &container_name, instance_name)
                            .await?
                    {
                        if let Some(line_tx) = &config.stdout_line_tx {
                            line_tx.send(status_line).ok();
                        }
                    }
                    next_status_poll = Some(Instant::now() + interval);
                }
            }
            sleep(check_interval).await;
        };

        let exit_code = status.code().unwrap_or(-1);
        await_log_tasks(stdout_task, stderr_task).await?;
        if let Ok(fatal) = fatal_line_rx.try_recv() {
            return Err(fatal_output_error(&fatal, &stdout_path, &stderr_path));
        }
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

async fn stream_to_log_file<R>(
    stream: &'static str,
    reader: R,
    path: PathBuf,
    line_tx: Option<mpsc::UnboundedSender<String>>,
    fatal_line_tx: mpsc::UnboundedSender<FatalOutputLine>,
    fatal_patterns: Arc<Vec<String>>,
) -> anyhow::Result<()>
where
    R: AsyncRead + Unpin,
{
    let mut file = fs::File::create(path).await?;
    let mut reader = BufReader::new(reader);
    let mut raw_line = Vec::new();
    loop {
        raw_line.clear();
        if reader.read_until(b'\n', &mut raw_line).await? == 0 {
            break;
        }
        file.write_all(&raw_line).await?;
        let line = String::from_utf8_lossy(&raw_line)
            .trim_end_matches(&['\r', '\n'][..])
            .to_string();
        if let Some(pattern) = matched_fatal_output_pattern(&line, &fatal_patterns) {
            fatal_line_tx
                .send(FatalOutputLine {
                    stream,
                    pattern,
                    line: line.clone(),
                })
                .ok();
        }
        if let Some(line_tx) = &line_tx {
            line_tx.send(line).ok();
        }
    }
    file.flush().await?;
    Ok(())
}

fn fatal_output_error(
    fatal: &FatalOutputLine,
    stdout_path: &Path,
    stderr_path: &Path,
) -> anyhow::Error {
    anyhow!(
        "RealityScan fatal output matched '{}' on {}: {}; stdout={}; stderr={}",
        fatal.pattern,
        fatal.stream,
        fatal.line,
        stdout_path.display(),
        stderr_path.display()
    )
}

fn matched_fatal_output_pattern(line: &str, patterns: &[String]) -> Option<String> {
    let line = line.to_ascii_lowercase();
    patterns
        .iter()
        .find(|pattern| {
            let pattern = pattern.trim();
            !pattern.is_empty() && line.contains(&pattern.to_ascii_lowercase())
        })
        .cloned()
}

pub fn parse_realityscan_status(raw: &str) -> Option<RealityScanStatusSample> {
    if let Some(sample) = parse_realityscan_print_progress_status(raw) {
        return Some(sample);
    }

    let mut progress_id = None;
    let mut progress_percent = None;
    let mut runtime_seconds = None;
    let mut eta_seconds = None;
    for token in raw.split_whitespace() {
        if let Some(value) = token.strip_prefix("id:") {
            progress_id = Some(value.to_string());
            continue;
        }
        if let Some(value) = token
            .strip_prefix("progress:")
            .and_then(|value| value.strip_suffix('%'))
        {
            progress_percent = value.parse::<f32>().ok();
            continue;
        }
        if let Some(value) = token
            .strip_prefix("runtime:")
            .and_then(|value| value.strip_suffix("sec"))
        {
            runtime_seconds = value.parse::<f64>().ok();
            continue;
        }
        if let Some(value) = token
            .strip_prefix("endEstimation:")
            .and_then(|value| value.strip_suffix("sec"))
        {
            eta_seconds = value.parse::<f64>().ok();
        }
    }
    Some(RealityScanStatusSample {
        progress_id: progress_id?,
        progress_percent: progress_percent?,
        runtime_seconds,
        eta_seconds,
        raw_status: raw.trim().to_string(),
    })
}

fn parse_realityscan_print_progress_status(raw: &str) -> Option<RealityScanStatusSample> {
    let mut tokens = raw.split_whitespace();
    let progress_id = tokens.next()?;
    let progress_value = tokens.next()?.parse::<f32>().ok()?;
    let runtime_seconds = tokens.next()?.parse::<f64>().ok();
    let eta_seconds = tokens.next()?.parse::<f64>().ok();
    let marker = tokens.next()?;
    if marker != "#progress" && marker != "#timeout" {
        return None;
    }

    let progress_percent = if progress_value <= 1.0 {
        progress_value * 100.0
    } else {
        progress_value
    };
    Some(RealityScanStatusSample {
        progress_id: progress_id.to_string(),
        progress_percent,
        runtime_seconds,
        eta_seconds,
        raw_status: raw.trim().to_string(),
    })
}

async fn await_log_tasks(
    stdout_task: JoinHandle<anyhow::Result<()>>,
    stderr_task: JoinHandle<anyhow::Result<()>>,
) -> anyhow::Result<()> {
    stdout_task
        .await
        .context("joining RealityScan stdout task")??;
    stderr_task
        .await
        .context("joining RealityScan stderr task")??;
    Ok(())
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

async fn poll_realityscan_status(
    runtime: &ContainerRuntime,
    container_name: &str,
    instance_name: &str,
) -> anyhow::Result<Option<String>> {
    let mut command = Command::new(runtime.binary());
    command
        .arg("exec")
        .arg(container_name)
        .arg("/opt/realityscan/bin/realityscan-cli")
        .arg("-headless")
        .arg("-silent")
        .arg("Z:\\job\\logs\\realityscan-crash-reports")
        .arg("-stdConsole")
        .arg("-getStatus")
        .arg(instance_name);
    let output = match tokio::time::timeout(Duration::from_secs(10), command.output()).await {
        Ok(output) => {
            output.with_context(|| format!("polling RealityScan status in {container_name}"))?
        }
        Err(_) => return Ok(None),
    };
    if !output.status.success() {
        return Ok(None);
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    Ok(stdout
        .lines()
        .map(str::trim)
        .find(|line| parse_realityscan_status(line).is_some())
        .map(ToString::to_string))
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

fn runtime_run_limit_args(runtime: &ContainerRuntime) -> &'static [&'static str] {
    match runtime {
        ContainerRuntime::Docker => &[],
        ContainerRuntime::Podman => &["--pids-limit=-1"],
    }
}

fn container_bind_mount_arg(mount: &ContainerBindMount) -> String {
    let access = if mount.read_only { ":ro" } else { "" };
    format!(
        "{}:{}{}",
        mount.host_path.display(),
        mount.container_path,
        access
    )
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

    #[test]
    fn podman_runs_without_pid_limit() {
        assert_eq!(
            runtime_run_limit_args(&ContainerRuntime::Podman),
            &["--pids-limit=-1"]
        );
        assert!(runtime_run_limit_args(&ContainerRuntime::Docker).is_empty());
    }

    #[test]
    fn formats_container_bind_mounts() {
        let mount = ContainerBindMount {
            host_path: PathBuf::from("/mnt/shared/cache/job-1"),
            container_path: "/root/.realityscan/realityscan".to_string(),
            read_only: false,
        };

        assert_eq!(
            container_bind_mount_arg(&mount),
            "/mnt/shared/cache/job-1:/root/.realityscan/realityscan"
        );
    }

    #[test]
    fn detects_fatal_realityscan_output_case_insensitively() {
        let patterns = vec![
            "processing failed:".to_string(),
            "operation failed.".to_string(),
        ];

        assert_eq!(
            matched_fatal_output_pattern("Processing failed: Operation failed.", &patterns)
                .as_deref(),
            Some("processing failed:")
        );
    }

    #[test]
    fn ignores_nonfatal_realityscan_output() {
        let patterns = vec!["processing failed:".to_string()];

        assert!(matched_fatal_output_pattern(
            "Feature detection completed in 120.000 seconds.",
            &patterns
        )
        .is_none());
    }

    #[tokio::test]
    async fn stream_to_log_file_handles_non_utf8_lines_lossily() {
        let path = std::env::temp_dir().join(format!(
            "rslogic-realityscan-stream-{}-{}.log",
            std::process::id(),
            std::thread::current().name().unwrap_or("test")
        ));
        let (mut writer, reader) = tokio::io::duplex(128);
        let (line_tx, mut line_rx) = mpsc::unbounded_channel();
        let (fatal_line_tx, mut fatal_line_rx) = mpsc::unbounded_channel();
        let fatal_patterns = Arc::new(vec!["fatal marker".to_string()]);
        let raw = b"progress \xff line\nfatal marker \xfe line\n";

        let task = tokio::spawn(stream_to_log_file(
            "stdout",
            reader,
            path.clone(),
            Some(line_tx),
            fatal_line_tx,
            fatal_patterns,
        ));
        writer.write_all(raw).await.unwrap();
        drop(writer);
        task.await.unwrap().unwrap();

        assert_eq!(fs::read(&path).await.unwrap(), raw);
        fs::remove_file(&path).await.ok();

        let first_line = line_rx.recv().await.unwrap();
        assert!(first_line.contains('\u{FFFD}'));
        let second_line = line_rx.recv().await.unwrap();
        assert!(second_line.contains("fatal marker"));

        let fatal = fatal_line_rx.recv().await.unwrap();
        assert_eq!(fatal.pattern, "fatal marker");
        assert!(fatal.line.contains('\u{FFFD}'));
    }

    #[test]
    fn parses_realityscan_get_status_output() {
        let sample = parse_realityscan_status(
            "id:0x10001 progress:57.5% runtime:4.26sec endEstimation:3.40sec",
        )
        .unwrap();

        assert_eq!(sample.progress_id, "0x10001");
        assert_eq!(sample.progress_percent, 57.5);
        assert_eq!(sample.runtime_seconds, Some(4.26));
        assert_eq!(sample.eta_seconds, Some(3.40));
    }

    #[test]
    fn parses_realityscan_print_progress_output() {
        let sample = parse_realityscan_status("65537 0.42 5005.49 6800.37 #progress").unwrap();

        assert_eq!(sample.progress_id, "65537");
        assert_eq!(sample.progress_percent, 42.0);
        assert_eq!(sample.runtime_seconds, Some(5005.49));
        assert_eq!(sample.eta_seconds, Some(6800.37));
    }

    #[test]
    fn parses_realityscan_print_timeout_output() {
        let sample = parse_realityscan_status("65537 0.42 4929.23 6767.07 #timeout").unwrap();

        assert_eq!(sample.progress_id, "65537");
        assert_eq!(sample.progress_percent, 42.0);
        assert_eq!(sample.runtime_seconds, Some(4929.23));
        assert_eq!(sample.eta_seconds, Some(6767.07));
    }
}
