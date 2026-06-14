use std::{
    fs,
    path::{Path, PathBuf},
    process::{Command as StdCommand, Stdio},
    sync::Arc,
    time::Duration,
};

use anyhow::{anyhow, Context};
use chrono::Utc;
use clap::Parser;
use futures_util::{SinkExt, StreamExt};
use rslogic_protocol::{
    machine_id_from_material, now, sign_challenge, AgentStatus, ClientEvent, ClientKeypair,
    DesiredState, EnrollmentRequest, EnrollmentRequestRecord, EnrollmentStatus, HardwareSummary,
    JobEvent, MachineTelemetry, PipelineJob, ServerCommand, SessionRequest, SessionToken,
    UploadedArtifact, WorkerProcessState, WorkerStatus, DEFAULT_AGENT_STATE_DIR,
    DEFAULT_MANAGEMENT_URL, DEFAULT_WORKER_STATE_DIR,
};
use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    process::Command as TokioCommand,
    sync::{mpsc, oneshot, Mutex},
    time,
};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{info, warn};
use tracing_subscriber::{fmt, EnvFilter};

#[derive(Debug, Parser)]
struct Args {
    #[arg(long, env = "RSLOGIC_MANAGEMENT_URL", default_value = DEFAULT_MANAGEMENT_URL)]
    management_url: String,
    #[arg(long, env = "RSLOGIC_AGENT_STATE_DIR", default_value = DEFAULT_AGENT_STATE_DIR)]
    state_dir: PathBuf,
    #[arg(long, env = "RSLOGIC_AGENT_HEARTBEAT_SECONDS", default_value_t = 10)]
    heartbeat_seconds: u64,
    #[arg(long, env = "RSLOGIC_WORKER_BIN", default_value = "rslogic-worker")]
    worker_bin: PathBuf,
    #[arg(long, env = "RSLOGIC_WORKER_STATE_DIR", default_value = DEFAULT_WORKER_STATE_DIR)]
    worker_state_dir: PathBuf,
    #[arg(long, env = "RSLOGIC_CONTAINER_RUNTIME", default_value = "docker")]
    container_runtime: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AgentDiskState {
    public_key: String,
    client_id: Option<String>,
    enrollment_request_id: Option<String>,
}

struct Agent {
    args: Args,
    http: reqwest::Client,
    private_key: String,
    disk_state: AgentDiskState,
}

struct ActiveWorker {
    job_id: String,
    cancel: oneshot::Sender<()>,
}

enum WorkerRunOutcome {
    Completed,
    Cancelled,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();
    let args = Args::parse();
    let mut agent = Agent::load_or_create(args).await?;
    agent.run().await
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    fmt().with_env_filter(filter).init();
}

impl Agent {
    async fn load_or_create(args: Args) -> anyhow::Result<Self> {
        tokio::fs::create_dir_all(&args.state_dir)
            .await
            .with_context(|| format!("creating {}", args.state_dir.display()))?;
        let identity_path = args.state_dir.join("identity.ed25519");
        let state_path = args.state_dir.join("client.json");

        let (private_key, disk_state) = if identity_path.is_file() && state_path.is_file() {
            let private_key = tokio::fs::read_to_string(&identity_path)
                .await
                .with_context(|| format!("reading {}", identity_path.display()))?
                .trim()
                .to_string();
            let raw_state = tokio::fs::read_to_string(&state_path)
                .await
                .with_context(|| format!("reading {}", state_path.display()))?;
            let disk_state = serde_json::from_str(&raw_state)?;
            (private_key, disk_state)
        } else {
            let keypair = ClientKeypair::generate();
            write_secret(&identity_path, &keypair.private_key)
                .await
                .with_context(|| format!("writing {}", identity_path.display()))?;
            let disk_state = AgentDiskState {
                public_key: keypair.public_key,
                client_id: None,
                enrollment_request_id: None,
            };
            write_json(&state_path, &disk_state).await?;
            (keypair.private_key, disk_state)
        };

        Ok(Self {
            args,
            http: reqwest::Client::new(),
            private_key,
            disk_state,
        })
    }

    async fn run(&mut self) -> anyhow::Result<()> {
        let client_id = self.ensure_approved_client().await?;
        let mut backoff = Duration::from_secs(1);
        loop {
            match self.connect_once(&client_id).await {
                Ok(()) => {
                    warn!(client_id, "management websocket disconnected");
                    backoff = Duration::from_secs(1);
                }
                Err(error) => {
                    warn!(client_id, %error, "management connection attempt failed");
                }
            }
            time::sleep(backoff).await;
            backoff = next_backoff(backoff);
        }
    }

    async fn connect_once(&self, client_id: &str) -> anyhow::Result<()> {
        let session = self.authenticate(client_id).await?;
        self.websocket_loop(client_id, &session).await
    }

    async fn ensure_approved_client(&mut self) -> anyhow::Result<String> {
        if let Some(client_id) = &self.disk_state.client_id {
            return Ok(client_id.clone());
        }

        if self.disk_state.enrollment_request_id.is_none() {
            let request = EnrollmentRequest {
                hostname: hostname(),
                machine_id: machine_id(),
                public_key: self.disk_state.public_key.clone(),
                hardware: hardware_summary(),
                agent_version: env!("CARGO_PKG_VERSION").to_string(),
            };
            let record: EnrollmentRequestRecord = self
                .http
                .post(format!(
                    "{}/api/client-enrollment/request",
                    self.args.management_url.trim_end_matches('/')
                ))
                .json(&request)
                .send()
                .await?
                .error_for_status()?
                .json()
                .await?;
            self.disk_state.enrollment_request_id = Some(record.request_id.clone());
            self.save_state().await?;
            info!(request_id = record.request_id, "created enrollment request");
        }

        loop {
            let request_id = self
                .disk_state
                .enrollment_request_id
                .clone()
                .ok_or_else(|| anyhow!("enrollment request id missing"))?;
            let record: EnrollmentRequestRecord = self
                .http
                .get(format!(
                    "{}/api/client-enrollment/request/{}",
                    self.args.management_url.trim_end_matches('/'),
                    request_id
                ))
                .send()
                .await?
                .error_for_status()?
                .json()
                .await?;
            match record.status {
                EnrollmentStatus::Approved => {
                    let client_id = record
                        .client_id
                        .ok_or_else(|| anyhow!("approved enrollment did not include client_id"))?;
                    self.disk_state.client_id = Some(client_id.clone());
                    self.save_state().await?;
                    info!(client_id, "enrollment approved");
                    return Ok(client_id);
                }
                EnrollmentStatus::Rejected => {
                    return Err(anyhow!(
                        "enrollment rejected: {}",
                        record
                            .rejection_reason
                            .unwrap_or_else(|| "no reason".to_string())
                    ));
                }
                EnrollmentStatus::Pending => {
                    info!(request_id, "waiting for enrollment approval");
                    time::sleep(Duration::from_secs(5)).await;
                }
            }
        }
    }

    async fn authenticate(&self, client_id: &str) -> anyhow::Result<SessionToken> {
        let base = self.args.management_url.trim_end_matches('/');
        let challenge: rslogic_protocol::Challenge = self
            .http
            .post(format!("{base}/api/clients/{client_id}/challenge"))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        let signature = sign_challenge(&self.private_key, &challenge)?;
        let session_request = SessionRequest {
            challenge_id: challenge.challenge_id,
            signature,
        };
        let session: SessionToken = self
            .http
            .post(format!("{base}/api/clients/{client_id}/session"))
            .json(&session_request)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        Ok(session)
    }

    async fn websocket_loop(&self, client_id: &str, session: &SessionToken) -> anyhow::Result<()> {
        let ws_url = websocket_url(&self.args.management_url, client_id, &session.token);
        let (socket, _) = connect_async(&ws_url).await?;
        let (mut writer, mut reader) = socket.split();
        let (outbound_tx, mut outbound_rx) = mpsc::channel::<ClientEvent>(128);
        let active_worker = Arc::new(Mutex::new(None));
        let desired_state = Arc::new(Mutex::new(DesiredState::default()));
        info!(client_id, ws_url, "connected to management websocket");

        let hello = ClientEvent::Hello {
            client_id: client_id.to_string(),
            agent_version: env!("CARGO_PKG_VERSION").to_string(),
            hostname: hostname(),
        };
        writer
            .send(Message::Text(serde_json::to_string(&hello)?.into()))
            .await?;

        let mut heartbeat = time::interval(Duration::from_secs(self.args.heartbeat_seconds));
        loop {
            tokio::select! {
                _ = heartbeat.tick() => {
                    let worker_state = if active_worker.lock().await.is_some() {
                        WorkerProcessState::Running
                    } else {
                        WorkerProcessState::Stopped
                    };
                    let telemetry = telemetry(&self.args.state_dir);
                    let event = ClientEvent::Heartbeat {
                        client_id: client_id.to_string(),
                        observed_at: now(),
                        telemetry: telemetry.clone(),
                    };
                    writer.send(Message::Text(serde_json::to_string(&event)?.into())).await?;

                    let status = ClientEvent::AgentStatus {
                        status: AgentStatus {
                            client_id: Some(client_id.to_string()),
                            agent_version: env!("CARGO_PKG_VERSION").to_string(),
                            connected: true,
                            worker_state,
                            telemetry,
                        },
                    };
                    writer.send(Message::Text(serde_json::to_string(&status)?.into())).await?;
                }
                message = reader.next() => {
                    match message {
                        Some(Ok(Message::Text(raw))) => {
                            match serde_json::from_str::<ServerCommand>(raw.as_ref()) {
                                Ok(command) => {
                                    self.handle_server_command(
                                        client_id,
                                        command,
                                        outbound_tx.clone(),
                                        active_worker.clone(),
                                        desired_state.clone(),
                                    ).await?;
                                }
                                Err(error) => warn!(raw = %raw, %error, "received invalid server command"),
                            }
                        }
                        Some(Ok(Message::Close(_))) | None => return Ok(()),
                        Some(Ok(_)) => {}
                        Some(Err(error)) => return Err(error.into()),
                    }
                }
                outbound = outbound_rx.recv() => {
                    if let Some(event) = outbound {
                        writer.send(Message::Text(serde_json::to_string(&event)?.into())).await?;
                    }
                }
            }
        }
    }

    async fn handle_server_command(
        &self,
        client_id: &str,
        command: ServerCommand,
        outbound: mpsc::Sender<ClientEvent>,
        active_worker: Arc<Mutex<Option<ActiveWorker>>>,
        desired_state: Arc<Mutex<DesiredState>>,
    ) -> anyhow::Result<()> {
        match command {
            ServerCommand::DesiredStateUpdated {
                desired_state: update,
            } => {
                info!(?update, "received desired state update");
                *desired_state.lock().await = update;
            }
            ServerCommand::AssignJob { job } => {
                let current_desired_state = desired_state.lock().await.clone();
                if let Some(message) = job_rejection_reason(&current_desired_state, &job.job_id) {
                    outbound
                        .send(ClientEvent::ErrorReport {
                            client_id: Some(client_id.to_string()),
                            message,
                            recoverable: true,
                        })
                        .await
                        .ok();
                    return Ok(());
                }
                let mut guard = active_worker.lock().await;
                if guard.is_some() {
                    outbound
                        .send(ClientEvent::ErrorReport {
                            client_id: Some(client_id.to_string()),
                            message: format!(
                                "cannot accept job {}; worker already busy",
                                job.job_id
                            ),
                            recoverable: true,
                        })
                        .await
                        .ok();
                    return Ok(());
                }
                let (cancel_tx, cancel_rx) = oneshot::channel();
                *guard = Some(ActiveWorker {
                    job_id: job.job_id.clone(),
                    cancel: cancel_tx,
                });
                drop(guard);
                spawn_worker_job(
                    client_id.to_string(),
                    job,
                    self.args.state_dir.clone(),
                    self.args.worker_bin.clone(),
                    self.args.worker_state_dir.clone(),
                    self.args.container_runtime.clone(),
                    outbound,
                    active_worker,
                    cancel_rx,
                );
            }
            ServerCommand::StartWorker => {
                info!("received start_worker command; worker starts when a job is assigned");
            }
            ServerCommand::StopWorker => {
                cancel_active_worker(client_id, None, active_worker, outbound).await;
            }
            ServerCommand::RestartWorker => {
                info!("received restart_worker command; next assigned job will use a fresh worker process");
            }
            ServerCommand::CancelJob { job_id } => {
                cancel_active_worker(client_id, Some(&job_id), active_worker, outbound).await;
            }
            ServerCommand::RequestLogs { lines } => {
                outbound
                    .send(ClientEvent::LogChunk {
                        job_id: None,
                        stream: "agent".to_string(),
                        lines: vec![format!("request_logs received for last {lines} lines; persistent log tailing is not implemented yet")],
                    })
                    .await
                    .ok();
            }
            ServerCommand::RotateKey => {
                outbound
                    .send(ClientEvent::ErrorReport {
                        client_id: Some(client_id.to_string()),
                        message:
                            "rotate_key requires a key-rotation protocol and is not implemented yet"
                                .to_string(),
                        recoverable: true,
                    })
                    .await
                    .ok();
            }
        }
        Ok(())
    }

    async fn save_state(&self) -> anyhow::Result<()> {
        write_json(&self.args.state_dir.join("client.json"), &self.disk_state).await
    }
}

fn job_rejection_reason(desired_state: &DesiredState, job_id: &str) -> Option<String> {
    if !desired_state.enabled {
        return Some(format!(
            "cannot accept job {job_id}; client is disabled by desired state"
        ));
    }
    if !desired_state.accept_jobs {
        return Some(format!(
            "cannot accept job {job_id}; desired state disables job acceptance"
        ));
    }
    if desired_state.max_concurrent_jobs == 0 {
        return Some(format!(
            "cannot accept job {job_id}; desired state max_concurrent_jobs is 0"
        ));
    }
    None
}

fn spawn_worker_job(
    client_id: String,
    job: PipelineJob,
    agent_state_dir: PathBuf,
    worker_bin: PathBuf,
    worker_state_dir: PathBuf,
    container_runtime: String,
    outbound: mpsc::Sender<ClientEvent>,
    active_worker: Arc<Mutex<Option<ActiveWorker>>>,
    cancel_rx: oneshot::Receiver<()>,
) {
    tokio::spawn(async move {
        let job_id = job.job_id.clone();
        let result = run_worker_job(
            job,
            agent_state_dir,
            worker_bin,
            worker_state_dir,
            container_runtime,
            outbound.clone(),
            cancel_rx,
        )
        .await;
        let process_state = match &result {
            Ok(WorkerRunOutcome::Completed) | Ok(WorkerRunOutcome::Cancelled) => {
                WorkerProcessState::Stopped
            }
            Err(_) => WorkerProcessState::Failed,
        };
        if let Err(error) = &result {
            outbound
                .send(ClientEvent::ErrorReport {
                    client_id: Some(client_id),
                    message: format!("worker job {job_id} failed: {error:#}"),
                    recoverable: true,
                })
                .await
                .ok();
        }
        outbound
            .send(ClientEvent::WorkerStatus {
                status: WorkerStatus {
                    worker_version: env!("CARGO_PKG_VERSION").to_string(),
                    process_state,
                    active_job_id: None,
                    supports_job_events_jsonl: true,
                },
            })
            .await
            .ok();
        let mut guard = active_worker.lock().await;
        if guard.as_ref().is_some_and(|active| active.job_id == job_id) {
            *guard = None;
        }
    });
}

async fn cancel_active_worker(
    client_id: &str,
    requested_job_id: Option<&str>,
    active_worker: Arc<Mutex<Option<ActiveWorker>>>,
    outbound: mpsc::Sender<ClientEvent>,
) {
    let cancel_request = {
        let mut guard = active_worker.lock().await;
        match guard.as_ref().map(|active| active.job_id.clone()) {
            None => Err("no active worker job to cancel".to_string()),
            Some(active_job_id) => {
                if let Some(job_id) = requested_job_id {
                    if active_job_id != job_id {
                        Err(format!(
                            "cannot cancel job {job_id}; active job is {}",
                            active_job_id
                        ))
                    } else {
                        Ok(guard.take().expect("active worker exists"))
                    }
                } else {
                    Ok(guard.take().expect("active worker exists"))
                }
            }
        }
    };
    let active = match cancel_request {
        Ok(active) => active,
        Err(message) => {
            outbound
                .send(ClientEvent::ErrorReport {
                    client_id: Some(client_id.to_string()),
                    message,
                    recoverable: true,
                })
                .await
                .ok();
            return;
        }
    };
    let cancelled_job_id = active.job_id.clone();
    if active.cancel.send(()).is_err() {
        outbound
            .send(ClientEvent::ErrorReport {
                client_id: Some(client_id.to_string()),
                message: format!("worker job {cancelled_job_id} already exited"),
                recoverable: true,
            })
            .await
            .ok();
    }
}

async fn run_worker_job(
    job: PipelineJob,
    agent_state_dir: PathBuf,
    worker_bin: PathBuf,
    worker_state_dir: PathBuf,
    container_runtime: String,
    outbound: mpsc::Sender<ClientEvent>,
    mut cancel_rx: oneshot::Receiver<()>,
) -> anyhow::Result<WorkerRunOutcome> {
    let job_dir = agent_state_dir.join("assigned-jobs");
    tokio::fs::create_dir_all(&job_dir).await?;
    let job_path = job_dir.join(format!("{}.json", job.job_id));
    write_json(&job_path, &job).await?;

    outbound
        .send(ClientEvent::WorkerStatus {
            status: WorkerStatus {
                worker_version: env!("CARGO_PKG_VERSION").to_string(),
                process_state: WorkerProcessState::Running,
                active_job_id: Some(job.job_id.clone()),
                supports_job_events_jsonl: true,
            },
        })
        .await
        .ok();

    let mut child = TokioCommand::new(&worker_bin)
        .arg("--state-dir")
        .arg(&worker_state_dir)
        .arg("--container-runtime")
        .arg(&container_runtime)
        .arg("run-job")
        .arg("--job")
        .arg(&job_path)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .with_context(|| format!("spawning worker {}", worker_bin.display()))?;

    let stdout_task = child.stdout.take().map(|stdout| {
        tokio::spawn(stream_worker_lines(
            job.job_id.clone(),
            "stdout",
            stdout,
            outbound.clone(),
        ))
    });
    let stderr_task = child.stderr.take().map(|stderr| {
        tokio::spawn(stream_worker_lines(
            job.job_id.clone(),
            "stderr",
            stderr,
            outbound.clone(),
        ))
    });

    let status = tokio::select! {
        status = child.wait() => status?,
        _ = &mut cancel_rx => {
            let _ = child.kill().await;
            let _ = child.wait().await;
            outbound
                .send(ClientEvent::JobEvent {
                    event: JobEvent {
                        job_id: job.job_id.clone(),
                        state: rslogic_protocol::JobState::Cancelled,
                        message: "worker process cancelled by management command".to_string(),
                        progress: 0.0,
                        observed_at: now(),
                        details: None,
                    },
                })
                .await
                .ok();
            if let Some(task) = stdout_task {
                let _ = task.await;
            }
            if let Some(task) = stderr_task {
                let _ = task.await;
            }
            return Ok(WorkerRunOutcome::Cancelled);
        }
    };
    if let Some(task) = stdout_task {
        let _ = task.await;
    }
    if let Some(task) = stderr_task {
        let _ = task.await;
    }
    if !status.success() {
        return Err(anyhow!("worker exited with status {status}"));
    }
    Ok(WorkerRunOutcome::Completed)
}

async fn stream_worker_lines<R>(
    job_id: String,
    stream: &'static str,
    reader: R,
    outbound: mpsc::Sender<ClientEvent>,
) -> anyhow::Result<()>
where
    R: tokio::io::AsyncRead + Unpin,
{
    let mut lines = BufReader::new(reader).lines();
    while let Some(line) = lines.next_line().await? {
        if stream == "stdout" {
            if let Some(event) = client_event_from_worker_stdout(&line) {
                outbound.send(event).await.ok();
                continue;
            }
        }
        outbound
            .send(ClientEvent::LogChunk {
                job_id: Some(job_id.clone()),
                stream: stream.to_string(),
                lines: vec![line],
            })
            .await
            .ok();
    }
    Ok(())
}

fn client_event_from_worker_stdout(line: &str) -> Option<ClientEvent> {
    if let Ok(event) = serde_json::from_str::<JobEvent>(line) {
        return Some(ClientEvent::JobEvent { event });
    }
    if let Ok(artifact) = serde_json::from_str::<UploadedArtifact>(line) {
        return Some(ClientEvent::ArtifactUploaded { artifact });
    }
    None
}

async fn write_json(path: &Path, value: &impl Serialize) -> anyhow::Result<()> {
    let data = serde_json::to_string_pretty(value)?;
    tokio::fs::write(path, data).await?;
    Ok(())
}

async fn write_secret(path: &Path, value: &str) -> anyhow::Result<()> {
    tokio::fs::write(path, value).await?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(path)?.permissions();
        perms.set_mode(0o600);
        fs::set_permissions(path, perms)?;
    }
    Ok(())
}

fn websocket_url(base: &str, client_id: &str, token: &str) -> String {
    let trimmed = base.trim_end_matches('/');
    let scheme = if trimmed.starts_with("https://") {
        "wss://"
    } else {
        "ws://"
    };
    let without_scheme = trimmed
        .trim_start_matches("https://")
        .trim_start_matches("http://");
    format!("{scheme}{without_scheme}/api/clients/{client_id}/connect?token={token}")
}

fn hardware_summary() -> HardwareSummary {
    HardwareSummary {
        hostname: hostname(),
        machine_id: machine_id(),
        os: std::env::consts::OS.to_string(),
        arch: std::env::consts::ARCH.to_string(),
        cpu_count: std::thread::available_parallelism()
            .ok()
            .and_then(|value| u32::try_from(value.get()).ok()),
        total_memory_bytes: memory_info().and_then(|memory| memory.total_bytes),
        gpu: gpu_name(),
        container_runtime: container_runtime(),
    }
}

fn telemetry(state_dir: &Path) -> MachineTelemetry {
    let load_average = load_average();
    let memory = memory_info();
    let disk = disk_info(state_dir);
    let gpu = gpu_info();
    MachineTelemetry {
        hostname: hostname(),
        cpu_count: std::thread::available_parallelism()
            .ok()
            .and_then(|value| u32::try_from(value.get()).ok()),
        uptime_seconds: uptime_seconds(),
        load_average_1m: load_average.map(|load| load.one_minute),
        load_average_5m: load_average.map(|load| load.five_minutes),
        load_average_15m: load_average.map(|load| load.fifteen_minutes),
        total_memory_bytes: memory.and_then(|memory| memory.total_bytes),
        available_memory_bytes: memory.and_then(|memory| memory.available_bytes),
        used_memory_bytes: memory.and_then(|memory| memory.used_bytes()),
        total_disk_bytes: disk.map(|disk| disk.total_bytes),
        free_disk_bytes: disk.map(|disk| disk.free_bytes),
        gpu: gpu.as_ref().and_then(|gpu| gpu.name.clone()),
        gpu_utilization_percent: gpu.as_ref().and_then(|gpu| gpu.utilization_percent),
        gpu_memory_total_bytes: gpu.as_ref().and_then(|gpu| gpu.memory_total_bytes),
        gpu_memory_used_bytes: gpu.as_ref().and_then(|gpu| gpu.memory_used_bytes),
        container_runtime: container_runtime(),
        observed_at: Utc::now(),
    }
}

fn hostname() -> String {
    std::env::var("HOSTNAME")
        .or_else(|_| std::env::var("COMPUTERNAME"))
        .ok()
        .filter(|value| !value.trim().is_empty())
        .or_else(hostname_command)
        .unwrap_or_else(|| "unknown-host".to_string())
}

fn machine_id() -> String {
    fs::read_to_string("/etc/machine-id")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| machine_id_from_material(&hostname()))
}

fn gpu_name() -> Option<String> {
    gpu_info().and_then(|gpu| gpu.name)
}

#[derive(Debug, Clone, Copy)]
struct LoadAverage {
    one_minute: f64,
    five_minutes: f64,
    fifteen_minutes: f64,
}

#[derive(Debug, Clone, Copy)]
struct MemoryInfo {
    total_bytes: Option<u64>,
    available_bytes: Option<u64>,
}

impl MemoryInfo {
    fn used_bytes(self) -> Option<u64> {
        Some(self.total_bytes?.saturating_sub(self.available_bytes?))
    }
}

#[derive(Debug, Clone, Copy)]
struct DiskInfo {
    total_bytes: u64,
    free_bytes: u64,
}

#[derive(Debug, Clone)]
struct GpuInfo {
    name: Option<String>,
    utilization_percent: Option<f64>,
    memory_total_bytes: Option<u64>,
    memory_used_bytes: Option<u64>,
}

fn uptime_seconds() -> Option<u64> {
    fs::read_to_string("/proc/uptime")
        .ok()?
        .split_whitespace()
        .next()?
        .parse::<f64>()
        .ok()
        .map(|value| value.max(0.0) as u64)
}

fn load_average() -> Option<LoadAverage> {
    let raw = fs::read_to_string("/proc/loadavg").ok()?;
    let mut parts = raw.split_whitespace();
    Some(LoadAverage {
        one_minute: parts.next()?.parse().ok()?,
        five_minutes: parts.next()?.parse().ok()?,
        fifteen_minutes: parts.next()?.parse().ok()?,
    })
}

fn memory_info() -> Option<MemoryInfo> {
    let raw = fs::read_to_string("/proc/meminfo").ok()?;
    let mut total_bytes = None;
    let mut available_bytes = None;
    for line in raw.lines() {
        if let Some(value) = meminfo_bytes(line, "MemTotal:") {
            total_bytes = Some(value);
        } else if let Some(value) = meminfo_bytes(line, "MemAvailable:") {
            available_bytes = Some(value);
        }
    }
    Some(MemoryInfo {
        total_bytes,
        available_bytes,
    })
}

fn meminfo_bytes(line: &str, key: &str) -> Option<u64> {
    let value = line.strip_prefix(key)?.split_whitespace().next()?;
    value.parse::<u64>().ok().map(|kib| kib * 1024)
}

fn disk_info(path: &Path) -> Option<DiskInfo> {
    let output = StdCommand::new("df")
        .args(["-B1", path.to_str()?])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let raw = String::from_utf8(output.stdout).ok()?;
    let data = raw.lines().nth(1)?;
    let parts: Vec<&str> = data.split_whitespace().collect();
    Some(DiskInfo {
        total_bytes: parts.get(1)?.parse().ok()?,
        free_bytes: parts.get(3)?.parse().ok()?,
    })
}

fn gpu_info() -> Option<GpuInfo> {
    command_stdout("nvidia-smi")?;
    let output = StdCommand::new("nvidia-smi")
        .args([
            "--query-gpu=name,utilization.gpu,memory.total,memory.used",
            "--format=csv,noheader,nounits",
        ])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }

    let raw = String::from_utf8(output.stdout).ok()?;
    let line = raw.lines().next()?.trim();
    let mut parts = line.split(',').map(str::trim);
    Some(GpuInfo {
        name: parts
            .next()
            .map(str::to_string)
            .filter(|value| !value.is_empty()),
        utilization_percent: parts.next().and_then(|value| value.parse::<f64>().ok()),
        memory_total_bytes: parts
            .next()
            .and_then(|value| value.parse::<u64>().ok())
            .map(mib_to_bytes),
        memory_used_bytes: parts
            .next()
            .and_then(|value| value.parse::<u64>().ok())
            .map(mib_to_bytes),
    })
}

fn mib_to_bytes(value: u64) -> u64 {
    value * 1024 * 1024
}

fn container_runtime() -> Option<String> {
    if command_stdout("podman").is_some() {
        return Some("podman".to_string());
    }
    if command_stdout("docker").is_some() {
        return Some("docker".to_string());
    }
    None
}

fn command_stdout(command: &str) -> Option<String> {
    let output = StdCommand::new(command).arg("--version").output().ok()?;
    if !output.status.success() {
        return None;
    }
    String::from_utf8(output.stdout)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn hostname_command() -> Option<String> {
    let output = StdCommand::new("hostname").output().ok()?;
    if !output.status.success() {
        return None;
    }
    String::from_utf8(output.stdout)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn next_backoff(current: Duration) -> Duration {
    Duration::from_secs((current.as_secs().max(1) * 2).min(60))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reconnect_backoff_caps_at_one_minute() {
        assert_eq!(next_backoff(Duration::from_secs(1)), Duration::from_secs(2));
        assert_eq!(
            next_backoff(Duration::from_secs(32)),
            Duration::from_secs(60)
        );
        assert_eq!(
            next_backoff(Duration::from_secs(60)),
            Duration::from_secs(60)
        );
    }

    #[test]
    fn worker_stdout_artifact_becomes_client_event() {
        let artifact = UploadedArtifact {
            job_id: "job-1".to_string(),
            artifact_id: "artifact-1".to_string(),
            filename: "summary.txt".to_string(),
            storage_uri: Some("s3://bucket/job-1/summary.txt".to_string()),
            content_type: Some("text/plain".to_string()),
            sha256: Some("abc123".to_string()),
            size_bytes: Some(42),
        };
        let line = serde_json::to_string(&artifact).unwrap();

        let parsed = client_event_from_worker_stdout(&line).unwrap();

        assert_eq!(parsed, ClientEvent::ArtifactUploaded { artifact });
    }

    #[test]
    fn meminfo_kib_values_are_converted_to_bytes() {
        assert_eq!(
            meminfo_bytes("MemTotal:       263792044 kB", "MemTotal:"),
            Some(263_792_044 * 1024)
        );
        assert_eq!(
            meminfo_bytes("MemAvailable:   123456 kB", "MemAvailable:"),
            Some(123_456 * 1024)
        );
    }

    #[test]
    fn gpu_memory_mib_values_are_converted_to_bytes() {
        assert_eq!(mib_to_bytes(32), 32 * 1024 * 1024);
    }

    #[test]
    fn desired_state_can_disable_job_acceptance() {
        let mut desired_state = DesiredState::default();
        assert!(job_rejection_reason(&desired_state, "job-1").is_none());

        desired_state.accept_jobs = false;
        assert_eq!(
            job_rejection_reason(&desired_state, "job-1").as_deref(),
            Some("cannot accept job job-1; desired state disables job acceptance")
        );

        desired_state.accept_jobs = true;
        desired_state.max_concurrent_jobs = 0;
        assert_eq!(
            job_rejection_reason(&desired_state, "job-1").as_deref(),
            Some("cannot accept job job-1; desired state max_concurrent_jobs is 0")
        );

        desired_state.max_concurrent_jobs = 1;
        desired_state.enabled = false;
        assert_eq!(
            job_rejection_reason(&desired_state, "job-1").as_deref(),
            Some("cannot accept job job-1; client is disabled by desired state")
        );
    }
}
