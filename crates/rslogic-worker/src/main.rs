use std::{
    collections::{HashMap, HashSet},
    io::{self, ErrorKind, Write as _},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
    time::Duration,
};

use anyhow::{anyhow, Context};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use clap::{Parser, Subcommand};
use futures_util::StreamExt;
use reqwest::Client;
use rslogic_protocol::{
    now, CameraIntrinsics, CloudfrontInput, JobEvent, JobEventDetails, JobEventKind,
    JobInputManifest, JobState, OrthoRenderMethod, OutputUploadTarget, PipelineJob,
    RealityScanPipeline, RealityScanStage, UploadedArtifact, DEFAULT_WORKER_STATE_DIR,
};
use rslogic_realityscan::{
    parse_realityscan_status, ContainerBindMount, ContainerRealityScanRunner, ContainerRuntime,
    RealityScanRunConfig, RealityScanRunner,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::{
    fs,
    io::{AsyncReadExt, AsyncWriteExt},
    sync::{mpsc, oneshot},
    time,
};
use tracing::{debug, info};
use tracing_subscriber::{fmt, EnvFilter};
use zip::{write::SimpleFileOptions, CompressionMethod, ZipWriter};

const INPUT_CACHE_MAX_UNUSED_DAYS: i64 = 30;
const REALITYSCAN_PHASE_MAX_RUNTIME_SECS: u64 = 7 * 24 * 60 * 60;
const ORTHO_REGION_WAIT_SECS: u64 = REALITYSCAN_PHASE_MAX_RUNTIME_SECS;
const REALITYSCAN_LIVENESS_CHECK_INTERVAL_SECS: u64 = 30;
const REALITYSCAN_SUCCESS_OUTPUT_SETTLE_SECS: u64 = 120;
const REALITYSCAN_PHASE_HEARTBEAT_SECS: u64 = 60;
const REALITYSCAN_PHASE_STALE_SECS: u64 = 10 * 60;
const PHASE_FRACTION_SCALE: f32 = 10_000.0;
const JOB_EVENTS_LOG: &str = "job-events.jsonl";

#[derive(Debug, Parser)]
struct Args {
    #[arg(long, env = "RSLOGIC_WORKER_STATE_DIR", default_value = DEFAULT_WORKER_STATE_DIR)]
    state_dir: PathBuf,
    #[arg(long, env = "RSLOGIC_CONTAINER_RUNTIME", default_value = "docker")]
    container_runtime: String,
    #[arg(long, env = "RSLOGIC_REALITYSCAN_CACHE_ROOT")]
    realityscan_cache_root: Option<PathBuf>,
    #[arg(
        long,
        env = "RSLOGIC_REALITYSCAN_PHASE_MAX_RUNTIME_SECS",
        default_value_t = REALITYSCAN_PHASE_MAX_RUNTIME_SECS
    )]
    realityscan_phase_max_runtime_secs: u64,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    RunJob {
        #[arg(long)]
        job: PathBuf,
    },
    DownloadOnly {
        #[arg(long)]
        manifest: PathBuf,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();
    let args = Args::parse();
    match &args.command {
        Command::RunJob { job } => {
            let raw = fs::read_to_string(job).await?;
            let pipeline_job: PipelineJob = serde_json::from_str(&raw)?;
            run_pipeline_job(&args, pipeline_job).await?;
        }
        Command::DownloadOnly { manifest } => {
            let raw = fs::read_to_string(manifest).await?;
            let manifest: JobInputManifest = serde_json::from_str(&raw)?;
            let job_dir = args.state_dir.join("jobs").join(&manifest.job_id);
            prepare_job_dir(&job_dir).await?;
            emit(
                &job_dir,
                &manifest.job_id,
                JobState::Accepted,
                "download-only job accepted",
                0.0,
            )
            .await?;
            download_inputs(&Client::new(), &manifest, &job_dir, &args.state_dir).await?;
        }
    }
    Ok(())
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    fmt().with_env_filter(filter).init();
}

async fn run_pipeline_job(args: &Args, job: PipelineJob) -> anyhow::Result<()> {
    let job_dir = args.state_dir.join("jobs").join(&job.job_id);
    prepare_job_dir(&job_dir).await?;
    write_json(&job_dir.join("manifest.json"), &job.manifest).await?;
    write_json(&job_dir.join("job.json"), &job).await?;

    let http = Client::builder()
        .timeout(Duration::from_secs(600))
        .build()?;

    if let Some(state) = read_worker_state(&job_dir).await? {
        if state.completed {
            emit(
                &job_dir,
                &job.job_id,
                JobState::Completed,
                "job already completed in local state",
                100.0,
            )
            .await?;
            return Ok(());
        }
        if !state.output_artifacts.is_empty() {
            emit(
                &job_dir,
                &job.job_id,
                JobState::Accepted,
                "resuming job from collected outputs",
                0.0,
            )
            .await?;
            upload_outputs(&http, &job, &job_dir, &state.output_artifacts).await?;
            write_worker_state(&job_dir, &job.job_id, state.output_artifacts, true).await?;
            emit(
                &job_dir,
                &job.job_id,
                JobState::Completed,
                "job completed",
                100.0,
            )
            .await?;
            return Ok(());
        }
    }

    emit(
        &job_dir,
        &job.job_id,
        JobState::Accepted,
        "job accepted",
        0.0,
    )
    .await?;
    download_inputs(&http, &job.manifest, &job_dir, &args.state_dir).await?;
    materialize_resume_project(&job.job_id, &job.pipeline, &job_dir, &args.state_dir).await?;
    run_realityscan(args, &job, &job_dir).await?;
    let outputs = collect_outputs(&job, &job_dir).await?;
    upload_outputs(&http, &job, &job_dir, &outputs).await?;
    write_worker_state(&job_dir, &job.job_id, outputs, true).await?;
    emit(
        &job_dir,
        &job.job_id,
        JobState::Completed,
        "job completed",
        100.0,
    )
    .await?;
    Ok(())
}

async fn prepare_job_dir(job_dir: &Path) -> anyhow::Result<()> {
    fs::create_dir_all(job_dir.join("inputs")).await?;
    fs::create_dir_all(job_dir.join("work")).await?;
    fs::create_dir_all(job_dir.join("outputs")).await?;
    fs::create_dir_all(job_dir.join("logs")).await?;
    Ok(())
}

async fn materialize_resume_project(
    job_id: &str,
    pipeline: &RealityScanPipeline,
    job_dir: &Path,
    state_dir: &Path,
) -> anyhow::Result<()> {
    let Some(source_job_id) = pipeline.resume_source_job_id.as_deref() else {
        return Ok(());
    };
    let Some(project_filename) = pipeline.resume_project_filename.as_deref() else {
        anyhow::bail!("resume_project_filename is required when resume_source_job_id is set");
    };
    validate_job_id_fragment(source_job_id)?;
    validate_output_filename(project_filename)?;

    let source_outputs = state_dir.join("jobs").join(source_job_id).join("outputs");
    let target_outputs = job_dir.join("outputs");
    let source_project = source_outputs.join(project_filename);
    let target_project = target_outputs.join(project_filename);
    if !source_project.is_file() {
        anyhow::bail!("resume project {} was not found", source_project.display());
    }

    copy_file_if_changed(&source_project, &target_project).await?;

    if let Some(stem) = Path::new(project_filename)
        .file_stem()
        .and_then(|value| value.to_str())
    {
        let source_sidecar = source_outputs.join(stem);
        if source_sidecar.is_dir() {
            let target_sidecar = target_outputs.join(stem);
            copy_dir_recursive(&source_sidecar, &target_sidecar).await?;
        }
    }

    emit(
        job_dir,
        job_id,
        JobState::Staging,
        &format!("staged resume project from job {source_job_id}"),
        32.0,
    )
    .await?;

    Ok(())
}

fn validate_job_id_fragment(value: &str) -> anyhow::Result<()> {
    if value.is_empty()
        || value.contains('/')
        || value.contains('\\')
        || value.contains("..")
        || !value
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '-' || ch == '_')
    {
        anyhow::bail!("invalid resume_source_job_id");
    }
    Ok(())
}

async fn copy_file_if_changed(source: &Path, target: &Path) -> anyhow::Result<()> {
    if let (Ok(source_meta), Ok(target_meta)) =
        (fs::metadata(source).await, fs::metadata(target).await)
    {
        if source_meta.len() == target_meta.len() {
            return Ok(());
        }
    }
    if let Some(parent) = target.parent() {
        fs::create_dir_all(parent).await?;
    }
    fs::copy(source, target)
        .await
        .with_context(|| format!("copying {} to {}", source.display(), target.display()))?;
    Ok(())
}

async fn copy_dir_recursive(source: &Path, target: &Path) -> anyhow::Result<()> {
    fs::create_dir_all(target).await?;
    let mut entries = fs::read_dir(source).await?;
    while let Some(entry) = entries.next_entry().await? {
        let source_path = entry.path();
        let target_path = target.join(entry.file_name());
        let metadata = entry.metadata().await?;
        if metadata.is_dir() {
            Box::pin(copy_dir_recursive(&source_path, &target_path)).await?;
        } else if metadata.is_file() {
            copy_file_if_changed(&source_path, &target_path).await?;
        }
    }
    Ok(())
}

async fn download_inputs(
    http: &Client,
    manifest: &JobInputManifest,
    job_dir: &Path,
    state_dir: &Path,
) -> anyhow::Result<()> {
    if manifest.expires_at < Utc::now() {
        return Err(anyhow!("input manifest expired at {}", manifest.expires_at));
    }
    let cache_root = input_cache_root(state_dir);
    prune_input_cache(&cache_root).await?;
    emit(
        job_dir,
        &manifest.job_id,
        JobState::Downloading,
        "downloading signed inputs",
        10.0,
    )
    .await?;
    for input in &manifest.inputs {
        let target = job_dir.join("inputs").join(&input.filename);
        if target.is_file() {
            if let Some(expected) = &input.sha256 {
                verify_sha256(&target, expected).await?;
                store_input_in_cache(&cache_root, input, &target).await?;
            }
            continue;
        }
        if input.sha256.is_some() && restore_input_from_cache(&cache_root, input, &target).await? {
            continue;
        }
        debug!(
            job_id = manifest.job_id,
            asset_id = input.asset_id,
            filename = input.filename,
            "downloading input"
        );
        download_input(http, input, &target).await?;
        if let Some(expected) = &input.sha256 {
            verify_sha256(&target, expected).await?;
            store_input_in_cache(&cache_root, input, &target).await?;
        }
    }
    emit(
        job_dir,
        &manifest.job_id,
        JobState::Verifying,
        "inputs downloaded and verified",
        30.0,
    )
    .await?;
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CachedInputMetadata {
    sha256: String,
    filename: String,
    size_bytes: Option<u64>,
    cached_at: DateTime<Utc>,
    last_used_at: DateTime<Utc>,
}

struct InputCachePaths {
    entry_dir: PathBuf,
    blob_path: PathBuf,
    metadata_path: PathBuf,
}

async fn download_input(
    http: &Client,
    input: &CloudfrontInput,
    target: &Path,
) -> anyhow::Result<()> {
    let response = http.get(&input.url).send().await?.error_for_status()?;
    let mut stream = response.bytes_stream();
    let tmp_target = target.with_extension(format!(
        "{}.download",
        target
            .extension()
            .and_then(|value| value.to_str())
            .unwrap_or("tmp")
    ));
    fs::remove_file(&tmp_target).await.ok();
    let mut file = fs::File::create(&tmp_target).await?;
    while let Some(chunk) = stream.next().await {
        file.write_all(&chunk?).await?;
    }
    file.flush().await?;
    fs::rename(&tmp_target, target).await?;
    Ok(())
}

fn input_cache_root(state_dir: &Path) -> PathBuf {
    state_dir.join("cache").join("inputs").join("sha256")
}

fn input_cache_paths(cache_root: &Path, sha256: &str) -> anyhow::Result<InputCachePaths> {
    let sha256 = normalized_sha256(sha256)?;
    let entry_dir = cache_root.join(&sha256[..2]).join(&sha256);
    Ok(InputCachePaths {
        blob_path: entry_dir.join("input"),
        metadata_path: entry_dir.join("metadata.json"),
        entry_dir,
    })
}

fn normalized_sha256(value: &str) -> anyhow::Result<String> {
    let normalized = value.trim().to_ascii_lowercase();
    if normalized.len() != 64 || !normalized.chars().all(|ch| ch.is_ascii_hexdigit()) {
        return Err(anyhow!("invalid sha256 value: {value}"));
    }
    Ok(normalized)
}

async fn restore_input_from_cache(
    cache_root: &Path,
    input: &CloudfrontInput,
    target: &Path,
) -> anyhow::Result<bool> {
    let Some(expected) = &input.sha256 else {
        return Ok(false);
    };
    let paths = input_cache_paths(cache_root, expected)?;
    if !paths.blob_path.is_file() {
        return Ok(false);
    }
    if verify_sha256(&paths.blob_path, expected).await.is_err() {
        fs::remove_dir_all(&paths.entry_dir).await.ok();
        return Ok(false);
    }
    link_or_copy_file(&paths.blob_path, target).await?;
    verify_sha256(target, expected).await?;
    write_cache_metadata(&paths, input, expected).await?;
    info!(
        asset_id = input.asset_id,
        filename = input.filename,
        "restored input from cache"
    );
    Ok(true)
}

async fn store_input_in_cache(
    cache_root: &Path,
    input: &CloudfrontInput,
    source: &Path,
) -> anyhow::Result<()> {
    let Some(expected) = &input.sha256 else {
        return Ok(());
    };
    let paths = input_cache_paths(cache_root, expected)?;
    fs::create_dir_all(&paths.entry_dir).await?;
    if paths.blob_path.is_file() {
        verify_sha256(&paths.blob_path, expected)
            .await
            .with_context(|| format!("verifying cached input {}", paths.blob_path.display()))?;
        write_cache_metadata(&paths, input, expected).await?;
        return Ok(());
    }
    let tmp_path = paths.entry_dir.join("input.tmp");
    fs::remove_file(&tmp_path).await.ok();
    link_or_copy_file(source, &tmp_path).await?;
    verify_sha256(&tmp_path, expected).await?;
    fs::rename(&tmp_path, &paths.blob_path).await?;
    write_cache_metadata(&paths, input, expected).await?;
    info!(
        asset_id = input.asset_id,
        filename = input.filename,
        "stored input in cache"
    );
    Ok(())
}

async fn write_cache_metadata(
    paths: &InputCachePaths,
    input: &CloudfrontInput,
    expected_sha256: &str,
) -> anyhow::Result<()> {
    let now = Utc::now();
    let cached_at = match fs::read_to_string(&paths.metadata_path).await {
        Ok(raw) => serde_json::from_str::<CachedInputMetadata>(&raw)
            .map(|metadata| metadata.cached_at)
            .unwrap_or(now),
        Err(error) if error.kind() == ErrorKind::NotFound => now,
        Err(error) => return Err(error.into()),
    };
    write_json(
        &paths.metadata_path,
        &CachedInputMetadata {
            sha256: normalized_sha256(expected_sha256)?,
            filename: input.filename.clone(),
            size_bytes: input.size_bytes,
            cached_at,
            last_used_at: now,
        },
    )
    .await
}

async fn link_or_copy_file(source: &Path, target: &Path) -> anyhow::Result<()> {
    if let Some(parent) = target.parent() {
        fs::create_dir_all(parent).await?;
    }
    fs::remove_file(target).await.ok();
    match fs::hard_link(source, target).await {
        Ok(()) => Ok(()),
        Err(_) => {
            fs::copy(source, target).await?;
            Ok(())
        }
    }
}

async fn prune_input_cache(cache_root: &Path) -> anyhow::Result<()> {
    if !cache_root.is_dir() {
        return Ok(());
    }
    let cutoff = Utc::now() - ChronoDuration::days(INPUT_CACHE_MAX_UNUSED_DAYS);
    let mut prefixes = fs::read_dir(cache_root).await?;
    while let Some(prefix) = prefixes.next_entry().await? {
        if !prefix.file_type().await?.is_dir() {
            continue;
        }
        let mut entries = fs::read_dir(prefix.path()).await?;
        while let Some(entry) = entries.next_entry().await? {
            if !entry.file_type().await?.is_dir() {
                continue;
            }
            let metadata_path = entry.path().join("metadata.json");
            let Ok(raw) = fs::read_to_string(&metadata_path).await else {
                continue;
            };
            let Ok(metadata) = serde_json::from_str::<CachedInputMetadata>(&raw) else {
                continue;
            };
            if metadata.last_used_at < cutoff {
                fs::remove_dir_all(entry.path()).await?;
            }
        }
    }
    Ok(())
}

async fn verify_sha256(path: &Path, expected: &str) -> anyhow::Result<()> {
    let mut file = fs::File::open(path).await?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0_u8; 1024 * 1024];
    loop {
        let read = file.read(&mut buffer).await?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    let actual = hex::encode(hasher.finalize());
    let expected = normalized_sha256(expected)?;
    if actual != expected {
        return Err(anyhow!(
            "sha256 mismatch for {}: expected {expected}, got {actual}",
            path.display()
        ));
    }
    Ok(())
}

async fn run_realityscan(args: &Args, job: &PipelineJob, job_dir: &Path) -> anyhow::Result<()> {
    emit(
        job_dir,
        &job.job_id,
        JobState::RunningRealityscan,
        "starting RealityScan stage",
        40.0,
    )
    .await?;
    if inputs_are_empty(job_dir).await? {
        fs::write(
            job_dir.join("logs").join("realityscan-skipped.log"),
            "RealityScan CLI skipped because the job has no input images.\n",
        )
        .await?;
        return Ok(());
    }
    let runtime = match args.container_runtime.as_str() {
        "podman" => ContainerRuntime::Podman,
        "docker" => ContainerRuntime::Docker,
        other => return Err(anyhow!("unsupported container runtime: {other}")),
    };
    let phases = realityscan_phases(&job.pipeline, &job.manifest)?;
    let runner = ContainerRealityScanRunner;
    let phase_count = phases.len().max(1) as f32;
    let realityscan_cache_dir = realityscan_job_cache_dir(args, &job.pipeline, &job.job_id)?;
    for (index, phase) in phases.iter().enumerate() {
        let file_stem = format!("{index:02}-{}", phase.name);
        prepare_realityscan_phase_artifacts(&job.pipeline, phase, job_dir).await?;
        let script_path = job_dir
            .join("work")
            .join(format!("run-realityscan-{file_stem}.sh"));
        let commands_path = job_dir.join("work").join(format!("{file_stem}.rscmd"));
        let windows_commands_path = format!("Z:\\job\\work\\{file_stem}.rscmd");
        let instance_name = realityscan_instance_name(&job.job_id, index, &phase.name);
        fs::write(
            &script_path,
            realityscan_cli_script(&job.pipeline, &windows_commands_path, &phase.commands)?,
        )
        .await?;
        let mut phase_commands = Vec::with_capacity(phase.commands.len() + 1);
        phase_commands.push(format!("-setInstanceName {instance_name}"));
        phase_commands.extend(phase.commands.clone());
        fs::write(&commands_path, phase_commands.join("\n")).await?;
        emit_with_details(
            job_dir,
            &job.job_id,
            JobState::RunningRealityscan,
            &format!("starting RealityScan phase {}", phase.name),
            40.0 + ((index as f32) / phase_count) * 40.0,
            Some(realityscan_phase_details(
                JobEventKind::Lifecycle,
                &phase.name,
                index,
                phases.len(),
            )),
        )
        .await?;
        let realityscan_log_prefix = format!("realityscan-{file_stem}");
        let stdout_log_path = job_dir
            .join("logs")
            .join(format!("{realityscan_log_prefix}.stdout.log"));
        let stderr_log_path = job_dir
            .join("logs")
            .join(format!("{realityscan_log_prefix}.stderr.log"));
        let (stdout_line_tx, stdout_line_rx) = mpsc::unbounded_channel();
        let phase_fraction = Arc::new(AtomicU32::new(0));
        let progress_task = tokio::spawn(monitor_realityscan_stdout(
            job.job_id.clone(),
            phase.name.clone(),
            index,
            phases.len(),
            job.manifest.inputs.len(),
            job_dir.to_path_buf(),
            stdout_line_rx,
            phase_fraction.clone(),
        ));
        let (heartbeat_stop_tx, heartbeat_stop_rx) = oneshot::channel();
        let heartbeat_task = tokio::spawn(emit_realityscan_phase_heartbeats(
            job.job_id.clone(),
            phase.name.clone(),
            index,
            phases.len(),
            job_dir.to_path_buf(),
            stdout_log_path.clone(),
            stderr_log_path.clone(),
            phase_fraction,
            heartbeat_stop_rx,
        ));
        let run_result = runner
            .run(RealityScanRunConfig {
                runtime: runtime.clone(),
                image: job.realityscan_image.clone(),
                job_dir: job_dir.to_path_buf(),
                command: vec![
                    "/bin/bash".to_string(),
                    format!("/job/work/run-realityscan-{file_stem}.sh"),
                ],
                gpu: true,
                extra_mounts: vec![ContainerBindMount {
                    host_path: realityscan_cache_dir.clone(),
                    container_path: "/root/.realityscan/realityscan".to_string(),
                    read_only: false,
                }],
                log_prefix: Some(realityscan_log_prefix),
                max_runtime_secs: (args.realityscan_phase_max_runtime_secs > 0)
                    .then_some(args.realityscan_phase_max_runtime_secs),
                liveness_check_interval_secs: Some(REALITYSCAN_LIVENESS_CHECK_INTERVAL_SECS),
                status_poll_interval_secs: None,
                realityscan_instance_name: Some(instance_name),
                fatal_output_patterns: realityscan_fatal_output_patterns(),
                success_output_paths: realityscan_phase_success_output_paths(
                    phase,
                    &job.pipeline,
                    job_dir,
                ),
                success_output_settle_secs: Some(REALITYSCAN_SUCCESS_OUTPUT_SETTLE_SECS),
                stdout_line_tx: Some(stdout_line_tx),
                stderr_line_tx: None,
            })
            .await;
        let _ = heartbeat_stop_tx.send(());
        heartbeat_task.await.ok();
        progress_task.await.ok();
        match run_result {
            Ok(_) => {}
            Err(error) => {
                let mut details = realityscan_phase_details(
                    JobEventKind::RealityScanFatal,
                    &phase.name,
                    index,
                    phases.len(),
                );
                details.stdout_log_path = Some(stdout_log_path.display().to_string());
                details.stderr_log_path = Some(stderr_log_path.display().to_string());
                details.raw_status = Some(format!("{error:#}"));
                emit_with_details(
                    job_dir,
                    &job.job_id,
                    JobState::Failed,
                    &format!("RealityScan phase {} failed: {error:#}", phase.name),
                    realityscan_phase_progress(index, phases.len(), 1.0),
                    Some(details),
                )
                .await
                .ok();
                return Err(error)
                    .with_context(|| format!("RealityScan phase {} failed", phase.name));
            }
        }
        emit_with_details(
            job_dir,
            &job.job_id,
            JobState::RunningRealityscan,
            &format!("completed RealityScan phase {}", phase.name),
            40.0 + (((index + 1) as f32) / phase_count) * 40.0,
            Some(realityscan_phase_details(
                JobEventKind::Lifecycle,
                &phase.name,
                index,
                phases.len(),
            )),
        )
        .await?;
    }
    Ok(())
}

fn realityscan_job_cache_dir(
    args: &Args,
    pipeline: &RealityScanPipeline,
    job_id: &str,
) -> anyhow::Result<PathBuf> {
    let cache_root = args
        .realityscan_cache_root
        .clone()
        .unwrap_or_else(|| args.state_dir.join("cache").join("realityscan"));
    let Some(namespace) = pipeline
        .runtime_settings
        .as_ref()
        .and_then(|settings| settings.cache_namespace.as_deref())
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return Ok(cache_root.join(job_id));
    };
    validate_realityscan_cache_namespace(namespace)?;
    Ok(cache_root.join("named").join(namespace))
}

fn validate_realityscan_cache_namespace(namespace: &str) -> anyhow::Result<()> {
    if namespace.len() > 128 {
        anyhow::bail!("realityscan cache_namespace cannot exceed 128 bytes");
    }
    if !namespace
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
    {
        anyhow::bail!(
            "realityscan cache_namespace can contain only ASCII letters, digits, '-' and '_'"
        );
    }
    Ok(())
}

async fn prepare_realityscan_phase_artifacts(
    pipeline: &RealityScanPipeline,
    phase: &RealityScanPhase,
    job_dir: &Path,
) -> anyhow::Result<()> {
    if !uses_generated_ortho_projection_params(pipeline)
        || !phase
            .commands
            .iter()
            .any(|command| command.contains("calculate-ortho.rsortho"))
    {
        return Ok(());
    }

    let region_filename = ortho_region_box_filename(pipeline).with_context(|| {
        "generated ortho projection parameters require an exported reconstruction region"
    })?;
    if phase.commands.iter().any(|command| {
        command.contains("-exportReconstructionRegion") && command.contains(region_filename)
    }) {
        return Ok(());
    }
    let region_path = job_dir.join("outputs").join(region_filename);
    let region_xml = fs::read_to_string(&region_path).await.with_context(|| {
        format!(
            "reading exported reconstruction region {}",
            region_path.display()
        )
    })?;
    let params = generated_ortho_projection_params_xml(pipeline, &region_xml)?;
    fs::write(
        job_dir.join("outputs").join("calculate-ortho.rsortho"),
        params,
    )
    .await?;
    Ok(())
}

async fn monitor_realityscan_stdout(
    job_id: String,
    phase_name: String,
    phase_index: usize,
    phase_count: usize,
    input_count: usize,
    job_dir: PathBuf,
    mut lines: mpsc::UnboundedReceiver<String>,
    phase_fraction: Arc<AtomicU32>,
) {
    let mut detected_images = 0_usize;
    let mut completed_stage_ids = HashSet::<&'static str>::new();
    while let Some(line) = lines.recv().await {
        if let Some(status) = parse_realityscan_status(&line) {
            let phase_fraction_value = status.progress_percent / 100.0;
            record_phase_fraction(&phase_fraction, phase_fraction_value);
            let mut details = realityscan_phase_details(
                JobEventKind::RealityScanStatus,
                &phase_name,
                phase_index,
                phase_count,
            );
            details.status_progress = Some(status.progress_percent);
            details.runtime_seconds = status.runtime_seconds;
            details.eta_seconds = status.eta_seconds;
            details.raw_status = Some(status.raw_status);
            emit_with_details(
                &job_dir,
                &job_id,
                JobState::RunningRealityscan,
                &format!(
                    "RealityScan {phase_name}: status {} {}",
                    status.progress_id,
                    format_age_seconds(status.runtime_seconds.map(|seconds| seconds as u64))
                ),
                realityscan_phase_progress(phase_index, phase_count, phase_fraction_value),
                Some(details),
            )
            .await
            .ok();
            continue;
        }

        if let Some(command) = parse_realityscan_command(&line) {
            if should_emit_realityscan_command(command) {
                let message = format!("RealityScan {phase_name}: command {command}");
                let phase_fraction_value = command_progress_hint(command);
                record_phase_fraction(&phase_fraction, phase_fraction_value);
                let progress =
                    realityscan_phase_progress(phase_index, phase_count, phase_fraction_value);
                let mut details = realityscan_phase_details(
                    JobEventKind::RealityScanCommand,
                    &phase_name,
                    phase_index,
                    phase_count,
                );
                details.command = Some(command.to_string());
                details.stage_id =
                    realityscan_command_stage_id(command, &phase_name).map(ToString::to_string);
                details.raw_status = Some(line.trim().to_string());
                emit_with_details(
                    &job_dir,
                    &job_id,
                    JobState::RunningRealityscan,
                    &message,
                    progress,
                    Some(details),
                )
                .await
                .ok();
            }
            continue;
        }

        if let Some(completion) = parse_realityscan_completion(&line, &phase_name) {
            if completed_stage_ids.insert(completion.stage_id) {
                record_phase_fraction(&phase_fraction, completion.phase_fraction);
                let mut details = realityscan_phase_details(
                    JobEventKind::RealityScanStatus,
                    &phase_name,
                    phase_index,
                    phase_count,
                );
                details.stage_id = Some(completion.stage_id.to_string());
                details.status_progress = Some(100.0);
                details.raw_status = Some(line.trim().to_string());
                emit_with_details(
                    &job_dir,
                    &job_id,
                    JobState::RunningRealityscan,
                    &format!(
                        "RealityScan {phase_name}: {} completed",
                        format_stage_event_id(completion.stage_id)
                    ),
                    realityscan_phase_progress(phase_index, phase_count, completion.phase_fraction),
                    Some(details),
                )
                .await
                .ok();
            }
            continue;
        }

        if line.contains("features in image") {
            detected_images += 1;
            if should_emit_feature_progress(detected_images, input_count) {
                let message = if input_count > 0 {
                    format!("RealityScan {phase_name}: feature detection {detected_images}/{input_count}")
                } else {
                    format!("RealityScan {phase_name}: feature detection {detected_images} images")
                };
                let fraction = if input_count > 0 {
                    ((detected_images as f32) / (input_count as f32)).clamp(0.0, 1.0) * 0.35
                } else {
                    0.2
                };
                record_phase_fraction(&phase_fraction, fraction);
                let mut details = realityscan_phase_details(
                    JobEventKind::RealityScanStatus,
                    &phase_name,
                    phase_index,
                    phase_count,
                );
                details.stage_id = Some("feature_detection".to_string());
                details.status_progress = Some((fraction / 0.35 * 100.0).clamp(0.0, 100.0));
                details.raw_status = Some(line.trim().to_string());
                emit_with_details(
                    &job_dir,
                    &job_id,
                    JobState::RunningRealityscan,
                    &message,
                    realityscan_phase_progress(phase_index, phase_count, fraction),
                    Some(details),
                )
                .await
                .ok();
            }
            continue;
        }

        if line.contains("Feature detection completed") {
            record_phase_fraction(&phase_fraction, 0.38);
            let mut details = realityscan_phase_details(
                JobEventKind::RealityScanStatus,
                &phase_name,
                phase_index,
                phase_count,
            );
            details.stage_id = Some("feature_detection".to_string());
            details.status_progress = Some(100.0);
            details.raw_status = Some(line.trim().to_string());
            emit_with_details(
                &job_dir,
                &job_id,
                JobState::RunningRealityscan,
                &format!("RealityScan {phase_name}: {}", line.trim()),
                realityscan_phase_progress(phase_index, phase_count, 0.38),
                Some(details),
            )
            .await
            .ok();
            continue;
        }

        if line.contains("Alignment completed") {
            record_phase_fraction(&phase_fraction, 0.82);
            let mut details = realityscan_phase_details(
                JobEventKind::RealityScanStatus,
                &phase_name,
                phase_index,
                phase_count,
            );
            details.stage_id = Some("alignment".to_string());
            details.status_progress = Some(100.0);
            details.raw_status = Some(line.trim().to_string());
            emit_with_details(
                &job_dir,
                &job_id,
                JobState::RunningRealityscan,
                &format!("RealityScan {phase_name}: {}", line.trim()),
                realityscan_phase_progress(phase_index, phase_count, 0.82),
                Some(details),
            )
            .await
            .ok();
        }
    }
}

async fn emit_realityscan_phase_heartbeats(
    job_id: String,
    phase_name: String,
    phase_index: usize,
    phase_count: usize,
    job_dir: PathBuf,
    stdout_log_path: PathBuf,
    stderr_log_path: PathBuf,
    phase_fraction: Arc<AtomicU32>,
    mut stop_rx: oneshot::Receiver<()>,
) {
    let mut interval = time::interval(Duration::from_secs(REALITYSCAN_PHASE_HEARTBEAT_SECS));
    interval.tick().await;
    loop {
        tokio::select! {
            _ = &mut stop_rx => break,
            _ = interval.tick() => {
                let stdout_age = file_age_seconds(&stdout_log_path).await;
                let stderr_age = file_age_seconds(&stderr_log_path).await;
                let outputs_age = file_age_seconds(&job_dir.join("outputs")).await;
                let latest_fraction =
                    phase_fraction_from_units(phase_fraction.load(Ordering::Relaxed));
                let stale = realityscan_phase_is_stale(stdout_age, outputs_age);
                let message = if stale {
                    format!(
                        "RealityScan {phase_name}: stale heartbeat stdout_age={} stderr_age={} outputs_age={} stale_after={}",
                        format_age_seconds(stdout_age),
                        format_age_seconds(stderr_age),
                        format_age_seconds(outputs_age),
                        format_age_seconds(Some(REALITYSCAN_PHASE_STALE_SECS)),
                    )
                } else {
                    format!(
                        "RealityScan {phase_name}: heartbeat stdout_age={} stderr_age={} outputs_age={}",
                        format_age_seconds(stdout_age),
                        format_age_seconds(stderr_age),
                        format_age_seconds(outputs_age),
                    )
                };
                let mut details = realityscan_phase_details(
                    JobEventKind::RealityScanHeartbeat,
                    &phase_name,
                    phase_index,
                    phase_count,
                );
                details.stdout_log_path = Some(stdout_log_path.display().to_string());
                details.stderr_log_path = Some(stderr_log_path.display().to_string());
                details.output_path = Some(job_dir.join("outputs").display().to_string());
                details.raw_status = Some(message.clone());
                emit_with_details(
                    &job_dir,
                    &job_id,
                    JobState::RunningRealityscan,
                    &message,
                    realityscan_phase_progress(phase_index, phase_count, latest_fraction),
                    Some(details),
                )
                .await
                .ok();
            }
        }
    }
}

async fn file_age_seconds(path: &Path) -> Option<u64> {
    let metadata = fs::metadata(path).await.ok()?;
    let modified = metadata.modified().ok()?;
    modified.elapsed().ok().map(|elapsed| elapsed.as_secs())
}

fn format_age_seconds(value: Option<u64>) -> String {
    value
        .map(|seconds| format!("{seconds}s"))
        .unwrap_or_else(|| "unknown".to_string())
}

fn realityscan_fatal_output_patterns() -> Vec<String> {
    vec![
        "processing failed:".to_string(),
        "operation failed.".to_string(),
        " failed after ".to_string(),
    ]
}

fn realityscan_phase_details(
    kind: JobEventKind,
    phase_name: &str,
    phase_index: usize,
    phase_count: usize,
) -> JobEventDetails {
    JobEventDetails {
        kind,
        stage_id: None,
        phase_id: Some(format!("{phase_index:02}-{phase_name}")),
        phase_index: Some(phase_index as u32),
        phase_count: Some(phase_count as u32),
        command: None,
        status_progress: None,
        runtime_seconds: None,
        eta_seconds: None,
        raw_status: None,
        stdout_log_path: None,
        stderr_log_path: None,
        output_path: None,
        fatal_pattern: None,
    }
}

fn realityscan_instance_name(job_id: &str, phase_index: usize, phase_name: &str) -> String {
    let job_fragment: String = job_id
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric())
        .take(8)
        .collect();
    let phase_fragment = realityscan_name_fragment(phase_name);
    format!(
        "rslogic_{}_{phase_index:02}_{phase_fragment}",
        if job_fragment.is_empty() {
            "job"
        } else {
            &job_fragment
        }
    )
}

fn realityscan_name_fragment(value: &str) -> String {
    let fragment: String = value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect();
    let fragment = fragment.trim_matches('_');
    if fragment.is_empty() {
        "phase".to_string()
    } else {
        fragment.to_string()
    }
}

fn parse_realityscan_command(line: &str) -> Option<&str> {
    let marker = "Executing command '";
    let start = line.find(marker)? + marker.len();
    let rest = &line[start..];
    let end = rest.find('\'')?;
    Some(&rest[..end])
}

fn should_emit_realityscan_command(command: &str) -> bool {
    matches!(
        command,
        "selectAllImages"
            | "selectImage"
            | "editInputSelection"
            | "addFolder"
            | "align"
            | "selectMaximalComponent"
            | "setReconstructionRegionAuto"
            | "setReconstructionRegionByDensity"
            | "calculatePreviewModel"
            | "calculateNormalModel"
            | "calculateHighModel"
            | "continueModelCalculation"
            | "correctColors"
            | "calculateTexture"
            | "calculateOrthoProjection"
            | "exportOrthoProjection"
            | "save"
            | "load"
    )
}

fn should_emit_feature_progress(detected_images: usize, input_count: usize) -> bool {
    detected_images == 1
        || detected_images % 50 == 0
        || (input_count > 0 && detected_images == input_count)
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct RealityScanCompletion {
    stage_id: &'static str,
    phase_fraction: f32,
}

fn parse_realityscan_completion(line: &str, phase_name: &str) -> Option<RealityScanCompletion> {
    let line = line.trim();
    if line.contains("Texturing Model completed") {
        return Some(RealityScanCompletion {
            stage_id: "calculate_texture",
            phase_fraction: 0.35,
        });
    }
    if line.contains("Color correction completed")
        || line.contains("Correcting colors completed")
        || line.contains("Correct Colors completed")
    {
        return Some(RealityScanCompletion {
            stage_id: "correct_colors",
            phase_fraction: 0.68,
        });
    }
    if line.contains("Calculating Orthographic Projection completed") {
        return Some(RealityScanCompletion {
            stage_id: "calculate_ortho_projection",
            phase_fraction: 0.62,
        });
    }
    if line.contains("Exporting Orthographic Projection completed") {
        return Some(RealityScanCompletion {
            stage_id: "export_ortho_projection",
            phase_fraction: 0.88,
        });
    }
    if (phase_name == "outputs" || phase_name == "single")
        && (line.contains("Saving Project completed") || line.contains("Save Project completed"))
    {
        return Some(RealityScanCompletion {
            stage_id: "save_project",
            phase_fraction: 0.98,
        });
    }
    None
}

fn command_progress_hint(command: &str) -> f32 {
    match command {
        "selectAllImages" => 0.03,
        "selectImage" | "editInputSelection" => 0.06,
        "addFolder" => 0.03,
        "align" => 0.36,
        "selectMaximalComponent" => 0.84,
        "setReconstructionRegionAuto" => 0.10,
        "setReconstructionRegionByDensity" => 0.12,
        "calculatePreviewModel"
        | "calculateNormalModel"
        | "calculateHighModel"
        | "continueModelCalculation" => 0.42,
        "correctColors" => 0.62,
        "calculateTexture" => 0.18,
        "calculateOrthoProjection" => 0.52,
        "exportOrthoProjection" => 0.78,
        "save" => 0.92,
        "load" => 0.02,
        _ => 0.05,
    }
}

fn realityscan_command_stage_id(command: &str, phase_name: &str) -> Option<&'static str> {
    match command {
        "selectAllImages" | "selectImage" | "editInputSelection" => Some("set_intrinsics"),
        "addFolder" | "align" => Some("align"),
        "selectMaximalComponent" => Some("select_maximal_component"),
        "setReconstructionRegionAuto" => Some("set_reconstruction_region_auto"),
        "setReconstructionRegionByDensity" => Some("set_reconstruction_region_by_density"),
        "calculatePreviewModel" => Some("calculate_preview_model"),
        "calculateNormalModel" => Some("calculate_normal_model"),
        "calculateHighModel" => Some("calculate_high_model"),
        "continueModelCalculation" => Some("continue_model_calculation"),
        "correctColors" => Some("correct_colors"),
        "calculateTexture" => Some("calculate_texture"),
        "calculateOrthoProjection" => Some("calculate_ortho_projection"),
        "exportOrthoProjection" => Some("export_ortho_projection"),
        "save" if phase_name == "outputs" || phase_name == "single" => Some("save_project"),
        _ => None,
    }
}

fn format_stage_event_id(stage_id: &str) -> String {
    stage_id.replace('_', " ")
}

fn record_phase_fraction(phase_fraction: &AtomicU32, fraction: f32) {
    phase_fraction.fetch_max(phase_fraction_units(fraction), Ordering::Relaxed);
}

fn phase_fraction_units(fraction: f32) -> u32 {
    (fraction.clamp(0.0, 1.0) * PHASE_FRACTION_SCALE).round() as u32
}

fn phase_fraction_from_units(units: u32) -> f32 {
    (units as f32 / PHASE_FRACTION_SCALE).clamp(0.0, 1.0)
}

fn realityscan_phase_is_stale(stdout_age: Option<u64>, outputs_age: Option<u64>) -> bool {
    stdout_age.is_some_and(|age| age >= REALITYSCAN_PHASE_STALE_SECS)
        && outputs_age.is_some_and(|age| age >= REALITYSCAN_PHASE_STALE_SECS)
}

fn realityscan_phase_progress(phase_index: usize, phase_count: usize, phase_fraction: f32) -> f32 {
    let phase_count = phase_count.max(1) as f32;
    let base = 40.0 + ((phase_index as f32) / phase_count) * 40.0;
    let span = 40.0 / phase_count;
    base + phase_fraction.clamp(0.0, 1.0) * span
}

async fn inputs_are_empty(job_dir: &Path) -> anyhow::Result<bool> {
    let mut entries = fs::read_dir(job_dir.join("inputs")).await?;
    Ok(entries.next_entry().await?.is_none())
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RealityScanPhase {
    name: String,
    commands: Vec<String>,
}

fn realityscan_phases(
    pipeline: &RealityScanPipeline,
    manifest: &JobInputManifest,
) -> anyhow::Result<Vec<RealityScanPhase>> {
    let stages = effective_stages(pipeline);
    let has_alignment_stage = stages.iter().any(is_alignment_stage);
    let resume_project = pipeline.resume_project_filename.as_deref();
    if resume_project.is_some() && has_alignment_stage {
        anyhow::bail!("resume_project_filename cannot be used with alignment stages");
    }
    let should_split = stages.iter().any(is_split_trigger_stage)
        && (has_alignment_stage || resume_project.is_some());
    if pipeline.single_session || !should_split {
        return Ok(vec![RealityScanPhase {
            name: "single".to_string(),
            commands: combined_realityscan_commands(pipeline, manifest, &stages)?,
        }]);
    }

    let mut phases = Vec::new();
    let mut latest_project = if has_alignment_stage {
        let mut align_commands = new_scene_commands(pipeline)?;
        for stage in stages.iter().filter(|stage| is_alignment_stage(stage)) {
            align_commands.extend(realityscan_stage_commands(stage, pipeline, manifest)?);
        }
        align_commands.push(save_project_command("aligned.rsproj"));
        align_commands.push("-quit".to_string());
        phases.push(RealityScanPhase {
            name: "align-save".to_string(),
            commands: align_commands,
        });
        "aligned.rsproj"
    } else {
        resume_project.expect("resume project checked by should_split")
    };

    let model_stages: Vec<RealityScanStage> = stages
        .iter()
        .filter(|stage| is_model_stage(stage))
        .cloned()
        .collect();
    let mut model_stage_commands = Vec::new();
    for stage in &model_stages {
        model_stage_commands.extend(realityscan_stage_commands(stage, pipeline, manifest)?);
    }
    let has_output_stage = stages.iter().any(is_output_stage);
    if !model_stage_commands.is_empty() {
        let mut commands = vec![load_project_command_for_stages(
            latest_project,
            pipeline,
            &model_stages,
        )];
        commands.extend(realityscan_runtime_setting_commands(pipeline)?);
        commands.extend(print_progress_commands(pipeline));
        commands.extend(model_stage_commands);
        if has_output_stage {
            commands.push(save_project_command("modeled.rsproj"));
            latest_project = "modeled.rsproj";
        }
        commands.push("-quit".to_string());
        phases.push(RealityScanPhase {
            name: "model-save".to_string(),
            commands,
        });
    }

    let mut output_stage_commands = Vec::new();
    for stage in stages.iter().filter(|stage| is_output_stage(stage)) {
        output_stage_commands.extend(realityscan_stage_commands(stage, pipeline, manifest)?);
    }
    if !output_stage_commands.is_empty() {
        let mut commands = vec![load_project_command(latest_project)];
        commands.extend(realityscan_runtime_setting_commands(pipeline)?);
        commands.extend(print_progress_commands(pipeline));
        commands.extend(output_stage_commands);
        commands.push("-quit".to_string());
        phases.push(RealityScanPhase {
            name: "outputs".to_string(),
            commands,
        });
    }

    Ok(phases)
}

fn effective_stages(pipeline: &RealityScanPipeline) -> Vec<RealityScanStage> {
    if pipeline.stages.is_empty() {
        RealityScanPipeline::default().stages
    } else {
        pipeline.stages.clone()
    }
}

fn combined_realityscan_commands(
    pipeline: &RealityScanPipeline,
    manifest: &JobInputManifest,
    stages: &[RealityScanStage],
) -> anyhow::Result<Vec<String>> {
    let mut commands = if let Some(project_filename) = pipeline.resume_project_filename.as_deref() {
        if stages.iter().any(is_alignment_stage) {
            anyhow::bail!("resume_project_filename cannot be used with alignment stages");
        }
        let mut commands = vec![load_project_command_for_stages(
            project_filename,
            pipeline,
            stages,
        )];
        commands.extend(realityscan_runtime_setting_commands(pipeline)?);
        commands
    } else {
        new_scene_commands(pipeline)?
    };
    commands.extend(print_progress_commands(pipeline));
    for stage in stages {
        commands.extend(realityscan_stage_commands(stage, pipeline, manifest)?);
    }
    commands.push("-quit".to_string());
    Ok(commands)
}

fn new_scene_commands(pipeline: &RealityScanPipeline) -> anyhow::Result<Vec<String>> {
    let mut commands = vec!["-newScene".to_string()];
    commands.extend(realityscan_coordinate_system_commands(pipeline)?);
    commands.extend(realityscan_runtime_setting_commands(pipeline)?);
    commands.extend(realityscan_alignment_setting_commands(pipeline));
    commands.push("-addFolder \"Z:\\job\\inputs\"".to_string());
    Ok(commands)
}

fn realityscan_runtime_setting_commands(
    pipeline: &RealityScanPipeline,
) -> anyhow::Result<Vec<String>> {
    let Some(settings) = &pipeline.runtime_settings else {
        return Ok(Vec::new());
    };

    let mut pairs = Vec::<(&'static str, String)>::new();
    push_bool_setting(&mut pairs, "appAutoSaveMode", settings.auto_save_mode);
    if let Some(value) = settings
        .auto_save_cli_handling
        .as_deref()
        .filter(|value| !value.trim().is_empty())
    {
        if value.contains('\n') || value.contains('\r') {
            anyhow::bail!("auto_save_cli_handling cannot contain newlines");
        }
        pairs.push(("appAutoSaveCliHandling", value.trim().to_string()));
    }
    push_u32_setting(&mut pairs, "appAutoClearCache", settings.auto_clear_cache);
    push_bool_setting(
        &mut pairs,
        "MvsGeometryGpuAccel",
        settings.geometry_gpu_accel,
    );
    push_u32_setting(
        &mut pairs,
        "mvsMaxVertexCountInPart",
        settings.max_vertex_count_in_part,
    );

    Ok(pairs
        .into_iter()
        .map(|(key, value)| format!("-set {}", rscmd_quote(&format!("{key}={value}"))))
        .collect())
}

fn realityscan_coordinate_system_commands(
    pipeline: &RealityScanPipeline,
) -> anyhow::Result<Vec<String>> {
    let mut commands = Vec::new();
    if let Some(value) = coordinate_system_value(
        "project_coordinate_system",
        pipeline.project_coordinate_system.as_deref(),
    )? {
        commands.push(format!(
            "-setProjectCoordinateSystem {}",
            rscmd_quote(&value)
        ));
    }
    if let Some(value) = coordinate_system_value(
        "output_coordinate_system",
        pipeline.output_coordinate_system.as_deref(),
    )? {
        commands.push(format!(
            "-setOutputCoordinateSystem {}",
            rscmd_quote(&value)
        ));
    }
    Ok(commands)
}

fn coordinate_system_value(label: &str, value: Option<&str>) -> anyhow::Result<Option<String>> {
    let Some(value) = value else {
        return Ok(None);
    };
    let trimmed = value.trim();
    if trimmed.is_empty() {
        anyhow::bail!("{label} cannot be empty");
    }
    if trimmed.contains('\n') || trimmed.contains('\r') {
        anyhow::bail!("{label} cannot contain newlines");
    }
    Ok(Some(trimmed.to_string()))
}

fn print_progress_commands(pipeline: &RealityScanPipeline) -> Vec<String> {
    match pipeline.print_progress_interval_seconds {
        Some(seconds) if seconds > 0 => vec![format!("-printProgress {seconds}")],
        _ => Vec::new(),
    }
}

fn is_alignment_stage(stage: &RealityScanStage) -> bool {
    matches!(
        stage,
        RealityScanStage::SetIntrinsics | RealityScanStage::Align
    )
}

fn is_model_stage(stage: &RealityScanStage) -> bool {
    matches!(
        stage,
        RealityScanStage::SelectMaximalComponent
            | RealityScanStage::SetReconstructionRegionAuto
            | RealityScanStage::SetReconstructionRegionByDensity
            | RealityScanStage::CalculatePreviewModel
            | RealityScanStage::CalculateNormalModel
            | RealityScanStage::CalculateHighModel
            | RealityScanStage::ContinueModelCalculation
    )
}

fn is_output_stage(stage: &RealityScanStage) -> bool {
    matches!(
        stage,
        RealityScanStage::CorrectColors
            | RealityScanStage::CalculateTexture
            | RealityScanStage::CalculateOrthoProjection
            | RealityScanStage::ExportOrthoProjection
            | RealityScanStage::SaveProject
    )
}

fn is_split_trigger_stage(stage: &RealityScanStage) -> bool {
    matches!(
        stage,
        RealityScanStage::SetReconstructionRegionAuto
            | RealityScanStage::SetReconstructionRegionByDensity
            | RealityScanStage::CalculatePreviewModel
            | RealityScanStage::CalculateNormalModel
            | RealityScanStage::CalculateHighModel
            | RealityScanStage::ContinueModelCalculation
            | RealityScanStage::CorrectColors
            | RealityScanStage::CalculateTexture
            | RealityScanStage::CalculateOrthoProjection
            | RealityScanStage::ExportOrthoProjection
    )
}

fn realityscan_phase_success_output_paths(
    phase: &RealityScanPhase,
    pipeline: &RealityScanPipeline,
    job_dir: &Path,
) -> Vec<PathBuf> {
    let outputs_dir = job_dir.join("outputs");
    let mut filenames = Vec::<String>::new();
    for filename in ["aligned.rsproj", "modeled.rsproj"] {
        if phase
            .commands
            .iter()
            .any(|command| command_writes_output_filename(command, filename))
        {
            filenames.push(filename.to_string());
        }
    }
    if phase
        .commands
        .iter()
        .any(|command| command_writes_output_filename(command, pipeline.project_filename.as_str()))
    {
        filenames.push(pipeline.project_filename.clone());
    }
    if let Some(filename) = pipeline.orthomosaic_filename.as_deref() {
        if phase
            .commands
            .iter()
            .any(|command| command_writes_output_filename(command, filename))
        {
            filenames.push(filename.to_string());
        }
    }

    filenames.sort();
    filenames.dedup();
    filenames
        .into_iter()
        .map(|filename| outputs_dir.join(filename))
        .collect()
}

fn command_writes_output_filename(command: &str, filename: &str) -> bool {
    let command = command.trim_start();
    (command.starts_with("-save ") || command.starts_with("-exportOrthoProjection "))
        && command.contains(&format!("Z:\\job\\outputs\\{filename}"))
}

fn save_project_command(filename: &str) -> String {
    format!("-save {}", rscmd_quote(&windows_output_path(filename)))
}

fn load_project_command(filename: &str) -> String {
    load_project_command_with_mode(filename, "deleteAutosave")
}

fn load_project_command_for_stages(
    filename: &str,
    pipeline: &RealityScanPipeline,
    stages: &[RealityScanStage],
) -> String {
    let autosave_handling = pipeline
        .runtime_settings
        .as_ref()
        .and_then(|settings| settings.auto_save_cli_handling.as_deref())
        .map(str::trim);
    let mode = if stages.contains(&RealityScanStage::ContinueModelCalculation)
        && autosave_handling.is_some_and(|value| value.eq_ignore_ascii_case("recover"))
    {
        "recoverAutosave"
    } else {
        "deleteAutosave"
    };
    load_project_command_with_mode(filename, mode)
}

fn load_project_command_with_mode(filename: &str, mode: &str) -> String {
    format!(
        "-load {} {mode}",
        rscmd_quote(&windows_output_path(filename)),
    )
}

fn realityscan_cli_script(
    pipeline: &RealityScanPipeline,
    windows_commands_path: &str,
    phase_commands: &[String],
) -> anyhow::Result<String> {
    if pipeline
        .ortho_pixel_size_meters
        .is_some_and(|value| !value.is_finite() || value <= 0.0)
    {
        anyhow::bail!("ortho_pixel_size_meters must be greater than zero");
    }
    let ortho_export_config = ortho_export_config_xml(pipeline);
    let ortho_projection_params = ortho_projection_params_xml(pipeline)?;
    let mut script = r#"set -euo pipefail
mkdir -p /job/outputs /job/logs/realityscan-crash-reports /tmp/runtime-rslogic
chmod 700 /tmp/runtime-rslogic
Xvfb :99 -screen 0 1920x1080x24 -nolisten tcp >/job/logs/xvfb.log 2>&1 &
xvfb_pid=$!
rslogic_rsortho_watcher_pid=""
cleanup() {
  if [ -n "${rslogic_rsortho_watcher_pid}" ]; then
    kill "${rslogic_rsortho_watcher_pid}" 2>/dev/null || true
    wait "${rslogic_rsortho_watcher_pid}" 2>/dev/null || true
  fi
  kill "${xvfb_pid}" 2>/dev/null || true
  wait "${xvfb_pid}" 2>/dev/null || true
}
trap cleanup EXIT
sleep 2
export DISPLAY=:99
cat > /job/outputs/export-ortho-config.xml <<'XML'
"#
    .to_string();
    script.push_str(&ortho_export_config);
    script.push_str("XML\n");
    if let Some(params) = ortho_projection_params {
        script.push_str("cat > /job/outputs/calculate-ortho.rsortho <<'XML'\n");
        script.push_str(&params);
        script.push_str("XML\n");
    } else if phase_needs_generated_ortho_projection_params(phase_commands) {
        if let Some(watcher) = generated_ortho_projection_params_watcher_script(pipeline) {
            script.push_str(&watcher);
        }
    }
    script.push_str(&format!(
        "/opt/realityscan/bin/realityscan-cli -headless -silent {} -stdConsole -execRSCMD {}\n",
        shell_quote("Z:\\job\\logs\\realityscan-crash-reports"),
        shell_quote(windows_commands_path)
    ));
    Ok(script)
}

fn phase_needs_generated_ortho_projection_params(phase_commands: &[String]) -> bool {
    phase_commands.iter().any(|command| {
        command.contains("-calculateOrthoProjection")
            && command.contains("Z:\\job\\outputs\\calculate-ortho.rsortho")
    })
}

fn generated_ortho_projection_params_watcher_script(
    pipeline: &RealityScanPipeline,
) -> Option<String> {
    if !uses_generated_ortho_projection_params(pipeline) {
        return None;
    }
    let region_filename = ortho_region_box_filename(pipeline)?;
    let pixel_size = pipeline.ortho_pixel_size_meters?;
    let method = pipeline.ortho_render_method.as_ref()?;
    Some(format!(
        r#"generate_ortho_projection_params_from_region() {{
  region_path='/job/outputs/{region_filename}'
  output_path='/job/outputs/calculate-ortho.rsortho'
  region_wait_seconds="${{RSLOGIC_ORTHO_REGION_WAIT_SECS:-{region_wait_seconds}}}"
  for elapsed in $(seq 1 "${{region_wait_seconds}}"); do
    if [ -s "${{output_path}}" ]; then
      return 0
    fi
    if [ -s "${{region_path}}" ] && grep -q '<ReconstructionRegion' "${{region_path}}"; then
      break
    fi
    if [ $((elapsed % 300)) -eq 0 ]; then
      echo "waiting for exported reconstruction region ${{region_path}} (${{elapsed}}s/${{region_wait_seconds}}s)" >&2
    fi
    sleep 1
  done
  if ! [ -s "${{region_path}}" ]; then
    echo "timed out waiting for exported reconstruction region ${{region_path}}" >&2
    return 1
  fi
  region_xml=$(cat "${{region_path}}")
  values=$(printf '%s' "${{region_xml}}" | sed -n 's/.*widthHeightDepth="\([^"]*\)".*/\1/p' | head -n 1)
  if [ -z "${{values}}" ]; then
    values=$(printf '%s' "${{region_xml}}" | tr '\n' ' ' | sed -n 's/.*<widthHeightDepth>\([^<]*\)<\/widthHeightDepth>.*/\1/p' | head -n 1)
  fi
  width_m=$(printf '%s\n' "${{values}}" | awk '{{print $1}}')
  height_m=$(printf '%s\n' "${{values}}" | awk '{{print $2}}')
  width_px=$(awk -v meters="${{width_m}}" -v pixel="{pixel_size}" 'BEGIN {{ if (meters <= 0 || pixel <= 0) exit 1; v = meters / pixel; px = int(v); if (v > px) px += 1; if (px < 1) px = 1; print px }}')
  height_px=$(awk -v meters="${{height_m}}" -v pixel="{pixel_size}" 'BEGIN {{ if (meters <= 0 || pixel <= 0) exit 1; v = meters / pixel; px = int(v); if (v > px) px += 1; if (px < 1) px = 1; print px }}')
  tmp="${{output_path}}.tmp"
  {{
    cat <<XML
<OrthoProjection width="${{width_px}}" height="${{height_px}}" name="Ortho projection 1" modelName="Model 1"
   colorType="{color_type}" boxSideConerIndex="21" bEmpty="0" backFaceColorType="1" backFaceColor="2130706687"
   projectionType="3" bShowOrthoProjection="1">
  <Header magic="5787472" version="2"/>
</OrthoProjection>
XML
    sed 's/[[:space:]]ownerId="[^"]*"//g' "${{region_path}}"
  }} > "${{tmp}}"
  mv "${{tmp}}" "${{output_path}}"
}}
generate_ortho_projection_params_from_region &
rslogic_rsortho_watcher_pid=$!
"#,
        region_wait_seconds = ORTHO_REGION_WAIT_SECS,
        pixel_size = format_decimal(pixel_size),
        color_type = realityscan_ortho_color_type(method),
    ))
}

#[cfg(test)]
fn realityscan_rscmd_script(
    pipeline: &RealityScanPipeline,
    manifest: &JobInputManifest,
) -> anyhow::Result<String> {
    validate_output_filename(&pipeline.project_filename)?;
    if let Some(filename) = &pipeline.orthomosaic_filename {
        validate_output_filename(filename)?;
    }
    let stages = if pipeline.stages.is_empty() {
        RealityScanPipeline::default().stages
    } else {
        pipeline.stages.clone()
    };
    let mut script = String::from("-newScene\n");
    for command in realityscan_coordinate_system_commands(pipeline)? {
        script.push_str(&command);
        script.push('\n');
    }
    for command in realityscan_alignment_setting_commands(pipeline) {
        script.push_str(&command);
        script.push('\n');
    }
    script.push_str("-addFolder \"Z:\\job\\inputs\"\n");
    for stage in stages {
        for command in realityscan_stage_commands(&stage, pipeline, manifest)? {
            script.push_str(&command);
            script.push('\n');
        }
    }
    script.push_str("-quit\n");
    Ok(script)
}

fn ortho_export_config_xml(pipeline: &RealityScanPipeline) -> String {
    let mut config = String::from(
        r#"<Configuration>
  <entry key="exportOrthoWorldCoordSystemAxesType" value="1"/>
  <entry key="exportOrthoAsBatch" value="false"/>
  <entry key="exportDSM" value="false"/>
  <entry key="exportOrthoInfoFile" value="true"/>
  <entry key="exportProjectionParametersFile" value="true"/>
  <entry key="exportOrthoAsBigTiff" value="true"/>
  <entry key="exportOrthoCompression" value="0"/>
  <entry key="exportOrthoWorldFile" value="-1"/>
"#,
    );
    if let Some(pixel_size) = pipeline.ortho_pixel_size_meters {
        config.push_str(&format!(
            r#"  <entry key="orthoPixelSize" value="{}"/>
"#,
            format_decimal(pixel_size)
        ));
    }
    config.push_str("</Configuration>\n");
    config
}

fn has_raw_ortho_projection_params(pipeline: &RealityScanPipeline) -> bool {
    pipeline
        .ortho_projection_params_xml
        .as_deref()
        .is_some_and(|value| !value.trim().is_empty())
}

fn uses_generated_ortho_projection_params(pipeline: &RealityScanPipeline) -> bool {
    !has_raw_ortho_projection_params(pipeline)
        && pipeline.ortho_pixel_size_meters.is_some()
        && pipeline.ortho_render_method.is_some()
}

fn has_ortho_projection_params(pipeline: &RealityScanPipeline) -> bool {
    has_raw_ortho_projection_params(pipeline) || uses_generated_ortho_projection_params(pipeline)
}

fn uses_auto_ortho_region_box(pipeline: &RealityScanPipeline) -> bool {
    has_ortho_projection_params(pipeline)
        && effective_stages(pipeline).contains(&RealityScanStage::SetReconstructionRegionAuto)
}

fn uses_density_ortho_region_box(pipeline: &RealityScanPipeline) -> bool {
    has_ortho_projection_params(pipeline)
        && effective_stages(pipeline).contains(&RealityScanStage::SetReconstructionRegionByDensity)
}

fn ortho_region_box_path(pipeline: &RealityScanPipeline) -> Option<&'static str> {
    Some(match ortho_region_box_filename(pipeline)? {
        "auto-ortho-region.rsbox" => "Z:\\job\\outputs\\auto-ortho-region.rsbox",
        "density-ortho-region.rsbox" => "Z:\\job\\outputs\\density-ortho-region.rsbox",
        _ => return None,
    })
}

fn ortho_region_box_filename(pipeline: &RealityScanPipeline) -> Option<&'static str> {
    if uses_auto_ortho_region_box(pipeline) {
        Some("auto-ortho-region.rsbox")
    } else if uses_density_ortho_region_box(pipeline) {
        Some("density-ortho-region.rsbox")
    } else {
        None
    }
}

fn ortho_projection_params_xml(pipeline: &RealityScanPipeline) -> anyhow::Result<Option<String>> {
    let Some(raw_xml) = pipeline.ortho_projection_params_xml.as_deref() else {
        return Ok(None);
    };
    let mut xml = raw_xml.trim().to_string();
    if xml.is_empty() {
        return Ok(None);
    }
    if xml.contains("\nXML\n") || xml.starts_with("XML\n") || xml.ends_with("\nXML") {
        anyhow::bail!("ortho_projection_params_xml cannot contain the heredoc delimiter XML");
    }
    if !xml.contains("<OrthoProjection") || !xml.contains("<ReconstructionRegion") {
        anyhow::bail!(
            "ortho_projection_params_xml must include OrthoProjection and ReconstructionRegion"
        );
    }
    if let Some(method) = &pipeline.ortho_render_method {
        xml = replace_xml_attribute_in_tag(
            &xml,
            "OrthoProjection",
            "colorType",
            realityscan_ortho_color_type(method),
        )?;
    }
    xml = replace_xml_attribute_in_tag(&xml, "OrthoProjection", "projectionType", "3")?;
    xml = replace_xml_attribute_in_tag(&xml, "OrthoProjection", "bEmpty", "0")?;
    xml = remove_xml_attribute_in_tag(&xml, "OrthoProjection", "modelGuid")?;
    xml = remove_xml_attribute_in_tag(&xml, "Residual", "ownerId")?;
    if let Some(pixel_size) = pipeline.ortho_pixel_size_meters {
        if let Some((width_meters, height_meters)) = parse_reconstruction_region_footprint(&xml) {
            let width = (width_meters / pixel_size).ceil().max(1.0) as u64;
            let height = (height_meters / pixel_size).ceil().max(1.0) as u64;
            xml =
                replace_xml_attribute_in_tag(&xml, "OrthoProjection", "width", &width.to_string())?;
            xml = replace_xml_attribute_in_tag(
                &xml,
                "OrthoProjection",
                "height",
                &height.to_string(),
            )?;
        }
    }
    if !xml.ends_with('\n') {
        xml.push('\n');
    }
    Ok(Some(xml))
}

fn generated_ortho_projection_params_xml(
    pipeline: &RealityScanPipeline,
    region_xml: &str,
) -> anyhow::Result<String> {
    let pixel_size = pipeline
        .ortho_pixel_size_meters
        .filter(|value| value.is_finite() && *value > 0.0)
        .context("ortho_pixel_size_meters must be greater than zero")?;
    let method = pipeline
        .ortho_render_method
        .as_ref()
        .context("generated ortho projection parameters require ortho_render_method")?;
    let mut region_xml = region_xml.trim().to_string();
    if !region_xml.contains("<ReconstructionRegion") {
        anyhow::bail!("exported reconstruction region is missing ReconstructionRegion XML");
    }
    let (width_meters, height_meters) = parse_reconstruction_region_footprint(&region_xml)
        .with_context(|| "exported reconstruction region is missing positive width/height/depth")?;
    let width = (width_meters / pixel_size).ceil().max(1.0) as u64;
    let height = (height_meters / pixel_size).ceil().max(1.0) as u64;
    region_xml = remove_xml_attribute_in_tag(&region_xml, "Residual", "ownerId")?;
    if !region_xml.ends_with('\n') {
        region_xml.push('\n');
    }

    Ok(format!(
        r#"<OrthoProjection width="{width}" height="{height}" name="Ortho projection 1" modelName="Model 1"
   colorType="{}" boxSideConerIndex="21" bEmpty="0" backFaceColorType="1" backFaceColor="2130706687"
   projectionType="3" bShowOrthoProjection="1">
  <Header magic="5787472" version="2"/>
</OrthoProjection>
{}"#,
        realityscan_ortho_color_type(method),
        region_xml
    ))
}

fn realityscan_ortho_color_type(method: &OrthoRenderMethod) -> &'static str {
    match method {
        OrthoRenderMethod::TrueOrthoTexturing => "texturing",
        OrthoRenderMethod::TrueOrthoColoring => "coloring",
        OrthoRenderMethod::ImageMosaicingGeneral => "general mosaicing",
        OrthoRenderMethod::ImageMosaicingAerial => "aerial mosaicing",
    }
}

fn ortho_region_scale_command(pipeline: &RealityScanPipeline) -> anyhow::Result<Option<String>> {
    let Some((width, height, depth)) = explicit_ortho_region_dimensions(pipeline)?.or_else(|| {
        let raw_xml = pipeline.ortho_projection_params_xml.as_deref()?;
        parse_reconstruction_region_dimensions(raw_xml)
    }) else {
        return Ok(None);
    };

    Ok(Some(format!(
        "-scaleReconstructionRegion {} {} {} center absolute",
        format_decimal(width),
        format_decimal(height),
        format_decimal(depth)
    )))
}

fn explicit_ortho_region_dimensions(
    pipeline: &RealityScanPipeline,
) -> anyhow::Result<Option<(f64, f64, f64)>> {
    let Some(settings) = pipeline.runtime_settings.as_ref() else {
        return Ok(None);
    };
    let values = [
        settings.ortho_region_width_meters,
        settings.ortho_region_height_meters,
        settings.ortho_region_depth_meters,
    ];
    let supplied = values.iter().filter(|value| value.is_some()).count();
    if supplied == 0 {
        return Ok(None);
    }
    if supplied != 3 {
        anyhow::bail!("ortho region width, height, and depth must be supplied together");
    }
    let width = values[0].unwrap();
    let height = values[1].unwrap();
    let depth = values[2].unwrap();
    if !width.is_finite()
        || width <= 0.0
        || !height.is_finite()
        || height <= 0.0
        || !depth.is_finite()
        || depth <= 0.0
    {
        anyhow::bail!("ortho region dimensions must be finite positive meter values");
    }
    Ok(Some((width, height, depth)))
}

fn parse_reconstruction_region_footprint(xml: &str) -> Option<(f64, f64)> {
    let (width, height, _) = parse_reconstruction_region_dimensions(xml)?;
    Some((width, height))
}

fn parse_reconstruction_region_dimensions(xml: &str) -> Option<(f64, f64, f64)> {
    let values_text = if let Some(value_start) = xml.find("widthHeightDepth=\"") {
        let value_start = value_start + "widthHeightDepth=\"".len();
        let value_end = value_start + xml[value_start..].find('"')?;
        &xml[value_start..value_end]
    } else {
        let value_start = xml.find("<widthHeightDepth>")? + "<widthHeightDepth>".len();
        let value_end = value_start + xml[value_start..].find("</widthHeightDepth>")?;
        &xml[value_start..value_end]
    };
    let values: Vec<f64> = values_text
        .split_whitespace()
        .filter_map(|value| value.parse::<f64>().ok())
        .collect();
    let width = *values.first()?;
    let height = *values.get(1)?;
    let depth = *values.get(2)?;
    (width.is_finite()
        && width > 0.0
        && height.is_finite()
        && height > 0.0
        && depth.is_finite()
        && depth > 0.0)
        .then_some((width, height, depth))
}

fn replace_xml_attribute_in_tag(
    xml: &str,
    tag: &str,
    attr: &str,
    value: &str,
) -> anyhow::Result<String> {
    let tag_start = xml
        .find(&format!("<{tag}"))
        .with_context(|| format!("missing XML tag {tag}"))?;
    let relative_tag_end = xml[tag_start..]
        .find('>')
        .with_context(|| format!("unterminated XML tag {tag}"))?;
    let tag_end = tag_start + relative_tag_end;
    let tag_contents = &xml[tag_start..tag_end];
    let attr_marker = format!("{attr}=\"");
    if let Some(relative_attr_start) = tag_contents.find(&attr_marker) {
        let value_start = tag_start + relative_attr_start + attr_marker.len();
        let value_end = value_start
            + xml[value_start..]
                .find('"')
                .with_context(|| format!("unterminated XML attribute {attr}"))?;
        let mut updated = String::with_capacity(xml.len() + value.len());
        updated.push_str(&xml[..value_start]);
        updated.push_str(value);
        updated.push_str(&xml[value_end..]);
        return Ok(updated);
    }

    let mut updated = String::with_capacity(xml.len() + attr.len() + value.len() + 4);
    updated.push_str(&xml[..tag_end]);
    updated.push(' ');
    updated.push_str(attr);
    updated.push_str("=\"");
    updated.push_str(value);
    updated.push('"');
    updated.push_str(&xml[tag_end..]);
    Ok(updated)
}

fn remove_xml_attribute_in_tag(xml: &str, tag: &str, attr: &str) -> anyhow::Result<String> {
    let Some(tag_start) = xml.find(&format!("<{tag}")) else {
        return Ok(xml.to_string());
    };
    let relative_tag_end = xml[tag_start..]
        .find('>')
        .with_context(|| format!("unterminated XML tag {tag}"))?;
    let tag_end = tag_start + relative_tag_end;
    let tag_contents = &xml[tag_start..tag_end];
    let attr_marker = format!("{attr}=\"");
    let Some(relative_attr_start) = tag_contents.find(&attr_marker) else {
        return Ok(xml.to_string());
    };
    let mut attr_start = tag_start + relative_attr_start;
    while attr_start > tag_start && xml.as_bytes()[attr_start - 1].is_ascii_whitespace() {
        attr_start -= 1;
    }
    let value_start = tag_start + relative_attr_start + attr_marker.len();
    let value_end = value_start
        + xml[value_start..]
            .find('"')
            .with_context(|| format!("unterminated XML attribute {attr}"))?
        + 1;
    let mut updated = String::with_capacity(xml.len());
    updated.push_str(&xml[..attr_start]);
    updated.push_str(&xml[value_end..]);
    Ok(updated)
}

fn realityscan_stage_commands(
    stage: &RealityScanStage,
    pipeline: &RealityScanPipeline,
    manifest: &JobInputManifest,
) -> anyhow::Result<Vec<String>> {
    match stage {
        RealityScanStage::SetIntrinsics => realityscan_intrinsics_commands(manifest),
        RealityScanStage::Align => {
            let mut commands = realityscan_input_alignment_commands(pipeline);
            commands.push("-align".to_string());
            Ok(commands)
        }
        RealityScanStage::SelectMaximalComponent => Ok(vec!["-selectMaximalComponent".to_string()]),
        RealityScanStage::SetReconstructionRegionAuto if uses_auto_ortho_region_box(pipeline) => {
            let mut commands = vec!["-setReconstructionRegionAuto".to_string()];
            if let Some(command) = ortho_region_scale_command(pipeline)? {
                commands.push(command);
            }
            commands.push(format!(
                "-exportReconstructionRegion {}",
                rscmd_quote("Z:\\job\\outputs\\auto-ortho-region.rsbox")
            ));
            Ok(commands)
        }
        RealityScanStage::SetReconstructionRegionAuto => {
            Ok(vec!["-setReconstructionRegionAuto".to_string()])
        }
        RealityScanStage::SetReconstructionRegionByDensity
            if has_ortho_projection_params(pipeline) =>
        {
            let mut commands = vec!["-setReconstructionRegionByDensity".to_string()];
            if let Some(command) = ortho_region_scale_command(pipeline)? {
                commands.push(command);
            }
            commands.push(format!(
                "-exportReconstructionRegion {}",
                rscmd_quote("Z:\\job\\outputs\\density-ortho-region.rsbox")
            ));
            Ok(commands)
        }
        RealityScanStage::SetReconstructionRegionByDensity => {
            Ok(vec!["-setReconstructionRegionByDensity".to_string()])
        }
        RealityScanStage::CalculatePreviewModel => Ok(vec!["-calculatePreviewModel".to_string()]),
        RealityScanStage::CalculateNormalModel => Ok(vec!["-calculateNormalModel".to_string()]),
        RealityScanStage::CalculateHighModel => Ok(vec!["-calculateHighModel".to_string()]),
        RealityScanStage::ContinueModelCalculation => {
            Ok(vec!["-continueModelCalculation".to_string()])
        }
        RealityScanStage::CorrectColors => Ok(vec!["-correctColors".to_string()]),
        RealityScanStage::CalculateTexture => Ok(vec!["-calculateTexture".to_string()]),
        RealityScanStage::CalculateOrthoProjection if has_ortho_projection_params(pipeline) => {
            let mut command = format!(
                "-calculateOrthoProjection {}",
                rscmd_quote("Z:\\job\\outputs\\calculate-ortho.rsortho")
            );
            if let Some(region_box_path) = ortho_region_box_path(pipeline) {
                command.push(' ');
                command.push_str(&rscmd_quote(region_box_path));
            }
            Ok(vec![command])
        }
        RealityScanStage::CalculateOrthoProjection => {
            Ok(vec!["-calculateOrthoProjection".to_string()])
        }
        RealityScanStage::ExportOrthoProjection => Ok(vec![format!(
            "-exportOrthoProjection {} {}",
            rscmd_quote(&windows_output_path(
                pipeline
                    .orthomosaic_filename
                    .as_deref()
                    .unwrap_or("orthomosaic.tif")
            )),
            rscmd_quote("Z:\\job\\outputs\\export-ortho-config.xml")
        )]),
        RealityScanStage::SaveProject => Ok(vec![format!(
            "-save {}",
            rscmd_quote(&windows_output_path(&pipeline.project_filename))
        )]),
    }
}

fn realityscan_alignment_setting_commands(pipeline: &RealityScanPipeline) -> Vec<String> {
    let Some(settings) = &pipeline.alignment_settings else {
        return Vec::new();
    };

    let mut pairs = Vec::<(&'static str, String)>::new();
    push_string_setting(
        &mut pairs,
        "sfmFeatureDetectionQuality",
        settings.feature_detection_quality.as_deref(),
    );
    push_u32_setting(
        &mut pairs,
        "sfmMaxFeaturesPerMpx",
        settings.max_features_per_mpx,
    );
    push_u32_setting(
        &mut pairs,
        "sfmMaxFeaturesPerImage",
        settings.max_features_per_image,
    );
    push_string_setting(
        &mut pairs,
        "sfmImagesOverlap",
        settings.images_overlap.as_deref(),
    );
    push_u32_setting(
        &mut pairs,
        "sfmImageDownscaleFactor",
        settings.image_downscale_factor,
    );
    push_float_setting(
        &mut pairs,
        "sfmMaxFeatureReprojectionError",
        settings.max_feature_reprojection_error,
    );
    push_string_setting(
        &mut pairs,
        "sfmDetectorSensitivity",
        settings.detector_sensitivity.as_deref(),
    );
    push_u32_setting(
        &mut pairs,
        "sfmPreselectorFeatures",
        settings.preselector_features,
    );
    push_bool_setting(
        &mut pairs,
        "sfmForceComponentRematch",
        settings.force_component_rematch,
    );
    push_bool_setting(
        &mut pairs,
        "sfmMergeGeoreferencedComponents",
        settings.merge_georeferenced_components,
    );
    push_bool_setting(
        &mut pairs,
        "sfmEnableCameraPrior",
        settings.enable_camera_prior,
    );
    push_float_setting(
        &mut pairs,
        "sfmCameraPriorAccuracyX",
        settings.camera_prior_accuracy_x,
    );
    push_float_setting(
        &mut pairs,
        "sfmCameraPriorAccuracyY",
        settings.camera_prior_accuracy_y,
    );
    push_float_setting(
        &mut pairs,
        "sfmCameraPriorAccuracyZ",
        settings.camera_prior_accuracy_z,
    );
    push_float_setting(
        &mut pairs,
        "sfmCameraPriorWeight",
        settings.camera_prior_weight,
    );
    push_float_setting(
        &mut pairs,
        "sfmCameraPriorAccuracyYaw",
        settings.camera_prior_accuracy_yaw,
    );
    push_float_setting(
        &mut pairs,
        "sfmCameraPriorAccuracyPitch",
        settings.camera_prior_accuracy_pitch,
    );
    push_float_setting(
        &mut pairs,
        "sfmCameraPriorAccuracyRoll",
        settings.camera_prior_accuracy_roll,
    );
    push_float_setting(
        &mut pairs,
        "sfmCameraPriorWeightOrientation",
        settings.camera_prior_weight_orientation,
    );

    pairs
        .into_iter()
        .map(|(key, value)| format!("-set {}", rscmd_quote(&format!("{key}={value}"))))
        .collect()
}

fn realityscan_input_alignment_commands(pipeline: &RealityScanPipeline) -> Vec<String> {
    let Some(settings) = &pipeline.alignment_settings else {
        return Vec::new();
    };

    let mut pairs = Vec::<(&'static str, String)>::new();
    push_u8_setting(
        &mut pairs,
        "inpPosePriorRelative",
        settings.input_relative_pose,
    );
    push_u8_setting(&mut pairs, "inpPose", settings.input_absolute_pose);
    push_u8_setting(
        &mut pairs,
        "inpPriorAccuracyInh",
        settings.input_prior_accuracy_source,
    );
    push_float_setting(&mut pairs, "inpuTx", settings.input_position_accuracy_x);
    push_float_setting(&mut pairs, "inpuTy", settings.input_position_accuracy_y);
    push_float_setting(&mut pairs, "inpuTz", settings.input_position_accuracy_z);
    push_float_setting(&mut pairs, "inpuRx", settings.input_yaw_accuracy);
    push_float_setting(&mut pairs, "inpuRy", settings.input_pitch_accuracy);
    push_float_setting(&mut pairs, "inpuRz", settings.input_roll_accuracy);

    if pairs.is_empty() {
        return Vec::new();
    }

    let mut commands = vec!["-selectAllImages".to_string()];
    commands.extend(edit_input_selection_commands(&pairs));
    commands
}

fn realityscan_intrinsics_commands(manifest: &JobInputManifest) -> anyhow::Result<Vec<String>> {
    let mut input_settings = Vec::new();
    let mut calibration_groups = HashMap::<String, i32>::new();
    let mut lens_groups = HashMap::<String, i32>::new();
    let mut next_calibration_group = 1_i32;
    let mut next_lens_group = 1_i32;

    for input in &manifest.inputs {
        let Some(intrinsics) = &input.camera_intrinsics else {
            continue;
        };
        let settings = input_intrinsics_settings(
            input,
            intrinsics,
            &mut calibration_groups,
            &mut next_calibration_group,
            &mut lens_groups,
            &mut next_lens_group,
        );
        if settings.is_empty() {
            continue;
        }
        input_settings.push((input, settings));
    }

    if let Some((_, first_settings)) = input_settings.first() {
        if input_settings.len() == manifest.inputs.len()
            && input_settings
                .iter()
                .all(|(_, settings)| settings == first_settings)
        {
            let mut commands = vec!["-selectAllImages".to_string()];
            commands.extend(edit_input_selection_commands(first_settings));
            return Ok(commands);
        }
    }

    let mut commands = Vec::new();
    for (input, settings) in input_settings {
        commands.push(format!(
            "-selectImage {} set",
            rscmd_quote(&windows_input_path(&input.filename))
        ));
        commands.extend(edit_input_selection_commands(&settings));
    }

    Ok(commands)
}

fn edit_input_selection_commands(settings: &[(&'static str, String)]) -> Vec<String> {
    settings
        .iter()
        .map(|(key, value)| {
            format!(
                "-editInputSelection {}",
                rscmd_quote(&format!("{key}={value}"))
            )
        })
        .collect()
}

fn input_intrinsics_settings(
    input: &CloudfrontInput,
    intrinsics: &CameraIntrinsics,
    calibration_groups: &mut HashMap<String, i32>,
    next_calibration_group: &mut i32,
    lens_groups: &mut HashMap<String, i32>,
    next_lens_group: &mut i32,
) -> Vec<(&'static str, String)> {
    let mut settings = Vec::new();
    let has_calibration_parameters = has_calibration_parameters(intrinsics);
    if has_calibration_values(intrinsics) {
        if let Some(group) = intrinsics.calibration_group.or_else(|| {
            has_calibration_parameters.then(|| {
                stable_group_id(
                    calibration_groups,
                    next_calibration_group,
                    &calibration_fingerprint(input, intrinsics),
                )
            })
        }) {
            settings.push(("inpCalibrationGroup", group.to_string()));
        }
        if intrinsics.calibration_prior.is_some() || has_calibration_parameters {
            settings.push(("inpCalibration", "1".to_string()));
        }
        push_float_setting(&mut settings, "inpFocal", intrinsics.focal_length_35mm);
        push_float_setting(&mut settings, "inpPPX", intrinsics.principal_point_x_mm);
        push_float_setting(&mut settings, "inpPPY", intrinsics.principal_point_y_mm);
        push_float_setting(&mut settings, "inpSkew", intrinsics.skew);
    }

    let has_distortion_parameters = has_distortion_parameters(intrinsics);
    if has_distortion_values(intrinsics) {
        if let Some(group) = intrinsics.lens_group.or_else(|| {
            has_distortion_parameters.then(|| {
                stable_group_id(
                    lens_groups,
                    next_lens_group,
                    &lens_fingerprint(input, intrinsics),
                )
            })
        }) {
            settings.push(("inpLensGroup", group.to_string()));
        }
        if intrinsics.distortion_prior.is_some() || has_distortion_parameters {
            settings.push(("inpDistortion", "1".to_string()));
        }
        if let Some(model) = intrinsics.distortion_model {
            settings.push(("inpDistortionModel", model.to_string()));
        }
        push_float_setting(&mut settings, "inpRadial1", intrinsics.radial_1);
        push_float_setting(&mut settings, "inpRadial2", intrinsics.radial_2);
        push_float_setting(&mut settings, "inpRadial3", intrinsics.radial_3);
        push_float_setting(&mut settings, "inpRadial4", intrinsics.radial_4);
        push_float_setting(&mut settings, "inpTangential1", intrinsics.tangential_1);
        push_float_setting(&mut settings, "inpTangential2", intrinsics.tangential_2);
    }

    settings
}

fn push_float_setting(
    settings: &mut Vec<(&'static str, String)>,
    key: &'static str,
    value: Option<f64>,
) {
    let Some(value) = value else {
        return;
    };
    if value.is_finite() {
        settings.push((key, format_decimal(value)));
    }
}

fn push_u32_setting(
    settings: &mut Vec<(&'static str, String)>,
    key: &'static str,
    value: Option<u32>,
) {
    if let Some(value) = value {
        settings.push((key, value.to_string()));
    }
}

fn push_u8_setting(
    settings: &mut Vec<(&'static str, String)>,
    key: &'static str,
    value: Option<u8>,
) {
    if let Some(value) = value {
        settings.push((key, value.to_string()));
    }
}

fn push_bool_setting(
    settings: &mut Vec<(&'static str, String)>,
    key: &'static str,
    value: Option<bool>,
) {
    if let Some(value) = value {
        settings.push((key, value.to_string()));
    }
}

fn push_string_setting(
    settings: &mut Vec<(&'static str, String)>,
    key: &'static str,
    value: Option<&str>,
) {
    if let Some(value) = value.filter(|value| !value.trim().is_empty()) {
        settings.push((key, value.trim().to_string()));
    }
}

fn has_calibration_values(intrinsics: &CameraIntrinsics) -> bool {
    intrinsics.calibration_group.is_some()
        || intrinsics.calibration_prior.is_some()
        || has_calibration_parameters(intrinsics)
}

fn has_calibration_parameters(intrinsics: &CameraIntrinsics) -> bool {
    intrinsics.focal_length_35mm.is_some()
        || intrinsics.principal_point_x_mm.is_some()
        || intrinsics.principal_point_y_mm.is_some()
        || intrinsics.skew.is_some()
}

fn has_distortion_values(intrinsics: &CameraIntrinsics) -> bool {
    intrinsics.lens_group.is_some()
        || intrinsics.distortion_prior.is_some()
        || has_distortion_parameters(intrinsics)
}

fn has_distortion_parameters(intrinsics: &CameraIntrinsics) -> bool {
    intrinsics.distortion_model.is_some()
        || intrinsics.radial_1.is_some()
        || intrinsics.radial_2.is_some()
        || intrinsics.radial_3.is_some()
        || intrinsics.radial_4.is_some()
        || intrinsics.tangential_1.is_some()
        || intrinsics.tangential_2.is_some()
}

fn stable_group_id(groups: &mut HashMap<String, i32>, next_group: &mut i32, key: &str) -> i32 {
    if let Some(group) = groups.get(key) {
        return *group;
    }
    let group = *next_group;
    *next_group += 1;
    groups.insert(key.to_string(), group);
    group
}

fn calibration_fingerprint(input: &CloudfrontInput, intrinsics: &CameraIntrinsics) -> String {
    format!(
        "{}|{:?}|{:?}|{:?}|{:?}",
        intrinsics.camera_id.as_deref().unwrap_or(&input.asset_id),
        intrinsics.focal_length_35mm,
        intrinsics.principal_point_x_mm,
        intrinsics.principal_point_y_mm,
        intrinsics.skew
    )
}

fn lens_fingerprint(input: &CloudfrontInput, intrinsics: &CameraIntrinsics) -> String {
    format!(
        "{}|{:?}|{:?}|{:?}|{:?}|{:?}|{:?}|{:?}",
        intrinsics.camera_id.as_deref().unwrap_or(&input.asset_id),
        intrinsics.distortion_model,
        intrinsics.radial_1,
        intrinsics.radial_2,
        intrinsics.radial_3,
        intrinsics.radial_4,
        intrinsics.tangential_1,
        intrinsics.tangential_2
    )
}

fn windows_output_path(filename: &str) -> String {
    format!("Z:\\job\\outputs\\{filename}")
}

fn windows_input_path(filename: &str) -> String {
    format!("Z:\\job\\inputs\\{filename}")
}

fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

fn rscmd_quote(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\\\""))
}

fn format_decimal(value: f64) -> String {
    let formatted = format!("{value:.10}");
    formatted
        .trim_end_matches('0')
        .trim_end_matches('.')
        .to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CollectedOutputArtifact {
    artifact_id: String,
    filename: String,
    relative_path: String,
    content_type: Option<String>,
    sha256: String,
    size_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WorkerJobState {
    job_id: String,
    updated_at: chrono::DateTime<Utc>,
    output_artifacts: Vec<CollectedOutputArtifact>,
    #[serde(default)]
    completed: bool,
}

async fn collect_outputs(
    job: &PipelineJob,
    job_dir: &Path,
) -> anyhow::Result<Vec<CollectedOutputArtifact>> {
    emit(
        job_dir,
        &job.job_id,
        JobState::CollectingOutputs,
        "collecting outputs",
        70.0,
    )
    .await?;
    let outputs_dir = job_dir.join("outputs");
    package_output_directories(&outputs_dir).await?;
    let artifacts = if job.output_targets.is_empty() {
        validate_required_pipeline_outputs(&job.pipeline, &outputs_dir).await?;
        discover_direct_output_artifacts(&outputs_dir).await?
    } else {
        collect_targeted_output_artifacts(&outputs_dir, &job.output_targets).await?
    };
    write_worker_state(job_dir, &job.job_id, artifacts.clone(), false).await?;
    Ok(artifacts)
}

async fn validate_required_pipeline_outputs(
    pipeline: &RealityScanPipeline,
    outputs_dir: &Path,
) -> anyhow::Result<()> {
    for filename in required_pipeline_output_filenames(pipeline) {
        validate_output_filename(&filename)?;
        let path = outputs_dir.join(&filename);
        if !path.is_file() {
            return Err(anyhow!(
                "expected pipeline output {} was not found at {}",
                filename,
                path.display()
            ));
        }
    }
    Ok(())
}

fn required_pipeline_output_filenames(pipeline: &RealityScanPipeline) -> Vec<String> {
    let stages = effective_stages(pipeline);
    let mut filenames = Vec::new();
    if stages.contains(&RealityScanStage::ExportOrthoProjection) {
        push_required_pipeline_output(
            &mut filenames,
            pipeline
                .orthomosaic_filename
                .as_deref()
                .unwrap_or("orthomosaic.tif"),
        );
    }
    if stages.contains(&RealityScanStage::SaveProject) && !pipeline.project_filename.is_empty() {
        push_required_pipeline_output(&mut filenames, &pipeline.project_filename);
    }
    filenames
}

fn push_required_pipeline_output(filenames: &mut Vec<String>, filename: &str) {
    if !filenames.iter().any(|existing| existing == filename) {
        filenames.push(filename.to_string());
    }
}

async fn read_worker_state(job_dir: &Path) -> anyhow::Result<Option<WorkerJobState>> {
    match fs::read_to_string(job_dir.join("state.json")).await {
        Ok(raw) => Ok(Some(serde_json::from_str(&raw)?)),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

async fn write_worker_state(
    job_dir: &Path,
    job_id: &str,
    output_artifacts: Vec<CollectedOutputArtifact>,
    completed: bool,
) -> anyhow::Result<()> {
    write_json(
        &job_dir.join("state.json"),
        &WorkerJobState {
            job_id: job_id.to_string(),
            updated_at: now(),
            output_artifacts,
            completed,
        },
    )
    .await
}

async fn package_output_directories(outputs_dir: &Path) -> anyhow::Result<()> {
    let mut entries = fs::read_dir(outputs_dir).await?;
    let mut directories = Vec::new();
    while let Some(entry) = entries.next_entry().await? {
        if entry.metadata().await?.is_dir() {
            let dirname = entry
                .file_name()
                .to_str()
                .ok_or_else(|| anyhow!("output directory name is not valid UTF-8"))?
                .to_string();
            validate_output_filename(&dirname)?;
            directories.push((dirname, entry.path()));
        }
    }
    directories.sort_by(|left, right| left.0.cmp(&right.0));
    for (dirname, directory) in directories {
        let archive = outputs_dir.join(format!("{dirname}.zip"));
        let archive_for_blocking = archive.clone();
        let directory_for_blocking = directory.clone();
        tokio::task::spawn_blocking(move || {
            create_directory_zip(&directory_for_blocking, &archive_for_blocking)
        })
        .await??;
    }
    Ok(())
}

fn create_directory_zip(directory: &Path, archive: &Path) -> anyhow::Result<()> {
    let file = std::fs::File::create(archive)
        .with_context(|| format!("creating output archive {}", archive.display()))?;
    let mut writer = ZipWriter::new(file);
    let options = SimpleFileOptions::default()
        .compression_method(CompressionMethod::Deflated)
        .unix_permissions(0o644);
    add_directory_to_zip(directory, directory, &mut writer, options)?;
    writer.finish()?;
    Ok(())
}

fn add_directory_to_zip(
    root: &Path,
    directory: &Path,
    writer: &mut ZipWriter<std::fs::File>,
    options: SimpleFileOptions,
) -> anyhow::Result<()> {
    let mut entries = std::fs::read_dir(directory)?.collect::<Result<Vec<_>, _>>()?;
    entries.sort_by_key(|entry| entry.path());
    for entry in entries {
        let path = entry.path();
        let metadata = entry.metadata()?;
        if metadata.is_dir() {
            add_directory_to_zip(root, &path, writer, options)?;
        } else if metadata.is_file() {
            let relative = path
                .strip_prefix(root)?
                .to_string_lossy()
                .replace('\\', "/");
            writer.start_file(relative, options)?;
            let mut input = std::fs::File::open(&path)?;
            std::io::copy(&mut input, writer)?;
        }
    }
    Ok(())
}

async fn discover_direct_output_artifacts(
    outputs_dir: &Path,
) -> anyhow::Result<Vec<CollectedOutputArtifact>> {
    let mut artifacts = Vec::new();
    let mut entries = fs::read_dir(outputs_dir).await?;
    while let Some(entry) = entries.next_entry().await? {
        let metadata = entry.metadata().await?;
        if !metadata.is_file() {
            continue;
        }
        let filename = entry
            .file_name()
            .to_str()
            .ok_or_else(|| anyhow!("output filename is not valid UTF-8"))?
            .to_string();
        validate_output_filename(&filename)?;
        artifacts.push(describe_output_artifact(&filename, &filename, &entry.path()).await?);
    }
    artifacts.sort_by(|left, right| left.filename.cmp(&right.filename));
    Ok(artifacts)
}

async fn collect_targeted_output_artifacts(
    outputs_dir: &Path,
    targets: &[OutputUploadTarget],
) -> anyhow::Result<Vec<CollectedOutputArtifact>> {
    let mut artifacts = Vec::with_capacity(targets.len());
    for target in targets {
        validate_output_filename(&target.filename)?;
        let path = outputs_dir.join(&target.filename);
        if !path.is_file() {
            return Err(anyhow!(
                "expected output artifact {} was not found at {}",
                target.artifact_id,
                path.display()
            ));
        }
        artifacts
            .push(describe_output_artifact(&target.artifact_id, &target.filename, &path).await?);
    }
    Ok(artifacts)
}

async fn describe_output_artifact(
    artifact_id: &str,
    filename: &str,
    path: &Path,
) -> anyhow::Result<CollectedOutputArtifact> {
    let metadata = fs::metadata(path).await?;
    Ok(CollectedOutputArtifact {
        artifact_id: artifact_id.to_string(),
        filename: filename.to_string(),
        relative_path: format!("outputs/{filename}"),
        content_type: Some(infer_content_type(filename).to_string()),
        sha256: file_sha256(path).await?,
        size_bytes: metadata.len(),
    })
}

async fn upload_outputs(
    http: &Client,
    job: &PipelineJob,
    job_dir: &Path,
    outputs: &[CollectedOutputArtifact],
) -> anyhow::Result<()> {
    emit(
        job_dir,
        &job.job_id,
        JobState::UploadingOutputs,
        "uploading output artifacts",
        85.0,
    )
    .await?;
    if job.output_targets.is_empty() {
        for output in outputs {
            emit_artifact_uploaded(UploadedArtifact {
                job_id: job.job_id.clone(),
                artifact_id: output.artifact_id.clone(),
                filename: output.filename.clone(),
                storage_uri: Some(format!(
                    "file://{}",
                    job_dir.join(&output.relative_path).display()
                )),
                content_type: output.content_type.clone(),
                sha256: Some(output.sha256.clone()),
                size_bytes: Some(output.size_bytes),
            })
            .await?;
        }
        return Ok(());
    }
    let outputs_by_artifact_id: HashMap<_, _> = outputs
        .iter()
        .map(|artifact| (artifact.artifact_id.as_str(), artifact))
        .collect();
    for target in &job.output_targets {
        let output = outputs_by_artifact_id
            .get(target.artifact_id.as_str())
            .ok_or_else(|| {
                anyhow!(
                    "collected output artifact {} is missing from upload set",
                    target.artifact_id
                )
            })?;
        let source = job_dir.join(&output.relative_path);
        if !source.is_file() {
            return Err(anyhow!(
                "collected output artifact {} is no longer present at {}",
                output.artifact_id,
                source.display()
            ));
        }
        let bytes = fs::read(&source)
            .await
            .with_context(|| format!("reading {}", source.display()))?;
        let sha256 = hex::encode(Sha256::digest(&bytes));
        let size_bytes = bytes.len() as u64;
        let upload_content_type = target.content_type.clone();
        let reported_content_type = target
            .content_type
            .clone()
            .or_else(|| output.content_type.clone());
        let mut request = http
            .request(target.method.parse()?, &target.url)
            .body(bytes);
        if let Some(content_type) = &upload_content_type {
            request = request.header("content-type", content_type);
        }
        for header in &target.headers {
            request = request.header(&header.name, &header.value);
        }
        request.send().await?.error_for_status()?;
        emit_artifact_uploaded(UploadedArtifact {
            job_id: job.job_id.clone(),
            artifact_id: target.artifact_id.clone(),
            filename: target.filename.clone(),
            storage_uri: target.storage_uri.clone(),
            content_type: reported_content_type,
            sha256: Some(sha256),
            size_bytes: Some(size_bytes),
        })
        .await?;
    }
    Ok(())
}

fn validate_output_filename(filename: &str) -> anyhow::Result<()> {
    if filename.trim().is_empty()
        || filename == "."
        || filename == ".."
        || filename.contains('/')
        || filename.contains('\\')
    {
        return Err(anyhow!("invalid output filename: {filename}"));
    }
    Ok(())
}

fn infer_content_type(filename: &str) -> &'static str {
    match filename
        .rsplit('.')
        .next()
        .unwrap_or("")
        .to_ascii_lowercase()
        .as_str()
    {
        "txt" | "log" => "text/plain",
        "json" => "application/json",
        "rsproj" => "application/octet-stream",
        "obj" => "model/obj",
        "ply" => "model/ply",
        "las" | "laz" => "application/octet-stream",
        "e57" => "application/octet-stream",
        "tif" | "tiff" => "image/tiff",
        "jpg" | "jpeg" => "image/jpeg",
        "png" => "image/png",
        "zip" => "application/zip",
        _ => "application/octet-stream",
    }
}

async fn file_sha256(path: &Path) -> anyhow::Result<String> {
    let mut file = fs::File::open(path).await?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0_u8; 1024 * 1024];
    loop {
        let read = file.read(&mut buffer).await?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hex::encode(hasher.finalize()))
}

async fn write_json(path: &Path, value: &impl serde::Serialize) -> anyhow::Result<()> {
    let data = serde_json::to_string_pretty(value)?;
    fs::write(path, data).await?;
    Ok(())
}

async fn emit(
    job_dir: &Path,
    job_id: &str,
    state: JobState,
    message: &str,
    progress: f32,
) -> anyhow::Result<()> {
    emit_with_details(job_dir, job_id, state, message, progress, None).await
}

async fn emit_with_details(
    job_dir: &Path,
    job_id: &str,
    state: JobState,
    message: &str,
    progress: f32,
    details: Option<JobEventDetails>,
) -> anyhow::Result<()> {
    let event = JobEvent {
        job_id: job_id.to_string(),
        state,
        message: message.to_string(),
        progress,
        observed_at: now(),
        details,
    };
    let line = serde_json::to_string(&event)?;
    println!("{line}");
    let _ = io::stdout().flush();
    append_job_event(job_dir, &line).await?;
    Ok(())
}

async fn append_job_event(job_dir: &Path, line: &str) -> anyhow::Result<()> {
    let path = job_dir.join("logs").join(JOB_EVENTS_LOG);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).await?;
    }
    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .await?;
    file.write_all(line.as_bytes()).await?;
    file.write_all(b"\n").await?;
    file.flush().await?;
    Ok(())
}

async fn emit_artifact_uploaded(artifact: UploadedArtifact) -> anyhow::Result<()> {
    println!("{}", serde_json::to_string(&artifact)?);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rslogic_protocol::{
        CameraIntrinsics, CloudfrontInput, RealityScanAlignmentSettings,
        RealityScanRuntimeSettings, UploadHeader,
    };
    use std::{
        io::{Read, Write},
        net::TcpListener,
        sync::mpsc,
        thread,
        time::Duration as StdDuration,
    };

    #[test]
    fn output_filename_rejects_path_traversal() {
        assert!(validate_output_filename("preview-ortho.rsproj").is_ok());
        assert!(validate_output_filename("../preview-ortho.rsproj").is_err());
        assert!(validate_output_filename("nested/preview-ortho.rsproj").is_err());
        assert!(validate_output_filename("").is_err());
    }

    #[test]
    fn content_type_is_inferred_from_filename() {
        assert_eq!(infer_content_type("model.obj"), "model/obj");
        assert_eq!(infer_content_type("preview.tif"), "image/tiff");
        assert_eq!(
            infer_content_type("unknown.bin"),
            "application/octet-stream"
        );
    }

    #[tokio::test]
    async fn emit_appends_job_event_jsonl() {
        let temp = tempfile::tempdir().unwrap();
        emit(
            temp.path(),
            "job-1",
            JobState::RunningRealityscan,
            "RealityScan align",
            42.0,
        )
        .await
        .unwrap();

        let raw = fs::read_to_string(temp.path().join("logs").join(JOB_EVENTS_LOG))
            .await
            .unwrap();
        let lines: Vec<_> = raw.lines().collect();
        assert_eq!(lines.len(), 1);
        let event: JobEvent = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(event.job_id, "job-1");
        assert_eq!(event.state, JobState::RunningRealityscan);
        assert_eq!(event.message, "RealityScan align");
        assert_eq!(event.progress, 42.0);
    }

    #[test]
    fn realityscan_command_is_parsed_from_stdout_line() {
        assert_eq!(
            parse_realityscan_command("Executing command 'calculateNormalModel'"),
            Some("calculateNormalModel")
        );
        assert_eq!(
            parse_realityscan_command(
                "Executing command 'addFolder' with parameter 'Z:\\job\\inputs'"
            ),
            Some("addFolder")
        );
        assert_eq!(parse_realityscan_command("Detected 40000 features"), None);
    }

    #[test]
    fn realityscan_command_stage_id_maps_submitted_steps() {
        assert_eq!(
            realityscan_command_stage_id("calculateOrthoProjection", "outputs"),
            Some("calculate_ortho_projection")
        );
        assert_eq!(
            realityscan_command_stage_id("editInputSelection", "align-save"),
            Some("set_intrinsics")
        );
        assert_eq!(
            realityscan_command_stage_id("save", "outputs"),
            Some("save_project")
        );
        assert_eq!(realityscan_command_stage_id("save", "model-save"), None);
        assert_eq!(realityscan_command_stage_id("load", "outputs"), None);
    }

    #[test]
    fn realityscan_completion_is_parsed_from_stdout_line() {
        assert_eq!(
            parse_realityscan_completion(
                "Exporting Orthographic Projection completed in 0.029 seconds.",
                "outputs",
            ),
            Some(RealityScanCompletion {
                stage_id: "export_ortho_projection",
                phase_fraction: 0.88,
            })
        );
        assert_eq!(
            parse_realityscan_completion(
                "Calculating Orthographic Projection completed in 6.060 seconds.",
                "outputs",
            ),
            Some(RealityScanCompletion {
                stage_id: "calculate_ortho_projection",
                phase_fraction: 0.62,
            })
        );
        assert_eq!(
            parse_realityscan_completion("Loading Project completed", "outputs"),
            None
        );
        assert_eq!(
            parse_realityscan_completion(
                "Saving Project completed in 0.535 seconds.",
                "align-save"
            ),
            None
        );
        assert_eq!(
            parse_realityscan_completion("Saving Project completed in 0.535 seconds.", "outputs"),
            Some(RealityScanCompletion {
                stage_id: "save_project",
                phase_fraction: 0.98,
            })
        );
    }

    #[test]
    fn realityscan_heartbeat_stale_detection_requires_stdout_and_outputs() {
        assert!(realityscan_phase_is_stale(
            Some(REALITYSCAN_PHASE_STALE_SECS),
            Some(REALITYSCAN_PHASE_STALE_SECS + 1)
        ));
        assert!(!realityscan_phase_is_stale(
            Some(REALITYSCAN_PHASE_STALE_SECS),
            Some(30)
        ));
        assert!(!realityscan_phase_is_stale(
            None,
            Some(REALITYSCAN_PHASE_STALE_SECS)
        ));
    }

    #[test]
    fn phase_fraction_tracker_only_moves_forward() {
        let phase_fraction = AtomicU32::new(0);
        record_phase_fraction(&phase_fraction, 0.88);
        record_phase_fraction(&phase_fraction, 0.05);
        assert!(
            (phase_fraction_from_units(phase_fraction.load(Ordering::Relaxed)) - 0.88).abs()
                < 0.001
        );
    }

    #[test]
    fn realityscan_emits_intrinsics_command_events() {
        assert!(should_emit_realityscan_command("selectAllImages"));
        assert!(should_emit_realityscan_command("selectImage"));
        assert!(should_emit_realityscan_command("editInputSelection"));
    }

    #[test]
    fn realityscan_phase_details_keep_phase_out_of_stage_id() {
        let details = realityscan_phase_details(JobEventKind::Lifecycle, "outputs", 2, 3);
        assert_eq!(details.phase_id.as_deref(), Some("02-outputs"));
        assert_eq!(details.stage_id, None);
    }

    #[test]
    fn realityscan_instance_name_is_short_and_space_free() {
        assert_eq!(
            realityscan_instance_name("9939ec30-9435-46fd", 1, "model save"),
            "rslogic_9939ec30_01_model_save"
        );
    }

    #[test]
    fn realityscan_feature_progress_is_throttled() {
        assert!(should_emit_feature_progress(1, 2202));
        assert!(should_emit_feature_progress(50, 2202));
        assert!(should_emit_feature_progress(2202, 2202));
        assert!(!should_emit_feature_progress(49, 2202));
    }

    #[test]
    fn realityscan_phase_progress_maps_to_worker_range() {
        assert!((realityscan_phase_progress(0, 3, 0.0) - 40.0).abs() < 0.001);
        assert!((realityscan_phase_progress(2, 3, 1.0) - 80.0).abs() < 0.001);
    }

    #[test]
    fn realityscan_script_sets_intrinsics_before_alignment() {
        let manifest = JobInputManifest {
            job_id: "job-1".to_string(),
            expires_at: Utc::now() + chrono::Duration::hours(1),
            inputs: vec![CloudfrontInput {
                asset_id: "asset-1".to_string(),
                filename: "image-1.jpg".to_string(),
                url: "https://example.test/image-1.jpg".to_string(),
                sha256: None,
                size_bytes: None,
                camera_intrinsics: Some(CameraIntrinsics {
                    camera_id: Some("DJI-M4E-wide".to_string()),
                    calibration_prior: Some(2),
                    focal_length_35mm: Some(24.0),
                    principal_point_x_mm: Some(0.12),
                    principal_point_y_mm: Some(-0.08),
                    distortion_prior: Some(2),
                    distortion_model: Some(2),
                    radial_1: Some(-0.01),
                    radial_2: Some(0.001),
                    ..CameraIntrinsics::default()
                }),
            }],
        };
        let pipeline = RealityScanPipeline {
            template_id: "test".to_string(),
            stages: vec![RealityScanStage::SetIntrinsics, RealityScanStage::Align],
            project_filename: "aligned.rsproj".to_string(),
            resume_source_job_id: None,
            resume_project_filename: None,
            project_coordinate_system: None,
            output_coordinate_system: None,
            orthomosaic_filename: None,
            ortho_pixel_size_meters: None,
            ortho_render_method: None,
            ortho_projection_params_xml: None,
            alignment_settings: None,
            runtime_settings: None,
            single_session: false,
            print_progress_interval_seconds: None,
        };

        let script = realityscan_rscmd_script(&pipeline, &manifest).unwrap();

        let select_index = script.find("-selectAllImages").unwrap();
        let align_index = script.find("-align").unwrap();
        assert!(select_index < align_index);
        assert!(!script.contains("-selectImage \"Z:\\job\\inputs\\image-1.jpg\" set"));
        assert!(script.contains("-editInputSelection \"inpCalibrationGroup=1\""));
        assert!(script.contains("-editInputSelection \"inpCalibration=1\""));
        assert!(script.contains("-editInputSelection \"inpFocal=24\""));
        assert!(script.contains("-editInputSelection \"inpPPX=0.12\""));
        assert!(script.contains("-editInputSelection \"inpPPY=-0.08\""));
        assert!(script.contains("-editInputSelection \"inpLensGroup=1\""));
        assert!(script.contains("-editInputSelection \"inpDistortion=1\""));
        assert!(script.contains("-editInputSelection \"inpDistortionModel=2\""));
        assert!(script.contains("-editInputSelection \"inpRadial1=-0.01\""));
        assert!(script.contains("-editInputSelection \"inpRadial2=0.001\""));
    }

    #[test]
    fn realityscan_script_sets_coordinate_systems_before_import() {
        let manifest = JobInputManifest {
            job_id: "job-1".to_string(),
            expires_at: Utc::now() + chrono::Duration::hours(1),
            inputs: Vec::new(),
        };
        let pipeline = RealityScanPipeline {
            template_id: "utm".to_string(),
            stages: vec![RealityScanStage::Align],
            project_filename: "aligned.rsproj".to_string(),
            resume_source_job_id: None,
            resume_project_filename: None,
            project_coordinate_system: Some("epsg:32618".to_string()),
            output_coordinate_system: Some("epsg:32618".to_string()),
            orthomosaic_filename: None,
            ortho_pixel_size_meters: None,
            ortho_render_method: None,
            ortho_projection_params_xml: None,
            alignment_settings: None,
            runtime_settings: None,
            single_session: false,
            print_progress_interval_seconds: None,
        };

        let script = realityscan_rscmd_script(&pipeline, &manifest).unwrap();

        let project_index = script.find("-setProjectCoordinateSystem").unwrap();
        let output_index = script.find("-setOutputCoordinateSystem").unwrap();
        let add_folder_index = script.find("-addFolder \"Z:\\job\\inputs\"").unwrap();
        assert!(project_index < add_folder_index);
        assert!(output_index < add_folder_index);
        assert!(script.contains("-setProjectCoordinateSystem \"epsg:32618\""));
        assert!(script.contains("-setOutputCoordinateSystem \"epsg:32618\""));
    }

    #[test]
    fn realityscan_script_applies_aggressive_alignment_priors() {
        let manifest = JobInputManifest {
            job_id: "job-1".to_string(),
            expires_at: Utc::now() + chrono::Duration::hours(1),
            inputs: Vec::new(),
        };
        let pipeline = RealityScanPipeline {
            template_id: "aggressive".to_string(),
            stages: vec![RealityScanStage::Align],
            project_filename: "aligned.rsproj".to_string(),
            resume_source_job_id: None,
            resume_project_filename: None,
            project_coordinate_system: None,
            output_coordinate_system: None,
            orthomosaic_filename: None,
            ortho_pixel_size_meters: None,
            ortho_render_method: None,
            ortho_projection_params_xml: None,
            alignment_settings: Some(RealityScanAlignmentSettings {
                feature_detection_quality: Some("High".to_string()),
                max_features_per_mpx: Some(20_000),
                max_features_per_image: Some(80_000),
                images_overlap: Some("Low".to_string()),
                image_downscale_factor: Some(1),
                max_feature_reprojection_error: Some(3.0),
                detector_sensitivity: Some("Ultra".to_string()),
                preselector_features: Some(30_000),
                force_component_rematch: Some(true),
                merge_georeferenced_components: Some(true),
                enable_camera_prior: Some(true),
                camera_prior_accuracy_x: Some(1.0),
                camera_prior_accuracy_y: Some(1.0),
                camera_prior_accuracy_z: Some(3.0),
                camera_prior_weight: Some(0.25),
                camera_prior_accuracy_yaw: Some(45.0),
                camera_prior_accuracy_pitch: Some(45.0),
                camera_prior_accuracy_roll: Some(45.0),
                camera_prior_weight_orientation: Some(0.05),
                input_relative_pose: Some(0),
                input_absolute_pose: Some(1),
                input_prior_accuracy_source: Some(1),
                input_position_accuracy_x: Some(1.0),
                input_position_accuracy_y: Some(1.0),
                input_position_accuracy_z: Some(3.0),
                input_yaw_accuracy: Some(45.0),
                input_pitch_accuracy: Some(45.0),
                input_roll_accuracy: Some(45.0),
            }),
            runtime_settings: None,
            single_session: false,
            print_progress_interval_seconds: None,
        };

        let script = realityscan_rscmd_script(&pipeline, &manifest).unwrap();

        let settings_index = script
            .find("-set \"sfmFeatureDetectionQuality=High\"")
            .unwrap();
        let add_folder_index = script.find("-addFolder \"Z:\\job\\inputs\"").unwrap();
        let input_prior_index = script.find("-editInputSelection \"inpPose=1\"").unwrap();
        let align_index = script.find("-align").unwrap();
        assert!(settings_index < add_folder_index);
        assert!(add_folder_index < input_prior_index);
        assert!(input_prior_index < align_index);
        assert!(script.contains("-set \"sfmImagesOverlap=Low\""));
        assert!(script.contains("-set \"sfmDetectorSensitivity=Ultra\""));
        assert!(script.contains("-set \"sfmMaxFeaturesPerMpx=20000\""));
        assert!(script.contains("-set \"sfmMaxFeaturesPerImage=80000\""));
        assert!(script.contains("-set \"sfmMaxFeatureReprojectionError=3\""));
        assert!(script.contains("-set \"sfmForceComponentRematch=true\""));
        assert!(script.contains("-set \"sfmMergeGeoreferencedComponents=true\""));
        assert!(script.contains("-set \"sfmEnableCameraPrior=true\""));
        assert!(script.contains("-set \"sfmCameraPriorAccuracyX=1\""));
        assert!(script.contains("-set \"sfmCameraPriorAccuracyZ=3\""));
        assert!(script.contains("-set \"sfmCameraPriorWeight=0.25\""));
        assert!(script.contains("-set \"sfmCameraPriorWeightOrientation=0.05\""));
        assert!(script.contains("-editInputSelection \"inpPosePriorRelative=0\""));
        assert!(script.contains("-editInputSelection \"inpPriorAccuracyInh=1\""));
        assert!(script.contains("-editInputSelection \"inpuTx=1\""));
        assert!(script.contains("-editInputSelection \"inpuTz=3\""));
        assert!(script.contains("-editInputSelection \"inpuRx=45\""));
    }

    #[test]
    fn realityscan_script_uses_per_image_intrinsics_when_settings_differ() {
        let manifest = JobInputManifest {
            job_id: "job-1".to_string(),
            expires_at: Utc::now() + chrono::Duration::hours(1),
            inputs: vec![
                CloudfrontInput {
                    asset_id: "asset-1".to_string(),
                    filename: "image-1.jpg".to_string(),
                    url: "https://example.test/image-1.jpg".to_string(),
                    sha256: None,
                    size_bytes: None,
                    camera_intrinsics: Some(CameraIntrinsics {
                        camera_id: Some("camera-a".to_string()),
                        distortion_prior: Some(1),
                        distortion_model: Some(2),
                        radial_1: Some(-0.01),
                        ..CameraIntrinsics::default()
                    }),
                },
                CloudfrontInput {
                    asset_id: "asset-2".to_string(),
                    filename: "image-2.jpg".to_string(),
                    url: "https://example.test/image-2.jpg".to_string(),
                    sha256: None,
                    size_bytes: None,
                    camera_intrinsics: Some(CameraIntrinsics {
                        camera_id: Some("camera-b".to_string()),
                        distortion_prior: Some(1),
                        distortion_model: Some(2),
                        radial_1: Some(-0.02),
                        ..CameraIntrinsics::default()
                    }),
                },
            ],
        };
        let pipeline = RealityScanPipeline {
            template_id: "test".to_string(),
            stages: vec![RealityScanStage::SetIntrinsics, RealityScanStage::Align],
            project_filename: "aligned.rsproj".to_string(),
            resume_source_job_id: None,
            resume_project_filename: None,
            project_coordinate_system: None,
            output_coordinate_system: None,
            orthomosaic_filename: None,
            ortho_pixel_size_meters: None,
            ortho_render_method: None,
            ortho_projection_params_xml: None,
            alignment_settings: None,
            runtime_settings: None,
            single_session: false,
            print_progress_interval_seconds: None,
        };

        let script = realityscan_rscmd_script(&pipeline, &manifest).unwrap();

        assert!(!script.contains("-selectAllImages"));
        assert!(script.contains("-selectImage \"Z:\\job\\inputs\\image-1.jpg\" set"));
        assert!(script.contains("-editInputSelection \"inpRadial1=-0.01\""));
        assert!(script.contains("-selectImage \"Z:\\job\\inputs\\image-2.jpg\" set"));
        assert!(script.contains("-editInputSelection \"inpRadial1=-0.02\""));
    }

    #[test]
    fn realityscan_script_exports_ortho_as_5cm_bigtiff() {
        let manifest = JobInputManifest {
            job_id: "job-1".to_string(),
            expires_at: Utc::now() + chrono::Duration::hours(1),
            inputs: Vec::new(),
        };
        let pipeline = RealityScanPipeline {
            template_id: "test".to_string(),
            stages: vec![RealityScanStage::ExportOrthoProjection],
            project_filename: "ortho.rsproj".to_string(),
            resume_source_job_id: None,
            resume_project_filename: None,
            project_coordinate_system: None,
            output_coordinate_system: None,
            orthomosaic_filename: Some("seaforth-5cm-orthomosaic.tif".to_string()),
            ortho_pixel_size_meters: Some(0.05),
            ortho_render_method: None,
            ortho_projection_params_xml: None,
            alignment_settings: None,
            runtime_settings: None,
            single_session: false,
            print_progress_interval_seconds: None,
        };

        let launcher =
            realityscan_cli_script(&pipeline, "Z:\\job\\work\\commands.rscmd", &[]).unwrap();
        let script = realityscan_rscmd_script(&pipeline, &manifest).unwrap();

        assert!(launcher.contains(r#"<entry key="exportOrthoAsBigTiff" value="true"/>"#));
        assert!(launcher.contains(r#"<entry key="exportProjectionParametersFile" value="true"/>"#));
        assert!(launcher.contains(r#"<entry key="orthoPixelSize" value="0.05"/>"#));
        assert!(launcher
            .contains("-headless -silent 'Z:\\job\\logs\\realityscan-crash-reports' -stdConsole"));
        assert!(launcher.contains("-execRSCMD 'Z:\\job\\work\\commands.rscmd'"));
        assert!(script.contains(
            "-exportOrthoProjection \"Z:\\job\\outputs\\seaforth-5cm-orthomosaic.tif\" \"Z:\\job\\outputs\\export-ortho-config.xml\""
        ));
    }

    #[test]
    fn realityscan_script_uses_aerial_mosaicing_ortho_params_without_texture() {
        let manifest = JobInputManifest {
            job_id: "job-1".to_string(),
            expires_at: Utc::now() + chrono::Duration::hours(1),
            inputs: Vec::new(),
        };
        let ortho_params = r#"<OrthoProjection width="1000" height="1000" name="Ortho projection 1" modelName="Model 1"
   modelGuid="{0573054B-851B-4ED6-B7A9-CC9953656DB4}" colorType="coloring"
   boxSideConerIndex="21" bEmpty="1" backFaceColorType="1" backFaceColor="2130706687"
   projectionType="0" bShowOrthoProjection="1">
  <Header magic="5787472" version="2"/>
</OrthoProjection>
<ReconstructionRegion globalCoordinateSystem="+proj=longlat +datum=WGS84 +no_defs"
   globalCoordinateSystemName="epsg:4326 - GPS (WGS 84)" isGeoreferenced="1" isLatLon="1">
  <yawPitchRoll>0.000670706172590691 8.25126196690305e-05 0.000216933504214264</yawPitchRoll>
  <widthHeightDepth>10 12 3</widthHeightDepth>
  <Header magic="5395016" version="2"/>
  <Residual R="1 0 0 0 1 0 0 0 1" t="0 0 0" s="1" ownerId="{650355CD-CD02-4AA7-B5BE-6CE234F28984}"/>
</ReconstructionRegion>"#;
        let pipeline = RealityScanPipeline {
            template_id: "aerial".to_string(),
            stages: vec![
                RealityScanStage::CalculateOrthoProjection,
                RealityScanStage::ExportOrthoProjection,
                RealityScanStage::SaveProject,
            ],
            project_filename: "aerial.rsproj".to_string(),
            resume_source_job_id: None,
            resume_project_filename: None,
            project_coordinate_system: None,
            output_coordinate_system: None,
            orthomosaic_filename: Some("aerial-5cm.tif".to_string()),
            ortho_pixel_size_meters: Some(0.05),
            ortho_render_method: Some(OrthoRenderMethod::ImageMosaicingAerial),
            ortho_projection_params_xml: Some(ortho_params.to_string()),
            alignment_settings: None,
            runtime_settings: None,
            single_session: false,
            print_progress_interval_seconds: None,
        };

        let launcher =
            realityscan_cli_script(&pipeline, "Z:\\job\\work\\commands.rscmd", &[]).unwrap();
        let script = realityscan_rscmd_script(&pipeline, &manifest).unwrap();

        assert!(!script.contains("-calculateTexture"));
        assert!(script
            .contains("-calculateOrthoProjection \"Z:\\job\\outputs\\calculate-ortho.rsortho\""));
        assert!(launcher.contains(r#"colorType="aerial mosaicing""#));
        assert!(launcher.contains(r#"projectionType="3""#));
        assert!(launcher.contains(r#"bEmpty="0""#));
        assert!(launcher.contains(r#"width="200""#));
        assert!(launcher.contains(r#"height="240""#));
        assert!(!launcher.contains("modelGuid="));
        assert!(!launcher.contains("ownerId="));
        assert!(launcher.contains("cat > /job/outputs/calculate-ortho.rsortho"));
    }

    #[test]
    fn generated_ortho_params_use_gps_region_aerial_mosaicing_and_5cm_pixels() {
        let region_xml = r#"<ReconstructionRegion globalCoordinateSystem="+proj=utm +zone=18 +datum=WGS84 +units=m +no_defs"
   globalCoordinateSystemName="epsg:32618 - WGS 84 / UTM zone 18N" isGeoreferenced="1"
   isLatLon="0" widthHeightDepth="1060.8247146434 1478.68901436919 132.175109863992">
  <globalCoordinateSystemWkt>PROJCS["WGS_1984_UTM_Zone_18N"]</globalCoordinateSystemWkt>
  <Header magic="5395016" version="2"/>
  <CentreEuclid centre="364081.34750773 1982464.67667797 39.2872236669064"/>
  <Residual R="1 0 0 0 1 0 0 0 1" t="0 0 0" s="1" ownerId="{AB54BBCB-EE7D-4547-B48A-AE7DA0A598D5}"/>
</ReconstructionRegion>"#;
        let pipeline = RealityScanPipeline {
            template_id: "generated-aerial".to_string(),
            stages: vec![RealityScanStage::CalculateOrthoProjection],
            project_filename: "aerial.rsproj".to_string(),
            resume_source_job_id: None,
            resume_project_filename: None,
            project_coordinate_system: Some("epsg:32618".to_string()),
            output_coordinate_system: Some("epsg:32618".to_string()),
            orthomosaic_filename: Some("aerial-5cm.tif".to_string()),
            ortho_pixel_size_meters: Some(0.05),
            ortho_render_method: Some(OrthoRenderMethod::ImageMosaicingAerial),
            ortho_projection_params_xml: None,
            alignment_settings: None,
            runtime_settings: None,
            single_session: false,
            print_progress_interval_seconds: None,
        };

        let params = generated_ortho_projection_params_xml(&pipeline, region_xml).unwrap();

        assert!(params.contains(r#"width="21217""#));
        assert!(params.contains(r#"height="29574""#));
        assert!(params.contains(r#"colorType="aerial mosaicing""#));
        assert!(params.contains(r#"projectionType="3""#));
        assert!(params.contains(r#"bEmpty="0""#));
        assert!(params.contains("epsg:32618 - WGS 84 / UTM zone 18N"));
        assert!(!params.contains("ownerId="));
        assert!(!params.contains("texturing"));
    }

    #[test]
    fn generated_aerial_ortho_pipeline_exports_region_and_uses_high_model() {
        let manifest = JobInputManifest {
            job_id: "job-1".to_string(),
            expires_at: Utc::now() + chrono::Duration::hours(1),
            inputs: Vec::new(),
        };
        let pipeline = RealityScanPipeline {
            template_id: "generated-aerial".to_string(),
            stages: vec![
                RealityScanStage::SetIntrinsics,
                RealityScanStage::Align,
                RealityScanStage::SelectMaximalComponent,
                RealityScanStage::SetReconstructionRegionByDensity,
                RealityScanStage::CalculateHighModel,
                RealityScanStage::CorrectColors,
                RealityScanStage::CalculateOrthoProjection,
                RealityScanStage::ExportOrthoProjection,
                RealityScanStage::SaveProject,
            ],
            project_filename: "density-high-color-aerial-5cm.rsproj".to_string(),
            resume_source_job_id: None,
            resume_project_filename: None,
            project_coordinate_system: Some("epsg:32618".to_string()),
            output_coordinate_system: Some("epsg:32618".to_string()),
            orthomosaic_filename: Some("density-high-color-aerial-5cm.tif".to_string()),
            ortho_pixel_size_meters: Some(0.05),
            ortho_render_method: Some(OrthoRenderMethod::ImageMosaicingAerial),
            ortho_projection_params_xml: None,
            alignment_settings: None,
            runtime_settings: None,
            single_session: false,
            print_progress_interval_seconds: Some(60),
        };

        let phases = realityscan_phases(&pipeline, &manifest).unwrap();

        assert_eq!(phases.len(), 3);
        assert!(phases[0]
            .commands
            .iter()
            .any(|command| command.contains("-setProjectCoordinateSystem \"epsg:32618\"")));
        assert!(phases[0]
            .commands
            .iter()
            .any(|command| command.contains("-setOutputCoordinateSystem \"epsg:32618\"")));
        assert!(phases[1].commands.contains(&format!(
            "-exportReconstructionRegion {}",
            rscmd_quote("Z:\\job\\outputs\\density-ortho-region.rsbox")
        )));
        assert!(phases[1]
            .commands
            .contains(&"-calculateHighModel".to_string()));
        assert!(phases[1]
            .commands
            .contains(&"-save \"Z:\\job\\outputs\\modeled.rsproj\"".to_string()));
        assert!(!phases[1].commands.contains(&"-correctColors".to_string()));
        assert!(phases[2].commands.contains(&"-correctColors".to_string()));
        assert!(!phases.iter().any(|phase| phase
            .commands
            .contains(&"-calculatePreviewModel".to_string())));
        assert!(!phases
            .iter()
            .any(|phase| phase.commands.contains(&"-calculateTexture".to_string())));
        assert!(phases[2].commands.iter().any(|command| command.contains(
            "-calculateOrthoProjection \"Z:\\job\\outputs\\calculate-ortho.rsortho\" \"Z:\\job\\outputs\\density-ortho-region.rsbox\""
        )));
        assert!(phases[2].commands.iter().any(|command| command.contains(
            "-exportOrthoProjection \"Z:\\job\\outputs\\density-high-color-aerial-5cm.tif\""
        )));

        let align_launcher = realityscan_cli_script(
            &pipeline,
            "Z:\\job\\work\\00-align-save.rscmd",
            &phases[0].commands,
        )
        .unwrap();
        let model_launcher = realityscan_cli_script(
            &pipeline,
            "Z:\\job\\work\\01-model-save.rscmd",
            &phases[1].commands,
        )
        .unwrap();
        let output_launcher = realityscan_cli_script(
            &pipeline,
            "Z:\\job\\work\\02-outputs.rscmd",
            &phases[2].commands,
        )
        .unwrap();

        assert!(!align_launcher.contains("generate_ortho_projection_params_from_region"));
        assert!(!model_launcher.contains("generate_ortho_projection_params_from_region"));
        assert!(output_launcher.contains("generate_ortho_projection_params_from_region"));
        assert!(output_launcher.contains("region_path='/job/outputs/density-ortho-region.rsbox'"));
    }

    #[test]
    fn high_aerial_split_phases_declare_success_outputs() {
        let manifest = JobInputManifest {
            job_id: "job-1".to_string(),
            expires_at: Utc::now() + chrono::Duration::hours(1),
            inputs: Vec::new(),
        };
        let pipeline = RealityScanPipeline {
            template_id: "generated-aerial".to_string(),
            stages: vec![
                RealityScanStage::SetIntrinsics,
                RealityScanStage::Align,
                RealityScanStage::SelectMaximalComponent,
                RealityScanStage::SetReconstructionRegionByDensity,
                RealityScanStage::CalculateHighModel,
                RealityScanStage::CorrectColors,
                RealityScanStage::CalculateOrthoProjection,
                RealityScanStage::ExportOrthoProjection,
                RealityScanStage::SaveProject,
            ],
            project_filename: "density-high-color-aerial-5cm.rsproj".to_string(),
            resume_source_job_id: None,
            resume_project_filename: None,
            project_coordinate_system: Some("epsg:32618".to_string()),
            output_coordinate_system: Some("epsg:32618".to_string()),
            orthomosaic_filename: Some("density-high-color-aerial-5cm.tif".to_string()),
            ortho_pixel_size_meters: Some(0.05),
            ortho_render_method: Some(OrthoRenderMethod::ImageMosaicingAerial),
            ortho_projection_params_xml: None,
            alignment_settings: None,
            runtime_settings: None,
            single_session: false,
            print_progress_interval_seconds: Some(60),
        };
        let job_dir = PathBuf::from("/tmp/rslogic-job");
        let phases = realityscan_phases(&pipeline, &manifest).unwrap();

        let paths: Vec<Vec<PathBuf>> = phases
            .iter()
            .map(|phase| realityscan_phase_success_output_paths(phase, &pipeline, &job_dir))
            .collect();

        assert_eq!(
            paths[0],
            vec![job_dir.join("outputs").join("aligned.rsproj")]
        );
        assert_eq!(
            paths[1],
            vec![job_dir.join("outputs").join("modeled.rsproj")]
        );
        assert_eq!(
            paths[2],
            vec![
                job_dir
                    .join("outputs")
                    .join("density-high-color-aerial-5cm.rsproj"),
                job_dir
                    .join("outputs")
                    .join("density-high-color-aerial-5cm.tif"),
            ]
        );
    }

    #[test]
    fn generated_aerial_ortho_pipeline_can_scale_density_region() {
        let manifest = JobInputManifest {
            job_id: "job-1".to_string(),
            expires_at: Utc::now() + chrono::Duration::hours(1),
            inputs: Vec::new(),
        };
        let pipeline = RealityScanPipeline {
            template_id: "generated-aerial".to_string(),
            stages: vec![
                RealityScanStage::SetReconstructionRegionByDensity,
                RealityScanStage::CalculatePreviewModel,
                RealityScanStage::CalculateOrthoProjection,
                RealityScanStage::ExportOrthoProjection,
            ],
            project_filename: "density-preview-color-aerial-5cm.rsproj".to_string(),
            orthomosaic_filename: Some("density-preview-color-aerial-5cm.tif".to_string()),
            ortho_pixel_size_meters: Some(0.05),
            ortho_render_method: Some(OrthoRenderMethod::ImageMosaicingAerial),
            runtime_settings: Some(RealityScanRuntimeSettings {
                ortho_region_width_meters: Some(900.0),
                ortho_region_height_meters: Some(900.0),
                ortho_region_depth_meters: Some(150.0),
                ..RealityScanRuntimeSettings::default()
            }),
            ..RealityScanPipeline::default()
        };

        let phases = realityscan_phases(&pipeline, &manifest).unwrap();
        let commands: Vec<&String> = phases
            .iter()
            .flat_map(|phase| phase.commands.iter())
            .collect();
        let density_index = commands
            .iter()
            .position(|command| *command == "-setReconstructionRegionByDensity")
            .expect("sets density region");
        let scale_index = commands
            .iter()
            .position(|command| {
                *command == "-scaleReconstructionRegion 900 900 150 center absolute"
            })
            .expect("scales density region");
        let export_index = commands
            .iter()
            .position(|command| {
                *command
                    == &format!(
                        "-exportReconstructionRegion {}",
                        rscmd_quote("Z:\\job\\outputs\\density-ortho-region.rsbox")
                    )
            })
            .expect("exports scaled density region");

        assert!(density_index < scale_index);
        assert!(scale_index < export_index);
        assert!(commands.iter().any(|command| command.contains(
            "-calculateOrthoProjection \"Z:\\job\\outputs\\calculate-ortho.rsortho\" \"Z:\\job\\outputs\\density-ortho-region.rsbox\""
        )));
    }

    #[test]
    fn realityscan_script_passes_auto_region_box_to_ortho_params() {
        let manifest = JobInputManifest {
            job_id: "job-1".to_string(),
            expires_at: Utc::now() + chrono::Duration::hours(1),
            inputs: Vec::new(),
        };
        let ortho_params = r#"<OrthoProjection width="1000" height="1000" name="Ortho projection 1" modelName="Model 1"
   colorType="aerial mosaicing" projectionType="3" bShowOrthoProjection="1">
  <Header magic="5787472" version="2"/>
</OrthoProjection>
<ReconstructionRegion globalCoordinateSystem="+proj=longlat +datum=WGS84 +no_defs"
   globalCoordinateSystemName="epsg:4326 - GPS (WGS 84)" isGeoreferenced="1" isLatLon="1">
  <widthHeightDepth>10 12 3</widthHeightDepth>
  <Header magic="5395016" version="2"/>
</ReconstructionRegion>"#;
        let pipeline = RealityScanPipeline {
            template_id: "aerial".to_string(),
            stages: vec![
                RealityScanStage::SetReconstructionRegionAuto,
                RealityScanStage::CalculateNormalModel,
                RealityScanStage::CalculateOrthoProjection,
                RealityScanStage::ExportOrthoProjection,
                RealityScanStage::SaveProject,
            ],
            project_filename: "aerial.rsproj".to_string(),
            resume_source_job_id: None,
            resume_project_filename: None,
            project_coordinate_system: None,
            output_coordinate_system: None,
            orthomosaic_filename: Some("aerial-5cm.tif".to_string()),
            ortho_pixel_size_meters: Some(0.05),
            ortho_render_method: Some(OrthoRenderMethod::ImageMosaicingAerial),
            ortho_projection_params_xml: Some(ortho_params.to_string()),
            alignment_settings: None,
            runtime_settings: None,
            single_session: false,
            print_progress_interval_seconds: None,
        };

        let script = realityscan_rscmd_script(&pipeline, &manifest).unwrap();

        assert!(script
            .contains("-exportReconstructionRegion \"Z:\\job\\outputs\\auto-ortho-region.rsbox\""));
        assert!(script.contains("-scaleReconstructionRegion 10 12 3 center absolute"));
        assert!(script.contains(
            "-calculateOrthoProjection \"Z:\\job\\outputs\\calculate-ortho.rsortho\" \"Z:\\job\\outputs\\auto-ortho-region.rsbox\""
        ));
    }

    #[test]
    fn realityscan_phases_save_after_align_before_normal_model() {
        let manifest = JobInputManifest {
            job_id: "job-1".to_string(),
            expires_at: Utc::now() + chrono::Duration::hours(1),
            inputs: Vec::new(),
        };
        let pipeline = RealityScanPipeline {
            template_id: "normal".to_string(),
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
            project_filename: "final.rsproj".to_string(),
            resume_source_job_id: None,
            resume_project_filename: None,
            project_coordinate_system: None,
            output_coordinate_system: None,
            orthomosaic_filename: Some("ortho.tif".to_string()),
            ortho_pixel_size_meters: Some(0.05),
            ortho_render_method: None,
            ortho_projection_params_xml: None,
            alignment_settings: None,
            runtime_settings: None,
            single_session: false,
            print_progress_interval_seconds: None,
        };

        let phases = realityscan_phases(&pipeline, &manifest).unwrap();

        assert_eq!(phases.len(), 3);
        assert_eq!(phases[0].name, "align-save");
        assert!(phases[0]
            .commands
            .contains(&"-save \"Z:\\job\\outputs\\aligned.rsproj\"".to_string()));
        assert!(!phases[0]
            .commands
            .contains(&"-calculateNormalModel".to_string()));
        assert_eq!(
            phases[1].commands[0],
            "-load \"Z:\\job\\outputs\\aligned.rsproj\" deleteAutosave"
        );
        assert!(phases[1]
            .commands
            .contains(&"-selectMaximalComponent".to_string()));
        assert!(phases[1]
            .commands
            .contains(&"-calculateNormalModel".to_string()));
        assert!(phases[1]
            .commands
            .contains(&"-save \"Z:\\job\\outputs\\modeled.rsproj\"".to_string()));
        assert_eq!(
            phases[2].commands[0],
            "-load \"Z:\\job\\outputs\\modeled.rsproj\" deleteAutosave"
        );
        assert!(phases[2].commands.iter().any(|command| {
            command.contains("-exportOrthoProjection \"Z:\\job\\outputs\\ortho.tif\"")
        }));
        assert!(phases[2]
            .commands
            .contains(&"-save \"Z:\\job\\outputs\\final.rsproj\"".to_string()));
    }

    #[test]
    fn realityscan_single_session_runtime_settings_avoid_split_boundary() {
        let manifest = JobInputManifest {
            job_id: "job-1".to_string(),
            expires_at: Utc::now() + chrono::Duration::hours(1),
            inputs: Vec::new(),
        };
        let pipeline = RealityScanPipeline {
            template_id: "density".to_string(),
            stages: vec![
                RealityScanStage::SetIntrinsics,
                RealityScanStage::Align,
                RealityScanStage::SelectMaximalComponent,
                RealityScanStage::SetReconstructionRegionByDensity,
                RealityScanStage::CalculateHighModel,
                RealityScanStage::CorrectColors,
                RealityScanStage::CalculateOrthoProjection,
                RealityScanStage::ExportOrthoProjection,
                RealityScanStage::SaveProject,
            ],
            project_filename: "density.rsproj".to_string(),
            resume_source_job_id: None,
            resume_project_filename: None,
            project_coordinate_system: Some("epsg:32618".to_string()),
            output_coordinate_system: Some("epsg:32618".to_string()),
            orthomosaic_filename: Some("density.tif".to_string()),
            ortho_pixel_size_meters: Some(0.05),
            ortho_render_method: Some(OrthoRenderMethod::ImageMosaicingAerial),
            ortho_projection_params_xml: None,
            alignment_settings: None,
            runtime_settings: Some(RealityScanRuntimeSettings {
                auto_save_mode: Some(true),
                auto_save_cli_handling: Some("recover".to_string()),
                auto_clear_cache: Some(999_999),
                geometry_gpu_accel: Some(true),
                max_vertex_count_in_part: Some(500_000),
                cache_namespace: None,
                ortho_region_width_meters: None,
                ortho_region_height_meters: None,
                ortho_region_depth_meters: None,
            }),
            single_session: true,
            print_progress_interval_seconds: Some(60),
        };

        let phases = realityscan_phases(&pipeline, &manifest).unwrap();

        assert_eq!(phases.len(), 1);
        assert_eq!(phases[0].name, "single");
        assert!(phases[0]
            .commands
            .contains(&"-set \"appAutoSaveMode=true\"".to_string()));
        assert!(phases[0]
            .commands
            .contains(&"-set \"appAutoSaveCliHandling=recover\"".to_string()));
        assert!(phases[0]
            .commands
            .contains(&"-set \"appAutoClearCache=999999\"".to_string()));
        assert!(phases[0]
            .commands
            .contains(&"-set \"MvsGeometryGpuAccel=true\"".to_string()));
        assert!(phases[0]
            .commands
            .contains(&"-set \"mvsMaxVertexCountInPart=500000\"".to_string()));
        assert!(!phases[0]
            .commands
            .iter()
            .any(|command| command.starts_with("-load ")));
        assert!(phases[0]
            .commands
            .contains(&"-calculateHighModel".to_string()));
        assert!(phases[0]
            .commands
            .iter()
            .any(|command| command.contains("-exportOrthoProjection")));

        let launcher = realityscan_cli_script(
            &pipeline,
            "Z:\\job\\work\\00-single.rscmd",
            &phases[0].commands,
        )
        .unwrap();
        assert!(launcher.contains("generate_ortho_projection_params_from_region"));
        assert!(launcher.contains("region_path='/job/outputs/density-ortho-region.rsbox'"));
        assert!(
            launcher.contains("region_wait_seconds=\"${RSLOGIC_ORTHO_REGION_WAIT_SECS:-604800}\"")
        );
        assert!(launcher.contains("colorType=\"aerial mosaicing\""));
        assert!(launcher.contains("rslogic_rsortho_watcher_pid=$!"));
    }

    #[test]
    fn realityscan_phase_runtime_default_allows_multi_day_density_jobs() {
        assert_eq!(REALITYSCAN_PHASE_MAX_RUNTIME_SECS, 7 * 24 * 60 * 60);
    }

    #[test]
    fn realityscan_align_only_template_stays_single_phase() {
        let manifest = JobInputManifest {
            job_id: "job-1".to_string(),
            expires_at: Utc::now() + chrono::Duration::hours(1),
            inputs: Vec::new(),
        };
        let pipeline = RealityScanPipeline {
            template_id: "align-only".to_string(),
            stages: vec![
                RealityScanStage::SetIntrinsics,
                RealityScanStage::Align,
                RealityScanStage::SelectMaximalComponent,
                RealityScanStage::SaveProject,
            ],
            project_filename: "aligned.rsproj".to_string(),
            resume_source_job_id: None,
            resume_project_filename: None,
            project_coordinate_system: None,
            output_coordinate_system: None,
            orthomosaic_filename: None,
            ortho_pixel_size_meters: None,
            ortho_render_method: None,
            ortho_projection_params_xml: None,
            alignment_settings: None,
            runtime_settings: None,
            single_session: false,
            print_progress_interval_seconds: None,
        };

        let phases = realityscan_phases(&pipeline, &manifest).unwrap();

        assert_eq!(phases.len(), 1);
        assert_eq!(phases[0].name, "single");
        assert!(phases[0]
            .commands
            .contains(&"-save \"Z:\\job\\outputs\\aligned.rsproj\"".to_string()));
    }

    #[test]
    fn realityscan_resume_project_runs_density_normal_color_and_outputs() {
        let manifest = JobInputManifest {
            job_id: "job-1".to_string(),
            expires_at: Utc::now() + chrono::Duration::hours(1),
            inputs: Vec::new(),
        };
        let ortho_params = r#"<OrthoProjection width="1000" height="1000" name="Ortho projection 1" modelName="Model 1"
   colorType="aerial mosaicing" projectionType="3" bShowOrthoProjection="1">
  <Header magic="5787472" version="2"/>
</OrthoProjection>
<ReconstructionRegion globalCoordinateSystem="+proj=longlat +datum=WGS84 +no_defs"
   globalCoordinateSystemName="epsg:4326 - GPS (WGS 84)" isGeoreferenced="1" isLatLon="1">
  <widthHeightDepth>1490.13422254291 6520.2009561944 213.254882812728</widthHeightDepth>
  <Header magic="5395016" version="2"/>
</ReconstructionRegion>"#;
        let pipeline = RealityScanPipeline {
            template_id: "resume-density-normal-color-aerial".to_string(),
            stages: vec![
                RealityScanStage::SetReconstructionRegionByDensity,
                RealityScanStage::CalculateNormalModel,
                RealityScanStage::CorrectColors,
                RealityScanStage::CalculateOrthoProjection,
                RealityScanStage::ExportOrthoProjection,
                RealityScanStage::SaveProject,
            ],
            project_filename: "density-normal-color-aerial.rsproj".to_string(),
            resume_source_job_id: Some("08fb2ede-481d-4b89-821f-52a726185643".to_string()),
            resume_project_filename: Some("aligned.rsproj".to_string()),
            project_coordinate_system: None,
            output_coordinate_system: None,
            orthomosaic_filename: Some("density-normal-color-aerial.tif".to_string()),
            ortho_pixel_size_meters: Some(0.05),
            ortho_render_method: Some(OrthoRenderMethod::ImageMosaicingAerial),
            ortho_projection_params_xml: Some(ortho_params.to_string()),
            alignment_settings: None,
            runtime_settings: None,
            single_session: false,
            print_progress_interval_seconds: Some(60),
        };

        let phases = realityscan_phases(&pipeline, &manifest).unwrap();

        assert_eq!(phases.len(), 2);
        assert_eq!(phases[0].name, "model-save");
        assert_eq!(
            phases[0].commands[0],
            "-load \"Z:\\job\\outputs\\aligned.rsproj\" deleteAutosave"
        );
        assert!(phases[0]
            .commands
            .contains(&"-printProgress 60".to_string()));
        assert!(phases[0]
            .commands
            .contains(&"-setReconstructionRegionByDensity".to_string()));
        assert!(phases[0].commands.contains(&format!(
            "-exportReconstructionRegion {}",
            rscmd_quote("Z:\\job\\outputs\\density-ortho-region.rsbox")
        )));
        assert!(phases[0]
            .commands
            .contains(&"-calculateNormalModel".to_string()));
        assert!(!phases[0].commands.contains(&"-correctColors".to_string()));
        assert!(phases[0]
            .commands
            .contains(&"-save \"Z:\\job\\outputs\\modeled.rsproj\"".to_string()));
        assert_eq!(phases[1].name, "outputs");
        assert_eq!(
            phases[1].commands[0],
            "-load \"Z:\\job\\outputs\\modeled.rsproj\" deleteAutosave"
        );
        assert!(phases[1].commands.contains(&"-correctColors".to_string()));
        assert!(phases[1].commands.iter().any(|command| command.contains(
            "-calculateOrthoProjection \"Z:\\job\\outputs\\calculate-ortho.rsortho\" \"Z:\\job\\outputs\\density-ortho-region.rsbox\""
        )));
        assert!(phases[1].commands.iter().any(|command| command.contains(
            "-exportOrthoProjection \"Z:\\job\\outputs\\density-normal-color-aerial.tif\""
        )));
        assert!(phases[1].commands.contains(
            &"-save \"Z:\\job\\outputs\\density-normal-color-aerial.rsproj\"".to_string()
        ));
    }

    #[test]
    fn realityscan_resume_project_can_select_component_before_high_model() {
        let manifest = JobInputManifest {
            job_id: "job-1".to_string(),
            expires_at: Utc::now() + chrono::Duration::hours(1),
            inputs: Vec::new(),
        };
        let ortho_params = r#"<OrthoProjection width="1000" height="1000" name="Ortho projection 1" modelName="Model 1"
   colorType="aerial mosaicing" projectionType="3" bShowOrthoProjection="1">
  <Header magic="5787472" version="2"/>
</OrthoProjection>
<ReconstructionRegion globalCoordinateSystem="+proj=utm +zone=18 +datum=WGS84 +units=m +no_defs"
   globalCoordinateSystemName="epsg:32618 - WGS 84 / UTM zone 18N" isGeoreferenced="1" isLatLon="0">
  <widthHeightDepth>3080.134315077565 4035.639579713112 800</widthHeightDepth>
  <Header magic="5395016" version="2"/>
</ReconstructionRegion>"#;
        let pipeline = RealityScanPipeline {
            template_id: "resume-select-high-aerial".to_string(),
            stages: vec![
                RealityScanStage::SelectMaximalComponent,
                RealityScanStage::CalculateHighModel,
                RealityScanStage::CorrectColors,
                RealityScanStage::CalculateOrthoProjection,
                RealityScanStage::ExportOrthoProjection,
                RealityScanStage::SaveProject,
            ],
            project_filename: "selected-high-aerial.rsproj".to_string(),
            resume_source_job_id: Some("40d62c10-a753-4673-ab13-b3754ce91e7e".to_string()),
            resume_project_filename: Some("aligned.rsproj".to_string()),
            project_coordinate_system: Some("epsg:32618".to_string()),
            output_coordinate_system: Some("epsg:32618".to_string()),
            orthomosaic_filename: Some("selected-high-aerial.tif".to_string()),
            ortho_pixel_size_meters: Some(0.05),
            ortho_render_method: Some(OrthoRenderMethod::ImageMosaicingAerial),
            ortho_projection_params_xml: Some(ortho_params.to_string()),
            alignment_settings: None,
            runtime_settings: None,
            single_session: false,
            print_progress_interval_seconds: Some(60),
        };

        let phases = realityscan_phases(&pipeline, &manifest).unwrap();

        assert_eq!(phases.len(), 2);
        assert_eq!(phases[0].name, "model-save");
        assert_eq!(
            phases[0].commands[0],
            "-load \"Z:\\job\\outputs\\aligned.rsproj\" deleteAutosave"
        );
        assert!(phases[0]
            .commands
            .contains(&"-selectMaximalComponent".to_string()));
        assert!(phases[0]
            .commands
            .contains(&"-calculateHighModel".to_string()));
        assert!(phases[0]
            .commands
            .contains(&"-save \"Z:\\job\\outputs\\modeled.rsproj\"".to_string()));
        assert_eq!(phases[1].name, "outputs");
        assert_eq!(
            phases[1].commands[0],
            "-load \"Z:\\job\\outputs\\modeled.rsproj\" deleteAutosave"
        );
        assert!(phases[1].commands.iter().any(|command| command
            .contains("-calculateOrthoProjection \"Z:\\job\\outputs\\calculate-ortho.rsortho\"")));
        assert!(phases[1].commands.iter().any(|command| {
            command
                .contains("-exportOrthoProjection \"Z:\\job\\outputs\\selected-high-aerial.tif\"")
        }));
    }

    #[test]
    fn realityscan_continue_model_calculation_recovers_autosave() {
        let manifest = JobInputManifest {
            job_id: "job-1".to_string(),
            expires_at: Utc::now() + chrono::Duration::hours(1),
            inputs: Vec::new(),
        };
        let pipeline = RealityScanPipeline {
            template_id: "continue-high-aerial".to_string(),
            stages: vec![
                RealityScanStage::ContinueModelCalculation,
                RealityScanStage::CorrectColors,
                RealityScanStage::SaveProject,
            ],
            project_filename: "continued-high-aerial.rsproj".to_string(),
            resume_source_job_id: Some("source-job".to_string()),
            resume_project_filename: Some("density-high-color-aerial-5cm.rsproj".to_string()),
            runtime_settings: Some(RealityScanRuntimeSettings {
                auto_save_mode: Some(true),
                auto_save_cli_handling: Some("recover".to_string()),
                auto_clear_cache: Some(999_999),
                geometry_gpu_accel: Some(true),
                max_vertex_count_in_part: Some(2_000_000),
                cache_namespace: None,
                ortho_region_width_meters: None,
                ortho_region_height_meters: None,
                ortho_region_depth_meters: None,
            }),
            single_session: false,
            print_progress_interval_seconds: Some(60),
            ..RealityScanPipeline::default()
        };

        let phases = realityscan_phases(&pipeline, &manifest).unwrap();

        assert_eq!(phases.len(), 2);
        assert_eq!(phases[0].name, "model-save");
        assert_eq!(
            phases[0].commands[0],
            "-load \"Z:\\job\\outputs\\density-high-color-aerial-5cm.rsproj\" recoverAutosave"
        );
        assert!(phases[0]
            .commands
            .contains(&"-continueModelCalculation".to_string()));
        assert!(phases[0]
            .commands
            .contains(&"-set \"appAutoSaveCliHandling=recover\"".to_string()));
        assert!(phases[0]
            .commands
            .contains(&"-save \"Z:\\job\\outputs\\modeled.rsproj\"".to_string()));
        assert_eq!(phases[1].name, "outputs");
        assert_eq!(
            phases[1].commands[0],
            "-load \"Z:\\job\\outputs\\modeled.rsproj\" deleteAutosave"
        );
        assert!(phases[1]
            .commands
            .contains(&"-save \"Z:\\job\\outputs\\continued-high-aerial.rsproj\"".to_string()));
    }

    #[test]
    fn realityscan_cache_namespace_uses_named_cache_dir() {
        let args = Args {
            state_dir: PathBuf::from("/state"),
            container_runtime: "docker".to_string(),
            realityscan_cache_root: Some(PathBuf::from("/cache")),
            realityscan_phase_max_runtime_secs: REALITYSCAN_PHASE_MAX_RUNTIME_SECS,
            command: Command::DownloadOnly {
                manifest: PathBuf::from("manifest.json"),
            },
        };
        let pipeline = RealityScanPipeline {
            runtime_settings: Some(RealityScanRuntimeSettings {
                cache_namespace: Some("yallahs_high_mvs5m".to_string()),
                ..RealityScanRuntimeSettings::default()
            }),
            ..RealityScanPipeline::default()
        };

        assert_eq!(
            realityscan_job_cache_dir(&args, &pipeline, "job-1").unwrap(),
            PathBuf::from("/cache/named/yallahs_high_mvs5m")
        );

        let default_pipeline = RealityScanPipeline::default();
        assert_eq!(
            realityscan_job_cache_dir(&args, &default_pipeline, "job-1").unwrap(),
            PathBuf::from("/cache/job-1")
        );

        let invalid_pipeline = RealityScanPipeline {
            runtime_settings: Some(RealityScanRuntimeSettings {
                cache_namespace: Some("../bad".to_string()),
                ..RealityScanRuntimeSettings::default()
            }),
            ..RealityScanPipeline::default()
        };
        assert!(realityscan_job_cache_dir(&args, &invalid_pipeline, "job-1").is_err());
    }

    #[tokio::test]
    async fn materialize_resume_project_copies_project_and_sidecar() {
        let temp = tempfile::tempdir().unwrap();
        let state_dir = temp.path().join("state");
        let source_outputs = state_dir.join("jobs").join("source-job").join("outputs");
        fs::create_dir_all(source_outputs.join("aligned"))
            .await
            .unwrap();
        fs::write(source_outputs.join("aligned.rsproj"), b"project")
            .await
            .unwrap();
        fs::write(
            source_outputs.join("aligned").join("component.dat"),
            b"sidecar",
        )
        .await
        .unwrap();
        let job_dir = temp.path().join("jobs").join("target-job");
        prepare_job_dir(&job_dir).await.unwrap();
        let pipeline = RealityScanPipeline {
            resume_source_job_id: Some("source-job".to_string()),
            resume_project_filename: Some("aligned.rsproj".to_string()),
            ..RealityScanPipeline::default()
        };

        materialize_resume_project("target-job", &pipeline, &job_dir, &state_dir)
            .await
            .unwrap();

        assert_eq!(
            fs::read(job_dir.join("outputs").join("aligned.rsproj"))
                .await
                .unwrap(),
            b"project"
        );
        assert_eq!(
            fs::read(
                job_dir
                    .join("outputs")
                    .join("aligned")
                    .join("component.dat")
            )
            .await
            .unwrap(),
            b"sidecar"
        );
        let events = fs::read_to_string(job_dir.join("logs").join(JOB_EVENTS_LOG))
            .await
            .unwrap();
        assert!(events.contains("\"job_id\":\"target-job\""));
        assert!(events.contains("staged resume project from job source-job"));
    }

    #[tokio::test]
    async fn input_cache_restores_verified_blob() {
        let temp = tempfile::tempdir().unwrap();
        let cache_root = input_cache_root(temp.path());
        let source = temp.path().join("source.jpg");
        fs::write(&source, b"image-data").await.unwrap();
        let sha256 = file_sha256(&source).await.unwrap();
        let input = CloudfrontInput {
            asset_id: "asset-1".to_string(),
            filename: "image.jpg".to_string(),
            url: "https://example.test/image.jpg".to_string(),
            sha256: Some(sha256.clone()),
            size_bytes: Some(10),
            camera_intrinsics: None,
        };

        store_input_in_cache(&cache_root, &input, &source)
            .await
            .unwrap();
        let target = temp.path().join("job").join("inputs").join("image.jpg");
        let restored = restore_input_from_cache(&cache_root, &input, &target)
            .await
            .unwrap();

        assert!(restored);
        assert_eq!(fs::read(&target).await.unwrap(), b"image-data");
        assert_eq!(file_sha256(&target).await.unwrap(), sha256);
    }

    #[tokio::test]
    async fn input_cache_prunes_entries_unused_for_a_month() {
        let temp = tempfile::tempdir().unwrap();
        let cache_root = input_cache_root(temp.path());
        let source = temp.path().join("source.jpg");
        fs::write(&source, b"old-image-data").await.unwrap();
        let sha256 = file_sha256(&source).await.unwrap();
        let input = CloudfrontInput {
            asset_id: "asset-1".to_string(),
            filename: "image.jpg".to_string(),
            url: "https://example.test/image.jpg".to_string(),
            sha256: Some(sha256.clone()),
            size_bytes: None,
            camera_intrinsics: None,
        };
        store_input_in_cache(&cache_root, &input, &source)
            .await
            .unwrap();
        let paths = input_cache_paths(&cache_root, &sha256).unwrap();
        write_json(
            &paths.metadata_path,
            &CachedInputMetadata {
                sha256,
                filename: "image.jpg".to_string(),
                size_bytes: None,
                cached_at: Utc::now() - chrono::Duration::days(40),
                last_used_at: Utc::now() - chrono::Duration::days(31),
            },
        )
        .await
        .unwrap();

        prune_input_cache(&cache_root).await.unwrap();

        assert!(!paths.entry_dir.exists());
    }

    #[tokio::test]
    async fn output_directories_are_packaged_as_zip_artifacts() {
        let temp = tempfile::tempdir().unwrap();
        let outputs_dir = temp.path().join("outputs");
        let project_dir = outputs_dir.join("preview-ortho");
        fs::create_dir_all(&project_dir).await.unwrap();
        fs::write(project_dir.join("model.dat"), b"model-data")
            .await
            .unwrap();

        package_output_directories(&outputs_dir).await.unwrap();

        let archive_path = outputs_dir.join("preview-ortho.zip");
        assert!(archive_path.is_file());
        let archive = std::fs::File::open(archive_path).unwrap();
        let mut zip = zip::ZipArchive::new(archive).unwrap();
        let mut entry = zip.by_name("model.dat").unwrap();
        let mut data = String::new();
        entry.read_to_string(&mut data).unwrap();
        assert_eq!(data, "model-data");
    }

    #[tokio::test]
    async fn collect_outputs_requires_declared_pipeline_files() {
        let temp = tempfile::tempdir().unwrap();
        let job_dir = temp.path().join("job-1");
        let outputs_dir = job_dir.join("outputs");
        fs::create_dir_all(&outputs_dir).await.unwrap();
        fs::create_dir_all(job_dir.join("logs")).await.unwrap();
        fs::write(outputs_dir.join("export-ortho-config.xml"), b"config")
            .await
            .unwrap();

        let job = PipelineJob {
            job_id: "job-1".to_string(),
            job_name: None,
            manifest: JobInputManifest {
                job_id: "job-1".to_string(),
                expires_at: Utc::now() + chrono::Duration::hours(1),
                inputs: Vec::<CloudfrontInput>::new(),
            },
            output_targets: Vec::new(),
            realityscan_image: "unused".to_string(),
            pipeline: RealityScanPipeline {
                template_id: "density".to_string(),
                stages: vec![
                    RealityScanStage::ExportOrthoProjection,
                    RealityScanStage::SaveProject,
                ],
                project_filename: "density.rsproj".to_string(),
                orthomosaic_filename: Some("density.tif".to_string()),
                ..RealityScanPipeline::default()
            },
        };

        let error = collect_outputs(&job, &job_dir).await.unwrap_err();

        assert!(format!("{error:#}").contains("expected pipeline output density.tif"));
    }

    #[tokio::test]
    async fn upload_outputs_puts_artifact_to_target_url() {
        let temp = tempfile::tempdir().unwrap();
        let job_dir = temp.path().join("job-1");
        let outputs_dir = job_dir.join("outputs");
        fs::create_dir_all(&outputs_dir).await.unwrap();
        let output_path = outputs_dir.join("summary.txt");
        fs::write(&output_path, b"upload-body").await.unwrap();

        let (url, receiver, server) = put_receiver();
        let expires_at = Utc::now() + chrono::Duration::hours(1);
        let job = PipelineJob {
            job_id: "job-1".to_string(),
            job_name: None,
            manifest: JobInputManifest {
                job_id: "job-1".to_string(),
                expires_at,
                inputs: Vec::<CloudfrontInput>::new(),
            },
            output_targets: vec![OutputUploadTarget {
                artifact_id: "summary".to_string(),
                filename: "summary.txt".to_string(),
                method: "PUT".to_string(),
                url,
                storage_uri: Some("s3://test-bucket/job-1/summary.txt".to_string()),
                content_type: Some("text/plain".to_string()),
                headers: vec![UploadHeader {
                    name: "x-rslogic-test".to_string(),
                    value: "ok".to_string(),
                }],
                expires_at,
            }],
            realityscan_image: "unused".to_string(),
            pipeline: RealityScanPipeline::default(),
        };
        let outputs = vec![
            describe_output_artifact("summary", "summary.txt", &output_path)
                .await
                .unwrap(),
        ];

        upload_outputs(&Client::new(), &job, &job_dir, &outputs)
            .await
            .unwrap();

        let request = receiver.recv_timeout(StdDuration::from_secs(5)).unwrap();
        server.join().unwrap();
        assert!(request.head.starts_with("PUT /upload HTTP/1.1"));
        assert!(header_contains(&request.head, "content-type", "text/plain"));
        assert!(header_contains(&request.head, "x-rslogic-test", "ok"));
        assert_eq!(request.body, b"upload-body");
    }

    struct CapturedRequest {
        head: String,
        body: Vec<u8>,
    }

    fn put_receiver() -> (
        String,
        mpsc::Receiver<CapturedRequest>,
        thread::JoinHandle<()>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let (sender, receiver) = mpsc::channel();
        let handle = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(StdDuration::from_secs(5)))
                .unwrap();
            let mut data = Vec::new();
            let mut buffer = [0_u8; 4096];
            let mut expected_body_len = None;
            let mut header_end = None;
            loop {
                let read = stream.read(&mut buffer).unwrap();
                if read == 0 {
                    break;
                }
                data.extend_from_slice(&buffer[..read]);
                if header_end.is_none() {
                    header_end = find_header_end(&data);
                    if let Some(end) = header_end {
                        let head = String::from_utf8_lossy(&data[..end]).to_string();
                        expected_body_len = content_length(&head);
                    }
                }
                if let (Some(end), Some(length)) = (header_end, expected_body_len) {
                    if data.len().saturating_sub(end + 4) >= length {
                        break;
                    }
                }
            }
            stream
                .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n")
                .unwrap();
            let end = header_end.unwrap();
            sender
                .send(CapturedRequest {
                    head: String::from_utf8_lossy(&data[..end]).to_string(),
                    body: data[end + 4..].to_vec(),
                })
                .unwrap();
        });
        (format!("http://{addr}/upload"), receiver, handle)
    }

    fn find_header_end(data: &[u8]) -> Option<usize> {
        data.windows(4).position(|window| window == b"\r\n\r\n")
    }

    fn content_length(head: &str) -> Option<usize> {
        head.lines().find_map(|line| {
            let (name, value) = line.split_once(':')?;
            if name.eq_ignore_ascii_case("content-length") {
                value.trim().parse().ok()
            } else {
                None
            }
        })
    }

    fn header_contains(head: &str, expected_name: &str, expected_value: &str) -> bool {
        head.lines().any(|line| {
            let Some((name, value)) = line.split_once(':') else {
                return false;
            };
            name.eq_ignore_ascii_case(expected_name) && value.trim() == expected_value
        })
    }
}
