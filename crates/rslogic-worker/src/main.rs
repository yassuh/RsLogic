use std::{
    collections::HashMap,
    io::ErrorKind,
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::{anyhow, Context};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use clap::{Parser, Subcommand};
use futures_util::StreamExt;
use reqwest::Client;
use rslogic_protocol::{
    now, CameraIntrinsics, CloudfrontInput, JobEvent, JobInputManifest, JobState,
    OutputUploadTarget, PipelineJob, RealityScanPipeline, RealityScanStage, UploadedArtifact,
    DEFAULT_WORKER_STATE_DIR,
};
use rslogic_realityscan::{
    ContainerRealityScanRunner, ContainerRuntime, RealityScanRunConfig, RealityScanRunner,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::{
    fs,
    io::{AsyncReadExt, AsyncWriteExt},
};
use tracing::info;
use tracing_subscriber::{fmt, EnvFilter};
use zip::{write::SimpleFileOptions, CompressionMethod, ZipWriter};

const INPUT_CACHE_MAX_UNUSED_DAYS: i64 = 30;
const REALITYSCAN_PHASE_MAX_RUNTIME_SECS: u64 = 24 * 60 * 60;
const REALITYSCAN_LIVENESS_CHECK_INTERVAL_SECS: u64 = 30;

#[derive(Debug, Parser)]
struct Args {
    #[arg(long, env = "RSLOGIC_WORKER_STATE_DIR", default_value = DEFAULT_WORKER_STATE_DIR)]
    state_dir: PathBuf,
    #[arg(long, env = "RSLOGIC_CONTAINER_RUNTIME", default_value = "docker")]
    container_runtime: String,
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
                &job.job_id,
                JobState::Accepted,
                "resuming job from collected outputs",
                0.0,
            )
            .await?;
            upload_outputs(&http, &job, &job_dir, &state.output_artifacts).await?;
            write_worker_state(&job_dir, &job.job_id, state.output_artifacts, true).await?;
            emit(&job.job_id, JobState::Completed, "job completed", 100.0).await?;
            return Ok(());
        }
    }

    emit(&job.job_id, JobState::Accepted, "job accepted", 0.0).await?;
    download_inputs(&http, &job.manifest, &job_dir, &args.state_dir).await?;
    run_realityscan(args, &job, &job_dir).await?;
    let outputs = collect_outputs(&job, &job_dir).await?;
    upload_outputs(&http, &job, &job_dir, &outputs).await?;
    write_worker_state(&job_dir, &job.job_id, outputs, true).await?;
    emit(&job.job_id, JobState::Completed, "job completed", 100.0).await?;
    Ok(())
}

async fn prepare_job_dir(job_dir: &Path) -> anyhow::Result<()> {
    fs::create_dir_all(job_dir.join("inputs")).await?;
    fs::create_dir_all(job_dir.join("work")).await?;
    fs::create_dir_all(job_dir.join("outputs")).await?;
    fs::create_dir_all(job_dir.join("logs")).await?;
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
        info!(
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
    for (index, phase) in phases.iter().enumerate() {
        let file_stem = format!("{index:02}-{}", phase.name);
        let script_path = job_dir
            .join("work")
            .join(format!("run-realityscan-{file_stem}.sh"));
        let commands_path = job_dir.join("work").join(format!("{file_stem}.rscmd"));
        let windows_commands_path = format!("Z:\\job\\work\\{file_stem}.rscmd");
        fs::write(
            &script_path,
            realityscan_cli_script(&job.pipeline, &windows_commands_path)?,
        )
        .await?;
        fs::write(&commands_path, phase.commands.join("\n")).await?;
        emit(
            &job.job_id,
            JobState::RunningRealityscan,
            &format!("starting RealityScan phase {}", phase.name),
            40.0 + ((index as f32) / phase_count) * 40.0,
        )
        .await?;
        runner
            .run(RealityScanRunConfig {
                runtime: runtime.clone(),
                image: job.realityscan_image.clone(),
                job_dir: job_dir.to_path_buf(),
                command: vec![
                    "/bin/bash".to_string(),
                    format!("/job/work/run-realityscan-{file_stem}.sh"),
                ],
                gpu: true,
                log_prefix: Some(format!("realityscan-{file_stem}")),
                max_runtime_secs: Some(REALITYSCAN_PHASE_MAX_RUNTIME_SECS),
                liveness_check_interval_secs: Some(REALITYSCAN_LIVENESS_CHECK_INTERVAL_SECS),
            })
            .await
            .with_context(|| format!("RealityScan phase {} failed", phase.name))?;
    }
    Ok(())
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
    let should_split =
        stages.iter().any(is_split_trigger_stage) && stages.iter().any(is_alignment_stage);
    if !should_split {
        return Ok(vec![RealityScanPhase {
            name: "single".to_string(),
            commands: combined_realityscan_commands(pipeline, manifest, &stages)?,
        }]);
    }

    let mut phases = Vec::new();
    let mut align_commands = vec![
        "-newScene".to_string(),
        "-addFolder \"Z:\\job\\inputs\"".to_string(),
    ];
    for stage in stages.iter().filter(|stage| is_alignment_stage(stage)) {
        align_commands.extend(realityscan_stage_commands(stage, pipeline, manifest)?);
    }
    align_commands.push(save_project_command("aligned.rsproj"));
    align_commands.push("-quit".to_string());
    phases.push(RealityScanPhase {
        name: "align-save".to_string(),
        commands: align_commands,
    });

    let mut model_stage_commands = Vec::new();
    for stage in stages.iter().filter(|stage| is_model_stage(stage)) {
        model_stage_commands.extend(realityscan_stage_commands(stage, pipeline, manifest)?);
    }
    let has_output_stage = stages.iter().any(is_output_stage);
    let mut latest_project = "aligned.rsproj";
    if !model_stage_commands.is_empty() {
        let mut commands = vec![load_project_command(latest_project)];
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
    let mut commands = vec![
        "-newScene".to_string(),
        "-addFolder \"Z:\\job\\inputs\"".to_string(),
    ];
    for stage in stages {
        commands.extend(realityscan_stage_commands(stage, pipeline, manifest)?);
    }
    commands.push("-quit".to_string());
    Ok(commands)
}

fn is_alignment_stage(stage: &RealityScanStage) -> bool {
    matches!(
        stage,
        RealityScanStage::SetIntrinsics
            | RealityScanStage::Align
            | RealityScanStage::SelectMaximalComponent
    )
}

fn is_model_stage(stage: &RealityScanStage) -> bool {
    matches!(
        stage,
        RealityScanStage::SetReconstructionRegionAuto
            | RealityScanStage::CalculatePreviewModel
            | RealityScanStage::CalculateNormalModel
            | RealityScanStage::CalculateHighModel
    )
}

fn is_output_stage(stage: &RealityScanStage) -> bool {
    matches!(
        stage,
        RealityScanStage::CalculateTexture
            | RealityScanStage::CalculateOrthoProjection
            | RealityScanStage::ExportOrthoProjection
            | RealityScanStage::SaveProject
    )
}

fn is_split_trigger_stage(stage: &RealityScanStage) -> bool {
    is_model_stage(stage)
        || matches!(
            stage,
            RealityScanStage::CalculateTexture
                | RealityScanStage::CalculateOrthoProjection
                | RealityScanStage::ExportOrthoProjection
        )
}

fn save_project_command(filename: &str) -> String {
    format!("-save {}", rscmd_quote(&windows_output_path(filename)))
}

fn load_project_command(filename: &str) -> String {
    format!(
        "-load {} deleteAutosave",
        rscmd_quote(&windows_output_path(filename))
    )
}

fn realityscan_cli_script(
    pipeline: &RealityScanPipeline,
    windows_commands_path: &str,
) -> anyhow::Result<String> {
    if pipeline
        .ortho_pixel_size_meters
        .is_some_and(|value| !value.is_finite() || value <= 0.0)
    {
        anyhow::bail!("ortho_pixel_size_meters must be greater than zero");
    }
    let ortho_export_config = ortho_export_config_xml(pipeline);
    let mut script = r#"set -euo pipefail
mkdir -p /job/outputs /job/logs /tmp/runtime-rslogic
chmod 700 /tmp/runtime-rslogic
Xvfb :99 -screen 0 1920x1080x24 -nolisten tcp >/job/logs/xvfb.log 2>&1 &
xvfb_pid=$!
cleanup() {
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
    script.push_str(&format!(
        "XML\n/opt/realityscan/bin/realityscan-cli -headless -stdConsole -execRSCMD {}\n",
        shell_quote(windows_commands_path)
    ));
    Ok(script)
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
    let mut script = String::from("-newScene\n-addFolder \"Z:\\job\\inputs\"\n");
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

fn realityscan_stage_commands(
    stage: &RealityScanStage,
    pipeline: &RealityScanPipeline,
    manifest: &JobInputManifest,
) -> anyhow::Result<Vec<String>> {
    match stage {
        RealityScanStage::SetIntrinsics => realityscan_intrinsics_commands(manifest),
        RealityScanStage::Align => Ok(vec!["-align".to_string()]),
        RealityScanStage::SelectMaximalComponent => Ok(vec!["-selectMaximalComponent".to_string()]),
        RealityScanStage::SetReconstructionRegionAuto => {
            Ok(vec!["-setReconstructionRegionAuto".to_string()])
        }
        RealityScanStage::CalculatePreviewModel => Ok(vec!["-calculatePreviewModel".to_string()]),
        RealityScanStage::CalculateNormalModel => Ok(vec!["-calculateNormalModel".to_string()]),
        RealityScanStage::CalculateHighModel => Ok(vec!["-calculateHighModel".to_string()]),
        RealityScanStage::CalculateTexture => Ok(vec!["-calculateTexture".to_string()]),
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
        if let Some(prior) = intrinsics
            .calibration_prior
            .or_else(|| has_calibration_parameters.then_some(1))
        {
            settings.push(("inpCalibration", prior.to_string()));
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
        if let Some(prior) = intrinsics
            .distortion_prior
            .or_else(|| has_distortion_parameters.then_some(1))
        {
            settings.push(("inpDistortion", prior.to_string()));
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
        settings.push((key, value.to_string()));
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
        &job.job_id,
        JobState::CollectingOutputs,
        "collecting outputs",
        70.0,
    )
    .await?;
    let outputs_dir = job_dir.join("outputs");
    package_output_directories(&outputs_dir).await?;
    let artifacts = if job.output_targets.is_empty() {
        discover_direct_output_artifacts(&outputs_dir).await?
    } else {
        collect_targeted_output_artifacts(&outputs_dir, &job.output_targets).await?
    };
    write_worker_state(job_dir, &job.job_id, artifacts.clone(), false).await?;
    Ok(artifacts)
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

async fn emit(job_id: &str, state: JobState, message: &str, progress: f32) -> anyhow::Result<()> {
    let event = JobEvent {
        job_id: job_id.to_string(),
        state,
        message: message.to_string(),
        progress,
        observed_at: now(),
    };
    println!("{}", serde_json::to_string(&event)?);
    Ok(())
}

async fn emit_artifact_uploaded(artifact: UploadedArtifact) -> anyhow::Result<()> {
    println!("{}", serde_json::to_string(&artifact)?);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rslogic_protocol::{CameraIntrinsics, CloudfrontInput, UploadHeader};
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
            orthomosaic_filename: None,
            ortho_pixel_size_meters: None,
        };

        let script = realityscan_rscmd_script(&pipeline, &manifest).unwrap();

        let select_index = script.find("-selectAllImages").unwrap();
        let align_index = script.find("-align").unwrap();
        assert!(select_index < align_index);
        assert!(!script.contains("-selectImage \"Z:\\job\\inputs\\image-1.jpg\" set"));
        assert!(script.contains("-editInputSelection \"inpCalibrationGroup=1\""));
        assert!(script.contains("-editInputSelection \"inpCalibration=2\""));
        assert!(script.contains("-editInputSelection \"inpFocal=24\""));
        assert!(script.contains("-editInputSelection \"inpPPX=0.12\""));
        assert!(script.contains("-editInputSelection \"inpPPY=-0.08\""));
        assert!(script.contains("-editInputSelection \"inpLensGroup=1\""));
        assert!(script.contains("-editInputSelection \"inpDistortion=2\""));
        assert!(script.contains("-editInputSelection \"inpDistortionModel=2\""));
        assert!(script.contains("-editInputSelection \"inpRadial1=-0.01\""));
        assert!(script.contains("-editInputSelection \"inpRadial2=0.001\""));
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
            orthomosaic_filename: None,
            ortho_pixel_size_meters: None,
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
            orthomosaic_filename: Some("seaforth-5cm-orthomosaic.tif".to_string()),
            ortho_pixel_size_meters: Some(0.05),
        };

        let launcher = realityscan_cli_script(&pipeline, "Z:\\job\\work\\commands.rscmd").unwrap();
        let script = realityscan_rscmd_script(&pipeline, &manifest).unwrap();

        assert!(launcher.contains(r#"<entry key="exportOrthoAsBigTiff" value="true"/>"#));
        assert!(launcher.contains(r#"<entry key="orthoPixelSize" value="0.05"/>"#));
        assert!(launcher.contains("-execRSCMD 'Z:\\job\\work\\commands.rscmd'"));
        assert!(script.contains(
            "-exportOrthoProjection \"Z:\\job\\outputs\\seaforth-5cm-orthomosaic.tif\" \"Z:\\job\\outputs\\export-ortho-config.xml\""
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
            orthomosaic_filename: Some("ortho.tif".to_string()),
            ortho_pixel_size_meters: Some(0.05),
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
            orthomosaic_filename: None,
            ortho_pixel_size_meters: None,
        };

        let phases = realityscan_phases(&pipeline, &manifest).unwrap();

        assert_eq!(phases.len(), 1);
        assert_eq!(phases[0].name, "single");
        assert!(phases[0]
            .commands
            .contains(&"-save \"Z:\\job\\outputs\\aligned.rsproj\"".to_string()));
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
