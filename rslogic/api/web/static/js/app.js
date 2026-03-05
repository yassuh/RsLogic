import { clampPercent, formatClock, getJSON, postJSON, prettyJSON } from "./api.js";
import { createJobBuilder } from "./job-builder.js";

const elements = {
  bottomStatusText: document.querySelector("#bottom-status-text"),
  bottomUploadStatus: document.querySelector("#bottom-upload-status"),
  bottomIngestStatus: document.querySelector("#bottom-ingest-status"),
  bottomClock: document.querySelector("#bottom-clock"),
  systemSummary: document.querySelector("#system-summary"),
  refreshAllButton: document.querySelector("#refresh-all-button"),
  selectFirstClientButton: document.querySelector("#select-first-client-button"),

  uploadStateBadge: document.querySelector("#upload-state-badge"),
  uploadCurrentPath: document.querySelector("#upload-current-path"),
  uploadPathUpButton: document.querySelector("#upload-path-up-button"),
  uploadPathRefreshButton: document.querySelector("#upload-path-refresh-button"),
  uploadStartButton: document.querySelector("#upload-start-button"),
  uploadDirectoryList: document.querySelector("#upload-directory-list"),
  uploadOperationLog: document.querySelector("#upload-operation-log"),
  uploadProgress: document.querySelector("#upload-progress .progress-fill"),

  ingestStateBadge: document.querySelector("#ingest-state-badge"),
  ingestGroupName: document.querySelector("#ingest-group-name"),
  ingestLimit: document.querySelector("#ingest-limit"),
  ingestStartButton: document.querySelector("#ingest-start-button"),
  ingestOperationLog: document.querySelector("#ingest-operation-log"),
  ingestProgress: document.querySelector("#ingest-progress .progress-fill"),

  jobsStateBadge: document.querySelector("#jobs-state-badge"),
  jobsRefreshButton: document.querySelector("#jobs-refresh-button"),
  jobDetailRefreshButton: document.querySelector("#job-detail-refresh-button"),
  jobsList: document.querySelector("#jobs-list"),
  jobDetailId: document.querySelector("#job-detail-id"),
  jobDetail: document.querySelector("#job-detail"),

  clientsStateBadge: document.querySelector("#clients-state-badge"),
  clientsRefreshButton: document.querySelector("#clients-refresh-button"),
  clientClearQueuesButton: document.querySelector("#client-clear-queues-button"),
  clientsList: document.querySelector("#clients-list"),
  clientDetailId: document.querySelector("#client-detail-id"),
  clientDetail: document.querySelector("#client-detail"),

  jobName: document.querySelector("#job-name"),
  jobAutoAssign: document.querySelector("#job-auto-assign"),
  jobTargetClient: document.querySelector("#job-target-client"),
  jobClientId: document.querySelector("#job-client-id"),
  jobGroupId: document.querySelector("#job-group-id"),
  jobGroupName: document.querySelector("#job-group-name"),
  jobMetadataJson: document.querySelector("#job-metadata-json"),
  jobWorkflowSource: document.querySelector("#job-workflow-source"),
  jobFragmentSelect: document.querySelector("#job-fragment-select"),
  jobFragmentAppendButton: document.querySelector("#job-fragment-append-button"),
  jobFragmentReplaceButton: document.querySelector("#job-fragment-replace-button"),
  jobImportButton: document.querySelector("#job-import-button"),
  jobClearButton: document.querySelector("#job-clear-button"),
  jobStepList: document.querySelector("#job-step-list"),
  jobStepUpButton: document.querySelector("#job-step-up-button"),
  jobStepDownButton: document.querySelector("#job-step-down-button"),
  jobStepRemoveButton: document.querySelector("#job-step-remove-button"),
  jobStepKind: document.querySelector("#job-step-kind"),
  jobActionSelect: document.querySelector("#job-action-select"),
  jobStepAction: document.querySelector("#job-step-action"),
  jobStepDisplayName: document.querySelector("#job-step-display-name"),
  jobStepTimeout: document.querySelector("#job-step-timeout"),
  jobStepParams: document.querySelector("#job-step-params"),
  jobBuilderHelp: document.querySelector("#job-builder-help"),
  jobStepAddButton: document.querySelector("#job-step-add-button"),
  jobStepInsertButton: document.querySelector("#job-step-insert-button"),
  jobStepUpdateButton: document.querySelector("#job-step-update-button"),
  jobPreview: document.querySelector("#job-preview"),
  jobSubmitButton: document.querySelector("#job-submit-button"),
  jobSubmitLog: document.querySelector("#job-submit-log"),
  jobSubmitState: document.querySelector("#job-submit-state"),
};

const state = {
  uploadOperationId: null,
  ingestOperationId: null,
  selectedJobId: "",
  selectedClientId: "",
  jobs: [],
  clients: [],
};

let builder = {
  setTargetClient() {},
};

function setStatus(message, level = "idle") {
  elements.bottomStatusText.textContent = message;
  elements.systemSummary.textContent = `ORCHESTRATOR WEB | ${message}`;
  elements.bottomStatusText.className = `state-${level}`;
}

function setBadge(element, status, label) {
  element.className = `state-badge state-${status}`;
  element.textContent = label;
}

function renderOperation(operation, badge, progressFill, logPanel, bottomStatusLabel) {
  const status = operation?.status || "idle";
  const label = String(status).toUpperCase();
  setBadge(badge, status, label);
  const total = Number(operation?.progress_total || 0);
  const done = Number(operation?.progress_done || 0);
  const percent = total > 0 ? (done / total) * 100 : status === "done" ? 100 : 0;
  progressFill.style.width = `${clampPercent(percent)}%`;
  logPanel.textContent = operation ? prettyJSON(operation) : "";
  bottomStatusLabel.textContent = `${badge === elements.uploadStateBadge ? "UPLOAD" : "INGEST"}:${label}`;
}

async function pollOperation(operationId, render) {
  if (!operationId) {
    return;
  }
  const operation = await getJSON(`/ui/api/operations/${operationId}`);
  render(operation);
  if (operation.status === "running" || operation.status === "queued") {
    window.setTimeout(() => {
      pollOperation(operationId, render).catch((error) => setStatus(String(error), "error"));
    }, 800);
  }
}

async function loadDirectories(path = elements.uploadCurrentPath.value) {
  const query = path ? `?path=${encodeURIComponent(path)}` : "";
  const payload = await getJSON(`/ui/api/upload/directories${query}`);
  elements.uploadCurrentPath.value = payload.path;
  elements.uploadDirectoryList.replaceChildren();
  for (const item of payload.directories) {
    const button = document.createElement("button");
    button.type = "button";
    button.className = "directory-entry";
    button.innerHTML = `<span>${item.name}</span><span class="muted">${item.has_children ? "DIR+" : "DIR"}</span>`;
    button.addEventListener("click", () => {
      elements.uploadCurrentPath.value = item.path;
      loadDirectories(item.path).catch((error) => setStatus(String(error), "error"));
    });
    elements.uploadDirectoryList.append(button);
  }
  elements.uploadPathUpButton.dataset.parent = payload.parent || "";
}

async function startUpload() {
  const operation = await postJSON("/ui/api/upload", { path: elements.uploadCurrentPath.value.trim() });
  state.uploadOperationId = operation.operation_id;
  renderOperation(operation, elements.uploadStateBadge, elements.uploadProgress, elements.uploadOperationLog, elements.bottomUploadStatus);
  setStatus(`Upload queued for ${elements.uploadCurrentPath.value.trim()}`);
  await pollOperation(state.uploadOperationId, (value) =>
    renderOperation(value, elements.uploadStateBadge, elements.uploadProgress, elements.uploadOperationLog, elements.bottomUploadStatus),
  );
}

async function startIngest() {
  const payload = {
    group_name: elements.ingestGroupName.value.trim() || null,
    limit: elements.ingestLimit.value ? Number(elements.ingestLimit.value) : null,
  };
  const operation = await postJSON("/ui/api/ingest", payload);
  state.ingestOperationId = operation.operation_id;
  renderOperation(operation, elements.ingestStateBadge, elements.ingestProgress, elements.ingestOperationLog, elements.bottomIngestStatus);
  setStatus(`Ingest queued group=${payload.group_name || "-"} limit=${payload.limit || "-"}`);
  await pollOperation(state.ingestOperationId, (value) =>
    renderOperation(value, elements.ingestStateBadge, elements.ingestProgress, elements.ingestOperationLog, elements.bottomIngestStatus),
  );
}

function renderJobs() {
  elements.jobsList.replaceChildren();
  for (const job of state.jobs) {
    const button = document.createElement("button");
    button.type = "button";
    button.className = `job-row${job.job_id === state.selectedJobId ? " is-selected" : ""}`;
    const percent = clampPercent(job.progress || 0);
    button.innerHTML = `
      <div class="job-row-main">
        <span>${job.job_name || job.job_id}</span>
        <span class="job-row-meta">${job.status} | ${job.message || "-"}</span>
      </div>
      <div class="job-row-progress">
        <div class="progress-track"><div class="progress-fill" style="width:${percent}%"></div></div>
        <span>${percent.toFixed(0)}%</span>
      </div>
    `;
    button.addEventListener("click", () => {
      state.selectedJobId = job.job_id;
      elements.jobDetailId.value = job.job_id;
      renderJobs();
      loadJobDetail(job.job_id).catch((error) => setStatus(String(error), "error"));
    });
    elements.jobsList.append(button);
  }
}

async function refreshJobs() {
  state.jobs = await getJSON("/jobs");
  setBadge(elements.jobsStateBadge, "done", "READY");
  renderJobs();
}

async function loadJobDetail(jobId = elements.jobDetailId.value.trim()) {
  if (!jobId) {
    throw new Error("job id is required");
  }
  const payload = await getJSON(`/jobs/${jobId}`);
  elements.jobDetail.textContent = prettyJSON(payload);
  state.selectedJobId = jobId;
  renderJobs();
}

function renderClients() {
  elements.clientsList.replaceChildren();
  for (const client of state.clients) {
    const button = document.createElement("button");
    button.type = "button";
    button.className = `client-row${client.client_id === state.selectedClientId ? " is-selected" : ""}`;
    const age = client.heartbeat_age == null ? "n/a" : `${client.heartbeat_age}s`;
    button.innerHTML = `
      <div class="client-row-main">
        <span>${client.client_id}</span>
        <span class="client-row-meta">queue=${client.queue_depth ?? "n/a"} | age=${age}</span>
      </div>
      <span>${client.heartbeat?.status || "no-heartbeat"}</span>
    `;
    button.addEventListener("click", () => {
      state.selectedClientId = client.client_id;
      elements.clientDetailId.value = client.client_id;
      builder.setTargetClient(client.client_id);
      renderClients();
      loadClientDetail(client.client_id).catch((error) => setStatus(String(error), "error"));
    });
    elements.clientsList.append(button);
  }
}

async function refreshClients() {
  const payload = await getJSON("/ui/api/clients");
  state.clients = payload.clients || [];
  setBadge(elements.clientsStateBadge, "done", "READY");
  renderClients();
}

async function loadClientDetail(clientId = elements.clientDetailId.value.trim()) {
  if (!clientId) {
    throw new Error("client id is required");
  }
  const payload = await getJSON(`/ui/api/clients/${clientId}`);
  elements.clientDetail.textContent = prettyJSON(payload);
  state.selectedClientId = clientId;
  builder.setTargetClient(clientId);
  renderClients();
}

async function clearClientQueues() {
  const clientId = elements.clientDetailId.value.trim();
  if (!clientId) {
    throw new Error("client id is required");
  }
  const payload = await postJSON(`/ui/api/clients/${clientId}/clear-queues`, {});
  elements.clientDetail.textContent = prettyJSON(payload);
  setStatus(`Cleared queues for ${clientId}`);
  await refreshClients();
}

function refreshClock() {
  elements.bottomClock.textContent = formatClock();
}

async function refreshSelectedDetail() {
  const updates = [];
  if (state.selectedJobId) {
    updates.push(loadJobDetail(state.selectedJobId));
  }
  if (state.selectedClientId) {
    updates.push(loadClientDetail(state.selectedClientId));
  }
  await Promise.all(updates);
}

async function bootstrap() {
  const metadata = await getJSON("/ui/api/job-builder/metadata");
  builder = createJobBuilder(elements, metadata, {
    setStatus,
    setSubmitState(status, label) {
      setBadge(elements.jobSubmitState, status, label);
    },
    onJobSubmitted(job) {
      setStatus(`Dispatched ${job.job_id}`, "done");
      elements.jobDetailId.value = job.job_id;
      state.selectedJobId = job.job_id;
      refreshJobs().catch((error) => setStatus(String(error), "error"));
      loadJobDetail(job.job_id).catch((error) => setStatus(String(error), "error"));
    },
  });

  elements.refreshAllButton.addEventListener("click", () => {
    Promise.all([refreshJobs(), refreshClients(), loadDirectories(elements.uploadCurrentPath.value), refreshSelectedDetail()])
      .then(() => setStatus("Refreshed all panels", "done"))
      .catch((error) => setStatus(String(error), "error"));
  });
  elements.selectFirstClientButton.addEventListener("click", () => {
    const first = state.clients[0];
    if (!first) {
      setStatus("No active clients available", "error");
      return;
    }
    elements.clientDetailId.value = first.client_id;
    loadClientDetail(first.client_id).catch((error) => setStatus(String(error), "error"));
  });
  elements.uploadPathRefreshButton.addEventListener("click", () => {
    loadDirectories(elements.uploadCurrentPath.value).catch((error) => setStatus(String(error), "error"));
  });
  elements.uploadPathUpButton.addEventListener("click", () => {
    const parent = elements.uploadPathUpButton.dataset.parent;
    if (parent) {
      loadDirectories(parent).catch((error) => setStatus(String(error), "error"));
    }
  });
  elements.uploadStartButton.addEventListener("click", () => {
    startUpload().catch((error) => setStatus(String(error), "error"));
  });
  elements.ingestStartButton.addEventListener("click", () => {
    startIngest().catch((error) => setStatus(String(error), "error"));
  });
  elements.jobsRefreshButton.addEventListener("click", () => {
    refreshJobs().catch((error) => setStatus(String(error), "error"));
  });
  elements.jobDetailRefreshButton.addEventListener("click", () => {
    loadJobDetail().catch((error) => setStatus(String(error), "error"));
  });
  elements.clientsRefreshButton.addEventListener("click", () => {
    refreshClients().catch((error) => setStatus(String(error), "error"));
  });
  elements.clientClearQueuesButton.addEventListener("click", () => {
    clearClientQueues().catch((error) => setStatus(String(error), "error"));
  });

  refreshClock();
  window.setInterval(refreshClock, 1000);
  await Promise.all([loadDirectories(), refreshJobs(), refreshClients()]);
  window.setInterval(() => {
    refreshJobs().catch(() => {});
    refreshClients().catch(() => {});
    refreshSelectedDetail().catch(() => {});
  }, 5000);
  setStatus("Dashboard ready", "done");
}

bootstrap().catch((error) => setStatus(String(error), "error"));
