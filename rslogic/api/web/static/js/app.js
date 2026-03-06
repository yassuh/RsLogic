import { clampPercent, formatClock, getJSON, postJSON, prettyJSON } from "./api.js";
import { createJobBuilder } from "./job-builder.js";
import { createImagesView } from "./images-view.js";
import { buildDashboard } from "./ui-layout.js";
import { element, listRowButton, textStack } from "./ui-components.js";

const root = document.querySelector("#app-root");
const elements = buildDashboard(root);

const state = {
  activeWorkflow: "upload",
  uploadOperationId: null,
  ingestOperationId: null,
  selectedJobId: "",
  selectedClientId: "",
  jobs: [],
  clients: [],
};

const workflowLabels = {
  upload: "UPLOAD",
  build: "BUILD",
  jobs: "JOBS",
  clients: "CLIENTS",
  images: "IMAGES",
};

let builder = {
  setTargetClient() {},
};
let imagesView = {
  activate() {},
  refresh: async () => {},
};

function setStatus(message, level = "idle") {
  elements.bottomStatusText.textContent = message;
  elements.systemSummary.textContent = `ORCHESTRATOR WEB | ${workflowLabels[state.activeWorkflow]} | ${message}`;
  elements.bottomStatusText.className = `state-${level}`;
}

function setBadge(elementNode, status, label) {
  elementNode.className = `state-badge state-${status}`;
  elementNode.textContent = label;
}

function setActiveWorkflow(workflow) {
  if (!elements.workflowViews?.[workflow] || !elements.workflowButtons?.[workflow]) {
    throw new Error(`unknown workflow ${workflow}`);
  }
  state.activeWorkflow = workflow;
  for (const [key, panel] of Object.entries(elements.workflowViews)) {
    panel.classList.toggle("is-active", key === workflow);
  }
  for (const [key, button] of Object.entries(elements.workflowButtons)) {
    button.classList.toggle("is-active", key === workflow);
  }
  elements.workflowSummary.textContent = `WORKFLOW:${workflowLabels[workflow]}`;
  if (workflow === "images") {
    imagesView.activate();
  }
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

function createDirectoryRow(item) {
  const button = listRowButton({
    className: "directory-entry",
    children: [
      element("span", { text: item.name }),
      element("span", { className: "muted", text: item.has_children ? "DIR+" : "DIR" }),
    ],
  });
  button.addEventListener("click", () => {
    elements.uploadCurrentPath.value = item.path;
    loadDirectories(item.path).catch((error) => setStatus(String(error), "error"));
  });
  return button;
}

async function loadDirectories(path = elements.uploadCurrentPath.value) {
  const query = path ? `?path=${encodeURIComponent(path)}` : "";
  const payload = await getJSON(`/ui/api/upload/directories${query}`);
  elements.uploadCurrentPath.value = payload.path;
  elements.uploadDirectoryList.replaceChildren();
  for (const item of payload.directories) {
    elements.uploadDirectoryList.append(createDirectoryRow(item));
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

function createJobRow(job) {
  const percent = clampPercent(job.progress || 0);
  const button = listRowButton({
    className: "job-row",
    selected: job.job_id === state.selectedJobId,
    children: [
      textStack(job.job_name || job.job_id, `${job.status} | ${job.message || "-"}`, "job-row-main", "job-row-meta"),
      element("div", {
        className: "job-row-progress",
        children: [
          element("div", {
            className: "progress-track",
            children: [element("div", { className: "progress-fill", attrs: { style: `width:${percent}%` } })],
          }),
          element("span", { text: `${percent.toFixed(0)}%` }),
        ],
      }),
    ],
  });
  button.addEventListener("click", () => {
    state.selectedJobId = job.job_id;
    elements.jobDetailId.value = job.job_id;
    renderJobs();
    loadJobDetail(job.job_id).catch((error) => setStatus(String(error), "error"));
  });
  return button;
}

function renderJobs() {
  elements.jobsList.replaceChildren();
  for (const job of state.jobs) {
    elements.jobsList.append(createJobRow(job));
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

function createClientRow(client) {
  const age = client.heartbeat_age == null ? "n/a" : `${client.heartbeat_age}s`;
  const button = listRowButton({
    className: "client-row",
    selected: client.client_id === state.selectedClientId,
    children: [
      textStack(client.client_id, `queue=${client.queue_depth ?? "n/a"} | age=${age}`, "client-row-main", "client-row-meta"),
      element("span", { text: client.heartbeat?.status || "no-heartbeat" }),
    ],
  });
  button.addEventListener("click", () => {
    state.selectedClientId = client.client_id;
    elements.clientDetailId.value = client.client_id;
    builder.setTargetClient(client.client_id);
    renderClients();
    loadClientDetail(client.client_id).catch((error) => setStatus(String(error), "error"));
  });
  return button;
}

function renderClients() {
  elements.clientsList.replaceChildren();
  for (const client of state.clients) {
    elements.clientsList.append(createClientRow(client));
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
      setActiveWorkflow("jobs");
      setStatus(`Dispatched ${job.job_id}`, "done");
      elements.jobDetailId.value = job.job_id;
      state.selectedJobId = job.job_id;
      refreshJobs().catch((error) => setStatus(String(error), "error"));
      loadJobDetail(job.job_id).catch((error) => setStatus(String(error), "error"));
    },
  });
  imagesView = createImagesView(elements, { setStatus });

  for (const [workflow, button] of Object.entries(elements.workflowButtons)) {
    button.addEventListener("click", () => {
      setActiveWorkflow(workflow);
      setStatus(`${workflowLabels[workflow]} workflow active`, "done");
    });
  }

  elements.refreshAllButton.addEventListener("click", () => {
    Promise.all([refreshJobs(), refreshClients(), imagesView.refresh(), loadDirectories(elements.uploadCurrentPath.value), refreshSelectedDetail()])
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
  setActiveWorkflow(state.activeWorkflow);
  window.setInterval(refreshClock, 1000);
  await Promise.all([loadDirectories(), refreshJobs(), refreshClients(), imagesView.refresh()]);
  window.setInterval(() => {
    refreshJobs().catch(() => {});
    refreshClients().catch(() => {});
    refreshSelectedDetail().catch(() => {});
  }, 5000);
  setStatus("Dashboard ready", "done");
}

bootstrap().catch((error) => setStatus(String(error), "error"));
