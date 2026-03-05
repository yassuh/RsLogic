import { postJSON, prettyJSON } from "./api.js";

function clone(value) {
  return JSON.parse(JSON.stringify(value));
}

function createEmptyDraft() {
  return {
    job_name: "",
    auto_assign: true,
    target_client: "",
    client_id: "",
    group_id: "",
    group_name: "",
    metadata: {},
    steps: [],
  };
}

function usesFileStage(steps) {
  return steps.some(
    (step) =>
      String(step.kind || "").toLowerCase() === "file" &&
      ["stage", "file_stage", "file_stage_group"].includes(String(step.action || "").toLowerCase()),
  );
}

function normalizeStep(step) {
  return {
    kind: String(step.kind || "sdk").trim().toLowerCase(),
    action: String(step.action || "").trim().toLowerCase(),
    params: step.params && typeof step.params === "object" ? step.params : {},
    timeout_s: Math.max(0, Number.parseInt(step.timeout_s ?? 600, 10) || 0),
    display_name: String(step.display_name || "").trim() || undefined,
  };
}

function previewLines(draft) {
  const lines = [
    `job_name=${draft.job_name || "-"} auto_assign=${draft.auto_assign} steps=${draft.steps.length}`,
    `group=${draft.group_id || draft.group_name || "-"} target=${draft.client_id || draft.target_client || "-"}`,
    `metadata=${JSON.stringify(draft.metadata)}`,
  ];
  draft.steps.forEach((step, index) => {
    lines.push(
      `${index + 1}. ${step.kind}:${step.action} timeout=${step.timeout_s ?? 600} display=${step.display_name || "-"} params=${JSON.stringify(step.params || {})}`,
    );
  });
  return lines;
}

function actionEntries(metadata, kind) {
  return Object.entries(kind === "file" ? metadata.actions.file_steps || {} : metadata.actions.sdk_steps || {});
}

function actionDetails(metadata, kind, action) {
  const entry = (kind === "file" ? metadata.actions.file_steps : metadata.actions.sdk_steps)?.[action];
  if (!entry) {
    return [`kind=${kind}`, `action=${action || "<blank>"}`, "catalog=custom or dynamic action"];
  }
  const lines = [`kind=${kind}`, `action=${action}`];
  if (entry.description) {
    lines.push(entry.description);
  } else if (entry.method) {
    lines.push(entry.method);
  }
  if (entry.params) {
    lines.push(`params=${JSON.stringify(entry.params)}`);
  }
  if (Array.isArray(entry.required_params) && entry.required_params.length) {
    lines.push(`required=${entry.required_params.join(", ")}`);
  }
  if (Array.isArray(entry.requires_params) && entry.requires_params.length) {
    lines.push(`required=${entry.requires_params.join(", ")}`);
  }
  if (Array.isArray(entry.optional_params) && entry.optional_params.length) {
    lines.push(`optional=${entry.optional_params.join(", ")}`);
  }
  return lines;
}

function fragmentDetails(metadata, key) {
  const fragment = metadata.fragments.find((item) => item.key === key);
  if (!fragment) {
    return ["fragment=<unknown>"];
  }
  return [
    `fragment=${fragment.label}`,
    fragment.description,
    `steps=${fragment.steps.length}`,
  ];
}

export function createJobBuilder(elements, metadata, hooks) {
  const state = {
    metadata,
    draft: createEmptyDraft(),
    selectedStepIndex: 0,
  };

  const firstFragment = metadata.fragments[0];
  if (firstFragment) {
    state.draft.steps = clone(firstFragment.steps).map(normalizeStep);
  }

  function syncRootFieldsFromForm() {
    state.draft.job_name = elements.jobName.value.trim();
    state.draft.auto_assign = elements.jobAutoAssign.checked;
    state.draft.target_client = elements.jobTargetClient.value.trim();
    state.draft.client_id = elements.jobClientId.value.trim();
    state.draft.group_id = elements.jobGroupId.value.trim();
    state.draft.group_name = elements.jobGroupName.value.trim();
    const metadataText = elements.jobMetadataJson.value.trim() || "{}";
    const parsed = JSON.parse(metadataText);
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
      throw new Error("metadata JSON must be an object");
    }
    state.draft.metadata = parsed;
  }

  function syncFormFromRootFields() {
    elements.jobName.value = state.draft.job_name || "";
    elements.jobAutoAssign.checked = Boolean(state.draft.auto_assign);
    elements.jobTargetClient.value = state.draft.target_client || "";
    elements.jobClientId.value = state.draft.client_id || "";
    elements.jobGroupId.value = state.draft.group_id || "";
    elements.jobGroupName.value = state.draft.group_name || "";
    elements.jobMetadataJson.value = prettyJSON(state.draft.metadata || {});
  }

  function syncActionSelect() {
    const kind = elements.jobStepKind.value || "sdk";
    const entries = actionEntries(metadata, kind);
    elements.jobActionSelect.replaceChildren();
    const blank = document.createElement("option");
    blank.value = "";
    blank.textContent = "custom";
    elements.jobActionSelect.append(blank);
    for (const [action, entry] of entries) {
      const option = document.createElement("option");
      option.value = action;
      option.textContent = entry.description ? `${action} | ${entry.description}` : action;
      elements.jobActionSelect.append(option);
    }
    if (entries.some(([action]) => action === elements.jobStepAction.value.trim())) {
      elements.jobActionSelect.value = elements.jobStepAction.value.trim();
    } else {
      elements.jobActionSelect.value = "";
    }
  }

  function renderHelp() {
    const lines = [
      ...fragmentDetails(metadata, elements.jobFragmentSelect.value),
      "",
      ...actionDetails(metadata, elements.jobStepKind.value || "sdk", elements.jobStepAction.value.trim()),
    ];
    elements.jobBuilderHelp.textContent = lines.join("\n");
  }

  function renderStepList() {
    elements.jobStepList.replaceChildren();
    state.draft.steps.forEach((step, index) => {
      const button = document.createElement("button");
      button.type = "button";
      button.className = `step-entry${index === state.selectedStepIndex ? " is-selected" : ""}`;
      button.innerHTML = `<span>${index + 1}. ${step.kind}:${step.action}${step.display_name ? ` [${step.display_name}]` : ""}</span><span class="muted">${step.timeout_s ?? 600}s</span>`;
      button.addEventListener("click", () => {
        state.selectedStepIndex = index;
        loadEditorFromSelected();
        render();
      });
      elements.jobStepList.append(button);
    });
  }

  function renderPreview() {
    elements.jobPreview.textContent = previewLines(state.draft).join("\n");
  }

  function render() {
    syncFormFromRootFields();
    syncActionSelect();
    renderHelp();
    renderStepList();
    renderPreview();
  }

  function selectLastStep() {
    state.selectedStepIndex = Math.max(0, state.draft.steps.length - 1);
  }

  function loadEditorFromSelected() {
    const step = state.draft.steps[state.selectedStepIndex];
    if (!step) {
      elements.jobStepKind.value = "sdk";
      elements.jobStepAction.value = "";
      elements.jobStepDisplayName.value = "";
      elements.jobStepTimeout.value = "600";
      elements.jobStepParams.value = "{}";
      syncActionSelect();
      renderHelp();
      return;
    }
    elements.jobStepKind.value = step.kind || "sdk";
    elements.jobStepAction.value = step.action || "";
    elements.jobStepDisplayName.value = step.display_name || "";
    elements.jobStepTimeout.value = String(step.timeout_s ?? 600);
    elements.jobStepParams.value = prettyJSON(step.params || {});
    syncActionSelect();
    renderHelp();
  }

  function readEditorStep() {
    const paramsText = elements.jobStepParams.value.trim() || "{}";
    const params = JSON.parse(paramsText);
    if (!params || typeof params !== "object" || Array.isArray(params)) {
      throw new Error("step params JSON must be an object");
    }
    return normalizeStep({
      kind: elements.jobStepKind.value,
      action: elements.jobStepAction.value,
      display_name: elements.jobStepDisplayName.value,
      timeout_s: elements.jobStepTimeout.value,
      params,
    });
  }

  function applyFragment({ replace }) {
    const fragment = metadata.fragments.find((item) => item.key === elements.jobFragmentSelect.value);
    if (!fragment) {
      throw new Error("fragment not found");
    }
    syncRootFieldsFromForm();
    const nextSteps = clone(fragment.steps).map(normalizeStep);
    if (replace) {
      state.draft.steps = nextSteps;
      state.selectedStepIndex = 0;
    } else {
      state.draft.steps.push(...nextSteps);
      selectLastStep();
    }
    loadEditorFromSelected();
    render();
    hooks.setStatus(`${replace ? "Replaced" : "Appended"} fragment ${fragment.label}`);
  }

  function buildRequest() {
    syncRootFieldsFromForm();
    const steps = state.draft.steps.map(normalizeStep);
    if (!steps.length) {
      throw new Error("job must contain at least one step");
    }
    if (!state.draft.auto_assign && !state.draft.target_client && !state.draft.client_id) {
      throw new Error("target_client or client_id is required when auto_assign is false");
    }
    if (usesFileStage(steps) && !state.draft.group_id && !state.draft.group_name) {
      throw new Error("file stage workflows require group_id or group_name");
    }
    return {
      job_name: state.draft.job_name || undefined,
      auto_assign: state.draft.auto_assign,
      target_client: state.draft.target_client || undefined,
      client_id: state.draft.client_id || undefined,
      group_id: state.draft.group_id || undefined,
      group_name: state.draft.group_name || undefined,
      metadata: state.draft.metadata || {},
      steps,
    };
  }

  function withStatus(action) {
    return async () => {
      try {
        await action();
      } catch (error) {
        hooks.setStatus(String(error), "error");
      }
    };
  }

  elements.jobFragmentSelect.replaceChildren();
  for (const fragment of metadata.fragments) {
    const option = document.createElement("option");
    option.value = fragment.key;
    option.textContent = fragment.label;
    elements.jobFragmentSelect.append(option);
  }
  if (metadata.fragments[0]) {
    elements.jobFragmentSelect.value = metadata.fragments[0].key;
  }

  elements.jobFragmentAppendButton.addEventListener("click", withStatus(() => applyFragment({ replace: false })));
  elements.jobFragmentReplaceButton.addEventListener("click", withStatus(() => applyFragment({ replace: true })));

  elements.jobImportButton.addEventListener("click", withStatus(async () => {
    const text = elements.jobWorkflowSource.value.trim();
    if (!text) {
      throw new Error("workflow JSON or path is required");
    }
    const payload = await postJSON("/ui/api/job-builder/import", { source: text });
    state.draft.steps = payload.steps.map(normalizeStep);
    state.selectedStepIndex = 0;
    loadEditorFromSelected();
    render();
    hooks.setStatus(`Imported ${state.draft.steps.length} steps`);
  }));

  elements.jobClearButton.addEventListener("click", withStatus(() => {
    state.draft = createEmptyDraft();
    state.selectedStepIndex = 0;
    elements.jobWorkflowSource.value = "";
    loadEditorFromSelected();
    render();
    hooks.setStatus("Cleared job draft");
  }));

  elements.jobStepKind.addEventListener("change", () => {
    syncActionSelect();
    renderHelp();
  });
  elements.jobActionSelect.addEventListener("change", () => {
    if (elements.jobActionSelect.value) {
      elements.jobStepAction.value = elements.jobActionSelect.value;
    }
    renderHelp();
  });
  elements.jobStepAction.addEventListener("input", () => {
    syncActionSelect();
    renderHelp();
  });

  elements.jobStepAddButton.addEventListener("click", withStatus(() => {
    const step = readEditorStep();
    state.draft.steps.push(step);
    selectLastStep();
    render();
    hooks.setStatus(`Added ${step.action}`);
  }));
  elements.jobStepInsertButton.addEventListener("click", withStatus(() => {
    if (!state.draft.steps.length) {
      throw new Error("select a step before inserting");
    }
    const step = readEditorStep();
    state.draft.steps.splice(state.selectedStepIndex, 0, step);
    render();
    hooks.setStatus(`Inserted ${step.action}`);
  }));
  elements.jobStepUpdateButton.addEventListener("click", withStatus(() => {
    if (!state.draft.steps[state.selectedStepIndex]) {
      throw new Error("select a step before updating");
    }
    const step = readEditorStep();
    state.draft.steps[state.selectedStepIndex] = step;
    render();
    hooks.setStatus(`Updated ${step.action}`);
  }));
  elements.jobStepRemoveButton.addEventListener("click", withStatus(() => {
    if (!state.draft.steps[state.selectedStepIndex]) {
      throw new Error("select a step before removing");
    }
    const [removed] = state.draft.steps.splice(state.selectedStepIndex, 1);
    state.selectedStepIndex = Math.max(0, Math.min(state.selectedStepIndex, state.draft.steps.length - 1));
    loadEditorFromSelected();
    render();
    hooks.setStatus(`Removed ${removed.action}`);
  }));
  elements.jobStepUpButton.addEventListener("click", withStatus(() => {
    if (state.selectedStepIndex <= 0) {
      return;
    }
    const [step] = state.draft.steps.splice(state.selectedStepIndex, 1);
    state.selectedStepIndex -= 1;
    state.draft.steps.splice(state.selectedStepIndex, 0, step);
    render();
  }));
  elements.jobStepDownButton.addEventListener("click", withStatus(() => {
    if (state.selectedStepIndex >= state.draft.steps.length - 1) {
      return;
    }
    const [step] = state.draft.steps.splice(state.selectedStepIndex, 1);
    state.selectedStepIndex += 1;
    state.draft.steps.splice(state.selectedStepIndex, 0, step);
    render();
  }));

  elements.jobSubmitButton.addEventListener("click", withStatus(async () => {
    const request = buildRequest();
    hooks.setSubmitState("running", "SUBMITTING");
    elements.jobSubmitLog.textContent = prettyJSON(request);
    try {
      const job = await postJSON("/jobs", request);
      elements.jobSubmitLog.textContent = prettyJSON(job);
      hooks.setSubmitState("done", "DISPATCHED");
      hooks.onJobSubmitted(job);
    } catch (error) {
      hooks.setSubmitState("error", "ERROR");
      elements.jobSubmitLog.textContent = String(error);
      hooks.setStatus(String(error), "error");
    }
  }));

  syncFormFromRootFields();
  loadEditorFromSelected();
  render();

  return {
    setTargetClient(clientId) {
      state.draft.target_client = clientId || "";
      syncFormFromRootFields();
      renderPreview();
    },
  };
}
