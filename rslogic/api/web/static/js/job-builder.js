import { postJSON, prettyJSON } from "./api.js";
import { element, fieldRow, input, listRowButton, moveStack, textarea } from "./ui-components.js";

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
      String(step.action || "").toLowerCase() === "stage",
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

function actionEntry(metadata, kind, action) {
  return (kind === "file" ? metadata.actions.file_steps : metadata.actions.sdk_steps)?.[action];
}

function parseJSONObject(text, fallback = {}) {
  const raw = String(text || "").trim();
  if (!raw) {
    return clone(fallback);
  }
  const parsed = JSON.parse(raw);
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
    throw new Error("step params JSON must be an object");
  }
  return parsed;
}

function paramDefinitions(metadata, kind, action) {
  const entry = actionEntry(metadata, kind, action);
  if (!entry) {
    return [];
  }
  const required = new Set([...(entry.required_params || []), ...(entry.requires_params || [])].map((value) => String(value)));
  const optional = new Set((entry.optional_params || []).map((value) => String(value)));
  const definitions = new Map();

  if (entry.params && typeof entry.params === "object") {
    for (const [name, type] of Object.entries(entry.params)) {
      const typeLabel = String(type || "string");
      const inferredRequired = /\brequired\b/i.test(typeLabel);
      definitions.set(name, {
        name,
        typeLabel,
        required: required.has(name) || inferredRequired,
      });
    }
  }

  for (const name of required) {
    if (!definitions.has(name)) {
      definitions.set(name, { name, typeLabel: "string", required: true });
    } else {
      definitions.get(name).required = true;
    }
  }
  for (const name of optional) {
    if (!definitions.has(name)) {
      definitions.set(name, { name, typeLabel: "string", required: false });
    }
  }

  return [...definitions.values()];
}

function paramInputMode(typeLabel) {
  const normalized = String(typeLabel || "").toLowerCase();
  if (normalized.includes("bool")) {
    return "bool";
  }
  if (normalized.includes("dict") || normalized.includes("list") || normalized.includes("json") || normalized.includes("object")) {
    return "json";
  }
  if (normalized.includes("int")) {
    return "int";
  }
  if (normalized.includes("float") || normalized.includes("number")) {
    return "float";
  }
  return "text";
}

function formatParamValue(definition, value) {
  if (value == null) {
    return "";
  }
  const mode = paramInputMode(definition.typeLabel);
  if (mode === "json") {
    return prettyJSON(value);
  }
  if (mode === "bool") {
    return Boolean(value);
  }
  return String(value);
}

function parseParamValue(definition, control) {
  const mode = paramInputMode(definition.typeLabel);
  if (mode === "bool") {
    return Boolean(control.checked);
  }
  const raw = String(control.value || "").trim();
  if (!raw) {
    return undefined;
  }
  if (mode === "int") {
    const parsed = Number.parseInt(raw, 10);
    if (!Number.isFinite(parsed)) {
      throw new Error(`param ${definition.name} must be an integer`);
    }
    return parsed;
  }
  if (mode === "float") {
    const parsed = Number.parseFloat(raw);
    if (!Number.isFinite(parsed)) {
      throw new Error(`param ${definition.name} must be a number`);
    }
    return parsed;
  }
  if (mode === "json") {
    try {
      return JSON.parse(raw);
    } catch {
      throw new Error(`param ${definition.name} must be valid JSON`);
    }
  }
  return raw;
}

function actionDetails(metadata, kind, action) {
  const entry = actionEntry(metadata, kind, action);
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
    paramControls: [],
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
    const entries = actionEntries(metadata, kind).sort(([left], [right]) => left.localeCompare(right));
    elements.jobActionSelect.replaceChildren();
    const blank = document.createElement("option");
    blank.value = "";
    blank.textContent = "select action";
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

  function maybeApplyActionTemplate(kind, action, previousAction = "") {
    const currentText = elements.jobStepParams.value.trim();
    if (!currentText || currentText === "{}" || previousAction !== action) {
      elements.jobStepParams.value = "{}";
    }
  }

  function mergeEditorParamsFromFields() {
    const current = parseJSONObject(elements.jobStepParams.value, {});
    for (const definition of paramDefinitions(metadata, elements.jobStepKind.value || "sdk", elements.jobStepAction.value.trim())) {
      delete current[definition.name];
    }
    for (const { definition, control } of state.paramControls) {
      const value = parseParamValue(definition, control);
      if (value !== undefined) {
        current[definition.name] = value;
      }
    }
    return current;
  }

  function syncParamsJsonFromFields() {
    const merged = mergeEditorParamsFromFields();
    elements.jobStepParams.value = prettyJSON(merged);
  }

  function renderParamFields() {
    elements.jobStepParamFields.replaceChildren();
    state.paramControls = [];
    const definitions = paramDefinitions(metadata, elements.jobStepKind.value || "sdk", elements.jobStepAction.value.trim());
    if (!definitions.length) {
      elements.jobStepParamFields.append(
        element("div", {
          className: "job-step-param-empty",
          text: "No cataloged parameters. Use PARAMS JSON for custom payloads.",
        }),
      );
      return;
    }

    let params = {};
    try {
      params = parseJSONObject(elements.jobStepParams.value, {});
    } catch {
      params = {};
    }

    for (const definition of definitions) {
      const mode = paramInputMode(definition.typeLabel);
      const control =
        mode === "json"
          ? textarea({
              id: `job-step-param-${definition.name}`,
              spellcheck: false,
              text: formatParamValue(definition, params[definition.name]),
            })
          : input({
              id: `job-step-param-${definition.name}`,
              type: mode === "bool" ? "checkbox" : mode === "text" ? "text" : "number",
              spellcheck: false,
              value: mode === "bool" ? undefined : formatParamValue(definition, params[definition.name]),
              checked: mode === "bool" ? formatParamValue(definition, params[definition.name]) : undefined,
            });
      if (mode === "int" || mode === "float") {
        control.step = mode === "int" ? "1" : "any";
      }
      control.title = definition.typeLabel;
      const row = fieldRow({
        className: `field-row${mode === "bool" ? " checkbox-row" : ""}`,
        label: definition.required ? `${definition.name}*` : definition.name,
        control,
      });
      elements.jobStepParamFields.append(row);
      state.paramControls.push({ definition, control });
      control.addEventListener(mode === "bool" ? "change" : "input", () => {
        try {
          syncParamsJsonFromFields();
        } catch (error) {
          hooks.setStatus(String(error), "error");
        }
      });
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
      const row = element("div", {
        className: `step-row${index === state.selectedStepIndex ? " is-selected" : ""}`,
      });

      const stack = moveStack({
        upDisabled: index === 0,
        downDisabled: index >= state.draft.steps.length - 1,
        onUp(event) {
          event.stopPropagation();
          if (index === 0) {
            return;
          }
          const [moved] = state.draft.steps.splice(index, 1);
          state.selectedStepIndex = index - 1;
          state.draft.steps.splice(state.selectedStepIndex, 0, moved);
          render();
        },
        onDown(event) {
          event.stopPropagation();
          if (index >= state.draft.steps.length - 1) {
            return;
          }
          const [moved] = state.draft.steps.splice(index, 1);
          state.selectedStepIndex = index + 1;
          state.draft.steps.splice(state.selectedStepIndex, 0, moved);
          render();
        },
      });

      const button = listRowButton({
        className: "step-entry",
        selected: index === state.selectedStepIndex,
        children: [
          element("span", {
            text: `${index + 1}. ${step.kind}:${step.action}${step.display_name ? ` [${step.display_name}]` : ""}`,
          }),
          element("span", { className: "muted", text: `${step.timeout_s ?? 600}s` }),
        ],
      });
      button.addEventListener("click", () => {
        state.selectedStepIndex = index;
        loadEditorFromSelected();
        render();
      });
      row.append(stack, button);
      elements.jobStepList.append(row);
    });
  }

  function renderPreview() {
    elements.jobPreview.textContent = previewLines(state.draft).join("\n");
  }

  function render() {
    syncFormFromRootFields();
    syncActionSelect();
    renderParamFields();
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
      renderParamFields();
      renderHelp();
      return;
    }
    elements.jobStepKind.value = step.kind || "sdk";
    elements.jobStepAction.value = step.action || "";
    elements.jobStepDisplayName.value = step.display_name || "";
    elements.jobStepTimeout.value = String(step.timeout_s ?? 600);
    elements.jobStepParams.value = prettyJSON(step.params || {});
    syncActionSelect();
    renderParamFields();
    renderHelp();
  }

  function readEditorStep() {
    const params = mergeEditorParamsFromFields();
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
      throw new Error("stage workflows require group_id or group_name");
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

  elements.jobClearButton.addEventListener("click", withStatus(() => {
    state.draft = createEmptyDraft();
    state.selectedStepIndex = 0;
    loadEditorFromSelected();
    render();
    hooks.setStatus("Cleared job draft");
  }));

  elements.jobStepKind.addEventListener("change", () => {
    syncActionSelect();
    renderParamFields();
    renderHelp();
  });
  elements.jobActionSelect.addEventListener("change", () => {
    const previousAction = elements.jobStepAction.value.trim();
    if (elements.jobActionSelect.value) {
      elements.jobStepAction.value = elements.jobActionSelect.value;
      maybeApplyActionTemplate(elements.jobStepKind.value || "sdk", elements.jobActionSelect.value, previousAction);
    }
    renderParamFields();
    renderHelp();
  });
  elements.jobStepAction.addEventListener("input", () => {
    syncActionSelect();
    renderParamFields();
    renderHelp();
  });
  elements.jobStepParams.addEventListener("input", () => {
    try {
      parseJSONObject(elements.jobStepParams.value, {});
    } catch {
      return;
    }
    renderParamFields();
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
