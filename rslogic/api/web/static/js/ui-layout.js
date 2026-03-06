import {
  button,
  consolePanel,
  element,
  fieldBlock,
  fieldRow,
  input,
  listPanel,
  progressTrack,
  ribbon,
  ribbonBlock,
  select,
  stateBadge,
  subHeader,
  textarea,
  tile,
  toolbar,
  workflowButton,
  workflowView,
} from "./ui-components.js";

function remember(refs, name, node) {
  refs[name] = node;
  return node;
}

function buildTopRibbon(refs) {
  refs.workflowButtons = {
    upload: remember(refs, "workflowButtonUpload", workflowButton({ id: "workflow-button-upload", workflow: "upload", label: "UPLOAD" })),
    build: remember(refs, "workflowButtonBuild", workflowButton({ id: "workflow-button-build", workflow: "build", label: "BUILD" })),
    jobs: remember(refs, "workflowButtonJobs", workflowButton({ id: "workflow-button-jobs", workflow: "jobs", label: "JOBS" })),
    clients: remember(refs, "workflowButtonClients", workflowButton({ id: "workflow-button-clients", workflow: "clients", label: "CLIENTS" })),
    images: remember(refs, "workflowButtonImages", workflowButton({ id: "workflow-button-images", workflow: "images", label: "IMAGES" })),
  };
  return ribbon(
    [
      ribbonBlock([
        element("span", { className: "brand-mark", text: "RSLOGIC" }),
        remember(refs, "systemSummary", element("span", { id: "system-summary", text: "ORCHESTRATOR WEB" })),
        remember(refs, "workflowSummary", element("span", { id: "workflow-summary", text: "WORKFLOW:UPLOAD" })),
      ]),
      ribbonBlock(
        [
          refs.workflowButtons.upload,
          refs.workflowButtons.build,
          refs.workflowButtons.jobs,
          refs.workflowButtons.clients,
          refs.workflowButtons.images,
        ],
        "ribbon-block ribbon-workflows",
      ),
      ribbonBlock(
        [
          remember(refs, "refreshAllButton", button({ id: "refresh-all-button", text: "REFRESH ALL" })),
          remember(refs, "selectFirstClientButton", button({ id: "select-first-client-button", text: "FIRST ACTIVE CLIENT" })),
        ],
        "ribbon-block ribbon-actions",
      ),
    ],
    "ribbon ribbon-top",
  );
}

function buildUploadTile(refs) {
  return tile({
    className: "tile-upload",
    title: "UPLOAD FOLDER",
    badge: remember(refs, "uploadStateBadge", stateBadge("upload-state-badge", "IDLE")),
    body: [
      toolbar([
        remember(refs, "uploadPathUpButton", button({ id: "upload-path-up-button", text: "UP" })),
        remember(refs, "uploadPathRefreshButton", button({ id: "upload-path-refresh-button", text: "REFRESH" })),
        remember(refs, "uploadStartButton", button({ id: "upload-start-button", text: "RUN UPLOAD" })),
      ]),
      fieldRow({
        label: "PATH",
        control: remember(refs, "uploadCurrentPath", input({ id: "upload-current-path", spellcheck: false })),
      }),
      remember(refs, "uploadProgressTrack", progressTrack("upload-progress")),
      remember(refs, "uploadDirectoryList", listPanel("upload-directory-list", "list-panel directory-list")),
      remember(refs, "uploadOperationLog", consolePanel("upload-operation-log", "console-panel")),
    ],
  });
}

function buildIngestTile(refs) {
  return tile({
    className: "tile-ingest",
    title: "INGEST WAITING",
    badge: remember(refs, "ingestStateBadge", stateBadge("ingest-state-badge", "IDLE")),
    body: [
      element("div", {
        className: "form-grid compact-grid",
        children: [
          fieldRow({
            label: "GROUP",
            control: remember(refs, "ingestGroupName", input({ id: "ingest-group-name", spellcheck: false })),
          }),
          fieldRow({
            label: "LIMIT",
            control: remember(refs, "ingestLimit", input({ id: "ingest-limit", type: "number", attrs: { min: "1" } })),
          }),
        ],
      }),
      toolbar([
        remember(refs, "ingestStartButton", button({ id: "ingest-start-button", text: "RUN INGEST" })),
      ]),
      remember(refs, "ingestProgressTrack", progressTrack("ingest-progress")),
      remember(refs, "ingestOperationLog", consolePanel("ingest-operation-log", "console-panel")),
    ],
  });
}

function buildBuilderMetaColumn(refs) {
  return element("div", {
    className: "builder-column builder-column-meta",
    children: [
      element("div", {
        className: "form-grid",
        children: [
          fieldRow({
            label: "JOB NAME",
            control: remember(refs, "jobName", input({ id: "job-name", spellcheck: false })),
          }),
          fieldRow({
            className: "field-row checkbox-row",
            label: "AUTO ASSIGN",
            control: remember(refs, "jobAutoAssign", input({ id: "job-auto-assign", type: "checkbox", checked: true })),
          }),
          fieldRow({
            label: "TARGET CLIENT",
            control: remember(refs, "jobTargetClient", input({ id: "job-target-client", spellcheck: false })),
          }),
          fieldRow({
            label: "CLIENT ID",
            control: remember(refs, "jobClientId", input({ id: "job-client-id", spellcheck: false })),
          }),
          fieldRow({
            label: "GROUP ID",
            control: remember(refs, "jobGroupId", input({ id: "job-group-id", spellcheck: false })),
          }),
          fieldRow({
            label: "GROUP NAME",
            control: remember(refs, "jobGroupName", input({ id: "job-group-name", spellcheck: false })),
          }),
        ],
      }),
      fieldBlock({
        label: "METADATA JSON",
        control: remember(refs, "jobMetadataJson", textarea({ id: "job-metadata-json", spellcheck: false, text: "{}" })),
      }),
      toolbar([
        remember(refs, "jobClearButton", button({ id: "job-clear-button", text: "CLEAR" })),
      ]),
    ],
  });
}

function buildBuilderStepList(refs) {
  return element("div", {
    className: "step-list-panel",
    children: [
      subHeader("CURRENT STEPS"),
      remember(refs, "jobStepList", listPanel("job-step-list", "list-panel step-list")),
      toolbar([
        remember(refs, "jobStepUpButton", button({ id: "job-step-up-button", text: "MOVE UP" })),
        remember(refs, "jobStepDownButton", button({ id: "job-step-down-button", text: "MOVE DOWN" })),
        remember(refs, "jobStepRemoveButton", button({ id: "job-step-remove-button", text: "REMOVE" })),
      ]),
    ],
  });
}

function buildBuilderStepEditor(refs) {
  return element("div", {
    className: "step-editor-panel",
    children: [
      subHeader("STEP EDITOR"),
      element("div", {
        className: "form-grid compact-grid",
        children: [
          fieldRow({
            label: "KIND",
            control: remember(
              refs,
              "jobStepKind",
              select({
                id: "job-step-kind",
                options: [
                  { value: "sdk", label: "sdk" },
                  { value: "file", label: "file" },
                ],
              }),
            ),
          }),
          fieldRow({
            label: "ACTION PALETTE",
            control: remember(refs, "jobActionSelect", select({ id: "job-action-select" })),
          }),
          fieldRow({
            label: "ACTION",
            control: remember(refs, "jobStepAction", input({ id: "job-step-action", spellcheck: false })),
          }),
          fieldRow({
            label: "DISPLAY",
            control: remember(refs, "jobStepDisplayName", input({ id: "job-step-display-name", spellcheck: false })),
          }),
          fieldRow({
            label: "TIMEOUT",
            control: remember(
              refs,
              "jobStepTimeout",
              input({ id: "job-step-timeout", type: "number", value: "600", attrs: { min: "0" } }),
            ),
          }),
        ],
      }),
      fieldBlock({
        label: "PARAM FIELDS",
        control: remember(refs, "jobStepParamFields", element("div", { id: "job-step-param-fields", className: "job-step-param-fields" })),
      }),
      fieldBlock({
        label: "PARAMS JSON",
        control: remember(refs, "jobStepParams", textarea({ id: "job-step-params", spellcheck: false, text: "{}" })),
      }),
      remember(refs, "jobBuilderHelp", consolePanel("job-builder-help", "console-panel compact-console")),
      toolbar([
        remember(refs, "jobStepAddButton", button({ id: "job-step-add-button", text: "ADD TO END" })),
        remember(refs, "jobStepInsertButton", button({ id: "job-step-insert-button", text: "INSERT BEFORE" })),
        remember(refs, "jobStepUpdateButton", button({ id: "job-step-update-button", text: "UPDATE SELECTED" })),
      ]),
    ],
  });
}

function buildJobBuilderTile(refs) {
  return element("section", {
    className: "tile tile-job-builder",
    children: [
      element("div", {
        className: "tile-header",
        children: [
          element("span", { text: "JOB BUILDER" }),
          remember(refs, "jobSubmitState", stateBadge("job-submit-state", "READY")),
        ],
      }),
      element("div", {
        className: "tile-body tile-body-builder",
        children: [
          element("div", {
            className: "builder-top-grid",
            children: [
              buildBuilderMetaColumn(refs),
              element("div", {
                className: "builder-column builder-column-steps",
                children: [
                  toolbar([
                    remember(refs, "jobFragmentSelect", select({ id: "job-fragment-select" })),
                    remember(refs, "jobFragmentAppendButton", button({ id: "job-fragment-append-button", text: "APPEND CHAIN" })),
                    remember(refs, "jobFragmentReplaceButton", button({ id: "job-fragment-replace-button", text: "REPLACE CHAIN" })),
                  ]),
                  element("div", {
                    className: "builder-split",
                    children: [
                      buildBuilderStepList(refs),
                      buildBuilderStepEditor(refs),
                    ],
                  }),
                ],
              }),
            ],
          }),
          element("div", {
            className: "builder-bottom-grid",
            children: [
              element("div", {
                children: [
                  subHeader("REQUEST PREVIEW"),
                  remember(refs, "jobPreview", consolePanel("job-preview", "console-panel")),
                ],
              }),
              element("div", {
                children: [
                  subHeader("SUBMIT"),
                  toolbar([
                    remember(refs, "jobSubmitButton", button({ id: "job-submit-button", text: "DISPATCH JOB" })),
                  ]),
                  remember(refs, "jobSubmitLog", consolePanel("job-submit-log", "console-panel compact-console")),
                ],
              }),
            ],
          }),
        ],
      }),
    ],
  });
}

function buildJobsTile(refs) {
  return tile({
    className: "tile-jobs",
    title: "JOBS",
    badge: remember(refs, "jobsStateBadge", stateBadge("jobs-state-badge", "READY")),
    body: [
      toolbar([
        remember(refs, "jobsRefreshButton", button({ id: "jobs-refresh-button", text: "REFRESH JOBS" })),
        remember(refs, "jobDetailRefreshButton", button({ id: "job-detail-refresh-button", text: "LOAD DETAIL" })),
      ]),
      fieldRow({
        label: "JOB ID",
        control: remember(refs, "jobDetailId", input({ id: "job-detail-id", spellcheck: false })),
      }),
      remember(refs, "jobsList", listPanel("jobs-list", "list-panel jobs-list detail-list")),
      remember(refs, "jobDetail", consolePanel("job-detail", "console-panel detail-console")),
    ],
  });
}

function buildClientsTile(refs) {
  return tile({
    className: "tile-clients",
    title: "CLIENTS",
    badge: remember(refs, "clientsStateBadge", stateBadge("clients-state-badge", "READY")),
    body: [
      toolbar([
        remember(refs, "clientsRefreshButton", button({ id: "clients-refresh-button", text: "REFRESH CLIENTS" })),
        remember(refs, "clientClearQueuesButton", button({ id: "client-clear-queues-button", text: "CLEAR QUEUES" })),
      ]),
      fieldRow({
        label: "CLIENT ID",
        control: remember(refs, "clientDetailId", input({ id: "client-detail-id", spellcheck: false })),
      }),
      remember(refs, "clientsList", listPanel("clients-list", "list-panel clients-list detail-list")),
      remember(refs, "clientDetail", consolePanel("client-detail", "console-panel detail-console")),
    ],
  });
}

function buildImagesTile(refs) {
  return tile({
    className: "tile-images",
    title: "IMAGES",
    badge: remember(refs, "imagesStateBadge", stateBadge("images-state-badge", "READY")),
    body: [
      element("div", {
        className: "images-layout",
        children: [
          element("div", {
            className: "images-groups-panel",
            children: [
              subHeader("GROUPS"),
              element("div", {
                className: "form-grid",
                children: [
                  fieldRow({
                    label: "NAME",
                    control: remember(refs, "imagesGroupName", input({ id: "images-group-name", spellcheck: false })),
                  }),
                  fieldRow({
                    label: "DESCRIPTION",
                    control: remember(refs, "imagesGroupDescription", input({ id: "images-group-description", spellcheck: false })),
                  }),
                ],
              }),
              toolbar([
                remember(refs, "imagesRefreshButton", button({ id: "images-refresh-button", text: "REFRESH" })),
                remember(refs, "imagesCreateGroupButton", button({ id: "images-create-group-button", text: "CREATE GROUP" })),
                remember(refs, "imagesDeleteGroupButton", button({ id: "images-delete-group-button", text: "DELETE GROUP" })),
              ]),
              remember(refs, "imagesGroupsList", listPanel("images-groups-list", "list-panel images-groups-list detail-list")),
              remember(refs, "imagesGroupDetail", consolePanel("images-group-detail", "console-panel detail-console")),
            ],
          }),
          element("div", {
            className: "images-map-panel",
            children: [
              subHeader("MAP SELECTION"),
              toolbar([
                fieldRow({
                  className: "field-row inline-field-row",
                  label: "MODE",
                  control: remember(
                    refs,
                    "imagesSelectionMode",
                    select({
                      id: "images-selection-mode",
                      options: [
                        { value: "replace", label: "replace" },
                        { value: "add", label: "add" },
                        { value: "subtract", label: "subtract" },
                      ],
                    }),
                  ),
                }),
                remember(refs, "imagesFitButton", button({ id: "images-fit-button", text: "FIT TO MAP" })),
                remember(refs, "imagesSelectionClearButton", button({ id: "images-selection-clear-button", text: "CLEAR SEL" })),
                remember(refs, "imagesGroupAddButton", button({ id: "images-group-add-button", text: "ADD TO GROUP" })),
                remember(refs, "imagesGroupRemoveButton", button({ id: "images-group-remove-button", text: "REMOVE FROM GROUP" })),
                remember(refs, "imagesGroupReplaceButton", button({ id: "images-group-replace-button", text: "REPLACE GROUP" })),
              ]),
              element("div", {
                className: "images-map-summary",
                children: [
                  remember(refs, "imagesMapSummary", element("span", { id: "images-map-summary", text: "0 selected | 0 mapped assets" })),
                ],
              }),
              remember(
                refs,
                "imagesMapSurface",
                element("div", {
                  id: "images-map-surface",
                  className: "images-map-surface",
                  children: [
                    remember(
                      refs,
                      "imagesGlobe",
                      element("div", {
                        id: "images-globe",
                        className: "images-globe",
                      }),
                    ),
                    remember(refs, "imagesSelectionRect", element("div", { id: "images-selection-rect", className: "images-selection-rect" })),
                  ],
                }),
              ),
              remember(refs, "imagesSelectionLog", consolePanel("images-selection-log", "console-panel compact-console")),
            ],
          }),
        ],
      }),
    ],
  });
}

function buildWorkflowStage(refs) {
  refs.workflowViews = {
    upload: workflowView({
      className: "workflow-view-upload",
      workflow: "upload",
      children: [buildUploadTile(refs), buildIngestTile(refs)],
    }),
    build: workflowView({
      className: "workflow-view-build",
      workflow: "build",
      children: [buildJobBuilderTile(refs)],
    }),
    jobs: workflowView({
      className: "workflow-view-jobs",
      workflow: "jobs",
      children: [buildJobsTile(refs)],
    }),
    clients: workflowView({
      className: "workflow-view-clients",
      workflow: "clients",
      children: [buildClientsTile(refs)],
    }),
    images: workflowView({
      className: "workflow-view-images",
      workflow: "images",
      children: [buildImagesTile(refs)],
    }),
  };
  return element("main", {
    className: "workflow-stage",
    children: [
      refs.workflowViews.upload,
      refs.workflowViews.build,
      refs.workflowViews.jobs,
      refs.workflowViews.clients,
      refs.workflowViews.images,
    ],
  });
}

function buildBottomRibbon(refs) {
  return element("footer", {
    className: "ribbon ribbon-bottom",
    children: [
      ribbonBlock([
        remember(refs, "bottomStatusText", element("span", { id: "bottom-status-text", text: "READY" })),
      ]),
      ribbonBlock(
        [
          remember(refs, "bottomUploadStatus", element("span", { id: "bottom-upload-status", text: "UPLOAD:IDLE" })),
          remember(refs, "bottomIngestStatus", element("span", { id: "bottom-ingest-status", text: "INGEST:IDLE" })),
          remember(refs, "bottomClock", element("span", { id: "bottom-clock", text: "--:--:--" })),
        ],
        "ribbon-block ribbon-summary",
      ),
    ],
  });
}

export function buildDashboard(root) {
  const refs = {};
  const workspace = buildWorkflowStage(refs);
  const shell = element("div", {
    className: "app-shell",
    children: [
      buildTopRibbon(refs),
      workspace,
      buildBottomRibbon(refs),
    ],
  });
  root.replaceChildren(shell);
  refs.uploadProgress = refs.uploadProgressTrack.querySelector(".progress-fill");
  refs.ingestProgress = refs.ingestProgressTrack.querySelector(".progress-fill");
  return refs;
}
