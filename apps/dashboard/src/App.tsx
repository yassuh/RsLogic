import {
  Activity,
  ChevronDown,
  ChevronRight,
  Cloud,
  Cpu,
  Gauge,
  HardDrive,
  Image as ImageIcon,
  Map as MapIcon,
  MapPin,
  GripVertical,
  Plus,
  Radio,
  RefreshCw,
  Save,
  Server,
  Terminal,
  Trash2,
  Wifi,
  WifiOff,
  X,
} from "lucide-react"
import {
  Fragment,
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react"
import MapLibreMap, {
  Layer,
  NavigationControl,
  Popup,
  Source,
} from "@vis.gl/react-maplibre"
import {
  closestCenter,
  DndContext,
  KeyboardSensor,
  PointerSensor,
  useSensor,
  useSensors,
} from "@dnd-kit/core"
import {
  arrayMove,
  SortableContext,
  sortableKeyboardCoordinates,
  useSortable,
  verticalListSortingStrategy,
} from "@dnd-kit/sortable"
import { CSS } from "@dnd-kit/utilities"
import "maplibre-gl/dist/maplibre-gl.css"

import { Button } from "@/components/ui/button"

type Tab = "overview" | "clients" | "imagery" | "jobs" | "logs"
type StreamState = "connecting" | "live" | "reconnecting" | "offline"

type DesiredState = {
  enabled: boolean
  accept_jobs: boolean
  agent_version: string
  worker_version: string
  realityscan_image: string
  max_concurrent_jobs: number
}

type MachineTelemetry = {
  hostname: string
  cpu_count?: number | null
  uptime_seconds?: number | null
  load_average_1m?: number | null
  load_average_5m?: number | null
  load_average_15m?: number | null
  total_memory_bytes?: number | null
  available_memory_bytes?: number | null
  used_memory_bytes?: number | null
  total_disk_bytes?: number | null
  free_disk_bytes?: number | null
  gpu?: string | null
  gpu_utilization_percent?: number | null
  gpu_memory_total_bytes?: number | null
  gpu_memory_used_bytes?: number | null
  container_runtime?: string | null
  observed_at: string
}

type AgentStatus = {
  client_id?: string | null
  agent_version: string
  connected: boolean
  worker_state: string
  telemetry: MachineTelemetry
}

type WorkerStatus = {
  worker_version: string
  process_state: string
  active_job_id?: string | null
  supports_job_events_jsonl?: boolean
}

type EnrollmentRequest = {
  hostname: string
  machine_id: string
  agent_version: string
  hardware: {
    hostname: string
    machine_id: string
    os: string
    arch: string
    cpu_count?: number | null
    total_memory_bytes?: number | null
    gpu?: string | null
    container_runtime?: string | null
  }
}

type ApiClientRecord = {
  client_id: string
  public_key: string
  desired_state: DesiredState
  approved_at: string
  revoked_at?: string | null
  enrollment?: EnrollmentRequest | null
  latest_status?: AgentStatus | null
  latest_worker_status?: WorkerStatus | null
  latest_telemetry?: MachineTelemetry | null
  last_heartbeat_at?: string | null
}

type JobRecord = {
  job_id: string
  client_id: string
  state: string
  assigned_at: string
  updated_at: string
  completed_at?: string | null
  job: {
    job_name?: string | null
    realityscan_image?: string | null
    manifest?: {
      inputs?: Array<{
        asset_id: string
        filename: string
        size_bytes?: number | null
      }>
    }
    pipeline?: {
      template_id?: string
      stages?: string[]
      project_filename?: string
      orthomosaic_filename?: string | null
      ortho_pixel_size_meters?: number | null
    }
  }
}

type JobEvent = {
  job_id: string
  state: string
  message: string
  progress: number
  observed_at: string
  details?: JobEventDetails | null
}

type JobEventDetails = {
  kind: string
  stage_id?: string | null
  phase_id?: string | null
  phase_index?: number | null
  phase_count?: number | null
  command?: string | null
  status_progress?: number | null
  runtime_seconds?: number | null
  eta_seconds?: number | null
  raw_status?: string | null
  stdout_log_path?: string | null
  stderr_log_path?: string | null
  output_path?: string | null
  fatal_pattern?: string | null
}

type TimelineRow = {
  id: string
  label: string
  start: number
  end: number
  detail?: string
}

type JobTemplate = {
  template_id: string
  name: string
  description: string
  stages: string[]
  project_filename: string
  orthomosaic_filename?: string | null
  ortho_pixel_size_meters?: number | null
}

type SavedJobTemplate = JobTemplate & {
  saved_at: string
}

type TemplateOption = JobTemplate & {
  source: "built_in" | "custom"
  key: string
  saved_at?: string
}

type UploadedArtifact = {
  job_id: string
  artifact_id: string
  filename: string
  storage_uri?: string | null
  content_type?: string | null
  sha256?: string | null
  size_bytes?: number | null
}

type SelectedJobAsset = {
  asset_id: string
  filename: string
  group_name?: string | null
  latitude?: number | null
  longitude?: number | null
  size_bytes?: number | null
}

type BuildJobResponse = {
  dry_run: boolean
  selected_assets: SelectedJobAsset[]
  job?: (JobRecord["job"] & { job_id?: string }) | null
  queued_command?: unknown
  warnings: string[]
}

type AdminStreamMessage = {
  type: "snapshot"
  reason: string
  observed_at: string
  clients: ApiClientRecord[]
  jobs: JobRecord[]
  job_events?: JobEvent[]
}

type ApiHealth = {
  status: string
  version?: string
  protocol_version?: string
  build_sha?: string | null
  capabilities?: {
    admin_job_events?: boolean
    admin_websocket_job_events?: boolean
    artifacts?: boolean
    cloudfront_manifests?: boolean
    postgres_store?: boolean
    studio_api?: boolean
    worker_status?: boolean
  }
}

type ClientSource =
  | "loading"
  | "api"
  | "api-empty"
  | "ws"
  | "ws-empty"
  | "fallback"
type ImagerySource = "idle" | "loading" | "api" | "api-empty" | "error"

type TelemetrySample = {
  observedAt: string
  loadPercent: number | null
  memoryPercent: number | null
  diskPercent: number | null
  gpuPercent: number | null
  gpuMemoryPercent: number | null
}

type StudioImageAsset = {
  id?: string
  asset_id?: string
  account_id?: number | null
  image_group_id?: string | number | null
  image_group_name?: string | null
  image_group?: Record<string, unknown> | null
  group_id?: string | number | null
  group_name?: string | null
  imagery_source_id?: string | number | null
  imagery_source_name?: string | null
  source_id?: string | number | null
  source_name?: string | null
  source_version_id?: string | number | null
  source_version_name?: string | null
  project_id?: string | number | null
  project_name?: string | null
  batch_id?: string | number | null
  batch_name?: string | null
  uri?: string | null
  filename?: string | null
  cloudfront_path?: string | null
  object_key?: string | null
  sha256?: string | null
  file_size?: number | null
  size_bytes?: number | null
  captured_at?: string | null
  latitude?: number | null
  longitude?: number | null
  image_width?: number | null
  image_height?: number | null
  bucket_name?: string | null
  drone_model?: string | null
  camera_make?: string | null
  camera_model?: string | null
}

type ImageryResponse = {
  image_assets: StudioImageAsset[]
}

const savedJobTemplatesStorageKey = "rslogic.dashboard.savedJobTemplates.v1"

const realityScanStageOrder = [
  "set_intrinsics",
  "align",
  "select_maximal_component",
  "set_reconstruction_region_auto",
  "calculate_preview_model",
  "calculate_normal_model",
  "calculate_high_model",
  "calculate_texture",
  "calculate_ortho_projection",
  "export_ortho_projection",
  "save_project",
]

const fallbackClients: ApiClientRecord[] = [
  {
    client_id: "client-yassuh-1-preview",
    public_key: "preview-public-key-ed25519-7f66e2d8d7d0",
    approved_at: new Date().toISOString(),
    revoked_at: null,
    desired_state: {
      enabled: true,
      accept_jobs: true,
      agent_version: "0.1.0",
      worker_version: "0.1.0",
      realityscan_image: "yassuh/realityscan:local",
      max_concurrent_jobs: 1,
    },
    enrollment: {
      hostname: "yassuh-1",
      machine_id: "preview-machine-yassuh-1",
      agent_version: "0.1.0",
      hardware: {
        hostname: "yassuh-1",
        machine_id: "preview-machine-yassuh-1",
        os: "nixos",
        arch: "x86_64",
        cpu_count: 64,
        total_memory_bytes: 270_000_000_000,
        gpu: "RTX 5090",
        container_runtime: "docker/podman",
      },
    },
    latest_status: null,
    latest_worker_status: null,
    latest_telemetry: null,
    last_heartbeat_at: null,
  },
]

export function App() {
  const [activeTab, setActiveTab] = useState<Tab>("clients")
  const [clients, setClients] = useState<ApiClientRecord[]>(fallbackClients)
  const [jobs, setJobs] = useState<JobRecord[]>([])
  const [jobEvents, setJobEvents] = useState<JobEvent[]>([])
  const [artifacts, setArtifacts] = useState<UploadedArtifact[]>([])
  const [jobTemplates, setJobTemplates] = useState<JobTemplate[]>([])
  const [jobError, setJobError] = useState<string | null>(null)
  const [jobEventsError, setJobEventsError] = useState<string | null>(null)
  const [apiHealth, setApiHealth] = useState<ApiHealth | null>(null)
  const [apiHealthError, setApiHealthError] = useState<string | null>(null)
  const [clientSource, setClientSource] = useState<ClientSource>("loading")
  const [clientError, setClientError] = useState<string | null>(null)
  const [imageryAssets, setImageryAssets] = useState<StudioImageAsset[]>([])
  const [imagerySource, setImagerySource] = useState<ImagerySource>("idle")
  const [imageryError, setImageryError] = useState<string | null>(null)
  const [streamState, setStreamState] = useState<StreamState>("connecting")
  const [streamReason, setStreamReason] = useState("boot")
  const [lastStreamAt, setLastStreamAt] = useState<string | null>(null)
  const [streamEvents, setStreamEvents] = useState<string[]>([])
  const [telemetryHistory, setTelemetryHistory] = useState<
    Record<string, TelemetrySample[]>
  >({})
  const [expandedClientIds, setExpandedClientIds] = useState<string[]>([
    fallbackClients[0].client_id,
  ])

  const appendStreamEvent = useCallback((line: string) => {
    setStreamEvents((current) => [line, ...current].slice(0, 18))
  }, [])

  const applySnapshot = useCallback(
    (
      records: ApiClientRecord[],
      jobRecords: JobRecord[],
      jobEventRecords: JobEvent[] | null,
      source: "api" | "ws",
      reason: string,
      observedAt: string
    ) => {
      setClients(records)
      setJobs(jobRecords)
      if (jobEventRecords) {
        setJobEvents(jobEventRecords)
        setJobEventsError(null)
      }
      setClientSource(records.length > 0 ? source : `${source}-empty`)
      setClientError(null)
      setStreamReason(reason)
      setLastStreamAt(observedAt)
      setExpandedClientIds((current) => reconcileExpanded(current, records))
      setTelemetryHistory((current) => addTelemetrySamples(current, records))
    },
    []
  )

  const loadApiHealth = useCallback(async () => {
    try {
      const response = await fetch("/healthz", {
        headers: { Accept: "application/json" },
      })
      if (!response.ok) {
        throw new Error(`healthz ${response.status}`)
      }
      const health = (await response.json()) as ApiHealth
      setApiHealth(health)
      setApiHealthError(null)
      if (health.capabilities?.admin_job_events === false) {
        setJobEventsError("server reports job events disabled")
      }
    } catch (error) {
      setApiHealth(null)
      setApiHealthError(
        error instanceof Error ? error.message : "health unavailable"
      )
    }
  }, [])

  const loadClients = useCallback(async () => {
    setClientSource("loading")
    setClientError(null)
    try {
      const [
        clientsResponse,
        jobsResponse,
        artifactsResponse,
        jobEventsResponse,
      ] = await Promise.all([
        fetch("/api/admin/clients", {
          headers: { Accept: "application/json" },
        }),
        fetch("/api/admin/jobs", {
          headers: { Accept: "application/json" },
        }),
        fetch("/api/admin/artifacts", {
          headers: { Accept: "application/json" },
        }),
        fetch("/api/admin/job-events?limit=400", {
          headers: { Accept: "application/json" },
        }),
      ])
      if (!clientsResponse.ok) {
        throw new Error(`clients api ${clientsResponse.status}`)
      }

      const records = (await clientsResponse.json()) as ApiClientRecord[]
      const jobRecords = jobsResponse.ok
        ? ((await jobsResponse.json()) as JobRecord[])
        : []
      const artifactRecords = artifactsResponse.ok
        ? ((await artifactsResponse.json()) as UploadedArtifact[])
        : []
      let jobEventRecords: JobEvent[] | null = null
      if (jobEventsResponse.ok) {
        jobEventRecords = (await jobEventsResponse.json()) as JobEvent[]
        setJobEventsError(null)
      } else {
        setJobEventsError(`job-events api ${jobEventsResponse.status}`)
      }
      setArtifacts(artifactRecords)
      applySnapshot(
        records,
        jobRecords,
        jobEventRecords,
        "api",
        "manual_refresh",
        new Date().toISOString()
      )
      appendStreamEvent(
        `${formatClock(new Date().toISOString())} http refresh ${records.length} clients`
      )
    } catch (error) {
      setClients(fallbackClients)
      setJobs([])
      setJobEvents([])
      setJobEventsError(null)
      setClientSource("fallback")
      setArtifacts([])
      setExpandedClientIds([fallbackClients[0].client_id])
      setClientError(error instanceof Error ? error.message : "api unavailable")
    }
  }, [appendStreamEvent, applySnapshot])

  const loadArtifacts = useCallback(async () => {
    try {
      const response = await fetch("/api/admin/artifacts", {
        headers: { Accept: "application/json" },
      })
      if (!response.ok) {
        throw new Error(await apiErrorMessage(response, "artifacts api"))
      }
      setArtifacts((await response.json()) as UploadedArtifact[])
    } catch (error) {
      setJobError(
        error instanceof Error ? error.message : "artifacts unavailable"
      )
    }
  }, [])

  const loadJobTemplates = useCallback(async () => {
    try {
      const response = await fetch("/api/admin/job-templates", {
        headers: { Accept: "application/json" },
      })
      if (!response.ok) {
        throw new Error(await apiErrorMessage(response, "job templates api"))
      }
      setJobTemplates((await response.json()) as JobTemplate[])
      setJobError(null)
    } catch (error) {
      setJobTemplates([])
      setJobError(
        error instanceof Error ? error.message : "templates unavailable"
      )
    }
  }, [])

  const loadImagery = useCallback(async () => {
    setImagerySource("loading")
    setImageryError(null)
    try {
      const response = await fetch("/api/admin/imagery/assets", {
        headers: { Accept: "application/json" },
      })
      if (!response.ok) {
        throw new Error(await apiErrorMessage(response, "studio imagery api"))
      }
      const payload = (await response.json()) as ImageryResponse
      const assets = payload.image_assets ?? []
      setImageryAssets(assets)
      setImagerySource(assets.length > 0 ? "api" : "api-empty")
    } catch (error) {
      setImageryAssets([])
      setImagerySource("error")
      setImageryError(
        error instanceof Error ? error.message : "studio api unavailable"
      )
    }
  }, [])

  useEffect(() => {
    const timer = window.setTimeout(() => {
      void loadApiHealth()
      void loadClients()
      void loadJobTemplates()
    }, 0)

    return () => window.clearTimeout(timer)
  }, [loadApiHealth, loadClients, loadJobTemplates])

  useEffect(() => {
    let socket: WebSocket | null = null
    let reconnectTimer: number | undefined
    let closedByEffect = false

    const connect = () => {
      setStreamState((current) =>
        current === "offline" ? "reconnecting" : "connecting"
      )
      socket = new WebSocket(adminEventsUrl())

      socket.addEventListener("open", () => {
        setStreamState("live")
        appendStreamEvent(
          `${formatClock(new Date().toISOString())} ws connected`
        )
      })

      socket.addEventListener("message", (event) => {
        try {
          const message = JSON.parse(String(event.data)) as AdminStreamMessage
          if (message.type !== "snapshot") return
          const hasJobEvents = Object.prototype.hasOwnProperty.call(
            message,
            "job_events"
          )
          if (!hasJobEvents) {
            setJobEventsError("admin stream missing job_events")
          }
          applySnapshot(
            message.clients,
            message.jobs,
            hasJobEvents ? (message.job_events ?? []) : null,
            "ws",
            message.reason,
            message.observed_at
          )
          appendStreamEvent(
            `${formatClock(message.observed_at)} ${message.reason} ${
              message.clients.length
            } clients`
          )
          if (message.reason === "artifact_uploaded") {
            void loadArtifacts()
          }
        } catch {
          appendStreamEvent(
            `${formatClock(new Date().toISOString())} invalid ws payload`
          )
        }
      })

      socket.addEventListener("close", () => {
        if (closedByEffect) return
        setStreamState("reconnecting")
        appendStreamEvent(
          `${formatClock(new Date().toISOString())} ws reconnecting`
        )
        reconnectTimer = window.setTimeout(connect, 2000)
      })

      socket.addEventListener("error", () => {
        setStreamState("offline")
      })
    }

    connect()

    return () => {
      closedByEffect = true
      if (reconnectTimer !== undefined) window.clearTimeout(reconnectTimer)
      socket?.close()
    }
  }, [appendStreamEvent, applySnapshot, loadArtifacts])

  const connectedClients = useMemo(
    () => clients.filter((client) => isClientLive(client)).length,
    [clients]
  )
  const activeJobs = useMemo(
    () => jobs.filter((job) => !isTerminalJobState(job.state)).length,
    [jobs]
  )
  const firstTelemetry = clients.find(
    (client) => client.latest_telemetry
  )?.latest_telemetry
  const avgLoad = averagePresent(
    clients.map((client) => loadPercent(client.latest_telemetry))
  )
  const gpuUtil = averagePresent(
    clients.map(
      (client) => client.latest_telemetry?.gpu_utilization_percent ?? null
    )
  )

  const toggleClient = (clientId: string) => {
    setExpandedClientIds((current) =>
      current.includes(clientId)
        ? current.filter((id) => id !== clientId)
        : [...current, clientId]
    )
  }

  return (
    <main className="h-svh overflow-hidden bg-background font-mono text-[13px] text-foreground">
      <div className="grid h-svh min-h-0 grid-cols-[13rem_1fr] max-lg:grid-cols-1 max-lg:grid-rows-[auto_1fr]">
        <aside className="min-h-0 overflow-y-auto border-r bg-sidebar text-sidebar-foreground max-lg:border-r-0 max-lg:border-b">
          <div className="flex h-11 items-center gap-2 border-b px-3">
            <div className="flex size-7 items-center justify-center rounded bg-sidebar-primary text-sidebar-primary-foreground">
              <Server className="size-4" aria-hidden="true" />
            </div>
            <div className="min-w-0">
              <div className="truncate text-xs font-medium uppercase">
                RsLogic
              </div>
              <div className="truncate text-[11px] text-muted-foreground">
                v2 local console
              </div>
            </div>
          </div>

          <nav className="grid gap-px p-2 text-xs max-lg:grid-cols-5">
            <NavButton
              active={activeTab === "overview"}
              icon={Activity}
              label="Overview"
              onClick={() => setActiveTab("overview")}
            />
            <NavButton
              active={activeTab === "clients"}
              icon={Server}
              label="Clients"
              onClick={() => setActiveTab("clients")}
            />
            <NavButton
              active={activeTab === "imagery"}
              icon={MapIcon}
              label="Imagery"
              onClick={() => {
                setActiveTab("imagery")
                if (imagerySource === "idle")
                  window.setTimeout(() => void loadImagery(), 0)
              }}
            />
            <NavButton
              active={activeTab === "jobs"}
              icon={Cloud}
              label="Jobs"
              onClick={() => {
                setActiveTab("jobs")
                if (imagerySource === "idle")
                  window.setTimeout(() => void loadImagery(), 0)
                if (jobTemplates.length === 0)
                  window.setTimeout(() => void loadJobTemplates(), 0)
                window.setTimeout(() => void loadArtifacts(), 0)
              }}
            />
            <NavButton
              active={activeTab === "logs"}
              icon={Terminal}
              label="Logs"
              onClick={() => setActiveTab("logs")}
            />
          </nav>

          <div className="border-t p-3 text-[11px] leading-5 text-muted-foreground">
            <InfoLine label="host" value="local" />
            <InfoLine label="mode" value="dev" />
            <InfoLine label="transport" value={streamStateLabel(streamState)} />
            <InfoLine label="api" value={clientSourceLabel(clientSource)} />
            <InfoLine
              label="build"
              value={apiBuildLabel(apiHealth, apiHealthError)}
            />
            <InfoLine
              label="proto"
              value={apiProtocolLabel(apiHealth, apiHealthError)}
            />
            <InfoLine
              label="events"
              value={apiEventsCapabilityLabel(apiHealth, apiHealthError)}
            />
            <InfoLine
              label="last"
              value={lastStreamAt ? formatClock(lastStreamAt) : "-"}
            />
          </div>
        </aside>

        <section className="flex min-h-0 min-w-0 flex-col">
          <header className="flex h-11 items-center justify-between border-b px-3 max-sm:h-auto max-sm:flex-col max-sm:items-start max-sm:gap-2 max-sm:py-3">
            <div className="flex min-w-0 items-center gap-2">
              <Terminal className="size-4 text-primary" aria-hidden="true" />
              <h1 className="truncate text-xs font-medium uppercase">
                dashboard / {activeTab}
              </h1>
            </div>
            <div className="flex items-center gap-2 text-[11px] text-muted-foreground">
              <span
                className={`size-2 rounded-full ${
                  streamState === "live" ? "bg-primary" : "bg-destructive"
                }`}
              />
              {streamStateLabel(streamState)} / {streamReason}
            </div>
          </header>

          <div className="grid min-h-0 flex-1 grid-rows-[auto_minmax(0,1fr)] gap-3 overflow-hidden p-3">
            <div className="grid gap-3 md:grid-cols-4">
              <Metric
                icon={Radio}
                label="clients"
                value={`${connectedClients}/${clients.length}`}
                detail="live / total"
              />
              <Metric
                icon={Gauge}
                label="load"
                value={formatPercent(avgLoad)}
                detail={
                  firstTelemetry ? compactLoad(firstTelemetry) : "no telemetry"
                }
              />
              <Metric
                icon={Cpu}
                label="gpu"
                value={formatPercent(gpuUtil)}
                detail={firstTelemetry?.gpu ?? "no gpu"}
              />
              <Metric
                icon={HardDrive}
                label="jobs"
                value={String(activeJobs)}
                detail={`${jobs.length} known`}
              />
            </div>

            <div className="min-h-0 overflow-hidden">
              {activeTab === "clients" ? (
                <ClientsView
                  clients={clients}
                  jobs={jobs}
                  source={clientSource}
                  error={clientError}
                  streamState={streamState}
                  lastStreamAt={lastStreamAt}
                  streamEvents={streamEvents}
                  telemetryHistory={telemetryHistory}
                  expandedClientIds={expandedClientIds}
                  onRefresh={loadClients}
                  onToggleClient={toggleClient}
                />
              ) : activeTab === "overview" ? (
                <OverviewView
                  clients={clients}
                  jobs={jobs}
                  jobEvents={jobEvents}
                  jobEventsError={jobEventsError}
                  streamEvents={streamEvents}
                  telemetryHistory={telemetryHistory}
                />
              ) : activeTab === "imagery" ? (
                <ImageryView
                  assets={imageryAssets}
                  source={imagerySource}
                  error={imageryError}
                  onRefresh={loadImagery}
                />
              ) : activeTab === "jobs" ? (
                <JobsView
                  jobs={jobs}
                  jobEvents={jobEvents}
                  jobEventsError={jobEventsError}
                  clients={clients}
                  artifacts={artifacts}
                  templates={jobTemplates}
                  imageryAssets={imageryAssets}
                  imagerySource={imagerySource}
                  error={jobError}
                  onLoadImagery={loadImagery}
                  onRefresh={() => {
                    void loadClients()
                    void loadArtifacts()
                    void loadJobTemplates()
                  }}
                />
              ) : (
                <LiveEventPanel
                  events={streamEvents}
                  streamState={streamState}
                />
              )}
            </div>
          </div>
        </section>
      </div>
    </main>
  )
}

function NavButton({
  active,
  icon: Icon,
  label,
  onClick,
}: {
  active: boolean
  icon: import("react").ElementType
  label: string
  onClick: () => void
}) {
  return (
    <Button
      size="sm"
      variant={active ? "secondary" : "ghost"}
      className="h-7 justify-start gap-2 rounded"
      onClick={onClick}
    >
      <Icon className="size-4" aria-hidden="true" />
      {label}
    </Button>
  )
}

function ClientsView({
  clients,
  jobs,
  source,
  error,
  streamState,
  lastStreamAt,
  streamEvents,
  telemetryHistory,
  expandedClientIds,
  onRefresh,
  onToggleClient,
}: {
  clients: ApiClientRecord[]
  jobs: JobRecord[]
  source: ClientSource
  error: string | null
  streamState: StreamState
  lastStreamAt: string | null
  streamEvents: string[]
  telemetryHistory: Record<string, TelemetrySample[]>
  expandedClientIds: string[]
  onRefresh: () => void
  onToggleClient: (clientId: string) => void
}) {
  return (
    <div className="grid h-full min-h-0 min-w-0 auto-rows-max content-start gap-3 overflow-y-auto">
      <HostsTelemetryPanel
        clients={clients}
        telemetryHistory={telemetryHistory}
      />

      <div className="grid min-h-0 gap-3 xl:grid-cols-[1fr_22rem]">
        <div className="min-w-0 border bg-card">
          <PanelHeader
            title="clients"
            right={
              <div className="flex items-center gap-2">
                <span>{clientSourceLabel(source)}</span>
                <span>{lastStreamAt ? formatClock(lastStreamAt) : "-"}</span>
                <Button
                  size="icon-xs"
                  variant="ghost"
                  className="rounded"
                  onClick={onRefresh}
                  title="Refresh clients"
                >
                  <RefreshCw className="size-3" aria-hidden="true" />
                </Button>
              </div>
            }
          />

          {error ? (
            <div className="border-b px-3 py-2 text-[11px] text-muted-foreground">
              api fallback: {error}
            </div>
          ) : null}

          <div className="overflow-x-auto">
            <table className="w-full min-w-[980px] border-collapse text-left text-xs">
              <thead className="bg-muted/40 text-[11px] text-muted-foreground uppercase">
                <tr className="border-b">
                  <Th className="w-8" />
                  <Th>client id</Th>
                  <Th>host</Th>
                  <Th>state</Th>
                  <Th>jobs</Th>
                  <Th>worker</Th>
                  <Th>runtime</Th>
                  <Th>heartbeat</Th>
                  <Th>approved</Th>
                </tr>
              </thead>
              <tbody>
                {clients.length === 0 ? (
                  <tr>
                    <td className="px-3 py-6 text-muted-foreground" colSpan={9}>
                      no approved clients registered
                    </td>
                  </tr>
                ) : (
                  clients.map((client) => {
                    const expanded = expandedClientIds.includes(
                      client.client_id
                    )
                    const hostname =
                      client.latest_telemetry?.hostname ??
                      client.enrollment?.hardware.hostname ??
                      "-"
                    const clientState = client.revoked_at
                      ? "revoked"
                      : isClientLive(client)
                        ? "live"
                        : client.desired_state.enabled
                          ? "enabled"
                          : "disabled"
                    const clientJobs = jobs.filter(
                      (job) =>
                        job.client_id === client.client_id &&
                        !isTerminalJobState(job.state)
                    ).length

                    return (
                      <Fragment key={client.client_id}>
                        <tr className="border-b hover:bg-muted/25">
                          <Td>
                            <Button
                              size="icon-xs"
                              variant="ghost"
                              className="rounded"
                              aria-expanded={expanded}
                              onClick={() => onToggleClient(client.client_id)}
                            >
                              {expanded ? (
                                <ChevronDown
                                  className="size-3"
                                  aria-hidden="true"
                                />
                              ) : (
                                <ChevronRight
                                  className="size-3"
                                  aria-hidden="true"
                                />
                              )}
                            </Button>
                          </Td>
                          <Td className="font-medium">
                            {shortId(client.client_id)}
                          </Td>
                          <Td>{hostname}</Td>
                          <Td>
                            <StatusPill state={clientState} />
                          </Td>
                          <Td>
                            {client.desired_state.accept_jobs
                              ? `${clientJobs} active`
                              : "paused"}
                          </Td>
                          <Td>
                            {client.latest_worker_status?.process_state ??
                              client.latest_status?.worker_state ??
                              "-"}
                          </Td>
                          <Td>
                            {client.latest_telemetry?.container_runtime ??
                              client.enrollment?.hardware.container_runtime ??
                              "-"}
                          </Td>
                          <Td>{formatDateTime(client.last_heartbeat_at)}</Td>
                          <Td>{formatDate(client.approved_at)}</Td>
                        </tr>
                        {expanded ? (
                          <tr className="border-b bg-muted/10">
                            <td colSpan={9} className="px-3 py-3">
                              <ClientDetails
                                client={client}
                                history={
                                  telemetryHistory[client.client_id] ?? []
                                }
                              />
                            </td>
                          </tr>
                        ) : null}
                      </Fragment>
                    )
                  })
                )}
              </tbody>
            </table>
          </div>
        </div>

        <LiveEventPanel events={streamEvents} streamState={streamState} />
      </div>
    </div>
  )
}

function HostsTelemetryPanel({
  clients,
  telemetryHistory,
}: {
  clients: ApiClientRecord[]
  telemetryHistory: Record<string, TelemetrySample[]>
}) {
  return (
    <div className="min-w-0 self-start border bg-card">
      <PanelHeader
        title="hosts / live telemetry"
        right={`${clients.length} registered`}
      />
      <div className="divide-y">
        {clients.length === 0 ? (
          <div className="px-3 py-6 text-xs text-muted-foreground">
            no hosts online
          </div>
        ) : (
          clients.map((client) => (
            <HostTelemetryRow
              key={client.client_id}
              client={client}
              history={telemetryHistory[client.client_id] ?? []}
            />
          ))
        )}
      </div>
    </div>
  )
}

function HostTelemetryRow({
  client,
  history,
}: {
  client: ApiClientRecord
  history: TelemetrySample[]
}) {
  const telemetry = client.latest_telemetry
  const hostname =
    telemetry?.hostname ?? client.enrollment?.hardware.hostname ?? "-"
  const load = loadPercent(telemetry)
  const memory = memoryUsedPercent(telemetry)
  const disk = diskUsedPercent(telemetry)
  const gpu = telemetry?.gpu_utilization_percent ?? null
  const gpuMemory = gpuMemoryPercent(telemetry)
  const live = isClientLive(client)

  return (
    <div className="grid gap-3 px-3 py-3 xl:grid-cols-[13rem_1fr_12rem]">
      <div className="min-w-0">
        <div className="flex items-center gap-2">
          {live ? (
            <Wifi className="size-4 text-primary" aria-hidden="true" />
          ) : (
            <WifiOff
              className="size-4 text-muted-foreground"
              aria-hidden="true"
            />
          )}
          <div className="min-w-0">
            <div className="truncate text-sm font-medium">{hostname}</div>
            <div className="truncate text-[11px] text-muted-foreground">
              {shortId(client.client_id)}
            </div>
          </div>
        </div>
        <div className="mt-2 grid gap-1 text-[11px] leading-5 text-muted-foreground">
          <InfoLine
            label="heartbeat"
            value={formatDateTime(client.last_heartbeat_at)}
          />
          <InfoLine
            label="worker"
            value={
              client.latest_worker_status?.process_state ??
              client.latest_status?.worker_state ??
              "unknown"
            }
          />
          <InfoLine
            label="active"
            value={
              client.latest_worker_status?.active_job_id
                ? shortId(client.latest_worker_status.active_job_id)
                : "-"
            }
          />
          <InfoLine
            label="events"
            value={
              client.latest_worker_status?.supports_job_events_jsonl
                ? "jsonl"
                : "unknown"
            }
          />
          <InfoLine
            label="uptime"
            value={formatDuration(telemetry?.uptime_seconds)}
          />
        </div>
      </div>

      <div className="grid gap-2 sm:grid-cols-5">
        <ResourceDial
          label="load"
          value={load}
          detail={compactLoad(telemetry)}
        />
        <ResourceDial
          label="mem"
          value={memory}
          detail={`${formatBytes(telemetry?.used_memory_bytes)} used`}
        />
        <ResourceDial
          label="disk"
          value={disk}
          detail={`${formatBytes(telemetry?.free_disk_bytes)} free`}
        />
        <ResourceDial
          label="gpu"
          value={gpu}
          detail={telemetry?.gpu ? "util" : "missing"}
        />
        <ResourceDial
          label="vram"
          value={gpuMemory}
          detail={gpuMemoryLabel(telemetry)}
        />
      </div>

      <div className="grid min-w-0 gap-2">
        <div className="grid grid-cols-[3rem_1fr] items-center gap-2 text-[11px]">
          <span className="text-muted-foreground">load</span>
          <Sparkline samples={history} metric="loadPercent" />
        </div>
        <div className="grid grid-cols-[3rem_1fr] items-center gap-2 text-[11px]">
          <span className="text-muted-foreground">mem</span>
          <Sparkline samples={history} metric="memoryPercent" />
        </div>
        <div className="grid grid-cols-[3rem_1fr] items-center gap-2 text-[11px]">
          <span className="text-muted-foreground">gpu</span>
          <Sparkline samples={history} metric="gpuPercent" />
        </div>
      </div>
    </div>
  )
}

function ClientDetails({
  client,
  history,
}: {
  client: ApiClientRecord
  history: TelemetrySample[]
}) {
  const hardware = client.enrollment?.hardware
  const telemetry = client.latest_telemetry
  const status = client.latest_status

  return (
    <div className="grid gap-3 text-[11px] xl:grid-cols-[1fr_1fr_1.2fr_1fr]">
      <DetailBlock
        title="identity"
        rows={[
          ["client_id", client.client_id],
          [
            "machine_id",
            hardware?.machine_id ?? client.enrollment?.machine_id ?? "-",
          ],
          ["public_key", shortKey(client.public_key)],
        ]}
      />
      <DetailBlock
        title="hardware"
        rows={[
          ["os", hardware?.os ?? "-"],
          ["arch", hardware?.arch ?? "-"],
          ["cpu", hardware?.cpu_count ? String(hardware.cpu_count) : "-"],
          ["memory", formatBytes(hardware?.total_memory_bytes)],
          ["gpu", hardware?.gpu ?? telemetry?.gpu ?? "-"],
        ]}
      />
      <DetailBlock
        title="runtime"
        rows={[
          ["heartbeat", formatDateTime(client.last_heartbeat_at)],
          ["connected", status ? String(status.connected) : "-"],
          ["worker_state", status?.worker_state ?? "-"],
          ["uptime", formatDuration(telemetry?.uptime_seconds)],
          ["load", compactLoad(telemetry)],
          ["memory_used", formatBytes(telemetry?.used_memory_bytes)],
          ["memory_avail", formatBytes(telemetry?.available_memory_bytes)],
          ["disk_free", formatBytes(telemetry?.free_disk_bytes)],
          ["gpu_util", formatPercent(telemetry?.gpu_utilization_percent)],
          ["gpu_mem", gpuMemoryLabel(telemetry)],
        ]}
      />
      <div className="border bg-background/35">
        <div className="border-b bg-muted/30 px-2 py-1 text-muted-foreground uppercase">
          history
        </div>
        <div className="grid gap-2 p-2">
          <SparklineRow label="load" samples={history} metric="loadPercent" />
          <SparklineRow label="mem" samples={history} metric="memoryPercent" />
          <SparklineRow label="disk" samples={history} metric="diskPercent" />
          <SparklineRow label="gpu" samples={history} metric="gpuPercent" />
        </div>
      </div>
    </div>
  )
}

function OverviewView({
  clients,
  jobs,
  jobEvents,
  jobEventsError,
  streamEvents,
  telemetryHistory,
}: {
  clients: ApiClientRecord[]
  jobs: JobRecord[]
  jobEvents: JobEvent[]
  jobEventsError: string | null
  streamEvents: string[]
  telemetryHistory: Record<string, TelemetrySample[]>
}) {
  return (
    <div className="grid h-full min-h-0 auto-rows-max content-start gap-3 overflow-y-auto xl:grid-cols-[1fr_22rem]">
      <div className="grid min-w-0 auto-rows-max content-start gap-3">
        <HostsTelemetryPanel
          clients={clients}
          telemetryHistory={telemetryHistory}
        />
        <JobsView
          jobs={jobs}
          jobEvents={jobEvents}
          jobEventsError={jobEventsError}
          clients={clients}
          artifacts={[]}
          templates={[]}
          imageryAssets={[]}
          imagerySource="idle"
          error={null}
          onLoadImagery={() => undefined}
          onRefresh={() => undefined}
          compact
        />
      </div>
      <LiveEventPanel events={streamEvents} streamState="live" />
    </div>
  )
}

function JobsView({
  jobs,
  jobEvents,
  jobEventsError,
  clients,
  artifacts,
  templates,
  imageryAssets,
  imagerySource,
  error,
  onLoadImagery,
  onRefresh,
  compact = false,
}: {
  jobs: JobRecord[]
  jobEvents: JobEvent[]
  jobEventsError: string | null
  clients: ApiClientRecord[]
  artifacts: UploadedArtifact[]
  templates: JobTemplate[]
  imageryAssets: StudioImageAsset[]
  imagerySource: ImagerySource
  error: string | null
  onLoadImagery: () => void
  onRefresh: () => void
  compact?: boolean
}) {
  const liveClients = useMemo(
    () => clients.filter((client) => !client.revoked_at),
    [clients]
  )
  const groupOptions = useMemo(
    () => uniqueText(imageryAssets.map(frontendAssetGroupName)),
    [imageryAssets]
  )
  const artifactsByJob = useMemo(
    () => groupArtifactsByJob(artifacts),
    [artifacts]
  )
  const eventsByJob = useMemo(() => groupJobEventsByJob(jobEvents), [jobEvents])
  const [expandedJobIds, setExpandedJobIds] = useState<string[]>([])
  const [builderOpen, setBuilderOpen] = useState(false)
  const [selectedClientId, setSelectedClientId] = useState("")
  const [templateId, setTemplateId] = useState("")
  const [savedTemplates, setSavedTemplates] = useState<SavedJobTemplate[]>(() =>
    loadSavedJobTemplates()
  )
  const [jobName, setJobName] = useState("")
  const [customName, setCustomName] = useState("")
  const [customProjectFilename, setCustomProjectFilename] = useState("")
  const [customOrthomosaicFilename, setCustomOrthomosaicFilename] = useState("")
  const [customStageIds, setCustomStageIds] = useState<string[]>([])
  const [selectionMode, setSelectionMode] = useState<"group_name" | "polygon">(
    "group_name"
  )
  const [groupName, setGroupName] = useState("")
  const [polygonText, setPolygonText] = useState(defaultPolygonText)
  const [buildPreview, setBuildPreview] = useState<BuildJobResponse | null>(
    null
  )
  const [buildError, setBuildError] = useState<string | null>(null)
  const [buildState, setBuildState] = useState<"idle" | "dry_run" | "queue">(
    "idle"
  )
  const effectiveSelectedClientId =
    selectedClientId || liveClients[0]?.client_id || ""
  const effectiveGroupName = groupName || groupOptions[0] || ""

  const templateOptions = useMemo(
    () => jobTemplateOptions(templates, savedTemplates),
    [templates, savedTemplates]
  )
  const availableStages = useMemo(
    () =>
      orderedUniqueStages([
        ...realityScanStageOrder,
        ...templateOptions.flatMap((template) => template.stages),
      ]),
    [templateOptions]
  )
  const effectiveTemplateId = templateId || templateOptions[0]?.key || ""
  const selectedTemplate =
    templateOptions.find((template) => template.key === effectiveTemplateId) ??
    templateOptions[0] ??
    null

  const seedCustomTemplateForm = (template: TemplateOption | null) => {
    if (!template) return
    setCustomName(`${template.name} custom`)
    setCustomProjectFilename(template.project_filename)
    setCustomOrthomosaicFilename(template.orthomosaic_filename ?? "")
    setCustomStageIds(template.stages)
  }

  const resetPreview = () => {
    setBuildPreview(null)
    setBuildError(null)
  }

  const persistSavedTemplates = (nextTemplates: SavedJobTemplate[]) => {
    setSavedTemplates(nextTemplates)
    saveSavedJobTemplates(nextTemplates)
  }

  const saveCustomTemplate = () => {
    if (!selectedTemplate) {
      setBuildError("select a template before saving a custom template")
      return
    }
    const name = customName.trim()
    const stages = uniqueStages(customStageIds)
    if (!name) {
      setBuildError("custom template name is required")
      return
    }
    if (stages.length === 0) {
      setBuildError("custom template needs at least one stage")
      return
    }
    const template: SavedJobTemplate = {
      template_id: `custom_${slugify(name)}_${Date.now()}`,
      name,
      description: `Saved custom template based on ${selectedTemplate.name}.`,
      stages,
      project_filename:
        customProjectFilename.trim() ||
        selectedTemplate.project_filename ||
        "custom.rsproj",
      orthomosaic_filename: customOrthomosaicFilename.trim() || null,
      ortho_pixel_size_meters: selectedTemplate.ortho_pixel_size_meters ?? null,
      saved_at: new Date().toISOString(),
    }
    const nextTemplates = [
      template,
      ...savedTemplates.filter(
        (savedTemplate) => savedTemplate.template_id !== template.template_id
      ),
    ].slice(0, 24)
    persistSavedTemplates(nextTemplates)
    setTemplateId(templateOptionKey("custom", template.template_id))
    setBuildError(null)
    resetPreview()
  }

  const removeSavedTemplate = (template: TemplateOption) => {
    if (template.source !== "custom") return
    const nextTemplates = savedTemplates.filter(
      (savedTemplate) => savedTemplate.template_id !== template.template_id
    )
    persistSavedTemplates(nextTemplates)
    if (effectiveTemplateId === template.key) {
      setTemplateId(
        templateOptions.find((option) => option.source === "built_in")?.key ??
          ""
      )
    }
    resetPreview()
  }

  const updateCustomStages = (stages: string[]) => {
    setCustomStageIds(stages)
    resetPreview()
  }

  const submitBuild = async (dryRun: boolean) => {
    if (!effectiveSelectedClientId || !selectedTemplate) {
      setBuildError("select a client and template first")
      return
    }
    let source:
      | { mode: "group_name"; group_name: string }
      | { mode: "polygon"; coordinates: Array<[number, number]> }
    try {
      source =
        selectionMode === "group_name"
          ? { mode: "group_name", group_name: effectiveGroupName }
          : { mode: "polygon", coordinates: parsePolygonText(polygonText) }
    } catch (error) {
      setBuildError(error instanceof Error ? error.message : "invalid polygon")
      return
    }

    setBuildState(dryRun ? "dry_run" : "queue")
    setBuildError(null)
    try {
      const response = await fetch("/api/admin/jobs/build", {
        method: "POST",
        headers: {
          Accept: "application/json",
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          client_id: effectiveSelectedClientId,
          template_id: selectedTemplate.template_id,
          custom_template:
            selectedTemplate.source === "custom"
              ? jobTemplatePayload(selectedTemplate)
              : null,
          job_name: jobName.trim() || null,
          source,
          dry_run: dryRun,
        }),
      })
      if (!response.ok) {
        throw new Error(await apiErrorMessage(response, "job builder api"))
      }
      const payload = (await response.json()) as BuildJobResponse
      setBuildPreview(payload)
      if (!dryRun) {
        onRefresh()
      }
    } catch (error) {
      setBuildError(error instanceof Error ? error.message : "job build failed")
    } finally {
      setBuildState("idle")
    }
  }

  const toggleJob = (jobId: string) => {
    setExpandedJobIds((current) =>
      current.includes(jobId)
        ? current.filter((id) => id !== jobId)
        : [...current, jobId]
    )
  }

  if (compact) {
    return (
      <JobsTable
        jobs={jobs}
        eventsByJob={eventsByJob}
        eventsError={jobEventsError}
        artifactsByJob={artifactsByJob}
        expandedJobIds={expandedJobIds}
        onToggleJob={toggleJob}
      />
    )
  }

  return (
    <div className="relative h-full min-h-0 min-w-0">
      <JobsTable
        jobs={jobs}
        eventsByJob={eventsByJob}
        eventsError={jobEventsError}
        artifactsByJob={artifactsByJob}
        expandedJobIds={expandedJobIds}
        onToggleJob={toggleJob}
        headerRight={
          <div className="flex items-center gap-2 normal-case">
            {error ? (
              <span className="hidden max-w-72 truncate text-destructive sm:inline">
                jobs api: {error}
              </span>
            ) : null}
            <span>{jobs.length} known</span>
            <Button
              size="icon-xs"
              variant="ghost"
              className="rounded"
              onClick={onRefresh}
              title="Refresh jobs"
            >
              <RefreshCw className="size-3" aria-hidden="true" />
            </Button>
            <Button
              size="sm"
              className="h-6 rounded px-2 text-[11px]"
              onClick={() => {
                seedCustomTemplateForm(selectedTemplate)
                setBuilderOpen(true)
              }}
            >
              <Plus className="size-3" aria-hidden="true" />
              new job
            </Button>
          </div>
        }
      />

      {builderOpen ? (
        <div
          className="fixed inset-0 z-50 bg-background/80 p-2 backdrop-blur-sm sm:p-4"
          onMouseDown={(event) => {
            if (event.target === event.currentTarget) setBuilderOpen(false)
          }}
        >
          <div
            role="dialog"
            aria-modal="true"
            aria-label="job builder"
            className="mx-auto flex h-full min-h-0 max-w-5xl flex-col border bg-card shadow-xl"
          >
            <PanelHeader
              title="job builder"
              right={
                <div className="flex items-center gap-2 normal-case">
                  <span>{templateOptions.length} templates</span>
                  <Button
                    size="icon-xs"
                    variant="ghost"
                    className="rounded"
                    onClick={onRefresh}
                    title="Refresh jobs"
                  >
                    <RefreshCw className="size-3" aria-hidden="true" />
                  </Button>
                  <Button
                    size="icon-xs"
                    variant="ghost"
                    className="rounded"
                    onClick={() => setBuilderOpen(false)}
                    title="Close job builder"
                  >
                    <X className="size-3" aria-hidden="true" />
                  </Button>
                </div>
              }
            />
            {error ? (
              <div className="border-b px-3 py-2 text-[11px] text-muted-foreground">
                jobs api: {error}
              </div>
            ) : null}
            {buildError ? (
              <div className="border-b px-3 py-2 text-[11px] text-destructive">
                {buildError}
              </div>
            ) : null}

            <div className="grid border-b bg-background/35 px-3 py-2 text-[11px] sm:grid-cols-4">
              {["client", "imagery", "review", "queue"].map((label, index) => (
                <div
                  key={label}
                  className={`border-r px-2 last:border-r-0 ${jobBuilderStepClass(
                    index,
                    buildPreview
                  )}`}
                >
                  {index + 1}. {label}
                </div>
              ))}
            </div>

            <div className="min-h-0 flex-1 overflow-y-auto p-3">
              <div className="grid gap-3">
                <div className="grid gap-3 lg:grid-cols-[14rem_1fr]">
                  <Field label="client">
                    <select
                      className="h-8 w-full border bg-background px-2 text-xs"
                      value={effectiveSelectedClientId}
                      onChange={(event) => {
                        setSelectedClientId(event.target.value)
                        resetPreview()
                      }}
                    >
                      {liveClients.length === 0 ? (
                        <option value="">no clients</option>
                      ) : (
                        liveClients.map((client) => (
                          <option
                            key={client.client_id}
                            value={client.client_id}
                          >
                            {client.latest_telemetry?.hostname ??
                              client.enrollment?.hostname ??
                              shortId(client.client_id)}
                          </option>
                        ))
                      )}
                    </select>
                  </Field>
                  <Field label="job name">
                    <input
                      className="h-8 w-full border bg-background px-2 text-xs"
                      value={jobName}
                      placeholder={selectedTemplate?.name ?? "job name"}
                      onChange={(event) => {
                        setJobName(event.target.value)
                        resetPreview()
                      }}
                    />
                  </Field>
                </div>

                <TemplateLibrary
                  templates={templateOptions}
                  selectedTemplate={selectedTemplate}
                  selectedTemplateKey={effectiveTemplateId}
                  onSelectTemplate={(nextTemplateId) => {
                    const nextTemplate =
                      templateOptions.find(
                        (template) => template.key === nextTemplateId
                      ) ?? null
                    setTemplateId(nextTemplateId)
                    seedCustomTemplateForm(nextTemplate)
                    resetPreview()
                  }}
                  onRemoveTemplate={removeSavedTemplate}
                />

                <CustomTemplateEditor
                  availableStages={availableStages}
                  name={customName}
                  projectFilename={customProjectFilename}
                  orthomosaicFilename={customOrthomosaicFilename}
                  stageIds={customStageIds}
                  onNameChange={(value) => {
                    setCustomName(value)
                    resetPreview()
                  }}
                  onProjectFilenameChange={(value) => {
                    setCustomProjectFilename(value)
                    resetPreview()
                  }}
                  onOrthomosaicFilenameChange={(value) => {
                    setCustomOrthomosaicFilename(value)
                    resetPreview()
                  }}
                  onStageIdsChange={updateCustomStages}
                  onSave={saveCustomTemplate}
                />

                <div className="grid gap-3 lg:grid-cols-[10rem_1fr]">
                  <Field label="source">
                    <select
                      className="h-8 w-full border bg-background px-2 text-xs"
                      value={selectionMode}
                      onChange={(event) => {
                        setSelectionMode(
                          event.target.value === "polygon"
                            ? "polygon"
                            : "group_name"
                        )
                        resetPreview()
                      }}
                    >
                      <option value="group_name">group_name</option>
                      <option value="polygon">polygon</option>
                    </select>
                  </Field>

                  {selectionMode === "group_name" ? (
                    <Field label="image group">
                      <div className="grid grid-cols-[1fr_auto] gap-2">
                        <select
                          className="h-8 w-full border bg-background px-2 text-xs"
                          value={effectiveGroupName}
                          onChange={(event) => {
                            setGroupName(event.target.value)
                            resetPreview()
                          }}
                        >
                          {groupOptions.length === 0 ? (
                            <option value="">no groups loaded</option>
                          ) : (
                            groupOptions.map((group) => (
                              <option key={group} value={group}>
                                {group}
                              </option>
                            ))
                          )}
                        </select>
                        <Button
                          size="sm"
                          variant="secondary"
                          className="h-8 rounded"
                          onClick={onLoadImagery}
                        >
                          load
                        </Button>
                      </div>
                    </Field>
                  ) : (
                    <Field label="polygon lon,lat">
                      <textarea
                        className="min-h-24 w-full resize-y border bg-background px-2 py-2 text-xs leading-5"
                        value={polygonText}
                        onChange={(event) => {
                          setPolygonText(event.target.value)
                          resetPreview()
                        }}
                      />
                    </Field>
                  )}
                </div>

                <div className="grid gap-2 border bg-background/35 p-2 text-[11px] text-muted-foreground">
                  <InfoLine
                    label="imagery"
                    value={imagerySourceLabel(imagerySource)}
                  />
                  <InfoLine
                    label="assets loaded"
                    value={String(imageryAssets.length)}
                  />
                  <InfoLine
                    label="selection"
                    value={
                      selectionMode === "group_name"
                        ? effectiveGroupName || "-"
                        : "polygon"
                    }
                  />
                </div>

                <div className="flex flex-wrap gap-2">
                  <Button
                    size="sm"
                    variant="secondary"
                    className="h-8 rounded"
                    disabled={buildState !== "idle"}
                    onClick={() => void submitBuild(true)}
                  >
                    dry run
                  </Button>
                  <Button
                    size="sm"
                    className="h-8 rounded"
                    disabled={buildState !== "idle" || !buildPreview}
                    onClick={() => void submitBuild(false)}
                  >
                    queue job
                  </Button>
                  <span className="self-center text-[11px] text-muted-foreground">
                    {buildState === "idle" ? "ready" : "working"}
                  </span>
                </div>

                <JobBuildPreview preview={buildPreview} />
              </div>
            </div>
          </div>
        </div>
      ) : null}
    </div>
  )
}

function TemplateLibrary({
  templates,
  selectedTemplate,
  selectedTemplateKey,
  onSelectTemplate,
  onRemoveTemplate,
}: {
  templates: TemplateOption[]
  selectedTemplate: TemplateOption | null
  selectedTemplateKey: string
  onSelectTemplate: (templateKey: string) => void
  onRemoveTemplate: (template: TemplateOption) => void
}) {
  const builtInTemplates = templates.filter(
    (template) => template.source === "built_in"
  )
  const customTemplates = templates.filter(
    (template) => template.source === "custom"
  )

  return (
    <div className="grid gap-3 xl:grid-cols-[18rem_minmax(0,1fr)]">
      <div className="min-w-0 border bg-background/35">
        <PanelHeader
          title="template library"
          right={`${builtInTemplates.length} built-in / ${customTemplates.length} saved`}
        />
        <div className="max-h-80 overflow-y-auto">
          <TemplateOptionGroup
            title="built in"
            templates={builtInTemplates}
            selectedTemplateKey={selectedTemplateKey}
            onSelectTemplate={onSelectTemplate}
            onRemoveTemplate={onRemoveTemplate}
          />
          <TemplateOptionGroup
            title="saved custom"
            templates={customTemplates}
            selectedTemplateKey={selectedTemplateKey}
            emptyText="no saved custom templates"
            onSelectTemplate={onSelectTemplate}
            onRemoveTemplate={onRemoveTemplate}
          />
        </div>
      </div>

      <div className="min-w-0 border bg-background/35">
        <PanelHeader
          title="selected template"
          right={
            selectedTemplate ? templateSourceLabel(selectedTemplate) : "none"
          }
        />
        {selectedTemplate ? (
          <div className="grid gap-3 p-3 text-[11px]">
            <div className="grid gap-1">
              <div className="text-xs font-medium">{selectedTemplate.name}</div>
              <div className="text-muted-foreground">
                {selectedTemplate.description}
              </div>
            </div>
            <div className="grid gap-2 md:grid-cols-4">
              <InfoLine label="id" value={selectedTemplate.template_id} />
              <InfoLine
                label="project"
                value={selectedTemplate.project_filename}
              />
              <InfoLine
                label="ortho"
                value={selectedTemplate.orthomosaic_filename ?? "-"}
              />
              <InfoLine
                label="gsd"
                value={
                  selectedTemplate.ortho_pixel_size_meters
                    ? `${selectedTemplate.ortho_pixel_size_meters} m`
                    : "-"
                }
              />
            </div>
            <StageList stages={selectedTemplate.stages} />
          </div>
        ) : (
          <div className="px-3 py-6 text-xs text-muted-foreground">
            no templates loaded
          </div>
        )}
      </div>
    </div>
  )
}

function TemplateOptionGroup({
  title,
  templates,
  selectedTemplateKey,
  emptyText = "none",
  onSelectTemplate,
  onRemoveTemplate,
}: {
  title: string
  templates: TemplateOption[]
  selectedTemplateKey: string
  emptyText?: string
  onSelectTemplate: (templateKey: string) => void
  onRemoveTemplate: (template: TemplateOption) => void
}) {
  return (
    <section className="border-b last:border-b-0">
      <div className="border-b bg-muted/20 px-2 py-1 text-[10px] text-muted-foreground uppercase">
        {title}
      </div>
      {templates.length === 0 ? (
        <div className="px-2 py-3 text-[11px] text-muted-foreground">
          {emptyText}
        </div>
      ) : (
        <div className="grid">
          {templates.map((template) => {
            const selected = template.key === selectedTemplateKey
            return (
              <div
                key={template.key}
                className={`grid grid-cols-[1fr_auto] items-stretch border-b last:border-b-0 ${
                  selected ? "bg-primary/10" : "hover:bg-muted/25"
                }`}
              >
                <button
                  type="button"
                  className="grid min-w-0 gap-1 px-2 py-2 text-left"
                  aria-pressed={selected}
                  onClick={() => onSelectTemplate(template.key)}
                >
                  <span className="truncate text-xs font-medium">
                    {template.name}
                  </span>
                  <span className="truncate text-[11px] text-muted-foreground">
                    {template.stages.length} stages /{" "}
                    {template.orthomosaic_filename ? "ortho export" : "project"}
                  </span>
                </button>
                {template.source === "custom" ? (
                  <Button
                    size="icon-xs"
                    variant="ghost"
                    className="m-1 self-center rounded"
                    title="Delete saved template"
                    onClick={() => onRemoveTemplate(template)}
                  >
                    <Trash2 className="size-3" aria-hidden="true" />
                  </Button>
                ) : null}
              </div>
            )
          })}
        </div>
      )}
    </section>
  )
}

function CustomTemplateEditor({
  availableStages,
  name,
  projectFilename,
  orthomosaicFilename,
  stageIds,
  onNameChange,
  onProjectFilenameChange,
  onOrthomosaicFilenameChange,
  onStageIdsChange,
  onSave,
}: {
  availableStages: string[]
  name: string
  projectFilename: string
  orthomosaicFilename: string
  stageIds: string[]
  onNameChange: (value: string) => void
  onProjectFilenameChange: (value: string) => void
  onOrthomosaicFilenameChange: (value: string) => void
  onStageIdsChange: (stageIds: string[]) => void
  onSave: () => void
}) {
  const [stageToAdd, setStageToAdd] = useState("")
  const sensors = useSensors(
    useSensor(PointerSensor),
    useSensor(KeyboardSensor, {
      coordinateGetter: sortableKeyboardCoordinates,
    })
  )
  const addableStages = availableStages.filter(
    (stage) => !stageIds.includes(stage)
  )
  const effectiveStageToAdd = addableStages.includes(stageToAdd)
    ? stageToAdd
    : (addableStages[0] ?? "")

  const addStage = () => {
    if (!effectiveStageToAdd) return
    onStageIdsChange([...stageIds, effectiveStageToAdd])
    setStageToAdd("")
  }

  const removeStage = (stage: string) => {
    onStageIdsChange(stageIds.filter((value) => value !== stage))
  }

  const handleDragEnd = (event: import("@dnd-kit/core").DragEndEvent) => {
    const { active, over } = event
    if (!over || active.id === over.id) return
    const oldIndex = stageIds.indexOf(String(active.id))
    const newIndex = stageIds.indexOf(String(over.id))
    if (oldIndex === -1 || newIndex === -1) return
    onStageIdsChange(arrayMove(stageIds, oldIndex, newIndex))
  }

  return (
    <div className="border bg-background/35">
      <PanelHeader
        title="save custom template"
        right={
          <Button
            size="sm"
            className="h-6 rounded px-2 text-[11px] normal-case"
            onClick={onSave}
          >
            <Save className="size-3" aria-hidden="true" />
            save
          </Button>
        }
      />
      <div className="grid gap-3 p-3">
        <div className="grid gap-3 md:grid-cols-3">
          <Field label="custom name">
            <input
              className="h-8 w-full border bg-background px-2 text-xs"
              value={name}
              onChange={(event) => onNameChange(event.target.value)}
            />
          </Field>
          <Field label="project filename">
            <input
              className="h-8 w-full border bg-background px-2 text-xs"
              value={projectFilename}
              onChange={(event) => onProjectFilenameChange(event.target.value)}
            />
          </Field>
          <Field label="orthomosaic filename">
            <input
              className="h-8 w-full border bg-background px-2 text-xs"
              value={orthomosaicFilename}
              placeholder="optional"
              onChange={(event) =>
                onOrthomosaicFilenameChange(event.target.value)
              }
            />
          </Field>
        </div>
        <div className="grid gap-2">
          <div className="text-[11px] text-muted-foreground uppercase">
            ordered stages
          </div>
          <div className="grid gap-2 md:grid-cols-[minmax(0,1fr)_13rem]">
            <DndContext
              sensors={sensors}
              collisionDetection={closestCenter}
              onDragEnd={handleDragEnd}
            >
              <SortableContext
                items={stageIds}
                strategy={verticalListSortingStrategy}
              >
                <div className="grid gap-px border bg-border">
                  {stageIds.length === 0 ? (
                    <div className="bg-card px-3 py-6 text-xs text-muted-foreground">
                      no stages selected
                    </div>
                  ) : (
                    stageIds.map((stage, index) => (
                      <SortableStageRow
                        key={stage}
                        stage={stage}
                        index={index}
                        onRemove={removeStage}
                      />
                    ))
                  )}
                </div>
              </SortableContext>
            </DndContext>

            <div className="grid content-start gap-2 border bg-card p-2">
              <Field label="add stage">
                <select
                  className="h-8 w-full border bg-background px-2 text-xs"
                  value={effectiveStageToAdd}
                  disabled={addableStages.length === 0}
                  onChange={(event) => setStageToAdd(event.target.value)}
                >
                  {addableStages.length === 0 ? (
                    <option value="">all stages added</option>
                  ) : (
                    addableStages.map((stage) => (
                      <option key={stage} value={stage}>
                        {formatStage(stage)}
                      </option>
                    ))
                  )}
                </select>
              </Field>
              <Button
                size="sm"
                variant="secondary"
                className="h-8 rounded"
                disabled={!effectiveStageToAdd}
                onClick={addStage}
              >
                <Plus className="size-3" aria-hidden="true" />
                add stage
              </Button>
            </div>
          </div>
        </div>
      </div>
    </div>
  )
}

function SortableStageRow({
  stage,
  index,
  onRemove,
}: {
  stage: string
  index: number
  onRemove: (stage: string) => void
}) {
  const {
    attributes,
    listeners,
    setNodeRef,
    transform,
    transition,
    isDragging,
  } = useSortable({ id: stage })
  const style = {
    transform: CSS.Transform.toString(transform),
    transition,
  }

  return (
    <div
      ref={setNodeRef}
      style={style}
      className={`grid grid-cols-[2rem_2.5rem_1fr_2rem] items-center bg-card text-[11px] ${
        isDragging ? "relative z-10 opacity-80 shadow-md" : ""
      }`}
    >
      <button
        type="button"
        className="flex h-9 cursor-grab items-center justify-center border-r text-muted-foreground active:cursor-grabbing"
        title="Drag stage"
        {...attributes}
        {...listeners}
      >
        <GripVertical className="size-3" aria-hidden="true" />
      </button>
      <span className="border-r px-2 py-2 text-muted-foreground">
        {String(index + 1).padStart(2, "0")}
      </span>
      <span className="truncate px-2 py-2">{formatStage(stage)}</span>
      <Button
        size="icon-xs"
        variant="ghost"
        className="m-1 rounded"
        title="Remove stage"
        onClick={() => onRemove(stage)}
      >
        <X className="size-3" aria-hidden="true" />
      </Button>
    </div>
  )
}

function StageList({ stages }: { stages: string[] }) {
  return (
    <div className="grid gap-px border bg-border">
      {stages.map((stage, index) => (
        <div
          key={`${stage}-${index}`}
          className="grid grid-cols-[2.5rem_1fr] bg-card text-[11px]"
        >
          <span className="border-r px-2 py-1 text-muted-foreground">
            {String(index + 1).padStart(2, "0")}
          </span>
          <span className="px-2 py-1">{formatStage(stage)}</span>
        </div>
      ))}
    </div>
  )
}

function JobsTable({
  jobs,
  eventsByJob,
  eventsError,
  artifactsByJob,
  expandedJobIds,
  onToggleJob,
  headerRight,
}: {
  jobs: JobRecord[]
  eventsByJob: Map<string, JobEvent[]>
  eventsError: string | null
  artifactsByJob: Map<string, UploadedArtifact[]>
  expandedJobIds: string[]
  onToggleJob: (jobId: string) => void
  headerRight?: import("react").ReactNode
}) {
  return (
    <div className="flex h-full min-h-0 min-w-0 flex-col border bg-card">
      <PanelHeader title="jobs" right={headerRight ?? `${jobs.length} known`} />
      <div className="min-h-0 flex-1 overflow-auto">
        <table className="w-full min-w-[860px] border-collapse text-left text-xs">
          <thead className="sticky top-0 z-10 bg-muted/40 text-[11px] text-muted-foreground uppercase">
            <tr className="border-b">
              <Th className="w-8" />
              <Th>job id</Th>
              <Th>client</Th>
              <Th>state</Th>
              <Th>template</Th>
              <Th>inputs</Th>
              <Th>artifacts</Th>
              <Th>updated</Th>
            </tr>
          </thead>
          <tbody>
            {jobs.length === 0 ? (
              <tr>
                <td className="px-3 py-6 text-muted-foreground" colSpan={8}>
                  no jobs recorded
                </td>
              </tr>
            ) : (
              jobs.map((job) => {
                const expanded = expandedJobIds.includes(job.job_id)
                const jobArtifacts = artifactsByJob.get(job.job_id) ?? []
                const jobEvents = eventsByJob.get(job.job_id) ?? []
                return (
                  <Fragment key={job.job_id}>
                    <tr className="border-b hover:bg-muted/25">
                      <Td>
                        <Button
                          size="icon-xs"
                          variant="ghost"
                          className="rounded"
                          aria-expanded={expanded}
                          onClick={() => onToggleJob(job.job_id)}
                        >
                          {expanded ? (
                            <ChevronDown
                              className="size-3"
                              aria-hidden="true"
                            />
                          ) : (
                            <ChevronRight
                              className="size-3"
                              aria-hidden="true"
                            />
                          )}
                        </Button>
                      </Td>
                      <Td className="font-medium">{shortId(job.job_id)}</Td>
                      <Td>{shortId(job.client_id)}</Td>
                      <Td>
                        <StatusPill state={job.state} />
                      </Td>
                      <Td>{jobTemplateLabel(job)}</Td>
                      <Td>{job.job.manifest?.inputs?.length ?? 0}</Td>
                      <Td>{jobArtifacts.length}</Td>
                      <Td>{formatDateTime(job.updated_at)}</Td>
                    </tr>
                    {expanded ? (
                      <tr className="border-b bg-muted/10">
                        <td colSpan={8} className="px-3 py-3">
                          <JobDetails
                            job={job}
                            artifacts={jobArtifacts}
                            events={jobEvents}
                            eventsError={eventsError}
                          />
                        </td>
                      </tr>
                    ) : null}
                  </Fragment>
                )
              })
            )}
          </tbody>
        </table>
      </div>
    </div>
  )
}

function JobDetails({
  job,
  artifacts,
  events,
  eventsError,
}: {
  job: JobRecord
  artifacts: UploadedArtifact[]
  events: JobEvent[]
  eventsError: string | null
}) {
  const inputs = job.job.manifest?.inputs ?? []
  const pipeline = job.job.pipeline
  return (
    <div className="grid gap-3 text-[11px] xl:grid-cols-[23rem_minmax(0,1fr)]">
      <div className="min-w-0 xl:row-span-3 xl:w-[23rem] xl:self-start">
        <JobStageTimeline job={job} events={events} />
      </div>
      <div className="grid min-w-0 gap-3 md:grid-cols-2 xl:col-start-2">
        <DetailBlock
          title="pipeline"
          rows={[
            ["name", job.job.job_name ?? "-"],
            ["template", pipeline?.template_id ?? "-"],
            ["image", job.job.realityscan_image ?? "-"],
            ["stage count", String(pipeline?.stages?.length ?? 0)],
            ["project", pipeline?.project_filename ?? "-"],
            ["ortho", pipeline?.orthomosaic_filename ?? "-"],
          ]}
        />
        <DetailBlock
          title="runtime"
          rows={[
            ["job_id", job.job_id],
            ["client_id", job.client_id],
            ["state", job.state],
            ["assigned", formatDateTime(job.assigned_at)],
            ["updated", formatDateTime(job.updated_at)],
            ["completed", formatDateTime(job.completed_at)],
          ]}
        />
      </div>
      <div className="grid min-w-0 gap-3 xl:col-start-2 2xl:grid-cols-[minmax(0,1fr)_minmax(0,1fr)]">
        <RecentJobEvents events={events} error={eventsError} />
        <div className="min-w-0 border bg-background/35">
          <div className="border-b bg-muted/30 px-2 py-1 text-muted-foreground uppercase">
            artifacts / {artifacts.length}
          </div>
          <div className="max-h-48 overflow-auto">
            {artifacts.length === 0 ? (
              <div className="px-2 py-3 text-muted-foreground">
                no artifact events yet
              </div>
            ) : (
              <table className="w-full min-w-[620px] border-collapse text-left">
                <tbody>
                  {artifacts.map((artifact) => (
                    <tr
                      key={artifact.artifact_id}
                      className="border-b last:border-b-0"
                    >
                      <Td className="font-medium">{artifact.filename}</Td>
                      <Td>{formatFileBytes(artifact.size_bytes)}</Td>
                      <Td>{artifact.content_type ?? "-"}</Td>
                      <Td className="max-w-72 truncate">
                        {artifact.storage_uri ?? "-"}
                      </Td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>
        </div>
      </div>
      <div className="min-w-0 border bg-background/35 xl:col-start-2">
        <div className="border-b bg-muted/30 px-2 py-1 text-muted-foreground uppercase">
          inputs / {inputs.length}
        </div>
        <div className="max-h-48 overflow-auto">
          {inputs.length === 0 ? (
            <div className="px-2 py-3 text-muted-foreground">no inputs</div>
          ) : (
            <table className="w-full min-w-[520px] border-collapse text-left">
              <tbody>
                {inputs.slice(0, 120).map((input) => (
                  <tr key={input.asset_id} className="border-b last:border-b-0">
                    <Td className="font-medium">
                      {assetFilenameFromInput(input)}
                    </Td>
                    <Td>{shortId(input.asset_id)}</Td>
                    <Td>{formatFileBytes(input.size_bytes)}</Td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>
      </div>
    </div>
  )
}

function JobStageTimeline({
  job,
  events,
}: {
  job: JobRecord
  events: JobEvent[]
}) {
  const rows = useMemo(() => jobTimelineRows(job), [job])
  const sortedEvents = useMemo(() => sortJobEvents(events), [events])
  const latestEvent = sortedEvents.at(-1) ?? null
  const progress = jobProgress(job, latestEvent)
  const currentIndex = currentTimelineIndex(rows, progress)
  const latestMessage = latestEvent?.message ?? job.state

  return (
    <div className="min-w-0 border bg-background/35">
      <PanelHeader
        title="stage progress"
        right={`${formatPercent(progress)} / ${formatClock(latestEvent?.observed_at ?? job.updated_at)}`}
      />
      <div className="border-b px-3 py-2 text-[11px] text-muted-foreground">
        <span className="font-medium text-foreground">
          {timelineStatusLabel(job.state)}
        </span>{" "}
        / {latestMessage}
      </div>
      <div className="relative px-3 py-3">
        <div className="grid gap-2">
          {rows.map((row, index) => {
            const status = timelineRowStatus(row, index, currentIndex, progress)
            const localProgress = timelineRowLocalProgress(row, progress)
            const connectorProgress = timelineConnectorProgress(
              status,
              localProgress
            )
            return (
              <div
                key={row.id}
                className="relative grid grid-cols-[1.5rem_minmax(0,1fr)_2.75rem] items-start gap-2 [--timeline-dot-center-y:0.5625rem] [--timeline-dot-center:0.3125rem] [--timeline-row-gap:0.5rem]"
              >
                {index < rows.length - 1 && connectorProgress > 0 ? (
                  <span
                    className="absolute top-[var(--timeline-dot-center-y)] left-[var(--timeline-dot-center)] z-0 w-px -translate-x-1/2 bg-primary transition-[height] duration-700"
                    style={{
                      height: `calc((100% + var(--timeline-row-gap)) * ${connectorProgress})`,
                    }}
                    aria-hidden="true"
                  />
                ) : null}
                <span
                  className={`relative z-10 mt-1 size-2.5 justify-self-start rounded-full border ${
                    status === "complete"
                      ? "border-primary bg-primary"
                      : status === "current"
                        ? "border-primary bg-background ring-2 ring-primary/20"
                        : "border-border bg-background"
                  }`}
                  aria-hidden="true"
                />
                <div className="min-w-0 pb-1">
                  <div className="flex min-w-0 items-center gap-2">
                    <span className="truncate text-xs font-medium">
                      {row.label}
                    </span>
                    <span className="text-[10px] text-muted-foreground">
                      {timelineRowLabel(status)}
                    </span>
                  </div>
                  {status === "current" ? (
                    <div className="mt-1 h-1 max-w-72 overflow-hidden bg-muted">
                      <div
                        className="h-full bg-primary transition-[width] duration-700"
                        style={{ width: `${localProgress}%` }}
                      />
                    </div>
                  ) : null}
                  {row.detail ? (
                    <div className="mt-1 truncate text-[10px] text-muted-foreground">
                      {row.detail}
                    </div>
                  ) : null}
                </div>
                <div className="pt-0.5 text-[10px] text-muted-foreground">
                  {formatPercent(row.end)}
                </div>
              </div>
            )
          })}
        </div>
      </div>
    </div>
  )
}

function RecentJobEvents({
  events,
  error,
}: {
  events: JobEvent[]
  error: string | null
}) {
  const recentEvents = sortJobEvents(events).slice(-10).reverse()
  return (
    <div className="min-w-0 border bg-background/35">
      <div className="border-b bg-muted/30 px-2 py-1 text-muted-foreground uppercase">
        job events / {error ? "unavailable" : events.length}
      </div>
      <div className="max-h-48 overflow-auto">
        {error ? (
          <div className="px-2 py-3 text-muted-foreground">
            event stream unavailable: {error}
          </div>
        ) : recentEvents.length === 0 ? (
          <div className="px-2 py-3 text-muted-foreground">
            no worker events recorded yet
          </div>
        ) : (
          <table className="w-full min-w-[820px] border-collapse text-left">
            <tbody>
              {recentEvents.map((event) => (
                <tr
                  key={`${event.observed_at}-${event.message}`}
                  className="border-b last:border-b-0"
                >
                  <Td className="whitespace-nowrap text-muted-foreground">
                    {formatClock(event.observed_at)}
                  </Td>
                  <Td>
                    <StatusPill state={event.state} />
                  </Td>
                  <Td>{formatPercent(event.progress)}</Td>
                  <Td className="whitespace-nowrap text-muted-foreground">
                    {jobEventKindLabel(event)}
                  </Td>
                  <Td className="max-w-48 truncate">
                    {jobEventDetailLabel(event)}
                  </Td>
                  <Td className="max-w-[34rem] truncate">
                    <span title={event.message}>{event.message}</span>
                  </Td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  )
}

function JobBuildPreview({ preview }: { preview: BuildJobResponse | null }) {
  if (!preview) {
    return (
      <div className="border bg-background/35 px-3 py-6 text-xs text-muted-foreground">
        run a dry run to preview selected imagery before queueing
      </div>
    )
  }

  return (
    <div className="border bg-background/35">
      <PanelHeader
        title="selection preview"
        right={`${preview.selected_assets.length} assets`}
      />
      {preview.warnings.length > 0 ? (
        <div className="border-b px-3 py-2 text-[11px] text-muted-foreground">
          {preview.warnings.join(" / ")}
        </div>
      ) : null}
      <div className="max-h-64 overflow-auto">
        <table className="w-full min-w-[640px] border-collapse text-left text-xs">
          <thead className="sticky top-0 bg-muted/30 text-[11px] text-muted-foreground uppercase">
            <tr className="border-b">
              <Th>asset</Th>
              <Th>group</Th>
              <Th>lat</Th>
              <Th>lon</Th>
              <Th>size</Th>
            </tr>
          </thead>
          <tbody>
            {preview.selected_assets.map((asset) => (
              <tr key={asset.asset_id} className="border-b last:border-b-0">
                <Td className="font-medium">{asset.filename}</Td>
                <Td>{asset.group_name ?? "-"}</Td>
                <Td>{formatCoordinate(asset.latitude)}</Td>
                <Td>{formatCoordinate(asset.longitude)}</Td>
                <Td>{formatFileBytes(asset.size_bytes)}</Td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}

function Field({
  label,
  children,
}: {
  label: string
  children: import("react").ReactNode
}) {
  return (
    <label className="grid gap-1 text-[11px] text-muted-foreground uppercase">
      <span>{label}</span>
      {children}
    </label>
  )
}

function ImageryView({
  assets,
  source,
  error,
  onRefresh,
}: {
  assets: StudioImageAsset[]
  source: ImagerySource
  error: string | null
  onRefresh: () => void
}) {
  const geoAssets = useMemo(
    () => assets.filter(imageAssetWithLocation),
    [assets]
  )
  const assetGroups = useMemo(() => groupImageryAssets(assets), [assets])
  const [selectedAssetId, setSelectedAssetId] = useState<string | null>(null)
  const [expandedGroupKeys, setExpandedGroupKeys] = useState<string[] | null>(
    null
  )
  const effectiveSelectedAssetId =
    selectedAssetId &&
    assets.some((asset) => assetId(asset) === selectedAssetId)
      ? selectedAssetId
      : assets[0]
        ? assetId(geoAssets[0] ?? assets[0])
        : null
  const selectedAsset =
    assets.find((asset) => assetId(asset) === effectiveSelectedAssetId) ??
    geoAssets[0] ??
    assets[0] ??
    null
  const selectedGroupKey = selectedAsset
    ? imageAssetGroupIdentity(selectedAsset).key
    : null
  const defaultExpandedGroupKeys = selectedGroupKey
    ? [selectedGroupKey]
    : assetGroups[0]
      ? [assetGroups[0].key]
      : []
  const visibleExpandedGroupKeys = expandedGroupKeys ?? defaultExpandedGroupKeys

  const toggleImageGroup = (groupKey: string) => {
    setExpandedGroupKeys((current) => {
      const base = current ?? defaultExpandedGroupKeys
      return base.includes(groupKey)
        ? base.filter((key) => key !== groupKey)
        : [...base, groupKey]
    })
  }

  return (
    <div className="grid h-full min-h-0 gap-3 xl:grid-cols-[1fr_22rem]">
      <div className="flex min-h-0 min-w-0 flex-col border bg-card">
        <PanelHeader
          title="imagery / studio map"
          right={
            <div className="flex items-center gap-2">
              <span>{imagerySourceLabel(source)}</span>
              <Button
                size="icon-xs"
                variant="ghost"
                className="rounded"
                onClick={onRefresh}
                title="Refresh imagery"
              >
                <RefreshCw
                  className={`size-3 ${source === "loading" ? "animate-spin" : ""}`}
                  aria-hidden="true"
                />
              </Button>
            </div>
          }
        />

        {error ? (
          <div className="border-b px-3 py-2 text-[11px] text-muted-foreground">
            studio api: {error}
          </div>
        ) : null}

        <div className="grid flex-none gap-3 p-3 lg:grid-cols-[1fr_16rem]">
          <ImageryMap
            assets={geoAssets}
            selectedAssetId={selectedAsset ? assetId(selectedAsset) : null}
            onSelect={setSelectedAssetId}
          />
          <DetailBlock
            title="coverage"
            rows={[
              ["assets", String(assets.length)],
              ["geocoded", String(geoAssets.length)],
              [
                "missing_geo",
                String(Math.max(0, assets.length - geoAssets.length)),
              ],
              ["source", imagerySourceLabel(source)],
              ["bounds", imageryBoundsLabel(geoAssets)],
            ]}
          />
        </div>

        <ImageryGroupsPanel
          groups={assetGroups}
          source={source}
          selectedAssetId={selectedAsset ? assetId(selectedAsset) : null}
          expandedGroupKeys={visibleExpandedGroupKeys}
          onToggleGroup={toggleImageGroup}
          onSelectAsset={setSelectedAssetId}
        />
      </div>

      <ImageryAssetDetails asset={selectedAsset} />
    </div>
  )
}

function ImageryGroupsPanel({
  groups,
  source,
  selectedAssetId,
  expandedGroupKeys,
  onToggleGroup,
  onSelectAsset,
}: {
  groups: ImageryGroup[]
  source: ImagerySource
  selectedAssetId: string | null
  expandedGroupKeys: string[]
  onToggleGroup: (groupKey: string) => void
  onSelectAsset: (assetId: string) => void
}) {
  const expanded = new Set(expandedGroupKeys)

  return (
    <div className="flex min-h-0 flex-1 flex-col border-t">
      <PanelHeader
        title="image groups"
        right={`${groups.length} groups / ${sumGroupAssets(groups)} assets`}
      />
      <div
        data-testid="imagery-groups"
        className="min-h-0 flex-1 overflow-y-auto"
      >
        {groups.length === 0 ? (
          <div className="px-3 py-6 text-xs text-muted-foreground">
            {source === "loading"
              ? "loading studio imagery"
              : "no image assets loaded"}
          </div>
        ) : (
          groups.map((group) => {
            const isExpanded = expanded.has(group.key)
            return (
              <section key={group.key} className="border-b last:border-b-0">
                <button
                  type="button"
                  className="grid w-full grid-cols-[1rem_1fr_auto] items-center gap-2 bg-background/35 px-3 py-2 text-left text-xs hover:bg-muted/30"
                  aria-expanded={isExpanded}
                  onClick={() => onToggleGroup(group.key)}
                >
                  {isExpanded ? (
                    <ChevronDown
                      className="size-3 text-muted-foreground"
                      aria-hidden="true"
                    />
                  ) : (
                    <ChevronRight
                      className="size-3 text-muted-foreground"
                      aria-hidden="true"
                    />
                  )}
                  <span className="min-w-0">
                    <span className="block truncate font-medium">
                      {group.label}
                    </span>
                    <span className="block truncate text-[11px] text-muted-foreground">
                      {group.detail}
                    </span>
                  </span>
                  <span className="text-[11px] text-muted-foreground">
                    {group.assets.length}
                  </span>
                </button>

                {isExpanded ? (
                  <div className="overflow-x-auto">
                    <table className="w-full min-w-[820px] border-collapse text-left text-xs">
                      <thead className="bg-muted/25 text-[11px] text-muted-foreground uppercase">
                        <tr className="border-y">
                          <Th>asset</Th>
                          <Th>captured</Th>
                          <Th>lat</Th>
                          <Th>lon</Th>
                          <Th>camera</Th>
                          <Th>size</Th>
                        </tr>
                      </thead>
                      <tbody>
                        {group.assets.map((asset, index) => {
                          const id = assetId(asset)
                          const selected = selectedAssetId === id
                          return (
                            <tr
                              key={`${id}-${index}`}
                              className={`cursor-pointer border-b last:border-b-0 hover:bg-muted/25 ${
                                selected ? "bg-primary/10" : ""
                              }`}
                              tabIndex={0}
                              onClick={() => onSelectAsset(id)}
                              onKeyDown={(event) => {
                                if (
                                  event.key === "Enter" ||
                                  event.key === " "
                                ) {
                                  onSelectAsset(id)
                                }
                              }}
                            >
                              <Td className="font-medium">
                                {assetFilename(asset)}
                              </Td>
                              <Td>{formatDateTime(asset.captured_at)}</Td>
                              <Td>{formatCoordinate(asset.latitude)}</Td>
                              <Td>{formatCoordinate(asset.longitude)}</Td>
                              <Td>{cameraLabel(asset)}</Td>
                              <Td>{formatFileBytes(assetFileSize(asset))}</Td>
                            </tr>
                          )
                        })}
                      </tbody>
                    </table>
                  </div>
                ) : null}
              </section>
            )
          })
        )}
      </div>
    </div>
  )
}

function ImageryMap({
  assets,
  selectedAssetId,
  onSelect,
}: {
  assets: GeoImageAsset[]
  selectedAssetId: string | null
  onSelect: (assetId: string) => void
}) {
  const mapRef = useRef<import("@vis.gl/react-maplibre").MapRef | null>(null)
  const assetCoordinateKey = useMemo(
    () =>
      assets
        .map(
          (asset) =>
            `${assetId(asset)}:${asset.latitude.toFixed(7)},${asset.longitude.toFixed(7)}`
        )
        .join("|"),
    [assets]
  )
  const featureCollection = useMemo(
    () => imageryFeatureCollection(assets),
    [assets]
  )
  const initialViewState = useMemo(() => imageryInitialView(assets), [assets])
  const selectedAsset = assets.find(
    (asset) => assetId(asset) === selectedAssetId
  )
  const clusterLayer = useMemo(() => imageryClusterLayer(), [])
  const clusterCountLayer = useMemo(() => imageryClusterCountLayer(), [])
  const pointLayer = useMemo(
    () => imageryPointLayer(selectedAssetId),
    [selectedAssetId]
  )

  useEffect(() => {
    const map = mapRef.current
    if (!map || assets.length === 0) return
    const bounds = imageryLngLatBounds(assets)
    map.fitBounds(bounds, {
      duration: 0,
      maxZoom: 16,
      padding: 34,
    })
  }, [assetCoordinateKey, assets])

  const handleMapClick = useCallback(
    (event: import("@vis.gl/react-maplibre").MapLayerMouseEvent) => {
      const feature = event.features?.[0]
      if (!feature) return
      const properties = feature.properties as
        | { assetId?: string; cluster?: boolean; cluster_id?: number }
        | undefined
      if (typeof properties?.assetId === "string") {
        onSelect(properties.assetId)
        return
      }
      if (properties?.cluster) {
        const geometry = feature.geometry
        if (geometry.type === "Point") {
          const [longitude, latitude] = geometry.coordinates
          mapRef.current?.easeTo({
            center: [longitude, latitude],
            duration: 250,
            zoom: Math.min((mapRef.current?.getZoom() ?? 10) + 2, 17),
          })
        }
      }
    },
    [onSelect]
  )

  return (
    <div
      data-testid="imagery-map"
      data-asset-count={assets.length}
      data-map-provider="osm-maplibre"
      className="relative h-[24rem] overflow-hidden border bg-background/35"
    >
      <MapLibreMap
        ref={mapRef}
        initialViewState={initialViewState}
        interactiveLayerIds={["imagery-points", "imagery-clusters"]}
        mapStyle={osmMapStyle}
        onClick={handleMapClick}
        style={{ height: "100%", width: "100%" }}
      >
        <NavigationControl position="top-left" showCompass={false} />
        <Source
          id="imagery"
          type="geojson"
          data={featureCollection}
          cluster
          clusterMaxZoom={13}
          clusterRadius={36}
        >
          <Layer {...clusterLayer} />
          <Layer {...clusterCountLayer} />
          <Layer {...pointLayer} />
        </Source>
        {selectedAsset ? (
          <Popup
            longitude={selectedAsset.longitude}
            latitude={selectedAsset.latitude}
            anchor="top"
            closeButton={false}
            closeOnClick={false}
            offset={10}
          >
            <div className="grid gap-1 text-[11px]">
              <div className="font-medium">{assetFilename(selectedAsset)}</div>
              <div className="text-muted-foreground">
                {formatCoordinate(selectedAsset.latitude)},{" "}
                {formatCoordinate(selectedAsset.longitude)}
              </div>
            </div>
          </Popup>
        ) : null}
      </MapLibreMap>
      {assets.length === 0 ? (
        <div className="pointer-events-none absolute inset-0 flex items-center justify-center bg-background/70 text-xs text-muted-foreground">
          <div className="flex items-center gap-2">
            <MapPin className="size-4" aria-hidden="true" />
            no geocoded image assets
          </div>
        </div>
      ) : null}
    </div>
  )
}

function ImageryAssetDetails({ asset }: { asset: StudioImageAsset | null }) {
  return (
    <div className="min-h-0 min-w-0 overflow-y-auto border bg-card">
      <PanelHeader
        title="selected imagery"
        right={
          <div className="flex items-center gap-1">
            <ImageIcon className="size-3" aria-hidden="true" />
            <span>{asset ? shortId(assetId(asset)) : "-"}</span>
          </div>
        }
      />
      {asset ? (
        <div className="grid gap-3 p-3 text-[11px]">
          <DetailBlock
            title="identity"
            rows={[
              ["asset_id", assetId(asset)],
              ["account_id", asset.account_id ? String(asset.account_id) : "-"],
              ["group", imageAssetGroupIdentity(asset).label],
              ["filename", assetFilename(asset)],
              ["sha256", asset.sha256 ? shortKey(asset.sha256) : "-"],
            ]}
          />
          <DetailBlock
            title="capture"
            rows={[
              ["captured", formatDateTime(asset.captured_at)],
              ["latitude", formatCoordinate(asset.latitude)],
              ["longitude", formatCoordinate(asset.longitude)],
              ["drone", asset.drone_model ?? "-"],
              ["camera", cameraLabel(asset)],
            ]}
          />
          <DetailBlock
            title="file"
            rows={[
              ["dimensions", formatDimensions(asset)],
              ["size", formatFileBytes(assetFileSize(asset))],
              ["bucket", asset.bucket_name ?? "-"],
              ["object_key", asset.object_key ?? asset.cloudfront_path ?? "-"],
              ["uri", asset.uri ?? "-"],
            ]}
          />
        </div>
      ) : (
        <div className="px-3 py-6 text-xs text-muted-foreground">
          no asset selected
        </div>
      )}
    </div>
  )
}

function LiveEventPanel({
  events,
  streamState,
}: {
  events: string[]
  streamState: StreamState
}) {
  return (
    <div className="min-w-0 border bg-card">
      <PanelHeader
        title="event stream"
        right={
          <div className="flex items-center gap-1">
            {streamState === "live" ? (
              <Wifi className="size-3 text-primary" aria-hidden="true" />
            ) : (
              <WifiOff
                className="size-3 text-muted-foreground"
                aria-hidden="true"
              />
            )}
            <span>{streamStateLabel(streamState)}</span>
          </div>
        }
      />
      <div className="grid p-2 text-[11px] leading-5">
        {events.length === 0 ? (
          <div className="px-1 py-2 text-muted-foreground">
            waiting for backend stream
          </div>
        ) : (
          events.map((line, index) => (
            <div
              key={`${line}-${index}`}
              className="grid grid-cols-[0.75rem_1fr] gap-2 border-b border-border/60 py-1 last:border-b-0"
            >
              <span className="text-primary">&gt;</span>
              <span className="truncate text-muted-foreground">{line}</span>
            </div>
          ))
        )}
      </div>
    </div>
  )
}

function Metric({
  icon: Icon,
  label,
  value,
  detail,
}: {
  icon: import("react").ElementType
  label: string
  value: string
  detail: string
}) {
  return (
    <div className="grid grid-cols-[1.75rem_1fr] gap-2 border bg-card px-3 py-2">
      <div className="flex size-7 items-center justify-center rounded bg-muted text-muted-foreground">
        <Icon className="size-4" aria-hidden="true" />
      </div>
      <div className="min-w-0">
        <div className="flex items-center justify-between gap-2">
          <span className="truncate text-[11px] text-muted-foreground uppercase">
            {label}
          </span>
          <span className="truncate text-xs font-medium">{value}</span>
        </div>
        <div className="truncate text-[11px] text-muted-foreground">
          {detail}
        </div>
      </div>
    </div>
  )
}

function ResourceDial({
  label,
  value,
  detail,
}: {
  label: string
  value: number | null | undefined
  detail: string
}) {
  const percent = clampPercent(value)
  const empty = value === null || value === undefined
  const radius = 28
  const circumference = 2 * Math.PI * radius
  const activeLength = empty ? 0 : (percent / 100) * circumference
  const inactiveLength = circumference - activeLength
  const stroke = radialStroke(percent)

  return (
    <div className="grid min-h-[7.25rem] grid-cols-[4.5rem_1fr] items-center gap-2 border bg-background/35 px-2 py-2 sm:grid-cols-1 sm:justify-items-center">
      <div
        className="relative flex size-16 items-center justify-center"
        role="img"
        aria-label={`${label} ${empty ? "unavailable" : `${percent.toFixed(0)} percent`}`}
      >
        <svg
          className="absolute inset-0 size-16"
          viewBox="0 0 80 80"
          aria-hidden="true"
        >
          <circle
            cx="40"
            cy="40"
            r="34"
            fill="none"
            stroke="var(--muted)"
            strokeWidth="1"
          />
          <circle
            cx="40"
            cy="40"
            r={radius}
            fill="none"
            stroke="var(--border)"
            strokeWidth="9"
          />
          <circle
            cx="40"
            cy="40"
            r={radius}
            fill="none"
            stroke={stroke}
            strokeDasharray={`${activeLength} ${inactiveLength}`}
            strokeLinecap="round"
            strokeWidth="9"
            transform="rotate(-90 40 40)"
          />
          <circle
            cx="40"
            cy="40"
            r="18"
            fill="var(--card)"
            stroke="var(--border)"
            strokeWidth="1"
          />
        </svg>
        <div className="relative flex flex-col items-center justify-center">
          {empty ? (
            <span className="text-[11px] text-muted-foreground">-</span>
          ) : (
            <span className="text-xs font-semibold tabular-nums">
              {percent.toFixed(0)}%
            </span>
          )}
          <span className="mt-[-2px] text-[8px] text-muted-foreground uppercase">
            {label}
          </span>
        </div>
      </div>
      <div className="min-w-0 text-left sm:text-center">
        <div className="truncate text-[11px] text-muted-foreground uppercase">
          {label}
        </div>
        <div className="truncate text-[11px]">{detail}</div>
      </div>
    </div>
  )
}

function radialStroke(percent: number) {
  if (percent >= 90) return "var(--destructive)"
  if (percent >= 75) return "var(--chart-4)"
  return "var(--chart-2)"
}

function SparklineRow({
  label,
  samples,
  metric,
}: {
  label: string
  samples: TelemetrySample[]
  metric: keyof Omit<TelemetrySample, "observedAt">
}) {
  return (
    <div className="grid grid-cols-[3.5rem_1fr] items-center gap-2">
      <span className="text-muted-foreground">{label}</span>
      <Sparkline samples={samples} metric={metric} />
    </div>
  )
}

function Sparkline({
  samples,
  metric,
}: {
  samples: TelemetrySample[]
  metric: keyof Omit<TelemetrySample, "observedAt">
}) {
  const values = samples
    .map((sample) => sample[metric])
    .filter((value): value is number => value !== null && value !== undefined)
    .slice(-30)

  if (values.length === 0) {
    return (
      <div className="h-8 border bg-background/35 px-2 py-1 text-[11px] text-muted-foreground">
        no samples
      </div>
    )
  }

  const visibleValues = values.length === 1 ? [values[0], values[0]] : values
  const points = visibleValues
    .map((value, index) => {
      const x = (index / (visibleValues.length - 1)) * 100
      const y = 28 - (clampPercent(value) / 100) * 24
      return `${x.toFixed(2)},${y.toFixed(2)}`
    })
    .join(" ")

  return (
    <svg
      className="h-8 w-full border bg-background/35"
      viewBox="0 0 100 32"
      preserveAspectRatio="none"
    >
      <polyline
        points={points}
        fill="none"
        stroke="var(--primary)"
        strokeWidth="2"
        vectorEffect="non-scaling-stroke"
      />
    </svg>
  )
}

function PanelHeader({
  title,
  right,
}: {
  title: string
  right: import("react").ReactNode
}) {
  return (
    <div className="flex h-8 items-center justify-between border-b bg-muted/40 px-3 text-[11px] uppercase">
      <span className="font-medium">{title}</span>
      <span className="text-muted-foreground">{right}</span>
    </div>
  )
}

function DetailBlock({
  title,
  rows,
}: {
  title: string
  rows: Array<[string, string]>
}) {
  return (
    <div className="border bg-background/35">
      <div className="border-b bg-muted/30 px-2 py-1 text-muted-foreground uppercase">
        {title}
      </div>
      <div className="grid">
        {rows.map(([label, value]) => (
          <div
            key={label}
            className="grid grid-cols-[7rem_1fr] gap-2 border-b px-2 py-1 last:border-b-0"
          >
            <span className="text-muted-foreground">{label}</span>
            <span className="truncate">{value}</span>
          </div>
        ))}
      </div>
    </div>
  )
}

function InfoLine({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex justify-between gap-3">
      <span>{label}</span>
      <span className="truncate text-foreground">{value}</span>
    </div>
  )
}

function StatusPill({ state }: { state: string }) {
  const color =
    state === "live" || state === "enabled" || state === "completed"
      ? "bg-primary/15 text-primary"
      : state === "revoked" || state === "failed"
        ? "bg-destructive/15 text-destructive"
        : "bg-muted text-muted-foreground"

  return (
    <span className={`inline-flex h-5 items-center px-2 text-[11px] ${color}`}>
      {state}
    </span>
  )
}

function Th({
  children,
  className = "",
}: {
  children?: import("react").ReactNode
  className?: string
}) {
  return (
    <th className={`px-3 py-2 font-medium whitespace-nowrap ${className}`}>
      {children}
    </th>
  )
}

function Td({
  children,
  className = "",
}: {
  children: import("react").ReactNode
  className?: string
}) {
  return (
    <td className={`px-3 py-2 whitespace-nowrap ${className}`}>{children}</td>
  )
}

type GeoImageAsset = StudioImageAsset & {
  latitude: number
  longitude: number
}

type ImageryGroup = {
  key: string
  label: string
  detail: string
  source: string
  assets: StudioImageAsset[]
}

type ImageryFeatureCollection = {
  type: "FeatureCollection"
  features: Array<{
    type: "Feature"
    geometry: {
      type: "Point"
      coordinates: [number, number]
    }
    properties: {
      assetId: string
      filename: string
    }
  }>
}

type ImageryBounds = {
  minLat: number
  maxLat: number
  minLon: number
  maxLon: number
}

const osmMapStyle: import("maplibre-gl").StyleSpecification = {
  version: 8,
  sources: {
    osm: {
      type: "raster",
      tiles: ["https://tile.openstreetmap.org/{z}/{x}/{y}.png"],
      tileSize: 256,
      attribution:
        '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
    },
  },
  layers: [
    {
      id: "osm",
      type: "raster",
      source: "osm",
    },
  ],
}

function imageAssetWithLocation(
  asset: StudioImageAsset
): asset is GeoImageAsset {
  return isFiniteNumber(asset.latitude) && isFiniteNumber(asset.longitude)
}

function groupImageryAssets(assets: StudioImageAsset[]): ImageryGroup[] {
  const groups = new Map<
    string,
    {
      identity: ReturnType<typeof imageAssetGroupIdentity>
      assets: StudioImageAsset[]
    }
  >()

  for (const asset of assets) {
    const identity = imageAssetGroupIdentity(asset)
    const group = groups.get(identity.key)
    if (group) {
      group.assets.push(asset)
    } else {
      groups.set(identity.key, { identity, assets: [asset] })
    }
  }

  return [...groups.values()]
    .map(({ identity, assets: groupAssets }) => ({
      key: identity.key,
      label: identity.label,
      source: identity.source,
      detail: imageryGroupDetail(groupAssets, identity.source),
      assets: groupAssets.sort(compareImageAssets),
    }))
    .sort((left, right) => {
      const countDelta = right.assets.length - left.assets.length
      if (countDelta !== 0) return countDelta
      return left.label.localeCompare(right.label)
    })
}

function imageAssetGroupIdentity(asset: StudioImageAsset) {
  const groupName = scalarText(asset.group_name)
  if (groupName) {
    return {
      key: `group_name:${groupName}`,
      label: groupName,
      source: "group_name",
    }
  }

  const explicitGroup = firstPresent([
    groupCandidate("image_group", asset.image_group_id, asset.image_group_name),
    groupCandidate("group", asset.group_id, asset.group_name),
    groupCandidate(
      "imagery_source",
      asset.imagery_source_id,
      asset.imagery_source_name
    ),
    groupCandidate("source", asset.source_id, asset.source_name),
    groupCandidate(
      "source_version",
      asset.source_version_id,
      asset.source_version_name
    ),
    groupCandidate("project", asset.project_id, asset.project_name),
    groupCandidate("batch", asset.batch_id, asset.batch_name),
    nestedGroupCandidate("image_group", asset.image_group),
  ])

  if (explicitGroup) return explicitGroup

  const captureDay = captureDateKey(asset.captured_at)
  const camera = cameraLabel(asset)
  const cameraKey = camera === "-" ? "unknown camera" : camera
  const account = asset.account_id
    ? `account ${asset.account_id}`
    : "unknown account"
  const label = captureDay
    ? `${captureDay} / ${cameraKey}`
    : `${account} / ungrouped`
  return {
    key: `derived:${account}:${captureDay ?? "unknown_date"}:${cameraKey}`,
    label,
    source: "derived",
  }
}

function groupCandidate(
  prefix: string,
  rawId?: string | number | null,
  rawName?: string | null
) {
  const id = scalarText(rawId)
  const name = scalarText(rawName)
  if (!id && !name) return null
  return {
    key: `${prefix}:${id ?? name}`,
    label: name ?? `${prefix.replaceAll("_", " ")} ${id}`,
    source: prefix.replaceAll("_", " "),
  }
}

function nestedGroupCandidate(
  prefix: string,
  value?: Record<string, unknown> | null
) {
  if (!value || typeof value !== "object") return null
  const id = scalarText(value.id) ?? scalarText(value.group_id)
  const name =
    scalarText(value.name) ??
    scalarText(value.title) ??
    scalarText(value.label) ??
    scalarText(value.group_name)
  return groupCandidate(prefix, id, name)
}

function imageryGroupDetail(assets: StudioImageAsset[], source: string) {
  return compactListText([
    source,
    `${assets.length} assets`,
    `${assets.filter(imageAssetWithLocation).length} geo`,
    imageryGroupDateRange(assets),
    imageryGroupCameraSummary(assets),
    formatFileBytes(sumAssetBytes(assets)),
  ])
}

function imageryGroupDateRange(assets: StudioImageAsset[]) {
  const days = uniqueText(
    assets.map((asset) => captureDateKey(asset.captured_at))
  )
  if (days.length === 0) return null
  if (days.length === 1) return days[0]
  return `${days[0]}..${days[days.length - 1]}`
}

function imageryGroupCameraSummary(assets: StudioImageAsset[]) {
  const cameras = uniqueText(
    assets.map(cameraLabel).filter((value) => value !== "-")
  )
  if (cameras.length === 0) return null
  if (cameras.length === 1) return cameras[0]
  return `${cameras.length} cameras`
}

function sumGroupAssets(groups: ImageryGroup[]) {
  return groups.reduce((sum, group) => sum + group.assets.length, 0)
}

function sumAssetBytes(assets: StudioImageAsset[]) {
  const total = assets.reduce(
    (sum, asset) => sum + (assetFileSize(asset) ?? 0),
    0
  )
  return total > 0 ? total : null
}

function compareImageAssets(left: StudioImageAsset, right: StudioImageAsset) {
  const capturedDelta =
    timestampForSort(left.captured_at) - timestampForSort(right.captured_at)
  if (capturedDelta !== 0) return capturedDelta
  return assetFilename(left).localeCompare(assetFilename(right))
}

function timestampForSort(value?: string | null) {
  if (!value) return Number.MAX_SAFE_INTEGER
  const parsed = Date.parse(
    value.includes("T") ? value : value.replace(" ", "T")
  )
  return Number.isNaN(parsed) ? Number.MAX_SAFE_INTEGER : parsed
}

function captureDateKey(value?: string | null) {
  if (!value) return null
  const match = value.match(/^(\d{4}-\d{2}-\d{2})/)
  if (match) return match[1]
  const parsed = timestampForSort(value)
  if (parsed === Number.MAX_SAFE_INTEGER) return null
  return new Date(parsed).toISOString().slice(0, 10)
}

function firstPresent<T>(values: Array<T | null | undefined>) {
  return values.find(
    (value): value is T => value !== null && value !== undefined
  )
}

function scalarText(value: unknown) {
  if (typeof value === "string") {
    const trimmed = value.trim()
    return trimmed ? trimmed : null
  }
  if (typeof value === "number" && Number.isFinite(value)) return String(value)
  return null
}

function uniqueText(values: Array<string | null | undefined>) {
  return [
    ...new Set(values.filter((value): value is string => Boolean(value))),
  ].sort()
}

function compactListText(values: Array<string | null | undefined>) {
  return values.filter((value): value is string => Boolean(value)).join(" / ")
}

function isFiniteNumber(value: unknown): value is number {
  return typeof value === "number" && Number.isFinite(value)
}

function assetId(asset: StudioImageAsset) {
  return (
    asset.asset_id ?? asset.id ?? asset.uri ?? asset.filename ?? "unknown-asset"
  )
}

function assetFilename(asset: StudioImageAsset) {
  const filename = asset.filename?.trim()
  if (filename) return filename
  return basename(assetObjectPath(asset)) ?? shortId(assetId(asset))
}

function assetObjectPath(asset: StudioImageAsset) {
  return asset.object_key ?? asset.cloudfront_path ?? asset.uri ?? "-"
}

function assetFileSize(asset: StudioImageAsset) {
  return asset.file_size ?? asset.size_bytes ?? null
}

function cameraLabel(asset: StudioImageAsset) {
  return joinPresent([asset.camera_make, asset.camera_model])
}

function formatDimensions(asset: StudioImageAsset) {
  if (!asset.image_width || !asset.image_height) return "-"
  return `${formatInteger(asset.image_width)} x ${formatInteger(asset.image_height)}`
}

function formatCoordinate(value?: number | null) {
  if (!isFiniteNumber(value)) return "-"
  return value.toFixed(6)
}

function formatInteger(value?: number | null) {
  if (!isFiniteNumber(value)) return "-"
  return new Intl.NumberFormat(undefined, { maximumFractionDigits: 0 }).format(
    value
  )
}

function formatFileBytes(value?: number | null) {
  if (!isFiniteNumber(value)) return "-"
  if (value < 1024) return `${value.toFixed(0)} B`
  const units = ["KiB", "MiB", "GiB", "TiB"]
  let size = value / 1024
  let unitIndex = 0
  while (size >= 1024 && unitIndex < units.length - 1) {
    size /= 1024
    unitIndex += 1
  }
  return `${size.toFixed(size >= 10 ? 1 : 2)} ${units[unitIndex]}`
}

function basename(path: string) {
  const withoutQuery = path.split("?")[0]?.trim()
  if (!withoutQuery || withoutQuery === "-") return null
  const cleaned = withoutQuery.replace(/\/+$/, "")
  const name = cleaned.split("/").filter(Boolean).at(-1)
  return name && !name.includes("://") ? name : null
}

function joinPresent(values: Array<string | null | undefined>) {
  const present = values
    .map((value) => value?.trim())
    .filter((value): value is string => Boolean(value))
  return present.length > 0 ? present.join(" ") : "-"
}

function imageryMapBounds(assets: GeoImageAsset[]): ImageryBounds {
  const latitudes = assets.map((asset) => asset.latitude)
  const longitudes = assets.map((asset) => asset.longitude)
  let minLat = Math.min(...latitudes)
  let maxLat = Math.max(...latitudes)
  let minLon = Math.min(...longitudes)
  let maxLon = Math.max(...longitudes)

  if (minLat === maxLat) {
    minLat -= 0.001
    maxLat += 0.001
  }
  if (minLon === maxLon) {
    minLon -= 0.001
    maxLon += 0.001
  }

  return { minLat, maxLat, minLon, maxLon }
}

function imageryBoundsLabel(assets: GeoImageAsset[]) {
  if (assets.length === 0) return "-"
  const bounds = imageryMapBounds(assets)
  return `${formatCoordinate(bounds.minLat)}..${formatCoordinate(bounds.maxLat)} / ${formatCoordinate(
    bounds.minLon
  )}..${formatCoordinate(bounds.maxLon)}`
}

function imageryFeatureCollection(
  assets: GeoImageAsset[]
): ImageryFeatureCollection {
  return {
    type: "FeatureCollection",
    features: assets.map((asset) => ({
      type: "Feature",
      geometry: {
        type: "Point",
        coordinates: [asset.longitude, asset.latitude],
      },
      properties: {
        assetId: assetId(asset),
        filename: assetFilename(asset),
      },
    })),
  }
}

function imageryLngLatBounds(
  assets: GeoImageAsset[]
): [[number, number], [number, number]] {
  const bounds = imageryMapBounds(assets)
  return [
    [bounds.minLon, bounds.minLat],
    [bounds.maxLon, bounds.maxLat],
  ]
}

function imageryInitialView(assets: GeoImageAsset[]) {
  if (assets.length === 0) {
    return { longitude: -76.95, latitude: 18.15, zoom: 10 }
  }

  const bounds = imageryMapBounds(assets)
  return {
    longitude: (bounds.minLon + bounds.maxLon) / 2,
    latitude: (bounds.minLat + bounds.maxLat) / 2,
    zoom: imageryZoomForBounds(bounds),
  }
}

function imageryZoomForBounds(bounds: ImageryBounds) {
  const span = Math.max(
    bounds.maxLon - bounds.minLon,
    bounds.maxLat - bounds.minLat
  )
  if (span > 35) return 3
  if (span > 12) return 4
  if (span > 3) return 7
  if (span > 1) return 9
  if (span > 0.25) return 11
  return 14
}

function imageryClusterLayer(): import("maplibre-gl").CircleLayerSpecification {
  return {
    id: "imagery-clusters",
    type: "circle",
    source: "imagery",
    filter: ["has", "point_count"],
    paint: {
      "circle-color": [
        "step",
        ["get", "point_count"],
        "#38bdf8",
        20,
        "#2563eb",
        100,
        "#1d4ed8",
      ],
      "circle-opacity": 0.9,
      "circle-radius": ["step", ["get", "point_count"], 15, 20, 20, 100, 26],
      "circle-stroke-color": "#0f172a",
      "circle-stroke-width": 2,
    },
  }
}

function imageryClusterCountLayer(): import("maplibre-gl").SymbolLayerSpecification {
  return {
    id: "imagery-cluster-count",
    type: "symbol",
    source: "imagery",
    filter: ["has", "point_count"],
    layout: {
      "text-field": ["get", "point_count_abbreviated"],
      "text-font": ["Noto Sans Regular"],
      "text-size": 11,
    },
    paint: {
      "text-color": "#ffffff",
    },
  }
}

function imageryPointLayer(
  selectedAssetId: string | null
): import("maplibre-gl").CircleLayerSpecification {
  return {
    id: "imagery-points",
    type: "circle",
    source: "imagery",
    filter: ["!", ["has", "point_count"]],
    paint: {
      "circle-color": [
        "case",
        ["==", ["get", "assetId"], selectedAssetId ?? ""],
        "#2563eb",
        "#0ea5e9",
      ],
      "circle-opacity": 0.86,
      "circle-radius": [
        "case",
        ["==", ["get", "assetId"], selectedAssetId ?? ""],
        8,
        5,
      ],
      "circle-stroke-color": "#0f172a",
      "circle-stroke-width": [
        "case",
        ["==", ["get", "assetId"], selectedAssetId ?? ""],
        3,
        1.5,
      ],
    },
  }
}

function imagerySourceLabel(source: ImagerySource) {
  if (source === "idle") return "not loaded"
  if (source === "loading") return "loading"
  if (source === "api") return "studio api"
  if (source === "api-empty") return "studio empty"
  return "studio error"
}

function clientSourceLabel(source: ClientSource) {
  if (source === "loading") return "loading"
  if (source === "api") return "api"
  if (source === "api-empty") return "api empty"
  if (source === "ws") return "live ws"
  if (source === "ws-empty") return "live empty"
  return "fixture"
}

function jobEventKindLabel(event: JobEvent) {
  return formatStage(event.details?.kind ?? "lifecycle")
}

function jobEventDetailLabel(event: JobEvent) {
  const details = event.details
  if (!details) return "-"
  const parts = [
    details.phase_id ?? details.stage_id,
    details.command,
    details.status_progress != null
      ? `status ${formatPercent(details.status_progress)}`
      : null,
  ].filter(Boolean)
  return parts.length > 0 ? parts.join(" / ") : "-"
}

function apiBuildLabel(health: ApiHealth | null, error: string | null) {
  if (error) return "offline"
  if (!health) return "checking"
  const version = health.version ?? "unknown"
  const sha = health.build_sha ? ` ${health.build_sha.slice(0, 7)}` : ""
  return `v${version}${sha}`
}

function apiProtocolLabel(health: ApiHealth | null, error: string | null) {
  if (error) return "unknown"
  if (!health) return "checking"
  return health.protocol_version ?? "unknown"
}

function apiEventsCapabilityLabel(
  health: ApiHealth | null,
  error: string | null
) {
  if (error) return "unknown"
  if (!health?.capabilities) return "unknown"
  if (
    health.capabilities.admin_job_events &&
    health.capabilities.admin_websocket_job_events
  ) {
    return "http+ws"
  }
  if (health.capabilities.admin_job_events) return "http only"
  if (health.capabilities.admin_websocket_job_events) return "ws only"
  return "disabled"
}

function streamStateLabel(state: StreamState) {
  if (state === "live") return "ws live"
  if (state === "reconnecting") return "ws retry"
  if (state === "offline") return "ws offline"
  return "ws connecting"
}

function adminEventsUrl() {
  const scheme = window.location.protocol === "https:" ? "wss" : "ws"
  return `${scheme}://${window.location.host}/api/admin/events`
}

async function apiErrorMessage(response: Response, label: string) {
  const status = `${response.status} ${response.statusText}`.trim()
  try {
    const payload = (await response.json()) as {
      error?: string
      message?: string
    }
    return `${label} ${status}: ${payload.error ?? payload.message ?? "request failed"}`
  } catch {
    return `${label} ${status}`
  }
}

function addTelemetrySamples(
  current: Record<string, TelemetrySample[]>,
  clients: ApiClientRecord[]
) {
  const next = { ...current }
  for (const client of clients) {
    const telemetry = client.latest_telemetry
    if (!telemetry) continue
    const currentSamples = next[client.client_id] ?? []
    if (
      currentSamples[currentSamples.length - 1]?.observedAt ===
      telemetry.observed_at
    ) {
      continue
    }
    next[client.client_id] = [
      ...currentSamples,
      {
        observedAt: telemetry.observed_at,
        loadPercent: loadPercent(telemetry),
        memoryPercent: memoryUsedPercent(telemetry),
        diskPercent: diskUsedPercent(telemetry),
        gpuPercent: telemetry.gpu_utilization_percent ?? null,
        gpuMemoryPercent: gpuMemoryPercent(telemetry),
      },
    ].slice(-90)
  }
  return next
}

function reconcileExpanded(current: string[], records: ApiClientRecord[]) {
  const ids = new Set(records.map((record) => record.client_id))
  const kept = current.filter((id) => ids.has(id))
  if (kept.length > 0) return kept
  return records[0] ? [records[0].client_id] : []
}

function isClientLive(client: ApiClientRecord) {
  if (client.revoked_at) return false
  if (!client.last_heartbeat_at) return false
  const heartbeat = new Date(client.last_heartbeat_at).getTime()
  if (Number.isNaN(heartbeat)) return false
  return Date.now() - heartbeat < 15_000
}

function isTerminalJobState(state: string) {
  return ["completed", "failed", "cancelled"].includes(state)
}

function shortId(value: string) {
  if (value.length <= 24) return value
  return `${value.slice(0, 14)}...${value.slice(-6)}`
}

function shortKey(value: string) {
  if (value.length <= 32) return value
  return `${value.slice(0, 18)}...${value.slice(-10)}`
}

function formatDate(value: string) {
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) return "-"
  return new Intl.DateTimeFormat(undefined, {
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
  }).format(date)
}

function formatDateTime(value?: string | null) {
  if (!value) return "-"
  return formatDate(value)
}

function formatClock(value: string) {
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) return "--:--:--"
  return new Intl.DateTimeFormat(undefined, {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  }).format(date)
}

function formatBytes(value?: number | null) {
  if (value === null || value === undefined) return "-"
  const gib = value / 1024 / 1024 / 1024
  return `${gib.toFixed(1)} GiB`
}

function formatDuration(seconds?: number | null) {
  if (seconds === null || seconds === undefined) return "-"
  const hours = Math.floor(seconds / 3600)
  const minutes = Math.floor((seconds % 3600) / 60)
  return `${hours}h ${minutes}m`
}

function formatPercent(value?: number | null) {
  if (value === null || value === undefined || Number.isNaN(value)) return "-"
  return `${value.toFixed(0)}%`
}

function compactLoad(telemetry?: MachineTelemetry | null) {
  if (!telemetry) return "-"
  return compactList([
    telemetry.load_average_1m,
    telemetry.load_average_5m,
    telemetry.load_average_15m,
  ])
}

function compactList(values: Array<number | null | undefined>) {
  const present = values.filter(
    (value): value is number => value !== null && value !== undefined
  )
  if (present.length === 0) return "-"
  return present.map((value) => value.toFixed(2)).join(" / ")
}

function gpuMemoryLabel(telemetry?: MachineTelemetry | null) {
  if (!telemetry?.gpu_memory_total_bytes && !telemetry?.gpu_memory_used_bytes)
    return "-"
  return `${formatBytes(telemetry.gpu_memory_used_bytes)} / ${formatBytes(
    telemetry.gpu_memory_total_bytes
  )}`
}

function loadPercent(telemetry?: MachineTelemetry | null) {
  if (!telemetry?.load_average_1m || !telemetry.cpu_count) return null
  return clampPercent((telemetry.load_average_1m / telemetry.cpu_count) * 100)
}

function memoryUsedPercent(telemetry?: MachineTelemetry | null) {
  if (!telemetry?.total_memory_bytes) return null
  const used =
    telemetry.used_memory_bytes ??
    (telemetry.available_memory_bytes
      ? telemetry.total_memory_bytes - telemetry.available_memory_bytes
      : null)
  if (used === null) return null
  return clampPercent((used / telemetry.total_memory_bytes) * 100)
}

function diskUsedPercent(telemetry?: MachineTelemetry | null) {
  if (!telemetry?.total_disk_bytes || telemetry.free_disk_bytes === null)
    return null
  if (telemetry.free_disk_bytes === undefined) return null
  return clampPercent(
    ((telemetry.total_disk_bytes - telemetry.free_disk_bytes) /
      telemetry.total_disk_bytes) *
      100
  )
}

function gpuMemoryPercent(telemetry?: MachineTelemetry | null) {
  if (
    !telemetry?.gpu_memory_total_bytes ||
    telemetry.gpu_memory_used_bytes === null
  ) {
    return null
  }
  if (telemetry.gpu_memory_used_bytes === undefined) return null
  return clampPercent(
    (telemetry.gpu_memory_used_bytes / telemetry.gpu_memory_total_bytes) * 100
  )
}

function clampPercent(value?: number | null) {
  if (value === null || value === undefined || Number.isNaN(value)) return 0
  return Math.min(100, Math.max(0, value))
}

function averagePresent(values: Array<number | null | undefined>) {
  const present = values.filter(
    (value): value is number => value !== null && value !== undefined
  )
  if (present.length === 0) return null
  return present.reduce((sum, value) => sum + value, 0) / present.length
}

const defaultPolygonText = [
  "-77.000000,18.100000",
  "-76.900000,18.100000",
  "-76.900000,18.200000",
  "-77.000000,18.200000",
].join("\n")

function frontendAssetGroupName(asset: StudioImageAsset) {
  return asset.group_name ?? asset.image_group_name ?? null
}

function groupArtifactsByJob(artifacts: UploadedArtifact[]) {
  const grouped = new Map<string, UploadedArtifact[]>()
  for (const artifact of artifacts) {
    const group = grouped.get(artifact.job_id)
    if (group) {
      group.push(artifact)
    } else {
      grouped.set(artifact.job_id, [artifact])
    }
  }
  for (const group of grouped.values()) {
    group.sort((left, right) => left.filename.localeCompare(right.filename))
  }
  return grouped
}

function groupJobEventsByJob(events: JobEvent[]) {
  const grouped = new Map<string, JobEvent[]>()
  for (const event of events) {
    const group = grouped.get(event.job_id)
    if (group) {
      group.push(event)
    } else {
      grouped.set(event.job_id, [event])
    }
  }
  for (const group of grouped.values()) {
    group.sort(compareJobEvents)
  }
  return grouped
}

function sortJobEvents(events: JobEvent[]) {
  return [...events].sort(compareJobEvents)
}

function compareJobEvents(left: JobEvent, right: JobEvent) {
  return (
    new Date(left.observed_at).getTime() - new Date(right.observed_at).getTime()
  )
}

function jobTimelineRows(job: JobRecord): TimelineRow[] {
  const stages = job.job.pipeline?.stages ?? []
  const rows: TimelineRow[] = [
    {
      id: "accepted",
      label: "accept job",
      start: 0,
      end: 8,
      detail: "agent accepted assignment",
    },
    {
      id: "inputs",
      label: "stage inputs",
      start: 8,
      end: 35,
      detail: `${job.job.manifest?.inputs?.length ?? 0} images`,
    },
    {
      id: "prepare_realityscan",
      label: "prepare RealityScan",
      start: 35,
      end: 40,
      detail: "write scripts and command files",
    },
  ]

  const stageSpan = stages.length > 0 ? 40 / stages.length : 40
  stages.forEach((stage, index) => {
    const start = 40 + stageSpan * index
    rows.push({
      id: stage,
      label: formatStage(stage),
      start,
      end: start + stageSpan,
      detail: realityScanStageDetail(stage),
    })
  })

  rows.push(
    {
      id: "collecting_outputs",
      label: "collect outputs",
      start: 80,
      end: 88,
      detail: "discover project, ortho, and logs",
    },
    {
      id: "uploading_outputs",
      label: "upload outputs",
      start: 88,
      end: 96,
      detail: "presigned targets or local artifacts",
    },
    {
      id: "completed",
      label: "complete",
      start: 96,
      end: 100,
      detail: terminalTimelineDetail(job.state),
    }
  )

  return rows
}

function jobProgress(job: JobRecord, latestEvent: JobEvent | null) {
  if (latestEvent) return clampPercent(latestEvent.progress)
  return progressForJobState(job.state)
}

function progressForJobState(state: string) {
  switch (state) {
    case "assigned":
      return 0
    case "accepted":
      return 8
    case "resolving_inputs":
    case "downloading":
      return 18
    case "verifying":
      return 32
    case "staging":
      return 36
    case "running_realityscan":
      return 40
    case "collecting_outputs":
      return 84
    case "uploading_outputs":
      return 92
    case "completed":
      return 100
    case "failed":
    case "cancelled":
      return 100
    default:
      return 0
  }
}

function currentTimelineIndex(rows: TimelineRow[], progress: number) {
  if (progress >= 100) return rows.length - 1
  const index = rows.findIndex(
    (row) => progress >= row.start && progress < row.end
  )
  if (index !== -1) return index
  return rows.findIndex((row) => progress < row.end)
}

function timelineRowStatus(
  row: TimelineRow,
  index: number,
  currentIndex: number,
  progress: number
) {
  if (progress >= row.end || index < currentIndex) return "complete"
  if (index === currentIndex) return "current"
  return "pending"
}

function timelineRowLocalProgress(row: TimelineRow, progress: number) {
  if (progress >= row.end) return 100
  if (progress <= row.start) return 0
  return clampPercent(((progress - row.start) / (row.end - row.start)) * 100)
}

function timelineConnectorProgress(status: string, localProgress: number) {
  if (status === "complete") return 1
  if (status === "current") return localProgress / 100
  return 0
}

function timelineRowLabel(status: string) {
  if (status === "complete") return "done"
  if (status === "current") return "active"
  return "pending"
}

function timelineStatusLabel(state: string) {
  if (state === "completed") return "complete"
  if (state === "failed") return "failed"
  if (state === "cancelled") return "cancelled"
  return "running"
}

function realityScanStageDetail(stage: string) {
  switch (stage) {
    case "set_intrinsics":
      return "apply camera priors"
    case "align":
      return "features, matching, component solve"
    case "select_maximal_component":
      return "keep largest component"
    case "set_reconstruction_region_auto":
      return "derive model bounds"
    case "calculate_preview_model":
      return "preview mesh"
    case "calculate_normal_model":
      return "normal mesh"
    case "calculate_high_model":
      return "high mesh"
    case "calculate_texture":
      return "texture generation"
    case "calculate_ortho_projection":
      return "orthographic projection"
    case "export_ortho_projection":
      return "orthomosaic export"
    case "save_project":
      return "write rsproj"
    default:
      return undefined
  }
}

function terminalTimelineDetail(state: string) {
  if (state === "failed") return "job failed"
  if (state === "cancelled") return "job cancelled"
  return "terminal state"
}

function parsePolygonText(value: string): Array<[number, number]> {
  const trimmed = value.trim()
  if (!trimmed) throw new Error("polygon coordinates are empty")
  const rawPoints = trimmed.startsWith("[")
    ? (JSON.parse(trimmed) as unknown)
    : trimmed
        .split(/\n+/)
        .map((line) => line.trim())
        .filter(Boolean)
        .map((line) => line.split(/[,\s]+/).map(Number))

  if (!Array.isArray(rawPoints)) {
    throw new Error("polygon must be an array or newline lon,lat pairs")
  }
  const points = rawPoints.map((point) => {
    if (!Array.isArray(point) || point.length < 2) {
      throw new Error("each polygon point must be lon,lat")
    }
    const longitude = Number(point[0])
    const latitude = Number(point[1])
    if (!Number.isFinite(longitude) || !Number.isFinite(latitude)) {
      throw new Error("polygon coordinates must be finite numbers")
    }
    return [longitude, latitude] as [number, number]
  })
  if (points.length < 3) {
    throw new Error("polygon needs at least 3 points")
  }
  return points
}

function jobBuilderStepClass(index: number, preview: BuildJobResponse | null) {
  if (index <= 1) return "text-foreground"
  if (index === 2 && preview) return "text-foreground"
  if (index === 3 && preview) return "text-primary"
  return "text-muted-foreground"
}

function jobTemplateLabel(job: JobRecord) {
  return job.job.pipeline?.template_id ?? job.job.job_name ?? "-"
}

function jobTemplateOptions(
  builtInTemplates: JobTemplate[],
  savedTemplates: SavedJobTemplate[]
): TemplateOption[] {
  return [
    ...builtInTemplates.map((template) => ({
      ...template,
      source: "built_in" as const,
      key: templateOptionKey("built_in", template.template_id),
    })),
    ...savedTemplates.map((template) => ({
      ...template,
      source: "custom" as const,
      key: templateOptionKey("custom", template.template_id),
    })),
  ]
}

function templateOptionKey(
  source: TemplateOption["source"],
  templateId: string
) {
  return `${source}:${templateId}`
}

function templateSourceLabel(template: TemplateOption) {
  return template.source === "custom" ? "saved custom" : "built in"
}

function jobTemplatePayload(template: TemplateOption): JobTemplate {
  return {
    template_id: template.template_id,
    name: template.name,
    description: template.description,
    stages: template.stages,
    project_filename: template.project_filename,
    orthomosaic_filename: template.orthomosaic_filename ?? null,
    ortho_pixel_size_meters: template.ortho_pixel_size_meters ?? null,
  }
}

function loadSavedJobTemplates(): SavedJobTemplate[] {
  try {
    const raw = window.localStorage.getItem(savedJobTemplatesStorageKey)
    if (!raw) return []
    const parsed: unknown = JSON.parse(raw)
    if (!Array.isArray(parsed)) return []
    return parsed.filter(isSavedJobTemplate)
  } catch {
    return []
  }
}

function saveSavedJobTemplates(templates: SavedJobTemplate[]) {
  try {
    window.localStorage.setItem(
      savedJobTemplatesStorageKey,
      JSON.stringify(templates)
    )
  } catch {
    // localStorage can fail in private windows; keep the in-memory state.
  }
}

function isSavedJobTemplate(value: unknown): value is SavedJobTemplate {
  if (!value || typeof value !== "object") return false
  const record = value as Record<string, unknown>
  return (
    typeof record.template_id === "string" &&
    typeof record.name === "string" &&
    typeof record.description === "string" &&
    Array.isArray(record.stages) &&
    record.stages.every((stage) => typeof stage === "string") &&
    typeof record.project_filename === "string" &&
    (record.orthomosaic_filename === null ||
      record.orthomosaic_filename === undefined ||
      typeof record.orthomosaic_filename === "string") &&
    (record.ortho_pixel_size_meters === null ||
      record.ortho_pixel_size_meters === undefined ||
      typeof record.ortho_pixel_size_meters === "number") &&
    typeof record.saved_at === "string"
  )
}

function orderedUniqueStages(stages: string[]) {
  return [...new Set(stages.filter(Boolean))].sort((left, right) => {
    const leftIndex = stageSortIndex(left)
    const rightIndex = stageSortIndex(right)
    if (leftIndex !== rightIndex) return leftIndex - rightIndex
    return left.localeCompare(right)
  })
}

function uniqueStages(stages: string[]) {
  return [...new Set(stages.filter(Boolean))]
}

function stageSortIndex(stage: string) {
  const index = realityScanStageOrder.indexOf(stage)
  return index === -1 ? Number.MAX_SAFE_INTEGER : index
}

function slugify(value: string) {
  const slug = value
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "")
  return slug || "template"
}

function formatStage(stage: string) {
  return stage.replaceAll("_", " ")
}

function assetFilenameFromInput(input: {
  filename?: string | null
  asset_id: string
}) {
  const filename = input.filename?.trim()
  return filename || shortId(input.asset_id)
}

export default App
