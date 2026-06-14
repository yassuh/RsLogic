CREATE TABLE IF NOT EXISTS pipeline_jobs (
  job_id TEXT PRIMARY KEY,
  client_id TEXT NOT NULL REFERENCES clients(client_id) ON DELETE CASCADE,
  job_payload JSONB NOT NULL,
  state TEXT NOT NULL,
  assigned_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL,
  completed_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_pipeline_jobs_client_updated
  ON pipeline_jobs (client_id, updated_at DESC);

CREATE TABLE IF NOT EXISTS job_events (
  event_id TEXT PRIMARY KEY,
  job_id TEXT NOT NULL,
  client_id TEXT NOT NULL REFERENCES clients(client_id) ON DELETE CASCADE,
  event_payload JSONB NOT NULL,
  state TEXT NOT NULL,
  observed_at TIMESTAMPTZ NOT NULL,
  received_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_job_events_job_observed
  ON job_events (job_id, observed_at ASC);

CREATE TABLE IF NOT EXISTS uploaded_artifacts (
  artifact_id TEXT PRIMARY KEY,
  job_id TEXT NOT NULL,
  client_id TEXT NOT NULL REFERENCES clients(client_id) ON DELETE CASCADE,
  filename TEXT NOT NULL,
  sha256 TEXT,
  size_bytes BIGINT,
  artifact_payload JSONB NOT NULL,
  uploaded_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_uploaded_artifacts_job
  ON uploaded_artifacts (job_id, uploaded_at ASC);
