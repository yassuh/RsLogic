CREATE TABLE IF NOT EXISTS client_runtime_status (
  client_id TEXT PRIMARY KEY REFERENCES clients(client_id) ON DELETE CASCADE,
  heartbeat_at TIMESTAMPTZ,
  telemetry_payload JSONB,
  status_payload JSONB,
  updated_at TIMESTAMPTZ NOT NULL
);
