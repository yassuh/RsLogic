CREATE TABLE IF NOT EXISTS client_enrollment_requests (
  request_id TEXT PRIMARY KEY,
  status TEXT NOT NULL CHECK (status IN ('pending', 'approved', 'rejected')),
  client_id TEXT,
  request_payload JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL,
  decided_at TIMESTAMPTZ,
  rejection_reason TEXT
);

CREATE INDEX IF NOT EXISTS idx_client_enrollment_requests_status_created
  ON client_enrollment_requests (status, created_at);

CREATE TABLE IF NOT EXISTS clients (
  client_id TEXT PRIMARY KEY,
  public_key TEXT NOT NULL,
  desired_state JSONB NOT NULL,
  approved_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS client_challenges (
  challenge_id TEXT PRIMARY KEY,
  client_id TEXT NOT NULL REFERENCES clients(client_id) ON DELETE CASCADE,
  nonce TEXT NOT NULL,
  expires_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_client_challenges_client_expires
  ON client_challenges (client_id, expires_at);

CREATE TABLE IF NOT EXISTS client_sessions (
  token TEXT PRIMARY KEY,
  client_id TEXT NOT NULL REFERENCES clients(client_id) ON DELETE CASCADE,
  expires_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_client_sessions_client_expires
  ON client_sessions (client_id, expires_at);
