ALTER TABLE clients
  ADD COLUMN IF NOT EXISTS revoked_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_clients_revoked_at
  ON clients (revoked_at);
