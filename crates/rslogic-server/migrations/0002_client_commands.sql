CREATE TABLE IF NOT EXISTS client_commands (
  command_id TEXT PRIMARY KEY,
  client_id TEXT NOT NULL REFERENCES clients(client_id) ON DELETE CASCADE,
  command_payload JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL,
  delivered_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_client_commands_pending
  ON client_commands (client_id, created_at)
  WHERE delivered_at IS NULL;
