ALTER TABLE client_runtime_status
  ADD COLUMN IF NOT EXISTS worker_status_payload JSONB;
