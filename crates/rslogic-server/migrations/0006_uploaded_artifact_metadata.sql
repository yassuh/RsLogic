ALTER TABLE uploaded_artifacts
  ADD COLUMN IF NOT EXISTS storage_uri TEXT;

ALTER TABLE uploaded_artifacts
  ADD COLUMN IF NOT EXISTS content_type TEXT;
