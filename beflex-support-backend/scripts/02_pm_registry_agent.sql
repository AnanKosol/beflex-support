-- PM Center / PM Agent schema (Agent-Controller)
-- ใช้สำหรับ pre-create schema ใน Extension DB (PostgreSQL)

CREATE TABLE IF NOT EXISTS allops_raku_pm_customers (
  id BIGSERIAL PRIMARY KEY,
  code TEXT NOT NULL UNIQUE,
  name TEXT NOT NULL,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS allops_raku_pm_environments (
  id BIGSERIAL PRIMARY KEY,
  customer_id BIGINT NOT NULL UNIQUE REFERENCES allops_raku_pm_customers(id) ON DELETE CASCADE,
  name TEXT NOT NULL,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS allops_raku_pm_servers (
  id BIGSERIAL PRIMARY KEY,
  environment_id BIGINT NOT NULL REFERENCES allops_raku_pm_environments(id) ON DELETE CASCADE,
  server_key TEXT NOT NULL UNIQUE,
  name TEXT NOT NULL,
  host TEXT NOT NULL,
  site_code TEXT,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS allops_raku_pm_applications (
  id BIGSERIAL PRIMARY KEY,
  server_id BIGINT NOT NULL REFERENCES allops_raku_pm_servers(id) ON DELETE CASCADE,
  app_type TEXT NOT NULL,
  app_name TEXT NOT NULL,
  service_name TEXT,
  collector_profile JSONB NOT NULL DEFAULT '{}'::jsonb,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  enabled BOOLEAN NOT NULL DEFAULT true,
  created_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL,
  UNIQUE (server_id, app_type, app_name)
);

CREATE TABLE IF NOT EXISTS allops_raku_pm_agents (
  id BIGSERIAL PRIMARY KEY,
  agent_key TEXT NOT NULL UNIQUE,
  server_id BIGINT REFERENCES allops_raku_pm_servers(id) ON DELETE SET NULL,
  site_code TEXT,
  capabilities JSONB NOT NULL DEFAULT '{}'::jsonb,
  status TEXT NOT NULL DEFAULT 'ONLINE',
  last_seen_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS allops_raku_pm_snapshots (
  id BIGSERIAL PRIMARY KEY,
  customer_id BIGINT NOT NULL REFERENCES allops_raku_pm_customers(id) ON DELETE CASCADE,
  environment_id BIGINT NOT NULL REFERENCES allops_raku_pm_environments(id) ON DELETE CASCADE,
  server_id BIGINT NOT NULL REFERENCES allops_raku_pm_servers(id) ON DELETE CASCADE,
  application_id BIGINT REFERENCES allops_raku_pm_applications(id) ON DELETE SET NULL,
  collected_at TIMESTAMPTZ NOT NULL,
  trigger_type TEXT NOT NULL,
  source_agent TEXT NOT NULL,
  snapshot_json JSONB NOT NULL,
  report_txt TEXT,
  hash_sha256 TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS allops_raku_pm_jobs (
  id BIGSERIAL PRIMARY KEY,
  server_id BIGINT NOT NULL REFERENCES allops_raku_pm_servers(id) ON DELETE CASCADE,
  application_id BIGINT REFERENCES allops_raku_pm_applications(id) ON DELETE SET NULL,
  trigger_type TEXT NOT NULL,
  requested_by TEXT,
  requested_at TIMESTAMPTZ NOT NULL,
  assigned_agent_key TEXT,
  started_at TIMESTAMPTZ,
  finished_at TIMESTAMPTZ,
  status TEXT NOT NULL,
  snapshot_id BIGINT REFERENCES allops_raku_pm_snapshots(id) ON DELETE SET NULL,
  error_detail TEXT,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_allops_raku_pm_servers_env ON allops_raku_pm_servers(environment_id);
CREATE INDEX IF NOT EXISTS idx_allops_raku_pm_applications_server ON allops_raku_pm_applications(server_id);
CREATE INDEX IF NOT EXISTS idx_allops_raku_pm_agents_server ON allops_raku_pm_agents(server_id);
CREATE INDEX IF NOT EXISTS idx_allops_raku_pm_agents_seen ON allops_raku_pm_agents(last_seen_at DESC);
CREATE INDEX IF NOT EXISTS idx_allops_raku_pm_jobs_status_time ON allops_raku_pm_jobs(status, requested_at ASC);
CREATE INDEX IF NOT EXISTS idx_allops_raku_pm_jobs_server ON allops_raku_pm_jobs(server_id);
CREATE INDEX IF NOT EXISTS idx_allops_raku_pm_snapshots_server_time ON allops_raku_pm_snapshots(server_id, collected_at DESC);
CREATE INDEX IF NOT EXISTS idx_allops_raku_pm_snapshots_json ON allops_raku_pm_snapshots USING GIN (snapshot_json);
