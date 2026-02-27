const path = require('path');
const fs = require('fs');
const fsp = require('fs/promises');
const express = require('express');
const multer = require('multer');
const axios = require('axios');
const FormData = require('form-data');
const jwt = require('jsonwebtoken');
const dotenv = require('dotenv');
const { Pool } = require('pg');
const XLSX = require('xlsx');
const cron = require('node-cron');
const { createAlfrescoAuthProvider } = require('./services/alfresco-auth-provider');
const { runPmReport } = require('./scripts/pm-report');

dotenv.config();

const app = express();
app.use(express.json());

const PORT = Number(process.env.PORT || 3000);
const JWT_SECRET = process.env.ALLOPS_JWT_SECRET || 'change-this-in-production';
const JWT_EXPIRES_IN = process.env.ALLOPS_JWT_EXPIRES_IN || '8h';
const ALFRESCO_BASE_URL = process.env.ALFRESCO_BASE_URL || 'http://alfresco:8080';
const REQUIRED_GROUP = process.env.ALLOPS_REQUIRED_GROUP || 'GROUP_SUPPORT_WORKSPCE';
const PERMISSION_SERVICE_URL = process.env.PERMISSION_SERVICE_URL || 'http://permission-service/api/Excel/PostFile';
const PERMISSION_TIMEOUT_MS = Number(process.env.PERMISSION_TIMEOUT_MS || 30000);
const ALFRESCO_TIMEOUT_MS = Number(process.env.ALFRESCO_TIMEOUT_MS || 10000);
const UPLOAD_DIR = process.env.ALLOPS_UPLOAD_DIR || '/app/uploads';
const PGHOST = process.env.PGHOST;
const PGPORT = Number(process.env.PGPORT || 5432);
const PGDATABASE = process.env.PGDATABASE;
const PGUSER = process.env.PGUSER;
const PGPASSWORD = process.env.PGPASSWORD;
const PGSSL = (process.env.PGSSL || 'false').toLowerCase() === 'true';
const CREDENTIAL_MANAGER_URL = process.env.CREDENTIAL_MANAGER_URL || 'http://credential-manager-backend:3900';
const CREDENTIAL_MANAGER_TOKEN = process.env.CREDENTIAL_MANAGER_TOKEN || process.env.CREDENTIAL_MANAGER_AUTH_TOKEN || '';
const CREDENTIAL_SERVICE_NAME = 'alfresco';
const CREDENTIAL_USERNAME_ID = '1';
const CREDENTIAL_PASSWORD_ID = '2';
const PM_FIXED_CONTENT_PATH = '/mnt/alfresco/contentstore';
const PM_FIXED_POSTGRES_PATH = '/mnt/alfresco/postgresql-data';
const PM_FIXED_SOLR_PATH = '/mnt/alfresco';
const PM_FIXED_OUTPUT_DIR = '/app/pm';
const PM_FIXED_BACKUP_DIR = '/app/pm/backup';
const PM_FIXED_ENV_WORKSPACE = '/app/alfresco/.env';
const PM_FIXED_ENV_POSTGRESQL = '/app/postgresql/.env';
const PM_FIXED_WORKSPACE_SOURCE_DIR = '/app/source/beflex-workspace';
const PM_FIXED_POSTGRES_SOURCE_DIR = '/app/source/beflex-db';
const GROUP_IMPORT_REQUIRED_GROUP = process.env.ALLOPS_GROUP_IMPORT_REQUIRED_GROUP || 'GROUP_ALFRESCO_ADMINISTRATORS';

const IMPORTS_TABLE = 'allops_raku_imports';
const TASK_LOGS_TABLE = 'allops_raku_task_logs';
const AUDIT_EVENTS_TABLE = 'allops_raku_audit_events';
const SERVICE_PERMISSION_IMPORT = 'permission-import';
const SERVICE_GROUP_MEMBER_IMPORT = 'group-member-import';
const SERVICE_USER_CSV_IMPORT = 'user-csv-import';
const SERVICE_PM = 'pm-service';
const PM_CONFIG_TABLE = 'allops_raku_pm_config';
const PM_RUNS_TABLE = 'allops_raku_pm_runs';
const PM_SCRIPT_TIMEOUT_MS = Number(process.env.PM_SCRIPT_TIMEOUT_MS || 30 * 60 * 1000);
const PM_TIMEZONE = process.env.PM_TIMEZONE || 'Asia/Bangkok';

const upload = multer({
  storage: multer.memoryStorage(),
  limits: {
    fileSize: Number(process.env.ALLOPS_MAX_FILE_SIZE || 25 * 1024 * 1024)
  }
});

let pool;
const processingTasks = new Set();
let pmRunInProgress = false;
let pmCronTask = null;

function getNowIso() {
  return new Date().toISOString();
}

function toSafeFilename(name) {
  return (name || 'upload.xlsx').replace(/[^a-zA-Z0-9._-]/g, '_');
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function normalizeGroupId(rawGroupId) {
  const value = String(rawGroupId || '').trim();
  if (!value) {
    return '';
  }
  return value.toUpperCase().startsWith('GROUP_') ? value : `GROUP_${value}`;
}

const alfrescoAuthProvider = createAlfrescoAuthProvider({
  alfrescoBaseUrl: ALFRESCO_BASE_URL,
  alfrescoTimeoutMs: ALFRESCO_TIMEOUT_MS,
  credentialManagerUrl: CREDENTIAL_MANAGER_URL,
  credentialManagerToken: CREDENTIAL_MANAGER_TOKEN,
  credentialServiceName: CREDENTIAL_SERVICE_NAME,
  credentialUsernameId: CREDENTIAL_USERNAME_ID,
  credentialPasswordId: CREDENTIAL_PASSWORD_ID
});

async function addTaskLog(taskId, level, message) {
  await pool.query(
    `INSERT INTO ${TASK_LOGS_TABLE} (import_id, level, message, created_at) VALUES ($1, $2, $3, $4)`,
    [taskId, level, message, getNowIso()]
  );
}

async function updateImportStatus(taskId, status) {
  await pool.query(`UPDATE ${IMPORTS_TABLE} SET status = $1, updated_at = $2 WHERE id = $3`, [status, getNowIso(), taskId]);
}

async function addAuditEvent({
  serviceName,
  username,
  actionType,
  filename,
  status,
  message,
  entityType,
  entityId,
  metadata
}) {
  await pool.query(
    `
    INSERT INTO ${AUDIT_EVENTS_TABLE}
      (event_time, service_name, username, action_type, entity_type, entity_id, filename, status, message, metadata, created_at)
    VALUES
      ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb, $11)
    `,
    [
      getNowIso(),
      serviceName,
      username || null,
      actionType,
      entityType || null,
      entityId || null,
      filename || null,
      status,
      message || null,
      JSON.stringify(metadata || {}),
      getNowIso()
    ]
  );
}

async function safeAddAuditEvent(payload) {
  try {
    await addAuditEvent(payload);
  } catch (error) {
    console.error('Cannot write audit event', error?.message || error);
  }
}

function getDefaultPmConfig() {
  return {
    customer: process.env.PM_CUSTOMER || 'aegis',
    environment: process.env.PM_ENVIRONMENT || 'Prod',
    outputDir: PM_FIXED_OUTPUT_DIR,
    contentPath: PM_FIXED_CONTENT_PATH,
    postgresPath: PM_FIXED_POSTGRES_PATH,
    solrPath: PM_FIXED_SOLR_PATH,
    envWorkspace: PM_FIXED_ENV_WORKSPACE,
    envPostgresql: PM_FIXED_ENV_POSTGRESQL,
    workspaceSourceDir: PM_FIXED_WORKSPACE_SOURCE_DIR,
    postgresSourceDir: PM_FIXED_POSTGRES_SOURCE_DIR,
    backupDir: PM_FIXED_BACKUP_DIR,
    betaEnabled: true,
    cronEnabled: false,
    cronExpression: '0 2 * * *',
    retentionDays: Number(process.env.PM_RETENTION_DAYS || 30)
  };
}

function sanitizePmConfig(rawConfig = {}, baseConfig = getDefaultPmConfig()) {
  const value = { ...baseConfig, ...(rawConfig || {}) };
  const asString = (input, fallback) => {
    const text = String(input ?? fallback ?? '').trim();
    return text || String(fallback || '').trim();
  };

  const retention = Number(value.retentionDays);
  const betaEnabled = typeof value.betaEnabled === 'boolean' ? value.betaEnabled : Boolean(baseConfig.betaEnabled);

  return {
    customer: asString(value.customer, baseConfig.customer),
    environment: asString(value.environment, baseConfig.environment),
    outputDir: PM_FIXED_OUTPUT_DIR,
    contentPath: PM_FIXED_CONTENT_PATH,
    postgresPath: PM_FIXED_POSTGRES_PATH,
    solrPath: PM_FIXED_SOLR_PATH,
    envWorkspace: PM_FIXED_ENV_WORKSPACE,
    envPostgresql: PM_FIXED_ENV_POSTGRESQL,
    workspaceSourceDir: PM_FIXED_WORKSPACE_SOURCE_DIR,
    postgresSourceDir: PM_FIXED_POSTGRES_SOURCE_DIR,
    backupDir: PM_FIXED_BACKUP_DIR,
    betaEnabled,
    cronEnabled: betaEnabled ? Boolean(value.cronEnabled) : false,
    cronExpression: asString(value.cronExpression, baseConfig.cronExpression),
    retentionDays: Number.isFinite(retention) ? Math.max(1, Math.min(3650, Math.floor(retention))) : baseConfig.retentionDays
  };
}

async function listPmFiles(folderPath) {
  try {
    const names = await fsp.readdir(folderPath);
    const files = [];
    for (const name of names) {
      if (!/^pm_.*\.(txt|zip)$/i.test(name)) {
        continue;
      }

      const fullPath = path.join(folderPath, name);
      const stat = await fsp.stat(fullPath).catch(() => null);
      if (stat?.isFile()) {
        files.push({
          name,
          path: fullPath,
          mtimeMs: stat.mtimeMs
        });
      }
    }
    return files;
  } catch (error) {
    return [];
  }
}

async function runPmRetention(config) {
  if (!config.betaEnabled) {
    return {
      deletedFiles: 0,
      deletedRuns: 0
    };
  }

  const cutoff = Date.now() - (Number(config.retentionDays || 30) * 24 * 60 * 60 * 1000);
  let deletedFiles = 0;

  for (const target of [config.outputDir, config.backupDir]) {
    const files = await listPmFiles(target);
    for (const file of files) {
      if (file.mtimeMs < cutoff) {
        await fsp.unlink(file.path).catch(() => {});
        deletedFiles += 1;
      }
    }
  }

  const deletedRuns = await pool.query(
    `DELETE FROM ${PM_RUNS_TABLE} WHERE started_at < NOW() - ($1::text || ' days')::interval`,
    [String(config.retentionDays || 30)]
  );

  return {
    deletedFiles,
    deletedRuns: Number(deletedRuns.rowCount || 0)
  };
}

async function getPmConfig() {
  const row = await pool.query(`SELECT config, updated_at, updated_by, last_run_at FROM ${PM_CONFIG_TABLE} WHERE id = 1`);
  const config = sanitizePmConfig(row.rows[0]?.config || getDefaultPmConfig(), getDefaultPmConfig());
  return {
    config,
    updatedAt: row.rows[0]?.updated_at || null,
    updatedBy: row.rows[0]?.updated_by || null,
    lastRunAt: row.rows[0]?.last_run_at || null
  };
}

async function savePmConfig(config, username) {
  const merged = sanitizePmConfig(config, (await getPmConfig()).config);
  await pool.query(
    `
    INSERT INTO ${PM_CONFIG_TABLE} (id, config, updated_at, updated_by)
    VALUES (1, $1::jsonb, $2, $3)
    ON CONFLICT (id)
    DO UPDATE SET config = EXCLUDED.config, updated_at = EXCLUDED.updated_at, updated_by = EXCLUDED.updated_by
    `,
    [JSON.stringify(merged), getNowIso(), username || null]
  );
  return merged;
}

async function updatePmLastRunAt() {
  await pool.query(`UPDATE ${PM_CONFIG_TABLE} SET last_run_at = $1 WHERE id = 1`, [getNowIso()]);
}

async function refreshPmCronSchedule() {
  if (pmCronTask) {
    pmCronTask.stop();
    pmCronTask.destroy();
    pmCronTask = null;
  }

  const { config } = await getPmConfig();
  if (!config.betaEnabled || !config.cronEnabled) {
    return;
  }

  if (!cron.validate(config.cronExpression)) {
    console.warn(`Invalid PM cron expression: ${config.cronExpression}`);
    return;
  }

  pmCronTask = cron.schedule(
    config.cronExpression,
    () => {
      queuePmRun('CRON', 'system').catch((error) => {
        console.error('PM cron queue failed', error?.message || error);
      });
    },
    { timezone: PM_TIMEZONE }
  );
}

async function executePmRun(runId, triggerType, requestedBy) {
  const startedAt = Date.now();
  try {
    const { config } = await getPmConfig();
    if (!config.betaEnabled) {
      throw new Error('PM BETA is OFF');
    }

    await fsp.mkdir(config.outputDir, { recursive: true });
    await fsp.mkdir(config.backupDir, { recursive: true });

    const result = await runPmReport(config, {
      timeoutMs: PM_SCRIPT_TIMEOUT_MS,
      ipIncludeRegex: process.env.PM_IP_INCLUDE_REGEX
    });

    const retentionResult = await runPmRetention(config);
    const outputFile = result.outputFile || null;
    const message = `PM report completed in ${Math.max(1, Math.round((Date.now() - startedAt) / 1000))}s`;

    await pool.query(
      `
      UPDATE ${PM_RUNS_TABLE}
      SET status = $1, finished_at = $2, output_file = $3, message = $4, stdout_tail = $5, stderr_tail = $6, deleted_files = $7, deleted_rows = $8
      WHERE id = $9
      `,
      [
        'SUCCESS',
        getNowIso(),
        outputFile,
        message,
        String(result.stdout || '').slice(-2000),
        String(result.stderr || '').slice(-2000),
        retentionResult.deletedFiles,
        retentionResult.deletedRuns,
        runId
      ]
    );

    await updatePmLastRunAt();
    await safeAddAuditEvent({
      serviceName: SERVICE_PM,
      username: requestedBy,
      actionType: `RUN_PM_${triggerType}`,
      filename: outputFile,
      status: 'SUCCESS',
      message,
      entityType: 'pm_run',
      entityId: String(runId),
      metadata: {
        output_file: outputFile,
        retention: retentionResult
      }
    });
  } catch (error) {
    const detail = error?.stderr || error?.stdout || error?.message || 'Unknown PM run error';
    await pool.query(
      `
      UPDATE ${PM_RUNS_TABLE}
      SET status = $1, finished_at = $2, message = $3, stderr_tail = $4
      WHERE id = $5
      `,
      ['FAILED', getNowIso(), String(error?.message || 'PM run failed'), String(detail).slice(-2000), runId]
    );

    await safeAddAuditEvent({
      serviceName: SERVICE_PM,
      username: requestedBy,
      actionType: `RUN_PM_${triggerType}`,
      status: 'FAILED',
      message: String(error?.message || 'PM run failed'),
      entityType: 'pm_run',
      entityId: String(runId),
      metadata: {
        detail: String(detail).slice(-1000)
      }
    });
  } finally {
    pmRunInProgress = false;
  }
}

async function queuePmRun(triggerType, requestedBy) {
  const { config } = await getPmConfig();
  if (!config.betaEnabled) {
    throw new Error('PM BETA is OFF');
  }

  if (pmRunInProgress) {
    throw new Error('PM job is already running');
  }

  pmRunInProgress = true;
  const startedAt = getNowIso();
  const inserted = await pool.query(
    `
    INSERT INTO ${PM_RUNS_TABLE} (started_at, trigger_type, requested_by, status, message)
    VALUES ($1, $2, $3, $4, $5)
    RETURNING id
    `,
    [startedAt, triggerType, requestedBy || null, 'IN_PROGRESS', 'PM run is in progress']
  );
  const runId = inserted.rows[0].id;

  setImmediate(() => {
    executePmRun(runId, triggerType, requestedBy || 'system').catch((error) => {
      console.error('Unexpected PM run error', error);
      pmRunInProgress = false;
    });
  });

  return runId;
}

async function initDb() {
  await fsp.mkdir(UPLOAD_DIR, { recursive: true });

  if (!PGHOST || !PGDATABASE || !PGUSER || !PGPASSWORD) {
    throw new Error('Missing PostgreSQL configuration (PGHOST, PGDATABASE, PGUSER, PGPASSWORD)');
  }

  pool = new Pool({
    host: PGHOST,
    port: PGPORT,
    database: PGDATABASE,
    user: PGUSER,
    password: PGPASSWORD,
    ssl: PGSSL ? { rejectUnauthorized: false } : false,
    max: Number(process.env.PGPOOL_MAX || 10)
  });

  await pool.query('SELECT 1');

  await pool.query(`
    CREATE TABLE IF NOT EXISTS ${IMPORTS_TABLE} (
      id BIGSERIAL PRIMARY KEY,
      service_name TEXT NOT NULL DEFAULT '${SERVICE_PERMISSION_IMPORT}',
      timestamp TIMESTAMPTZ NOT NULL,
      username TEXT NOT NULL,
      action_type TEXT NOT NULL,
      filename TEXT NOT NULL,
      stored_filename TEXT,
      status TEXT NOT NULL,
      external_task_id TEXT,
      error_message TEXT,
      created_at TIMESTAMPTZ NOT NULL,
      updated_at TIMESTAMPTZ NOT NULL
    )
  `);

  await pool.query(`ALTER TABLE ${IMPORTS_TABLE} ADD COLUMN IF NOT EXISTS service_name TEXT`);
  await pool.query(`UPDATE ${IMPORTS_TABLE} SET service_name = $1 WHERE service_name IS NULL`, [SERVICE_PERMISSION_IMPORT]);
  await pool.query(`ALTER TABLE ${IMPORTS_TABLE} ALTER COLUMN service_name SET NOT NULL`);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS ${TASK_LOGS_TABLE} (
      id BIGSERIAL PRIMARY KEY,
      import_id BIGINT NOT NULL REFERENCES ${IMPORTS_TABLE}(id) ON DELETE CASCADE,
      level TEXT NOT NULL,
      message TEXT NOT NULL,
      created_at TIMESTAMPTZ NOT NULL
    )
  `);

  await pool.query(`CREATE INDEX IF NOT EXISTS idx_${TASK_LOGS_TABLE}_import_id ON ${TASK_LOGS_TABLE}(import_id)`);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_${IMPORTS_TABLE}_created_at ON ${IMPORTS_TABLE}(created_at DESC)`);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS ${AUDIT_EVENTS_TABLE} (
      id BIGSERIAL PRIMARY KEY,
      event_time TIMESTAMPTZ NOT NULL,
      service_name TEXT NOT NULL,
      username TEXT,
      action_type TEXT NOT NULL,
      entity_type TEXT,
      entity_id TEXT,
      filename TEXT,
      status TEXT NOT NULL,
      message TEXT,
      metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
      created_at TIMESTAMPTZ NOT NULL
    )
  `);

  await pool.query(`CREATE INDEX IF NOT EXISTS idx_${AUDIT_EVENTS_TABLE}_service_time ON ${AUDIT_EVENTS_TABLE}(service_name, event_time DESC)`);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_${AUDIT_EVENTS_TABLE}_username_time ON ${AUDIT_EVENTS_TABLE}(username, event_time DESC)`);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_${AUDIT_EVENTS_TABLE}_status_time ON ${AUDIT_EVENTS_TABLE}(status, event_time DESC)`);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS ${PM_CONFIG_TABLE} (
      id INTEGER PRIMARY KEY,
      config JSONB NOT NULL,
      updated_at TIMESTAMPTZ NOT NULL,
      updated_by TEXT,
      last_run_at TIMESTAMPTZ
    )
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS ${PM_RUNS_TABLE} (
      id BIGSERIAL PRIMARY KEY,
      started_at TIMESTAMPTZ NOT NULL,
      finished_at TIMESTAMPTZ,
      trigger_type TEXT NOT NULL,
      requested_by TEXT,
      status TEXT NOT NULL,
      output_file TEXT,
      message TEXT,
      stdout_tail TEXT,
      stderr_tail TEXT,
      deleted_files INTEGER NOT NULL DEFAULT 0,
      deleted_rows INTEGER NOT NULL DEFAULT 0
    )
  `);

  await pool.query(`CREATE INDEX IF NOT EXISTS idx_${PM_RUNS_TABLE}_started_at ON ${PM_RUNS_TABLE}(started_at DESC)`);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_${PM_RUNS_TABLE}_status ON ${PM_RUNS_TABLE}(status)`);

  await pool.query(
    `
    INSERT INTO ${PM_CONFIG_TABLE} (id, config, updated_at, updated_by)
    VALUES (1, $1::jsonb, $2, $3)
    ON CONFLICT (id) DO NOTHING
    `,
    [JSON.stringify(getDefaultPmConfig()), getNowIso(), 'system']
  );
}

function createAuthHeaderWithTicket(ticket) {
  const basic = Buffer.from(`ROLE_TICKET:${ticket}`).toString('base64');
  return `Basic ${basic}`;
}

async function loginToAlfresco(username, password) {
  const url = `${ALFRESCO_BASE_URL}/alfresco/api/-default-/public/authentication/versions/1/tickets`;
  const response = await axios.post(
    url,
    {
      userId: username,
      password
    },
    {
      timeout: ALFRESCO_TIMEOUT_MS
    }
  );

  const ticket = response?.data?.entry?.id;
  if (!ticket) {
    throw new Error('Unable to retrieve Alfresco ticket');
  }
  return ticket;
}

async function checkUserGroup(username, ticket) {
  const url = `${ALFRESCO_BASE_URL}/alfresco/api/-default-/public/alfresco/versions/1/people/${encodeURIComponent(username)}/groups?maxItems=1000`;
  const response = await axios.get(url, {
    timeout: ALFRESCO_TIMEOUT_MS,
    headers: {
      Authorization: createAuthHeaderWithTicket(ticket)
    }
  });

  const entries = response?.data?.list?.entries || [];
  return entries.some((item) => item?.entry?.id === REQUIRED_GROUP);
}

function formatAlfrescoError(error, fallbackMessage = 'Unknown Alfresco API error') {
  const status = error?.response?.status;
  const brief = error?.response?.data?.error?.briefSummary;
  const key = error?.response?.data?.error?.errorKey;
  const detail = brief || key || error?.message || fallbackMessage;
  return status ? `HTTP ${status}: ${detail}` : detail;
}

function authMiddleware(req, res, next) {
  const authorization = req.headers.authorization || '';
  const [type, token] = authorization.split(' ');

  if (type !== 'Bearer' || !token) {
    return res.status(401).json({ message: 'Missing or invalid authorization token' });
  }

  try {
    req.user = jwt.verify(token, JWT_SECRET);
    return next();
  } catch (error) {
    return res.status(401).json({ message: 'Token expired or invalid' });
  }
}

async function ensureGroupExists(groupId, displayName, authHeader) {
  const groupUrl = `${ALFRESCO_BASE_URL}/alfresco/api/-default-/public/alfresco/versions/1/groups/${encodeURIComponent(groupId)}`;

  try {
    await axios.get(groupUrl, {
      timeout: ALFRESCO_TIMEOUT_MS,
      headers: { Authorization: authHeader }
    });
    return { created: false };
  } catch (error) {
    if (error?.response?.status !== 404) {
      throw error;
    }
  }

  try {
    await axios.post(
      `${ALFRESCO_BASE_URL}/alfresco/api/-default-/public/alfresco/versions/1/groups`,
      {
        id: groupId,
        displayName: displayName || groupId
      },
      {
        timeout: ALFRESCO_TIMEOUT_MS,
        headers: {
          Authorization: authHeader,
          'Content-Type': 'application/json'
        }
      }
    );
    return { created: true };
  } catch (error) {
    if (error?.response?.status === 409) {
      return { created: false };
    }
    throw error;
  }
}

async function addUserToGroup(groupId, userId, authHeader) {
  try {
    await axios.post(
      `${ALFRESCO_BASE_URL}/alfresco/api/-default-/public/alfresco/versions/1/groups/${encodeURIComponent(groupId)}/members`,
      {
        id: userId,
        memberType: 'PERSON'
      },
      {
        timeout: ALFRESCO_TIMEOUT_MS,
        headers: {
          Authorization: authHeader,
          'Content-Type': 'application/json'
        }
      }
    );
    return { added: true };
  } catch (error) {
    if (error?.response?.status === 409) {
      return { added: false, reason: 'already-member' };
    }
    throw error;
  }
}

function parseGroupImportRows(filePath) {
  const workbook = XLSX.readFile(filePath, { cellDates: false });
  const firstSheet = workbook.SheetNames[0];
  if (!firstSheet) {
    return [];
  }

  const rows = XLSX.utils.sheet_to_json(workbook.Sheets[firstSheet], {
    defval: '',
    raw: false
  });

  return rows.map((row, index) => ({
    lineNo: index + 2,
    groupId: normalizeGroupId(row.group_id),
    groupDisplayName: String(row.group_display_name || '').trim(),
    userId: String(row.user_id || '').trim(),
    action: String(row.action || 'ADD').trim().toUpperCase()
  }));
}

function parseCsvLine(line) {
  const result = [];
  let current = '';
  let inQuotes = false;

  for (let i = 0; i < line.length; i += 1) {
    const char = line[i];
    const nextChar = line[i + 1];

    if (char === '"') {
      if (inQuotes && nextChar === '"') {
        current += '"';
        i += 1;
      } else {
        inQuotes = !inQuotes;
      }
      continue;
    }

    if (char === ',' && !inQuotes) {
      result.push(current.trim());
      current = '';
      continue;
    }

    current += char;
  }

  result.push(current.trim());
  return result;
}

function normalizeCsvHeader(header) {
  return String(header || '')
    .trim()
    .toLowerCase()
    .replace(/\s+/g, '')
    .replace(/_/g, '');
}

function parseCsvText(text) {
  const content = String(text || '').replace(/^\uFEFF/, '');
  const lines = content
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter((line) => line.length > 0);

  if (!lines.length) {
    return [];
  }

  const headers = parseCsvLine(lines[0]).map(normalizeCsvHeader);
  const rows = [];

  for (let i = 1; i < lines.length; i += 1) {
    const columns = parseCsvLine(lines[i]);
    const row = {};
    headers.forEach((header, index) => {
      row[header] = String(columns[index] || '').trim();
    });
    rows.push({ row, lineNo: i + 1 });
  }

  return rows;
}

function normalizeCsvUserRow(row, lineNo) {
  const username = row.username || row.userid || row.userid || row.user || row.id || '';
  const firstName = row.firstname || row.givenname || row.name || '';
  const lastName = row.lastname || row.surname || row.familyname || '';
  const email = row.email || row.mail || '';
  const password = row.password || row.pass || '';
  const groupsRaw = row.groups || row.group || '';
  const enabledRaw = row.enabled || row.isenabled || '';

  const groups = String(groupsRaw)
    .split(/[;|]/)
    .map((item) => normalizeGroupId(item))
    .filter((item) => item.length > 0);

  const enabledNormalized = String(enabledRaw || '').trim().toLowerCase();
  const enabled = enabledNormalized
    ? !['false', '0', 'no', 'n'].includes(enabledNormalized)
    : true;

  return {
    lineNo,
    username: String(username).trim(),
    firstName: String(firstName).trim(),
    lastName: String(lastName).trim(),
    email: String(email).trim(),
    password: String(password).trim(),
    groups,
    enabled
  };
}

function parseUserCsvRows(filePath) {
  const text = fs.readFileSync(filePath, 'utf8');
  const rawRows = parseCsvText(text);
  return rawRows.map(({ row, lineNo }) => normalizeCsvUserRow(row, lineNo));
}

async function createOrUpdateAlfrescoUser(row, authHeader) {
  const peopleUrl = `${ALFRESCO_BASE_URL}/alfresco/api/-default-/public/alfresco/versions/1/people`;
  const requestConfig = {
    timeout: ALFRESCO_TIMEOUT_MS,
    headers: {
      Authorization: authHeader,
      'Content-Type': 'application/json'
    }
  };

  const publicCreatePayload = {
    id: row.username,
    userName: row.username,
    firstName: row.firstName,
    lastName: row.lastName,
    email: row.email,
    password: row.password
  };

  const publicUpdatePayload = {
    firstName: row.firstName,
    lastName: row.lastName,
    email: row.email,
    enabled: row.enabled
  };

  const legacyCreatePayload = {
    userName: row.username,
    firstName: row.firstName,
    lastName: row.lastName,
    email: row.email,
    password: row.password
  };

  try {
    await axios.post(peopleUrl, publicCreatePayload, requestConfig);
    return { created: true, updated: false, endpoint: 'public-v1' };
  } catch (publicError) {
    if (publicError?.response?.status === 409) {
      await axios.put(
        `${peopleUrl}/${encodeURIComponent(row.username)}`,
        publicUpdatePayload,
        requestConfig
      );
      return { created: false, updated: true, endpoint: 'public-v1' };
    }

    const legacyCreateEndpoints = [
      `${ALFRESCO_BASE_URL}/alfresco/s/api/people`,
      `${ALFRESCO_BASE_URL}/alfresco/service/api/people`
    ];

    for (const endpoint of legacyCreateEndpoints) {
      try {
        await axios.post(endpoint, legacyCreatePayload, requestConfig);
        return { created: true, updated: false, endpoint: endpoint.replace(ALFRESCO_BASE_URL, '') };
      } catch (legacyError) {
        if (legacyError?.response?.status === 409) {
          try {
            await axios.put(
              `${peopleUrl}/${encodeURIComponent(row.username)}`,
              publicUpdatePayload,
              requestConfig
            );
            return { created: false, updated: true, endpoint: `${endpoint.replace(ALFRESCO_BASE_URL, '')}+public-update` };
          } catch (updateError) {
            throw updateError;
          }
        }
      }
    }

    throw publicError;
  }
}

async function processImport(taskId) {
  if (processingTasks.has(taskId)) {
    return;
  }

  processingTasks.add(taskId);

  try {
    const taskResult = await pool.query(`SELECT * FROM ${IMPORTS_TABLE} WHERE id = $1`, [taskId]);
    const task = taskResult.rows[0];
    if (!task) {
      return;
    }

    await updateImportStatus(taskId, 'PROCESSING');
    await addTaskLog(taskId, 'INFO', 'Start calling permission-service');
    await alfrescoAuthProvider.getValidatedServiceAuth({
      taskId,
      purpose: 'permission import pre-check',
      addTaskLog,
      formatError: formatAlfrescoError
    });
    await addTaskLog(taskId, 'INFO', 'Alfresco authentication via credential-manager: OK');
    await safeAddAuditEvent({
      serviceName: SERVICE_PERMISSION_IMPORT,
      username: task.username,
      actionType: 'PROCESS_IMPORT',
      filename: task.filename,
      status: 'PROCESSING',
      message: 'Import task processing started',
      entityType: 'import_task',
      entityId: String(taskId),
      metadata: { stored_filename: task.stored_filename }
    });

    const storedFilePath = path.join(UPLOAD_DIR, task.stored_filename);

    let response;
    let attempt = 0;
    const maxAttempts = 3;

    while (attempt < maxAttempts) {
      attempt += 1;
      try {
        const form = new FormData();
        form.append('file', fs.createReadStream(storedFilePath));
        await addTaskLog(taskId, 'INFO', `Calling permission-service (attempt ${attempt}/${maxAttempts})`);
        response = await axios.post(PERMISSION_SERVICE_URL, form, {
          headers: {
            ...form.getHeaders(),
            accept: 'text/plain'
          },
          timeout: PERMISSION_TIMEOUT_MS,
          maxBodyLength: Infinity,
          maxContentLength: Infinity
        });
        break;
      } catch (error) {
        const message = error?.message || 'Unknown permission-service error';
        await addTaskLog(taskId, 'WARN', `Attempt ${attempt} failed: ${message}`);

        if (attempt >= maxAttempts) {
          throw error;
        }

        await new Promise((resolve) => setTimeout(resolve, 1000 * attempt));
      }
    }

    const responseBody = typeof response.data === 'string' ? response.data : JSON.stringify(response.data);
    await pool.query(
      `UPDATE ${IMPORTS_TABLE} SET status = $1, external_task_id = $2, updated_at = $3, error_message = $4 WHERE id = $5`,
      ['COMPLETED', null, getNowIso(), null, taskId]
    );
    await addTaskLog(taskId, 'INFO', `permission-service completed (${response.status})`);
    await addTaskLog(taskId, 'INFO', `Response: ${responseBody.slice(0, 800)}`);
    await safeAddAuditEvent({
      serviceName: SERVICE_PERMISSION_IMPORT,
      username: task.username,
      actionType: 'PROCESS_IMPORT',
      filename: task.filename,
      status: 'COMPLETED',
      message: `permission-service completed (${response.status})`,
      entityType: 'import_task',
      entityId: String(taskId),
      metadata: { response_status: response.status }
    });
  } catch (error) {
    const reason = error?.response?.data
      ? JSON.stringify(error.response.data).slice(0, 800)
      : (error?.message || 'Unknown error');

    await pool.query(
      `UPDATE ${IMPORTS_TABLE} SET status = $1, error_message = $2, updated_at = $3 WHERE id = $4`,
      ['FAILED', reason, getNowIso(), taskId]
    );
    await addTaskLog(taskId, 'ERROR', `Import failed: ${reason}`);
    await safeAddAuditEvent({
      serviceName: SERVICE_PERMISSION_IMPORT,
      username: task?.username,
      actionType: 'PROCESS_IMPORT',
      filename: task?.filename,
      status: 'FAILED',
      message: `Import failed: ${reason}`,
      entityType: 'import_task',
      entityId: String(taskId),
      metadata: { error: reason }
    });
  } finally {
    processingTasks.delete(taskId);
  }
}

async function processGroupMembershipImport(taskId) {
  if (processingTasks.has(taskId)) {
    return;
  }

  processingTasks.add(taskId);

  try {
    const taskResult = await pool.query(`SELECT * FROM ${IMPORTS_TABLE} WHERE id = $1`, [taskId]);
    const task = taskResult.rows[0];
    if (!task) {
      return;
    }

    await updateImportStatus(taskId, 'PROCESSING');
    await addTaskLog(taskId, 'INFO', 'Start processing group-member import');

    const { credential, authHeader } = await alfrescoAuthProvider.getValidatedServiceAuth({
      taskId,
      requiredGroupId: GROUP_IMPORT_REQUIRED_GROUP,
      purpose: 'user/group management',
      addTaskLog,
      formatError: formatAlfrescoError
    });

    const inputPath = path.join(UPLOAD_DIR, task.stored_filename);
    const rows = parseGroupImportRows(inputPath);
    if (!rows.length) {
      throw new Error('No rows found in Excel');
    }

    const chunkSize = rows.length > 1000 ? 100 : rows.length;
    const needDelay = rows.length > 1000;

    let createdGroups = 0;
    let addedMembers = 0;
    let skippedRows = 0;
    let failedRows = 0;

    for (let i = 0; i < rows.length; i += chunkSize) {
      const chunk = rows.slice(i, i + chunkSize);
      await addTaskLog(taskId, 'INFO', `Processing batch ${Math.floor(i / chunkSize) + 1}, rows ${i + 1}-${i + chunk.length}`);

      for (const row of chunk) {
        try {
          if (!row.groupId) {
            skippedRows += 1;
            await addTaskLog(taskId, 'WARN', `Line ${row.lineNo}: missing group_id`);
            continue;
          }

          if (!row.groupId.startsWith('GROUP_')) {
            skippedRows += 1;
            await addTaskLog(taskId, 'WARN', `Line ${row.lineNo}: group_id must start with GROUP_`);
            continue;
          }

          if (!row.userId) {
            skippedRows += 1;
            await addTaskLog(taskId, 'WARN', `Line ${row.lineNo}: missing user_id`);
            continue;
          }

          if (row.action !== 'ADD') {
            skippedRows += 1;
            await addTaskLog(taskId, 'WARN', `Line ${row.lineNo}: unsupported action '${row.action}'`);
            continue;
          }

          const groupResult = await ensureGroupExists(row.groupId, row.groupDisplayName, authHeader);
          if (groupResult.created) {
            createdGroups += 1;
            await addTaskLog(taskId, 'INFO', `Line ${row.lineNo}: created group ${row.groupId}`);
          }

          const memberResult = await addUserToGroup(row.groupId, row.userId, authHeader);
          if (memberResult.added) {
            addedMembers += 1;
            await addTaskLog(taskId, 'INFO', `Line ${row.lineNo}: added ${row.userId} to ${row.groupId}`);
          } else {
            skippedRows += 1;
            await addTaskLog(taskId, 'INFO', `Line ${row.lineNo}: skip ${row.userId} in ${row.groupId} (${memberResult.reason})`);
          }
        } catch (error) {
          failedRows += 1;
          let reason = error?.response?.data
            ? JSON.stringify(error.response.data).slice(0, 400)
            : (error?.message || 'Unknown row error');

          if (error?.response?.status === 403) {
            reason = `Permission denied by Alfresco (403). Service account '${credential.username}' needs admin privileges (e.g. '${GROUP_IMPORT_REQUIRED_GROUP}') to create groups/add members.`;
          }

          await addTaskLog(taskId, 'ERROR', `Line ${row.lineNo}: ${reason}`);
        }
      }

      if (needDelay && i + chunkSize < rows.length) {
        await sleep(500);
      }
    }

    const summary = {
      totalRows: rows.length,
      createdGroups,
      addedMembers,
      skippedRows,
      failedRows
    };

    const finalStatus = failedRows > 0 ? 'COMPLETED_WITH_ERRORS' : 'COMPLETED';

    await pool.query(
      `UPDATE ${IMPORTS_TABLE} SET status = $1, updated_at = $2, error_message = $3 WHERE id = $4`,
      [finalStatus, getNowIso(), failedRows > 0 ? `Failed rows: ${failedRows}` : null, taskId]
    );

    await addTaskLog(taskId, 'INFO', `Summary: ${JSON.stringify(summary)}`);
    await safeAddAuditEvent({
      serviceName: SERVICE_GROUP_MEMBER_IMPORT,
      username: task.username,
      actionType: 'IMPORT_USER_TO_GROUP',
      filename: task.filename,
      status: finalStatus,
      message: 'Group-member import finished',
      entityType: 'import_task',
      entityId: String(taskId),
      metadata: summary
    });
  } catch (error) {
    const reason = error?.message || 'Unknown error';
    await pool.query(
      `UPDATE ${IMPORTS_TABLE} SET status = $1, error_message = $2, updated_at = $3 WHERE id = $4`,
      ['FAILED', reason, getNowIso(), taskId]
    );
    await addTaskLog(taskId, 'ERROR', `Group-member import failed: ${reason}`);
    await safeAddAuditEvent({
      serviceName: SERVICE_GROUP_MEMBER_IMPORT,
      actionType: 'IMPORT_USER_TO_GROUP',
      status: 'FAILED',
      message: reason,
      entityType: 'import_task',
      entityId: String(taskId)
    });
  } finally {
    processingTasks.delete(taskId);
  }
}

async function processUserCsvImport(taskId) {
  if (processingTasks.has(taskId)) {
    return;
  }

  processingTasks.add(taskId);

  try {
    const taskResult = await pool.query(`SELECT * FROM ${IMPORTS_TABLE} WHERE id = $1`, [taskId]);
    const task = taskResult.rows[0];
    if (!task) {
      return;
    }

    await updateImportStatus(taskId, 'PROCESSING');
    await addTaskLog(taskId, 'INFO', 'Start processing user CSV import');

    const { authHeader } = await alfrescoAuthProvider.getValidatedServiceAuth({
      taskId,
      requiredGroupId: GROUP_IMPORT_REQUIRED_GROUP,
      purpose: 'user/group management',
      addTaskLog,
      formatError: formatAlfrescoError
    });

    const inputPath = path.join(UPLOAD_DIR, task.stored_filename);
    const rows = parseUserCsvRows(inputPath);

    if (!rows.length) {
      throw new Error('No rows found in CSV');
    }

    let createdUsers = 0;
    let updatedUsers = 0;
    let addedGroupMemberships = 0;
    let skippedRows = 0;
    let failedRows = 0;

    for (const row of rows) {
      try {
        if (!row.username || !row.firstName || !row.lastName || !row.email) {
          skippedRows += 1;
          await addTaskLog(taskId, 'WARN', `Line ${row.lineNo}: missing required fields (username, firstName, lastName, email)`);
          continue;
        }

        if (!row.password) {
          skippedRows += 1;
          await addTaskLog(taskId, 'WARN', `Line ${row.lineNo}: missing password`);
          continue;
        }

        const userResult = await createOrUpdateAlfrescoUser(row, authHeader);
        if (userResult.created) {
          createdUsers += 1;
          await addTaskLog(taskId, 'INFO', `Line ${row.lineNo}: created user ${row.username} (${userResult.endpoint || 'unknown-endpoint'})`);
        } else if (userResult.updated) {
          updatedUsers += 1;
          await addTaskLog(taskId, 'INFO', `Line ${row.lineNo}: updated user ${row.username} (${userResult.endpoint || 'unknown-endpoint'})`);
        }

        for (const groupId of row.groups) {
          await ensureGroupExists(groupId, groupId.replace(/^GROUP_/, ''), authHeader);
          const memberResult = await addUserToGroup(groupId, row.username, authHeader);
          if (memberResult.added) {
            addedGroupMemberships += 1;
            await addTaskLog(taskId, 'INFO', `Line ${row.lineNo}: added ${row.username} to ${groupId}`);
          }
        }
      } catch (error) {
        failedRows += 1;
        const reason = formatAlfrescoError(error, 'Cannot create/update user from CSV');
        const detail = error?.response?.data ? JSON.stringify(error.response.data).slice(0, 800) : '';
        await addTaskLog(taskId, 'ERROR', `Line ${row.lineNo}: ${reason}`);
        if (detail) {
          await addTaskLog(taskId, 'ERROR', `Line ${row.lineNo} detail: ${detail}`);
        }
      }
    }

    const summary = {
      totalRows: rows.length,
      createdUsers,
      updatedUsers,
      addedGroupMemberships,
      skippedRows,
      failedRows
    };

    const finalStatus = failedRows > 0 ? 'COMPLETED_WITH_ERRORS' : 'COMPLETED';

    await pool.query(
      `UPDATE ${IMPORTS_TABLE} SET status = $1, updated_at = $2, error_message = $3 WHERE id = $4`,
      [finalStatus, getNowIso(), failedRows > 0 ? `Failed rows: ${failedRows}` : null, taskId]
    );

    await addTaskLog(taskId, 'INFO', `Summary: ${JSON.stringify(summary)}`);
    await safeAddAuditEvent({
      serviceName: SERVICE_USER_CSV_IMPORT,
      username: task.username,
      actionType: 'IMPORT_USERS_CSV',
      filename: task.filename,
      status: finalStatus,
      message: 'User CSV import finished',
      entityType: 'import_task',
      entityId: String(taskId),
      metadata: summary
    });
  } catch (error) {
    const reason = error?.message || 'Unknown error';
    await pool.query(
      `UPDATE ${IMPORTS_TABLE} SET status = $1, error_message = $2, updated_at = $3 WHERE id = $4`,
      ['FAILED', reason, getNowIso(), taskId]
    );
    await addTaskLog(taskId, 'ERROR', `User CSV import failed: ${reason}`);
    await safeAddAuditEvent({
      serviceName: SERVICE_USER_CSV_IMPORT,
      actionType: 'IMPORT_USERS_CSV',
      status: 'FAILED',
      message: reason,
      entityType: 'import_task',
      entityId: String(taskId)
    });
  } finally {
    processingTasks.delete(taskId);
  }
}

app.get('/health', async (req, res) => {
  res.json({
    status: 'ok',
    now: getNowIso()
  });
});

app.post('/api/auth/login', async (req, res) => {
  const { username, password } = req.body || {};

  if (!username || !password) {
    return res.status(400).json({ message: 'username and password are required' });
  }

  try {
    const ticket = await loginToAlfresco(username, password);
    const inGroup = await checkUserGroup(username, ticket);

    if (!inGroup) {
      return res.status(403).json({ message: `User is not in required group: ${REQUIRED_GROUP}` });
    }

    const token = jwt.sign(
      {
        sub: username,
        username
      },
      JWT_SECRET,
      { expiresIn: JWT_EXPIRES_IN }
    );

    await safeAddAuditEvent({
      serviceName: 'auth',
      username,
      actionType: 'LOGIN',
      status: 'SUCCESS',
      message: 'User authenticated successfully',
      entityType: 'user',
      entityId: username
    });

    return res.json({
      token,
      username,
      expiresIn: JWT_EXPIRES_IN
    });
  } catch (error) {
    const status = error?.response?.status;
    if (status === 401) {
      await safeAddAuditEvent({
        serviceName: 'auth',
        username,
        actionType: 'LOGIN',
        status: 'FAILED',
        message: 'Invalid Alfresco credentials',
        entityType: 'user',
        entityId: username
      });
      return res.status(401).json({ message: 'Invalid Alfresco credentials' });
    }

    await safeAddAuditEvent({
      serviceName: 'auth',
      username,
      actionType: 'LOGIN',
      status: 'FAILED',
      message: error?.message || 'Authentication failed',
      entityType: 'user',
      entityId: username
    });

    return res.status(500).json({ message: 'Authentication failed', detail: error?.message || 'Unknown error' });
  }
});

app.post('/api/imports', authMiddleware, upload.single('file'), async (req, res) => {
  const user = req.user?.username || req.user?.sub;
  const file = req.file;

  if (!file) {
    return res.status(400).json({ message: 'file is required' });
  }

  const originalName = file.originalname || 'unknown.xlsx';
  const lower = originalName.toLowerCase();
  if (!lower.endsWith('.xlsx')) {
    return res.status(400).json({ message: 'Only .xlsx files are allowed' });
  }

  const now = getNowIso();

  try {
    const inserted = await pool.query(
      `
      INSERT INTO ${IMPORTS_TABLE} (service_name, timestamp, username, action_type, filename, status, created_at, updated_at)
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
      RETURNING id
      `,
      [SERVICE_PERMISSION_IMPORT, now, user, 'IMPORT_PERMISSION', originalName, 'IN_PROGRESS', now, now]
    );

    const taskId = inserted.rows[0].id;
    const safeName = toSafeFilename(originalName);
    const storedFilename = `${taskId}_${safeName}`;
    const outputPath = path.join(UPLOAD_DIR, storedFilename);

    await fsp.writeFile(outputPath, file.buffer);

    await pool.query(
      `UPDATE ${IMPORTS_TABLE} SET stored_filename = $1, updated_at = $2 WHERE id = $3`,
      [storedFilename, getNowIso(), taskId]
    );

    await addTaskLog(taskId, 'INFO', `Upload received from user ${user}`);
    await addTaskLog(taskId, 'INFO', `File saved as ${storedFilename}`);
    await safeAddAuditEvent({
      serviceName: SERVICE_PERMISSION_IMPORT,
      username: user,
      actionType: 'UPLOAD_PERMISSION_FILE',
      filename: originalName,
      status: 'IN_PROGRESS',
      message: 'Import task accepted',
      entityType: 'import_task',
      entityId: String(taskId),
      metadata: { stored_filename: storedFilename }
    });

    setImmediate(() => {
      processImport(taskId).catch((error) => {
        console.error('Unexpected task error', error);
      });
    });

    return res.status(202).json({
      task_id: taskId,
      status: 'IN_PROGRESS',
      message: 'Import task accepted'
    });
  } catch (error) {
    await safeAddAuditEvent({
      serviceName: SERVICE_PERMISSION_IMPORT,
      username: user,
      actionType: 'UPLOAD_PERMISSION_FILE',
      filename: originalName,
      status: 'FAILED',
      message: error?.message || 'Cannot queue import'
    });
    return res.status(500).json({ message: 'Cannot queue import', detail: error?.message || 'Unknown error' });
  }
});

app.get('/api/tasks/:id', authMiddleware, async (req, res) => {
  const id = Number(req.params.id);
  const taskResult = await pool.query(
    `SELECT id, service_name, timestamp, username, action_type, filename, stored_filename, status, error_message, created_at, updated_at FROM ${IMPORTS_TABLE} WHERE id = $1`,
    [id]
  );
  const task = taskResult.rows[0];

  if (!task) {
    return res.status(404).json({ message: 'Task not found' });
  }

  return res.json({ task });
});

app.get('/api/tasks/:id/logs', authMiddleware, async (req, res) => {
  const id = Number(req.params.id);
  const logsResult = await pool.query(
    `SELECT id, import_id, level, message, created_at FROM ${TASK_LOGS_TABLE} WHERE import_id = $1 ORDER BY id DESC LIMIT 200`,
    [id]
  );
  const logs = logsResult.rows;
  return res.json({ logs });
});

app.get('/api/reports/imports', authMiddleware, async (req, res) => {
  const limit = Math.min(Number(req.query.limit || 50), 200);
  const serviceName = req.query.service_name ? String(req.query.service_name) : null;
  const params = [];
  let whereClause = '';

  if (serviceName) {
    params.push(serviceName);
    whereClause = `WHERE service_name = $${params.length}`;
  }

  params.push(limit);
  const rowsResult = await pool.query(
    `
    SELECT id, service_name, timestamp, username, action_type, filename, stored_filename, status, error_message, created_at, updated_at
    FROM ${IMPORTS_TABLE}
    ${whereClause}
    ORDER BY id DESC
    LIMIT $${params.length}
    `,
    params
  );
  const rows = rowsResult.rows;

  return res.json({
    items: rows,
    count: rows.length
  });
});

app.post('/api/group-memberships/import', authMiddleware, upload.single('file'), async (req, res) => {
  const user = req.user?.username || req.user?.sub;
  const file = req.file;

  if (!file) {
    return res.status(400).json({ message: 'file is required' });
  }

  const originalName = file.originalname || 'group-member-import.xlsx';
  if (!originalName.toLowerCase().endsWith('.xlsx')) {
    return res.status(400).json({ message: 'Only .xlsx files are allowed' });
  }

  const now = getNowIso();

  try {
    const inserted = await pool.query(
      `
      INSERT INTO ${IMPORTS_TABLE} (service_name, timestamp, username, action_type, filename, status, created_at, updated_at)
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
      RETURNING id
      `,
      [SERVICE_GROUP_MEMBER_IMPORT, now, user, 'IMPORT_USER_TO_GROUP', originalName, 'IN_PROGRESS', now, now]
    );

    const taskId = inserted.rows[0].id;
    const storedFilename = `${taskId}_${toSafeFilename(originalName)}`;
    const outputPath = path.join(UPLOAD_DIR, storedFilename);

    await fsp.writeFile(outputPath, file.buffer);
    await pool.query(
      `UPDATE ${IMPORTS_TABLE} SET stored_filename = $1, updated_at = $2 WHERE id = $3`,
      [storedFilename, getNowIso(), taskId]
    );

    await addTaskLog(taskId, 'INFO', `Upload received from user ${user}`);
    await addTaskLog(taskId, 'INFO', `File saved as ${storedFilename}`);
    await addTaskLog(taskId, 'INFO', 'Recommendation: run off-peak to reduce temporary Solr re-index impact');

    await safeAddAuditEvent({
      serviceName: SERVICE_GROUP_MEMBER_IMPORT,
      username: user,
      actionType: 'IMPORT_USER_TO_GROUP',
      filename: originalName,
      status: 'IN_PROGRESS',
      message: 'Group-member import task accepted',
      entityType: 'import_task',
      entityId: String(taskId),
      metadata: {
        off_peak_recommendation: true,
        batching_policy: 'if rows > 1000, chunk=100, delay=500ms'
      }
    });

    setImmediate(() => {
      processGroupMembershipImport(taskId).catch((error) => {
        console.error('Unexpected group import task error', error);
      });
    });

    return res.status(202).json({
      task_id: taskId,
      status: 'IN_PROGRESS',
      message: 'Group-member import task accepted'
    });
  } catch (error) {
    return res.status(500).json({ message: 'Cannot queue group-member import', detail: error?.message || 'Unknown error' });
  }
});

app.post('/api/users/import-csv', authMiddleware, upload.single('file'), async (req, res) => {
  const user = req.user?.username || req.user?.sub;
  const file = req.file;

  if (!file) {
    return res.status(400).json({ message: 'file is required' });
  }

  const originalName = file.originalname || 'users.csv';
  if (!originalName.toLowerCase().endsWith('.csv')) {
    return res.status(400).json({ message: 'Only .csv files are allowed' });
  }

  const now = getNowIso();

  try {
    const inserted = await pool.query(
      `
      INSERT INTO ${IMPORTS_TABLE} (service_name, timestamp, username, action_type, filename, status, created_at, updated_at)
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
      RETURNING id
      `,
      [SERVICE_USER_CSV_IMPORT, now, user, 'IMPORT_USERS_CSV', originalName, 'IN_PROGRESS', now, now]
    );

    const taskId = inserted.rows[0].id;
    const storedFilename = `${taskId}_${toSafeFilename(originalName)}`;
    const outputPath = path.join(UPLOAD_DIR, storedFilename);

    await fsp.writeFile(outputPath, file.buffer);
    await pool.query(
      `UPDATE ${IMPORTS_TABLE} SET stored_filename = $1, updated_at = $2 WHERE id = $3`,
      [storedFilename, getNowIso(), taskId]
    );

    await addTaskLog(taskId, 'INFO', `Upload received from user ${user}`);
    await addTaskLog(taskId, 'INFO', `File saved as ${storedFilename}`);
    await addTaskLog(taskId, 'INFO', 'CSV headers recommended: username,firstName,lastName,email,password,groups,enabled');

    setImmediate(() => {
      processUserCsvImport(taskId).catch((error) => {
        console.error('Unexpected user csv import task error', error);
      });
    });

    return res.status(202).json({
      task_id: taskId,
      status: 'IN_PROGRESS',
      message: 'User CSV import task accepted'
    });
  } catch (error) {
    return res.status(500).json({ message: 'Cannot queue user CSV import', detail: error?.message || 'Unknown error' });
  }
});

app.get('/api/reports/audit', authMiddleware, async (req, res) => {
  const limit = Math.min(Number(req.query.limit || 100), 500);
  const serviceName = req.query.service_name ? String(req.query.service_name) : null;
  const status = req.query.status ? String(req.query.status) : null;
  const username = req.query.username ? String(req.query.username) : null;

  const conditions = [];
  const params = [];

  if (serviceName) {
    params.push(serviceName);
    conditions.push(`service_name = $${params.length}`);
  }
  if (status) {
    params.push(status);
    conditions.push(`status = $${params.length}`);
  }
  if (username) {
    params.push(username);
    conditions.push(`username = $${params.length}`);
  }

  params.push(limit);
  const whereClause = conditions.length ? `WHERE ${conditions.join(' AND ')}` : '';

  const result = await pool.query(
    `
    SELECT id, event_time, service_name, username, action_type, entity_type, entity_id, filename, status, message, metadata
    FROM ${AUDIT_EVENTS_TABLE}
    ${whereClause}
    ORDER BY event_time DESC
    LIMIT $${params.length}
    `,
    params
  );

  return res.json({
    items: result.rows,
    count: result.rows.length
  });
});

app.get('/api/reports/audit/services', authMiddleware, async (req, res) => {
  const result = await pool.query(
    `
    SELECT service_name, status, COUNT(*)::bigint AS total
    FROM ${AUDIT_EVENTS_TABLE}
    GROUP BY service_name, status
    ORDER BY service_name, status
    `
  );

  return res.json({
    items: result.rows,
    count: result.rows.length
  });
});

app.get('/api/pm/config', authMiddleware, async (req, res) => {
  const data = await getPmConfig();
  return res.json({
    ...data,
    running: pmRunInProgress,
    cronActive: Boolean(pmCronTask)
  });
});

app.put('/api/pm/config', authMiddleware, async (req, res) => {
  const user = req.user?.username || req.user?.sub || 'unknown';
  const saved = await savePmConfig(req.body || {}, user);
  await refreshPmCronSchedule();
  return res.json({
    config: saved,
    message: 'PM configuration updated',
    cronActive: Boolean(pmCronTask)
  });
});

app.post('/api/pm/run', authMiddleware, async (req, res) => {
  const user = req.user?.username || req.user?.sub || 'unknown';
  try {
    const runId = await queuePmRun('MANUAL', user);
    return res.status(202).json({
      run_id: runId,
      status: 'IN_PROGRESS',
      message: 'PM run started'
    });
  } catch (error) {
    return res.status(409).json({ message: error?.message || 'PM job is already running' });
  }
});

app.get('/api/pm/runs', authMiddleware, async (req, res) => {
  const limit = Math.min(Number(req.query.limit || 50), 200);
  const errorsOnly = String(req.query.errors_only || 'false').toLowerCase() === 'true';
  const result = await pool.query(
    `
    SELECT id, started_at, finished_at, trigger_type, requested_by, status, output_file, message, stdout_tail, stderr_tail, deleted_files, deleted_rows
    FROM ${PM_RUNS_TABLE}
    ${errorsOnly ? "WHERE status = 'FAILED'" : ''}
    ORDER BY id DESC
    LIMIT $1
    `,
    [limit]
  );

  return res.json({
    items: result.rows,
    count: result.rows.length,
    running: pmRunInProgress
  });
});

async function main() {
  await initDb();
  await refreshPmCronSchedule();
  app.listen(PORT, () => {
    console.log(`beflex-support-backend listening on ${PORT}`);
  });
}

main().catch((error) => {
  console.error('Failed to start server', error);
  process.exit(1);
});

process.on('SIGTERM', async () => {
  if (pmCronTask) {
    pmCronTask.stop();
    pmCronTask.destroy();
  }
  if (pool) {
    await pool.end();
  }
  process.exit(0);
});
