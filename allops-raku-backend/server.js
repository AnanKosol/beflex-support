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

dotenv.config();

const app = express();
app.use(express.json());

const PORT = Number(process.env.PORT || 3000);
const JWT_SECRET = process.env.ALLOPS_JWT_SECRET || 'change-this-in-production';
const JWT_EXPIRES_IN = process.env.ALLOPS_JWT_EXPIRES_IN || '8h';
const ALFRESCO_BASE_URL = process.env.ALFRESCO_BASE_URL || 'http://alfresco:8080';
const REQUIRED_GROUP = process.env.ALLOPS_REQUIRED_GROUP || 'GROUP_allops-raku';
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
const CREDENTIAL_SERVICE_NAME = process.env.CREDENTIAL_SERVICE_NAME || 'alfresco';
const CREDENTIAL_USERNAME_ID = process.env.CREDENTIAL_USERNAME_ID || '1';
const CREDENTIAL_PASSWORD_ID = process.env.CREDENTIAL_PASSWORD_ID || '2';
const ALFRESCO_ADMIN_USERNAME = process.env.ALLOPS_ALFRESCO_ADMIN_USERNAME || '';
const ALFRESCO_ADMIN_PASSWORD = process.env.ALLOPS_ALFRESCO_ADMIN_PASSWORD || '';

const IMPORTS_TABLE = 'allops_raku_imports';
const TASK_LOGS_TABLE = 'allops_raku_task_logs';
const AUDIT_EVENTS_TABLE = 'allops_raku_audit_events';
const SERVICE_PERMISSION_IMPORT = 'permission-import';
const SERVICE_GROUP_MEMBER_IMPORT = 'group-member-import';

const upload = multer({
  storage: multer.memoryStorage(),
  limits: {
    fileSize: Number(process.env.ALLOPS_MAX_FILE_SIZE || 25 * 1024 * 1024)
  }
});

let pool;
const processingTasks = new Set();

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

function parseCredentialPayload(payload) {
  const candidates = [
    payload,
    payload?.data,
    payload?.entry,
    payload?.credential,
    payload?.credentials,
    payload?.result
  ];

  for (const item of candidates) {
    if (!item || typeof item !== 'object') {
      continue;
    }

    const username = item.username || item.userName || item.user || item.alfrescoUsername;
    const password = item.password || item.secret || item.alfrescoPassword;

    if (username && password) {
      return { username, password };
    }
  }

  return null;
}

function normalizeUrl(baseUrl, endpoint) {
  return `${String(baseUrl || '').replace(/\/$/, '')}${endpoint}`;
}

function extractValueFromResponse(data) {
  if (typeof data === 'string' || typeof data === 'number') {
    return String(data).trim();
  }
  if (!data || typeof data !== 'object') {
    return '';
  }

  const value = data.value || data.data || data.entry?.value || data.result?.value;
  return value ? String(value).trim() : '';
}

function extractCredentialFromExport(data) {
  if (!data) {
    return null;
  }

  if (Array.isArray(data)) {
    const byKey = Object.fromEntries(
      data
        .filter((item) => item && typeof item === 'object')
        .map((item) => [String(item.key || item.name || '').toLowerCase(), String(item.value || '').trim()])
    );
    const username = byKey['alfresco/username'] || byKey.username || byKey.user || byKey.user_id;
    const password = byKey['alfresco/password'] || byKey.password || byKey.pass || byKey.secret;
    if (username && password) {
      return { username, password };
    }
  }

  if (typeof data === 'object') {
    const candidate = parseCredentialPayload(data);
    if (candidate?.username && candidate?.password) {
      return candidate;
    }

    const directUsername = data['alfresco/username'] || data.username || data.user || data.user_id;
    const directPassword = data['alfresco/password'] || data.password || data.pass || data.secret;
    if (directUsername && directPassword) {
      return {
        username: String(directUsername).trim(),
        password: String(directPassword).trim()
      };
    }
  }

  return null;
}

async function fetchCredentialValueById(id, headers) {
  const endpoints = [
    `/credentials/${id}/value`,
    `/api/credential-manager/credentials/${id}/value`
  ];

  for (const endpoint of endpoints) {
    try {
      const response = await axios.get(normalizeUrl(CREDENTIAL_MANAGER_URL, endpoint), {
        headers,
        timeout: 8000
      });

      const value = extractValueFromResponse(response.data);
      if (value) {
        return { value, source: endpoint };
      }
    } catch (error) {
      continue;
    }
  }

  return null;
}

async function getAlfrescoServiceCredential() {
  if (ALFRESCO_ADMIN_USERNAME && ALFRESCO_ADMIN_PASSWORD) {
    return {
      username: ALFRESCO_ADMIN_USERNAME,
      password: ALFRESCO_ADMIN_PASSWORD,
      source: 'env'
    };
  }

  const endpoints = [
    `/export/service/${CREDENTIAL_SERVICE_NAME}`,
    `/api/credential-manager/export/service/${CREDENTIAL_SERVICE_NAME}`,
    '/api/credentials/alfresco',
    '/api/credentials?service=alfresco',
    '/api/credential/alfresco',
    '/api/secrets/alfresco'
  ];

  const headers = {};
  if (CREDENTIAL_MANAGER_TOKEN) {
    headers.Authorization = `Bearer ${CREDENTIAL_MANAGER_TOKEN}`;
    headers['x-api-token'] = CREDENTIAL_MANAGER_TOKEN;
  }

  for (const endpoint of endpoints) {
    try {
      const response = await axios.get(normalizeUrl(CREDENTIAL_MANAGER_URL, endpoint), {
        headers,
        timeout: 8000
      });

      const credential = extractCredentialFromExport(response.data) || parseCredentialPayload(response.data);
      if (credential?.username && credential?.password) {
        return {
          ...credential,
          source: `credential-manager:${endpoint}`
        };
      }
    } catch (error) {
      continue;
    }
  }

  const usernameValue = await fetchCredentialValueById(CREDENTIAL_USERNAME_ID, headers);
  const passwordValue = await fetchCredentialValueById(CREDENTIAL_PASSWORD_ID, headers);
  if (usernameValue?.value && passwordValue?.value) {
    return {
      username: usernameValue.value,
      password: passwordValue.value,
      source: `credential-manager:${usernameValue.source}+${passwordValue.source}`
    };
  }

  throw new Error('Cannot get Alfresco service credential from credential-manager or environment');
}

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

function getBasicAuthHeader(username, password) {
  const basic = Buffer.from(`${username}:${password}`).toString('base64');
  return `Basic ${basic}`;
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

async function isUserAlreadyInGroup(groupId, userId, authHeader) {
  const memberUrl = `${ALFRESCO_BASE_URL}/alfresco/api/-default-/public/alfresco/versions/1/groups/${encodeURIComponent(groupId)}/members?where=${encodeURIComponent(`(id='${userId}')`)}&maxItems=100`;
  const response = await axios.get(memberUrl, {
    timeout: ALFRESCO_TIMEOUT_MS,
    headers: { Authorization: authHeader }
  });

  const entries = response?.data?.list?.entries || [];
  return entries.some((item) => item?.entry?.id === userId);
}

async function addUserToGroup(groupId, userId, authHeader) {
  if (await isUserAlreadyInGroup(groupId, userId, authHeader)) {
    return { added: false, reason: 'already-member' };
  }

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

    const credential = await getAlfrescoServiceCredential();
    const authHeader = getBasicAuthHeader(credential.username, credential.password);
    await addTaskLog(taskId, 'INFO', `Using credential source: ${credential.source}`);

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
          const reason = error?.response?.data
            ? JSON.stringify(error.response.data).slice(0, 400)
            : (error?.message || 'Unknown row error');
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

async function main() {
  await initDb();
  app.listen(PORT, () => {
    console.log(`allops-raku-backend listening on ${PORT}`);
  });
}

main().catch((error) => {
  console.error('Failed to start server', error);
  process.exit(1);
});

process.on('SIGTERM', async () => {
  if (pool) {
    await pool.end();
  }
  process.exit(0);
});
