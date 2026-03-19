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
const ExcelJS = require('exceljs');
const cron = require('node-cron');
const crypto = require('crypto');
const { createAlfrescoAuthProvider } = require('./services/alfresco-auth-provider');
const { runPmReport } = require('./scripts/pm-report');
const { registerQueryPermissionRoutes } = require('./routes/query-permission-routes');

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
const SERVICE_QUERY_SIZING = 'query-sizing';
const SERVICE_QUERY_ADD_PERMISSION = 'query-add-permission';
const QUERY_SIZING_TABLE = 'allops_raku_query_sizing_reports';
const QUERY_SIZING_MAX_ITEMS = 100;
const QUERY_SIZING_PAGE_DELAY_MS = Number(process.env.QUERY_SIZING_PAGE_DELAY_MS || 150);
const QUERY_PERMISSION_TEMPLATE_TABLE = 'allops_raku_permission_templates';
const QUERY_PERMISSION_RUN_TABLE = 'allops_raku_permission_query_runs';
const QUERY_PERMISSION_RUN_ITEM_TABLE = 'allops_raku_permission_query_run_items';
const QUERY_PERMISSION_SETTINGS_TABLE = 'allops_raku_query_permission_settings';
const QUERY_PERMISSION_MAX_ITEMS = 100;
const QUERY_PERMISSION_PAGE_DELAY_MS = Number(process.env.QUERY_PERMISSION_PAGE_DELAY_MS || 150);
const QUERY_PERMISSION_RETENTION_DAYS = Number(process.env.QUERY_PERMISSION_RETENTION_DAYS || 2);
const QUERY_PERMISSION_ADD_CONCURRENCY = Math.max(1, Number(process.env.QUERY_PERMISSION_ADD_CONCURRENCY || 5));
const QUERY_PERMISSION_ADD_MAX_RETRIES = Math.max(0, Number(process.env.QUERY_PERMISSION_ADD_MAX_RETRIES || 2));
const QUERY_PERMISSION_ADD_RETRY_BASE_MS = Math.max(100, Number(process.env.QUERY_PERMISSION_ADD_RETRY_BASE_MS || 500));
const QUERY_PERMISSION_DETAIL_RETENTION_DAYS = Math.max(1, Number(process.env.QUERY_PERMISSION_DETAIL_RETENTION_DAYS || 30));
const QUERY_PERMISSION_DETAIL_CLEANUP_CRON = process.env.QUERY_PERMISSION_DETAIL_CLEANUP_CRON || '30 2 * * *';
const PM_CONFIG_TABLE = 'allops_raku_pm_config';
const PM_RUNS_TABLE = 'allops_raku_pm_runs';
const PM_CUSTOMER_TABLE = 'allops_raku_pm_customers';
const PM_ENVIRONMENT_TABLE = 'allops_raku_pm_environments';
const PM_SERVER_TABLE = 'allops_raku_pm_servers';
const PM_APPLICATION_TABLE = 'allops_raku_pm_applications';
const PM_AGENT_TABLE = 'allops_raku_pm_agents';
const PM_JOB_TABLE = 'allops_raku_pm_jobs';
const PM_SNAPSHOT_TABLE = 'allops_raku_pm_snapshots';
const PM_SCRIPT_TIMEOUT_MS = Number(process.env.PM_SCRIPT_TIMEOUT_MS || 30 * 60 * 1000);
const PM_TIMEZONE = process.env.PM_TIMEZONE || 'Asia/Bangkok';
const PM_AGENT_SHARED_TOKEN = process.env.PM_AGENT_SHARED_TOKEN || '';

const upload = multer({
  storage: multer.memoryStorage(),
  limits: {
    fileSize: Number(process.env.ALLOPS_MAX_FILE_SIZE || 25 * 1024 * 1024)
  }
});

let pool;
const processingTasks = new Set();
const querySizingProcessingRuns = new Set();
const queryPermissionProcessingRuns = new Set();
let pmRunInProgress = false;
let pmCronTask = null;
let queryPermissionCleanupTimer = null;
let queryPermissionCleanupCronTask = null;
let queryPermissionRuntimeSettings = null;

function getNowIso() {
  return new Date().toISOString();
}

function getDefaultQueryPermissionSettings() {
  return {
    addConcurrency: QUERY_PERMISSION_ADD_CONCURRENCY,
    addMaxRetries: QUERY_PERMISSION_ADD_MAX_RETRIES,
    addRetryBaseMs: QUERY_PERMISSION_ADD_RETRY_BASE_MS,
    detailRetentionDays: QUERY_PERMISSION_DETAIL_RETENTION_DAYS,
    detailCleanupCron: QUERY_PERMISSION_DETAIL_CLEANUP_CRON
  };
}

function sanitizeQueryPermissionSettings(raw = {}, fallback = getDefaultQueryPermissionSettings()) {
  const asInt = (value, base, min, max) => {
    const n = Number(value);
    if (!Number.isFinite(n)) {
      return base;
    }
    return Math.min(max, Math.max(min, Math.floor(n)));
  };

  const cronValue = String(raw.detailCleanupCron ?? fallback.detailCleanupCron ?? '').trim() || fallback.detailCleanupCron;
  const cronParts = cronValue.split(/\s+/).filter(Boolean);
  const safeCron = cronParts.length === 5 ? cronValue : fallback.detailCleanupCron;

  return {
    addConcurrency: asInt(raw.addConcurrency, fallback.addConcurrency, 1, 20),
    addMaxRetries: asInt(raw.addMaxRetries, fallback.addMaxRetries, 0, 10),
    addRetryBaseMs: asInt(raw.addRetryBaseMs, fallback.addRetryBaseMs, 100, 10000),
    detailRetentionDays: asInt(raw.detailRetentionDays, fallback.detailRetentionDays, 1, 365),
    detailCleanupCron: safeCron
  };
}

function getQueryPermissionRuntimeSettings() {
  if (!queryPermissionRuntimeSettings) {
    queryPermissionRuntimeSettings = getDefaultQueryPermissionSettings();
  }
  return { ...queryPermissionRuntimeSettings };
}

async function applyQueryPermissionRuntimeSettings(settings) {
  queryPermissionRuntimeSettings = sanitizeQueryPermissionSettings(settings, getDefaultQueryPermissionSettings());
  return getQueryPermissionRuntimeSettings();
}

function toSafeFilename(name) {
  return (name || 'upload.xlsx').replace(/[^a-zA-Z0-9._-]/g, '_');
}

function toUploadSubdir(name) {
  return toSafeFilename(String(name || 'misc').trim() || 'misc');
}

function buildStoredUploadPath(serviceName, fileName) {
  return `${toUploadSubdir(serviceName)}/${toSafeFilename(fileName)}`;
}

async function writeStoredUploadFile(storedFilename, buffer) {
  const outputPath = path.join(UPLOAD_DIR, storedFilename);
  await fsp.mkdir(path.dirname(outputPath), { recursive: true });
  await fsp.writeFile(outputPath, buffer);
  return outputPath;
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

function formatSizeInMb(bytes) {
  const value = Number(bytes || 0) / (1024 * 1024);
  return Number(value.toFixed(2));
}

function formatSizeInGb(bytes) {
  const value = Number(bytes || 0) / (1024 * 1024 * 1024);
  return Number(value.toFixed(4));
}

function toPositiveInt(value, fallback) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return fallback;
  }
  return Math.floor(parsed);
}

function truncateMessage(value, maxLen = 1000) {
  const text = String(value || '').trim();
  if (!text) {
    return '';
  }
  return text.length <= maxLen ? text : `${text.slice(0, maxLen)}...`;
}

function csvEscape(value) {
  const text = String(value ?? '');
  if (/[",\n\r]/.test(text)) {
    return `"${text.replace(/"/g, '""')}"`;
  }
  return text;
}

function toCsv(rows) {
  return rows.map((row) => row.map((cell) => csvEscape(cell)).join(',')).join('\n');
}

function normalizeAftsQuery(input) {
  const raw = String(input || '').trim();
  const hadJsonEscapedQuotes = /\\"/.test(raw);
  let normalized = hadJsonEscapedQuotes ? raw.replace(/\\"/g, '"') : raw;

  // Strip outer wrapper quotes caused by pasting JSON-encoded strings.
  // e.g. pasting: "PATH:\"/app:...\"  AND cm:name:'test'"
  //   raw starts with bare `"` (not `\"`) → JSON string wrapper → strip leading `"`
  //   raw ends with bare `"` (not `\"`)   → JSON string wrapper → strip trailing `"`
  if (hadJsonEscapedQuotes) {
    if (raw.startsWith('"')) {
      normalized = normalized.slice(1);
    }
    if (raw.endsWith('"') && !raw.endsWith('\\"')) {
      normalized = normalized.slice(0, -1);
    }
    normalized = normalized.trim();
  }

  return {
    raw,
    normalized,
    hadJsonEscapedQuotes
  };
}

function normalizePermissionTargetType(input) {
  const value = String(input || 'all').trim().toLowerCase();
  if (value === 'folder' || value === 'cm:folder') {
    return 'folder';
  }
  if (value === 'file' || value === 'cm:file' || value === 'cm:content') {
    return 'file';
  }
  return 'all';
}

function cleanupPermissionQueryText(input) {
  let value = String(input || '').trim();
  if (!value) {
    return '';
  }

  let previous = null;
  while (value !== previous) {
    previous = value;
    value = value
      // Auto-fix adjacent clauses with missing boolean operator, e.g.
      // PATH:"..." cm:name:'x' -> PATH:"..." AND cm:name:'x'
      .replace(/(["'\)\]])\s+(?=(?:[A-Za-z_][A-Za-z0-9_]*\s*:|NOT\s*\())/g, '$1 AND ')
      .replace(/\s+(AND|OR)\s+(AND|OR)\s+/ig, ' $1 ')
      .replace(/\(\s*(AND|OR)\s+/ig, '(')
      .replace(/\s+(AND|OR)\s*\)/ig, ')')
      .replace(/^\s*(AND|OR)\s+/ig, '')
      .replace(/\s+(AND|OR)\s*$/ig, '')
      .replace(/\(\s*\)/g, ' ')
      .replace(/\s{2,}/g, ' ')
      .trim();
  }

  return value;
}

function buildPermissionQueryDefinition(baseQuery, targetType) {
  const normalized = normalizeAftsQuery(baseQuery).normalized;
  let detectedTargetType = null;
  const strippedQuery = normalized.replace(/TYPE\s*:\s*"cm:(content|folder)"/ig, (_match, nodeType) => {
    detectedTargetType = nodeType.toLowerCase() === 'content' ? 'file' : 'folder';
    return ' ';
  });

  const sanitizedQuery = cleanupPermissionQueryText(strippedQuery);
  const effectiveTargetType = normalizePermissionTargetType(detectedTargetType || targetType);
  const suffix = effectiveTargetType === 'folder'
    ? 'TYPE:"cm:folder"'
    : (effectiveTargetType === 'file' ? 'TYPE:"cm:content"' : '');
  const effectiveQuery = sanitizedQuery
    ? (suffix ? `(${sanitizedQuery}) AND ${suffix}` : sanitizedQuery)
    : suffix;

  return {
    rawQuery: normalized,
    sanitizedQuery,
    targetType: effectiveTargetType,
    detectedTargetType,
    hadEmbeddedType: Boolean(detectedTargetType),
    effectiveQuery
  };
}

function buildPermissionSearchQuery(baseQuery, targetType) {
  return buildPermissionQueryDefinition(baseQuery, targetType).effectiveQuery;
}

async function readFirstSheetRows(filePath) {
  const workbook = new ExcelJS.Workbook();
  await workbook.xlsx.readFile(filePath);
  const worksheet = workbook.worksheets[0];
  if (!worksheet) {
    return [];
  }

  const headerRow = worksheet.getRow(1);
  const headers = [];
  headerRow.eachCell({ includeEmpty: true }, (cell, columnNumber) => {
    headers[columnNumber] = String(cell?.text || '').trim();
  });

  const rows = [];
  for (let rowIndex = 2; rowIndex <= worksheet.rowCount; rowIndex += 1) {
    const row = worksheet.getRow(rowIndex);
    const mapped = {};
    let hasValue = false;

    for (let colIndex = 1; colIndex < headers.length; colIndex += 1) {
      const key = String(headers[colIndex] || '').trim();
      if (!key) {
        continue;
      }
      const cellText = String(row.getCell(colIndex)?.text || '').trim();
      mapped[key] = cellText;
      if (cellText) {
        hasValue = true;
      }
    }

    if (hasValue) {
      rows.push(mapped);
    }
  }

  return rows;
}

async function parsePermissionExcelRows(filePath) {
  const rows = await readFirstSheetRows(filePath);
  return rows.map((row, index) => {
    const groupName = normalizeGroupId(row.Group_Name || row.group_name || row.group || row.GROUP_NAME);
    const role = String(row.Role || row.role || '').trim();
    return {
      lineNo: index + 2,
      groupName,
      role
    };
  });
}

function extractNodePath(entry) {
  const pathName = String(entry?.path?.name || '').trim();
  if (pathName) {
    return pathName;
  }
  return String(entry?.name || '').trim();
}

async function processQuerySizingRun(runId) {
  if (querySizingProcessingRuns.has(runId)) {
    return;
  }

  querySizingProcessingRuns.add(runId);

  try {
    const runResult = await pool.query(
      `SELECT id, queried_at, username, query_text, status FROM ${QUERY_SIZING_TABLE} WHERE id = $1`,
      [runId]
    );
    const run = runResult.rows[0];
    if (!run) {
      return;
    }

    await pool.query(
      `UPDATE ${QUERY_SIZING_TABLE} SET status = $1, message = $2, updated_at = $3 WHERE id = $4`,
      ['PROCESSING', 'Query sizing started', getNowIso(), runId]
    );

    await safeAddAuditEvent({
      serviceName: SERVICE_QUERY_SIZING,
      username: run.username,
      actionType: 'QUERY_SIZING_RUN',
      status: 'PROCESSING',
      message: 'Query sizing run started',
      entityType: 'query_sizing_run',
      entityId: String(runId),
      metadata: {
        maxItemsPerPage: QUERY_SIZING_MAX_ITEMS
      }
    });

    const { authHeader, credential } = await alfrescoAuthProvider.getValidatedServiceAuth({
      purpose: 'query sizing search',
      formatError: formatAlfrescoError
    });

    const searchUrl = `${ALFRESCO_BASE_URL}/alfresco/api/-default-/public/search/versions/1/search`;
    let skipCount = 0;
    let page = 1;
    let hasMoreItems = true;
    let totalFiles = 0;
    let totalSizeBytes = 0;

    while (hasMoreItems) {
      const queryBody = {
        query: {
          language: 'afts',
          query: run.query_text
        },
        include: ['properties', 'path'],
        paging: {
          maxItems: QUERY_SIZING_MAX_ITEMS,
          skipCount
        },
        sort: [{ type: 'FIELD', field: 'cm:name', ascending: 'false' }]
      };

      const response = await axios.post(searchUrl, queryBody, {
        timeout: ALFRESCO_TIMEOUT_MS,
        headers: {
          Authorization: authHeader,
          'Content-Type': 'application/json'
        }
      });

      const entries = response?.data?.list?.entries || [];
      const pagination = response?.data?.list?.pagination || {};

      let batchSizeBytes = 0;
      for (const item of entries) {
        const sizeInBytes = Number(item?.entry?.content?.sizeInBytes || 0);
        if (Number.isFinite(sizeInBytes) && sizeInBytes > 0) {
          batchSizeBytes += sizeInBytes;
        }
      }

      totalFiles += entries.length;
      totalSizeBytes += batchSizeBytes;

      const statusMessage = `Processing page ${page} (batch: ${entries.length} files)`;
      await pool.query(
        `
        UPDATE ${QUERY_SIZING_TABLE}
        SET total_files = $1,
            total_size_bytes = $2,
            total_size_mb = $3,
            total_size_gb = $4,
            message = $5,
            updated_at = $6
        WHERE id = $7
        `,
        [
          totalFiles,
          totalSizeBytes,
          formatSizeInMb(totalSizeBytes),
          formatSizeInGb(totalSizeBytes),
          statusMessage,
          getNowIso(),
          runId
        ]
      );

      hasMoreItems = Boolean(pagination.hasMoreItems);
      skipCount += QUERY_SIZING_MAX_ITEMS;
      page += 1;

      if (hasMoreItems && QUERY_SIZING_PAGE_DELAY_MS > 0) {
        await sleep(QUERY_SIZING_PAGE_DELAY_MS);
      }
    }

    const finalMessage = `Completed with ${totalFiles} files using service account '${credential.username}'`;
    await pool.query(
      `
      UPDATE ${QUERY_SIZING_TABLE}
      SET status = $1,
          total_files = $2,
          total_size_bytes = $3,
          total_size_mb = $4,
          total_size_gb = $5,
          message = $6,
          finished_at = $7,
          updated_at = $8
      WHERE id = $9
      `,
      [
        'COMPLETED',
        totalFiles,
        totalSizeBytes,
        formatSizeInMb(totalSizeBytes),
        formatSizeInGb(totalSizeBytes),
        finalMessage,
        getNowIso(),
        getNowIso(),
        runId
      ]
    );

    await safeAddAuditEvent({
      serviceName: SERVICE_QUERY_SIZING,
      username: run.username,
      actionType: 'QUERY_SIZING_RUN',
      status: 'COMPLETED',
      message: finalMessage,
      entityType: 'query_sizing_run',
      entityId: String(runId),
      metadata: {
        total_files: totalFiles,
        total_size_bytes: totalSizeBytes,
        total_size_mb: formatSizeInMb(totalSizeBytes),
        total_size_gb: formatSizeInGb(totalSizeBytes),
        max_items_per_page: QUERY_SIZING_MAX_ITEMS
      }
    });
  } catch (error) {
    const reason = truncateMessage(formatAlfrescoError(error, 'Query sizing failed'), 1000);
    await pool.query(
      `
      UPDATE ${QUERY_SIZING_TABLE}
      SET status = $1,
          message = $2,
          finished_at = $3,
          updated_at = $4
      WHERE id = $5
      `,
      ['FAILED', reason, getNowIso(), getNowIso(), runId]
    );

    await safeAddAuditEvent({
      serviceName: SERVICE_QUERY_SIZING,
      actionType: 'QUERY_SIZING_RUN',
      status: 'FAILED',
      message: reason,
      entityType: 'query_sizing_run',
      entityId: String(runId)
    });
  } finally {
    querySizingProcessingRuns.delete(runId);
  }
}

async function processPermissionQueryRun(runId) {
  if (queryPermissionProcessingRuns.has(runId)) {
    return;
  }

  queryPermissionProcessingRuns.add(runId);

  try {
    const runResult = await pool.query(`SELECT * FROM ${QUERY_PERMISSION_RUN_TABLE} WHERE id = $1`, [runId]);
    const run = runResult.rows[0];
    if (!run) {
      return;
    }

    const effectiveQuery = buildPermissionSearchQuery(run.query_text, run.target_type);
    if (!effectiveQuery) {
      throw new Error('query is required');
    }

    await pool.query(
      `
      UPDATE ${QUERY_PERMISSION_RUN_TABLE}
      SET status = $1,
          effective_query = $2,
          message = $3,
          listed_count = 0,
          updated_at = $4
      WHERE id = $5
      `,
      ['LISTING', effectiveQuery, 'Query listing started', getNowIso(), runId]
    );
    await pool.query(`DELETE FROM ${QUERY_PERMISSION_RUN_ITEM_TABLE} WHERE run_id = $1`, [runId]);

    await safeAddAuditEvent({
      serviceName: SERVICE_QUERY_ADD_PERMISSION,
      username: run.username,
      actionType: 'QUERY_LIST',
      status: 'LISTING',
      message: 'Permission query listing started',
      entityType: 'permission_run',
      entityId: String(runId),
      metadata: {
        target_type: run.target_type,
        query: run.query_text
      }
    });

    const { authHeader } = await alfrescoAuthProvider.getValidatedServiceAuth({
      purpose: 'query add permission listing',
      formatError: formatAlfrescoError
    });

    const searchUrl = `${ALFRESCO_BASE_URL}/alfresco/api/-default-/public/search/versions/1/search`;
    let skipCount = 0;
    let hasMoreItems = true;
    let page = 1;
    let listedCount = 0;

    while (hasMoreItems) {
      const response = await axios.post(
        searchUrl,
        {
          query: {
            language: 'afts',
            query: effectiveQuery
          },
          include: ['path'],
          paging: {
            maxItems: QUERY_PERMISSION_MAX_ITEMS,
            skipCount
          },
          sort: [{ type: 'FIELD', field: 'cm:name', ascending: 'true' }]
        },
        {
          timeout: ALFRESCO_TIMEOUT_MS,
          headers: {
            Authorization: authHeader,
            'Content-Type': 'application/json'
          }
        }
      );

      const entries = response?.data?.list?.entries || [];
      const pagination = response?.data?.list?.pagination || {};

      for (const item of entries) {
        const entry = item?.entry || {};
        const nodeId = String(entry.id || '').trim();
        if (!nodeId) {
          continue;
        }

        const nodeType = String(entry.nodeType || '').trim();
        const nodeRef = `workspace://SpacesStore/${nodeId}`;
        const nodeName = String(entry.name || '').trim();
        const nodePath = extractNodePath(entry);

        await pool.query(
          `
          INSERT INTO ${QUERY_PERMISSION_RUN_ITEM_TABLE}
            (run_id, node_ref, node_id, node_type, node_name, node_path, status, message, created_at, updated_at)
          VALUES
            ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
          ON CONFLICT (run_id, node_id)
          DO UPDATE SET node_type = EXCLUDED.node_type,
                        node_name = EXCLUDED.node_name,
                        node_path = EXCLUDED.node_path,
                        updated_at = EXCLUDED.updated_at
          `,
          [runId, nodeRef, nodeId, nodeType, nodeName, nodePath, 'LISTED', null, getNowIso(), getNowIso()]
        );

        listedCount += 1;
      }

      await pool.query(
        `UPDATE ${QUERY_PERMISSION_RUN_TABLE} SET listed_count = $1, message = $2, updated_at = $3 WHERE id = $4`,
        [listedCount, `Listing page ${page} (${entries.length} items)`, getNowIso(), runId]
      );

      hasMoreItems = Boolean(pagination.hasMoreItems);
      skipCount += QUERY_PERMISSION_MAX_ITEMS;
      page += 1;

      if (hasMoreItems && QUERY_PERMISSION_PAGE_DELAY_MS > 0) {
        await sleep(QUERY_PERMISSION_PAGE_DELAY_MS);
      }
    }

    await pool.query(
      `
      UPDATE ${QUERY_PERMISSION_RUN_TABLE}
      SET status = $1,
          listed_count = $2,
          message = $3,
          finished_at = $4,
          updated_at = $5
      WHERE id = $6
      `,
      ['LISTED', listedCount, `List completed (${listedCount} items)`, getNowIso(), getNowIso(), runId]
    );

    await safeAddAuditEvent({
      serviceName: SERVICE_QUERY_ADD_PERMISSION,
      username: run.username,
      actionType: 'QUERY_LIST',
      status: 'LISTED',
      message: `List completed (${listedCount} items)`,
      entityType: 'permission_run',
      entityId: String(runId),
      metadata: {
        listed_count: listedCount,
        target_type: run.target_type
      }
    });
  } catch (error) {
    const reason = truncateMessage(formatAlfrescoError(error, 'Permission query listing failed'), 1000);
    await pool.query(
      `
      UPDATE ${QUERY_PERMISSION_RUN_TABLE}
      SET status = $1,
          message = $2,
          finished_at = $3,
          updated_at = $4
      WHERE id = $5
      `,
      ['FAILED', reason, getNowIso(), getNowIso(), runId]
    );

    await safeAddAuditEvent({
      serviceName: SERVICE_QUERY_ADD_PERMISSION,
      actionType: 'QUERY_LIST',
      status: 'FAILED',
      message: reason,
      entityType: 'permission_run',
      entityId: String(runId)
    });
  } finally {
    queryPermissionProcessingRuns.delete(runId);
  }
}

function extractSiteShortNameFromNodePath(nodePath) {
  const normalizedPath = String(nodePath || '').trim();
  if (!normalizedPath) {
    return '';
  }

  const match = normalizedPath.match(/\/Sites\/([^/]+)/i);
  return String(match?.[1] || '').trim();
}

async function listSiteMemberIds(siteShortName, authHeader) {
  const memberIds = new Set();
  let hasExplicitGroupMembers = false;
  const endpoint = `${ALFRESCO_BASE_URL}/alfresco/api/-default-/public/alfresco/versions/1/sites/${encodeURIComponent(siteShortName)}/members`;

  const addCandidateGroupKeys = (value) => {
    const raw = String(value || '').trim();
    if (!raw) {
      return;
    }

    const normalized = normalizeGroupId(raw);
    const plain = normalized.replace(/^GROUP_/i, '');

    [raw, normalized, plain, `GROUP_${plain}`].forEach((candidate) => {
      const token = String(candidate || '').trim();
      if (!token) {
        return;
      }
      memberIds.add(token.toLowerCase());
    });
  };

  const collectMembers = async (whereClause = '') => {
    let skipCount = 0;
    let hasMoreItems = true;

    while (hasMoreItems) {
      const response = await axios.get(endpoint, {
        timeout: ALFRESCO_TIMEOUT_MS,
        headers: { Authorization: authHeader },
        params: {
          maxItems: 100,
          skipCount,
          ...(whereClause ? { where: whereClause } : {})
        }
      });

      const entries = response?.data?.list?.entries || [];
      for (const entryWrapper of entries) {
        const entry = entryWrapper?.entry || {};
        const memberType = String(entry.memberType || entry.authorityType || '').trim().toUpperCase();
        if (memberType === 'GROUP') {
          hasExplicitGroupMembers = true;
        }

        const rawId = String(entry.id || '').trim();
        const authorityId = String(entry.authorityId || '').trim();
        const authorityName = String(entry.authorityName || '').trim();
        const looksLikeGroup = [rawId, authorityId, authorityName].some((value) => /^GROUP_/i.test(String(value || '').trim()));
        if (looksLikeGroup) {
          hasExplicitGroupMembers = true;
        }

        addCandidateGroupKeys(entry.id);
        addCandidateGroupKeys(entry.authorityId);
        addCandidateGroupKeys(entry.authorityName);
      }

      const pagination = response?.data?.list?.pagination || {};
      hasMoreItems = Boolean(pagination.hasMoreItems);
      skipCount += Number(pagination.count || entries.length || 0);

      if (!entries.length) {
        break;
      }
    }
  };

  await collectMembers('');

  try {
    await collectMembers(`(memberType='GROUP')`);
  } catch (_error) {
    // Some versions may not support this where clause; keep base result as fallback.
  }

  return {
    memberIds,
    hasExplicitGroupMembers
  };
}

async function isGroupMemberOfSite(siteShortName, groupId, authHeader) {
  const normalized = normalizeGroupId(groupId);
  if (!normalized) {
    return false;
  }

  const plain = normalized.replace(/^GROUP_/i, '');
  const candidates = Array.from(new Set([
    normalized,
    plain,
    normalized.toUpperCase(),
    plain.toUpperCase(),
    normalized.toLowerCase(),
    plain.toLowerCase()
  ].filter(Boolean)));
  let notFoundCount = 0;

  for (const candidate of candidates) {
    try {
      await axios.get(
        `${ALFRESCO_BASE_URL}/alfresco/api/-default-/public/alfresco/versions/1/sites/${encodeURIComponent(siteShortName)}/members/${encodeURIComponent(candidate)}`,
        {
          timeout: ALFRESCO_TIMEOUT_MS,
          headers: { Authorization: authHeader }
        }
      );
      return { isMember: true, notFoundOnly: false };
    } catch (error) {
      const status = Number(error?.response?.status || 0);
      if (status === 404) {
        notFoundCount += 1;
        continue;
      }
      throw error;
    }
  }

  return {
    isMember: false,
    notFoundOnly: notFoundCount > 0
  };
}

async function validateSiteMembershipForGroups(siteShortName, groupNames, authHeader) {
  const normalizedGroups = Array.from(new Set(
    (groupNames || [])
      .map((name) => normalizeGroupId(name))
      .filter((name) => /^GROUP_/i.test(name))
  ));

  if (!normalizedGroups.length) {
    return [];
  }

  const { memberIds: siteMemberIds, hasExplicitGroupMembers } = await listSiteMemberIds(siteShortName, authHeader);
  const missingGroups = [];
  let directCheckCount = 0;
  let directNotFoundOnlyCount = 0;

  for (const groupId of normalizedGroups) {
    const plain = groupId.replace(/^GROUP_/i, '');
    const inMemberList = [groupId, plain, `GROUP_${plain}`]
      .map((item) => String(item || '').toLowerCase())
      .some((key) => siteMemberIds.has(key));

    if (inMemberList) {
      continue;
    }

    const directMemberResult = await isGroupMemberOfSite(siteShortName, groupId, authHeader);
    directCheckCount += 1;
    if (directMemberResult?.notFoundOnly) {
      directNotFoundOnlyCount += 1;
    }

    if (!directMemberResult?.isMember) {
      missingGroups.push(groupId);
    }
  }

  const allGroupsFlaggedMissing = missingGroups.length === normalizedGroups.length;
  const allDirectChecksNotFoundOnly = directCheckCount > 0 && directNotFoundOnlyCount === directCheckCount;
  const cannotReliablyValidateGroups = !hasExplicitGroupMembers && allGroupsFlaggedMissing && allDirectChecksNotFoundOnly;

  if (cannotReliablyValidateGroups) {
    // Some ACS environments don't expose group memberships through public site-members APIs.
    // Avoid blocking permission add with false negatives in that case.
    return [];
  }

  return missingGroups;
}

async function addPermissionsToNode(item, rows, authHeader, inheritPermissions = true) {
  const nodeId = String(item?.node_id || '').trim();

  const classifyPermissionPutError = (error) => {
    const status = Number(error?.response?.status || 0);
    const brief = String(error?.response?.data?.error?.briefSummary || '');
    const key = String(error?.response?.data?.error?.errorKey || '');
    const message = String(error?.message || '');
    const rawDetail = `${brief} ${key} ${message}`.toLowerCase();

    if (status === 404) {
      return `HTTP 404: Node not found (${nodeId})`;
    }
    if (status === 403) {
      return `HTTP 403: Permission denied to modify node (${nodeId})`;
    }
    const looksLikeGroupInvalid = ['authority', 'group', 'does not exist', 'not found'].some((h) => rawDetail.includes(h));
    const looksLikeRoleInvalid = ['permission', 'invalid value', 'invalid permission', 'accessstatus', 'role'].some((h) => rawDetail.includes(h));
    if ((status === 400 || status === 422) && looksLikeGroupInvalid && !looksLikeRoleInvalid) {
      return `HTTP ${status}: Group not found`;
    }
    if ((status === 400 || status === 422) && looksLikeRoleInvalid) {
      return `HTTP ${status}: Permission role invalid`;
    }
    return formatAlfrescoError(error, 'permission replace failed');
  };

  // Build fresh locallySet from Excel rows (REPLACE semantics — clears all old permissions)
  const locallySet = rows
    .map((row) => ({
      authorityId: String(row?.groupName || '').trim(),
      name: String(row?.role || '').trim(),
      accessStatus: 'ALLOWED'
    }))
    .filter((e) => e.authorityId && e.name);

  try {
    await axios.put(
      `${ALFRESCO_BASE_URL}/alfresco/api/-default-/public/alfresco/versions/1/nodes/${encodeURIComponent(nodeId)}`,
      {
        permissions: {
          isInheritanceEnabled: Boolean(inheritPermissions),
          locallySet
        }
      },
      {
        timeout: ALFRESCO_TIMEOUT_MS,
        headers: {
          Authorization: authHeader,
          'Content-Type': 'application/json'
        }
      }
    );
    return { applied: locallySet.length, errors: [], retryable: false };
  } catch (error) {
    const status = Number(error?.response?.status || 0);
    const code = String(error?.code || '').toUpperCase();
    const retryable = [429, 500, 502, 503, 504].includes(status)
      || ['ECONNABORTED', 'ETIMEDOUT', 'ECONNRESET', 'EPIPE', 'ENOTFOUND'].includes(code);
    return { applied: 0, errors: [classifyPermissionPutError(error)], retryable };
  }
}

async function addPermissionsToNodeWithRetry(item, rows, authHeader, inheritPermissions = true) {
  const settings = getQueryPermissionRuntimeSettings();
  const maxAttempts = Number(settings.addMaxRetries || 0) + 1;
  let attempt = 0;
  let lastResult = { applied: 0, errors: ['Unknown permission update error'], retryable: false };

  while (attempt < maxAttempts) {
    attempt += 1;
    const result = await addPermissionsToNode(item, rows, authHeader, inheritPermissions);
    lastResult = result;

    if (!result.errors.length || !result.retryable || attempt >= maxAttempts) {
      return result;
    }

    const backoffMs = Number(settings.addRetryBaseMs || 500) * (2 ** (attempt - 1));
    await sleep(backoffMs);
  }

  return lastResult;
}

async function runPermissionUpdatesWithConcurrency({
  items,
  permissionRows,
  authHeader,
  inheritPermissions,
  onItemResult
}) {
  if (!items.length) {
    return;
  }

  const settings = getQueryPermissionRuntimeSettings();
  const concurrency = Math.min(Number(settings.addConcurrency || 5), items.length);
  let cursor = 0;

  const worker = async () => {
    while (true) {
      const index = cursor;
      cursor += 1;
      if (index >= items.length) {
        return;
      }

      const item = items[index];
      const update = await addPermissionsToNodeWithRetry(item, permissionRows, authHeader, inheritPermissions);
      await onItemResult(item, update);
    }
  };

  await Promise.all(Array.from({ length: concurrency }, () => worker()));
}

async function processPermissionAddRun(runId, source = 'run') {
  if (queryPermissionProcessingRuns.has(runId)) {
    throw new Error('Run is already processing');
  }

  queryPermissionProcessingRuns.add(runId);

  try {
    const runResult = await pool.query(
      `
      SELECT r.*, t.permission_filename AS template_permission_filename, t.permission_stored_filename AS template_permission_stored_filename, t.inherit_permissions AS template_inherit_permissions
      FROM ${QUERY_PERMISSION_RUN_TABLE} r
      LEFT JOIN ${QUERY_PERMISSION_TEMPLATE_TABLE} t ON t.id = r.template_id
      WHERE r.id = $1
      `,
      [runId]
    );
    const run = runResult.rows[0];
    if (!run) {
      throw new Error('Run not found');
    }

    if (!['LISTED', 'COMPLETED_WITH_ERRORS'].includes(String(run.status || ''))) {
      throw new Error('Run must be LISTED before Add Permission');
    }

    const selectedSource = source === 'template' ? 'template' : 'run';
    const storedFilename = selectedSource === 'template' ? run.template_permission_stored_filename : run.permission_stored_filename;
    const displayFilename = selectedSource === 'template' ? run.template_permission_filename : run.permission_filename;

    if (!storedFilename) {
      throw new Error(selectedSource === 'template'
        ? 'Template has no permission excel. Please attach file first.'
        : 'Run has no permission excel. Please import permission file first.');
    }

    const permissionFilePath = path.join(UPLOAD_DIR, storedFilename);
    if (!fs.existsSync(permissionFilePath)) {
      throw new Error('Permission file not found on server');
    }

    const permissionRows = (await parsePermissionExcelRows(permissionFilePath))
      .filter((row) => row.groupName && row.role);

    if (!permissionRows.length) {
      throw new Error('Permission file has no valid rows. Required columns: Group_Name, Role');
    }

    await pool.query(
      `
      UPDATE ${QUERY_PERMISSION_RUN_TABLE}
      SET status = $1,
          add_source = $2,
          add_started_at = $3,
          add_success_count = 0,
          add_failed_count = 0,
          message = $4,
          updated_at = $5
      WHERE id = $6
      `,
      ['ADDING_PERMISSION', selectedSource, getNowIso(), `Adding permission from ${displayFilename || storedFilename}`, getNowIso(), runId]
    );

    const itemsResult = await pool.query(
      `SELECT id, node_id, node_ref, node_name, node_path FROM ${QUERY_PERMISSION_RUN_ITEM_TABLE} WHERE run_id = $1 ORDER BY id ASC`,
      [runId]
    );
    const items = itemsResult.rows;

    if (!items.length) {
      throw new Error('No listed nodes found in this run');
    }

    const { authHeader } = await alfrescoAuthProvider.getValidatedServiceAuth({
      requiredGroupId: GROUP_IMPORT_REQUIRED_GROUP,
      purpose: 'query add permission update',
      formatError: formatAlfrescoError
    });

    const allGroupNames = permissionRows.map((row) => row.groupName);
    const siteItems = items
      .map((item) => ({ item, siteShortName: extractSiteShortNameFromNodePath(item.node_path) }))
      .filter((entry) => Boolean(entry.siteShortName));
    const uniqueSites = Array.from(new Set(siteItems.map((entry) => entry.siteShortName)));

    const missingBySite = [];
    for (const siteShortName of uniqueSites) {
      const missingGroups = await validateSiteMembershipForGroups(siteShortName, allGroupNames, authHeader);
      if (missingGroups.length) {
        missingBySite.push({ siteShortName, missingGroups });
      }
    }

    if (missingBySite.length) {
      const detail = missingBySite
        .map((entry) => `${entry.siteShortName}: ${entry.missingGroups.join(', ')}`)
        .join(' | ');
      const validationMessage = truncateMessage(`Site membership validation failed. Missing group membership -> ${detail}`, 1000);

      await pool.query(
        `UPDATE ${QUERY_PERMISSION_RUN_ITEM_TABLE} SET status = $1, message = $2, updated_at = $3 WHERE run_id = $4 AND status = $5`,
        ['ADD_FAILED', validationMessage, getNowIso(), runId, 'LISTED']
      );

      await pool.query(
        `UPDATE ${QUERY_PERMISSION_RUN_TABLE} SET status = $1, add_success_count = 0, add_failed_count = $2, message = $3, finished_at = $4, add_finished_at = $5, updated_at = $6 WHERE id = $7`,
        ['FAILED', items.length, validationMessage, getNowIso(), getNowIso(), getNowIso(), runId]
      );

      await safeAddAuditEvent({
        serviceName: SERVICE_QUERY_ADD_PERMISSION,
        username: run.username,
        actionType: 'ADD_PERMISSION',
        status: 'FAILED',
        message: validationMessage,
        entityType: 'permission_run',
        entityId: String(runId),
        metadata: {
          add_source: selectedSource,
          missing_site_memberships: missingBySite
        }
      });

      return;
    }

    let successItems = 0;
    let failedItems = 0;
    const inheritPermissions = Boolean(run.template_inherit_permissions ?? true);

    await runPermissionUpdatesWithConcurrency({
      items,
      permissionRows,
      authHeader,
      inheritPermissions,
      onItemResult: async (item, update) => {
        if (!update.errors.length) {
          successItems += 1;
          await pool.query(
            `UPDATE ${QUERY_PERMISSION_RUN_ITEM_TABLE} SET status = $1, message = $2, updated_at = $3 WHERE id = $4`,
            ['ADD_SUCCESS', `Applied ${update.applied} permission row(s)`, getNowIso(), item.id]
          );
        } else {
          failedItems += 1;
          await pool.query(
            `UPDATE ${QUERY_PERMISSION_RUN_ITEM_TABLE} SET status = $1, message = $2, updated_at = $3 WHERE id = $4`,
            ['ADD_FAILED', truncateMessage(update.errors.join(' | '), 1000), getNowIso(), item.id]
          );
        }

        await pool.query(
          `UPDATE ${QUERY_PERMISSION_RUN_TABLE} SET add_success_count = $1, add_failed_count = $2, message = $3, updated_at = $4 WHERE id = $5`,
          [
            successItems,
            failedItems,
            `Adding permission... success=${successItems}, failed=${failedItems}, concurrency=${getQueryPermissionRuntimeSettings().addConcurrency}`,
            getNowIso(),
            runId
          ]
        );
      }
    });

    const finalStatus = failedItems > 0 ? 'COMPLETED_WITH_ERRORS' : 'COMPLETED';
    await pool.query(
      `
      UPDATE ${QUERY_PERMISSION_RUN_TABLE}
      SET status = $1,
          add_success_count = $2,
          add_failed_count = $3,
          add_finished_at = $4,
          finished_at = $5,
          message = $6,
          updated_at = $7
      WHERE id = $8
      `,
      [
        finalStatus,
        successItems,
        failedItems,
        getNowIso(),
        getNowIso(),
        `Add permission completed: success=${successItems}, failed=${failedItems}`,
        getNowIso(),
        runId
      ]
    );

    await safeAddAuditEvent({
      serviceName: SERVICE_QUERY_ADD_PERMISSION,
      username: run.username,
      actionType: 'ADD_PERMISSION',
      status: finalStatus,
      message: `Add permission completed: success=${successItems}, failed=${failedItems}`,
      entityType: 'permission_run',
      entityId: String(runId),
      metadata: {
        add_source: selectedSource,
        permission_file: displayFilename || storedFilename,
        total_items: items.length,
        add_success_count: successItems,
        add_failed_count: failedItems
      }
    });
  } finally {
    queryPermissionProcessingRuns.delete(runId);
  }
}

async function processPermissionRetryFailedRun(runId, source = 'template') {
  if (queryPermissionProcessingRuns.has(runId)) {
    throw new Error('Run is already processing');
  }

  queryPermissionProcessingRuns.add(runId);

  try {
    const runResult = await pool.query(
      `
      SELECT r.*, t.permission_filename AS template_permission_filename, t.permission_stored_filename AS template_permission_stored_filename, t.inherit_permissions AS template_inherit_permissions
      FROM ${QUERY_PERMISSION_RUN_TABLE} r
      LEFT JOIN ${QUERY_PERMISSION_TEMPLATE_TABLE} t ON t.id = r.template_id
      WHERE r.id = $1
      `,
      [runId]
    );

    const run = runResult.rows[0];
    if (!run) {
      throw new Error('Run not found');
    }

    const runStatus = String(run.status || '').toUpperCase();
    if (['QUEUED', 'LISTING', 'ADDING_PERMISSION'].includes(runStatus)) {
      throw new Error(`Cannot retry while run is in status ${runStatus}`);
    }

    const selectedSource = source === 'run' ? 'run' : 'template';
    const storedFilename = selectedSource === 'template' ? run.template_permission_stored_filename : run.permission_stored_filename;
    const displayFilename = selectedSource === 'template' ? run.template_permission_filename : run.permission_filename;

    if (!storedFilename) {
      throw new Error(selectedSource === 'template'
        ? 'Template has no permission excel. Please attach file first.'
        : 'Run has no permission excel. Please import permission file first.');
    }

    const permissionFilePath = path.join(UPLOAD_DIR, storedFilename);
    if (!fs.existsSync(permissionFilePath)) {
      throw new Error('Permission file not found on server');
    }

    const permissionRows = (await parsePermissionExcelRows(permissionFilePath))
      .filter((row) => row.groupName && row.role);

    if (!permissionRows.length) {
      throw new Error('Permission file has no valid rows. Required columns: Group_Name, Role');
    }

    const itemsResult = await pool.query(
      `SELECT id, node_id, node_ref, node_name, node_path FROM ${QUERY_PERMISSION_RUN_ITEM_TABLE} WHERE run_id = $1 AND status = 'ADD_FAILED' ORDER BY id ASC`,
      [runId]
    );
    const failedItems = itemsResult.rows;

    if (!failedItems.length) {
      throw new Error('No failed items to retry');
    }

    await pool.query(
      `
      UPDATE ${QUERY_PERMISSION_RUN_TABLE}
      SET status = $1,
          add_source = $2,
          add_started_at = $3,
          message = $4,
          updated_at = $5
      WHERE id = $6
      `,
      ['ADDING_PERMISSION', selectedSource, getNowIso(), `Retrying failed items from ${displayFilename || storedFilename}`, getNowIso(), runId]
    );

    const { authHeader } = await alfrescoAuthProvider.getValidatedServiceAuth({
      requiredGroupId: GROUP_IMPORT_REQUIRED_GROUP,
      purpose: 'query add permission retry failed items',
      formatError: formatAlfrescoError
    });

    const allGroupNames = permissionRows.map((row) => row.groupName);
    const siteItems = failedItems
      .map((item) => ({ item, siteShortName: extractSiteShortNameFromNodePath(item.node_path) }))
      .filter((entry) => Boolean(entry.siteShortName));
    const uniqueSites = Array.from(new Set(siteItems.map((entry) => entry.siteShortName)));

    const missingBySite = [];
    for (const siteShortName of uniqueSites) {
      const missingGroups = await validateSiteMembershipForGroups(siteShortName, allGroupNames, authHeader);
      if (missingGroups.length) {
        missingBySite.push({ siteShortName, missingGroups });
      }
    }

    if (missingBySite.length) {
      const detail = missingBySite
        .map((entry) => `${entry.siteShortName}: ${entry.missingGroups.join(', ')}`)
        .join(' | ');
      const validationMessage = truncateMessage(`Site membership validation failed before retry. Missing group membership -> ${detail}`, 1000);

      await pool.query(
        `UPDATE ${QUERY_PERMISSION_RUN_ITEM_TABLE} SET message = $1, updated_at = $2 WHERE run_id = $3 AND status = $4`,
        [validationMessage, getNowIso(), runId, 'ADD_FAILED']
      );

      await pool.query(
        `UPDATE ${QUERY_PERMISSION_RUN_TABLE} SET status = $1, message = $2, finished_at = $3, add_finished_at = $4, updated_at = $5 WHERE id = $6`,
        ['FAILED', validationMessage, getNowIso(), getNowIso(), getNowIso(), runId]
      );

      await safeAddAuditEvent({
        serviceName: SERVICE_QUERY_ADD_PERMISSION,
        username: run.username,
        actionType: 'ADD_PERMISSION_RETRY_FAILED',
        status: 'FAILED',
        message: validationMessage,
        entityType: 'permission_run',
        entityId: String(runId),
        metadata: {
          add_source: selectedSource,
          missing_site_memberships: missingBySite
        }
      });

      return;
    }

    let retriedSuccess = 0;
    let retriedFailed = 0;
    const inheritPermissions = Boolean(run.template_inherit_permissions ?? true);

    await runPermissionUpdatesWithConcurrency({
      items: failedItems,
      permissionRows,
      authHeader,
      inheritPermissions,
      onItemResult: async (item, update) => {
        if (!update.errors.length) {
          retriedSuccess += 1;
          await pool.query(
            `UPDATE ${QUERY_PERMISSION_RUN_ITEM_TABLE} SET status = $1, message = $2, updated_at = $3 WHERE id = $4`,
            ['ADD_SUCCESS', `Retry applied ${update.applied} permission row(s)`, getNowIso(), item.id]
          );
        } else {
          retriedFailed += 1;
          await pool.query(
            `UPDATE ${QUERY_PERMISSION_RUN_ITEM_TABLE} SET status = $1, message = $2, updated_at = $3 WHERE id = $4`,
            ['ADD_FAILED', truncateMessage(update.errors.join(' | '), 1000), getNowIso(), item.id]
          );
        }
      }
    });

    const summaryResult = await pool.query(
      `
      SELECT
        SUM(CASE WHEN status = 'LISTED' THEN 1 ELSE 0 END)::bigint AS wait_count,
        SUM(CASE WHEN status = 'ADD_SUCCESS' THEN 1 ELSE 0 END)::bigint AS success_count,
        SUM(CASE WHEN status = 'ADD_FAILED' THEN 1 ELSE 0 END)::bigint AS failed_count
      FROM ${QUERY_PERMISSION_RUN_ITEM_TABLE}
      WHERE run_id = $1
      `,
      [runId]
    );

    const summary = summaryResult.rows[0] || {};
    const waitCount = Number(summary.wait_count || 0);
    const successCount = Number(summary.success_count || 0);
    const failedCount = Number(summary.failed_count || 0);
    const finalStatus = failedCount > 0 ? 'COMPLETED_WITH_ERRORS' : (waitCount > 0 ? 'LISTED' : 'COMPLETED');

    await pool.query(
      `
      UPDATE ${QUERY_PERMISSION_RUN_TABLE}
      SET status = $1,
          add_success_count = $2,
          add_failed_count = $3,
          add_finished_at = $4,
          finished_at = $5,
          message = $6,
          updated_at = $7
      WHERE id = $8
      `,
      [
        finalStatus,
        successCount,
        failedCount,
        getNowIso(),
        getNowIso(),
        `Retry failed completed: retried=${failedItems.length}, success=${retriedSuccess}, failed=${retriedFailed}`,
        getNowIso(),
        runId
      ]
    );

    await safeAddAuditEvent({
      serviceName: SERVICE_QUERY_ADD_PERMISSION,
      username: run.username,
      actionType: 'ADD_PERMISSION_RETRY_FAILED',
      status: finalStatus,
      message: `Retry failed completed: retried=${failedItems.length}, success=${retriedSuccess}, failed=${retriedFailed}`,
      entityType: 'permission_run',
      entityId: String(runId),
      metadata: {
        add_source: selectedSource,
        permission_file: displayFilename || storedFilename,
        retried_items: failedItems.length,
        retried_success: retriedSuccess,
        retried_failed: retriedFailed,
        add_success_count: successCount,
        add_failed_count: failedCount
      }
    });
  } finally {
    queryPermissionProcessingRuns.delete(runId);
  }
}

async function cleanupQueryPermissionData() {
  const retentionSetting = Number(getQueryPermissionRuntimeSettings().detailRetentionDays || 30);
  const retentionDays = Number.isFinite(retentionSetting)
    ? Math.max(1, Math.floor(retentionSetting))
    : 30;

  const staleRuns = await pool.query(
    `
    SELECT r.id
    FROM ${QUERY_PERMISSION_RUN_TABLE} r
    WHERE r.status IN ('LISTED', 'COMPLETED', 'COMPLETED_WITH_ERRORS', 'FAILED')
      AND r.detail_cleared_at IS NULL
      AND COALESCE(r.finished_at, r.updated_at, r.created_at) < NOW() - ($1::text || ' days')::interval
      AND EXISTS (SELECT 1 FROM ${QUERY_PERMISSION_RUN_ITEM_TABLE} i WHERE i.run_id = r.id)
    ORDER BY r.id ASC
    LIMIT 200
    `,
    [String(retentionDays)]
  );

  if (!staleRuns.rows.length) {
    return { clearedRuns: 0, clearedItems: 0 };
  }

  let clearedRuns = 0;
  let clearedItems = 0;

  for (const row of staleRuns.rows) {
    const runId = Number(row.id);
    const deleted = await pool.query(`DELETE FROM ${QUERY_PERMISSION_RUN_ITEM_TABLE} WHERE run_id = $1`, [runId]);
    const removed = Number(deleted.rowCount || 0);
    if (removed <= 0) {
      continue;
    }

    const clearNote = `Details cleared by retention policy (${retentionDays} day(s)); summary kept`;
    await pool.query(
      `
      UPDATE ${QUERY_PERMISSION_RUN_TABLE}
      SET detail_cleared_at = $1,
          detail_cleared_item_count = $2,
          detail_cleared_reason = $3,
          message = $4,
          updated_at = $5
      WHERE id = $6
      `,
      [getNowIso(), removed, clearNote, truncateMessage(clearNote, 1000), getNowIso(), runId]
    );

    clearedRuns += 1;
    clearedItems += removed;
  }

  return { clearedRuns, clearedItems };
}

async function loadQueryPermissionSettings() {
  const defaults = getDefaultQueryPermissionSettings();
  const result = await pool.query(
    `
    SELECT add_concurrency, add_max_retries, add_retry_base_ms, detail_retention_days, detail_cleanup_cron
    FROM ${QUERY_PERMISSION_SETTINGS_TABLE}
    WHERE id = 1
    `
  );

  if (!result.rows[0]) {
    await pool.query(
      `
      INSERT INTO ${QUERY_PERMISSION_SETTINGS_TABLE}
        (id, add_concurrency, add_max_retries, add_retry_base_ms, detail_retention_days, detail_cleanup_cron, updated_at, updated_by)
      VALUES
        (1, $1, $2, $3, $4, $5, $6, $7)
      `,
      [
        defaults.addConcurrency,
        defaults.addMaxRetries,
        defaults.addRetryBaseMs,
        defaults.detailRetentionDays,
        defaults.detailCleanupCron,
        getNowIso(),
        'system-default'
      ]
    );
    await applyQueryPermissionRuntimeSettings(defaults);
    return getQueryPermissionRuntimeSettings();
  }

  const dbSettings = {
    addConcurrency: result.rows[0].add_concurrency,
    addMaxRetries: result.rows[0].add_max_retries,
    addRetryBaseMs: result.rows[0].add_retry_base_ms,
    detailRetentionDays: result.rows[0].detail_retention_days,
    detailCleanupCron: result.rows[0].detail_cleanup_cron
  };

  await applyQueryPermissionRuntimeSettings(dbSettings);
  return getQueryPermissionRuntimeSettings();
}

async function saveQueryPermissionSettings(nextSettings, updatedBy = 'unknown') {
  const merged = sanitizeQueryPermissionSettings({
    ...getQueryPermissionRuntimeSettings(),
    ...(nextSettings || {})
  }, getDefaultQueryPermissionSettings());

  await pool.query(
    `
    INSERT INTO ${QUERY_PERMISSION_SETTINGS_TABLE}
      (id, add_concurrency, add_max_retries, add_retry_base_ms, detail_retention_days, detail_cleanup_cron, updated_at, updated_by)
    VALUES
      (1, $1, $2, $3, $4, $5, $6, $7)
    ON CONFLICT (id) DO UPDATE SET
      add_concurrency = EXCLUDED.add_concurrency,
      add_max_retries = EXCLUDED.add_max_retries,
      add_retry_base_ms = EXCLUDED.add_retry_base_ms,
      detail_retention_days = EXCLUDED.detail_retention_days,
      detail_cleanup_cron = EXCLUDED.detail_cleanup_cron,
      updated_at = EXCLUDED.updated_at,
      updated_by = EXCLUDED.updated_by
    `,
    [
      merged.addConcurrency,
      merged.addMaxRetries,
      merged.addRetryBaseMs,
      merged.detailRetentionDays,
      merged.detailCleanupCron,
      getNowIso(),
      updatedBy
    ]
  );

  await applyQueryPermissionRuntimeSettings(merged);
  await refreshQueryPermissionCleanupSchedule();
  return getQueryPermissionRuntimeSettings();
}

async function refreshQueryPermissionCleanupSchedule() {
  if (queryPermissionCleanupCronTask) {
    queryPermissionCleanupCronTask.stop();
    if (typeof queryPermissionCleanupCronTask.destroy === 'function') {
      queryPermissionCleanupCronTask.destroy();
    }
    queryPermissionCleanupCronTask = null;
  }

  const cronExpression = String(getQueryPermissionRuntimeSettings().detailCleanupCron || QUERY_PERMISSION_DETAIL_CLEANUP_CRON);
  queryPermissionCleanupCronTask = cron.schedule(cronExpression, () => {
    cleanupQueryPermissionData().catch((error) => {
      console.error('Query permission cleanup failed', error?.message || error);
    });
  }, { timezone: PM_TIMEZONE });
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
  await fsp.mkdir(path.join(UPLOAD_DIR, toUploadSubdir(SERVICE_PERMISSION_IMPORT)), { recursive: true });
  await fsp.mkdir(path.join(UPLOAD_DIR, toUploadSubdir(SERVICE_GROUP_MEMBER_IMPORT)), { recursive: true });
  await fsp.mkdir(path.join(UPLOAD_DIR, toUploadSubdir(SERVICE_USER_CSV_IMPORT)), { recursive: true });
  await fsp.mkdir(path.join(UPLOAD_DIR, toUploadSubdir(SERVICE_QUERY_ADD_PERMISSION)), { recursive: true });

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

  await pool.query(
    `
    CREATE TABLE IF NOT EXISTS ${QUERY_SIZING_TABLE} (
      id BIGSERIAL PRIMARY KEY,
      queried_at TIMESTAMPTZ NOT NULL,
      username TEXT NOT NULL,
      query_text TEXT NOT NULL,
      status TEXT NOT NULL,
      total_files BIGINT NOT NULL DEFAULT 0,
      total_size_bytes BIGINT NOT NULL DEFAULT 0,
      total_size_mb NUMERIC(20, 2) NOT NULL DEFAULT 0,
      total_size_gb NUMERIC(20, 4) NOT NULL DEFAULT 0,
      message TEXT,
      created_at TIMESTAMPTZ NOT NULL,
      updated_at TIMESTAMPTZ NOT NULL,
      finished_at TIMESTAMPTZ
    )
    `
  );

  await pool.query(`CREATE INDEX IF NOT EXISTS idx_${QUERY_SIZING_TABLE}_queried_at ON ${QUERY_SIZING_TABLE}(queried_at DESC)`);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_${QUERY_SIZING_TABLE}_username ON ${QUERY_SIZING_TABLE}(username, queried_at DESC)`);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_${QUERY_SIZING_TABLE}_status ON ${QUERY_SIZING_TABLE}(status, queried_at DESC)`);

  await pool.query(
    `
    CREATE TABLE IF NOT EXISTS ${QUERY_PERMISSION_TEMPLATE_TABLE} (
      id BIGSERIAL PRIMARY KEY,
      template_name TEXT NOT NULL,
      query_text TEXT NOT NULL,
      target_type TEXT NOT NULL DEFAULT 'all',
      permission_filename TEXT,
      permission_stored_filename TEXT,
      inherit_permissions BOOLEAN NOT NULL DEFAULT TRUE,
      created_by TEXT NOT NULL,
      updated_by TEXT,
      created_at TIMESTAMPTZ NOT NULL,
      updated_at TIMESTAMPTZ NOT NULL
    )
    `
  );

  await pool.query(
    `ALTER TABLE ${QUERY_PERMISSION_TEMPLATE_TABLE} ADD COLUMN IF NOT EXISTS inherit_permissions BOOLEAN NOT NULL DEFAULT TRUE`
  );

  await pool.query(
    `
    CREATE TABLE IF NOT EXISTS ${QUERY_PERMISSION_RUN_TABLE} (
      id BIGSERIAL PRIMARY KEY,
      template_id BIGINT REFERENCES ${QUERY_PERMISSION_TEMPLATE_TABLE}(id) ON DELETE SET NULL,
      queried_at TIMESTAMPTZ NOT NULL,
      username TEXT NOT NULL,
      query_text TEXT NOT NULL,
      effective_query TEXT,
      target_type TEXT NOT NULL DEFAULT 'all',
      status TEXT NOT NULL,
      listed_count BIGINT NOT NULL DEFAULT 0,
      add_success_count BIGINT NOT NULL DEFAULT 0,
      add_failed_count BIGINT NOT NULL DEFAULT 0,
      permission_filename TEXT,
      permission_stored_filename TEXT,
      add_source TEXT,
      message TEXT,
      created_at TIMESTAMPTZ NOT NULL,
      updated_at TIMESTAMPTZ NOT NULL,
      finished_at TIMESTAMPTZ,
      add_started_at TIMESTAMPTZ,
      add_finished_at TIMESTAMPTZ,
      detail_cleared_at TIMESTAMPTZ,
      detail_cleared_item_count BIGINT NOT NULL DEFAULT 0,
      detail_cleared_reason TEXT
    )
    `
  );

  await pool.query(`ALTER TABLE ${QUERY_PERMISSION_RUN_TABLE} ADD COLUMN IF NOT EXISTS detail_cleared_at TIMESTAMPTZ`);
  await pool.query(`ALTER TABLE ${QUERY_PERMISSION_RUN_TABLE} ADD COLUMN IF NOT EXISTS detail_cleared_item_count BIGINT NOT NULL DEFAULT 0`);
  await pool.query(`ALTER TABLE ${QUERY_PERMISSION_RUN_TABLE} ADD COLUMN IF NOT EXISTS detail_cleared_reason TEXT`);

  await pool.query(
    `
    CREATE TABLE IF NOT EXISTS ${QUERY_PERMISSION_RUN_ITEM_TABLE} (
      id BIGSERIAL PRIMARY KEY,
      run_id BIGINT NOT NULL REFERENCES ${QUERY_PERMISSION_RUN_TABLE}(id) ON DELETE CASCADE,
      node_ref TEXT NOT NULL,
      node_id TEXT NOT NULL,
      node_type TEXT,
      node_name TEXT,
      node_path TEXT,
      status TEXT NOT NULL DEFAULT 'LISTED',
      message TEXT,
      created_at TIMESTAMPTZ NOT NULL,
      updated_at TIMESTAMPTZ NOT NULL,
      CONSTRAINT uq_${QUERY_PERMISSION_RUN_ITEM_TABLE}_run_node UNIQUE (run_id, node_id)
    )
    `
  );

  await pool.query(`CREATE INDEX IF NOT EXISTS idx_${QUERY_PERMISSION_TEMPLATE_TABLE}_updated_at ON ${QUERY_PERMISSION_TEMPLATE_TABLE}(updated_at DESC)`);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_${QUERY_PERMISSION_RUN_TABLE}_queried_at ON ${QUERY_PERMISSION_RUN_TABLE}(queried_at DESC)`);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_${QUERY_PERMISSION_RUN_TABLE}_status ON ${QUERY_PERMISSION_RUN_TABLE}(status, queried_at DESC)`);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_${QUERY_PERMISSION_RUN_TABLE}_username ON ${QUERY_PERMISSION_RUN_TABLE}(username, queried_at DESC)`);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_${QUERY_PERMISSION_RUN_ITEM_TABLE}_run_id ON ${QUERY_PERMISSION_RUN_ITEM_TABLE}(run_id, id DESC)`);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_${QUERY_PERMISSION_RUN_ITEM_TABLE}_status ON ${QUERY_PERMISSION_RUN_ITEM_TABLE}(status, run_id DESC)`);

  await pool.query(
    `
    CREATE TABLE IF NOT EXISTS ${QUERY_PERMISSION_SETTINGS_TABLE} (
      id INTEGER PRIMARY KEY,
      add_concurrency INTEGER NOT NULL,
      add_max_retries INTEGER NOT NULL,
      add_retry_base_ms INTEGER NOT NULL,
      detail_retention_days INTEGER NOT NULL,
      detail_cleanup_cron TEXT NOT NULL,
      updated_at TIMESTAMPTZ NOT NULL,
      updated_by TEXT
    )
    `
  );

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
    CREATE TABLE IF NOT EXISTS ${PM_CUSTOMER_TABLE} (
      id BIGSERIAL PRIMARY KEY,
      code TEXT NOT NULL UNIQUE,
      name TEXT NOT NULL,
      metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
      created_at TIMESTAMPTZ NOT NULL,
      updated_at TIMESTAMPTZ NOT NULL
    )
    `
  );

  await pool.query(
    `
    CREATE TABLE IF NOT EXISTS ${PM_ENVIRONMENT_TABLE} (
      id BIGSERIAL PRIMARY KEY,
      customer_id BIGINT NOT NULL UNIQUE REFERENCES ${PM_CUSTOMER_TABLE}(id) ON DELETE CASCADE,
      name TEXT NOT NULL,
      metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
      created_at TIMESTAMPTZ NOT NULL,
      updated_at TIMESTAMPTZ NOT NULL
    )
    `
  );

  await pool.query(
    `
    CREATE TABLE IF NOT EXISTS ${PM_SERVER_TABLE} (
      id BIGSERIAL PRIMARY KEY,
      environment_id BIGINT NOT NULL REFERENCES ${PM_ENVIRONMENT_TABLE}(id) ON DELETE CASCADE,
      server_key TEXT NOT NULL UNIQUE,
      name TEXT NOT NULL,
      host TEXT NOT NULL,
      site_code TEXT,
      metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
      created_at TIMESTAMPTZ NOT NULL,
      updated_at TIMESTAMPTZ NOT NULL
    )
    `
  );

  await pool.query(
    `
    CREATE TABLE IF NOT EXISTS ${PM_APPLICATION_TABLE} (
      id BIGSERIAL PRIMARY KEY,
      server_id BIGINT NOT NULL REFERENCES ${PM_SERVER_TABLE}(id) ON DELETE CASCADE,
      app_type TEXT NOT NULL,
      app_name TEXT NOT NULL,
      service_name TEXT,
      collector_profile JSONB NOT NULL DEFAULT '{}'::jsonb,
      metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
      enabled BOOLEAN NOT NULL DEFAULT true,
      created_at TIMESTAMPTZ NOT NULL,
      updated_at TIMESTAMPTZ NOT NULL,
      UNIQUE (server_id, app_type, app_name)
    )
    `
  );

  await pool.query(
    `
    CREATE TABLE IF NOT EXISTS ${PM_AGENT_TABLE} (
      id BIGSERIAL PRIMARY KEY,
      agent_key TEXT NOT NULL UNIQUE,
      server_id BIGINT REFERENCES ${PM_SERVER_TABLE}(id) ON DELETE SET NULL,
      site_code TEXT,
      capabilities JSONB NOT NULL DEFAULT '{}'::jsonb,
      status TEXT NOT NULL DEFAULT 'ONLINE',
      last_seen_at TIMESTAMPTZ,
      created_at TIMESTAMPTZ NOT NULL,
      updated_at TIMESTAMPTZ NOT NULL
    )
    `
  );

  await pool.query(
    `
    CREATE TABLE IF NOT EXISTS ${PM_SNAPSHOT_TABLE} (
      id BIGSERIAL PRIMARY KEY,
      customer_id BIGINT NOT NULL REFERENCES ${PM_CUSTOMER_TABLE}(id) ON DELETE CASCADE,
      environment_id BIGINT NOT NULL REFERENCES ${PM_ENVIRONMENT_TABLE}(id) ON DELETE CASCADE,
      server_id BIGINT NOT NULL REFERENCES ${PM_SERVER_TABLE}(id) ON DELETE CASCADE,
      application_id BIGINT REFERENCES ${PM_APPLICATION_TABLE}(id) ON DELETE SET NULL,
      collected_at TIMESTAMPTZ NOT NULL,
      trigger_type TEXT NOT NULL,
      source_agent TEXT NOT NULL,
      snapshot_json JSONB NOT NULL,
      report_txt TEXT,
      hash_sha256 TEXT NOT NULL,
      created_at TIMESTAMPTZ NOT NULL
    )
    `
  );

  await pool.query(
    `
    CREATE TABLE IF NOT EXISTS ${PM_JOB_TABLE} (
      id BIGSERIAL PRIMARY KEY,
      server_id BIGINT NOT NULL REFERENCES ${PM_SERVER_TABLE}(id) ON DELETE CASCADE,
      application_id BIGINT REFERENCES ${PM_APPLICATION_TABLE}(id) ON DELETE SET NULL,
      trigger_type TEXT NOT NULL,
      requested_by TEXT,
      requested_at TIMESTAMPTZ NOT NULL,
      assigned_agent_key TEXT,
      started_at TIMESTAMPTZ,
      finished_at TIMESTAMPTZ,
      status TEXT NOT NULL,
      snapshot_id BIGINT REFERENCES ${PM_SNAPSHOT_TABLE}(id) ON DELETE SET NULL,
      error_detail TEXT,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    `
  );

  await pool.query(`CREATE INDEX IF NOT EXISTS idx_${PM_SERVER_TABLE}_env ON ${PM_SERVER_TABLE}(environment_id)`);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_${PM_APPLICATION_TABLE}_server ON ${PM_APPLICATION_TABLE}(server_id)`);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_${PM_AGENT_TABLE}_server ON ${PM_AGENT_TABLE}(server_id)`);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_${PM_AGENT_TABLE}_seen ON ${PM_AGENT_TABLE}(last_seen_at DESC)`);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_${PM_JOB_TABLE}_status_time ON ${PM_JOB_TABLE}(status, requested_at ASC)`);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_${PM_JOB_TABLE}_server ON ${PM_JOB_TABLE}(server_id)`);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_${PM_SNAPSHOT_TABLE}_server_time ON ${PM_SNAPSHOT_TABLE}(server_id, collected_at DESC)`);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_${PM_SNAPSHOT_TABLE}_json ON ${PM_SNAPSHOT_TABLE} USING GIN (snapshot_json)`);

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

function normalizeCode(value, fallback = 'unknown') {
  const normalized = String(value || fallback)
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9._-]+/g, '-')
    .replace(/^-+|-+$/g, '');
  return normalized || fallback;
}

function asText(value, fallback = '') {
  const text = String(value ?? fallback).trim();
  return text || String(fallback || '').trim();
}

function toSafeJson(value, fallback = {}) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    return fallback;
  }
  return value;
}

function hashSnapshot(payload) {
  const serialized = JSON.stringify(payload || {});
  return crypto.createHash('sha256').update(serialized).digest('hex');
}

function agentAuthMiddleware(req, res, next) {
  if (!PM_AGENT_SHARED_TOKEN) {
    return res.status(503).json({ message: 'PM agent token is not configured' });
  }

  const candidate = String(
    req.headers['x-agent-token']
    || req.headers['x-api-token']
    || (req.headers.authorization || '').replace(/^Bearer\s+/i, '')
    || ''
  ).trim();

  if (!candidate || candidate !== PM_AGENT_SHARED_TOKEN) {
    return res.status(401).json({ message: 'Invalid agent token' });
  }

  return next();
}

async function upsertPmRegistryHierarchy(input = {}, updatedBy = 'system') {
  const customerInput = toSafeJson(input.customer, {});
  const environmentInput = toSafeJson(input.environment, {});
  const serverInput = toSafeJson(input.server, {});
  const applications = Array.isArray(input.applications) ? input.applications : [];

  const customerCode = normalizeCode(customerInput.code || customerInput.name || 'customer');
  const customerName = asText(customerInput.name, customerCode);
  const environmentName = asText(environmentInput.name, 'prod');
  const serverKey = normalizeCode(serverInput.serverKey || serverInput.host || serverInput.name || `${customerCode}-${environmentName}`);
  const serverHost = asText(serverInput.host, serverKey);
  const serverName = asText(serverInput.name, serverHost);

  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    const customerRow = await client.query(
      `
      INSERT INTO ${PM_CUSTOMER_TABLE} (code, name, metadata, created_at, updated_at)
      VALUES ($1, $2, $3::jsonb, $4, $5)
      ON CONFLICT (code)
      DO UPDATE SET name = EXCLUDED.name, metadata = EXCLUDED.metadata, updated_at = EXCLUDED.updated_at
      RETURNING id, code, name
      `,
      [customerCode, customerName, JSON.stringify(toSafeJson(customerInput.metadata, {})), getNowIso(), getNowIso()]
    );
    const customerId = customerRow.rows[0].id;

    const environmentRow = await client.query(
      `
      INSERT INTO ${PM_ENVIRONMENT_TABLE} (customer_id, name, metadata, created_at, updated_at)
      VALUES ($1, $2, $3::jsonb, $4, $5)
      ON CONFLICT (customer_id)
      DO UPDATE SET name = EXCLUDED.name, metadata = EXCLUDED.metadata, updated_at = EXCLUDED.updated_at
      RETURNING id, customer_id, name
      `,
      [customerId, environmentName, JSON.stringify(toSafeJson(environmentInput.metadata, {})), getNowIso(), getNowIso()]
    );
    const environmentId = environmentRow.rows[0].id;

    const serverRow = await client.query(
      `
      INSERT INTO ${PM_SERVER_TABLE} (environment_id, server_key, name, host, site_code, metadata, created_at, updated_at)
      VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7, $8)
      ON CONFLICT (server_key)
      DO UPDATE SET
        environment_id = EXCLUDED.environment_id,
        name = EXCLUDED.name,
        host = EXCLUDED.host,
        site_code = EXCLUDED.site_code,
        metadata = EXCLUDED.metadata,
        updated_at = EXCLUDED.updated_at
      RETURNING id, environment_id, server_key, name, host, site_code
      `,
      [
        environmentId,
        serverKey,
        serverName,
        serverHost,
        asText(serverInput.siteCode, 'default-site'),
        JSON.stringify(toSafeJson(serverInput.metadata, {})),
        getNowIso(),
        getNowIso()
      ]
    );
    const serverId = serverRow.rows[0].id;

    const appRows = [];
    for (const app of applications) {
      const appType = normalizeCode(app?.appType || app?.type || 'other');
      const appName = asText(app?.appName || app?.name, appType);
      const serviceName = asText(app?.serviceName, '');
      const profile = toSafeJson(app?.collectorProfile, {});
      const metadata = toSafeJson(app?.metadata, {});

      const inserted = await client.query(
        `
        INSERT INTO ${PM_APPLICATION_TABLE}
          (server_id, app_type, app_name, service_name, collector_profile, metadata, enabled, created_at, updated_at)
        VALUES
          ($1, $2, $3, $4, $5::jsonb, $6::jsonb, $7, $8, $9)
        ON CONFLICT (server_id, app_type, app_name)
        DO UPDATE SET
          service_name = EXCLUDED.service_name,
          collector_profile = EXCLUDED.collector_profile,
          metadata = EXCLUDED.metadata,
          enabled = EXCLUDED.enabled,
          updated_at = EXCLUDED.updated_at
        RETURNING id, server_id, app_type, app_name, service_name, collector_profile, metadata, enabled
        `,
        [serverId, appType, appName, serviceName || null, JSON.stringify(profile), JSON.stringify(metadata), app?.enabled !== false, getNowIso(), getNowIso()]
      );
      appRows.push(inserted.rows[0]);
    }

    await client.query('COMMIT');

    await safeAddAuditEvent({
      serviceName: SERVICE_PM,
      username: updatedBy,
      actionType: 'PM_REGISTRY_UPSERT',
      status: 'SUCCESS',
      message: 'PM registry updated',
      entityType: 'pm_server',
      entityId: String(serverId),
      metadata: {
        customer_id: customerId,
        environment_id: environmentId,
        applications: appRows.length
      }
    });

    return {
      customer: customerRow.rows[0],
      environment: environmentRow.rows[0],
      server: serverRow.rows[0],
      applications: appRows
    };
  } catch (error) {
    await client.query('ROLLBACK').catch(() => {});
    throw error;
  } finally {
    client.release();
  }
}

async function createPmJob({ serverId, applicationId, triggerType = 'MANUAL', requestedBy = 'system', requestedAt = getNowIso() }) {
  const result = await pool.query(
    `
    INSERT INTO ${PM_JOB_TABLE}
      (server_id, application_id, trigger_type, requested_by, requested_at, status)
    VALUES
      ($1, $2, $3, $4, $5, 'PENDING')
    RETURNING id, server_id, application_id, trigger_type, requested_by, requested_at, status
    `,
    [serverId, applicationId || null, triggerType, requestedBy, requestedAt]
  );
  return result.rows[0];
}

async function listPmCenterServers() {
  const result = await pool.query(
    `
    SELECT
      s.id AS server_id,
      e.name AS env,
      s.server_key,
      s.name AS server_name,
      s.host AS server_ip,
      CASE
        WHEN LOWER(COALESCE(s.metadata->>'pm_enabled', '')) IN ('true', 'false') THEN (s.metadata->>'pm_enabled')::boolean
        ELSE true
      END AS pm_enabled,
      ag.agent_key,
      ag.status AS agent_status,
      ag.last_seen_at
    FROM ${PM_SERVER_TABLE} s
    JOIN ${PM_ENVIRONMENT_TABLE} e ON e.id = s.environment_id
    LEFT JOIN LATERAL (
      SELECT agent_key, status, last_seen_at
      FROM ${PM_AGENT_TABLE} ag
      WHERE ag.server_id = s.id
      ORDER BY ag.last_seen_at DESC NULLS LAST, ag.id DESC
      LIMIT 1
    ) ag ON true
    ORDER BY e.name ASC, s.name ASC, s.id ASC
    `
  );

  return result.rows.map((row) => ({
    server_id: row.server_id,
    env: row.env,
    server_key: row.server_key,
    server_name: row.server_name,
    server_ip: row.server_ip,
    status: row.pm_enabled ? 'Active' : 'Stop',
    pm_enabled: row.pm_enabled,
    agent_key: row.agent_key || null,
    agent_status: row.agent_status || null,
    last_seen_at: row.last_seen_at || null
  }));
}

async function setPmServerEnabled(serverId, enabled) {
  const result = await pool.query(
    `
    UPDATE ${PM_SERVER_TABLE}
    SET metadata = jsonb_set(COALESCE(metadata, '{}'::jsonb), '{pm_enabled}', to_jsonb($1::boolean), true),
        updated_at = $2
    WHERE id = $3
    RETURNING id
    `,
    [Boolean(enabled), getNowIso(), serverId]
  );

  return result.rows[0] || null;
}

async function setPmServerEnabledAll(enabled) {
  const result = await pool.query(
    `
    UPDATE ${PM_SERVER_TABLE}
    SET metadata = jsonb_set(COALESCE(metadata, '{}'::jsonb), '{pm_enabled}', to_jsonb($1::boolean), true),
        updated_at = $2
    `,
    [Boolean(enabled), getNowIso()]
  );

  return Number(result.rowCount || 0);
}

async function isPmServerEnabled(serverId) {
  const result = await pool.query(
    `
    SELECT
      CASE
        WHEN LOWER(COALESCE(metadata->>'pm_enabled', '')) IN ('true', 'false') THEN (metadata->>'pm_enabled')::boolean
        ELSE true
      END AS pm_enabled
    FROM ${PM_SERVER_TABLE}
    WHERE id = $1
    `,
    [serverId]
  );

  if (!result.rows[0]) {
    return null;
  }
  return result.rows[0].pm_enabled !== false;
}

async function queueManualPmJobsForActiveServers(requestedBy, targetServerId = null) {
  const params = [];
  const conditions = [
    `
    CASE
      WHEN LOWER(COALESCE(s.metadata->>'pm_enabled', '')) IN ('true', 'false') THEN (s.metadata->>'pm_enabled')::boolean
      ELSE true
    END = true
    `
  ];

  if (targetServerId) {
    params.push(targetServerId);
    conditions.push(`s.id = $${params.length}`);
  }

  const where = conditions.length ? `WHERE ${conditions.join(' AND ')}` : '';
  const result = await pool.query(
    `
    SELECT s.id AS server_id
    FROM ${PM_SERVER_TABLE} s
    ${where}
    ORDER BY s.id ASC
    `,
    params
  );

  if (!result.rows.length) {
    return [];
  }

  const jobs = [];
  for (const row of result.rows) {
    const job = await createPmJob({
      serverId: row.server_id,
      applicationId: null,
      triggerType: 'MANUAL',
      requestedBy
    });
    jobs.push(job);
  }

  return jobs;
}

async function registerOrUpdateAgent({ agentKey, serverId = null, siteCode = 'default-site', capabilities = {} }) {
  const key = normalizeCode(agentKey || 'agent');
  const result = await pool.query(
    `
    INSERT INTO ${PM_AGENT_TABLE} (agent_key, server_id, site_code, capabilities, last_seen_at, status, created_at, updated_at)
    VALUES ($1, $2, $3, $4::jsonb, $5, 'ONLINE', $6, $7)
    ON CONFLICT (agent_key)
    DO UPDATE SET
      server_id = COALESCE(EXCLUDED.server_id, ${PM_AGENT_TABLE}.server_id),
      site_code = EXCLUDED.site_code,
      capabilities = EXCLUDED.capabilities,
      last_seen_at = EXCLUDED.last_seen_at,
      status = 'ONLINE',
      updated_at = EXCLUDED.updated_at
    RETURNING id, agent_key, server_id, site_code, capabilities, status, last_seen_at
    `,
    [key, serverId, asText(siteCode, 'default-site'), JSON.stringify(toSafeJson(capabilities, {})), getNowIso(), getNowIso(), getNowIso()]
  );
  return result.rows[0];
}

async function heartbeatAgent(agentKey) {
  const key = normalizeCode(agentKey || 'agent');
  const updated = await pool.query(
    `
    UPDATE ${PM_AGENT_TABLE}
    SET last_seen_at = $1, status = 'ONLINE', updated_at = $2
    WHERE agent_key = $3
    RETURNING id, agent_key, server_id, site_code, capabilities, status, last_seen_at
    `,
    [getNowIso(), getNowIso(), key]
  );
  if (!updated.rows[0]) {
    return registerOrUpdateAgent({ agentKey: key });
  }
  return updated.rows[0];
}

async function claimNextPmJob(agentKey) {
  const key = normalizeCode(agentKey || 'agent');
  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    const agentResult = await client.query(
      `SELECT id, agent_key, server_id FROM ${PM_AGENT_TABLE} WHERE agent_key = $1 FOR UPDATE`,
      [key]
    );

    if (!agentResult.rows[0]) {
      await client.query('ROLLBACK');
      return null;
    }

    const agent = agentResult.rows[0];
    const jobResult = await client.query(
      `
      SELECT id, server_id, application_id, trigger_type, requested_by, requested_at
      FROM ${PM_JOB_TABLE}
      WHERE status = 'PENDING'
        AND (server_id = $1 OR $1 IS NULL)
      ORDER BY requested_at ASC, id ASC
      FOR UPDATE SKIP LOCKED
      LIMIT 1
      `,
      [agent.server_id]
    );

    const job = jobResult.rows[0];
    if (!job) {
      await client.query('COMMIT');
      return null;
    }

    await client.query(
      `
      UPDATE ${PM_JOB_TABLE}
      SET status = 'RUNNING', assigned_agent_key = $1, started_at = $2, updated_at = $3
      WHERE id = $4
      `,
      [key, getNowIso(), getNowIso(), job.id]
    );

    await client.query('COMMIT');
    return {
      ...job,
      assigned_agent_key: key,
      status: 'RUNNING'
    };
  } catch (error) {
    await client.query('ROLLBACK').catch(() => {});
    throw error;
  } finally {
    client.release();
  }
}

async function completePmJobWithSnapshot({ jobId, agentKey, snapshot, reportTxt = '' }) {
  const safeSnapshot = toSafeJson(snapshot, {});
  const snapshotHash = hashSnapshot(safeSnapshot);
  const now = getNowIso();

  const jobResult = await pool.query(
    `
    SELECT j.id, j.server_id, j.application_id, j.trigger_type, j.assigned_agent_key,
           s.environment_id, e.customer_id
    FROM ${PM_JOB_TABLE} j
    JOIN ${PM_SERVER_TABLE} s ON s.id = j.server_id
    JOIN ${PM_ENVIRONMENT_TABLE} e ON e.id = s.environment_id
    WHERE j.id = $1
    `,
    [jobId]
  );
  const job = jobResult.rows[0];
  if (!job) {
    throw new Error('PM job not found');
  }

  if (job.assigned_agent_key && normalizeCode(job.assigned_agent_key) !== normalizeCode(agentKey)) {
    throw new Error('PM job is assigned to another agent');
  }

  const snapshotResult = await pool.query(
    `
    INSERT INTO ${PM_SNAPSHOT_TABLE}
      (customer_id, environment_id, server_id, application_id, collected_at, trigger_type, source_agent, snapshot_json, report_txt, hash_sha256, created_at)
    VALUES
      ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9, $10, $11)
    RETURNING id, hash_sha256, collected_at
    `,
    [job.customer_id, job.environment_id, job.server_id, job.application_id, now, job.trigger_type, normalizeCode(agentKey), JSON.stringify(safeSnapshot), String(reportTxt || ''), snapshotHash, now]
  );

  await pool.query(
    `
    UPDATE ${PM_JOB_TABLE}
    SET status = 'SUCCESS', finished_at = $1, snapshot_id = $2, error_detail = NULL, updated_at = $3
    WHERE id = $4
    `,
    [now, snapshotResult.rows[0].id, now, jobId]
  );

  return snapshotResult.rows[0];
}

async function failPmJob({ jobId, errorDetail = 'Unknown error' }) {
  await pool.query(
    `
    UPDATE ${PM_JOB_TABLE}
    SET status = 'FAILED', finished_at = $1, error_detail = $2, updated_at = $3
    WHERE id = $4
    `,
    [getNowIso(), String(errorDetail || 'Unknown error').slice(0, 4000), getNowIso(), jobId]
  );
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

async function parseGroupImportRows(filePath) {
  const rows = await readFirstSheetRows(filePath);
  return rows.map((row, index) => ({
    lineNo: index + 2,
    groupId: normalizeGroupId(row.group_id || row.Group_ID || row.groupId || row.GROUP_ID),
    groupDisplayName: String(row.group_display_name || row.Group_Display_Name || row.groupDisplayName || '').trim(),
    userId: String(row.user_id || row.User_ID || row.userId || '').trim(),
    action: String(row.action || row.Action || 'ADD').trim().toUpperCase()
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
    const rows = await parseGroupImportRows(inputPath);
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
    const safeName = `${taskId}_${toSafeFilename(originalName)}`;
    const storedFilename = buildStoredUploadPath(SERVICE_PERMISSION_IMPORT, safeName);

    await writeStoredUploadFile(storedFilename, file.buffer);

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
    const safeName = `${taskId}_${toSafeFilename(originalName)}`;
    const storedFilename = buildStoredUploadPath(SERVICE_GROUP_MEMBER_IMPORT, safeName);

    await writeStoredUploadFile(storedFilename, file.buffer);
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
    const safeName = `${taskId}_${toSafeFilename(originalName)}`;
    const storedFilename = buildStoredUploadPath(SERVICE_USER_CSV_IMPORT, safeName);

    await writeStoredUploadFile(storedFilename, file.buffer);
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

registerQueryPermissionRoutes(app, {
  axios,
  pool,
  getPool: () => pool,
  authMiddleware,
  upload,
  fsp,
  path,
  fs,
  UPLOAD_DIR,
  QUERY_PERMISSION_TEMPLATE_TABLE,
  QUERY_PERMISSION_RUN_TABLE,
  QUERY_PERMISSION_RUN_ITEM_TABLE,
  SERVICE_QUERY_ADD_PERMISSION,
  safeAddAuditEvent,
  alfrescoAuthProvider,
  ALFRESCO_BASE_URL,
  ALFRESCO_TIMEOUT_MS,
  buildPermissionQueryDefinition,
  normalizeAftsQuery,
  normalizePermissionTargetType,
  buildPermissionSearchQuery,
  formatAlfrescoError,
  toSafeFilename,
  getNowIso,
  processPermissionQueryRun,
  processPermissionAddRun,
  processPermissionRetryFailedRun,
  getQueryPermissionRuntimeSettings,
  saveQueryPermissionSettings,
  toPositiveInt,
  truncateMessage
});

app.post('/api/query-sizing/runs', authMiddleware, async (req, res) => {
  const user = req.user?.username || req.user?.sub || 'unknown';
  const normalizedQuery = normalizeAftsQuery(req.body?.query);
  const queryText = normalizedQuery.normalized;

  if (!queryText) {
    return res.status(400).json({ message: 'query is required' });
  }

  if (queryText.length > 8000) {
    return res.status(400).json({ message: 'query is too long (max 8000 chars)' });
  }

  const now = getNowIso();
  const inserted = await pool.query(
    `
    INSERT INTO ${QUERY_SIZING_TABLE}
      (queried_at, username, query_text, status, total_files, total_size_bytes, total_size_mb, total_size_gb, message, created_at, updated_at)
    VALUES
      ($1, $2, $3, $4, 0, 0, 0, 0, $5, $6, $7)
    RETURNING id, queried_at, username, query_text, status, total_files, total_size_bytes, total_size_mb, total_size_gb, message, created_at, updated_at, finished_at
    `,
    [
      now,
      user,
      queryText,
      'IN_PROGRESS',
      normalizedQuery.hadJsonEscapedQuotes
        ? `Queued. Backend normalized escaped quotes (\\\" -> \") and uses fixed paging maxItems=${QUERY_SIZING_MAX_ITEMS}`
        : `Queued. Backend uses fixed paging maxItems=${QUERY_SIZING_MAX_ITEMS}`,
      now,
      now
    ]
  );

  const run = inserted.rows[0];

  setImmediate(() => {
    processQuerySizingRun(run.id).catch((error) => {
      console.error('Unexpected query sizing task error', error);
    });
  });

  return res.status(202).json({
    message: 'Query sizing run accepted',
    run,
    maxItems: QUERY_SIZING_MAX_ITEMS,
    warnings: normalizedQuery.hadJsonEscapedQuotes
      ? ['Query looked JSON-escaped. Backend normalized \\\" to ".']
      : []
  });
});

app.get('/api/query-sizing/runs/:id', authMiddleware, async (req, res) => {
  const runId = Number(req.params.id);
  if (!Number.isFinite(runId) || runId <= 0) {
    return res.status(400).json({ message: 'Invalid run id' });
  }

  const result = await pool.query(
    `
    SELECT
      id,
      queried_at,
      username,
      query_text,
      status,
      total_files,
      total_size_bytes,
      total_size_mb::float8 AS total_size_mb,
      total_size_gb::float8 AS total_size_gb,
      message,
      created_at,
      updated_at,
      finished_at
    FROM ${QUERY_SIZING_TABLE}
    WHERE id = $1
    `,
    [runId]
  );

  const run = result.rows[0];
  if (!run) {
    return res.status(404).json({ message: 'Run not found' });
  }

  return res.json({ run });
});

app.get('/api/reports/query-sizing', authMiddleware, async (req, res) => {
  const pageSize = [10, 30, 100].includes(Number(req.query.pageSize)) ? Number(req.query.pageSize) : 30;
  const page = toPositiveInt(req.query.page, 1);
  const offset = (page - 1) * pageSize;

  const countResult = await pool.query(`SELECT COUNT(*)::bigint AS total FROM ${QUERY_SIZING_TABLE}`);
  const dataResult = await pool.query(
    `
    SELECT
      id,
      queried_at,
      username,
      query_text,
      status,
      total_files,
      total_size_bytes,
      total_size_mb::float8 AS total_size_mb,
      total_size_gb::float8 AS total_size_gb,
      message,
      created_at,
      updated_at,
      finished_at
    FROM ${QUERY_SIZING_TABLE}
    ORDER BY id DESC
    LIMIT $1 OFFSET $2
    `,
    [pageSize, offset]
  );

  return res.json({
    items: dataResult.rows,
    total: Number(countResult.rows[0]?.total || 0),
    page,
    pageSize,
    maxItems: QUERY_SIZING_MAX_ITEMS
  });
});

app.get('/api/reports/query-sizing/export.csv', authMiddleware, async (req, res) => {
  const result = await pool.query(
    `
    SELECT
      queried_at,
      query_text,
      username,
      total_files,
      total_size_mb::float8 AS total_size_mb,
      total_size_gb::float8 AS total_size_gb,
      status,
      message
    FROM ${QUERY_SIZING_TABLE}
    ORDER BY id DESC
    `
  );

  const rows = [
    ['Query Date', 'Query Command', 'User', 'Total Files', 'Total size (MB)', 'Total size (GB)', 'Status', 'Message status']
  ];

  for (const item of result.rows) {
    rows.push([
      item.queried_at || '',
      item.query_text || '',
      item.username || '',
      item.total_files ?? 0,
      item.total_size_mb ?? 0,
      item.total_size_gb ?? 0,
      item.status || '',
      item.message || ''
    ]);
  }

  const dateToken = new Date().toISOString().replace(/[:.]/g, '-');
  const filename = `query-sizing-report-${dateToken}.csv`;

  res.setHeader('Content-Type', 'text/csv; charset=utf-8');
  res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
  return res.send(`\uFEFF${toCsv(rows)}\n`);
});

app.post('/api/reports/query-sizing/export.csv', authMiddleware, async (req, res) => {
  const rawIds = Array.isArray(req.body?.ids) ? req.body.ids : [];
  const ids = rawIds
    .map((value) => Number(value))
    .filter((value) => Number.isFinite(value) && value > 0)
    .map((value) => Math.floor(value));

  if (!ids.length) {
    return res.status(400).json({ message: 'ids is required and must contain at least one valid id' });
  }

  const result = await pool.query(
    `
    SELECT
      queried_at,
      query_text,
      username,
      total_files,
      total_size_mb::float8 AS total_size_mb,
      total_size_gb::float8 AS total_size_gb,
      status,
      message
    FROM ${QUERY_SIZING_TABLE}
    WHERE id = ANY($1::bigint[])
    ORDER BY id DESC
    `,
    [ids]
  );

  const rows = [
    ['Query Date', 'Query Command', 'User', 'Total Files', 'Total size (MB)', 'Total size (GB)', 'Status', 'Message status']
  ];

  for (const item of result.rows) {
    rows.push([
      item.queried_at || '',
      item.query_text || '',
      item.username || '',
      item.total_files ?? 0,
      item.total_size_mb ?? 0,
      item.total_size_gb ?? 0,
      item.status || '',
      item.message || ''
    ]);
  }

  const dateToken = new Date().toISOString().replace(/[:.]/g, '-');
  const filename = `query-sizing-report-selected-${dateToken}.csv`;

  res.setHeader('Content-Type', 'text/csv; charset=utf-8');
  res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
  return res.send(`\uFEFF${toCsv(rows)}\n`);
});

app.delete('/api/reports/query-sizing/:id', authMiddleware, async (req, res) => {
  const runId = Number(req.params.id);
  if (!Number.isFinite(runId) || runId <= 0) {
    return res.status(400).json({ message: 'Invalid run id' });
  }

  const deleted = await pool.query(`DELETE FROM ${QUERY_SIZING_TABLE} WHERE id = $1 RETURNING id`, [runId]);
  if (!deleted.rows[0]) {
    return res.status(404).json({ message: 'Run not found' });
  }

  return res.json({ message: 'Query sizing report deleted', id: runId });
});

app.get('/api/pm/registry/tree', authMiddleware, async (req, res) => {
  const result = await pool.query(
    `
    SELECT
      c.id AS customer_id,
      c.code AS customer_code,
      c.name AS customer_name,
      e.id AS environment_id,
      e.name AS environment_name,
      s.id AS server_id,
      s.server_key,
      s.name AS server_name,
      s.host AS server_host,
      s.site_code,
      a.id AS application_id,
      a.app_type,
      a.app_name,
      a.service_name,
      a.collector_profile,
      a.metadata,
      a.enabled
    FROM ${PM_CUSTOMER_TABLE} c
    JOIN ${PM_ENVIRONMENT_TABLE} e ON e.customer_id = c.id
    JOIN ${PM_SERVER_TABLE} s ON s.environment_id = e.id
    LEFT JOIN ${PM_APPLICATION_TABLE} a ON a.server_id = s.id
    ORDER BY c.name, e.name, s.name, a.app_type, a.app_name
    `
  );

  return res.json({ items: result.rows, count: result.rows.length });
});

app.put('/api/pm/registry/upsert', authMiddleware, async (req, res) => {
  const user = req.user?.username || req.user?.sub || 'unknown';
  try {
    const data = await upsertPmRegistryHierarchy(req.body || {}, user);
    return res.json({ message: 'PM registry updated', ...data });
  } catch (error) {
    return res.status(400).json({ message: error?.message || 'Cannot update PM registry' });
  }
});

app.post('/api/pm/jobs/dispatch', authMiddleware, async (req, res) => {
  const user = req.user?.username || req.user?.sub || 'unknown';
  const serverId = Number(req.body?.serverId);
  const applicationId = req.body?.applicationId ? Number(req.body.applicationId) : null;
  const triggerType = asText(req.body?.triggerType, 'MANUAL').toUpperCase();

  if (!Number.isFinite(serverId) || serverId <= 0) {
    return res.status(400).json({ message: 'serverId is required' });
  }

  const serverExists = await pool.query(`SELECT id FROM ${PM_SERVER_TABLE} WHERE id = $1`, [serverId]);
  if (!serverExists.rows[0]) {
    return res.status(404).json({ message: 'Server not found in PM registry' });
  }

  const enabled = await isPmServerEnabled(serverId);
  if (enabled === false) {
    return res.status(409).json({ message: 'Server is paused. Please start server before dispatch.' });
  }

  if (applicationId) {
    const appExists = await pool.query(`SELECT id FROM ${PM_APPLICATION_TABLE} WHERE id = $1 AND server_id = $2`, [applicationId, serverId]);
    if (!appExists.rows[0]) {
      return res.status(404).json({ message: 'Application not found for selected server' });
    }
  }

  const job = await createPmJob({
    serverId,
    applicationId,
    triggerType,
    requestedBy: user
  });

  await safeAddAuditEvent({
    serviceName: SERVICE_PM,
    username: user,
    actionType: 'PM_DISPATCH_JOB',
    status: 'SUCCESS',
    message: 'PM job dispatched',
    entityType: 'pm_job',
    entityId: String(job.id),
    metadata: {
      server_id: serverId,
      application_id: applicationId,
      trigger_type: triggerType
    }
  });

  return res.status(202).json({ message: 'PM job queued', job });
});

app.get('/api/pm/center/servers', authMiddleware, async (req, res) => {
  const items = await listPmCenterServers();
  return res.json({ items, count: items.length });
});

app.post('/api/pm/center/servers', authMiddleware, async (req, res) => {
  const user = req.user?.username || req.user?.sub || 'unknown';
  const { config } = await getPmConfig();

  const serverIp = asText(req.body?.serverIp, '').trim();
  if (!serverIp) {
    return res.status(400).json({ message: 'serverIp is required' });
  }

  const serverName = asText(req.body?.serverName, serverIp);
  const serverKey = normalizeCode(req.body?.serverKey || serverIp);
  const payload = {
    customer: {
      code: asText(config.customer, 'customer')
    },
    environment: {
      name: asText(config.environment, 'prod')
    },
    server: {
      serverKey,
      name: serverName,
      host: serverIp,
      siteCode: asText(req.body?.siteCode, 'default-site'),
      metadata: {
        pm_enabled: true
      }
    },
    applications: []
  };

  try {
    const data = await upsertPmRegistryHierarchy(payload, user);
    return res.status(201).json({
      message: 'Server added',
      server: data.server
    });
  } catch (error) {
    return res.status(400).json({ message: error?.message || 'Cannot add server' });
  }
});

app.patch('/api/pm/center/servers/:id/state', authMiddleware, async (req, res) => {
  const serverId = Number(req.params.id);
  const enabled = Boolean(req.body?.enabled);

  if (!Number.isFinite(serverId) || serverId <= 0) {
    return res.status(400).json({ message: 'Invalid server id' });
  }

  const updated = await setPmServerEnabled(serverId, enabled);
  if (!updated) {
    return res.status(404).json({ message: 'Server not found' });
  }

  return res.json({
    message: enabled ? 'Server started' : 'Server paused',
    serverId,
    enabled
  });
});

app.post('/api/pm/center/servers/state-all', authMiddleware, async (req, res) => {
  const enabled = Boolean(req.body?.enabled);
  const affected = await setPmServerEnabledAll(enabled);
  return res.json({
    message: enabled ? 'All servers started' : 'All servers paused',
    affected,
    enabled
  });
});

app.delete('/api/pm/center/servers/:id', authMiddleware, async (req, res) => {
  const serverId = Number(req.params.id);
  if (!Number.isFinite(serverId) || serverId <= 0) {
    return res.status(400).json({ message: 'Invalid server id' });
  }

  const deleted = await pool.query(`DELETE FROM ${PM_SERVER_TABLE} WHERE id = $1 RETURNING id`, [serverId]);
  if (!deleted.rows[0]) {
    return res.status(404).json({ message: 'Server not found' });
  }

  return res.json({ message: 'Server deleted', serverId });
});

app.post('/api/pm/center/start-manual', authMiddleware, async (req, res) => {
  const user = req.user?.username || req.user?.sub || 'unknown';
  const serverId = req.body?.serverId ? Number(req.body.serverId) : null;

  if (serverId !== null && (!Number.isFinite(serverId) || serverId <= 0)) {
    return res.status(400).json({ message: 'Invalid server id' });
  }

  const jobs = await queueManualPmJobsForActiveServers(user, serverId);
  if (!jobs.length) {
    return res.status(404).json({ message: 'No active server found for manual PM' });
  }

  return res.status(202).json({
    message: 'Manual PM jobs queued',
    count: jobs.length,
    jobs
  });
});

app.get('/api/pm/center/reports', authMiddleware, async (req, res) => {
  const pageSize = [30, 50, 100].includes(Number(req.query.pageSize)) ? Number(req.query.pageSize) : 30;
  const page = Math.max(1, Number(req.query.page || 1));
  const offset = (page - 1) * pageSize;
  const serverId = req.query.serverId ? Number(req.query.serverId) : null;

  const params = [];
  const conditions = [];
  if (serverId) {
    params.push(serverId);
    conditions.push(`j.server_id = $${params.length}`);
  }

  const where = conditions.length ? `WHERE ${conditions.join(' AND ')}` : '';
  const countSql = `
    SELECT COUNT(*)::bigint AS total
    FROM ${PM_JOB_TABLE} j
    ${where}
  `;
  const countResult = await pool.query(countSql, params);

  params.push(pageSize);
  params.push(offset);
  const dataSql = `
    SELECT
      j.id AS job_id,
      j.server_id,
      s.name AS server_name,
      s.server_key,
      c.code AS customer_code,
      e.name AS env,
      COALESCE(sn.collected_at, j.finished_at, j.started_at, j.requested_at) AS pm_date,
      j.status,
      j.error_detail,
      sn.id AS snapshot_id
    FROM ${PM_JOB_TABLE} j
    JOIN ${PM_SERVER_TABLE} s ON s.id = j.server_id
    JOIN ${PM_ENVIRONMENT_TABLE} e ON e.id = s.environment_id
    JOIN ${PM_CUSTOMER_TABLE} c ON c.id = e.customer_id
    LEFT JOIN ${PM_SNAPSHOT_TABLE} sn ON sn.id = j.snapshot_id
    ${where}
    ORDER BY j.id DESC
    LIMIT $${params.length - 1} OFFSET $${params.length}
  `;
  const result = await pool.query(dataSql, params);

  return res.json({
    items: result.rows,
    total: Number(countResult.rows[0]?.total || 0),
    page,
    pageSize
  });
});

app.get('/api/pm/snapshots/:id/download-json', authMiddleware, async (req, res) => {
  const snapshotId = Number(req.params.id);
  if (!Number.isFinite(snapshotId) || snapshotId <= 0) {
    return res.status(400).json({ message: 'Invalid snapshot id' });
  }

  const result = await pool.query(
    `
    SELECT sn.id, sn.snapshot_json, c.code AS customer_code, e.name AS env, s.name AS server_name, sn.collected_at
    FROM ${PM_SNAPSHOT_TABLE} sn
    JOIN ${PM_CUSTOMER_TABLE} c ON c.id = sn.customer_id
    JOIN ${PM_ENVIRONMENT_TABLE} e ON e.id = sn.environment_id
    JOIN ${PM_SERVER_TABLE} s ON s.id = sn.server_id
    WHERE sn.id = $1
    `,
    [snapshotId]
  );

  const row = result.rows[0];
  if (!row) {
    return res.status(404).json({ message: 'Snapshot not found' });
  }

  const dateToken = String(row.collected_at || '').slice(0, 10) || 'unknown-date';
  const filename = toSafeFilename(`${row.customer_code}-${row.env}-${row.server_name}-${dateToken}.json`);

  res.setHeader('Content-Type', 'application/json; charset=utf-8');
  res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
  return res.send(JSON.stringify(row.snapshot_json || {}, null, 2));
});

app.get('/api/pm/snapshots/:id/download-txt', authMiddleware, async (req, res) => {
  const snapshotId = Number(req.params.id);
  if (!Number.isFinite(snapshotId) || snapshotId <= 0) {
    return res.status(400).json({ message: 'Invalid snapshot id' });
  }

  const result = await pool.query(
    `
    SELECT sn.id, sn.report_txt, c.code AS customer_code, e.name AS env, s.name AS server_name, sn.collected_at
    FROM ${PM_SNAPSHOT_TABLE} sn
    JOIN ${PM_CUSTOMER_TABLE} c ON c.id = sn.customer_id
    JOIN ${PM_ENVIRONMENT_TABLE} e ON e.id = sn.environment_id
    JOIN ${PM_SERVER_TABLE} s ON s.id = sn.server_id
    WHERE sn.id = $1
    `,
    [snapshotId]
  );

  const row = result.rows[0];
  if (!row) {
    return res.status(404).json({ message: 'Snapshot not found' });
  }

  const dateToken = String(row.collected_at || '').slice(0, 10) || 'unknown-date';
  const filename = toSafeFilename(`${row.customer_code}-${row.env}-${row.server_name}-${dateToken}.txt`);

  res.setHeader('Content-Type', 'text/plain; charset=utf-8');
  res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
  return res.send(String(row.report_txt || ''));
});

app.get('/api/pm/jobs', authMiddleware, async (req, res) => {
  const limit = Math.min(Number(req.query.limit || 100), 500);
  const status = req.query.status ? String(req.query.status).toUpperCase() : null;

  const params = [];
  let where = '';
  if (status) {
    params.push(status);
    where = `WHERE j.status = $${params.length}`;
  }

  params.push(limit);
  const result = await pool.query(
    `
    SELECT j.id, j.server_id, s.server_key, s.name AS server_name,
           j.application_id, a.app_type, a.app_name,
           j.trigger_type, j.requested_by, j.requested_at,
           j.assigned_agent_key, j.started_at, j.finished_at,
           j.status, j.snapshot_id, j.error_detail
    FROM ${PM_JOB_TABLE} j
    JOIN ${PM_SERVER_TABLE} s ON s.id = j.server_id
    LEFT JOIN ${PM_APPLICATION_TABLE} a ON a.id = j.application_id
    ${where}
    ORDER BY j.id DESC
    LIMIT $${params.length}
    `,
    params
  );

  return res.json({ items: result.rows, count: result.rows.length });
});

app.get('/api/pm/snapshots', authMiddleware, async (req, res) => {
  const limit = Math.min(Number(req.query.limit || 100), 500);
  const serverId = req.query.server_id ? Number(req.query.server_id) : null;
  const applicationId = req.query.application_id ? Number(req.query.application_id) : null;

  const params = [];
  const conditions = [];
  if (serverId) {
    params.push(serverId);
    conditions.push(`sn.server_id = $${params.length}`);
  }
  if (applicationId) {
    params.push(applicationId);
    conditions.push(`sn.application_id = $${params.length}`);
  }

  const whereClause = conditions.length ? `WHERE ${conditions.join(' AND ')}` : '';
  params.push(limit);

  const result = await pool.query(
    `
    SELECT sn.id, sn.customer_id, c.name AS customer_name,
           sn.environment_id, e.name AS environment_name,
           sn.server_id, s.name AS server_name, s.server_key,
           sn.application_id, a.app_type, a.app_name,
           sn.collected_at, sn.trigger_type, sn.source_agent,
           sn.hash_sha256
    FROM ${PM_SNAPSHOT_TABLE} sn
    JOIN ${PM_CUSTOMER_TABLE} c ON c.id = sn.customer_id
    JOIN ${PM_ENVIRONMENT_TABLE} e ON e.id = sn.environment_id
    JOIN ${PM_SERVER_TABLE} s ON s.id = sn.server_id
    LEFT JOIN ${PM_APPLICATION_TABLE} a ON a.id = sn.application_id
    ${whereClause}
    ORDER BY sn.collected_at DESC
    LIMIT $${params.length}
    `,
    params
  );

  return res.json({ items: result.rows, count: result.rows.length });
});

app.post('/api/pm/agents/register', agentAuthMiddleware, async (req, res) => {
  try {
    const agent = await registerOrUpdateAgent({
      agentKey: req.body?.agentKey,
      serverId: req.body?.serverId ? Number(req.body.serverId) : null,
      siteCode: req.body?.siteCode,
      capabilities: req.body?.capabilities
    });
    return res.json({ message: 'Agent registered', agent });
  } catch (error) {
    return res.status(400).json({ message: error?.message || 'Cannot register agent' });
  }
});

app.post('/api/pm/agents/heartbeat', agentAuthMiddleware, async (req, res) => {
  const agentKey = req.body?.agentKey;
  if (!agentKey) {
    return res.status(400).json({ message: 'agentKey is required' });
  }
  const agent = await heartbeatAgent(agentKey);
  return res.json({ status: 'ONLINE', agent });
});

app.get('/api/pm/agents/jobs/next', agentAuthMiddleware, async (req, res) => {
  const agentKey = String(req.query.agentKey || '').trim();
  if (!agentKey) {
    return res.status(400).json({ message: 'agentKey is required' });
  }

  const job = await claimNextPmJob(agentKey);
  if (!job) {
    return res.status(204).send();
  }

  let application = null;
  if (job.application_id) {
    const appResult = await pool.query(
      `
      SELECT id, server_id, app_type, app_name, service_name, collector_profile, metadata
      FROM ${PM_APPLICATION_TABLE}
      WHERE id = $1
      `,
      [job.application_id]
    );
    application = appResult.rows[0] || null;
  }

  const serverResult = await pool.query(`SELECT id, server_key, name, host, site_code, metadata FROM ${PM_SERVER_TABLE} WHERE id = $1`, [job.server_id]);

  return res.json({
    job,
    server: serverResult.rows[0] || null,
    application
  });
});

app.post('/api/pm/agents/jobs/:id/result', agentAuthMiddleware, async (req, res) => {
  const jobId = Number(req.params.id);
  const agentKey = req.body?.agentKey;

  if (!Number.isFinite(jobId) || jobId <= 0) {
    return res.status(400).json({ message: 'Invalid job id' });
  }
  if (!agentKey) {
    return res.status(400).json({ message: 'agentKey is required' });
  }

  try {
    const snapshot = await completePmJobWithSnapshot({
      jobId,
      agentKey,
      snapshot: req.body?.snapshot,
      reportTxt: req.body?.reportTxt
    });

    return res.json({ message: 'PM job completed', snapshot });
  } catch (error) {
    return res.status(400).json({ message: error?.message || 'Cannot complete PM job' });
  }
});

app.post('/api/pm/agents/jobs/:id/fail', agentAuthMiddleware, async (req, res) => {
  const jobId = Number(req.params.id);
  if (!Number.isFinite(jobId) || jobId <= 0) {
    return res.status(400).json({ message: 'Invalid job id' });
  }

  await failPmJob({
    jobId,
    errorDetail: req.body?.errorDetail || req.body?.message || 'Agent reported failure'
  });

  return res.json({ message: 'PM job marked as failed', jobId });
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
  await loadQueryPermissionSettings();
  await refreshQueryPermissionCleanupSchedule();

  cleanupQueryPermissionData().catch((error) => {
    console.error('Initial query permission cleanup failed', error?.message || error);
  });

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
    if (typeof pmCronTask.destroy === 'function') {
      pmCronTask.destroy();
    }
  }
  if (queryPermissionCleanupCronTask) {
    queryPermissionCleanupCronTask.stop();
    if (typeof queryPermissionCleanupCronTask.destroy === 'function') {
      queryPermissionCleanupCronTask.destroy();
    }
    queryPermissionCleanupCronTask = null;
  }
  if (pool) {
    await pool.end();
  }
  process.exit(0);
});
