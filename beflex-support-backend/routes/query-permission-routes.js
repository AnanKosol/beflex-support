function mapPermissionRunItemStatus(rawStatus) {
  const status = String(rawStatus || '').toUpperCase();
  if (status === 'ADD_SUCCESS') {
    return 'success';
  }
  if (status === 'ADD_FAILED') {
    return 'failed';
  }
  return 'wait';
}

function registerQueryPermissionRoutes(app, deps) {
  const {
    axios,
    pool: initialPool,
    getPool,
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
  } = deps;

  const pool = new Proxy({}, {
    get(_target, prop) {
      const db = typeof getPool === 'function' ? getPool() : initialPool;
      if (!db) {
        throw new Error('Database pool is not initialized');
      }
      const value = db[prop];
      return typeof value === 'function' ? value.bind(db) : value;
    }
  });

  const queryPermissionSubdir = toSafeFilename(SERVICE_QUERY_ADD_PERMISSION || 'query-add-permission');
  const buildStoredFilename = (scope, fileName) => `${queryPermissionSubdir}/${scope}/${toSafeFilename(fileName)}`;
  const writeStoredFile = async (storedFilename, buffer) => {
    const fullPath = path.join(UPLOAD_DIR, storedFilename);
    await fsp.mkdir(path.dirname(fullPath), { recursive: true });
    await fsp.writeFile(fullPath, buffer);
  };

  app.get('/api/query-permission/templates', authMiddleware, async (_req, res) => {
    const result = await pool.query(
      `
      SELECT id, template_name, query_text, target_type, permission_filename, inherit_permissions, created_by, updated_by, created_at, updated_at
      FROM ${QUERY_PERMISSION_TEMPLATE_TABLE}
      ORDER BY id DESC
      `
    );

    return res.json({ items: result.rows, count: result.rows.length });
  });

  app.get('/api/query-permission/settings', authMiddleware, async (_req, res) => {
    return res.json({
      item: getQueryPermissionRuntimeSettings()
    });
  });

  app.put('/api/query-permission/settings', authMiddleware, async (req, res) => {
    const user = req.user?.username || req.user?.sub || 'unknown';
    const payload = {};

    if (Object.prototype.hasOwnProperty.call(req.body || {}, 'addConcurrency')) {
      payload.addConcurrency = req.body.addConcurrency;
    }
    if (Object.prototype.hasOwnProperty.call(req.body || {}, 'addMaxRetries')) {
      payload.addMaxRetries = req.body.addMaxRetries;
    }
    if (Object.prototype.hasOwnProperty.call(req.body || {}, 'addRetryBaseMs')) {
      payload.addRetryBaseMs = req.body.addRetryBaseMs;
    }
    if (Object.prototype.hasOwnProperty.call(req.body || {}, 'detailRetentionDays')) {
      payload.detailRetentionDays = req.body.detailRetentionDays;
    }
    if (Object.prototype.hasOwnProperty.call(req.body || {}, 'detailCleanupCron')) {
      payload.detailCleanupCron = req.body.detailCleanupCron;
    }

    const item = await saveQueryPermissionSettings(payload, user);
    await safeAddAuditEvent({
      serviceName: SERVICE_QUERY_ADD_PERMISSION,
      username: user,
      actionType: 'SETTINGS_UPDATE',
      status: 'SUCCESS',
      message: 'Query permission settings updated',
      entityType: 'query_permission_settings',
      entityId: '1',
      metadata: item
    });

    return res.json({
      message: 'Settings updated',
      item
    });
  });

  app.post('/api/query-permission/templates/test', authMiddleware, async (req, res) => {
    const rawQuery = normalizeAftsQuery(req.body?.queryText).normalized;
    const definition = buildPermissionQueryDefinition(req.body?.queryText, req.body?.targetType);

    if (!rawQuery) {
      return res.status(400).json({ message: 'queryText is required' });
    }

    const effectiveQuery = definition.effectiveQuery;
    if (!effectiveQuery) {
      return res.status(400).json({ message: 'queryText is required' });
    }

    try {
      const { authHeader } = await alfrescoAuthProvider.getValidatedServiceAuth({
        purpose: 'query add permission preview',
        formatError: formatAlfrescoError
      });

      const searchUrl = `${ALFRESCO_BASE_URL}/alfresco/api/-default-/public/search/versions/1/search`;
      const response = await axios.post(
        searchUrl,
        {
          query: {
            language: 'afts',
            query: effectiveQuery
          },
          paging: {
            maxItems: 1,
            skipCount: 0
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

      return res.json({
        totalItems: Number(response?.data?.list?.pagination?.totalItems || 0),
        queryText: definition.sanitizedQuery,
        targetType: definition.targetType,
        detectedTargetType: definition.detectedTargetType
      });
    } catch (error) {
      const reason = truncateMessage(formatAlfrescoError(error, 'Permission query preview failed'), 1000);
      return res.status(500).json({ message: reason });
    }
  });

  app.post('/api/query-permission/templates', authMiddleware, upload.single('file'), async (req, res) => {
    const user = req.user?.username || req.user?.sub || 'unknown';
    const templateName = String(req.body?.templateName || '').trim();
    const rawQuery = normalizeAftsQuery(req.body?.queryText).normalized;
    const definition = buildPermissionQueryDefinition(req.body?.queryText, req.body?.targetType);
    const queryText = definition.sanitizedQuery;
    const targetType = definition.targetType;

    if (!templateName) {
      return res.status(400).json({ message: 'templateName is required' });
    }
    if (!rawQuery) {
      return res.status(400).json({ message: 'queryText is required' });
    }

    let permissionFilename = null;
    let permissionStoredFilename = null;

    if (req.file) {
      const originalName = req.file.originalname || 'permission-template.xlsx';
      if (!originalName.toLowerCase().endsWith('.xlsx')) {
        return res.status(400).json({ message: 'Only .xlsx files are allowed' });
      }
      permissionFilename = originalName;
      permissionStoredFilename = buildStoredFilename('templates', `template_${Date.now()}_${originalName}`);
      await writeStoredFile(permissionStoredFilename, req.file.buffer);
    }

    const now = getNowIso();
    const inheritPermissions = req.body?.inheritPermissions === 'false' || req.body?.inheritPermissions === false ? false : true;
    const inserted = await pool.query(
      `
      INSERT INTO ${QUERY_PERMISSION_TEMPLATE_TABLE}
        (template_name, query_text, target_type, permission_filename, permission_stored_filename, inherit_permissions, created_by, updated_by, created_at, updated_at)
      VALUES
        ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
      RETURNING id, template_name, query_text, target_type, permission_filename, inherit_permissions, created_by, updated_by, created_at, updated_at
      `,
      [templateName, queryText, targetType, permissionFilename, permissionStoredFilename, inheritPermissions, user, user, now, now]
    );

    await safeAddAuditEvent({
      serviceName: SERVICE_QUERY_ADD_PERMISSION,
      username: user,
      actionType: 'TEMPLATE_CREATE',
      status: 'SUCCESS',
      message: `Template created: ${templateName}`,
      entityType: 'permission_template',
      entityId: String(inserted.rows[0].id),
      metadata: {
        has_permission_file: Boolean(permissionStoredFilename)
      }
    });

    return res.status(201).json({ item: inserted.rows[0] });
  });

  app.put('/api/query-permission/templates/:id', authMiddleware, upload.single('file'), async (req, res) => {
    const templateId = Number(req.params.id);
    if (!Number.isFinite(templateId) || templateId <= 0) {
      return res.status(400).json({ message: 'Invalid template id' });
    }

    const user = req.user?.username || req.user?.sub || 'unknown';
    const existingResult = await pool.query(`SELECT * FROM ${QUERY_PERMISSION_TEMPLATE_TABLE} WHERE id = $1`, [templateId]);
    const existing = existingResult.rows[0];
    if (!existing) {
      return res.status(404).json({ message: 'Template not found' });
    }

    const templateName = String(req.body?.templateName || existing.template_name).trim();
    const incomingRawQuery = req.body?.queryText ?? existing.query_text;
    const rawQuery = normalizeAftsQuery(incomingRawQuery).normalized;
    const definition = buildPermissionQueryDefinition(incomingRawQuery, req.body?.targetType || existing.target_type);
    const queryText = definition.sanitizedQuery;
    const targetType = definition.targetType;
    const removePermissionFileRaw = String(req.body?.removePermissionFile || '').toLowerCase();
    const removePermissionFile = removePermissionFileRaw === 'true' || removePermissionFileRaw === 'yes-remove';

    if (!templateName) {
      return res.status(400).json({ message: 'templateName is required' });
    }
    if (!rawQuery) {
      return res.status(400).json({ message: 'queryText is required' });
    }

    let permissionFilename = existing.permission_filename;
    let permissionStoredFilename = existing.permission_stored_filename;

    if (removePermissionFile) {
      if (permissionStoredFilename) {
        await fsp.unlink(path.join(UPLOAD_DIR, permissionStoredFilename)).catch(() => {});
      }
      permissionFilename = null;
      permissionStoredFilename = null;
    }

    if (req.file) {
      const originalName = req.file.originalname || 'permission-template.xlsx';
      if (!originalName.toLowerCase().endsWith('.xlsx')) {
        return res.status(400).json({ message: 'Only .xlsx files are allowed' });
      }

      if (permissionStoredFilename) {
        await fsp.unlink(path.join(UPLOAD_DIR, permissionStoredFilename)).catch(() => {});
      }

      permissionFilename = originalName;
      permissionStoredFilename = buildStoredFilename('templates', `template_${Date.now()}_${originalName}`);
      await writeStoredFile(permissionStoredFilename, req.file.buffer);
    }

    const now = getNowIso();
    const inheritPermissions = req.body?.inheritPermissions !== undefined
      ? (req.body.inheritPermissions === 'false' || req.body.inheritPermissions === false ? false : true)
      : Boolean(existing.inherit_permissions ?? true);
    const updated = await pool.query(
      `
      UPDATE ${QUERY_PERMISSION_TEMPLATE_TABLE}
      SET template_name = $1,
          query_text = $2,
          target_type = $3,
          permission_filename = $4,
          permission_stored_filename = $5,
          inherit_permissions = $6,
          updated_by = $7,
          updated_at = $8
      WHERE id = $9
      RETURNING id, template_name, query_text, target_type, permission_filename, inherit_permissions, created_by, updated_by, created_at, updated_at
      `,
      [templateName, queryText, targetType, permissionFilename, permissionStoredFilename, inheritPermissions, user, now, templateId]
    );

    return res.json({ item: updated.rows[0] });
  });

  app.delete('/api/query-permission/templates/:id', authMiddleware, async (req, res) => {
    const templateId = Number(req.params.id);
    if (!Number.isFinite(templateId) || templateId <= 0) {
      return res.status(400).json({ message: 'Invalid template id' });
    }

    const existing = await pool.query(`SELECT * FROM ${QUERY_PERMISSION_TEMPLATE_TABLE} WHERE id = $1`, [templateId]);
    const row = existing.rows[0];
    if (!row) {
      return res.status(404).json({ message: 'Template not found' });
    }

    if (row.permission_stored_filename) {
      await fsp.unlink(path.join(UPLOAD_DIR, row.permission_stored_filename)).catch(() => {});
    }

    await pool.query(`DELETE FROM ${QUERY_PERMISSION_TEMPLATE_TABLE} WHERE id = $1`, [templateId]);
    return res.json({ message: 'Template deleted', id: templateId });
  });

  app.get('/api/query-permission/templates/:id/permission-file', authMiddleware, async (req, res) => {
    const templateId = Number(req.params.id);
    if (!Number.isFinite(templateId) || templateId <= 0) {
      return res.status(400).json({ message: 'Invalid template id' });
    }

    const result = await pool.query(`SELECT permission_filename, permission_stored_filename FROM ${QUERY_PERMISSION_TEMPLATE_TABLE} WHERE id = $1`, [templateId]);
    const row = result.rows[0];
    if (!row) {
      return res.status(404).json({ message: 'Template not found' });
    }
    if (!row.permission_stored_filename) {
      return res.status(404).json({ message: 'Template has no permission file' });
    }

    const filePath = path.join(UPLOAD_DIR, row.permission_stored_filename);
    if (!fs.existsSync(filePath)) {
      return res.status(404).json({ message: 'Permission file not found on server' });
    }

    return res.download(filePath, row.permission_filename || 'permission-template.xlsx');
  });

  app.post('/api/query-permission/runs', authMiddleware, async (req, res) => {
    const user = req.user?.username || req.user?.sub || 'unknown';
    const templateId = req.body?.templateId ? Number(req.body.templateId) : null;
    const now = getNowIso();

    let template = null;
    if (templateId) {
      const templateResult = await pool.query(`SELECT * FROM ${QUERY_PERMISSION_TEMPLATE_TABLE} WHERE id = $1`, [templateId]);
      template = templateResult.rows[0] || null;
      if (!template) {
        return res.status(404).json({ message: 'Template not found' });
      }
    }

    const definition = buildPermissionQueryDefinition(req.body?.queryText || template?.query_text, req.body?.targetType || template?.target_type || 'all');
    const queryText = definition.sanitizedQuery;
    const targetType = definition.targetType;
    if (!definition.effectiveQuery) {
      return res.status(400).json({ message: 'queryText is required' });
    }

    const inserted = await pool.query(
      `
      INSERT INTO ${QUERY_PERMISSION_RUN_TABLE}
        (template_id, queried_at, username, query_text, target_type, status, listed_count, add_success_count, add_failed_count, message, created_at, updated_at)
      VALUES
        ($1, $2, $3, $4, $5, $6, 0, 0, 0, $7, $8, $9)
      RETURNING id, template_id, queried_at, username, query_text, effective_query, target_type, status, listed_count, add_success_count, add_failed_count,
                permission_filename, add_source, message, created_at, updated_at, finished_at, add_started_at, add_finished_at
      `,
      [template?.id || null, now, user, queryText, targetType, 'QUEUED', 'Run queued', now, now]
    );

    const run = inserted.rows[0];
    setImmediate(() => {
      processPermissionQueryRun(run.id).catch((error) => {
        console.error('Unexpected query permission run error', error);
      });
    });

    return res.status(202).json({ message: 'Run accepted', run });
  });

  app.post('/api/query-permission/runs/:id/permission-file', authMiddleware, upload.single('file'), async (req, res) => {
    const runId = Number(req.params.id);
    if (!Number.isFinite(runId) || runId <= 0) {
      return res.status(400).json({ message: 'Invalid run id' });
    }

    if (!req.file) {
      return res.status(400).json({ message: 'file is required' });
    }

    const runResult = await pool.query(`SELECT id, permission_stored_filename FROM ${QUERY_PERMISSION_RUN_TABLE} WHERE id = $1`, [runId]);
    const run = runResult.rows[0];
    if (!run) {
      return res.status(404).json({ message: 'Run not found' });
    }

    const originalName = req.file.originalname || 'permission.xlsx';
    if (!originalName.toLowerCase().endsWith('.xlsx')) {
      return res.status(400).json({ message: 'Only .xlsx files are allowed' });
    }

    if (run.permission_stored_filename) {
      await fsp.unlink(path.join(UPLOAD_DIR, run.permission_stored_filename)).catch(() => {});
    }

    const storedFilename = buildStoredFilename('runs', `run_${runId}_${Date.now()}_${originalName}`);
    await writeStoredFile(storedFilename, req.file.buffer);

    await pool.query(
      `UPDATE ${QUERY_PERMISSION_RUN_TABLE} SET permission_filename = $1, permission_stored_filename = $2, updated_at = $3 WHERE id = $4`,
      [originalName, storedFilename, getNowIso(), runId]
    );

    return res.json({ message: 'Permission file imported', permission_filename: originalName });
  });

  app.post('/api/query-permission/runs/:id/add-permission', authMiddleware, async (req, res) => {
    const runId = Number(req.params.id);
    if (!Number.isFinite(runId) || runId <= 0) {
      return res.status(400).json({ message: 'Invalid run id' });
    }

    const source = String(req.body?.source || 'run').toLowerCase() === 'template' ? 'template' : 'run';

    try {
      setImmediate(() => {
        processPermissionAddRun(runId, source).catch(async (error) => {
          const reason = truncateMessage(error?.message || 'Add permission failed', 1000);
          await pool.query(
            `UPDATE ${QUERY_PERMISSION_RUN_TABLE} SET status = $1, message = $2, finished_at = $3, updated_at = $4 WHERE id = $5`,
            ['FAILED', reason, getNowIso(), getNowIso(), runId]
          ).catch(() => {});
        });
      });

      return res.status(202).json({ message: 'Add permission started', run_id: runId, source });
    } catch (error) {
      return res.status(500).json({ message: error?.message || 'Cannot start add permission' });
    }
  });

  app.post('/api/query-permission/runs/:id/retry-failed', authMiddleware, async (req, res) => {
    const runId = Number(req.params.id);
    if (!Number.isFinite(runId) || runId <= 0) {
      return res.status(400).json({ message: 'Invalid run id' });
    }

    const source = String(req.body?.source || 'template').toLowerCase() === 'run' ? 'run' : 'template';

    try {
      setImmediate(() => {
        processPermissionRetryFailedRun(runId, source).catch(async (error) => {
          const reason = truncateMessage(error?.message || 'Retry failed items failed', 1000);
          await pool.query(
            `UPDATE ${QUERY_PERMISSION_RUN_TABLE} SET status = $1, message = $2, finished_at = $3, updated_at = $4 WHERE id = $5`,
            ['FAILED', reason, getNowIso(), getNowIso(), runId]
          ).catch(() => {});
        });
      });

      return res.status(202).json({ message: 'Retry failed items started', run_id: runId, source });
    } catch (error) {
      return res.status(500).json({ message: error?.message || 'Cannot start retry failed items' });
    }
  });

  app.delete('/api/query-permission/runs/:id', authMiddleware, async (req, res) => {
    const runId = Number(req.params.id);
    if (!Number.isFinite(runId) || runId <= 0) {
      return res.status(400).json({ message: 'Invalid run id' });
    }

    const user = req.user?.username || req.user?.sub || 'unknown';
    const runResult = await pool.query(
      `SELECT id, status, permission_stored_filename FROM ${QUERY_PERMISSION_RUN_TABLE} WHERE id = $1`,
      [runId]
    );

    const run = runResult.rows[0];
    if (!run) {
      return res.status(404).json({ message: 'Run not found' });
    }

    const status = String(run.status || '').toUpperCase();
    if (['QUEUED', 'LISTING', 'ADDING_PERMISSION'].includes(status)) {
      return res.status(409).json({ message: `Cannot delete run in status: ${status}` });
    }

    if (run.permission_stored_filename) {
      await fsp.unlink(path.join(UPLOAD_DIR, run.permission_stored_filename)).catch(() => {});
    }

    await pool.query(`DELETE FROM ${QUERY_PERMISSION_RUN_TABLE} WHERE id = $1`, [runId]);

    await safeAddAuditEvent({
      serviceName: SERVICE_QUERY_ADD_PERMISSION,
      username: user,
      actionType: 'RUN_DELETE',
      status: 'SUCCESS',
      message: `Run deleted: ${runId}`,
      entityType: 'permission_run',
      entityId: String(runId)
    });

    return res.json({ message: 'Run deleted', id: runId });
  });

  app.get('/api/query-permission/runs/:id', authMiddleware, async (req, res) => {
    const runId = Number(req.params.id);
    if (!Number.isFinite(runId) || runId <= 0) {
      return res.status(400).json({ message: 'Invalid run id' });
    }

    const runResult = await pool.query(
      `
      SELECT
        r.id,
        r.template_id,
        t.template_name,
        r.queried_at,
        r.username,
        r.query_text,
        r.effective_query,
        r.target_type,
        r.status,
        r.listed_count,
        r.add_success_count,
        r.add_failed_count,
        r.permission_filename,
        r.add_source,
        r.message,
        r.created_at,
        r.updated_at,
        r.finished_at,
        r.add_started_at,
        r.add_finished_at,
        t.permission_filename AS template_permission_filename
      FROM ${QUERY_PERMISSION_RUN_TABLE} r
      LEFT JOIN ${QUERY_PERMISSION_TEMPLATE_TABLE} t ON t.id = r.template_id
      WHERE r.id = $1
      `,
      [runId]
    );

    const run = runResult.rows[0];
    if (!run) {
      return res.status(404).json({ message: 'Run not found' });
    }

    const itemsResult = await pool.query(
      `
      SELECT id, node_ref, node_id, node_type, node_name, node_path, status, message, created_at, updated_at
      FROM ${QUERY_PERMISSION_RUN_ITEM_TABLE}
      WHERE run_id = $1
      ORDER BY id DESC
      LIMIT 200
      `,
      [runId]
    );

    return res.json({
      run: {
        ...run,
        can_add_from_template: Boolean(run.template_permission_filename),
        can_add_from_run_file: Boolean(run.permission_filename)
      },
      items: itemsResult.rows,
      item_count: itemsResult.rows.length
    });
  });

  app.get('/api/query-permission/runs/:id/items', authMiddleware, async (req, res) => {
    const runId = Number(req.params.id);
    if (!Number.isFinite(runId) || runId <= 0) {
      return res.status(400).json({ message: 'Invalid run id' });
    }

    const pageSize = [30, 50, 100].includes(Number(req.query.pageSize)) ? Number(req.query.pageSize) : 30;
    const page = toPositiveInt(req.query.page, 1);
    const offset = (page - 1) * pageSize;
    const statusFilter = String(req.query.status || 'all').trim().toLowerCase();
    const searchText = String(req.query.search || '').trim();

    const runResult = await pool.query(
      `
      SELECT
        r.id,
        r.template_id,
        t.template_name,
        r.queried_at,
        r.username,
        r.target_type,
        r.status,
        r.listed_count,
        r.add_success_count,
        r.add_failed_count,
        r.message,
        r.created_at,
        r.updated_at,
        r.detail_cleared_at,
        r.detail_cleared_item_count,
        r.detail_cleared_reason,
        t.permission_filename AS template_permission_filename
      FROM ${QUERY_PERMISSION_RUN_TABLE} r
      LEFT JOIN ${QUERY_PERMISSION_TEMPLATE_TABLE} t ON t.id = r.template_id
      WHERE r.id = $1
      `,
      [runId]
    );

    const run = runResult.rows[0];
    if (!run) {
      return res.status(404).json({ message: 'Run not found' });
    }

    const conditions = ['run_id = $1'];
    const params = [runId];
    let index = params.length;

    if (statusFilter === 'wait') {
      index += 1;
      params.push('LISTED');
      conditions.push(`status = $${index}`);
    } else if (statusFilter === 'success') {
      index += 1;
      params.push('ADD_SUCCESS');
      conditions.push(`status = $${index}`);
    } else if (statusFilter === 'failed') {
      index += 1;
      params.push('ADD_FAILED');
      conditions.push(`status = $${index}`);
    }

    if (searchText) {
      index += 1;
      params.push(`%${searchText}%`);
      conditions.push(`(node_name ILIKE $${index} OR node_id ILIKE $${index} OR node_ref ILIKE $${index} OR node_path ILIKE $${index})`);
    }

    const whereClause = conditions.join(' AND ');

    index += 1;
    params.push(pageSize);
    const limitIndex = index;

    index += 1;
    params.push(offset);
    const offsetIndex = index;

    const countResult = await pool.query(
      `SELECT COUNT(*)::bigint AS total FROM ${QUERY_PERMISSION_RUN_ITEM_TABLE} WHERE ${whereClause}`,
      params.slice(0, limitIndex - 1)
    );

    const itemsResult = await pool.query(
      `
      SELECT id, run_id, node_ref, node_id, node_type, node_name, node_path, status, message, created_at, updated_at
      FROM ${QUERY_PERMISSION_RUN_ITEM_TABLE}
      WHERE ${whereClause}
      ORDER BY id DESC
      LIMIT $${limitIndex} OFFSET $${offsetIndex}
      `,
      params
    );

    const detailsCleared = Boolean(run.detail_cleared_at);
    let summary = {};
    if (detailsCleared) {
      const listedCount = Number(run.listed_count || 0);
      const successCount = Number(run.add_success_count || 0);
      const failedCount = Number(run.add_failed_count || 0);
      summary = {
        total: listedCount,
        wait_count: Math.max(0, listedCount - successCount - failedCount),
        success_count: successCount,
        failed_count: failedCount
      };
    } else {
      const summaryResult = await pool.query(
        `
        SELECT
          COUNT(*)::bigint AS total,
          SUM(CASE WHEN status = 'LISTED' THEN 1 ELSE 0 END)::bigint AS wait_count,
          SUM(CASE WHEN status = 'ADD_SUCCESS' THEN 1 ELSE 0 END)::bigint AS success_count,
          SUM(CASE WHEN status = 'ADD_FAILED' THEN 1 ELSE 0 END)::bigint AS failed_count
        FROM ${QUERY_PERMISSION_RUN_ITEM_TABLE}
        WHERE run_id = $1
        `,
        [runId]
      );
      summary = summaryResult.rows[0] || {};
    }
    const mappedItems = itemsResult.rows.map((item) => ({
      ...item,
      display_status: mapPermissionRunItemStatus(item.status)
    }));

    return res.json({
      run: {
        ...run,
        can_run: ['LISTED', 'COMPLETED_WITH_ERRORS'].includes(String(run.status || ''))
          && Boolean(run.template_permission_filename),
        can_retry_failed: !['QUEUED', 'LISTING', 'ADDING_PERMISSION'].includes(String(run.status || ''))
          && Boolean(run.template_permission_filename)
          && Number(summary.failed_count || 0) > 0,
        details_cleared: detailsCleared,
        display_status: run.status
      },
      summary: {
        all: Number(summary.total || 0),
        wait: Number(summary.wait_count || 0),
        success: Number(summary.success_count || 0),
        failed: Number(summary.failed_count || 0)
      },
      items: mappedItems,
      total: Number(countResult.rows[0]?.total || 0),
      page,
      pageSize,
      status: ['all', 'wait', 'success', 'failed'].includes(statusFilter) ? statusFilter : 'all',
      search: searchText
    });
  });

  app.get('/api/reports/query-permission', authMiddleware, async (req, res) => {
    const pageSize = [30, 50, 100].includes(Number(req.query.pageSize)) ? Number(req.query.pageSize) : 30;
    const page = toPositiveInt(req.query.page, 1);
    const offset = (page - 1) * pageSize;

    const countResult = await pool.query(`SELECT COUNT(*)::bigint AS total FROM ${QUERY_PERMISSION_RUN_TABLE}`);
    const dataResult = await pool.query(
      `
      SELECT
        r.id,
        r.template_id,
        t.template_name,
        r.queried_at,
        r.username,
        r.query_text,
        r.effective_query,
        r.target_type,
        r.status,
        r.listed_count,
        r.add_success_count,
        r.add_failed_count,
        r.permission_filename,
        r.add_source,
        r.message,
        r.created_at,
        r.updated_at,
        r.finished_at,
        r.add_started_at,
        r.add_finished_at,
        r.detail_cleared_at,
        r.detail_cleared_item_count,
        r.detail_cleared_reason,
        t.permission_filename AS template_permission_filename
      FROM ${QUERY_PERMISSION_RUN_TABLE} r
      LEFT JOIN ${QUERY_PERMISSION_TEMPLATE_TABLE} t ON t.id = r.template_id
      ORDER BY r.id DESC
      LIMIT $1 OFFSET $2
      `,
      [pageSize, offset]
    );

    const mappedRows = dataResult.rows.map((row) => ({
      ...row,
      details_cleared: Boolean(row.detail_cleared_at),
      can_run: ['LISTED', 'COMPLETED_WITH_ERRORS'].includes(String(row.status || ''))
        && Boolean(row.template_permission_filename)
    }));

    return res.json({
      items: mappedRows,
      total: Number(countResult.rows[0]?.total || 0),
      page,
      pageSize
    });
  });
}

module.exports = {
  registerQueryPermissionRoutes
};
