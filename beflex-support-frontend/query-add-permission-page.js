const apiBase = '/api/beflex-support';

const elements = {
  loginStatus: document.getElementById('qapLoginStatus'),
  logoutBtn: document.getElementById('qapLogoutBtn'),

  templateName: document.getElementById('qapTemplateName'),
  queryText: document.getElementById('qapQueryText'),
  queryWarning: document.getElementById('qapQueryWarning'),
  targetType: document.getElementById('qapTargetType'),
  templatePermissionFile: document.getElementById('qapTemplatePermissionFile'),
  testTemplateBtn: document.getElementById('qapTestTemplateBtn'),
  guideDetails: document.getElementById('qapGuideDetails'),
  guideManualBtn: document.getElementById('qapGuideManualBtn'),
  guideSettingBtn: document.getElementById('qapGuideSettingBtn'),
  guideManualSection: document.getElementById('qapGuideManualSection'),
  guideSettingSection: document.getElementById('qapGuideSettingSection'),
  settingAddConcurrency: document.getElementById('qapSettingAddConcurrency'),
  settingAddMaxRetries: document.getElementById('qapSettingAddMaxRetries'),
  settingAddRetryBaseMs: document.getElementById('qapSettingAddRetryBaseMs'),
  settingDetailRetentionDays: document.getElementById('qapSettingDetailRetentionDays'),
  settingDetailCleanupCron: document.getElementById('qapSettingDetailCleanupCron'),
  reloadSettingsBtn: document.getElementById('qapReloadSettingsBtn'),
  saveSettingsBtn: document.getElementById('qapSaveSettingsBtn'),
  settingsStatus: document.getElementById('qapSettingsStatus'),
  settingsError: document.getElementById('qapSettingsError'),
  useExample1Btn: document.getElementById('qapUseExample1Btn'),
  useExample2Btn: document.getElementById('qapUseExample2Btn'),
  useExample3Btn: document.getElementById('qapUseExample3Btn'),
  example1: document.getElementById('qapExample1'),
  example2: document.getElementById('qapExample2'),
  example3: document.getElementById('qapExample3'),
  createTemplateBtn: document.getElementById('qapCreateTemplateBtn'),
  clearTemplateBtn: document.getElementById('qapClearTemplateBtn'),
  templateTestResult: document.getElementById('qapTemplateTestResult'),
  templateStatus: document.getElementById('qapTemplateStatus'),
  templateError: document.getElementById('qapTemplateError'),

  inheritPermissions: document.getElementById('qapTemplateInheritPermissions'),

  templateBody: document.getElementById('qapTemplateBody'),
  templatePageSize: document.getElementById('qapTemplatePageSize'),
  templatePrevBtn: document.getElementById('qapTemplatePrevBtn'),
  templateNextBtn: document.getElementById('qapTemplateNextBtn'),
  templatePagingLabel: document.getElementById('qapTemplatePagingLabel'),

  reportBody: document.getElementById('qapReportBody'),
  pageSize: document.getElementById('qapPageSize'),
  prevPageBtn: document.getElementById('qapPrevPageBtn'),
  nextPageBtn: document.getElementById('qapNextPageBtn'),
  pagingLabel: document.getElementById('qapPagingLabel'),
  runStatus: document.getElementById('qapRunStatus'),
  runError: document.getElementById('qapRunError'),

  templateModal: document.getElementById('qapTemplateModal'),
  editTemplateId: document.getElementById('qapEditTemplateId'),
  editTemplateName: document.getElementById('qapEditTemplateName'),
  editTargetType: document.getElementById('qapEditTargetType'),
  editQueryText: document.getElementById('qapEditQueryText'),
  editCurrentFile: document.getElementById('qapEditCurrentFile'),
  editPermissionFile: document.getElementById('qapEditPermissionFile'),
  removePermissionFile: document.getElementById('qapRemovePermissionFile'),
  editInheritPermissions: document.getElementById('qapEditInheritPermissions'),
  saveTemplateBtn: document.getElementById('qapSaveTemplateBtn'),
  deleteTemplateBtn: document.getElementById('qapDeleteTemplateBtn'),
  downloadTemplateFileBtn: document.getElementById('qapDownloadTemplateFileBtn'),
  closeTemplateModalBtn: document.getElementById('qapCloseTemplateModalBtn'),
  editTemplateStatus: document.getElementById('qapEditTemplateStatus'),
  editTemplateError: document.getElementById('qapEditTemplateError'),

  messageModal: document.getElementById('qapMessageModal'),
  messageModalText: document.getElementById('qapMessageModalText'),
  closeMessageModalBtn: document.getElementById('qapCloseMessageModalBtn')
};

let token = localStorage.getItem('allopsToken') || '';
let idleTimer = null;
let pageTimer = null;
let currentPage = 1;
let totalPages = 1;
let templateCurrentPage = 1;
let templateTotalPages = 1;
let templates = [];
let isTestingTemplate = false;
let activeGuideCategory = 'manual';

const idleTimeoutMinutes = Number(document.querySelector('.main-content')?.dataset?.sessionTimeoutMinutes || 30);
const idleTimeoutMs = idleTimeoutMinutes * 60 * 1000;
const activityEvents = ['click', 'keydown', 'touchstart', 'scroll', 'focus'];

function setStatus(element, text, kind = '') {
  if (!element) {
    return;
  }
  element.textContent = text;
  element.className = 'status';
  if (kind) {
    element.classList.add(kind);
  }
}

function escapeHtml(value) {
  return String(value ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#039;');
}

function clearSession() {
  localStorage.removeItem('allopsToken');
  localStorage.removeItem('allopsUsername');
}

function navigateToLogin() {
  window.location.href = 'index.html';
}

function stopPageRefresh() {
  if (pageTimer) {
    clearInterval(pageTimer);
    pageTimer = null;
  }
}

function stopIdleTimeout() {
  if (idleTimer) {
    clearTimeout(idleTimer);
    idleTimer = null;
  }
}

function resetIdleTimeout() {
  if (!token) {
    return;
  }

  stopIdleTimeout();
  idleTimer = setTimeout(() => {
    stopPageRefresh();
    stopIdleTimeout();
    clearSession();
    sessionStorage.setItem('allopsSessionExpired', '1');
    navigateToLogin();
  }, idleTimeoutMs);
}

function startIdleTimeout() {
  activityEvents.forEach((eventName) => {
    window.addEventListener(eventName, resetIdleTimeout, { passive: true });
  });
  resetIdleTimeout();
}

async function callApi(path, options = {}) {
  const headers = options.headers || {};
  if (token) {
    headers.Authorization = `Bearer ${token}`;
  }
  if (options.body && !(options.body instanceof FormData) && !headers['Content-Type']) {
    headers['Content-Type'] = 'application/json';
  }

  const response = await fetch(`${apiBase}${path}`, {
    ...options,
    headers
  });

  const data = await response.json().catch(() => ({}));
  if (response.status === 401) {
    clearSession();
    stopPageRefresh();
    stopIdleTimeout();
    sessionStorage.setItem('allopsSessionExpired', '1');
    navigateToLogin();
    throw new Error('Session expired');
  }

  if (!response.ok) {
    throw new Error(data.message || `Request failed: ${response.status}`);
  }

  return data;
}

function mapRunStatus(value) {
  const raw = String(value || '').toUpperCase();
  if (raw === 'QUEUED' || raw === 'LISTING') {
    return 'query';
  }
  if (raw === 'LISTED') {
    return 'wait';
  }
  if (raw === 'ADDING_PERMISSION') {
    return 'running';
  }
  if (raw === 'COMPLETED') {
    return 'success';
  }
  if (raw === 'FAILED' || raw === 'COMPLETED_WITH_ERRORS') {
    return 'failed';
  }
  return 'running';
}

function shortMessage(value) {
  const text = String(value || '-').trim();
  if (text.length <= 80) {
    return text;
  }
  return `${text.slice(0, 80)}...`;
}

function looksLikeJsonEscapedQuery(query) {
  return /\\"/.test(String(query || '')) || /^"[\s\S]/.test(String(query || '').trim());
}

function detectEmbeddedType(query) {
  const matched = String(query || '').match(/TYPE\s*:\s*"cm:(content|folder)"/i);
  if (!matched) {
    return '';
  }
  return matched[1].toLowerCase() === 'content' ? 'file' : 'folder';
}

function hasLikelyMissingBooleanBetweenClauses(query) {
  return /(["'\)\]])\s+(?=(?:[A-Za-z_][A-Za-z0-9_]*\s*:|NOT\s*\())/.test(String(query || ''));
}

function setQueryWarning() {
  if (!elements.queryWarning) {
    return;
  }

  const query = String(elements.queryText.value || '').trim();
  const warnings = [];
  const hasEscapedQuotes = /\\"/.test(query);
  const hasOuterQuote = /^"[\s\S]/.test(query.trim());
  if (hasEscapedQuotes || hasOuterQuote) {
    let msg = 'Query มีรูปแบบ JSON-encoded —';
    if (hasOuterQuote) msg += ' ระบบจะตัด outer " ที่ครอบอยู่ออก';
    if (hasEscapedQuotes) msg += (hasOuterQuote ? ' และ' : '') + ' แปลง \\" เป็น " อัตโนมัติ';
    msg += ' ก่อนส่งไป Alfresco';
    warnings.push(msg);
  }

  const embeddedType = detectEmbeddedType(query);
  if (embeddedType) {
    const label = embeddedType === 'file' ? 'File' : 'Folder';
    warnings.push(`พบ TYPE ใน AFTS Query ระบบจะตัดออกจาก Query และใช้ค่า Type = ${label} แทน`);
  }

  if (hasLikelyMissingBooleanBetweenClauses(query)) {
    warnings.push('พบเงื่อนไขที่อาจลืม AND/OR ระหว่าง clause (เช่น PATH:"..." cm:name:"...") ระบบจะเติม AND ให้อัตโนมัติ');
  }

  if (!warnings.length) {
    elements.queryWarning.textContent = '';
    elements.queryWarning.classList.add('hidden');
    return;
  }

  elements.queryWarning.textContent = warnings.join(' | ');
  elements.queryWarning.classList.remove('hidden');
}

function fillExample(exampleElement) {
  const exampleText = String(exampleElement?.textContent || '').trim();
  if (!exampleText) {
    return;
  }
  elements.queryText.value = exampleText;
  setQueryWarning();
  updateCreateButtonState();
}

function switchGuideCategory(category) {
  activeGuideCategory = category === 'setting' ? 'setting' : 'manual';
  const isSetting = activeGuideCategory === 'setting';

  if (elements.guideManualBtn) {
    elements.guideManualBtn.classList.toggle('active', !isSetting);
  }
  if (elements.guideSettingBtn) {
    elements.guideSettingBtn.classList.toggle('active', isSetting);
  }
  if (elements.guideManualSection) {
    elements.guideManualSection.classList.toggle('hidden', isSetting);
  }
  if (elements.guideSettingSection) {
    elements.guideSettingSection.classList.toggle('hidden', !isSetting);
  }
}

function fillSettingsForm(item) {
  if (!item) {
    return;
  }
  if (elements.settingAddConcurrency) {
    elements.settingAddConcurrency.value = Number(item.addConcurrency || 0);
  }
  if (elements.settingAddMaxRetries) {
    elements.settingAddMaxRetries.value = Number(item.addMaxRetries || 0);
  }
  if (elements.settingAddRetryBaseMs) {
    elements.settingAddRetryBaseMs.value = Number(item.addRetryBaseMs || 0);
  }
  if (elements.settingDetailRetentionDays) {
    elements.settingDetailRetentionDays.value = Number(item.detailRetentionDays || 0);
  }
  if (elements.settingDetailCleanupCron) {
    elements.settingDetailCleanupCron.value = String(item.detailCleanupCron || '');
  }
}

function getSettingsPayloadFromForm() {
  return {
    addConcurrency: Number(elements.settingAddConcurrency?.value || 0),
    addMaxRetries: Number(elements.settingAddMaxRetries?.value || 0),
    addRetryBaseMs: Number(elements.settingAddRetryBaseMs?.value || 0),
    detailRetentionDays: Number(elements.settingDetailRetentionDays?.value || 0),
    detailCleanupCron: String(elements.settingDetailCleanupCron?.value || '').trim()
  };
}

async function loadQueryPermissionSettings(showLoadedStatus = false) {
  if (!elements.settingsStatus) {
    return;
  }

  if (elements.settingsError) {
    elements.settingsError.textContent = '';
  }

  setStatus(elements.settingsStatus, 'Loading...', 'progress');
  const result = await callApi('/query-permission/settings');
  fillSettingsForm(result.item || {});
  setStatus(elements.settingsStatus, showLoadedStatus ? 'Loaded' : 'Idle', showLoadedStatus ? 'completed' : '');
}

async function saveQueryPermissionSettings() {
  if (!elements.settingsStatus) {
    return;
  }

  if (elements.settingsError) {
    elements.settingsError.textContent = '';
  }

  const payload = getSettingsPayloadFromForm();
  if (!payload.detailCleanupCron) {
    elements.settingsError.textContent = 'กรุณากรอกค่า Detail Cleanup Cron';
    return;
  }

  try {
    setStatus(elements.settingsStatus, 'Saving...', 'progress');
    const result = await callApi('/query-permission/settings', {
      method: 'PUT',
      body: JSON.stringify(payload)
    });

    fillSettingsForm(result.item || payload);
    setStatus(elements.settingsStatus, 'Saved', 'completed');
  } catch (error) {
    setStatus(elements.settingsStatus, 'Failed', 'failed');
    if (elements.settingsError) {
      elements.settingsError.textContent = error.message;
    }
  }
}

function updateCreateButtonState() {
  const hasFile = Boolean(elements.templatePermissionFile.files?.[0]);
  const hasQuery = Boolean(String(elements.queryText.value || '').trim());
  elements.createTemplateBtn.disabled = !hasFile;
  elements.testTemplateBtn.disabled = isTestingTemplate || !hasQuery;
}

function clearTemplateForm() {
  elements.templateName.value = '';
  elements.queryText.value = '';
  elements.targetType.value = 'folder';
  elements.templatePermissionFile.value = '';
  if (elements.inheritPermissions) {
    elements.inheritPermissions.checked = true;
  }
  elements.templateTestResult.textContent = 'totalItems: -';
  elements.templateError.textContent = '';
  setQueryWarning();
  updateCreateButtonState();
}

async function testTemplateQuery() {
  elements.templateError.textContent = '';
  const queryText = String(elements.queryText.value || '').trim();
  const targetType = String(elements.targetType.value || 'folder');

  if (!queryText) {
    elements.templateError.textContent = 'กรุณากรอก Query ก่อนทดสอบ';
    return;
  }

  isTestingTemplate = true;
  setQueryWarning();
  updateCreateButtonState();

  try {
    setStatus(elements.templateStatus, 'Testing...', 'progress');
    elements.templateTestResult.textContent = 'totalItems: ...';

    const result = await callApi('/query-permission/templates/test', {
      method: 'POST',
      body: JSON.stringify({ queryText, targetType })
    });

    if (result.targetType && ['folder', 'file', 'all'].includes(String(result.targetType))) {
      elements.targetType.value = result.targetType;
    }
    elements.templateTestResult.textContent = `totalItems: ${Number(result.totalItems || 0)}`;
    setStatus(elements.templateStatus, 'Tested', 'completed');
  } catch (error) {
    elements.templateTestResult.textContent = 'totalItems: -';
    setStatus(elements.templateStatus, 'Failed', 'failed');
    elements.templateError.textContent = error.message;
  } finally {
    isTestingTemplate = false;
    updateCreateButtonState();
  }
}

async function createTemplate() {
  elements.templateError.textContent = '';
  const templateName = String(elements.templateName.value || '').trim();
  const queryText = String(elements.queryText.value || '').trim();
  const targetType = String(elements.targetType.value || 'folder');
  const file = elements.templatePermissionFile.files?.[0] || null;

  if (!file) {
    elements.templateError.textContent = 'กรุณาแนบ Permission Excel ก่อน Create template';
    return;
  }
  if (!templateName || !queryText) {
    elements.templateError.textContent = 'กรุณากรอก Template Name และ AFTS Query';
    return;
  }

  try {
    setStatus(elements.templateStatus, 'Creating...', 'progress');
    const inheritPermissions = elements.inheritPermissions ? elements.inheritPermissions.checked : true;
    const formData = new FormData();
    formData.append('templateName', templateName);
    formData.append('queryText', queryText);
    formData.append('targetType', targetType);
    formData.append('inheritPermissions', inheritPermissions ? 'true' : 'false');
    formData.append('file', file);

    await callApi('/query-permission/templates', {
      method: 'POST',
      body: formData,
      headers: {}
    });

    clearTemplateForm();
    await loadTemplates();
    setStatus(elements.templateStatus, 'Created', 'completed');
  } catch (error) {
    setStatus(elements.templateStatus, 'Failed', 'failed');
    elements.templateError.textContent = error.message;
  }
}

function getTemplateById(templateId) {
  return templates.find((item) => Number(item.id) === Number(templateId)) || null;
}

function renderTemplateTable() {
  const pageSize = Number(elements.templatePageSize.value || 30);
  const total = templates.length;
  templateTotalPages = Math.max(1, Math.ceil(total / pageSize));
  templateCurrentPage = Math.min(templateCurrentPage, templateTotalPages);
  const offset = (templateCurrentPage - 1) * pageSize;
  const rows = templates.slice(offset, offset + pageSize);

  elements.templateBody.innerHTML = '';
  if (!rows.length) {
    elements.templateBody.innerHTML = '<tr><td colspan="5">No template data</td></tr>';
  } else {
    rows.forEach((item, index) => {
      const tr = document.createElement('tr');
      const runningType = String(item.target_type || 'all').toLowerCase();
      tr.innerHTML = `
        <td>${offset + index + 1}</td>
        <td>${escapeHtml(item.template_name || '-')}</td>
        <td>${escapeHtml(runningType)}</td>
        <td>${escapeHtml(item.permission_filename || '-')}</td>
        <td>
          <button class="btn-secondary qap-template-detail-btn" data-template-id="${item.id}" type="button">details & edit</button>
          <button class="btn-primary qap-template-query-btn" data-template-id="${item.id}" type="button">Query</button>
        </td>
      `;
      elements.templateBody.appendChild(tr);
    });
  }

  elements.templatePagingLabel.textContent = `Page ${templateCurrentPage} / ${templateTotalPages}`;
  elements.templatePrevBtn.disabled = templateCurrentPage <= 1;
  elements.templateNextBtn.disabled = templateCurrentPage >= templateTotalPages;

  document.querySelectorAll('.qap-template-detail-btn').forEach((button) => {
    button.addEventListener('click', () => {
      openTemplateModal(Number(button.getAttribute('data-template-id')));
    });
  });

  document.querySelectorAll('.qap-template-query-btn').forEach((button) => {
    button.addEventListener('click', async () => {
      const templateId = Number(button.getAttribute('data-template-id'));
      await runTemplateQuery(templateId);
    });
  });
}

async function loadTemplates() {
  const result = await callApi('/query-permission/templates');
  templates = Array.isArray(result.items) ? result.items : [];
  renderTemplateTable();
}

function openTemplateModal(templateId) {
  const template = getTemplateById(templateId);
  if (!template) {
    return;
  }

  elements.editTemplateId.value = String(template.id);
  elements.editTemplateName.value = template.template_name || '';
  elements.editTargetType.value = template.target_type || 'folder';
  elements.editQueryText.value = template.query_text || '';
  elements.editCurrentFile.textContent = template.permission_filename || '-';
  elements.editPermissionFile.value = '';
  elements.removePermissionFile.checked = false;
  if (elements.editInheritPermissions) {
    elements.editInheritPermissions.checked = template.inherit_permissions !== false;
  }
  elements.editTemplateError.textContent = '';
  setStatus(elements.editTemplateStatus, 'Idle');

  elements.templateModal.classList.remove('hidden');
  elements.templateModal.setAttribute('aria-hidden', 'false');
}

function closeTemplateModal() {
  elements.templateModal.classList.add('hidden');
  elements.templateModal.setAttribute('aria-hidden', 'true');
}

async function saveTemplate() {
  elements.editTemplateError.textContent = '';
  const templateId = Number(elements.editTemplateId.value || 0);
  const templateName = String(elements.editTemplateName.value || '').trim();
  const queryText = String(elements.editQueryText.value || '').trim();
  const targetType = String(elements.editTargetType.value || 'folder');
  const replaceFile = elements.editPermissionFile.files?.[0] || null;
  const wantsRemoveCurrentFile = Boolean(elements.removePermissionFile.checked);

  if (!templateId) {
    elements.editTemplateError.textContent = 'ไม่พบ template id';
    return;
  }
  if (!templateName || !queryText) {
    elements.editTemplateError.textContent = 'กรุณากรอก Template Name และ Query';
    return;
  }

  if (wantsRemoveCurrentFile && !replaceFile) {
    const confirmed = window.confirm('ยืนยันลบไฟล์ Excel ปัจจุบันออกจาก Template นี้ใช่หรือไม่?');
    if (!confirmed) {
      elements.removePermissionFile.checked = false;
      return;
    }
  }

  try {
    setStatus(elements.editTemplateStatus, 'Saving...', 'progress');
    const inheritPermissions = elements.editInheritPermissions ? elements.editInheritPermissions.checked : true;
    const formData = new FormData();
    formData.append('templateName', templateName);
    formData.append('queryText', queryText);
    formData.append('targetType', targetType);
    formData.append('inheritPermissions', inheritPermissions ? 'true' : 'false');

    if (wantsRemoveCurrentFile) {
      formData.append('removePermissionFile', 'yes-remove');
    }

    if (replaceFile) {
      formData.append('file', replaceFile);
    }

    await callApi(`/query-permission/templates/${templateId}`, {
      method: 'PUT',
      body: formData,
      headers: {}
    });

    await loadTemplates();
    setStatus(elements.editTemplateStatus, 'Saved', 'completed');
    closeTemplateModal();
  } catch (error) {
    setStatus(elements.editTemplateStatus, 'Failed', 'failed');
    elements.editTemplateError.textContent = error.message;
  }
}

async function deleteTemplate() {
  elements.editTemplateError.textContent = '';
  const templateId = Number(elements.editTemplateId.value || 0);
  if (!templateId) {
    elements.editTemplateError.textContent = 'ไม่พบ template id';
    return;
  }

  if (!window.confirm(`Delete template #${templateId} ?`)) {
    return;
  }

  try {
    setStatus(elements.editTemplateStatus, 'Deleting...', 'progress');
    await callApi(`/query-permission/templates/${templateId}`, { method: 'DELETE' });
    await loadTemplates();
    closeTemplateModal();
    setStatus(elements.templateStatus, 'Deleted', 'completed');
  } catch (error) {
    setStatus(elements.editTemplateStatus, 'Failed', 'failed');
    elements.editTemplateError.textContent = error.message;
  }
}

async function downloadTemplateFile() {
  const templateId = Number(elements.editTemplateId.value || 0);
  if (!templateId) {
    elements.editTemplateError.textContent = 'ไม่พบ template id';
    return;
  }

  const headers = {};
  if (token) {
    headers.Authorization = `Bearer ${token}`;
  }

  const response = await fetch(`${apiBase}/query-permission/templates/${templateId}/permission-file`, {
    method: 'GET',
    headers
  });

  if (!response.ok) {
    let message = 'Download failed';
    try {
      const payload = await response.json();
      message = payload.message || message;
    } catch (_error) {
      // ignore
    }
    throw new Error(message);
  }

  const blob = await response.blob();
  const disposition = response.headers.get('content-disposition') || '';
  const matched = disposition.match(/filename="([^"]+)"/i);
  const filename = matched?.[1] || 'permission-template.xlsx';

  const url = URL.createObjectURL(blob);
  const anchor = document.createElement('a');
  anchor.href = url;
  anchor.download = filename;
  document.body.appendChild(anchor);
  anchor.click();
  document.body.removeChild(anchor);
  URL.revokeObjectURL(url);
}

async function runTemplateQuery(templateId) {
  elements.runError.textContent = '';
  if (!templateId) {
    return;
  }

  try {
    setStatus(elements.runStatus, 'Querying...', 'progress');
    await callApi('/query-permission/runs', {
      method: 'POST',
      body: JSON.stringify({ templateId })
    });

    await loadReports(1);
    setStatus(elements.runStatus, 'Queued', 'completed');
  } catch (error) {
    setStatus(elements.runStatus, 'Failed', 'failed');
    elements.runError.textContent = error.message;
  }
}

async function runAddPermission(runId) {
  elements.runError.textContent = '';
  if (!runId) {
    return;
  }

  try {
    setStatus(elements.runStatus, 'Running...', 'progress');
    await callApi(`/query-permission/runs/${runId}/add-permission`, {
      method: 'POST',
      body: JSON.stringify({ source: 'template' })
    });

    await loadReports(currentPage);
    setStatus(elements.runStatus, 'Started', 'completed');
  } catch (error) {
    setStatus(elements.runStatus, 'Failed', 'failed');
    elements.runError.textContent = error.message;
  }
}

async function deletePermissionRun(runId) {
  elements.runError.textContent = '';
  if (!runId) {
    return;
  }

  if (!window.confirm(`Delete run #${runId}?`)) {
    return;
  }

  try {
    setStatus(elements.runStatus, 'Deleting...', 'progress');
    await callApi(`/query-permission/runs/${runId}`, {
      method: 'DELETE'
    });

    await loadReports(currentPage);
    setStatus(elements.runStatus, 'Deleted', 'completed');
  } catch (error) {
    setStatus(elements.runStatus, 'Failed', 'failed');
    elements.runError.textContent = error.message;
  }
}

function showMessageModal(text) {
  elements.messageModalText.textContent = String(text || '-');
  elements.messageModal.classList.remove('hidden');
  elements.messageModal.setAttribute('aria-hidden', 'false');
}

function closeMessageModal() {
  elements.messageModal.classList.add('hidden');
  elements.messageModal.setAttribute('aria-hidden', 'true');
}

function updateQueryPaging(page, total, pageSize) {
  currentPage = page;
  totalPages = Math.max(1, Math.ceil(total / pageSize));
  elements.pagingLabel.textContent = `Page ${currentPage} / ${totalPages}`;
  elements.prevPageBtn.disabled = currentPage <= 1;
  elements.nextPageBtn.disabled = currentPage >= totalPages;
}

function renderReports(items) {
  elements.reportBody.innerHTML = '';
  if (!items.length) {
    elements.reportBody.innerHTML = '<tr><td colspan="8">No query report data</td></tr>';
    return;
  }

  items.forEach((item) => {
    const noQuery = Number(item.id || 0);
    const mappedStatus = mapRunStatus(item.status);
    const detailsCleared = Boolean(item.details_cleared || item.detail_cleared_at);
    const statusLabel = detailsCleared ? `${mappedStatus} (cleared)` : mappedStatus;
    const canRun = Boolean(item.can_run);
    const canDelete = mappedStatus !== 'query' && mappedStatus !== 'running';
    const clearSuffix = detailsCleared
      ? ` | details cleared (rows=${Number(item.detail_cleared_item_count || 0)})`
      : '';
    const fullMessage = `${String(item.message || '-')}${clearSuffix}`;

    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td>${noQuery}</td>
      <td>${escapeHtml(item.template_name || '-')}</td>
      <td>${escapeHtml(item.target_type || '-')}</td>
      <td>${escapeHtml(String(item.listed_count || 0))}</td>
      <td>${escapeHtml(String(item.add_failed_count || 0))}</td>
      <td>${escapeHtml(statusLabel)}</td>
      <td>
        <span>${escapeHtml(shortMessage(fullMessage))}</span>
        <button class="btn-secondary qap-message-btn" data-message="${escapeHtml(fullMessage)}" type="button">details_message</button>
      </td>
      <td>
        <button class="btn-secondary qap-details-query-btn" data-run-id="${noQuery}" type="button">Details Query</button>
        ${canRun ? `<button class="btn-primary qap-run-btn" data-run-id="${noQuery}" type="button">Run</button>` : ''}
        ${canDelete ? `<button class="btn-secondary qap-delete-run-btn" data-run-id="${noQuery}" type="button">Delete</button>` : ''}
      </td>
    `;
    elements.reportBody.appendChild(tr);
  });

  document.querySelectorAll('.qap-message-btn').forEach((button) => {
    button.addEventListener('click', () => {
      const rawMessage = button.getAttribute('data-message') || '-';
      const unescaped = rawMessage
        .replace(/&quot;/g, '"')
        .replace(/&#039;/g, "'")
        .replace(/&lt;/g, '<')
        .replace(/&gt;/g, '>')
        .replace(/&amp;/g, '&');
      showMessageModal(unescaped);
    });
  });

  document.querySelectorAll('.qap-details-query-btn').forEach((button) => {
    button.addEventListener('click', () => {
      const runId = Number(button.getAttribute('data-run-id'));
      if (!runId) {
        return;
      }
      window.location.href = `query-add-permission-detail.html?runId=${runId}`;
    });
  });

  document.querySelectorAll('.qap-run-btn').forEach((button) => {
    button.addEventListener('click', async () => {
      const runId = Number(button.getAttribute('data-run-id'));
      await runAddPermission(runId);
    });
  });

  document.querySelectorAll('.qap-delete-run-btn').forEach((button) => {
    button.addEventListener('click', async () => {
      const runId = Number(button.getAttribute('data-run-id'));
      await deletePermissionRun(runId);
    });
  });
}

async function loadReports(page = 1) {
  const pageSize = Number(elements.pageSize.value || 30);
  const params = new URLSearchParams({ page: String(page), pageSize: String(pageSize) });
  const result = await callApi(`/reports/query-permission?${params.toString()}`);
  renderReports(Array.isArray(result.items) ? result.items : []);
  updateQueryPaging(Number(result.page || 1), Number(result.total || 0), Number(result.pageSize || pageSize));
}

function startPageRefresh() {
  stopPageRefresh();
  pageTimer = setInterval(() => {
    loadReports(currentPage).catch((error) => {
      elements.runError.textContent = error.message;
    });
  }, 5000);
}

function bindEvents() {
  elements.logoutBtn.addEventListener('click', () => {
    stopPageRefresh();
    stopIdleTimeout();
    clearSession();
    navigateToLogin();
  });

  elements.templatePermissionFile.addEventListener('change', updateCreateButtonState);
  elements.queryText.addEventListener('input', () => {
    setQueryWarning();
    updateCreateButtonState();
  });
  elements.targetType.addEventListener('change', updateCreateButtonState);
  elements.useExample1Btn.addEventListener('click', () => fillExample(elements.example1));
  elements.useExample2Btn.addEventListener('click', () => fillExample(elements.example2));
  elements.useExample3Btn.addEventListener('click', () => fillExample(elements.example3));

  if (elements.guideManualBtn && elements.guideSettingBtn) {
    elements.guideManualBtn.addEventListener('click', () => switchGuideCategory('manual'));
    elements.guideSettingBtn.addEventListener('click', () => switchGuideCategory('setting'));
  }
  if (elements.reloadSettingsBtn) {
    elements.reloadSettingsBtn.addEventListener('click', () => {
      loadQueryPermissionSettings(true).catch((error) => {
        setStatus(elements.settingsStatus, 'Failed', 'failed');
        if (elements.settingsError) {
          elements.settingsError.textContent = error.message;
        }
      });
    });
  }
  if (elements.saveSettingsBtn) {
    elements.saveSettingsBtn.addEventListener('click', saveQueryPermissionSettings);
  }

  elements.testTemplateBtn.addEventListener('click', testTemplateQuery);
  elements.createTemplateBtn.addEventListener('click', createTemplate);
  elements.clearTemplateBtn.addEventListener('click', clearTemplateForm);

  elements.templatePageSize.addEventListener('change', () => {
    templateCurrentPage = 1;
    renderTemplateTable();
  });

  elements.templatePrevBtn.addEventListener('click', () => {
    if (templateCurrentPage > 1) {
      templateCurrentPage -= 1;
      renderTemplateTable();
    }
  });

  elements.templateNextBtn.addEventListener('click', () => {
    if (templateCurrentPage < templateTotalPages) {
      templateCurrentPage += 1;
      renderTemplateTable();
    }
  });

  elements.saveTemplateBtn.addEventListener('click', saveTemplate);
  elements.deleteTemplateBtn.addEventListener('click', deleteTemplate);

  elements.downloadTemplateFileBtn.addEventListener('click', async () => {
    try {
      await downloadTemplateFile();
    } catch (error) {
      elements.editTemplateError.textContent = error.message;
    }
  });

  elements.closeTemplateModalBtn.addEventListener('click', closeTemplateModal);

  elements.pageSize.addEventListener('change', () => {
    loadReports(1).catch((error) => {
      elements.runError.textContent = error.message;
    });
  });

  elements.prevPageBtn.addEventListener('click', () => {
    if (currentPage > 1) {
      loadReports(currentPage - 1).catch((error) => {
        elements.runError.textContent = error.message;
      });
    }
  });

  elements.nextPageBtn.addEventListener('click', () => {
    if (currentPage < totalPages) {
      loadReports(currentPage + 1).catch((error) => {
        elements.runError.textContent = error.message;
      });
    }
  });

  elements.closeMessageModalBtn.addEventListener('click', closeMessageModal);
}

async function init() {
  if (!token) {
    navigateToLogin();
    return;
  }

  const currentUser = localStorage.getItem('allopsUsername') || '-';
  elements.loginStatus.textContent = `Authenticated: ${currentUser}`;
  elements.loginStatus.className = 'status completed';

  bindEvents();
  startIdleTimeout();
  clearTemplateForm();
  switchGuideCategory('manual');

  await loadTemplates();
  await loadReports(1);
  await loadQueryPermissionSettings();
  startPageRefresh();
}

window.addEventListener('beforeunload', () => {
  stopPageRefresh();
  stopIdleTimeout();
});

init().catch((error) => {
  setStatus(elements.runStatus, 'Failed', 'failed');
  elements.runError.textContent = error.message;
});
