const apiBase = '/api/beflex-support';

const elements = {
  topStatus: document.getElementById('pmTopStatus'),
  logoutBtn: document.getElementById('pmLogoutBtn'),
  centerToggleBtn: document.getElementById('pmCenterToggleBtn'),

  customer: document.getElementById('pmCustomer'),
  environment: document.getElementById('pmEnvironment'),
  cronExpression: document.getElementById('pmCronExpression'),
  cronEnabled: document.getElementById('pmCronEnabled'),
  retentionDays: document.getElementById('pmRetentionDays'),
  editDetailsBtn: document.getElementById('pmEditDetailsBtn'),
  startPmBtn: document.getElementById('pmStartPmBtn'),
  detailsStatus: document.getElementById('pmDetailsStatus'),
  detailsError: document.getElementById('pmDetailsError'),
  detailsCard: document.getElementById('pmDetailsCard'),

  serverIp: document.getElementById('pmServerIp'),
  addServerBtn: document.getElementById('pmAddServerBtn'),
  serversBody: document.getElementById('pmServersBody'),
  startAllBtn: document.getElementById('pmStartAllBtn'),
  pauseAllBtn: document.getElementById('pmPauseAllBtn'),
  serverStatus: document.getElementById('pmServerStatus'),
  serverError: document.getElementById('pmServerError'),
  serverCard: document.getElementById('pmServerCard'),

  reportServerFilter: document.getElementById('pmReportServerFilter'),
  searchServerBtn: document.getElementById('pmSearchServerBtn'),
  clearSearchBtn: document.getElementById('pmClearSearchBtn'),
  reportsBody: document.getElementById('pmReportsBody'),
  pageSize: document.getElementById('pmPageSize'),
  prevPageBtn: document.getElementById('pmPrevPageBtn'),
  nextPageBtn: document.getElementById('pmNextPageBtn'),
  pagingLabel: document.getElementById('pmPagingLabel'),
  reportError: document.getElementById('pmReportError'),
  reportCard: document.getElementById('pmReportCard'),

  messageModal: document.getElementById('pmMessageModal'),
  messageDetail: document.getElementById('pmMessageDetail'),
  messageCloseBtn: document.getElementById('pmMessageCloseBtn')
};

let token = localStorage.getItem('allopsToken') || '';
let idleTimer = null;
let poller = null;
let detailsEditMode = false;
let pmEnabled = true;
let servers = [];
let reports = [];
let reportTotal = 0;
let currentPage = 1;
let currentServerId = null;

const idleTimeoutMinutes = Number(document.querySelector('.main-content')?.dataset?.sessionTimeoutMinutes || 30);
const idleTimeoutMs = idleTimeoutMinutes * 60 * 1000;
const activityEvents = ['click', 'keydown', 'touchstart', 'scroll', 'focus'];

function navigateToLogin() {
  window.location.href = 'index.html';
}

function clearSession() {
  localStorage.removeItem('allopsToken');
  localStorage.removeItem('allopsUsername');
}

function setStatus(target, text, kind = '') {
  if (!target) {
    return;
  }
  target.textContent = text;
  target.className = 'status';
  if (kind) {
    target.classList.add(kind);
  }
}

function setError(target, message) {
  if (!target) {
    return;
  }
  target.textContent = message || '';
}

function formatTime(value) {
  if (!value) {
    return '-';
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  const bangkokDate = new Date(date.getTime() + (7 * 60 * 60 * 1000));
  return `${bangkokDate.toISOString().slice(0, 19).replace('T', ' ')} +07`;
}

function formatDateToken(value) {
  if (!value) {
    return 'unknown-date';
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return 'unknown-date';
  }
  return date.toISOString().slice(0, 10);
}

function escapeHtml(value) {
  return String(value ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#039;');
}

async function callApi(path, options = {}) {
  const headers = options.headers || {};
  if (token) {
    headers.Authorization = `Bearer ${token}`;
  }

  const response = await fetch(`${apiBase}${path}`, {
    ...options,
    headers
  });

  const data = await response.json().catch(() => ({}));

  if (response.status === 401) {
    stopPolling();
    stopIdleTimeout();
    clearSession();
    sessionStorage.setItem('allopsSessionExpired', '1');
    navigateToLogin();
    throw new Error('Session expired');
  }

  if (!response.ok) {
    throw new Error(data.message || `Request failed: ${response.status}`);
  }

  return data;
}

async function downloadWithAuth(path, fallbackName) {
  const headers = {};
  if (token) {
    headers.Authorization = `Bearer ${token}`;
  }

  const response = await fetch(`${apiBase}${path}`, { headers });
  if (!response.ok) {
    const data = await response.json().catch(() => ({}));
    throw new Error(data.message || `Download failed: ${response.status}`);
  }

  const disposition = response.headers.get('content-disposition') || '';
  const matched = disposition.match(/filename="([^"]+)"/i);
  const filename = matched?.[1] || fallbackName;

  const blob = await response.blob();
  const objectUrl = URL.createObjectURL(blob);
  const anchor = document.createElement('a');
  anchor.href = objectUrl;
  anchor.download = filename;
  document.body.appendChild(anchor);
  anchor.click();
  document.body.removeChild(anchor);
  URL.revokeObjectURL(objectUrl);
}

function readConfigForm() {
  return {
    customer: elements.customer.value.trim(),
    environment: elements.environment.value.trim(),
    cronExpression: elements.cronExpression.value.trim(),
    cronEnabled: pmEnabled ? elements.cronEnabled.checked : false,
    retentionDays: Number(elements.retentionDays.value || 30),
    betaEnabled: pmEnabled
  };
}

function fillConfigForm(config = {}) {
  elements.customer.value = config.customer || '';
  elements.environment.value = config.environment || '';
  elements.cronExpression.value = config.cronExpression || '';
  elements.cronEnabled.checked = Boolean(config.cronEnabled);
  elements.retentionDays.value = String(config.retentionDays || 30);
}

function applyDetailsEditable(editable) {
  detailsEditMode = Boolean(editable);
  elements.customer.disabled = !editable;
  elements.environment.disabled = !editable;
  elements.cronExpression.disabled = !editable;
  elements.cronEnabled.disabled = !editable;
  elements.retentionDays.disabled = !editable;

  elements.editDetailsBtn.textContent = editable ? 'Save details' : 'Edit details';
  setStatus(elements.detailsStatus, editable ? 'Editing' : 'Read only', editable ? 'progress' : 'completed');
}

function applyCenterState(enabled) {
  pmEnabled = Boolean(enabled);
  elements.centerToggleBtn.textContent = pmEnabled ? 'ON' : 'OFF';
  elements.centerToggleBtn.classList.toggle('off', !pmEnabled);

  const opDisabled = !pmEnabled;
  elements.startPmBtn.disabled = opDisabled;
  elements.addServerBtn.disabled = opDisabled;
  elements.startAllBtn.disabled = opDisabled;
  elements.pauseAllBtn.disabled = opDisabled;
  elements.editDetailsBtn.disabled = opDisabled;

  [elements.detailsCard, elements.serverCard, elements.reportCard].forEach((card) => {
    if (!card) {
      return;
    }
    card.classList.toggle('hidden', !pmEnabled);
  });

  if (!pmEnabled) {
    detailsEditMode = false;
    elements.customer.value = '';
    elements.environment.value = '';
    elements.cronExpression.value = '';
    elements.cronEnabled.checked = false;
    elements.retentionDays.value = '30';
    elements.serverIp.value = '';
    servers = [];
    reports = [];
    reportTotal = 0;
    currentPage = 1;
    currentServerId = null;
    renderServers();
    renderServerFilter();
    renderReports();
    closeMessageModal();
    stopPolling();
  }

  setStatus(elements.topStatus, pmEnabled ? 'PM ON' : 'PM OFF', pmEnabled ? 'completed' : 'failed');
}

function renderServers() {
  elements.serversBody.innerHTML = '';

  if (!servers.length) {
    elements.serversBody.innerHTML = '<tr><td colspan="6">No server</td></tr>';
    return;
  }

  for (const item of servers) {
    const tr = document.createElement('tr');
    const actionLabel = item.pm_enabled ? 'Pause' : 'Start';

    tr.innerHTML = `
      <td>${escapeHtml(item.env || '-')}</td>
      <td>${item.server_id}</td>
      <td>${escapeHtml(item.server_name || '-')}</td>
      <td>${escapeHtml(item.server_ip || '-')}</td>
      <td>${escapeHtml(item.status || '-')}</td>
      <td>
        <button class="btn-secondary pm-mini-btn" data-server-action="toggle" data-server-id="${item.server_id}" data-server-enabled="${item.pm_enabled ? '1' : '0'}">${actionLabel}</button>
        <button class="btn-secondary pm-mini-btn" data-server-action="delete" data-server-id="${item.server_id}">Delete</button>
      </td>
    `;

    elements.serversBody.appendChild(tr);
  }
}

function renderServerFilter() {
  const selected = elements.reportServerFilter.value;
  elements.reportServerFilter.innerHTML = '';

  const allOption = document.createElement('option');
  allOption.value = '';
  allOption.textContent = 'All Server';
  elements.reportServerFilter.appendChild(allOption);

  for (const item of servers) {
    const option = document.createElement('option');
    option.value = String(item.server_id);
    option.textContent = item.server_name || item.server_key || `Server ${item.server_id}`;
    elements.reportServerFilter.appendChild(option);
  }

  if (selected && servers.some((item) => String(item.server_id) === selected)) {
    elements.reportServerFilter.value = selected;
  }
}

function buildReportFilename(row) {
  const customerCode = row.customer_code || 'customer';
  const env = row.env || 'env';
  const serverName = row.server_name || row.server_key || 'server';
  const dateToken = formatDateToken(row.pm_date);
  return `${customerCode}-${env}-${serverName}-${dateToken}`;
}

function renderReports() {
  elements.reportsBody.innerHTML = '';

  if (!reports.length) {
    elements.reportsBody.innerHTML = '<tr><td colspan="7">No report data</td></tr>';
    return;
  }

  const baseNo = ((currentPage - 1) * Number(elements.pageSize.value || 30));
  for (let i = 0; i < reports.length; i += 1) {
    const row = reports[i];
    const no = baseNo + i + 1;
    const fileBase = buildReportFilename(row);
    const message = row.error_detail || (row.status === 'SUCCESS' ? 'OK' : row.status || '-');
    const shortMessage = message.length > 40 ? `${message.slice(0, 40)}...` : message;

    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td>${no}</td>
      <td>${escapeHtml(row.server_name || '-')}</td>
      <td>${escapeHtml(fileBase)}</td>
      <td>${escapeHtml(formatTime(row.pm_date))}</td>
      <td>${escapeHtml(row.status || '-')}</td>
      <td>
        ${escapeHtml(shortMessage)}
        ${message ? `<button class="btn-secondary pm-mini-btn" data-report-action="view-message" data-message="${escapeHtml(message)}">👁</button>` : ''}
      </td>
      <td>
        ${row.snapshot_id ? `<button class="btn-secondary pm-mini-btn" data-report-action="download-json" data-snapshot-id="${row.snapshot_id}">Download json</button>` : '-'}
        ${row.snapshot_id ? `<button class="btn-secondary pm-mini-btn" data-report-action="download-txt" data-snapshot-id="${row.snapshot_id}">Download txt</button>` : ''}
      </td>
    `;

    elements.reportsBody.appendChild(tr);
  }

  const totalPage = Math.max(1, Math.ceil(reportTotal / Number(elements.pageSize.value || 30)));
  elements.prevPageBtn.disabled = currentPage <= 1;
  elements.nextPageBtn.disabled = currentPage >= totalPage;
  elements.pagingLabel.textContent = `Page ${currentPage}/${totalPage} (${reportTotal})`;
}

function openMessageModal(message) {
  elements.messageDetail.textContent = message || '-';
  elements.messageModal.classList.remove('hidden');
}

function closeMessageModal() {
  elements.messageModal.classList.add('hidden');
}

async function loadConfig() {
  const data = await callApi('/pm/config');
  fillConfigForm(data.config || {});
  applyCenterState(data.config?.betaEnabled !== false);
  applyDetailsEditable(false);
}

async function loadServers() {
  if (!pmEnabled) {
    return;
  }
  const data = await callApi('/pm/center/servers');
  servers = Array.isArray(data.items) ? data.items : [];
  renderServers();
  renderServerFilter();
}

async function loadReports() {
  if (!pmEnabled) {
    return;
  }
  const pageSize = Number(elements.pageSize.value || 30);
  const query = new URLSearchParams();
  query.set('page', String(currentPage));
  query.set('pageSize', String(pageSize));
  if (currentServerId) {
    query.set('serverId', String(currentServerId));
  }

  const data = await callApi(`/pm/center/reports?${query.toString()}`);
  reports = Array.isArray(data.items) ? data.items : [];
  reportTotal = Number(data.total || 0);
  renderReports();
}

async function refreshPageData() {
  await loadConfig();
  if (!pmEnabled) {
    return;
  }
  await Promise.all([loadServers(), loadReports()]);
}

async function onToggleCenter() {
  setError(elements.detailsError, '');
  try {
    const nextEnabled = !pmEnabled;
    let payload = readConfigForm();
    if (!pmEnabled && nextEnabled) {
      const current = await callApi('/pm/config');
      payload = {
        customer: current?.config?.customer || '',
        environment: current?.config?.environment || '',
        cronExpression: current?.config?.cronExpression || '',
        cronEnabled: Boolean(current?.config?.cronEnabled),
        retentionDays: Number(current?.config?.retentionDays || 30)
      };
    }
    payload.betaEnabled = nextEnabled;
    payload.cronEnabled = nextEnabled ? payload.cronEnabled : false;

    await callApi('/pm/config', {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    });

    await refreshPageData();
  } catch (error) {
    setError(elements.detailsError, error.message);
  }
}

async function onEditDetails() {
  setError(elements.detailsError, '');

  if (!detailsEditMode) {
    applyDetailsEditable(true);
    return;
  }

  try {
    const payload = readConfigForm();
    await callApi('/pm/config', {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    });

    applyDetailsEditable(false);
    setStatus(elements.detailsStatus, 'Saved', 'completed');
    await loadConfig();
  } catch (error) {
    setError(elements.detailsError, error.message);
    setStatus(elements.detailsStatus, 'Failed', 'failed');
  }
}

async function onStartPm() {
  if (!pmEnabled) {
    return;
  }
  setError(elements.detailsError, '');
  try {
    setStatus(elements.detailsStatus, 'Queueing...', 'progress');
    await callApi('/pm/center/start-manual', { method: 'POST' });
    setStatus(elements.detailsStatus, 'Queued', 'completed');
    await loadReports();
  } catch (error) {
    setError(elements.detailsError, error.message);
    setStatus(elements.detailsStatus, 'Failed', 'failed');
  }
}

async function onAddServer() {
  if (!pmEnabled) {
    return;
  }
  setError(elements.serverError, '');
  const serverIp = elements.serverIp.value.trim();
  if (!serverIp) {
    setError(elements.serverError, 'Server IP is required');
    return;
  }

  try {
    setStatus(elements.serverStatus, 'Adding...', 'progress');
    await callApi('/pm/center/servers', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ serverIp })
    });
    elements.serverIp.value = '';
    await loadServers();
    setStatus(elements.serverStatus, 'Added', 'completed');
  } catch (error) {
    setError(elements.serverError, error.message);
    setStatus(elements.serverStatus, 'Failed', 'failed');
  }
}

async function onSetAllServers(enabled) {
  if (!pmEnabled) {
    return;
  }
  setError(elements.serverError, '');
  try {
    setStatus(elements.serverStatus, enabled ? 'Starting all...' : 'Pausing all...', 'progress');
    await callApi('/pm/center/servers/state-all', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ enabled })
    });
    await loadServers();
    setStatus(elements.serverStatus, enabled ? 'All Active' : 'All Stop', 'completed');
  } catch (error) {
    setError(elements.serverError, error.message);
    setStatus(elements.serverStatus, 'Failed', 'failed');
  }
}

async function onServerTableClick(event) {
  if (!pmEnabled) {
    return;
  }
  const button = event.target.closest('button[data-server-action]');
  if (!button) {
    return;
  }

  const action = button.dataset.serverAction;
  const serverId = Number(button.dataset.serverId);
  if (!serverId) {
    return;
  }

  setError(elements.serverError, '');

  try {
    if (action === 'toggle') {
      const currentlyEnabled = button.dataset.serverEnabled === '1';
      await callApi(`/pm/center/servers/${serverId}/state`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ enabled: !currentlyEnabled })
      });
    }

    if (action === 'delete') {
      await callApi(`/pm/center/servers/${serverId}`, { method: 'DELETE' });
    }

    await Promise.all([loadServers(), loadReports()]);
    setStatus(elements.serverStatus, 'Updated', 'completed');
  } catch (error) {
    setError(elements.serverError, error.message);
    setStatus(elements.serverStatus, 'Failed', 'failed');
  }
}

async function onSearchServer() {
  if (!pmEnabled) {
    return;
  }
  currentServerId = elements.reportServerFilter.value ? Number(elements.reportServerFilter.value) : null;
  currentPage = 1;
  setError(elements.reportError, '');
  try {
    await loadReports();
  } catch (error) {
    setError(elements.reportError, error.message);
  }
}

async function onClearSearch() {
  if (!pmEnabled) {
    return;
  }
  currentServerId = null;
  currentPage = 1;
  elements.reportServerFilter.value = '';
  setError(elements.reportError, '');
  try {
    await loadReports();
  } catch (error) {
    setError(elements.reportError, error.message);
  }
}

async function onReportTableClick(event) {
  if (!pmEnabled) {
    return;
  }
  const button = event.target.closest('button[data-report-action]');
  if (!button) {
    return;
  }

  const action = button.dataset.reportAction;

  if (action === 'view-message') {
    openMessageModal(button.dataset.message || '');
    return;
  }

  const snapshotId = Number(button.dataset.snapshotId);
  if (!snapshotId) {
    return;
  }

  setError(elements.reportError, '');

  try {
    if (action === 'download-json') {
      await downloadWithAuth(`/pm/snapshots/${snapshotId}/download-json`, `pm-${snapshotId}.json`);
    }
    if (action === 'download-txt') {
      await downloadWithAuth(`/pm/snapshots/${snapshotId}/download-txt`, `pm-${snapshotId}.txt`);
    }
  } catch (error) {
    setError(elements.reportError, error.message);
  }
}

function startPolling() {
  if (!pmEnabled) {
    return;
  }
  stopPolling();
  poller = setInterval(() => {
    Promise.all([loadServers(), loadReports()]).catch((error) => {
      setError(elements.reportError, error.message);
    });
  }, 15000);
}

function stopPolling() {
  if (poller) {
    clearInterval(poller);
    poller = null;
  }
}

function stopIdleTimeout() {
  if (idleTimer) {
    clearTimeout(idleTimer);
    idleTimer = null;
  }
}

function onIdleTimeout() {
  stopPolling();
  stopIdleTimeout();
  clearSession();
  sessionStorage.setItem('allopsSessionExpired', '1');
  navigateToLogin();
}

function resetIdleTimeout() {
  if (!token) {
    return;
  }
  stopIdleTimeout();
  idleTimer = setTimeout(onIdleTimeout, idleTimeoutMs);
}

function startIdleTimeout() {
  activityEvents.forEach((eventName) => {
    window.addEventListener(eventName, resetIdleTimeout, { passive: true });
  });
  resetIdleTimeout();
}

function bindEvents() {
  elements.logoutBtn.addEventListener('click', () => {
    stopPolling();
    stopIdleTimeout();
    clearSession();
    navigateToLogin();
  });

  elements.centerToggleBtn.addEventListener('click', () => {
    onToggleCenter().catch((error) => {
      setError(elements.detailsError, error.message);
    });
  });

  elements.editDetailsBtn.addEventListener('click', () => {
    onEditDetails().catch((error) => {
      setError(elements.detailsError, error.message);
    });
  });

  elements.startPmBtn.addEventListener('click', () => {
    onStartPm().catch((error) => {
      setError(elements.detailsError, error.message);
    });
  });

  elements.addServerBtn.addEventListener('click', () => {
    onAddServer().catch((error) => {
      setError(elements.serverError, error.message);
    });
  });

  elements.startAllBtn.addEventListener('click', () => {
    onSetAllServers(true).catch((error) => {
      setError(elements.serverError, error.message);
    });
  });

  elements.pauseAllBtn.addEventListener('click', () => {
    onSetAllServers(false).catch((error) => {
      setError(elements.serverError, error.message);
    });
  });

  elements.serversBody.addEventListener('click', (event) => {
    onServerTableClick(event).catch((error) => {
      setError(elements.serverError, error.message);
    });
  });

  elements.searchServerBtn.addEventListener('click', () => {
    onSearchServer().catch((error) => {
      setError(elements.reportError, error.message);
    });
  });

  elements.clearSearchBtn.addEventListener('click', () => {
    onClearSearch().catch((error) => {
      setError(elements.reportError, error.message);
    });
  });

  elements.pageSize.addEventListener('change', () => {
    currentPage = 1;
    loadReports().catch((error) => {
      setError(elements.reportError, error.message);
    });
  });

  elements.prevPageBtn.addEventListener('click', () => {
    if (currentPage > 1) {
      currentPage -= 1;
      loadReports().catch((error) => {
        setError(elements.reportError, error.message);
      });
    }
  });

  elements.nextPageBtn.addEventListener('click', () => {
    const totalPage = Math.max(1, Math.ceil(reportTotal / Number(elements.pageSize.value || 30)));
    if (currentPage < totalPage) {
      currentPage += 1;
      loadReports().catch((error) => {
        setError(elements.reportError, error.message);
      });
    }
  });

  elements.reportsBody.addEventListener('click', (event) => {
    onReportTableClick(event).catch((error) => {
      setError(elements.reportError, error.message);
    });
  });

  elements.messageCloseBtn.addEventListener('click', closeMessageModal);
  elements.messageModal.addEventListener('click', (event) => {
    if (event.target === elements.messageModal) {
      closeMessageModal();
    }
  });
}

function init() {
  if (!token) {
    navigateToLogin();
    return;
  }

  bindEvents();
  startIdleTimeout();

  refreshPageData()
    .then(() => {
      setStatus(elements.serverStatus, 'Ready', 'completed');
      setStatus(elements.detailsStatus, 'Read only', 'completed');
      if (pmEnabled) {
        startPolling();
      }
    })
    .catch((error) => {
      setError(elements.reportError, error.message);
    });
}

window.addEventListener('beforeunload', () => {
  stopPolling();
  stopIdleTimeout();
});

init();
