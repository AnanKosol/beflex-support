const apiBase = '/api/beflex-support';

const elements = {
  loginStatus: document.getElementById('auditLoginStatus'),
  logoutBtn: document.getElementById('auditLogoutBtn'),
  serviceFilter: document.getElementById('auditServiceFilter'),
  statusFilter: document.getElementById('auditStatusFilter'),
  usernameFilter: document.getElementById('auditUsernameFilter'),
  limitFilter: document.getElementById('auditLimitFilter'),
  searchBtn: document.getElementById('auditSearchBtn'),
  clearBtn: document.getElementById('auditClearBtn'),
  refreshBtn: document.getElementById('auditRefreshBtn'),
  status: document.getElementById('auditStatus'),
  error: document.getElementById('auditError'),
  serviceSummaryBody: document.getElementById('auditServiceSummaryBody'),
  eventsBody: document.getElementById('auditEventsBody')
};

let token = localStorage.getItem('allopsToken') || '';
let idleTimer = null;

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

function setStatus(text, kind = '') {
  elements.status.textContent = text;
  elements.status.className = 'status';
  if (kind) {
    elements.status.classList.add(kind);
  }
}

function setError(message) {
  elements.error.textContent = message || '';
}

function escapeHtml(value) {
  return String(value ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#039;');
}

function formatTime(value) {
  if (!value) {
    return '-';
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return String(value);
  }
  const bangkokDate = new Date(date.getTime() + (7 * 60 * 60 * 1000));
  return `${bangkokDate.toISOString().slice(0, 19).replace('T', ' ')} +07`;
}

async function callApi(path) {
  const headers = {};
  if (token) {
    headers.Authorization = `Bearer ${token}`;
  }

  const response = await fetch(`${apiBase}${path}`, { headers });
  const data = await response.json().catch(() => ({}));

  if (response.status === 401) {
    clearSession();
    navigateToLogin();
    throw new Error('Session expired');
  }

  if (!response.ok) {
    throw new Error(data.message || `Request failed: ${response.status}`);
  }

  return data;
}

function buildFilterQuery() {
  const query = new URLSearchParams();
  const service = elements.serviceFilter.value;
  const status = elements.statusFilter.value;
  const username = elements.usernameFilter.value.trim();
  const limit = Number(elements.limitFilter.value || 100);

  if (service) {
    query.set('service_name', service);
  }
  if (status) {
    query.set('status', status);
  }
  if (username) {
    query.set('username', username);
  }
  query.set('limit', String(limit));

  return query.toString();
}

function renderServiceFilter(summaryItems) {
  const previousValue = elements.serviceFilter.value;
  const serviceNames = Array.from(new Set((summaryItems || []).map((item) => String(item.service_name || '').trim()).filter(Boolean))).sort();

  elements.serviceFilter.innerHTML = '<option value="">All</option>';
  serviceNames.forEach((serviceName) => {
    const option = document.createElement('option');
    option.value = serviceName;
    option.textContent = serviceName;
    elements.serviceFilter.appendChild(option);
  });

  if (previousValue && serviceNames.includes(previousValue)) {
    elements.serviceFilter.value = previousValue;
  }
}

function renderSummary(items) {
  elements.serviceSummaryBody.innerHTML = '';

  if (!items.length) {
    elements.serviceSummaryBody.innerHTML = '<tr><td colspan="3">No data</td></tr>';
    return;
  }

  items.forEach((item) => {
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td>${escapeHtml(item.service_name || '-')}</td>
      <td>${escapeHtml(item.status || '-')}</td>
      <td>${escapeHtml(item.total || '0')}</td>
    `;
    elements.serviceSummaryBody.appendChild(tr);
  });
}

function renderEvents(items) {
  elements.eventsBody.innerHTML = '';

  if (!items.length) {
    elements.eventsBody.innerHTML = '<tr><td colspan="8">No audit events</td></tr>';
    return;
  }

  items.forEach((item) => {
    const metadata = item.metadata && typeof item.metadata === 'object'
      ? JSON.stringify(item.metadata, null, 2)
      : '';

    const entity = [item.entity_type, item.entity_id].filter(Boolean).join(' / ') || '-';
    const message = item.message || '-';
    const shortMessage = message.length > 120 ? `${message.slice(0, 120)}...` : message;

    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td>${escapeHtml(formatTime(item.event_time))}</td>
      <td>${escapeHtml(item.service_name || '-')}</td>
      <td>${escapeHtml(item.username || '-')}</td>
      <td>${escapeHtml(item.action_type || '-')}</td>
      <td>${escapeHtml(entity)}</td>
      <td>${escapeHtml(item.status || '-')}</td>
      <td title="${escapeHtml(message)}">${escapeHtml(shortMessage)}</td>
      <td>
        ${metadata
          ? `<details><summary>View</summary><div class="logs">${escapeHtml(metadata)}</div></details>`
          : '-'}
      </td>
    `;
    elements.eventsBody.appendChild(tr);
  });
}

async function loadAuditPage() {
  setError('');
  setStatus('Loading...', 'progress');

  try {
    const [summaryResult, eventsResult] = await Promise.all([
      callApi('/reports/audit/services'),
      callApi(`/reports/audit?${buildFilterQuery()}`)
    ]);

    const summaryItems = Array.isArray(summaryResult.items) ? summaryResult.items : [];
    const eventItems = Array.isArray(eventsResult.items) ? eventsResult.items : [];

    renderServiceFilter(summaryItems);
    renderSummary(summaryItems);
    renderEvents(eventItems);
    setStatus(`Loaded ${eventItems.length} events`, 'completed');
  } catch (error) {
    setError(error.message);
    setStatus('Failed', 'failed');
  }
}

function stopIdleTimeout() {
  if (idleTimer) {
    clearTimeout(idleTimer);
    idleTimer = null;
  }
}

function onIdleTimeout() {
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
    stopIdleTimeout();
    clearSession();
    navigateToLogin();
  });

  elements.searchBtn.addEventListener('click', () => {
    loadAuditPage();
  });

  elements.refreshBtn.addEventListener('click', () => {
    loadAuditPage();
  });

  elements.clearBtn.addEventListener('click', () => {
    elements.serviceFilter.value = '';
    elements.statusFilter.value = '';
    elements.usernameFilter.value = '';
    elements.limitFilter.value = '100';
    loadAuditPage();
  });
}

function init() {
  if (!token) {
    navigateToLogin();
    return;
  }

  bindEvents();
  startIdleTimeout();
  loadAuditPage();
}

window.addEventListener('beforeunload', () => {
  stopIdleTimeout();
});

init();