const apiBase = '/api/beflex-support';

const elements = {
  loginStatus: document.getElementById('qsLoginStatus'),
  logoutBtn: document.getElementById('qsLogoutBtn'),
  queryInput: document.getElementById('qsQueryInput'),
  runBtn: document.getElementById('qsRunBtn'),
  runStatus: document.getElementById('qsRunStatus'),
  runError: document.getElementById('qsRunError'),
  queryWarning: document.getElementById('qsQueryWarning'),
  runMeta: document.getElementById('qsRunMeta'),
  realtimeBox: document.getElementById('qsRealtimeBox'),
  reportBody: document.getElementById('qsReportBody'),
  selectAllPageCb: document.getElementById('qsSelectAllPageCb'),
  pageSize: document.getElementById('qsPageSize'),
  checkAllBtn: document.getElementById('qsCheckAllBtn'),
  uncheckPageBtn: document.getElementById('qsUncheckPageBtn'),
  clearSelectedBtn: document.getElementById('qsClearSelectedBtn'),
  exportSelectedCsvBtn: document.getElementById('qsExportSelectedCsvBtn'),
  exportCsvBtn: document.getElementById('qsExportCsvBtn'),
  prevPageBtn: document.getElementById('qsPrevPageBtn'),
  nextPageBtn: document.getElementById('qsNextPageBtn'),
  selectedCount: document.getElementById('qsSelectedCount'),
  pagingLabel: document.getElementById('qsPagingLabel'),
  useExample1Btn: document.getElementById('qsUseExample1Btn'),
  useExample2Btn: document.getElementById('qsUseExample2Btn'),
  useExample3Btn: document.getElementById('qsUseExample3Btn'),
  example1: document.getElementById('qsExample1'),
  example2: document.getElementById('qsExample2'),
  example3: document.getElementById('qsExample3')
};

let token = localStorage.getItem('allopsToken') || '';
let currentRunId = null;
let pollingTimer = null;
let idleTimer = null;
let currentPage = 1;
let totalPages = 1;
let currentPageRunIds = [];
const selectedRunIds = new Set();

const idleTimeoutMinutes = Number(document.querySelector('.main-content')?.dataset?.sessionTimeoutMinutes || 30);
const idleTimeoutMs = idleTimeoutMinutes * 60 * 1000;
const activityEvents = ['click', 'keydown', 'touchstart', 'scroll', 'focus'];
const finalStatuses = new Set(['COMPLETED', 'FAILED']);

function navigateToLogin() {
  window.location.href = 'index.html';
}

function clearSession() {
  localStorage.removeItem('allopsToken');
  localStorage.removeItem('allopsUsername');
}

function stopPolling() {
  if (pollingTimer) {
    clearInterval(pollingTimer);
    pollingTimer = null;
  }
}

function stopIdleTimeout() {
  if (idleTimer) {
    clearTimeout(idleTimer);
    idleTimer = null;
  }
}

function setRunStatus(text, kind = '') {
  elements.runStatus.textContent = text;
  elements.runStatus.className = 'status';
  if (kind) {
    elements.runStatus.classList.add(kind);
  }
}

function setRunError(message) {
  elements.runError.textContent = message || '';
}

function looksLikeJsonEscapedQuery(query) {
  return /\\"/.test(String(query || '')) || /^"[\s\S]/.test(String(query || '').trim());
}

function setQueryWarning(query) {
  if (!elements.queryWarning) {
    return;
  }

  const hasEscapedQuotes = /\\"/.test(String(query || ''));
  const hasOuterQuote = /^"[\s\S]/.test(String(query || '').trim());
  if (hasEscapedQuotes || hasOuterQuote) {
    let msg = 'Warning: Query มีรูปแบบ JSON-encoded —';
    if (hasOuterQuote) msg += ' ระบบจะตัด outer " ที่ครอบอยู่ออก';
    if (hasEscapedQuotes) msg += (hasOuterQuote ? ' และ' : '') + ' แปลง \\" เป็น " อัตโนมัติ';
    msg += ' ก่อนส่งไป Alfresco';
    elements.queryWarning.textContent = msg;
    elements.queryWarning.classList.remove('hidden');
    return;
  }

  elements.queryWarning.textContent = '';
  elements.queryWarning.classList.add('hidden');
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

function toDisplayNumber(value, fallback = '0') {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    return fallback;
  }
  return String(parsed);
}

function shortenQuery(query) {
  const text = String(query || '').trim();
  if (!text) {
    return '-';
  }
  if (text.length <= 130) {
    return text;
  }
  return `${text.slice(0, 130)}...`;
}

async function callApi(path, options = {}) {
  const headers = options.headers || {};
  if (token) {
    headers.Authorization = `Bearer ${token}`;
  }

  if (options.body && !headers['Content-Type']) {
    headers['Content-Type'] = 'application/json';
  }

  const response = await fetch(`${apiBase}${path}`, {
    ...options,
    headers
  });

  const data = await response.json().catch(() => ({}));

  if (response.status === 401) {
    clearSession();
    stopPolling();
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

function updateSelectedCount() {
  if (!elements.selectedCount) {
    return;
  }
  elements.selectedCount.textContent = `Selected ${selectedRunIds.size}`;
}

function syncSelectAllPageCheckbox() {
  if (!elements.selectAllPageCb) {
    return;
  }

  if (!currentPageRunIds.length) {
    elements.selectAllPageCb.checked = false;
    elements.selectAllPageCb.indeterminate = false;
    return;
  }

  const selectedOnPage = currentPageRunIds.filter((id) => selectedRunIds.has(id)).length;
  elements.selectAllPageCb.checked = selectedOnPage > 0 && selectedOnPage === currentPageRunIds.length;
  elements.selectAllPageCb.indeterminate = selectedOnPage > 0 && selectedOnPage < currentPageRunIds.length;
}

function setCurrentPageSelection(checked) {
  currentPageRunIds.forEach((id) => {
    if (checked) {
      selectedRunIds.add(id);
    } else {
      selectedRunIds.delete(id);
    }
  });

  document.querySelectorAll('.qs-row-select').forEach((input) => {
    input.checked = checked;
  });

  updateSelectedCount();
  syncSelectAllPageCheckbox();
}

async function downloadReportCsv(selectedIds = null) {
  const headers = {};
  if (token) {
    headers.Authorization = `Bearer ${token}`;
  }

  const options = { headers, method: 'GET' };

  if (Array.isArray(selectedIds)) {
    options.method = 'POST';
    options.headers = {
      ...headers,
      'Content-Type': 'application/json'
    };
    options.body = JSON.stringify({ ids: selectedIds });
  }

  const response = await fetch(`${apiBase}/reports/query-sizing/export.csv`, options);

  if (response.status === 401) {
    clearSession();
    stopPolling();
    stopIdleTimeout();
    sessionStorage.setItem('allopsSessionExpired', '1');
    navigateToLogin();
    throw new Error('Session expired');
  }

  if (!response.ok) {
    let message = `Export failed: ${response.status}`;
    try {
      const data = await response.json();
      if (data?.message) {
        message = data.message;
      }
    } catch (_error) {
      // keep fallback message
    }
    throw new Error(message);
  }

  const blob = await response.blob();
  const disposition = response.headers.get('content-disposition') || '';
  const matched = disposition.match(/filename="([^"]+)"/i);
  const filename = matched?.[1] || 'query-sizing-report.csv';

  const objectUrl = URL.createObjectURL(blob);
  const anchor = document.createElement('a');
  anchor.href = objectUrl;
  anchor.download = filename;
  document.body.appendChild(anchor);
  anchor.click();
  document.body.removeChild(anchor);
  URL.revokeObjectURL(objectUrl);
}

function renderRealtime(run) {
  if (!run) {
    elements.runMeta.textContent = 'No run selected';
    elements.realtimeBox.textContent = 'No realtime data';
    return;
  }

  elements.runMeta.textContent = `Run #${run.id} | user: ${run.username || '-'} | queried: ${formatTime(run.queried_at)}`;
  elements.realtimeBox.textContent = [
    `Status: ${run.status || '-'}`,
    `Total Files: ${toDisplayNumber(run.total_files)}`,
    `Total size (MB): ${toDisplayNumber(run.total_size_mb)}`,
    `Total size (GB): ${toDisplayNumber(run.total_size_gb)}`,
    `Message: ${run.message || '-'}`
  ].join('\n');

  if (run.status === 'FAILED') {
    setRunStatus('Failed', 'failed');
  } else if (run.status === 'COMPLETED') {
    setRunStatus('Completed', 'completed');
  } else {
    setRunStatus(run.status || 'In Progress', 'progress');
  }
}

function renderReports(items) {
  elements.reportBody.innerHTML = '';
  currentPageRunIds = [];

  if (!items.length) {
    elements.reportBody.innerHTML = '<tr><td colspan="9">No report data</td></tr>';
    updateSelectedCount();
    syncSelectAllPageCheckbox();
    return;
  }

  items.forEach((item) => {
    const runId = Number(item.id);
    if (Number.isFinite(runId) && runId > 0) {
      currentPageRunIds.push(runId);
    }

    const tr = document.createElement('tr');
    const statusText = item.status || '-';
    const message = item.message || '-';
    const checked = selectedRunIds.has(runId) ? 'checked' : '';

    tr.innerHTML = `
      <td class="qs-select-cell"><input class="qs-row-select" data-run-id="${item.id}" type="checkbox" ${checked} /></td>
      <td>${escapeHtml(formatTime(item.queried_at))}</td>
      <td title="${escapeHtml(item.query_text || '')}">${escapeHtml(shortenQuery(item.query_text))}</td>
      <td>${escapeHtml(item.username || '-')}</td>
      <td>${escapeHtml(toDisplayNumber(item.total_files))}</td>
      <td>${escapeHtml(toDisplayNumber(item.total_size_mb, '0.00'))}</td>
      <td>${escapeHtml(toDisplayNumber(item.total_size_gb, '0.0000'))}</td>
      <td title="${escapeHtml(message)}">${escapeHtml(statusText)} | ${escapeHtml(shortenQuery(message))}</td>
      <td>
        <button class="btn-secondary qs-delete-btn" data-run-id="${item.id}" type="button">Delete</button>
      </td>
    `;

    elements.reportBody.appendChild(tr);
  });

  document.querySelectorAll('.qs-row-select').forEach((input) => {
    input.addEventListener('change', () => {
      const runId = Number(input.getAttribute('data-run-id'));
      if (!Number.isFinite(runId) || runId <= 0) {
        return;
      }

      if (input.checked) {
        selectedRunIds.add(runId);
      } else {
        selectedRunIds.delete(runId);
      }

      updateSelectedCount();
      syncSelectAllPageCheckbox();
    });
  });

  document.querySelectorAll('.qs-delete-btn').forEach((button) => {
    button.addEventListener('click', async () => {
      const runId = Number(button.getAttribute('data-run-id'));
      if (!Number.isFinite(runId) || runId <= 0) {
        return;
      }

      const confirmed = window.confirm(`Delete query report #${runId} ?`);
      if (!confirmed) {
        return;
      }

      try {
        await callApi(`/reports/query-sizing/${runId}`, { method: 'DELETE' });
        selectedRunIds.delete(runId);
        if (currentRunId === runId) {
          currentRunId = null;
          stopPolling();
          renderRealtime(null);
        }
        await loadReports(currentPage);
      } catch (error) {
        setRunError(error.message);
      }
    });
  });

  updateSelectedCount();
  syncSelectAllPageCheckbox();
}

function updatePaging(page, total, pageSize) {
  currentPage = page;
  totalPages = Math.max(1, Math.ceil(total / pageSize));
  elements.pagingLabel.textContent = `Page ${currentPage} / ${totalPages}`;
  elements.prevPageBtn.disabled = currentPage <= 1;
  elements.nextPageBtn.disabled = currentPage >= totalPages;
}

async function loadReports(page = 1) {
  const pageSize = Number(elements.pageSize.value || 30);
  const query = new URLSearchParams({
    page: String(page),
    pageSize: String(pageSize)
  });

  const result = await callApi(`/reports/query-sizing?${query.toString()}`);
  const items = Array.isArray(result.items) ? result.items : [];
  renderReports(items);
  updatePaging(Number(result.page || 1), Number(result.total || 0), Number(result.pageSize || pageSize));
}

async function loadRun(runId) {
  const result = await callApi(`/query-sizing/runs/${runId}`);
  const run = result.run || null;
  renderRealtime(run);

  if (run && finalStatuses.has(run.status)) {
    stopPolling();
    await loadReports(currentPage);
  }
}

function startPolling(runId) {
  stopPolling();
  pollingTimer = setInterval(() => {
    loadRun(runId).catch((error) => {
      setRunError(error.message);
    });
  }, 2500);
}

async function runQuery() {
  setRunError('');
  const query = String(elements.queryInput.value || '').trim();
  setQueryWarning(query);

  if (!query) {
    setRunError('กรุณากรอก query ก่อน');
    return;
  }

  try {
    setRunStatus('Submitting...', 'progress');
    const result = await callApi('/query-sizing/runs', {
      method: 'POST',
      body: JSON.stringify({ query })
    });

    if (Array.isArray(result.warnings) && result.warnings.length) {
      elements.queryWarning.textContent = result.warnings.join(' ');
      elements.queryWarning.classList.remove('hidden');
    }

    const run = result.run;
    currentRunId = Number(run?.id || 0);
    if (!currentRunId) {
      throw new Error('Run id is missing');
    }

    renderRealtime(run);
    startPolling(currentRunId);
    await loadReports(1);
  } catch (error) {
    setRunStatus('Failed', 'failed');
    setRunError(error.message);
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

function useExample(exampleElement) {
  const text = String(exampleElement?.textContent || '').trim();
  elements.queryInput.value = text;
  elements.queryInput.focus();
}

function bindEvents() {
  elements.logoutBtn.addEventListener('click', () => {
    stopPolling();
    stopIdleTimeout();
    clearSession();
    navigateToLogin();
  });

  elements.runBtn.addEventListener('click', () => {
    runQuery();
  });

  elements.pageSize.addEventListener('change', () => {
    loadReports(1).catch((error) => {
      setRunError(error.message);
    });
  });

  elements.selectAllPageCb.addEventListener('change', () => {
    setCurrentPageSelection(elements.selectAllPageCb.checked);
  });

  elements.checkAllBtn.addEventListener('click', () => {
    setCurrentPageSelection(true);
  });

  elements.uncheckPageBtn.addEventListener('click', () => {
    setCurrentPageSelection(false);
  });

  elements.clearSelectedBtn.addEventListener('click', () => {
    selectedRunIds.clear();
    document.querySelectorAll('.qs-row-select').forEach((input) => {
      input.checked = false;
    });
    updateSelectedCount();
    syncSelectAllPageCheckbox();
  });

  elements.exportSelectedCsvBtn.addEventListener('click', async () => {
    try {
      setRunError('');
      if (!selectedRunIds.size) {
        setRunError('กรุณาเลือกรายการก่อน export CSV');
        return;
      }
      await downloadReportCsv(Array.from(selectedRunIds));
    } catch (error) {
      setRunError(error.message);
    }
  });

  elements.exportCsvBtn.addEventListener('click', async () => {
    try {
      setRunError('');
      await downloadReportCsv(null);
    } catch (error) {
      setRunError(error.message);
    }
  });

  elements.prevPageBtn.addEventListener('click', () => {
    if (currentPage > 1) {
      loadReports(currentPage - 1).catch((error) => {
        setRunError(error.message);
      });
    }
  });

  elements.nextPageBtn.addEventListener('click', () => {
    if (currentPage < totalPages) {
      loadReports(currentPage + 1).catch((error) => {
        setRunError(error.message);
      });
    }
  });

  elements.useExample1Btn.addEventListener('click', () => useExample(elements.example1));
  elements.useExample2Btn.addEventListener('click', () => useExample(elements.example2));
  elements.useExample3Btn.addEventListener('click', () => useExample(elements.example3));
  elements.queryInput.addEventListener('input', () => {
    setQueryWarning(elements.queryInput.value);
  });
}

function init() {
  if (!token) {
    navigateToLogin();
    return;
  }

  const currentUser = localStorage.getItem('allopsUsername') || '-';
  elements.loginStatus.textContent = `Authenticated: ${currentUser}`;
  elements.loginStatus.className = 'status completed';

  useExample(elements.example1);
  setQueryWarning(elements.queryInput.value);
  bindEvents();
  startIdleTimeout();
  loadReports(1).catch((error) => {
    setRunError(error.message);
    setRunStatus('Failed', 'failed');
  });
}

window.addEventListener('beforeunload', () => {
  stopPolling();
  stopIdleTimeout();
});

init();
