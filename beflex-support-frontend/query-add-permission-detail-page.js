const apiBase = '/api/beflex-support';

const elements = {
  runMeta: document.getElementById('qapdRunMeta'),
  runSummary: document.getElementById('qapdRunSummary'),
  backBtn: document.getElementById('qapdBackBtn'),
  runBtn: document.getElementById('qapdRunBtn'),
  retryFailedBtn: document.getElementById('qapdRetryFailedBtn'),
  statusFilter: document.getElementById('qapdStatusFilter'),
  searchText: document.getElementById('qapdSearchText'),
  searchBtn: document.getElementById('qapdSearchBtn'),
  clearBtn: document.getElementById('qapdClearBtn'),
  pageSize: document.getElementById('qapdPageSize'),
  prevBtn: document.getElementById('qapdPrevBtn'),
  nextBtn: document.getElementById('qapdNextBtn'),
  pagingLabel: document.getElementById('qapdPagingLabel'),
  body: document.getElementById('qapdBody'),
  error: document.getElementById('qapdError'),
  topBtn: document.getElementById('qapdTopBtn'),
  uuidModal: document.getElementById('qapdUuidModal'),
  uuidModalText: document.getElementById('qapdUuidModalText'),
  closeUuidModalBtn: document.getElementById('qapdCloseUuidModalBtn')
};

const queryParams = new URLSearchParams(window.location.search);
const runId = Number(queryParams.get('runId') || 0);
let token = localStorage.getItem('allopsToken') || '';
let currentPage = 1;
let totalPages = 1;
let refreshTimer = null;
let isRunSubmitting = false;
let runCapabilities = {
  canRun: false,
  canRetryFailed: false
};

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

function shortMessage(value, maxLen = 90) {
  const text = String(value || '').trim();
  if (!text) {
    return '-';
  }
  if (text.length <= maxLen) {
    return text;
  }
  return `${text.slice(0, maxLen)}...`;
}

function clearSession() {
  localStorage.removeItem('allopsToken');
  localStorage.removeItem('allopsUsername');
}

function navigateToLogin() {
  window.location.href = 'index.html';
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
  if (raw === 'LISTED') {
    return 'wait';
  }
  if (raw === 'COMPLETED') {
    return 'success';
  }
  if (raw === 'FAILED' || raw === 'COMPLETED_WITH_ERRORS') {
    return 'failed';
  }
  if (raw === 'QUEUED' || raw === 'LISTING') {
    return 'query';
  }
  return 'running';
}

function renderRunInfo(payload) {
  const run = payload.run || {};
  const summary = payload.summary || {};
  const rawStatus = String(run.status || '').toUpperCase();
  const runDate = String(run.created_at || run.queried_at || '-');

  elements.runMeta.textContent = `No_Query:${run.id || '-'} , ${run.template_name || '-'} , ${runDate}`;
  elements.runSummary.innerHTML = `
    <span class="qapd-sum-chip qapd-sum-all">all(${Number(summary.all || 0)})</span>
    <span class="qapd-sum-chip qapd-sum-wait">wait(${Number(summary.wait || 0)})</span>
    <span class="qapd-sum-chip qapd-sum-success">success(${Number(summary.success || 0)})</span>
    <span class="qapd-sum-chip qapd-sum-failed">failed(${Number(summary.failed || 0)})</span>
    ${run.details_cleared ? `<span class="qapd-sum-chip qapd-sum-failed">details cleared (${Number(run.detail_cleared_item_count || 0)})</span>` : ''}
  `;

  const canRun = Boolean(run.can_run) && !isRunSubmitting;
  const canRetryFailed = Boolean(run.can_retry_failed) && !isRunSubmitting;
  runCapabilities = {
    canRun: Boolean(run.can_run),
    canRetryFailed: Boolean(run.can_retry_failed)
  };

  elements.runBtn.disabled = !canRun;
  elements.retryFailedBtn.disabled = !canRetryFailed;
  elements.runBtn.classList.toggle('hidden', !canRun);
  elements.retryFailedBtn.classList.toggle('hidden', !canRetryFailed);

  let retryTooltip = 'Retry failed items';
  if (isRunSubmitting) {
    retryTooltip = 'กำลังส่งคำขออยู่ กรุณารอสักครู่';
  } else if (canRetryFailed) {
    retryTooltip = `Retry failed items (${Number(summary.failed || 0)} rows)`;
  } else if (Number(summary.failed || 0) <= 0) {
    retryTooltip = 'no failed rows';
  } else if (['QUEUED', 'LISTING', 'ADDING_PERMISSION'].includes(rawStatus)) {
    retryTooltip = 'run กำลังทำงาน';
  } else if (!run.template_permission_filename) {
    retryTooltip = 'template ไม่มีไฟล์ permission สำหรับ retry';
  } else {
    retryTooltip = 'ไม่สามารถ retry ได้ในสถานะปัจจุบัน';
  }
  elements.retryFailedBtn.title = retryTooltip;
  elements.retryFailedBtn.setAttribute('aria-label', retryTooltip);

  if (isRunSubmitting) {
    elements.retryFailedBtn.title = 'กำลังส่งคำขออยู่ กรุณารอสักครู่';
  }
}

function renderItems(payload) {
  const items = Array.isArray(payload.items) ? payload.items : [];
  elements.body.innerHTML = '';

  if (!items.length) {
    if (payload.run?.details_cleared) {
      elements.body.innerHTML = '<tr><td colspan="7">Details ถูก clear ตาม retention policy แล้ว (เก็บไว้เฉพาะ summary)</td></tr>';
    } else {
      elements.body.innerHTML = '<tr><td colspan="7">No item data</td></tr>';
    }
    return;
  }

  const pageSize = Number(payload.pageSize || 30);
  const offset = (Number(payload.page || 1) - 1) * pageSize;

  items.forEach((item, index) => {
    const tr = document.createElement('tr');
    const fullMessage = String(item.message || '').trim();
    const messagePreview = shortMessage(fullMessage);
    const escapedMessage = escapeHtml(fullMessage);
    const displayStatus = String(item.display_status || 'wait');
    const statusClass = displayStatus === 'success'
      ? 'qapd-item-status-success'
      : (displayStatus === 'failed' ? 'qapd-item-status-failed' : 'qapd-item-status-wait');
    tr.innerHTML = `
      <td>${offset + index + 1}</td>
      <td>${escapeHtml(item.node_name || '-')}</td>
      <td>${escapeHtml(item.node_id || '-')}</td>
      <td>${escapeHtml(item.node_type || '-')}</td>
      <td><span class="qapd-item-status ${statusClass}">${escapeHtml(displayStatus)}</span></td>
      <td>
        <span>${escapeHtml(messagePreview)}</span>
      </td>
      <td>
        <button class="btn-secondary qapd-details-btn" type="button" data-row='${escapeHtml(JSON.stringify(item))}'>i</button>
        <button class="btn-secondary qapd-message-btn" type="button" data-message="${escapedMessage}" aria-label="View full error" title="View full error">
          <svg viewBox="0 0 24 24" width="14" height="14" aria-hidden="true" focusable="false">
            <path d="M12 5C6 5 2 12 2 12s4 7 10 7 10-7 10-7-4-7-10-7zm0 11a4 4 0 1 1 0-8 4 4 0 0 1 0 8z" fill="currentColor"></path>
            <circle cx="12" cy="12" r="2" fill="currentColor"></circle>
          </svg>
        </button>
      </td>
    `;
    elements.body.appendChild(tr);
  });

  document.querySelectorAll('.qapd-details-btn').forEach((button) => {
    button.addEventListener('click', () => {
      const data = button.getAttribute('data-row') || '{}';
      const raw = data
        .replace(/&quot;/g, '"')
        .replace(/&#039;/g, "'")
        .replace(/&lt;/g, '<')
        .replace(/&gt;/g, '>')
        .replace(/&amp;/g, '&');
      let item = {};
      try {
        item = JSON.parse(raw);
      } catch (_error) {
        item = {};
      }

      const lines = [
        `Name: ${item.node_name || '-'}`,
        `UUID: ${item.node_id || '-'}`,
        `Type: ${item.node_type || '-'}`,
        `Status: ${item.display_status || '-'}`,
        `NodeRef: ${item.node_ref || '-'}`,
        `Path: ${item.node_path || '-'}`,
        `Message: ${item.message || '-'}`
      ];
      elements.uuidModalText.textContent = lines.join('\n');
      elements.uuidModal.classList.remove('hidden');
      elements.uuidModal.setAttribute('aria-hidden', 'false');
    });
  });

  document.querySelectorAll('.qapd-message-btn').forEach((button) => {
    button.addEventListener('click', () => {
      const rawMessage = button.getAttribute('data-message') || '-';
      const unescaped = rawMessage
        .replace(/&quot;/g, '"')
        .replace(/&#039;/g, "'")
        .replace(/&lt;/g, '<')
        .replace(/&gt;/g, '>')
        .replace(/&amp;/g, '&');

      elements.uuidModalText.textContent = unescaped || '-';
      elements.uuidModal.classList.remove('hidden');
      elements.uuidModal.setAttribute('aria-hidden', 'false');
    });
  });
}

function updatePaging(page, total, pageSize) {
  currentPage = page;
  totalPages = Math.max(1, Math.ceil(total / pageSize));
  elements.pagingLabel.textContent = `Page ${currentPage} / ${totalPages}`;
  elements.prevBtn.disabled = currentPage <= 1;
  elements.nextBtn.disabled = currentPage >= totalPages;
}

async function loadRunItems(page = 1) {
  elements.error.textContent = '';
  const pageSize = Number(elements.pageSize.value || 30);
  const status = String(elements.statusFilter.value || 'all');
  const search = String(elements.searchText.value || '').trim();

  const params = new URLSearchParams({
    page: String(page),
    pageSize: String(pageSize),
    status,
    search
  });

  const result = await callApi(`/query-permission/runs/${runId}/items?${params.toString()}`);
  renderRunInfo(result);
  renderItems(result);
  updatePaging(Number(result.page || 1), Number(result.total || 0), Number(result.pageSize || pageSize));
}

function stopRefresh() {
  if (refreshTimer) {
    clearInterval(refreshTimer);
    refreshTimer = null;
  }
}

function startRefresh() {
  stopRefresh();
  refreshTimer = setInterval(() => {
    loadRunItems(currentPage).catch((error) => {
      elements.error.textContent = error.message;
    });
  }, 5000);
}

async function runAddPermission() {
  if (!runId || isRunSubmitting || !runCapabilities.canRun) {
    return;
  }

  try {
    isRunSubmitting = true;
    elements.runBtn.disabled = true;
    setStatus(elements.statusBadge, 'Running...', 'progress');

    await callApi(`/query-permission/runs/${runId}/add-permission`, {
      method: 'POST',
      body: JSON.stringify({ source: 'template' })
    });

    await loadRunItems(currentPage);
  } catch (error) {
    elements.error.textContent = error.message;
  } finally {
    isRunSubmitting = false;
  }
}

async function retryFailedItems() {
  if (!runId || isRunSubmitting || !runCapabilities.canRetryFailed) {
    return;
  }

  try {
    isRunSubmitting = true;
    elements.runBtn.disabled = true;
    elements.retryFailedBtn.disabled = true;
    setStatus(elements.statusBadge, 'Retrying...', 'progress');

    await callApi(`/query-permission/runs/${runId}/retry-failed`, {
      method: 'POST',
      body: JSON.stringify({ source: 'template' })
    });

    await loadRunItems(currentPage);
  } catch (error) {
    elements.error.textContent = error.message;
  } finally {
    isRunSubmitting = false;
  }
}

function bindEvents() {
  elements.backBtn.addEventListener('click', () => {
    window.location.href = 'query-add-permission.html';
  });

  elements.runBtn.addEventListener('click', runAddPermission);
  elements.retryFailedBtn.addEventListener('click', retryFailedItems);

  elements.searchBtn.addEventListener('click', () => {
    loadRunItems(1).catch((error) => {
      elements.error.textContent = error.message;
    });
  });

  elements.clearBtn.addEventListener('click', () => {
    elements.statusFilter.value = 'all';
    elements.searchText.value = '';
    loadRunItems(1).catch((error) => {
      elements.error.textContent = error.message;
    });
  });

  elements.statusFilter.addEventListener('change', () => {
    loadRunItems(1).catch((error) => {
      elements.error.textContent = error.message;
    });
  });

  elements.pageSize.addEventListener('change', () => {
    loadRunItems(1).catch((error) => {
      elements.error.textContent = error.message;
    });
  });

  elements.prevBtn.addEventListener('click', () => {
    if (currentPage > 1) {
      loadRunItems(currentPage - 1).catch((error) => {
        elements.error.textContent = error.message;
      });
    }
  });

  elements.nextBtn.addEventListener('click', () => {
    if (currentPage < totalPages) {
      loadRunItems(currentPage + 1).catch((error) => {
        elements.error.textContent = error.message;
      });
    }
  });

  elements.topBtn.addEventListener('click', () => {
    window.scrollTo({ top: 0, behavior: 'smooth' });
  });

  elements.closeUuidModalBtn.addEventListener('click', () => {
    elements.uuidModal.classList.add('hidden');
    elements.uuidModal.setAttribute('aria-hidden', 'true');
  });
}

async function init() {
  if (!token) {
    navigateToLogin();
    return;
  }

  if (!runId) {
    elements.error.textContent = 'Invalid run id';
    elements.runBtn.disabled = true;
    elements.retryFailedBtn.disabled = true;
    elements.runBtn.classList.add('hidden');
    elements.retryFailedBtn.classList.add('hidden');
    return;
  }

  bindEvents();
  await loadRunItems(1);
  startRefresh();
}

window.addEventListener('beforeunload', () => {
  stopRefresh();
});

init().catch((error) => {
  elements.error.textContent = error.message;
});
