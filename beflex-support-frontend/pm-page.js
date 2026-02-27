const apiBase = '/api/beflex-support';
const pmLoginStatus = document.getElementById('pmLoginStatus');
const pmLogoutBtn = document.getElementById('pmLogoutBtn');
const pmSaveBtn = document.getElementById('pmSaveBtn');
const pmRunBtn = document.getElementById('pmRunBtn');
const pmRefreshBtn = document.getElementById('pmRefreshBtn');
const pmRunStatus = document.getElementById('pmRunStatus');
const pmMessage = document.getElementById('pmMessage');
const pmError = document.getElementById('pmError');
const pmRunsBody = document.getElementById('pmRunsBody');
const pmErrorLogs = document.getElementById('pmErrorLogs');
const pmErrorsOnly = document.getElementById('pmErrorsOnly');
const pmBetaToggleBtn = document.getElementById('pmBetaToggleBtn');
const pmFeatureContent = document.getElementById('pmFeatureContent');
const pmBetaOffNotice = document.getElementById('pmBetaOffNotice');

const fields = {
  customer: document.getElementById('pmCustomer'),
  environment: document.getElementById('pmEnvironment'),
  retentionDays: document.getElementById('pmRetentionDays'),
  cronExpression: document.getElementById('pmCronExpression'),
  cronEnabled: document.getElementById('pmCronEnabled')
};

let token = localStorage.getItem('allopsToken') || '';
let poller = null;
let idleTimer = null;
let pmBetaEnabled = true;
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

function setStatus(text, kind = 'progress') {
  pmRunStatus.textContent = text;
  pmRunStatus.className = 'status';
  if (kind === 'completed') pmRunStatus.classList.add('completed');
  if (kind === 'failed') pmRunStatus.classList.add('failed');
  if (kind === 'progress') pmRunStatus.classList.add('progress');
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

function fillConfig(config) {
  applyBetaState(config.betaEnabled !== false);

  Object.entries(fields).forEach(([key, input]) => {
    if (!input) {
      return;
    }
    if (input.type === 'checkbox') {
      input.checked = Boolean(config[key]);
    } else {
      input.value = config[key] ?? '';
    }
  });
}

function readConfigFromForm() {
  return {
    customer: fields.customer.value.trim(),
    environment: fields.environment.value.trim(),
    retentionDays: Number(fields.retentionDays.value || 30),
    cronExpression: fields.cronExpression.value.trim(),
    cronEnabled: pmBetaEnabled ? fields.cronEnabled.checked : false,
    betaEnabled: pmBetaEnabled
  };
}

function applyBetaState(enabled) {
  pmBetaEnabled = Boolean(enabled);

  if (pmBetaToggleBtn) {
    pmBetaToggleBtn.textContent = pmBetaEnabled ? 'ON' : 'OFF';
    pmBetaToggleBtn.classList.toggle('off', !pmBetaEnabled);
  }

  if (pmFeatureContent) {
    pmFeatureContent.style.display = pmBetaEnabled ? '' : 'none';
  }

  if (pmBetaOffNotice) {
    pmBetaOffNotice.style.display = pmBetaEnabled ? 'none' : '';
  }
}

function renderRuns(items) {
  pmRunsBody.innerHTML = '';

  if (!items.length) {
    pmRunsBody.innerHTML = '<tr><td colspan="6">No records</td></tr>';
    pmErrorLogs.textContent = 'No errors';
    return;
  }

  let latestError = null;

  for (const row of items) {
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td>${row.id}</td>
      <td>${formatTime(row.started_at)}</td>
      <td>${row.trigger_type || ''}</td>
      <td>${row.status || ''}</td>
      <td>${row.output_file || '-'}</td>
      <td>${row.message || ''}</td>
    `;
    pmRunsBody.appendChild(tr);

    if (!latestError && row.status === 'FAILED') {
      latestError = row;
    }
  }

  if (latestError) {
    pmErrorLogs.textContent = latestError.stderr_tail || latestError.message || 'Error detail is empty';
  } else {
    pmErrorLogs.textContent = 'No errors';
  }
}

async function loadPmData() {
  const [configResult, runsResult] = await Promise.all([
    callApi('/pm/config'),
    callApi(`/pm/runs?limit=50&errors_only=${pmErrorsOnly.checked ? 'true' : 'false'}`)
  ]);

  fillConfig(configResult.config || {});

  const currentUser = localStorage.getItem('allopsUsername') || 'unknown';
  pmLoginStatus.textContent = `Authenticated: ${currentUser}`;
  pmLoginStatus.className = 'status completed';

  if (configResult.running) {
    setStatus('In Progress', 'progress');
  } else if (!configResult.config?.betaEnabled) {
    setStatus('OFF', 'failed');
  } else {
    setStatus('Idle', 'completed');
  }

  pmMessage.textContent = `BETA: ${configResult.config?.betaEnabled ? 'ON' : 'OFF'} | Cron: ${configResult.config?.cronEnabled ? 'Enabled' : 'Disabled'} | Active: ${configResult.cronActive ? 'Yes' : 'No'} | Last run: ${formatTime(configResult.lastRunAt)}`;

  if (configResult.config?.betaEnabled) {
    renderRuns(runsResult.items || []);
  }
}

async function onSave() {
  if (!pmBetaEnabled) {
    pmError.textContent = 'PM BETA is OFF';
    return;
  }

  pmError.textContent = '';
  pmMessage.textContent = '';

  try {
    setStatus('Saving...', 'progress');
    await callApi('/pm/config', {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(readConfigFromForm())
    });
    setStatus('Saved', 'completed');
    pmMessage.textContent = 'PM settings updated';
    await loadPmData();
  } catch (error) {
    setStatus('Failed', 'failed');
    pmError.textContent = error.message;
  }
}

async function onRunNow() {
  if (!pmBetaEnabled) {
    pmError.textContent = 'PM BETA is OFF';
    return;
  }

  pmError.textContent = '';
  pmMessage.textContent = '';

  try {
    setStatus('Submitting...', 'progress');
    await callApi('/pm/run', { method: 'POST' });
    setStatus('In Progress', 'progress');
    pmMessage.textContent = 'PM run started';
    startPolling();
  } catch (error) {
    setStatus('Failed', 'failed');
    pmError.textContent = error.message;
  }
}

async function onToggleBeta() {
  pmError.textContent = '';
  pmMessage.textContent = '';

  const nextEnabled = !pmBetaEnabled;

  try {
    setStatus('Saving...', 'progress');
    await callApi('/pm/config', {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        customer: fields.customer.value.trim(),
        environment: fields.environment.value.trim(),
        retentionDays: Number(fields.retentionDays.value || 30),
        cronExpression: fields.cronExpression.value.trim(),
        cronEnabled: nextEnabled ? fields.cronEnabled.checked : false,
        betaEnabled: nextEnabled
      })
    });

    if (!nextEnabled) {
      stopPolling();
    }

    await loadPmData();
    if (nextEnabled) {
      startPolling();
    }
    setStatus(nextEnabled ? 'ON' : 'OFF', nextEnabled ? 'completed' : 'failed');
  } catch (error) {
    setStatus('Failed', 'failed');
    pmError.textContent = error.message;
  }
}

function stopPolling() {
  if (poller) {
    clearInterval(poller);
    poller = null;
  }
}

function startPolling() {
  stopPolling();
  poller = setInterval(() => {
    loadPmData().catch((error) => {
      pmError.textContent = error.message;
    });
  }, 5000);
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

function init() {
  if (!token) {
    navigateToLogin();
    return;
  }

  startIdleTimeout();

  pmSaveBtn.addEventListener('click', onSave);
  pmRunBtn.addEventListener('click', onRunNow);
  pmRefreshBtn.addEventListener('click', () => {
    loadPmData().catch((error) => {
      pmError.textContent = error.message;
    });
  });
  pmErrorsOnly.addEventListener('change', () => {
    if (!pmBetaEnabled) {
      return;
    }

    loadPmData().catch((error) => {
      pmError.textContent = error.message;
    });
  });

  if (pmBetaToggleBtn) {
    pmBetaToggleBtn.addEventListener('click', onToggleBeta);
  }

  pmLogoutBtn.addEventListener('click', () => {
    stopPolling();
    stopIdleTimeout();
    clearSession();
    navigateToLogin();
  });

  loadPmData().then(() => {
    if (pmBetaEnabled) {
      startPolling();
    }
  }).catch((error) => {
    pmError.textContent = error.message;
  });
}

window.addEventListener('beforeunload', () => {
  stopPolling();
  stopIdleTimeout();
});

init();
