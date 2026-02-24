const apiBase = '/api/allops-raku';
const mainContent = document.querySelector('.main-content');

const usernameInput = document.getElementById('username');
const passwordInput = document.getElementById('password');
const loginBtn = document.getElementById('loginBtn');
const loginStatus = document.getElementById('loginStatus');
const loginError = document.getElementById('loginError');
const logoutBtn = document.getElementById('logoutBtn');

const fileInput = document.getElementById('fileInput');
const uploadBtn = document.getElementById('uploadBtn');
const refreshBtn = document.getElementById('refreshBtn');
const uploadError = document.getElementById('uploadError');
const taskStatus = document.getElementById('taskStatus');
const taskMeta = document.getElementById('taskMeta');
const logsBox = document.getElementById('logsBox');
const historyBody = document.getElementById('historyBody');
const pageDesc = document.getElementById('pageDesc');

const serviceName = mainContent?.dataset?.serviceName || 'permission-import';
const uploadEndpoint = mainContent?.dataset?.uploadEndpoint || '/imports';
const configuredPageTitle = mainContent?.dataset?.pageTitle;
const configuredPageDesc = mainContent?.dataset?.pageDesc;
const configuredUploadLabel = mainContent?.dataset?.uploadLabel;
const configuredHistoryTitle = mainContent?.dataset?.historyTitle;

let token = '';
let currentTaskId = null;
let taskPolling = null;
const pagePath = window.location.pathname;
const onServicePage = pagePath.endsWith('/service.html') || pagePath.endsWith('/service') || pagePath.endsWith('/group-service.html') || pagePath.endsWith('/group-service');
const onLoginPage = !onServicePage;

function getSavedToken() {
  return localStorage.getItem('allopsToken') || '';
}

function getSavedUsername() {
  return localStorage.getItem('allopsUsername') || '';
}

function saveSession(nextToken, username) {
  localStorage.setItem('allopsToken', nextToken);
  localStorage.setItem('allopsUsername', username || '');
}

function clearSession() {
  localStorage.removeItem('allopsToken');
  localStorage.removeItem('allopsUsername');
}

function setTaskStatus(text, kind = 'progress') {
  if (!taskStatus) {
    return;
  }
  taskStatus.textContent = text;
  taskStatus.className = 'status';
  if (kind === 'completed') taskStatus.classList.add('completed');
  if (kind === 'failed') taskStatus.classList.add('failed');
  if (kind === 'progress') taskStatus.classList.add('progress');
}

function formatBangkokTime(value) {
  if (!value) {
    return '';
  }

  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }

  const bangkokDate = new Date(date.getTime() + (7 * 60 * 60 * 1000));
  return `${bangkokDate.toISOString().slice(0, 19).replace('T', ' ')} +07`;
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

  if (!response.ok) {
    throw new Error(data.message || `Request failed: ${response.status}`);
  }

  return data;
}

function renderHistory(items) {
  if (!historyBody) {
    return;
  }
  historyBody.innerHTML = '';
  if (!items.length) {
    historyBody.innerHTML = '<tr><td colspan="6">No records</td></tr>';
    return;
  }

  for (const row of items) {
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td>${row.id}</td>
      <td>${formatBangkokTime(row.timestamp)}</td>
      <td>${row.username || ''}</td>
      <td>${row.filename || ''}</td>
      <td>${row.status || ''}</td>
      <td>${row.error_message || ''}</td>
    `;
    historyBody.appendChild(tr);
  }
}

async function loadHistory() {
  const result = await callApi(`/reports/imports?limit=30&service_name=${encodeURIComponent(serviceName)}`);
  renderHistory(result.items || []);
}

async function loadTaskStatus(taskId) {
  const [{ task }, { logs }] = await Promise.all([
    callApi(`/tasks/${taskId}`),
    callApi(`/tasks/${taskId}/logs`)
  ]);

  if (taskMeta) {
    taskMeta.textContent = `Task #${task.id} | service: ${task.service_name || '-'} | file: ${task.filename} | user: ${task.username}`;
  }

  const text = (logs || [])
    .slice()
    .reverse()
    .map((line) => `[${formatBangkokTime(line.created_at)}] [${line.level}] ${line.message}`)
    .join('\n');

  if (logsBox) {
    logsBox.textContent = text || 'No logs';
  }

  if (task.status === 'COMPLETED') {
    setTaskStatus('Completed', 'completed');
    stopPolling();
    await loadHistory();
  } else if (task.status === 'FAILED') {
    setTaskStatus('Failed', 'failed');
    stopPolling();
    await loadHistory();
  } else {
    setTaskStatus(task.status || 'In Progress', 'progress');
  }
}

function startPolling(taskId) {
  stopPolling();
  taskPolling = setInterval(() => {
    loadTaskStatus(taskId).catch((error) => {
      uploadError.textContent = error.message;
    });
  }, 3000);
}

function stopPolling() {
  if (taskPolling) {
    clearInterval(taskPolling);
    taskPolling = null;
  }
}

async function handleUpload() {
  if (!uploadError || !fileInput) {
    return;
  }
  uploadError.textContent = '';

  const file = fileInput.files[0];
  if (!file) {
    uploadError.textContent = 'กรุณาเลือกไฟล์ก่อน';
    return;
  }

  if (!file.name.toLowerCase().endsWith('.xlsx')) {
    uploadError.textContent = 'รองรับเฉพาะไฟล์ .xlsx เท่านั้น';
    return;
  }

  const formData = new FormData();
  formData.append('file', file);

  try {
    setTaskStatus('Submitting task...', 'progress');
    const result = await callApi(uploadEndpoint, {
      method: 'POST',
      body: formData
    });

    currentTaskId = result.task_id;
    if (taskMeta) {
      taskMeta.textContent = `Task #${currentTaskId} accepted`;
    }
    if (logsBox) {
      logsBox.textContent = 'Waiting for logs...';
    }
    setTaskStatus('In Progress', 'progress');

    await loadTaskStatus(currentTaskId);
    startPolling(currentTaskId);
  } catch (error) {
    setTaskStatus('Failed', 'failed');
    uploadError.textContent = error.message;
  }
}

function navigateToLogin() {
  window.location.href = 'index.html';
}

function navigateToService() {
  window.location.href = 'service.html';
}

function applyPageConfig() {
  if (!mainContent) {
    return;
  }

  if (configuredPageTitle) {
    const heading = mainContent.querySelector('h1');
    if (heading) {
      heading.textContent = configuredPageTitle;
    }
  }

  if (configuredPageDesc && pageDesc) {
    pageDesc.textContent = configuredPageDesc;
  }

  if (configuredUploadLabel) {
    const label = document.querySelector('label[for="fileInput"]');
    if (label) {
      label.textContent = configuredUploadLabel;
    }
  }

  if (configuredHistoryTitle) {
    const historyTitle = document.querySelector('#historyCard h3');
    if (historyTitle) {
      historyTitle.textContent = configuredHistoryTitle;
    }
  }
}

applyPageConfig();

if (loginBtn) {
  loginBtn.addEventListener('click', async () => {
    loginError.textContent = '';
    const username = usernameInput.value.trim();
    const password = passwordInput.value;

    if (!username || !password) {
      loginError.textContent = 'กรุณากรอก username/password';
      return;
    }

    try {
      loginStatus.textContent = 'Authenticating...';
      const result = await callApi('/auth/login', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ username, password })
      });

      token = result.token;
      saveSession(result.token, result.username);
      loginStatus.textContent = `Authenticated: ${result.username}`;
      loginStatus.className = 'status completed';
      navigateToService();
    } catch (error) {
      loginStatus.textContent = 'Not authenticated';
      loginStatus.className = 'status';
      loginError.textContent = error.message;
    }
  });
}

if (uploadBtn) {
  uploadBtn.addEventListener('click', handleUpload);
}

if (refreshBtn) {
  refreshBtn.addEventListener('click', () => {
    loadHistory().catch((error) => {
      uploadError.textContent = error.message;
    });
  });
}

if (logoutBtn) {
  logoutBtn.addEventListener('click', () => {
    stopPolling();
    clearSession();
    navigateToLogin();
  });
}

if (onServicePage) {
  token = getSavedToken();
  if (!token) {
    navigateToLogin();
  } else {
    const currentUser = getSavedUsername();
    if (loginStatus) {
      loginStatus.textContent = `Authenticated: ${currentUser}`;
      loginStatus.className = 'status completed';
    }
    loadHistory().catch((error) => {
      if (uploadError) {
        uploadError.textContent = error.message;
      }
    });
  }
}

if (onLoginPage) {
  token = getSavedToken();
  if (token) {
    navigateToService();
  }
}

window.addEventListener('beforeunload', () => {
  stopPolling();
});
