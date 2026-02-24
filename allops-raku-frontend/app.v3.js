const apiBase = '/api/allops-raku';

const loginCard = document.getElementById('loginCard');
const importCard = document.getElementById('importCard');
const logsCard = document.getElementById('logsCard');
const historyCard = document.getElementById('historyCard');

const usernameInput = document.getElementById('username');
const passwordInput = document.getElementById('password');
const loginBtn = document.getElementById('loginBtn');
const loginStatus = document.getElementById('loginStatus');
const loginError = document.getElementById('loginError');

const fileInput = document.getElementById('fileInput');
const uploadBtn = document.getElementById('uploadBtn');
const refreshBtn = document.getElementById('refreshBtn');
const uploadError = document.getElementById('uploadError');
const taskStatus = document.getElementById('taskStatus');
const taskMeta = document.getElementById('taskMeta');
const logsBox = document.getElementById('logsBox');
const historyBody = document.getElementById('historyBody');

let token = '';
let currentTaskId = null;
let taskPolling = null;

function setTaskStatus(text, kind = 'progress') {
  taskStatus.textContent = text;
  taskStatus.className = 'status';
  if (kind === 'completed') taskStatus.classList.add('completed');
  if (kind === 'failed') taskStatus.classList.add('failed');
  if (kind === 'progress') taskStatus.classList.add('progress');
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
  historyBody.innerHTML = '';
  if (!items.length) {
    historyBody.innerHTML = '<tr><td colspan="6">No records</td></tr>';
    return;
  }

  for (const row of items) {
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td>${row.id}</td>
      <td>${row.timestamp || ''}</td>
      <td>${row.username || ''}</td>
      <td>${row.filename || ''}</td>
      <td>${row.status || ''}</td>
      <td>${row.error_message || ''}</td>
    `;
    historyBody.appendChild(tr);
  }
}

async function loadHistory() {
  const result = await callApi('/reports/imports?limit=30');
  renderHistory(result.items || []);
}

async function loadTaskStatus(taskId) {
  const [{ task }, { logs }] = await Promise.all([
    callApi(`/tasks/${taskId}`),
    callApi(`/tasks/${taskId}/logs`)
  ]);

  taskMeta.textContent = `Task #${task.id} | file: ${task.filename} | user: ${task.username}`;

  const text = (logs || [])
    .slice()
    .reverse()
    .map((line) => `[${line.created_at}] [${line.level}] ${line.message}`)
    .join('\n');

  logsBox.textContent = text || 'No logs';

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
    loginStatus.textContent = `Authenticated: ${result.username}`;
    loginStatus.className = 'status completed';

    importCard.classList.remove('hidden');
    logsCard.classList.remove('hidden');
    historyCard.classList.remove('hidden');
    await loadHistory();
  } catch (error) {
    loginStatus.textContent = 'Not authenticated';
    loginStatus.className = 'status';
    loginError.textContent = error.message;
  }
});

uploadBtn.addEventListener('click', async () => {
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
    const result = await callApi('/imports', {
      method: 'POST',
      body: formData
    });

    currentTaskId = result.task_id;
    taskMeta.textContent = `Task #${currentTaskId} accepted`;
    logsBox.textContent = 'Waiting for logs...';
    setTaskStatus('In Progress', 'progress');

    await loadTaskStatus(currentTaskId);
    startPolling(currentTaskId);
  } catch (error) {
    setTaskStatus('Failed', 'failed');
    uploadError.textContent = error.message;
  }
});

refreshBtn.addEventListener('click', () => {
  loadHistory().catch((error) => {
    uploadError.textContent = error.message;
  });
});

window.addEventListener('beforeunload', () => {
  stopPolling();
});
