const fs = require('fs/promises');
const path = require('path');
const os = require('os');
const { execFile } = require('child_process');
const { promisify } = require('util');

const execFileAsync = promisify(execFile);
const DEFAULT_IP_INCLUDE_REGEX = '^(en|eth|eno|enp|ens|em|bond|team|wlan)';
const SEPARATOR_LINE = '=====================================================================';

function sanitizeToken(value) {
  const token = String(value || 'unknown').replace(/\s+/g, '_').replace(/[^a-zA-Z0-9_-]/g, '');
  return token || 'unknown';
}

function shellEscape(value) {
  return `'${String(value || '').replace(/'/g, `'"'"'`)}'`;
}

async function runShell(command, timeoutMs = 15000) {
  try {
    const result = await execFileAsync('sh', ['-lc', command], {
      timeout: timeoutMs,
      maxBuffer: 4 * 1024 * 1024
    });
    return {
      ok: true,
      stdout: String(result.stdout || '').trimEnd(),
      stderr: String(result.stderr || '').trimEnd()
    };
  } catch (error) {
    return {
      ok: false,
      stdout: String(error?.stdout || '').trimEnd(),
      stderr: String(error?.stderr || error?.message || '').trimEnd()
    };
  }
}

async function pathExists(targetPath) {
  try {
    await fs.access(targetPath);
    return true;
  } catch (error) {
    return false;
  }
}

function parseEnvContent(rawText) {
  const rows = [];
  const lines = String(rawText || '').split(/\r?\n/);
  for (const line of lines) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith('#')) {
      continue;
    }

    const equalsIndex = trimmed.indexOf('=');
    if (equalsIndex <= 0) {
      continue;
    }

    const key = trimmed.slice(0, equalsIndex).trim();
    let value = trimmed.slice(equalsIndex + 1).trim();
    if ((value.startsWith('"') && value.endsWith('"')) || (value.startsWith("'") && value.endsWith("'"))) {
      value = value.slice(1, -1);
    }
    rows.push([key, value]);
  }
  return rows;
}

async function parseEnvFile(filePath) {
  try {
    const content = await fs.readFile(filePath, 'utf8');
    return parseEnvContent(content);
  } catch (error) {
    return null;
  }
}

function findEnvValue(entries, key) {
  for (const [entryKey, entryValue] of entries || []) {
    if (entryKey === key) {
      return entryValue;
    }
  }
  return '';
}

function maskEnvValue(key, value) {
  if (key === 'PGPASSWORD' || key === 'PGPASSWORD_FILE') {
    return '********';
  }
  return value;
}

function addBanner(lines, title) {
  lines.push(SEPARATOR_LINE);
  lines.push(`#${title}`);
  lines.push(SEPARATOR_LINE);
}

function formatTimestampForFilename(date = new Date()) {
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, '0');
  const day = String(date.getDate()).padStart(2, '0');
  const hours = String(date.getHours()).padStart(2, '0');
  const minutes = String(date.getMinutes()).padStart(2, '0');
  return `${year}-${month}-${day}_${hours}-${minutes}`;
}

async function getDirectorySize(pathname) {
  const result = await runShell(`du -sh -- ${shellEscape(pathname)}`);
  if (!result.ok || !result.stdout) {
    return `${pathname} does not exist`;
  }
  return result.stdout.split(/\s+/)[0] || `${pathname} does not exist`;
}

async function getContentStoreYearSummary(basePath) {
  if (!(await pathExists(basePath))) {
    return [`${basePath} does not exist`, ''];
  }

  const result = await runShell(`du -h --max-depth=1 -- ${shellEscape(basePath)} | sort -k2V`);
  if (!result.ok) {
    return [`${basePath} does not exist`, ''];
  }

  const lines = String(result.stdout || '')
    .split(/\r?\n/)
    .filter((line) => line.trim())
    .filter((line) => !line.endsWith(`\t${basePath}`) && !line.endsWith(` ${basePath}`));
  lines.push('');
  return lines;
}

async function getContentStoreLastMonths(basePath, months) {
  const lines = [];
  const now = new Date();
  const start = new Date(now.getFullYear(), now.getMonth(), 1);

  for (let offset = 0; offset < months; offset += 1) {
    const target = new Date(start.getFullYear(), start.getMonth() - offset, 1);
    const year = target.getFullYear();
    const month = target.getMonth() + 1;
    const paddedMonth = String(month).padStart(2, '0');
    const candidatePadded = path.join(basePath, String(year), paddedMonth);
    const candidatePlain = path.join(basePath, String(year), String(month));

    let size = 'missing';
    if (await pathExists(candidatePadded)) {
      size = await getDirectorySize(candidatePadded);
    } else if (await pathExists(candidatePlain)) {
      size = await getDirectorySize(candidatePlain);
    }

    lines.push(`${year}/${month}: ${size}`);
  }
  lines.push('');
  return lines;
}

async function getContentStoreTotalFiles(basePath) {
  if (!(await pathExists(basePath))) {
    return [`${basePath} does not exist`, ''];
  }

  const yearDirs = [];
  const entries = await fs.readdir(basePath, { withFileTypes: true }).catch(() => []);
  for (const entry of entries) {
    if (entry.isDirectory()) {
      yearDirs.push(path.join(basePath, entry.name));
    }
  }

  const reportLines = [];
  let total = 0;

  for (const yearDir of yearDirs.sort()) {
    const result = await runShell(`find ${shellEscape(yearDir)} -type f | wc -l`);
    const count = Number.parseInt(String(result.stdout || '').trim(), 10);
    const safeCount = Number.isFinite(count) ? count : 0;
    total += safeCount;
    reportLines.push(`${yearDir}: ${safeCount}`);
  }

  reportLines.push(`Total file: ${total} file`);
  reportLines.push('');
  return reportLines;
}

async function getPathSizing(pathname) {
  const result = await runShell(`du -sh -- ${shellEscape(pathname)}`);
  if (!result.ok || !result.stdout) {
    return [`${pathname} does not exist`, ''];
  }
  return [result.stdout, ''];
}

async function commandExists(name) {
  const result = await runShell(`command -v ${shellEscape(name)} >/dev/null 2>&1 && echo yes || echo no`);
  return result.stdout.trim() === 'yes';
}

async function runBackupJob(label, sourceDir, destDir, excludes, backupDate) {
  if (!(await pathExists(sourceDir))) {
    return `${label} backup: source ${sourceDir} not found`;
  }
  if (!(await commandExists('zip'))) {
    return `${label} backup: zip command not found`;
  }

  await fs.mkdir(destDir, { recursive: true });
  const zipPath = path.join(destDir, `pm_${label}_${backupDate}.zip`);
  await fs.rm(zipPath, { force: true }).catch(() => {});

  const args = ['-r', zipPath, '.'];
  for (const pattern of String(excludes || '').split(' ').map((item) => item.trim()).filter(Boolean)) {
    const clean = pattern.replace(/^\//, '');
    args.push('-x', `./${clean}`, `./${clean}/*`);
  }

  try {
    await execFileAsync('zip', args, {
      cwd: sourceDir,
      timeout: 10 * 60 * 1000,
      maxBuffer: 4 * 1024 * 1024
    });
    return `${label} backup file: ${zipPath}`;
  } catch (error) {
    return `${label} backup: zip command failed`;
  }
}

async function getDockerDetailsLines() {
  const lines = [];
  if (await commandExists('docker')) {
    const dockerVersion = await runShell('docker --version | head -n 1');
    const dockerComposeVersion = await runShell('docker compose version | head -n 1');
    lines.push(`docker v: ${dockerVersion.stdout || 'unknown'}`);
    lines.push(`docker compose v: ${dockerComposeVersion.stdout || 'docker compose command not found'}`);
    lines.push('docker stats:');
    const stats = await runShell('docker stats --no-stream --format "{{.Container}}\t{{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.MemPerc}}\t{{.NetIO}}\t{{.BlockIO}}\t{{.PIDs}}"');
    if (stats.stdout) {
      lines.push('CONTAINER ID\tNAME\tCPU %\tMEM USAGE / LIMIT\tMEM %\tNET I/O\tBLOCK I/O\tPIDS');
      lines.push(...stats.stdout.split(/\r?\n/).filter((line) => line.trim()));
    } else {
      lines.push('docker command not found');
    }
  } else {
    lines.push('docker v: docker command not found');
    lines.push('docker compose v: docker compose command not found');
    lines.push('docker stats:');
    lines.push('docker command not found');
  }
  lines.push('');
  return lines;
}

async function runPmReport(config, options = {}) {
  const ipIncludeRegex = options.ipIncludeRegex || DEFAULT_IP_INCLUDE_REGEX;
  const now = new Date();
  const outputDir = config.outputDir;
  await fs.mkdir(outputDir, { recursive: true });

  const fileTimestamp = formatTimestampForFilename(now);
  const customerToken = sanitizeToken(config.customer);
  const environmentToken = sanitizeToken(config.environment);
  const filename = `pm_${customerToken}_${environmentToken}_${fileTimestamp}.txt`;
  const outputFile = path.join(outputDir, filename);

  const dateText = await runShell("date '+%Y-%m-%d %H:%M:%S %Z'");
  const generatedAt = dateText.stdout || now.toISOString();
  const diskReport = await runShell("df -h | grep -v '/docker/overlay2'");
  const ipReport = await runShell(`ip -o -4 addr show | awk -v pattern=${shellEscape(ipIncludeRegex)} '($2 ~ pattern) {split($4, a, "/"); print a[1] " (" $2 ")"}' | paste -sd ', ' -`);
  const osRelease = await runShell('cat /etc/*release 2>/dev/null | grep "DISTRIB_DESCRIPTION" | sort -u | paste -sd ", " -');

  const totalMem = os.totalmem();
  const freeMem = os.freemem();
  const usedMem = Math.max(0, totalMem - freeMem);
  const ramTotalGb = (totalMem / (1024 ** 3)).toFixed(2);
  const ramUsedGb = (usedMem / (1024 ** 3)).toFixed(2);

  const envWorkspaceEntries = await parseEnvFile(config.envWorkspace);
  const envPostgresEntries = await parseEnvFile(config.envPostgresql);

  const lines = [];
  lines.push('-------------------------------------------------');
  lines.push('# Server Details');
  lines.push('-------------------------------------------------');
  lines.push(`Customer: ${config.customer}`);
  lines.push(`Environment: ${config.environment}`);
  lines.push(`Generated At: ${generatedAt}`);
  lines.push('');
  lines.push(`Hostname: ${os.hostname()}`);
  lines.push(`RAM Total: ${ramTotalGb} GB`);
  lines.push(`RAM Used: ${ramUsedGb} GB`);
  lines.push(`CPU Processor: ${os.cpus().length}`);
  lines.push(`CPU Model Name: ${os.cpus()[0]?.model || 'Unknown'}`);
  lines.push(`OS Release: ${osRelease.stdout || 'Unknown'}`);
  lines.push(`ip addr show: ${ipReport.stdout || 'Unknown'}`);
  lines.push('');
  lines.push('Harddisk Server:');
  lines.push(diskReport.stdout || 'Unknown');
  lines.push('');

  addBanner(lines, 'beflex app details');
  if (!envWorkspaceEntries) {
    lines.push(`${config.envWorkspace} not found`);
  } else {
    for (const [key, value] of envWorkspaceEntries) {
      if (key === 'SERVER_NAME') {
        break;
      }
      lines.push(`${key}: ${maskEnvValue(key, value)}`);
    }
  }
  lines.push('');

  addBanner(lines, 'postgresql details');
  if (!envPostgresEntries) {
    lines.push(`${config.envPostgresql} not found`);
  } else {
    const postgresTag = findEnvValue(envPostgresEntries, 'POSTGRES_TAG');
    if (postgresTag) {
      lines.push(`POSTGRES_TAG: ${postgresTag}`);
    } else {
      lines.push(`No matching keys found in ${config.envPostgresql}`);
    }
  }
  lines.push('');

  addBanner(lines, 'Alfresco Content Store');
  lines.push(`Alf ContentStore path=${config.contentPath}`);
  lines.push(`Alf ContentStore sizing= ${await getDirectorySize(config.contentPath)}`);
  lines.push('');

  addBanner(lines, 'Alf ContentStore year');
  lines.push(...await getContentStoreYearSummary(config.contentPath));

  addBanner(lines, 'Alf ContentStore 13M older');
  lines.push(...await getContentStoreLastMonths(config.contentPath, 13));

  addBanner(lines, 'Alf Contnetstore total file');
  lines.push(...await getContentStoreTotalFiles(config.contentPath));

  addBanner(lines, 'Database sizing (PostgreSQL)');
  lines.push(...await getPathSizing(config.postgresPath));

  addBanner(lines, 'Solr sizing');
  lines.push(...await getPathSizing(path.join(config.solrPath, 'solr-data')));

  addBanner(lines, 'Back up');
  const backupDate = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}-${String(now.getDate()).padStart(2, '0')}`;
  lines.push(await runBackupJob('workspace', config.workspaceSourceDir, config.backupDir, process.env.WORKSPACE_BACKUP_EXCLUDES || 'backup data logs config/glowroot', backupDate));
  lines.push(await runBackupJob('postgresql', config.postgresSourceDir, config.backupDir, process.env.POSTGRES_BACKUP_EXCLUDES || '', backupDate));
  lines.push('');

  addBanner(lines, 'Docker details');
  lines.push(...await getDockerDetailsLines());

  await fs.writeFile(outputFile, `${lines.join('\n')}\n`, 'utf8');

  return {
    outputFile,
    stdout: `Report written to ${outputFile}\n`,
    stderr: ''
  };
}

module.exports = {
  runPmReport
};
