import express from "express";
import crypto from "crypto";

const app = express();
const PORT = process.env.PORT || 3000;
const DISCOGS_API_BASE = "https://api.discogs.com";
const USER_AGENT = "discogs-bulk-importer/2.1";
const jobs = new Map();

app.use(express.json({ limit: "5mb" }));

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function discogsHeaders(token) {
  return {
    Authorization: `Discogs token=${token}`,
    "User-Agent": USER_AGENT,
    Accept: "application/json"
  };
}

function normalizeKey(key) {
  return String(key || "").trim().toLowerCase().replace(/\s+/g, "_");
}

function releaseIdFromValue(value) {
  if (value == null) return null;
  const raw = String(value).trim();
  if (!raw) return null;
  if (/^\d+$/.test(raw)) return raw;
  const match = raw.match(/\/release\/(\d+)/i);
  return match ? match[1] : null;
}

function extractReleases(rows) {
  const candidates = [
    "release_id",
    "discogs_release_id",
    "discogs_id",
    "id",
    "discogs_url",
    "url",
    "release_url"
  ];

  const releases = [];

  rows.forEach((row, index) => {
    const normalizedRow = Object.fromEntries(
      Object.entries(row).map(([k, v]) => [normalizeKey(k), v])
    );

    let releaseId = null;
    let sourceKey = null;
    for (const key of candidates) {
      if (key in normalizedRow) {
        const parsed = releaseIdFromValue(normalizedRow[key]);
        if (parsed) {
          releaseId = parsed;
          sourceKey = key;
          break;
        }
      }
    }

    if (!releaseId) return;

    releases.push({
      index: index + 1,
      releaseId,
      sourceKey: sourceKey || "unknown"
    });
  });

  return releases;
}

async function verifyToken(token) {
  const response = await fetch(`${DISCOGS_API_BASE}/oauth/identity`, {
    headers: discogsHeaders(token)
  });
  const text = await response.text();
  let body;
  try {
    body = JSON.parse(text);
  } catch {
    body = { raw: text };
  }
  return { ok: response.ok, status: response.status, body };
}

async function addReleaseToCollection({ username, token, releaseId, folderId = 1 }) {
  const url = `${DISCOGS_API_BASE}/users/${encodeURIComponent(username)}/collection/folders/${encodeURIComponent(folderId)}/releases/${encodeURIComponent(releaseId)}`;
  const response = await fetch(url, {
    method: "POST",
    headers: discogsHeaders(token)
  });

  const text = await response.text();
  let body;
  try {
    body = JSON.parse(text);
  } catch {
    body = { raw: text };
  }

  const remaining = response.headers.get("x-discogs-ratelimit-remaining");
  const used = response.headers.get("x-discogs-ratelimit-used");

  return {
    ok: response.ok,
    status: response.status,
    body,
    rateLimit: { remaining, used }
  };
}

function createJob({ username, total, folderId }) {
  const id = crypto.randomUUID();
  const job = {
    id,
    username,
    folderId,
    total,
    processed: 0,
    succeeded: 0,
    failed: 0,
    state: "queued",
    startedAt: null,
    finishedAt: null,
    results: []
  };
  jobs.set(id, job);
  return job;
}

async function runImportJob(job, { username, token, releases, folderId = 1 }) {
  job.state = "running";
  job.startedAt = new Date().toISOString();

  for (const item of releases) {
    const releaseId = String(item.releaseId || "").trim();

    if (!/^\d+$/.test(releaseId)) {
      job.failed += 1;
      job.processed += 1;
      job.results.push({ releaseId, ok: false, message: "Invalid release ID" });
      continue;
    }

    try {
      const result = await addReleaseToCollection({ username, token, releaseId, folderId });

      if (result.ok) {
        job.succeeded += 1;
        job.results.push({
          releaseId,
          ok: true,
          message: "Added",
          status: result.status,
          rateLimit: result.rateLimit
        });
      } else {
        job.failed += 1;
        job.results.push({
          releaseId,
          ok: false,
          message: result.body?.message || "Request failed",
          status: result.status,
          rateLimit: result.rateLimit,
          body: result.body
        });
      }
    } catch (error) {
      job.failed += 1;
      job.results.push({
        releaseId,
        ok: false,
        message: error.message || "Unexpected error"
      });
    }

    job.processed += 1;
    await sleep(1100);
  }

  job.state = "finished";
  job.finishedAt = new Date().toISOString();
}

const html = String.raw`<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Discogs Bulk Importer</title>
  <script src="https://cdn.jsdelivr.net/npm/papaparse@5.4.1/papaparse.min.js"></script>
  <style>
    body { font-family: system-ui, sans-serif; margin: 0; background: #0f172a; color: #e2e8f0; }
    .wrap { max-width: 1000px; margin: 0 auto; padding: 24px; }
    .card { background: #111827; border: 1px solid #334155; border-radius: 16px; padding: 20px; margin-bottom: 16px; }
    h1, h2 { margin-top: 0; }
    .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(260px, 1fr)); gap: 12px; }
    input, button { width: 100%; box-sizing: border-box; padding: 12px; border-radius: 10px; border: 1px solid #475569; background: #0b1220; color: #e2e8f0; }
    button { cursor: pointer; font-weight: 600; }
    .muted { color: #94a3b8; font-size: 14px; }
    .row { display: flex; gap: 12px; flex-wrap: wrap; }
    .row > * { flex: 1 1 220px; }
    table { width: 100%; border-collapse: collapse; font-size: 14px; }
    th, td { text-align: left; padding: 8px; border-bottom: 1px solid #243041; }
    .progress { width: 100%; height: 18px; background: #0b1220; border: 1px solid #334155; border-radius: 999px; overflow: hidden; }
    .bar { height: 100%; width: 0%; background: linear-gradient(90deg, #38bdf8, #22c55e); transition: width 0.3s ease; }
    .status { white-space: pre-wrap; background: #0b1220; border-radius: 12px; padding: 12px; max-height: 320px; overflow: auto; font-family: ui-monospace, monospace; }
    code { background: #0b1220; padding: 2px 6px; border-radius: 6px; }
  </style>
</head>
<body>
  <div class="wrap">
    <div class="card">
      <h1>Discogs Bulk Importer</h1>
      <p class="muted">Everything imports into folder <code>1</code>. Existing copies are not skipped, so duplicates can be added as extra copies.</p>
      <div class="grid">
        <div>
          <label>Discogs username</label>
          <input id="username" placeholder="your_discogs_username" />
        </div>
        <div>
          <label>Personal access token</label>
          <input id="token" type="password" placeholder="paste Discogs token" />
        </div>
        <div>
          <label>Collection folder</label>
          <input id="folderId" value="1" disabled />
        </div>
      </div>
      <p class="muted">CSV columns accepted: <code>release_id</code>, <code>discogs_release_id</code>, <code>discogs_url</code>, <code>url</code>.</p>
    </div>

    <div class="card">
      <h2>1. Connect account</h2>
      <div class="row">
        <button id="verifyBtn">Verify token</button>
      </div>
    </div>

    <div class="card">
      <h2>2. Upload CSV and preview</h2>
      <input id="csvFile" type="file" accept=".csv,text/csv" />
      <div class="row" style="margin-top:12px;">
        <button id="previewBtn">Preview CSV</button>
        <button id="startBtn">Start import</button>
      </div>
      <p id="summary" class="muted">No CSV loaded.</p>
      <div style="overflow:auto; margin-top:12px;">
        <table>
          <thead><tr><th>#</th><th>Release ID</th><th>Source</th></tr></thead>
          <tbody id="previewBody"></tbody>
        </table>
      </div>
    </div>

    <div class="card">
      <h2>3. Progress</h2>
      <div class="progress"><div id="bar" class="bar"></div></div>
      <p id="progressText" class="muted">Idle.</p>
      <div id="status" class="status">Ready.</div>
    </div>
  </div>

  <script>
    const state = { rows: [], releases: [], pollTimer: null };

    function setStatus(text) {
      document.getElementById('status').textContent = text;
    }

    function appendStatus(text) {
      const el = document.getElementById('status');
      el.textContent += '\n' + text;
      el.scrollTop = el.scrollHeight;
    }

    function renderPreview() {
      const body = document.getElementById('previewBody');
      body.innerHTML = '';
      const summary = document.getElementById('summary');
      if (!state.releases.length) {
        summary.textContent = 'No release IDs found.';
        return;
      }
      summary.textContent = 'Found ' + state.releases.length + ' rows with release IDs. Duplicates are preserved.';
      state.releases.slice(0, 200).forEach(function(item, idx) {
        const tr = document.createElement('tr');
        tr.innerHTML = '<td>' + (idx + 1) + '</td><td>' + item.releaseId + '</td><td>' + item.sourceKey + '</td>';
        body.appendChild(tr);
      });
    }

    function readCsvFile(file) {
      return new Promise(function(resolve, reject) {
        Papa.parse(file, {
          header: true,
          skipEmptyLines: true,
          complete: function(results) { resolve(results.data); },
          error: reject
        });
      });
    }

    async function previewCsv() {
      const file = document.getElementById('csvFile').files[0];
      if (!file) throw new Error('Choose a CSV first.');
      setStatus('Parsing CSV...');
      state.rows = await readCsvFile(file);
      const response = await fetch('/api/preview', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ rows: state.rows })
      });
      const data = await response.json();
      if (!response.ok) throw new Error(data.message || 'Preview failed');
      state.releases = data.releases;
      renderPreview();
      setStatus('Parsed ' + data.rowCount + ' rows. Found ' + data.releaseCount + ' importable release rows.');
    }

    async function verifyToken() {
      const token = document.getElementById('token').value.trim();
      const username = document.getElementById('username').value.trim();
      if (!token || !username) throw new Error('Enter username and token first.');
      setStatus('Verifying token...');
      const response = await fetch('/api/verify', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ username, token })
      });
      const data = await response.json();
      if (!response.ok) throw new Error(data.message || 'Verification failed');
      setStatus(JSON.stringify(data, null, 2));
    }

    async function pollJob(jobId) {
      const response = await fetch('/api/jobs/' + jobId);
      const data = await response.json();
      if (!response.ok) throw new Error(data.message || 'Failed to read job');

      const pct = data.total ? Math.round((data.processed / data.total) * 100) : 0;
      document.getElementById('bar').style.width = pct + '%';
      document.getElementById('progressText').textContent = pct + '% — ' + data.processed + '/' + data.total + ' processed, ' + data.succeeded + ' added, ' + data.failed + ' failed';
      setStatus(JSON.stringify(data.recentResults, null, 2));

      if (data.state === 'finished') {
        clearInterval(state.pollTimer);
        state.pollTimer = null;
        appendStatus('\nImport complete.');
      }
    }

    async function startImport() {
      const token = document.getElementById('token').value.trim();
      const username = document.getElementById('username').value.trim();
      if (!token || !username) throw new Error('Enter username and token first.');
      if (!state.releases.length) throw new Error('Preview the CSV first.');

      setStatus('Starting import job...');
      const response = await fetch('/api/import/start', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ username, token, folderId: 1, releases: state.releases })
      });
      const data = await response.json();
      if (!response.ok) throw new Error(data.message || 'Failed to start import');
      if (state.pollTimer) clearInterval(state.pollTimer);
      await pollJob(data.jobId);
      state.pollTimer = setInterval(function() {
        pollJob(data.jobId).catch(function(err) { setStatus(String(err)); });
      }, 1500);
    }

    document.getElementById('previewBtn').addEventListener('click', function() { previewCsv().catch(function(err) { setStatus(String(err)); }); });
    document.getElementById('verifyBtn').addEventListener('click', function() { verifyToken().catch(function(err) { setStatus(String(err)); }); });
    document.getElementById('startBtn').addEventListener('click', function() { startImport().catch(function(err) { setStatus(String(err)); }); });
  </script>
</body>
</html>`;

app.get("/", (_req, res) => {
  res.type("html").send(html);
});

app.post('/api/preview', (req, res) => {
  const rows = Array.isArray(req.body?.rows) ? req.body.rows : [];
  const releases = extractReleases(rows);
  res.json({ rowCount: rows.length, releaseCount: releases.length, releases });
});

app.post('/api/verify', async (req, res) => {
  const { username, token } = req.body || {};
  if (!username || !token) {
    return res.status(400).json({ ok: false, message: 'username and token are required' });
  }

  try {
    const result = await verifyToken(token);
    if (!result.ok) {
      return res.status(result.status).json({ ok: false, message: result.body?.message || 'Verification failed', body: result.body });
    }

    const actualUsername = String(result.body?.username || '');
    return res.json({
      ok: true,
      identity: result.body,
      usernameMatches: actualUsername.toLowerCase() === String(username).toLowerCase(),
      enteredUsername: username,
      actualUsername
    });
  } catch (error) {
    return res.status(500).json({ ok: false, message: error.message || 'Unexpected error' });
  }
});

app.post('/api/import/start', async (req, res) => {
  const { username, token, folderId = 1, releases = [] } = req.body || {};
  if (!username || !token) {
    return res.status(400).json({ ok: false, message: 'username and token are required' });
  }
  if (!Array.isArray(releases) || releases.length === 0) {
    return res.status(400).json({ ok: false, message: 'releases must be a non-empty array' });
  }

  const job = createJob({ username, total: releases.length, folderId });
  runImportJob(job, { username, token, releases, folderId }).catch((error) => {
    job.state = 'finished';
    job.finishedAt = new Date().toISOString();
    job.results.push({ ok: false, message: error.message || 'Fatal job error' });
  });

  res.json({ ok: true, jobId: job.id, total: job.total, folderId: job.folderId });
});

app.get('/api/jobs/:id', (req, res) => {
  const job = jobs.get(req.params.id);
  if (!job) {
    return res.status(404).json({ ok: false, message: 'Job not found' });
  }

  res.json({
    ok: true,
    id: job.id,
    username: job.username,
    folderId: job.folderId,
    state: job.state,
    total: job.total,
    processed: job.processed,
    succeeded: job.succeeded,
    failed: job.failed,
    startedAt: job.startedAt,
    finishedAt: job.finishedAt,
    recentResults: job.results.slice(-20)
  });
});

app.listen(PORT, () => {
  console.log(`Discogs bulk importer listening on http://localhost:${PORT}`);
});
