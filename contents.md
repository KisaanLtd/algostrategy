# 📄 Project File Contents

## 📁 algostrategy/

### 📄 Dockerfile

```
# ─────────────────────────────────────────────
#  Stage 1: Build TA-Lib from source
# ─────────────────────────────────────────────
FROM python:3.11-slim AS talib-builder

RUN apt-get update && apt-get install -y \
    wget build-essential gcc make \
    && rm -rf /var/lib/apt/lists/*

# Download and compile TA-Lib C library
RUN wget https://downloads.sourceforge.net/project/ta-lib/ta-lib/0.4.0/ta-lib-0.4.0-src.tar.gz \
    && tar -xzf ta-lib-0.4.0-src.tar.gz \
    && cd ta-lib \
    && ./configure --prefix=/usr/local \
    && make \
    && make install \
    && ldconfig

# ─────────────────────────────────────────────
#  Stage 2: Final app image
# ─────────────────────────────────────────────
FROM python:3.11-slim

# Copy compiled TA-Lib from builder stage
COPY --from=talib-builder /usr/local/lib/libta_lib* /usr/local/lib/
COPY --from=talib-builder /usr/local/include/ta-lib /usr/local/include/ta-lib

RUN apt-get update && apt-get install -y \
    libgomp1 git \
    && ldconfig \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy app source
COPY . .

# Cloud Run uses PORT env variable (default 8080)
ENV PORT=8080

# Gunicorn serves Flask app
CMD ["gunicorn", "--bind", "0.0.0.0:8080", "--workers", "2", "--timeout", "120", "main:app"]
```

### 📄 cloud-run-job.yaml

```yaml
# ─────────────────────────────────────────────
#  cloud-run-job.yaml
#  Deploy as: gcloud run jobs replace cloud-run-job.yaml
# ─────────────────────────────────────────────
apiVersion: run.googleapis.com/v1
kind: Job
metadata:
  name: algostrategy-pipeline
  annotations:
    run.googleapis.com/launch-stage: GA
spec:
  template:
    spec:
      taskCount: 1
      timeoutSeconds: 3600       # 1 hour max per run (covers full market session)
      template:
        spec:
          containers:
            - image: gcr.io/algostratgy/algostrategy:latest
              command: ["python", "scripts/tvdata_update.py"]
              resources:
                limits:
                  cpu: "1"
                  memory: "1Gi"
              env:
                # ── Load all secrets from Secret Manager ──
                - name: DB_HOST
                  valueFrom:
                    secretKeyRef:
                      name: db-host
                      key: latest
                - name: DB_PORT
                  valueFrom:
                    secretKeyRef:
                      name: db-port
                      key: latest
                - name: DB_USER
                  valueFrom:
                    secretKeyRef:
                      name: db-user
                      key: latest
                - name: DB_PASSWORD
                  valueFrom:
                    secretKeyRef:
                      name: db-password
                      key: latest
                - name: DB_NAME
                  valueFrom:
                    secretKeyRef:
                      name: db-name
                      key: latest
                - name: API_KEY
                  valueFrom:
                    secretKeyRef:
                      name: breeze-api-key
                      key: latest
                - name: API_SECRET
                  valueFrom:
                    secretKeyRef:
                      name: breeze-api-secret
                      key: latest
                - name: SESSION_TOKEN
                  valueFrom:
                    secretKeyRef:
                      name: breeze-session-token
                      key: latest   # Update this secret daily before market open
          serviceAccountName: algostrategy-sa@algostratgy.iam.gserviceaccount.com
```

### 📄 deploy.sh

```sh
#!/bin/bash
# ─────────────────────────────────────────────
#  deploy.sh — One-time GCP setup for algostrategy
#  Run from your local machine with gcloud CLI installed
# ─────────────────────────────────────────────

PROJECT_ID="algostratgy"         # ← CHANGE THIS
REGION="asia-south1"                       # Mumbai — closest to NSE
IMAGE="gcr.io/$PROJECT_ID/algostrategy"
SA_NAME="algostrategy-sa"

echo "==> Setting project"
gcloud config set project $PROJECT_ID

# ── Enable required APIs ──────────────────────
echo "==> Enabling APIs"
gcloud services enable \
  run.googleapis.com \
  cloudscheduler.googleapis.com \
  secretmanager.googleapis.com \
  containerregistry.googleapis.com \
  cloudbuild.googleapis.com

# ── Create Service Account ────────────────────
echo "==> Creating service account"
gcloud iam service-accounts create $SA_NAME \
  --display-name="AlgoStrategy Runner"

gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:$SA_NAME@$PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/secretmanager.secretAccessor"

gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:$SA_NAME@$PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/run.invoker"

# ── Store secrets in Secret Manager ──────────
echo "==> Creating secrets (fill values when prompted)"
for SECRET in db-host db-port db-user db-password db-name breeze-api-key breeze-api-secret breeze-session-token; do
  read -sp "Enter value for $SECRET: " SECRET_VALUE
  echo ""
  echo -n "$SECRET_VALUE" | gcloud secrets create $SECRET --data-file=-
done

# ── Copy ca.pem into Secret Manager ──────────
echo "==> Storing ca.pem as secret"
gcloud secrets create db-ca-cert --data-file=ca.pem

# ── Build and push Docker image ───────────────
echo "==> Building Docker image (this takes ~5-8 min for TA-Lib compile)"
gcloud builds submit --tag $IMAGE .

# ── Deploy Flask service (main.py) ────────────
echo "==> Deploying Flask web service"
gcloud run deploy algostrategy-web \
  --image=$IMAGE \
  --platform=managed \
  --region=$REGION \
  --service-account="$SA_NAME@$PROJECT_ID.iam.gserviceaccount.com" \
  --no-allow-unauthenticated \
  --memory=512Mi \
  --cpu=1 \
  --min-instances=0 \
  --max-instances=2

# ── Deploy data pipeline as Cloud Run Job ─────
echo "==> Deploying tvdata pipeline Job"
gcloud run jobs replace cloud-run-job.yaml \
  --region=$REGION

# ── Schedule Job at 09:14 IST Mon–Fri ─────────
echo "==> Creating Cloud Scheduler trigger"
gcloud scheduler jobs create http algostrategy-market-trigger \
  --schedule="44 3 * * 1-5" \
  --time-zone="UTC" \
  --uri="https://run.googleapis.com/v2/projects/$PROJECT_ID/locations/$REGION/jobs/algostrategy-pipeline:run" \
  --http-method=POST \
  --oauth-service-account-email="$SA_NAME@$PROJECT_ID.iam.gserviceaccount.com" \
  --location=$REGION

echo ""
echo "✅ Deployment complete!"
echo "   Web service: https://algostrategy-web-xxxx-$REGION.run.app"
echo "   Pipeline Job: runs weekdays at 09:14 IST"
```

### 📄 firebase.json

```json
{
  "hosting": {
    "public": "public",
    "ignore": [
      "firebase.json",
      "**/.*",
      "**/node_modules/**"
    ],
    "rewrites": [
      {
        "source": "/breeze/**",
        "run": {
          "serviceId": "algostratgy-web",
          "region": "asia-south1"
        }
      },
      {
        "source": "/status",
        "run": {
          "serviceId": "algostratgy-web",
          "region": "asia-south1"
        }
      },
      {
        "source": "/data",
        "run": {
          "serviceId": "algostratgy-web",
          "region": "asia-south1"
        }
      },
      {
        "source": "/**",
        "run": {
          "serviceId": "algostratgy-web",
          "region": "asia-south1"
        }
      }
    ]
  }
}
```

### 📄 main.py

```py
import os
import asyncio
import aiomysql
import ssl
from datetime import datetime
import pytz
from flask import Flask, jsonify, render_template_string, request
from google.cloud import secretmanager
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)
IST = pytz.timezone("Asia/Kolkata")

HOLIDAYS = [
    # 2025
    "2025-02-26", "2025-03-14", "2025-03-31", "2025-04-10", "2025-04-14",
    "2025-04-18", "2025-05-01", "2025-08-15", "2025-08-27", "2025-10-02",
    "2025-10-21", "2025-10-22", "2025-11-05", "2025-11-15", "2025-12-25",
    # 2026 — NSE Circular NSE/CMTR/71775
    "2026-01-26", "2026-03-03", "2026-03-26", "2026-03-31", "2026-04-03",
    "2026-04-14", "2026-05-01", "2026-05-28", "2026-06-26", "2026-09-14",
    "2026-10-02", "2026-10-20", "2026-11-10", "2026-11-24", "2026-12-25",
]

DASHBOARD_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>AlgoStrategy — Live Dashboard</title>
<link href="https://fonts.googleapis.com/css2?family=Share+Tech+Mono&family=Rajdhani:wght@400;600;700&display=swap" rel="stylesheet">
<style>
  :root {
    --bg: #0a0e14; --panel: #0f1620; --border: #1a2840;
    --accent: #00d4ff; --accent2: #00ff88; --warn: #ffaa00;
    --danger: #ff4455; --text: #c8d8e8; --muted: #4a6080;
  }
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body {
    background: var(--bg); color: var(--text);
    font-family: 'Rajdhani', sans-serif; font-size: 16px;
    min-height: 100vh; overflow-x: hidden;
  }
  body::before {
    content: ''; position: fixed; inset: 0;
    background:
      radial-gradient(ellipse 80% 50% at 50% -10%, rgba(0,212,255,0.08) 0%, transparent 60%),
      repeating-linear-gradient(0deg, transparent, transparent 39px, rgba(0,212,255,0.03) 40px),
      repeating-linear-gradient(90deg, transparent, transparent 39px, rgba(0,212,255,0.03) 40px);
    pointer-events: none; z-index: 0;
  }
  .container { max-width: 1100px; margin: 0 auto; padding: 40px 24px; position: relative; z-index: 1; }
  header { display: flex; align-items: center; justify-content: space-between; margin-bottom: 32px; }
  .logo { font-family: 'Share Tech Mono', monospace; font-size: 13px; color: var(--accent); letter-spacing: 2px; }
  .logo span { color: var(--muted); }
  h1 { font-size: 26px; font-weight: 700; letter-spacing: 3px; text-transform: uppercase; color: #fff; }
  .live-dot {
    width: 8px; height: 8px; border-radius: 50%; background: var(--accent2);
    box-shadow: 0 0 8px var(--accent2); animation: pulse 2s infinite;
    display: inline-block; margin-right: 8px;
  }
  @keyframes pulse { 0%,100% { opacity: 1; } 50% { opacity: 0.3; } }

  /* refresh flash — fades the value briefly on update */
  @keyframes flash { 0% { opacity: 0.3; } 100% { opacity: 1; } }
  .flash { animation: flash 0.4s ease-out; }

  /* spinner on card title when fetching */
  .spin {
    display: inline-block; width: 8px; height: 8px;
    border: 1.5px solid var(--muted); border-top-color: var(--accent);
    border-radius: 50%; animation: spinner 0.7s linear infinite;
    margin-left: 8px; vertical-align: middle; opacity: 0;
    transition: opacity 0.2s;
  }
  .spin.active { opacity: 1; }
  @keyframes spinner { to { transform: rotate(360deg); } }

  .status-bar {
    background: var(--panel); border: 1px solid var(--border);
    border-top: 2px solid var(--accent); border-radius: 4px;
    padding: 20px 24px; margin-bottom: 24px;
    display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 20px;
  }
  .stat-item label {
    font-family: 'Share Tech Mono', monospace; font-size: 10px;
    color: var(--muted); letter-spacing: 2px; text-transform: uppercase;
    display: block; margin-bottom: 6px;
  }
  .stat-item .value { font-size: 20px; font-weight: 700; color: #fff; }
  .stat-item .value.green  { color: var(--accent2); }
  .stat-item .value.red    { color: var(--danger); }
  .stat-item .value.yellow { color: var(--warn); }
  .stat-item .value.blue   { color: var(--accent); }
  .stat-item .sub {
    font-family: 'Share Tech Mono', monospace; font-size: 10px;
    color: var(--muted); margin-top: 3px;
  }
  .grid { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; margin-bottom: 24px; }
  @media (max-width: 700px) { .grid { grid-template-columns: 1fr; } }
  .card {
    background: var(--panel); border: 1px solid var(--border);
    border-radius: 4px; padding: 20px;
  }
  .card-title {
    display: flex; align-items: center; justify-content: space-between;
    font-family: 'Share Tech Mono', monospace; font-size: 11px;
    letter-spacing: 2px; color: var(--muted); text-transform: uppercase;
    margin-bottom: 16px; padding-bottom: 10px; border-bottom: 1px solid var(--border);
  }
  .card-title-left { display: flex; align-items: center; gap: 6px; }
  .last-updated {
    font-family: 'Share Tech Mono', monospace; font-size: 9px;
    color: var(--muted); opacity: 0.6; font-weight: 400; letter-spacing: 0.5px;
  }
  .data-table { width: 100%; border-collapse: collapse; font-size: 13px; }
  .data-table th {
    font-family: 'Share Tech Mono', monospace; font-size: 10px;
    color: var(--muted); letter-spacing: 1px; text-align: left;
    padding: 6px 8px; border-bottom: 1px solid var(--border);
  }
  .data-table td { padding: 7px 8px; border-bottom: 1px solid rgba(26,40,64,0.5); color: var(--text); font-size: 13px; }
  .data-table tr:last-child td { border-bottom: none; }
  .data-table tr:hover td { background: rgba(0,212,255,0.04); }
  .badge {
    font-family: 'Share Tech Mono', monospace; font-size: 10px;
    padding: 2px 8px; border-radius: 2px; display: inline-block;
  }
  .badge.call { background: rgba(0,255,136,0.1); color: var(--accent2); border: 1px solid rgba(0,255,136,0.3); }
  .badge.put  { background: rgba(255,68,85,0.1);  color: var(--danger);  border: 1px solid rgba(255,68,85,0.3); }
  .badge.none { background: rgba(74,96,128,0.2);  color: var(--muted);   border: 1px solid rgba(74,96,128,0.3); }
  .signal-row { display: flex; align-items: center; gap: 10px; padding: 9px 0; border-bottom: 1px solid rgba(26,40,64,0.5); }
  .signal-row:last-child { border-bottom: none; }
  .signal-label { font-family: 'Share Tech Mono', monospace; font-size: 10px; color: var(--muted); min-width: 110px; }
  .signal-value { font-size: 15px; font-weight: 600; color: #fff; }
  .signal-value.green { color: var(--accent2); }
  .signal-value.red   { color: var(--danger); }
  .signal-value.blue  { color: var(--accent); }
  .loading { color: var(--muted); font-family: 'Share Tech Mono', monospace; font-size: 12px; }
  .full-width { grid-column: 1 / -1; }
  footer {
    font-family: 'Share Tech Mono', monospace; font-size: 11px;
    color: var(--muted); text-align: center; margin-top: 40px; letter-spacing: 1px;
  }
  /* countdown bar */
  .refresh-bar {
    height: 2px; background: var(--border); border-radius: 1px;
    margin-top: 8px; overflow: hidden;
  }
  .refresh-bar-fill {
    height: 100%; background: var(--accent); border-radius: 1px;
    transition: width linear;
  }
</style>
</head>
<body>
<div class="container">

  <header>
    <div>
      <div class="logo">ALGOSTRATGY <span>// LIVE DASHBOARD</span></div>
      <h1><span class="live-dot"></span>System Status</h1>
    </div>
    <div style="font-family:'Share Tech Mono',monospace;font-size:12px;color:var(--muted);text-align:right;">
      <div id="clock" style="font-size:16px;color:var(--accent);">--:--:--</div>
      <div style="margin-top:2px;">IST</div>
      <div id="next-refresh" style="margin-top:4px;font-size:10px;color:var(--muted);">next: --s</div>
    </div>
  </header>

  <!-- STATUS BAR: updates every 15s via /status + /signals -->
  <div class="status-bar" id="status-bar">
    <div class="stat-item">
      <label>Market Status</label>
      <div class="value" id="market-status">—</div>
    </div>
    <div class="stat-item">
      <label>Day High</label>
      <div class="value green" id="dayhigh-val">—</div>
    </div>
    <div class="stat-item">
      <label>Day Low</label>
      <div class="value red" id="daylow-val">—</div>
    </div>
    <div class="stat-item">
      <label>Option Signal</label>
      <div class="value" id="option-type-status">—</div>
    </div>
    <div class="stat-item">
      <label>Strike Price</label>
      <div class="value blue" id="strike-status">—</div>
    </div>
    <div class="stat-item">
      <label>Bull / Bear</label>
      <div class="value" id="bull-bear-status">—</div>
    </div>
  </div>

  <div class="grid">

    <!-- LATEST SIGNAL — refreshes every 15s -->
    <div class="card">
      <div class="card-title">
        <div class="card-title-left">
          Latest Signal
          <span class="spin" id="spin-signal"></span>
        </div>
        <span class="last-updated" id="lu-signal">—</span>
      </div>
      <div id="signal-panel"><span class="loading">fetching...</span></div>
    </div>

    <!-- OHLC — refreshes every 30s -->
    <div class="card">
      <div class="card-title">
        <div class="card-title-left">
          Latest OHLC
          <span class="spin" id="spin-ohlc"></span>
        </div>
        <span class="last-updated" id="lu-ohlc">—</span>
      </div>
      <div id="ohlc-table"><span class="loading">fetching...</span></div>
    </div>

  </div>

  <div class="grid">

    <!-- SIGNAL HISTORY — refreshes every 30s -->
    <div class="card">
      <div class="card-title">
        <div class="card-title-left">
          Signal History (Today)
          <span class="spin" id="spin-history"></span>
        </div>
        <span class="last-updated" id="lu-history">—</span>
      </div>
      <div id="signals-table"><span class="loading">fetching...</span></div>
    </div>

    <!-- DB STATUS — refreshes every 60s -->
    <div class="card">
      <div class="card-title">
        <div class="card-title-left">
          DB Row Counts
          <span class="spin" id="spin-db"></span>
        </div>
        <span class="last-updated" id="lu-db">—</span>
      </div>
      <div id="db-panel"><span class="loading">fetching...</span></div>
      <div class="refresh-bar" style="margin-top:12px;">
        <div class="refresh-bar-fill" id="db-bar" style="width:100%;"></div>
      </div>
    </div>

  </div>

</div>
<footer style="padding-bottom:32px;">
  ALGOSTRATGY · asia-south1 · auto-refresh active
</footer>

<script>
  // ── Helpers ────────────────────────────────────────────────────────────────
  function fmt(v, dec=2) {
    if (v === null || v === undefined) return '—';
    const n = parseFloat(v);
    return isNaN(n) ? String(v) : n.toFixed(dec);
  }
  function fmtTime(iso) {
    if (!iso) return '—';
    return iso.split(' ')[1]?.slice(0,5) || iso;
  }
  function flash(id) {
    const el = document.getElementById(id);
    if (!el) return;
    el.classList.remove('flash');
    void el.offsetWidth;
    el.classList.add('flash');
  }
  function setLU(id) {
    const el = document.getElementById(id);
    if (el) {
      const now = new Date();
      const h = String(now.getHours()).padStart(2,'0');
      const m = String(now.getMinutes()).padStart(2,'0');
      const s = String(now.getSeconds()).padStart(2,'0');
      el.textContent = `updated ${h}:${m}:${s}`;
    }
  }
  function spin(id, on) {
    const el = document.getElementById('spin-' + id);
    if (el) el.classList.toggle('active', on);
  }

  // ── IST clock ──────────────────────────────────────────────────────────────
  function updateClock() {
    const ist = new Date(new Date().toLocaleString('en-US', { timeZone: 'Asia/Kolkata' }));
    const h = String(ist.getHours()).padStart(2,'0');
    const m = String(ist.getMinutes()).padStart(2,'0');
    const s = String(ist.getSeconds()).padStart(2,'0');
    document.getElementById('clock').textContent = `${h}:${m}:${s}`;
  }
  setInterval(updateClock, 1000);
  updateClock();

  // ── Countdown to next candle+refresh ───────────────────────────────────────
  function updateCountdown() {
    const now = new Date();
    const ist = new Date(now.toLocaleString('en-US', { timeZone: 'Asia/Kolkata' }));
    const secsIntoMin = ist.getSeconds();
    const secsLeft = 60 - secsIntoMin;
    document.getElementById('next-refresh').textContent = `next candle: ${secsLeft}s`;
  }
  setInterval(updateCountdown, 1000);
  updateCountdown();

  // ── /status ── every 15s ───────────────────────────────────────────────────
  async function fetchStatus() {
    try {
      const r = await fetch('/status');
      const d = await r.json();
      const el = document.getElementById('market-status');
      if (d.market_open) {
        el.textContent = 'OPEN';
        el.className = 'value green';
      } else {
        el.textContent = 'CLOSED';
        el.className = 'value red';
      }
      flash('market-status');
    } catch(e) {}
  }

  // ── /signals ── every 15s ──────────────────────────────────────────────────
  async function fetchSignals() {
    spin('signal', true);
    try {
      const r = await fetch('/signals');
      const d = await r.json();

      // ── status bar fields from latest ──
      const lat = d.latest;
      if (lat) {
        // dayhigh / daylow
        const dh = document.getElementById('dayhigh-val');
        const dl = document.getElementById('daylow-val');
        dh.textContent = fmt(lat.dayhigh, 2);
        dl.textContent = fmt(lat.daylow, 2);
        flash('dayhigh-val'); flash('daylow-val');

        // option type + strike
        const ot = document.getElementById('option-type-status');
        const sp = document.getElementById('strike-status');
        if (lat.option_type) {
          ot.textContent = lat.option_type.toUpperCase();
          ot.className = 'value ' + (lat.option_type === 'call' ? 'green' : 'red');
        } else { ot.textContent = '—'; ot.className = 'value'; }
        sp.textContent = lat.strike_price || '—';
        flash('option-type-status'); flash('strike-status');

        // bull / bear
        const bb = document.getElementById('bull-bear-status');
        if (lat.Bull == 1) { bb.textContent = '▲ BULL'; bb.className = 'value green'; }
        else if (lat.Bear == 1) { bb.textContent = '▼ BEAR'; bb.className = 'value red'; }
        else { bb.textContent = '— NEUTRAL'; bb.className = 'value'; }
        flash('bull-bear-status');

        // ── signal panel ──
        const callSig = lat.callbuy_signal == 1;
        const putSig  = lat.putbuy_signal  == 1;
        document.getElementById('signal-panel').innerHTML = `
          <div class="signal-row">
            <span class="signal-label">TIME</span>
            <span class="signal-value blue">${fmtTime(lat.datetime)}</span>
          </div>
          <div class="signal-row">
            <span class="signal-label">OPTION TYPE</span>
            <span class="signal-value ${lat.option_type === 'call' ? 'green' : lat.option_type === 'put' ? 'red' : ''}">
              ${lat.option_type ? lat.option_type.toUpperCase() : '—'}
            </span>
          </div>
          <div class="signal-row">
            <span class="signal-label">STRIKE</span>
            <span class="signal-value blue">${lat.strike_price || '—'}</span>
          </div>
          <div class="signal-row">
            <span class="signal-label">CALL ENTRY</span>
            <span class="signal-value">${fmt(lat.call_entry_trigger)}</span>
          </div>
          <div class="signal-row">
            <span class="signal-label">CALL SL</span>
            <span class="signal-value">${fmt(lat.call_sl_trigger)}</span>
          </div>
          <div class="signal-row">
            <span class="signal-label">PUT ENTRY</span>
            <span class="signal-value">${fmt(lat.put_entry_trigger)}</span>
          </div>
          <div class="signal-row">
            <span class="signal-label">PUT SL</span>
            <span class="signal-value">${fmt(lat.put_sl_trigger)}</span>
          </div>
          <div class="signal-row">
            <span class="signal-label">BUY SIGNAL</span>
            <span class="signal-value ${callSig || putSig ? 'green' : ''}">
              ${callSig ? 'CALL ▲' : putSig ? 'PUT ▼' : '— NONE'}
            </span>
          </div>
          <div class="signal-row">
            <span class="signal-label">ORDER ID</span>
            <span class="signal-value ${lat.order_id ? 'green' : ''}">${lat.order_id || '—'}</span>
          </div>
          <div class="signal-row">
            <span class="signal-label">DAY HIGH</span>
            <span class="signal-value green">${fmt(lat.dayhigh)}</span>
          </div>
          <div class="signal-row" style="border:none;">
            <span class="signal-label">DAY LOW</span>
            <span class="signal-value red">${fmt(lat.daylow)}</span>
          </div>
        `;
      } else {
        document.getElementById('signal-panel').innerHTML =
          '<span class="loading">no signals today yet</span>';
      }

      // ── history table ──
      spin('history', true);
      if (d.history && d.history.length > 0) {
        let rows = d.history.map(row => {
          const ot = row.option_type || '';
          const badge = ot ? `<span class="badge ${ot}">${ot.toUpperCase()}</span>` : '—';
          const sig = row.callbuy_signal == 1 ? '<span style="color:var(--accent2);">▲ CALL</span>'
                    : row.putbuy_signal  == 1 ? '<span style="color:var(--danger);">▼ PUT</span>'
                    : '—';
          return `<tr>
            <td>${fmtTime(row.datetime)}</td>
            <td>${badge}</td>
            <td>${row.strike_price || '—'}</td>
            <td>${fmt(row.call_entry_trigger)}</td>
            <td>${fmt(row.put_entry_trigger)}</td>
            <td>${sig}</td>
            <td>${row.order_id ? '<span style="color:var(--accent2);">✓</span>' : '—'}</td>
          </tr>`;
        }).join('');
        document.getElementById('signals-table').innerHTML = `
          <table class="data-table">
            <thead><tr>
              <th>TIME</th><th>TYPE</th><th>STRIKE</th>
              <th>CALL E</th><th>PUT E</th><th>SIGNAL</th><th>ORD</th>
            </tr></thead>
            <tbody>${rows}</tbody>
          </table>`;
      } else {
        document.getElementById('signals-table').innerHTML =
          '<span class="loading">no signal history today</span>';
      }
      setLU('signal'); setLU('history');
      flash('signal-panel');
    } catch(e) {
      document.getElementById('signal-panel').innerHTML =
        '<span class="loading">error — retrying...</span>';
    }
    spin('signal', false);
    spin('history', false);
  }

  // ── /data ── every 30s ─────────────────────────────────────────────────────
  async function fetchOHLC() {
    spin('ohlc', true);
    try {
      const r = await fetch('/data');
      const d = await r.json();
      if (!d.data || d.data.length === 0) {
        document.getElementById('ohlc-table').innerHTML =
          '<span class="loading">no data</span>';
      } else {
        let rows = d.data.slice(0, 8).map(row => {
          const chg = row.close - row.open;
          const color = chg >= 0 ? 'var(--accent2)' : 'var(--danger)';
          return `<tr>
            <td>${fmtTime(row.datetime)}</td>
            <td>${fmt(row.open)}</td>
            <td style="color:var(--accent2);">${fmt(row.high)}</td>
            <td style="color:var(--danger);">${fmt(row.low)}</td>
            <td style="color:${color};font-weight:600;">${fmt(row.close)}</td>
            <td style="color:${color};">${chg >= 0 ? '+' : ''}${fmt(chg)}</td>
          </tr>`;
        }).join('');
        document.getElementById('ohlc-table').innerHTML = `
          <table class="data-table">
            <thead><tr>
              <th>TIME</th><th>OPEN</th><th>HIGH</th>
              <th>LOW</th><th>CLOSE</th><th>CHG</th>
            </tr></thead>
            <tbody>${rows}</tbody>
          </table>`;
        setLU('ohlc');
        flash('ohlc-table');
      }
    } catch(e) {
      document.getElementById('ohlc-table').innerHTML =
        '<span class="loading">error — retrying...</span>';
    }
    spin('ohlc', false);
  }

  // ── /db ── every 60s ───────────────────────────────────────────────────────
  let dbBarTimer = null;
  async function fetchDB() {
    spin('db', true);
    try {
      const r = await fetch('/db');
      const d = await r.json();
      const connected = d.status === 'connected';
      document.getElementById('db-panel').innerHTML = `
        <div class="signal-row">
          <span class="signal-label">CONNECTION</span>
          <span class="signal-value ${connected ? 'green' : 'red'}">
            ${connected ? '● CONNECTED' : '✕ ERROR'}
          </span>
        </div>
        <div class="signal-row">
          <span class="signal-label">ohlctick_1mdata</span>
          <span class="signal-value blue">${(d.ohlctick_1mdata || 0).toLocaleString()}</span>
        </div>
        <div class="signal-row" style="border:none;">
          <span class="signal-label">indicators_data</span>
          <span class="signal-value blue">${(d.indicators_data || 0).toLocaleString()}</span>
        </div>
      `;
      setLU('db');
      flash('db-panel');
      // animate countdown bar
      const bar = document.getElementById('db-bar');
      if (bar) {
        bar.style.transition = 'none';
        bar.style.width = '100%';
        setTimeout(() => {
          bar.style.transition = 'width 60s linear';
          bar.style.width = '0%';
        }, 50);
      }
    } catch(e) {
      document.getElementById('db-panel').innerHTML =
        '<span class="loading">db unreachable — retrying...</span>';
    }
    spin('db', false);
  }

  // ── Polling schedule ───────────────────────────────────────────────────────
  // Status + signals: every 15s (aligned to candle close + 4-6s offset)
  // OHLC: every 30s
  // DB:   every 60s

  function startPolling() {
    // initial load — stagger to avoid simultaneous requests
    fetchStatus();
    fetchSignals();
    setTimeout(fetchOHLC, 500);
    setTimeout(fetchDB,    1000);

    setInterval(fetchStatus,  15000);
    setInterval(fetchSignals, 15000);
    setInterval(fetchOHLC,    30000);
    setInterval(fetchDB,      60000);
  }

  startPolling();
</script>
</body>
</html>
"""


# ── Helpers ───────────────────────────────────────────────────────────────────

def is_market_open():
    now = datetime.now(IST)
    return now.replace(hour=9, minute=15, second=0, microsecond=0) <= now <= \
           now.replace(hour=15, minute=30, second=0, microsecond=0)

def is_business_day():
    now = datetime.now(IST)
    return now.weekday() < 5 and now.strftime('%Y-%m-%d') not in HOLIDAYS

async def get_db_pool():
    ssl_ctx = None
    ca_path = os.path.join(os.path.dirname(__file__), 'ca.pem')
    if os.path.exists(ca_path):
        ssl_ctx = ssl.create_default_context(cafile=ca_path)
    return await aiomysql.create_pool(
        host=os.getenv("DB_HOST"),
        port=int(os.getenv("DB_PORT", 3306)),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        db=os.getenv("DB_NAME"),
        autocommit=True,
        ssl=ssl_ctx,
    )

# ── Routes ────────────────────────────────────────────────────────────────────

@app.route("/")
def dashboard():
    return render_template_string(DASHBOARD_HTML)

@app.route("/health")
def health():
    return jsonify({"status": "ok"}), 200

@app.route("/status")
def status():
    now = datetime.now(IST)
    return jsonify({
        "time_ist":     now.strftime("%Y-%m-%d %H:%M:%S"),
        "market_open":  is_market_open(),
        "business_day": is_business_day(),
        "weekday":      now.strftime("%A"),
        "date":         now.strftime("%Y-%m-%d"),
    }), 200

@app.route("/data")
def latest_data():
    async def fetch():
        try:
            pool = await get_db_pool()
            async with pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cursor:
                    await cursor.execute(
                        "SELECT datetime, open, high, low, close, ohlc4 "
                        "FROM ohlctick_1mdata ORDER BY datetime DESC LIMIT 10"
                    )
                    rows = await cursor.fetchall()
            pool.close()
            await pool.wait_closed()
            for row in rows:
                if hasattr(row.get('datetime'), 'strftime'):
                    row['datetime'] = row['datetime'].strftime("%Y-%m-%d %H:%M:%S")
            return {"data": rows, "count": len(rows)}, 200
        except Exception as e:
            return {"error": str(e), "data": []}, 500
    result, code = asyncio.run(fetch())
    return jsonify(result), code

@app.route("/signals")
def signals():
    async def fetch():
        try:
            pool = await get_db_pool()
            async with pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cursor:
                    # Latest signal
                    await cursor.execute(
                        "SELECT * FROM placeorder_track "
                        "WHERE DATE(datetime) = CURDATE() "
                        "ORDER BY datetime DESC LIMIT 1"
                    )
                    latest = await cursor.fetchone()

                    # Recent history (last 20 rows today)
                    await cursor.execute(
                        "SELECT datetime, option_type, strike_price, "
                        "call_entry_trigger, put_entry_trigger, "
                        "Bull, Bear, callbuy_signal, putbuy_signal, order_id "
                        "FROM placeorder_track "
                        "WHERE DATE(datetime) = CURDATE() "
                        "ORDER BY datetime DESC LIMIT 20"
                    )
                    history = await cursor.fetchall()

            pool.close()
            await pool.wait_closed()

            def fmt_row(row):
                if row and hasattr(row.get('datetime'), 'strftime'):
                    row['datetime'] = row['datetime'].strftime("%Y-%m-%d %H:%M:%S")
                return row

            return {
                "latest":  fmt_row(latest),
                "history": [fmt_row(r) for r in history],
                "count":   len(history),
            }, 200
        except Exception as e:
            return {"error": str(e), "latest": None, "history": []}, 500
    result, code = asyncio.run(fetch())
    return jsonify(result), code

@app.route("/db")
def db_check():
    async def check():
        try:
            pool = await get_db_pool()
            async with pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute("SELECT COUNT(*) FROM ohlctick_1mdata")
                    ohlc_count = (await cursor.fetchone())[0]
                    await cursor.execute("SELECT COUNT(*) FROM indicators_data")
                    ind_count = (await cursor.fetchone())[0]
            pool.close()
            await pool.wait_closed()
            return {
                "status": "connected",
                "ohlctick_1mdata":  ohlc_count,
                "indicators_data":  ind_count,
            }, 200
        except Exception as e:
            return {"status": "error", "message": str(e)}, 500
    result, code = asyncio.run(check())
    return jsonify(result), code

@app.route("/breeze/callback", methods=["GET"])
def breeze_callback():
    session_token = request.args.get('apisession')
    if not session_token:
        return jsonify({"error": "No session token received"}), 400
    try:
        client = secretmanager.SecretManagerServiceClient()
        response = client.add_secret_version(
            request={
                "parent": "projects/algostratgy/secrets/breeze-session-token",
                "payload": {"data": session_token.encode("utf-8")}
            }
        )
        now = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S IST")
        return f"""
        <html>
        <body style="font-family:monospace;background:#0a0e14;color:#00ff88;padding:40px;text-align:center;">
            <h2>✅ Session Token Saved</h2>
            <p>Saved at: {now}</p>
            <p>Version: {response.name}</p>
            <p>Pipeline will use this token from next execution.</p>
        </body>
        </html>
        """, 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/breeze/status", methods=["GET"])
def breeze_status():
    try:
        client = secretmanager.SecretManagerServiceClient()
        versions = client.list_secret_versions(
            request={
                "parent": "projects/algostratgy/secrets/breeze-session-token",
                "filter": "state=ENABLED"
            }
        )
        latest = next(iter(versions), None)
        if latest:
            return jsonify({
                "token_saved": True,
                "saved_at":    str(latest.create_time),
                "state":       str(latest.state),
            }), 200
        return jsonify({"token_saved": False}), 200
    except Exception as e:
        return jsonify({"token_saved": False, "error": str(e)}), 500

# ── Entry ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
```

### 📄 requirements.txt

```txt
pip
autopep8
Flask==3.0.3
gunicorn==22.0.0
Werkzeug==3.0.6
aiomysql
pandas
numpy
breeze-connect
tradingview-datafeed
python-dotenv
cryptography
TA-Lib==0.6.8
pytz
requests
websocket-client
google-cloud-secret-manager
```

### 📄 run_indicators.sh

```sh
#!/bin/bash
# run_indicators.sh
# Runs indicatordata_all.py (full recalculation + truncate) first,
# then starts indicator_update.py (live per-minute indicator update loop).

set -e

echo "=== [$(date -u '+%Y-%m-%d %H:%M:%S')] Starting indicator pipeline ==="

echo "--- Step 1: Full indicator recalculation (indicatordata_all.py) ---"
python scripts/indicatordata_all.py
echo "--- Step 1 complete ---"

echo "--- Step 2: Live indicator update loop (indicator_update.py) ---"
python scripts/indicator_update.py
echo "--- Step 2 complete ---"

echo "=== Indicator pipeline job finished ==="
```

### 📄 run_pipeline.sh

```sh
#!/bin/bash
# run_pipeline.sh
# Runs tvdata.py (full historical fetch + truncate) first,
# then starts tvdata_update.py (live per-minute update loop).

set -e

echo "=== [$(date -u '+%Y-%m-%d %H:%M:%S')] Starting pipeline ==="

echo "--- Step 1: Full OHLC data refresh (tvdata.py) ---"
python scripts/tvdata.py
echo "--- Step 1 complete ---"

echo "--- Step 2: Live OHLC update loop (tvdata_update.py) ---"
python scripts/tvdata_update.py
echo "--- Step 2 complete ---"

echo "=== Pipeline job finished ==="
```

### 📄 show_data.py

```py

import asyncio
import aiomysql
from dotenv import load_dotenv
import os
import ssl

async def main():
    load_dotenv()
    db_config = {
        "host": os.getenv("DB_HOST"),
        "port": int(os.getenv("DB_PORT")),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "db": os.getenv("DB_NAME"),
    }

    ssl_context = ssl.create_default_context(cafile='ca.pem')

    try:
        pool = await aiomysql.create_pool(
            host=db_config['host'],
            port=db_config['port'],
            user=db_config['user'],
            password=db_config['password'],
            db=db_config['db'],
            autocommit=True,
            ssl=ssl_context
        )

        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                print("--- Tables in database ---")
                await cursor.execute("SHOW TABLES;")
                tables = await cursor.fetchall()
                for table in tables:
                    print(table[0])

                print("\n--- Data from ohlctick_1mdata (first 10 rows) ---")
                await cursor.execute("SELECT * FROM ohlctick_1mdata LIMIT 10;")
                rows = await cursor.fetchall()
                for row in rows:
                    print(row)

        pool.close()
        await pool.wait_closed()

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    asyncio.run(main())

```

## 📁 public/

### 📄 public/404.html

```html
<!DOCTYPE html>
<html>
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Page Not Found</title>

    <style media="screen">
      body { background: #ECEFF1; color: rgba(0,0,0,0.87); font-family: Roboto, Helvetica, Arial, sans-serif; margin: 0; padding: 0; }
      #message { background: white; max-width: 360px; margin: 100px auto 16px; padding: 32px 24px 16px; border-radius: 3px; }
      #message h3 { color: #888; font-weight: normal; font-size: 16px; margin: 16px 0 12px; }
      #message h2 { color: #ffa100; font-weight: bold; font-size: 16px; margin: 0 0 8px; }
      #message h1 { font-size: 22px; font-weight: 300; color: rgba(0,0,0,0.6); margin: 0 0 16px;}
      #message p { line-height: 140%; margin: 16px 0 24px; font-size: 14px; }
      #message a { display: block; text-align: center; background: #039be5; text-transform: uppercase; text-decoration: none; color: white; padding: 16px; border-radius: 4px; }
      #message, #message a { box-shadow: 0 1px 3px rgba(0,0,0,0.12), 0 1px 2px rgba(0,0,0,0.24); }
      #load { color: rgba(0,0,0,0.4); text-align: center; font-size: 13px; }
      @media (max-width: 600px) {
        body, #message { margin-top: 0; background: white; box-shadow: none; }
        body { border-top: 16px solid #ffa100; }
      }
    </style>
  </head>
  <body>
    <div id="message">
      <h2>404</h2>
      <h1>Page Not Found</h1>
      <p>The specified file was not found on this website. Please check the URL for mistakes and try again.</p>
      <h3>Why am I seeing this?</h3>
      <p>This page was generated by the Firebase Command-Line Interface. To modify it, edit the <code>404.html</code> file in your project's configured <code>public</code> directory.</p>
    </div>
  </body>
</html>

```

### 📄 public/index.html

```html
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>AlgoStratgy — Ops Console</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500;600&family=IBM+Plex+Sans:wght@300;400;500&display=swap" rel="stylesheet">
<style>
  :root {
    --bg:        #0a0c0f;
    --bg2:       #111419;
    --bg3:       #181c23;
    --border:    #1f2530;
    --border2:   #2a3140;
    --green:     #00d084;
    --green-dim: #00d08422;
    --red:       #ff4757;
    --red-dim:   #ff475722;
    --amber:     #ffb300;
    --amber-dim: #ffb30022;
    --blue:      #3d8ef8;
    --blue-dim:  #3d8ef818;
    --purple:    #b06ef3;
    --text:      #e2e8f0;
    --text2:     #8896a8;
    --text3:     #4a5568;
    --mono:      'IBM Plex Mono', monospace;
    --sans:      'IBM Plex Sans', sans-serif;
  }

  * { box-sizing: border-box; margin: 0; padding: 0; }

  body {
    background: var(--bg);
    color: var(--text);
    font-family: var(--sans);
    min-height: 100vh;
    overflow-x: hidden;
  }

  /* ── scanline overlay ── */
  body::before {
    content: '';
    position: fixed; inset: 0; z-index: 0; pointer-events: none;
    background: repeating-linear-gradient(
      0deg, transparent, transparent 2px,
      rgba(0,0,0,0.03) 2px, rgba(0,0,0,0.03) 4px
    );
  }

  /* ── top bar ── */
  .topbar {
    position: sticky; top: 0; z-index: 100;
    background: rgba(10,12,15,0.92);
    backdrop-filter: blur(12px);
    border-bottom: 1px solid var(--border);
    display: flex; align-items: center; justify-content: space-between;
    padding: 0 2rem; height: 52px;
  }
  .topbar-left { display: flex; align-items: center; gap: 1rem; }
  .logo {
    font-family: var(--mono); font-size: 0.85rem; font-weight: 600;
    color: var(--green); letter-spacing: 0.08em;
    display: flex; align-items: center; gap: 0.5rem;
  }
  .logo-dot {
    width: 8px; height: 8px; border-radius: 50%;
    background: var(--green);
    box-shadow: 0 0 8px var(--green);
    animation: pulse 2s ease-in-out infinite;
  }
  @keyframes pulse {
    0%,100% { opacity: 1; transform: scale(1); }
    50%      { opacity: 0.5; transform: scale(0.8); }
  }
  .tag {
    font-family: var(--mono); font-size: 0.65rem;
    background: var(--bg3); border: 1px solid var(--border2);
    color: var(--text3); padding: 2px 8px; border-radius: 3px;
    letter-spacing: 0.05em;
  }
  .clock {
    font-family: var(--mono); font-size: 0.75rem;
    color: var(--text2); letter-spacing: 0.05em;
  }
  .market-badge {
    font-family: var(--mono); font-size: 0.65rem; font-weight: 600;
    padding: 3px 10px; border-radius: 3px; letter-spacing: 0.1em;
    transition: all 0.3s;
  }
  .market-badge.open  { background: var(--green-dim);  color: var(--green); border: 1px solid var(--green); }
  .market-badge.closed{ background: var(--red-dim);    color: var(--red);   border: 1px solid var(--red);   }

  /* ── layout ── */
  .main { position: relative; z-index: 1; max-width: 1100px; margin: 0 auto; padding: 2rem; }

  /* ── section header ── */
  .section-label {
    font-family: var(--mono); font-size: 0.65rem; font-weight: 600;
    color: var(--text3); letter-spacing: 0.15em; text-transform: uppercase;
    display: flex; align-items: center; gap: 0.75rem; margin-bottom: 1rem;
  }
  .section-label::after {
    content: ''; flex: 1; height: 1px; background: var(--border);
  }

  /* ── action cards ── */
  .action-grid {
    display: grid; grid-template-columns: 1fr 1fr; gap: 1rem;
    margin-bottom: 2rem;
  }
  @media (max-width: 640px) { .action-grid { grid-template-columns: 1fr; } }

  .action-card {
    background: var(--bg2); border: 1px solid var(--border);
    border-radius: 6px; padding: 1.5rem;
    display: flex; flex-direction: column; gap: 0.75rem;
    transition: border-color 0.2s, transform 0.15s;
    cursor: default;
  }
  .action-card:hover { border-color: var(--border2); transform: translateY(-1px); }

  .card-icon {
    width: 36px; height: 36px; border-radius: 6px;
    display: flex; align-items: center; justify-content: center;
    font-size: 1.1rem;
  }
  .card-icon.green  { background: var(--green-dim);  }
  .card-icon.blue   { background: var(--blue-dim);   }
  .card-icon.amber  { background: var(--amber-dim);  }
  .card-icon.red    { background: var(--red-dim);    }

  .card-title {
    font-family: var(--mono); font-size: 0.8rem; font-weight: 600;
    color: var(--text); letter-spacing: 0.03em;
  }
  .card-desc {
    font-size: 0.78rem; color: var(--text2); line-height: 1.5;
  }

  .btn {
    display: inline-flex; align-items: center; gap: 0.5rem;
    font-family: var(--mono); font-size: 0.75rem; font-weight: 600;
    padding: 0.5rem 1rem; border-radius: 4px; border: 1px solid;
    text-decoration: none; cursor: pointer; transition: all 0.15s;
    letter-spacing: 0.05em; width: fit-content;
  }
  .btn-green {
    background: var(--green-dim); color: var(--green);
    border-color: var(--green);
  }
  .btn-green:hover { background: var(--green); color: #000; }

  .btn-blue {
    background: var(--blue-dim); color: var(--blue);
    border-color: var(--blue);
  }
  .btn-blue:hover { background: var(--blue); color: #fff; }

  .btn-amber {
    background: var(--amber-dim); color: var(--amber);
    border-color: var(--amber);
  }
  .btn-amber:hover { background: var(--amber); color: #000; }

  .btn-ghost {
    background: transparent; color: var(--text2);
    border-color: var(--border2);
  }
  .btn-ghost:hover { border-color: var(--text2); color: var(--text); }

  /* ── status grid ── */
  .status-grid {
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    gap: 1px; background: var(--border);
    border: 1px solid var(--border); border-radius: 6px;
    overflow: hidden; margin-bottom: 2rem;
  }
  @media (max-width: 700px) { .status-grid { grid-template-columns: repeat(2,1fr); } }

  .stat-cell {
    background: var(--bg2); padding: 1rem 1.2rem;
    display: flex; flex-direction: column; gap: 0.35rem;
  }
  .stat-label {
    font-family: var(--mono); font-size: 0.6rem; font-weight: 500;
    color: var(--text3); letter-spacing: 0.12em; text-transform: uppercase;
  }
  .stat-value {
    font-family: var(--mono); font-size: 1.1rem; font-weight: 600;
    color: var(--text);
  }
  .stat-value.green  { color: var(--green); }
  .stat-value.red    { color: var(--red); }
  .stat-value.amber  { color: var(--amber); }
  .stat-sub {
    font-family: var(--mono); font-size: 0.65rem; color: var(--text3);
  }

  /* ── endpoints table ── */
  .endpoint-list {
    background: var(--bg2); border: 1px solid var(--border);
    border-radius: 6px; overflow: hidden; margin-bottom: 2rem;
  }
  .endpoint-row {
    display: grid; grid-template-columns: 120px 1fr auto;
    align-items: center; gap: 1rem; padding: 0.7rem 1.2rem;
    border-bottom: 1px solid var(--border);
    transition: background 0.15s;
  }
  .endpoint-row:last-child { border-bottom: none; }
  .endpoint-row:hover { background: var(--bg3); }

  .method-badge {
    font-family: var(--mono); font-size: 0.62rem; font-weight: 600;
    padding: 2px 7px; border-radius: 3px; letter-spacing: 0.08em;
    width: fit-content;
  }
  .method-badge.get  { background: var(--green-dim); color: var(--green); border: 1px solid var(--green); }

  .endpoint-path {
    font-family: var(--mono); font-size: 0.75rem; color: var(--text);
  }
  .endpoint-desc {
    font-size: 0.72rem; color: var(--text2); margin-top: 2px;
  }
  .endpoint-link {
    font-family: var(--mono); font-size: 0.65rem; color: var(--text3);
    text-decoration: none; padding: 3px 8px; border: 1px solid var(--border2);
    border-radius: 3px; white-space: nowrap; transition: all 0.15s;
  }
  .endpoint-link:hover { color: var(--blue); border-color: var(--blue); }

  /* ── jobs table ── */
  .jobs-table {
    background: var(--bg2); border: 1px solid var(--border);
    border-radius: 6px; overflow: hidden; margin-bottom: 2rem;
  }
  .jobs-header {
    display: grid; grid-template-columns: 2fr 1fr 1fr 1fr;
    padding: 0.5rem 1.2rem;
    background: var(--bg3); border-bottom: 1px solid var(--border);
  }
  .jobs-header span {
    font-family: var(--mono); font-size: 0.6rem; font-weight: 600;
    color: var(--text3); letter-spacing: 0.1em; text-transform: uppercase;
  }
  .job-row {
    display: grid; grid-template-columns: 2fr 1fr 1fr 1fr;
    align-items: center; padding: 0.7rem 1.2rem;
    border-bottom: 1px solid var(--border); transition: background 0.15s;
  }
  .job-row:last-child { border-bottom: none; }
  .job-row:hover { background: var(--bg3); }
  .job-name {
    font-family: var(--mono); font-size: 0.73rem; color: var(--text);
  }
  .job-name small {
    display: block; font-size: 0.62rem; color: var(--text3); margin-top: 2px;
  }
  .job-cell {
    font-family: var(--mono); font-size: 0.7rem; color: var(--text2);
  }
  .dot-green { color: var(--green); }
  .dot-amber { color: var(--amber); }

  /* ── token status panel ── */
  .token-panel {
    background: var(--bg2); border: 1px solid var(--border);
    border-radius: 6px; padding: 1.5rem; margin-bottom: 2rem;
    display: flex; flex-direction: column; gap: 1rem;
  }
  .token-row {
    display: flex; align-items: center; justify-content: space-between;
    gap: 1rem; flex-wrap: wrap;
  }
  .token-info { display: flex; flex-direction: column; gap: 0.25rem; }
  .token-info p { font-size: 0.78rem; color: var(--text2); line-height: 1.6; }
  .token-info code {
    font-family: var(--mono); font-size: 0.7rem;
    background: var(--bg3); border: 1px solid var(--border2);
    padding: 2px 7px; border-radius: 3px; color: var(--amber);
  }
  .token-steps {
    background: var(--bg3); border: 1px solid var(--border);
    border-radius: 4px; padding: 1rem;
  }
  .step {
    display: flex; gap: 0.75rem; align-items: flex-start;
    font-size: 0.75rem; color: var(--text2); line-height: 1.5;
    margin-bottom: 0.5rem;
  }
  .step:last-child { margin-bottom: 0; }
  .step-num {
    font-family: var(--mono); font-size: 0.65rem; font-weight: 600;
    background: var(--border2); color: var(--text3);
    width: 20px; height: 20px; border-radius: 50%;
    display: flex; align-items: center; justify-content: center;
    flex-shrink: 0; margin-top: 1px;
  }

  /* ── footer ── */
  .footer {
    border-top: 1px solid var(--border); padding: 1.5rem 2rem;
    display: flex; align-items: center; justify-content: space-between;
    flex-wrap: wrap; gap: 1rem;
  }
  .footer-left { font-family: var(--mono); font-size: 0.65rem; color: var(--text3); }
  .footer-right { display: flex; gap: 0.75rem; }

  /* ── toast ── */
  .toast {
    position: fixed; bottom: 1.5rem; right: 1.5rem; z-index: 999;
    background: var(--bg3); border: 1px solid var(--green);
    color: var(--green); font-family: var(--mono); font-size: 0.73rem;
    padding: 0.6rem 1.2rem; border-radius: 4px;
    box-shadow: 0 4px 20px rgba(0,208,132,0.15);
    transform: translateY(20px); opacity: 0;
    transition: all 0.25s; pointer-events: none;
  }
  .toast.show { transform: translateY(0); opacity: 1; }

  /* ── live fetch indicator ── */
  .fetching { animation: blink 1s step-end infinite; }
  @keyframes blink { 50% { opacity: 0; } }
</style>
</head>
<body>

<!-- TOP BAR -->
<div class="topbar">
  <div class="topbar-left">
    <div class="logo">
      <div class="logo-dot"></div>
      ALGOSTRATGY
    </div>
    <span class="tag">CLOUD RUN · ASIA-SOUTH1</span>
  </div>
  <div style="display:flex; align-items:center; gap:1rem;">
    <span class="clock" id="clock">--:--:-- IST</span>
    <span class="market-badge closed" id="market-badge">CLOSED</span>
  </div>
</div>

<!-- MAIN -->
<div class="main">

  <!-- STATUS STRIP -->
  <div class="status-grid" style="margin-bottom:2rem; margin-top:0.5rem;">
    <div class="stat-cell">
      <div class="stat-label">Project</div>
      <div class="stat-value" style="font-size:0.85rem;">algostratgy</div>
      <div class="stat-sub">GCP · Mumbai</div>
    </div>
    <div class="stat-cell">
      <div class="stat-label">Market Hours</div>
      <div class="stat-value" id="stat-market">--</div>
      <div class="stat-sub">09:15 – 15:30 IST</div>
    </div>
    <div class="stat-cell">
      <div class="stat-label">Breeze Token</div>
      <div class="stat-value amber" id="stat-token">checking…</div>
      <div class="stat-sub" id="stat-token-sub">–</div>
    </div>
    <div class="stat-cell">
      <div class="stat-label">DB Rows</div>
      <div class="stat-value" id="stat-rows" style="font-size:0.85rem;">checking…</div>
      <div class="stat-sub" id="stat-rows-sub">ohlc / indicators</div>
    </div>
  </div>

  <!-- PRIMARY ACTIONS -->
  <div class="section-label">Primary Actions</div>
  <div class="action-grid">

    <!-- Session Token -->
    <div class="action-card">
      <div class="card-icon green">🔑</div>
      <div>
        <div class="card-title">Update Session Token</div>
        <div class="card-desc">Log in to ICICI Breeze portal. After login you will be redirected to the callback URL and the session token will be saved to Secret Manager automatically.</div>
      </div>
      <div class="token-steps">
        <div class="step">
          <div class="step-num">1</div>
          <span>Click <strong style="color:var(--text);">Open Breeze Login</strong> — ICICI login page opens</span>
        </div>
        <div class="step">
          <div class="step-num">2</div>
          <span>Complete login. Browser redirects to <code style="font-family:var(--mono);font-size:0.68rem;color:var(--green);">/breeze/callback</code></span>
        </div>
        <div class="step">
          <div class="step-num">3</div>
          <span>Token saved to <code style="font-family:var(--mono);font-size:0.68rem;color:var(--amber);">breeze-session-token</code> in Secret Manager</span>
        </div>
      </div>
      <div style="display:flex; gap:0.75rem; flex-wrap:wrap;">
        <a href="https://api.icicidirect.com/apiuser/login?api_key=O625jr603bS399999661n%2b270wuS313C"
           target="_blank" class="btn btn-green">
          ↗ Open Breeze Login
        </a>
        <a href="/breeze/status" target="_blank" class="btn btn-ghost">
          Check Token Status
        </a>
      </div>
    </div>

    <!-- Dashboard -->
    <div class="action-card">
      <div class="card-icon blue">📊</div>
      <div>
        <div class="card-title">Live Dashboard</div>
        <div class="card-desc">Open the Cloud Run web service dashboard — live OHLC data, indicator signals, option buying status, and system health.</div>
      </div>
      <div style="display:flex; gap:0.75rem; flex-wrap:wrap;">
        <a href="https://algostratgy-web-1018458560096.asia-south1.run.app/"
           target="_blank" class="btn btn-blue">
          ↗ Open Dashboard
        </a>
        <a href="https://algostratgy-web-1018458560096.asia-south1.run.app/signals"
           target="_blank" class="btn btn-ghost">
          Signals JSON
        </a>
        <a href="https://algostratgy-web-1018458560096.asia-south1.run.app/db"
           target="_blank" class="btn btn-ghost">
          DB Status
        </a>
      </div>
    </div>

  </div>

  <!-- TOKEN STATUS -->
  <div class="section-label">Session Token</div>
  <div class="token-panel">
    <div class="token-row">
      <div class="token-info">
        <div class="card-title">breeze-session-token</div>
        <p>Stored in GCP Secret Manager. Auto-expires after 24h (<code>version-destroy-ttl=86400</code>). Must be refreshed each trading day before 09:14 IST when schedulers fire.</p>
      </div>
      <div style="display:flex; gap:0.5rem;">
        <a href="/breeze/status" target="_blank" class="btn btn-ghost" style="white-space:nowrap;">
          /breeze/status ↗
        </a>
        <button class="btn btn-amber" onclick="refreshTokenStatus()" id="refresh-btn">
          ↻ Refresh
        </button>
      </div>
    </div>
    <div id="token-detail" style="font-family:var(--mono);font-size:0.72rem;color:var(--text2);
         background:var(--bg3);border:1px solid var(--border);border-radius:4px;padding:0.75rem;">
      Fetching token status…
    </div>
  </div>

  <!-- API ENDPOINTS -->
  <div class="section-label">API Endpoints</div>
  <div class="endpoint-list">
    <div class="endpoint-row">
      <div><span class="method-badge get">GET</span></div>
      <div>
        <div class="endpoint-path">/</div>
        <div class="endpoint-desc">Live dashboard — OHLC table, signals, system status</div>
      </div>
      <a href="https://algostratgy-web-1018458560096.asia-south1.run.app/" target="_blank" class="endpoint-link">Open ↗</a>
    </div>
    <div class="endpoint-row">
      <div><span class="method-badge get">GET</span></div>
      <div>
        <div class="endpoint-path">/signals</div>
        <div class="endpoint-desc">Latest option buying signals from placeorder_track</div>
      </div>
      <a href="https://algostratgy-web-1018458560096.asia-south1.run.app/signals" target="_blank" class="endpoint-link">Open ↗</a>
    </div>
    <div class="endpoint-row">
      <div><span class="method-badge get">GET</span></div>
      <div>
        <div class="endpoint-path">/data</div>
        <div class="endpoint-desc">Latest 10 OHLC rows from ohlctick_1mdata</div>
      </div>
      <a href="https://algostratgy-web-1018458560096.asia-south1.run.app/data" target="_blank" class="endpoint-link">Open ↗</a>
    </div>
    <div class="endpoint-row">
      <div><span class="method-badge get">GET</span></div>
      <div>
        <div class="endpoint-path">/db</div>
        <div class="endpoint-desc">DB connection + row counts for all tables</div>
      </div>
      <a href="https://algostratgy-web-1018458560096.asia-south1.run.app/db" target="_blank" class="endpoint-link">Open ↗</a>
    </div>
    <div class="endpoint-row">
      <div><span class="method-badge get">GET</span></div>
      <div>
        <div class="endpoint-path">/status</div>
        <div class="endpoint-desc">Market open/closed status, IST time, weekday</div>
      </div>
      <a href="https://algostratgy-web-1018458560096.asia-south1.run.app/status" target="_blank" class="endpoint-link">Open ↗</a>
    </div>
    <div class="endpoint-row">
      <div><span class="method-badge get">GET</span></div>
      <div>
        <div class="endpoint-path">/breeze/status</div>
        <div class="endpoint-desc">Timestamp of last saved Breeze session token</div>
      </div>
      <a href="/breeze/status" target="_blank" class="endpoint-link">Open ↗</a>
    </div>
    <div class="endpoint-row">
      <div><span class="method-badge get">GET</span></div>
      <div>
        <div class="endpoint-path">/health</div>
        <div class="endpoint-desc">Service health check — returns <code style="font-family:var(--mono);font-size:0.68rem;">{"status":"ok"}</code></div>
      </div>
      <a href="https://algostratgy-web-1018458560096.asia-south1.run.app/health" target="_blank" class="endpoint-link">Open ↗</a>
    </div>
  </div>

  <!-- CLOUD RUN JOBS -->
  <div class="section-label">Cloud Run Jobs</div>
  <div class="jobs-table">
    <div class="jobs-header">
      <span>Job Name</span>
      <span>Schedule (UTC)</span>
      <span>Script</span>
      <span>Status</span>
    </div>
    <div class="job-row">
      <div class="job-name">
        algostratgy-pipeline
        <small>OHLC fetch + live update</small>
      </div>
      <div class="job-cell">44 3 * * 1–5</div>
      <div class="job-cell">run_pipeline.sh</div>
      <div class="job-cell dot-green">● ENABLED</div>
    </div>
    <div class="job-row">
      <div class="job-name">
        algostratgy-indicator-update
        <small>Indicator calc + live loop</small>
      </div>
      <div class="job-cell">44 3 * * 1–5</div>
      <div class="job-cell">run_indicators.sh</div>
      <div class="job-cell dot-green">● ENABLED</div>
    </div>
    <div class="job-row">
      <div class="job-name">
        algostratgy-optionbuying-scheduled
        <small>Signal eval + order placement</small>
      </div>
      <div class="job-cell">44 3 * * 1–5</div>
      <div class="job-cell">optionbuying.py scheduled</div>
      <div class="job-cell dot-green">● ENABLED</div>
    </div>
    <div class="job-row">
      <div class="job-name">
        algostratgy-order-stream
        <small>WebSocket order tracking</small>
      </div>
      <div class="job-cell">44 3 * * 1–5</div>
      <div class="job-cell">order_stream.py</div>
      <div class="job-cell dot-green">● ENABLED</div>
    </div>
    <div class="job-row">
      <div class="job-name">
        algostratgy-indicators
        <small>Full history recalculation (on-demand)</small>
      </div>
      <div class="job-cell">–</div>
      <div class="job-cell">indicatordata_all.py</div>
      <div class="job-cell dot-amber">◐ ON-DEMAND</div>
    </div>
    <div class="job-row">
      <div class="job-name">
        algostratgy-optionbuying
        <small>Single signal eval (on-demand)</small>
      </div>
      <div class="job-cell">–</div>
      <div class="job-cell">optionbuying.py once</div>
      <div class="job-cell dot-amber">◐ ON-DEMAND</div>
    </div>
  </div>

  <!-- DAILY CHECKLIST -->
  <div class="section-label">Daily Pre-Market Checklist</div>
  <div style="background:var(--bg2);border:1px solid var(--border);border-radius:6px;
              padding:1.25rem 1.5rem; margin-bottom:2rem;">
    <div style="display:grid;gap:0.6rem;">
      <label class="step" style="cursor:pointer;">
        <input type="checkbox" style="accent-color:var(--green);margin-top:3px;flex-shrink:0;">
        <span><strong style="color:var(--text);">08:45 IST</strong> — Open Breeze login, complete 2FA, confirm callback redirect</span>
      </label>
      <label class="step" style="cursor:pointer;">
        <input type="checkbox" style="accent-color:var(--green);margin-top:3px;flex-shrink:0;">
        <span>Verify <code style="font-family:var(--mono);font-size:0.68rem;color:var(--amber);">/breeze/status</code> shows today's token timestamp</span>
      </label>
      <label class="step" style="cursor:pointer;">
        <input type="checkbox" style="accent-color:var(--green);margin-top:3px;flex-shrink:0;">
        <span><strong style="color:var(--text);">09:14 IST</strong> — All 4 schedulers auto-fire. Confirm jobs start in GCP console</span>
      </label>
      <label class="step" style="cursor:pointer;">
        <input type="checkbox" style="accent-color:var(--green);margin-top:3px;flex-shrink:0;">
        <span>Check <code style="font-family:var(--mono);font-size:0.68rem;color:var(--green);">/db</code> for non-zero row counts after 09:16</span>
      </label>
      <label class="step" style="cursor:pointer;">
        <input type="checkbox" style="accent-color:var(--green);margin-top:3px;flex-shrink:0;">
        <span>Monitor <code style="font-family:var(--mono);font-size:0.68rem;color:var(--blue);">/signals</code> from 09:20 for first option signal</span>
      </label>
    </div>
  </div>

</div>

<!-- FOOTER -->
<div class="footer">
  <div class="footer-left">
    ALGOSTRATGY · asia-south1 · algostratgy-sa@algostratgy.iam.gserviceaccount.com
  </div>
  <div class="footer-right">
    <a href="https://console.cloud.google.com/run?project=algostratgy" target="_blank" class="btn btn-ghost" style="font-size:0.65rem; padding:3px 10px;">GCP Console ↗</a>
    <a href="https://console.cloud.google.com/logs?project=algostratgy" target="_blank" class="btn btn-ghost" style="font-size:0.65rem; padding:3px 10px;">Cloud Logs ↗</a>
  </div>
</div>

<div class="toast" id="toast"></div>

<script>
  const BASE = 'https://algostratgy-web-1018458560096.asia-south1.run.app';

  // ── Clock + market status ──────────────────────────────────────────────────
  function updateClock() {
    const now = new Date();
    const ist = new Date(now.toLocaleString('en-US', { timeZone: 'Asia/Kolkata' }));
    const h = String(ist.getHours()).padStart(2,'0');
    const m = String(ist.getMinutes()).padStart(2,'0');
    const s = String(ist.getSeconds()).padStart(2,'0');
    document.getElementById('clock').textContent = `${h}:${m}:${s} IST`;

    const totalMin = ist.getHours() * 60 + ist.getMinutes();
    const openMin  = 9 * 60 + 15;
    const closeMin = 15 * 60 + 30;
    const day = ist.getDay(); // 0=Sun, 6=Sat
    const isWeekday = day >= 1 && day <= 5;
    const isOpen = isWeekday && totalMin >= openMin && totalMin < closeMin;

    const badge = document.getElementById('market-badge');
    const stat  = document.getElementById('stat-market');
    if (isOpen) {
      badge.textContent = 'OPEN';
      badge.className = 'market-badge open';
      stat.textContent = 'OPEN';
      stat.className = 'stat-value green';
    } else {
      badge.textContent = 'CLOSED';
      badge.className = 'market-badge closed';
      // time to next open
      let next = '';
      if (!isWeekday || totalMin >= closeMin) {
        next = 'Opens next trading day';
      } else {
        const mins = openMin - totalMin;
        next = `Opens in ${Math.floor(mins/60)}h ${mins%60}m`;
      }
      stat.textContent = 'CLOSED';
      stat.className = 'stat-value red';
    }
  }
  setInterval(updateClock, 1000);
  updateClock();

  // ── Toast ──────────────────────────────────────────────────────────────────
  function showToast(msg) {
    const t = document.getElementById('toast');
    t.textContent = msg;
    t.classList.add('show');
    setTimeout(() => t.classList.remove('show'), 2500);
  }

  // ── Fetch token status ─────────────────────────────────────────────────────
  async function refreshTokenStatus() {
    const el  = document.getElementById('token-detail');
    const btn = document.getElementById('refresh-btn');
    const statEl    = document.getElementById('stat-token');
    const statSubEl = document.getElementById('stat-token-sub');
    el.textContent = 'Fetching…';
    btn.textContent = '↻ Fetching…';
    try {
      const r = await fetch('/breeze/status');
      const j = await r.json();
      el.textContent = JSON.stringify(j, null, 2);
      if (j.last_saved) {
        statEl.textContent = 'SAVED';
        statEl.className = 'stat-value green';
        // show time ago
        const saved = new Date(j.last_saved);
        const diffMin = Math.round((Date.now() - saved) / 60000);
        statSubEl.textContent = diffMin < 60
          ? `${diffMin}m ago`
          : `${Math.floor(diffMin/60)}h ${diffMin%60}m ago`;
        showToast('✓ Token status refreshed');
      } else {
        statEl.textContent = 'NOT SET';
        statEl.className = 'stat-value red';
        statSubEl.textContent = 'Login required';
      }
    } catch(e) {
      el.textContent = 'Error fetching token status — service may be sleeping';
      statEl.textContent = 'ERROR';
      statEl.className = 'stat-value red';
    }
    btn.textContent = '↻ Refresh';
  }

  // ── Fetch DB row counts ────────────────────────────────────────────────────
  async function fetchDBStatus() {
    const rowEl    = document.getElementById('stat-rows');
    const rowSubEl = document.getElementById('stat-rows-sub');
    try {
      const r = await fetch(`${BASE}/db`);
      const j = await r.json();
      // Expected keys: ohlctick_1mdata, indicators_data (or similar)
      const keys = Object.keys(j).filter(k => typeof j[k] === 'number');
      if (keys.length >= 2) {
        rowEl.textContent = keys.map(k => j[k].toLocaleString()).join(' / ');
        rowEl.className = 'stat-value green';
        rowSubEl.textContent = keys.join(' / ');
      } else if (keys.length === 1) {
        rowEl.textContent = j[keys[0]].toLocaleString();
        rowEl.className = 'stat-value';
        rowSubEl.textContent = keys[0];
      } else {
        rowEl.textContent = JSON.stringify(j).slice(0,30);
        rowEl.className = 'stat-value';
      }
    } catch(e) {
      rowEl.textContent = 'unavailable';
      rowEl.className = 'stat-value red';
    }
  }

  // ── Init ───────────────────────────────────────────────────────────────────
  refreshTokenStatus();
  fetchDBStatus();
</script>
</body>
</html>
```

## 📁 scripts/

### 📄 scripts/breeze_import.py

```py
import os
from dotenv import load_dotenv
from breeze_connect import BreezeConnect
from datetime import datetime,date,timedelta
import pytz

load_dotenv()

# My credentials
api_key = os.getenv("API_KEY")
api_secret = os.getenv("API_SECRET")
session_token = os.getenv("SESSION_TOKEN")

api = BreezeConnect(api_key=api_key)
api.generate_session(api_secret=api_secret, session_token=str(session_token))

db_config = {
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT", 3306)),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
    "autocommit": True,
    "minsize": 5,
    "maxsize": 50
}

holidays=[
        "2024-12-25",
        "2025-02-26",
        "2025-03-14",
		"2025-03-31",
		"2025-04-10",
		"2025-04-14",
        "2025-04-18",
        "2025-05-01",
        "2025-08-15",
        "2025-08-27",
        "2025-10-02",
        "2025-10-21",
        "2025-10-22",
        "2025-11-05",
		"2025-12-25"
    ]
# expiry_date="2024-11-13T06:00:00.000Z"
stock_code ="CNXBAN"
options_basket = [{"stock_code": "CNXBAN", "strike_price": "48800", "right": "put"},
    {"stock_code": "CNXBAN", "strike_price": "51700", "right": "call"},]

######################################################

def get_last_wednesday(year, month, timezone):
    """
    Helper function to calculate the last Wednesday of a given month.
    """
    if month == 12:
        last_day = timezone.localize(datetime(year, month, 31))
    else:
        first_day_next_month = timezone.localize(datetime(year, month + 1, 1))
        last_day = first_day_next_month - timedelta(days=1)

    # Calculate the last Wednesday of the month
    offset = (last_day.weekday() - 3) % 7  # 2 represents thursday
    return last_day - timedelta(days=offset)

def get_monthly_expiry():
    """
    Function to calculate the monthly expiry date based on the last Wednesday of the month.
    """
    timezone = pytz.timezone('Asia/Kolkata')
    today_date = datetime.now(timezone)
    year = today_date.year
    month = today_date.month

    # Get last Wednesday of the current month
    current_month_last_wednesday = get_last_wednesday(year, month, timezone)
    while current_month_last_wednesday.strftime('%Y-%m-%d') in holidays:
            current_month_last_wednesday -= timedelta(days=1)

    # Check if today is later than the current month's last Wednesday
    if today_date.date() > current_month_last_wednesday.date():
        # Move to the next month
        if month == 12:  # Handle year transition
            year += 1
            month = 1
        else:
            month += 1

        # Get last Wednesday of the next month
        next_month_last_wednesday = get_last_wednesday(year, month, timezone)

        # Adjust if the calculated date falls on a holiday
        while next_month_last_wednesday.strftime('%Y-%m-%d') in holidays:
            next_month_last_wednesday -= timedelta(days=1)

        return next_month_last_wednesday.strftime('%Y-%m-%d')

    # Default: return the last Wednesday of the current month
    return current_month_last_wednesday.strftime('%Y-%m-%d')

# Get and print the expiry date
expiry_date = get_monthly_expiry()

###########################

# source .venv/bin/activate
# python scripts/indicator_update.py

```

### 📄 scripts/indicator_update.py

```py
import asyncio
import aiomysql
import pytz
import pandas as pd
import talib
import numpy as np
import os
import ssl
from datetime import datetime

IST = pytz.timezone('Asia/Kolkata')

holidays = [
    # 2025
    "2025-02-26", "2025-03-14", "2025-03-31", "2025-04-10",
    "2025-04-14", "2025-04-18", "2025-05-01", "2025-08-15",
    "2025-08-27", "2025-10-02", "2025-10-21", "2025-10-22",
    "2025-11-05", "2025-11-15", "2025-12-25",
    # 2026
    "2026-01-26", "2026-03-03", "2026-03-26", "2026-03-31",
    "2026-04-03", "2026-04-14", "2026-05-01", "2026-05-28",
    "2026-06-26", "2026-09-14", "2026-10-02", "2026-10-20",
    "2026-11-10", "2026-11-24", "2026-12-25",
]

db_config = {
    "host":     os.getenv("DB_HOST"),
    "port":     int(os.getenv("DB_PORT", 3306)),
    "user":     os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
}


class IndicatorUpdate:

    def __init__(self):
        pass

    # ── DB pool ───────────────────────────────────────────────────────────────

    async def get_mysql_pool(self):
        ssl_ctx = None
        ca_path = os.path.join(os.path.dirname(__file__), '..', 'ca.pem')
        if os.path.exists(ca_path):
            ssl_ctx = ssl.create_default_context(cafile=ca_path)

        pool = await aiomysql.create_pool(
            host=db_config['host'],
            port=db_config['port'],
            user=db_config['user'],
            password=db_config['password'],
            db=db_config['database'],
            autocommit=True,
            ssl=ssl_ctx,
            minsize=2,
            maxsize=10,
        )
        return pool

    # ── Market calendar ───────────────────────────────────────────────────────

    def is_market_open(self):
        now = datetime.now(IST)
        return (now.replace(hour=9,  minute=15, second=0, microsecond=0) <= now < now.replace(hour=15, minute=30, second=0, microsecond=0))

    def is_business_day(self, date):
        return date.weekday() < 5 and date.strftime('%Y-%m-%d') not in holidays

    # ── DDL ───────────────────────────────────────────────────────────────────

    async def create_tables_if_not_exists(self, pool):
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute('''
                    CREATE TABLE IF NOT EXISTS indicators_data (
                        datetime      DATETIME PRIMARY KEY,
                        open          DOUBLE, high  DOUBLE, low   DOUBLE, close DOUBLE,
                        ohlc4         DOUBLE,
                        linearreg     DOUBLE,
                        lri_slope     DOUBLE, lri_intercept DOUBLE,
                        lri_curve     DOUBLE, lri_angle     DOUBLE,
                        ema26         DOUBLE,
                        hma26_5m      DOUBLE,
                        BuyCall       INTEGER, BuyPut  INTEGER,
                        Bull          INTEGER, Bear    INTEGER,
                        ATR           DOUBLE,
                        VStop2        DOUBLE,  VStop3        DOUBLE,
                        TrendUp2      INTEGER, TrendUp3      INTEGER,
                        Max           DOUBLE,  Min           DOUBLE,
                        cup_vstop2    DOUBLE,  cup_vstop3    DOUBLE,
                        cdn_vstop2    DOUBLE,  cdn_vstop3    DOUBLE,
                        dayhigh       DOUBLE,  daylow        DOUBLE,
                        st_avg        DOUBLE,  st_max        DOUBLE,
                        st_min        DOUBLE,  supertrend    DOUBLE,
                        st_dir        INTEGER
                    )
                ''')

    # ── Gap checker ───────────────────────────────────────────────────────────

    async def check_missing_or_duplicate_keys(self, pool):
        now           = pd.Timestamp.now(tz='Asia/Kolkata')
        open_time_dt  = now.replace(hour=9,  minute=15, second=0, microsecond=0)
        close_time_dt = now.replace(hour=15, minute=30, second=0, microsecond=0)

        period_now      = pd.Period.now('1min')
        previous_candle = (period_now - 1).start_time.tz_localize('Asia/Kolkata')
        min_datetime    = min(close_time_dt, previous_candle)

        open_str = open_time_dt.strftime('%Y-%m-%d %H:%M:%S')
        min_str  = min_datetime.strftime('%Y-%m-%d %H:%M:%S')

        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                query = f"""
                    WITH RECURSIVE datetime_sequence AS (
                        SELECT '{open_str}' AS dt
                        UNION ALL
                        SELECT DATE_ADD(dt, INTERVAL 1 MINUTE)
                        FROM datetime_sequence
                        WHERE dt < '{min_str}'
                    )
                    SELECT COUNT(*) AS num_issues
                    FROM (
                        SELECT ds.dt AS missing_or_dup
                        FROM datetime_sequence ds
                        LEFT JOIN (
                            SELECT `datetime`, COUNT(*) AS cnt
                            FROM indicators_data
                            WHERE `datetime` >= '{open_str}' AND `datetime` <= '{min_str}'
                            GROUP BY `datetime`
                        ) t ON ds.dt = t.`datetime`
                        LEFT JOIN indicators_data id ON ds.dt = id.`datetime`
                        WHERE t.`datetime` IS NULL
                           OR t.cnt > 1
                           OR COALESCE(id.open, id.high, id.low, id.close, id.ohlc4) = 0
                    ) AS issues;
                """
                await cursor.execute(query)
                result     = await cursor.fetchone()
                num_issues = result[0] if result else 0
                if num_issues:
                    print(f"Missing/duplicate candles in indicators_data: {num_issues}")
                return num_issues

    # ── Data fetch ────────────────────────────────────────────────────────────

    async def fetch_ohlctick_1mdata(self, pool):
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT * FROM ohlctick_1mdata ORDER BY datetime")
                result  = await cur.fetchall()
                columns = [col[0] for col in cur.description]
                data    = pd.DataFrame(result, columns=columns)
                data.sort_values(by='datetime', inplace=True)
                return data

    async def fetch_ohlctick_5mdata(self, pool):
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT * FROM ohlctick_5mdata ORDER BY datetime")
                result  = await cur.fetchall()
                columns = [col[0] for col in cur.description]
                data    = pd.DataFrame(result, columns=columns)
                data.sort_values(by='datetime', inplace=True)
                return data

    # ── HMA (5-min) → forward-fill onto 1-min index ──────────────────────────
    def _hma(self, series: pd.Series, length: int) -> pd.Series:
        half  = max(1, length // 2)
        sqrtn = max(1, int(np.floor(np.sqrt(length))))
        wma_half = talib.WMA(series, timeperiod=half)
        wma_full = talib.WMA(series, timeperiod=length)
        diff     = 2 * wma_half - wma_full
        return talib.WMA(diff, timeperiod=sqrtn)
    # ── HMA (5-min) → forward-fill onto 1-min index ──────────────────────────

    def merge_hma26_5m(self, data_1m, data_5m):
        df5 = data_5m.copy()
        df5['datetime'] = pd.to_datetime(df5['datetime'])
        df5.set_index('datetime', inplace=True)
        df5['hma26_5m'] = self._hma(df5['hlc3'], 26).round(2)

        df1 = data_1m.copy()
        df1['datetime'] = pd.to_datetime(df1['datetime'])
        df1.set_index('datetime', inplace=True)
        df1['hma26_5m'] = df5['hma26_5m'].reindex(df1.index, method='ffill')
        df1.reset_index(inplace=True)
        return df1

    # ── Indicator calculations (unchanged) ───────────────────────────────────

    async def calculate_vstop(self, data):
        data['ATR']      = talib.ATR(data['high'], data['low'], data['close'], timeperiod=252)
        data['VStop2']   = np.nan
        data['VStop3']   = np.nan
        data['TrendUp2'] = True
        data['TrendUp3'] = True
        data['Max']      = data['close']
        data['Min']      = data['close']

        cup_vstop2 = np.nan; cup_vstop3 = np.nan
        cdn_vstop2 = np.nan; cdn_vstop3 = np.nan
        cup_vstop2_arr = np.full(len(data), np.nan)
        cup_vstop3_arr = np.full(len(data), np.nan)
        cdn_vstop2_arr = np.full(len(data), np.nan)
        cdn_vstop3_arr = np.full(len(data), np.nan)

        for i in range(252, len(data)):
            src    = data['close'].iloc[i]
            atr_m2 = data['ATR'].iloc[i] * 2
            atr_m3 = data['ATR'].iloc[i] * 3

            data.at[i, 'Max'] = max(data['Max'].iloc[i - 1], src)
            data.at[i, 'Min'] = min(data['Min'].iloc[i - 1], src)

            prev2 = data['VStop2'].iloc[i - 1]
            if data['TrendUp2'].iloc[i - 1]:
                data.at[i, 'VStop2'] = max(prev2 if not np.isnan(prev2) else src,
                                           data['Max'].iloc[i] - atr_m2)
            else:
                data.at[i, 'VStop2'] = min(prev2 if not np.isnan(prev2) else src,
                                           data['Min'].iloc[i] + atr_m2)
            data.at[i, 'TrendUp2'] = src >= data['VStop2'].iloc[i]
            if data['TrendUp2'].iloc[i] != data['TrendUp2'].iloc[i - 1]:
                data.at[i, 'Max']    = src
                data.at[i, 'Min']    = src
                data.at[i, 'VStop2'] = (data['Max'].iloc[i] - atr_m2
                                        if data['TrendUp2'].iloc[i]
                                        else data['Min'].iloc[i] + atr_m2)

            prev3 = data['VStop3'].iloc[i - 1]
            if data['TrendUp3'].iloc[i - 1]:
                data.at[i, 'VStop3'] = max(prev3 if not np.isnan(prev3) else src,
                                           data['Max'].iloc[i] - atr_m3)
            else:
                data.at[i, 'VStop3'] = min(prev3 if not np.isnan(prev3) else src,
                                           data['Min'].iloc[i] + atr_m3)
            data.at[i, 'TrendUp3'] = src >= data['VStop3'].iloc[i]
            if data['TrendUp3'].iloc[i] != data['TrendUp3'].iloc[i - 1]:
                data.at[i, 'Max']    = src
                data.at[i, 'Min']    = src
                data.at[i, 'VStop3'] = (data['Max'].iloc[i] - atr_m3
                                        if data['TrendUp3'].iloc[i]
                                        else data['Min'].iloc[i] + atr_m3)

            v2_now  = data['VStop2'].iloc[i];  v3_now  = data['VStop3'].iloc[i]
            v2_prev = data['VStop2'].iloc[i-1]; v3_prev = data['VStop3'].iloc[i-1]
            if not (np.isnan(v2_now) or np.isnan(v3_now) or
                    np.isnan(v2_prev) or np.isnan(v3_prev)):
                if v2_prev <= v3_prev and v2_now > v3_now:
                    cup_vstop2 = v2_now; cup_vstop3 = v3_now
                elif v2_prev >= v3_prev and v2_now < v3_now:
                    cdn_vstop2 = v2_now; cdn_vstop3 = v3_now

            cup_vstop2_arr[i] = cup_vstop2; cup_vstop3_arr[i] = cup_vstop3
            cdn_vstop2_arr[i] = cdn_vstop2; cdn_vstop3_arr[i] = cdn_vstop3

        data['cup_vstop2'] = cup_vstop2_arr; data['cup_vstop3'] = cup_vstop3_arr
        data['cdn_vstop2'] = cdn_vstop2_arr; data['cdn_vstop3'] = cdn_vstop3_arr
        data[['ATR','VStop2','VStop3',
              'cup_vstop2','cup_vstop3',
              'cdn_vstop2','cdn_vstop3']] = \
            data[['ATR','VStop2','VStop3',
                  'cup_vstop2','cup_vstop3',
                  'cdn_vstop2','cdn_vstop3']].round(2)
        return data

    async def calculate_supertrend(self, data, atr_period=63, atr_mult=3.0):
        atr  = talib.ATR(data['high'], data['low'], data['close'],
                         timeperiod=atr_period) * atr_mult
        hl2  = (data['high'] + data['low']) / 2
        n    = len(data)
        upper_arr  = np.full(n, np.nan); lower_arr  = np.full(n, np.nan)
        os_arr     = np.zeros(n, dtype=int)
        spt_arr    = np.full(n, np.nan)
        st_max_arr = np.full(n, np.nan); st_min_arr = np.full(n, np.nan)

        close = data['close'].to_numpy()
        hl2v  = hl2.to_numpy()
        atrv  = atr.to_numpy()

        for i in range(1, n):
            if np.isnan(atrv[i]):
                continue
            up = hl2v[i] + atrv[i]; dn = hl2v[i] - atrv[i]

            prev_upper = upper_arr[i-1]
            upper_arr[i] = (min(up, prev_upper) if not np.isnan(prev_upper)
                            and close[i-1] < prev_upper else up)

            prev_lower = lower_arr[i-1]
            lower_arr[i] = (max(dn, prev_lower) if not np.isnan(prev_lower)
                            and close[i-1] > prev_lower else dn)

            if   close[i] > upper_arr[i]: os_arr[i] = 1
            elif close[i] < lower_arr[i]: os_arr[i] = 0
            else:                         os_arr[i] = os_arr[i-1]

            spt_arr[i] = lower_arr[i] if os_arr[i] == 1 else upper_arr[i]

            prev_spt = spt_arr[i-1]
            crossed  = (not np.isnan(prev_spt) and
                        ((close[i-1] <= prev_spt and close[i] > spt_arr[i]) or
                         (close[i-1] >= prev_spt and close[i] < spt_arr[i])))

            prev_stmax = st_max_arr[i-1]
            if   np.isnan(prev_stmax):  st_max_arr[i] = close[i]
            elif crossed:               st_max_arr[i] = max(prev_stmax, close[i])
            elif os_arr[i] == 1:        st_max_arr[i] = max(close[i], prev_stmax)
            else:                       st_max_arr[i] = min(spt_arr[i], prev_stmax)

            prev_stmin = st_min_arr[i-1]
            if   np.isnan(prev_stmin):  st_min_arr[i] = close[i]
            elif crossed:               st_min_arr[i] = min(prev_stmin, close[i])
            elif os_arr[i] == 0:        st_min_arr[i] = min(close[i], prev_stmin)
            else:                       st_min_arr[i] = max(spt_arr[i], prev_stmin)

        data['supertrend'] = spt_arr;  data['st_dir'] = os_arr
        data['st_max']     = st_max_arr; data['st_min'] = st_min_arr
        data['st_avg']     = np.where(
            ~np.isnan(st_max_arr) & ~np.isnan(st_min_arr),
            (st_max_arr + st_min_arr) / 2, np.nan)
        data[['supertrend','st_max','st_min','st_avg']] = \
            data[['supertrend','st_max','st_min','st_avg']].round(2)
        return data

    async def calculate_additional_indicators(self, data):
        data['linearreg']     = talib.LINEARREG(data['close'], timeperiod=63)
        data['lri_slope']     = talib.LINEARREG_SLOPE(data['close'], timeperiod=63)
        data['lri_intercept'] = talib.LINEARREG_INTERCEPT(data['close'], timeperiod=63)
        data['lri_curve']     = data['linearreg'] - data['lri_intercept']
        data['lri_angle']     = talib.LINEARREG_ANGLE(data['close'], timeperiod=63)
        data['ema26']         = talib.EMA(data['close'], timeperiod=26)

        data['BuyCall'] = (data['lri_slope'] > 0).astype(int)
        data['BuyPut']  = (data['lri_slope'] < 0).astype(int)

        data['Bull'] = (
            (data['close']     > data['linearreg']) &
            (data['lri_slope'] > 0) &
            (data['TrendUp3']  == 1) &
            (data['close']     > data['open']) &
            (data['BuyCall']   == 1)
        ).astype(int)
        data['Bear'] = (
            (data['close']     < data['linearreg']) &
            (data['lri_slope'] < 0) &
            (data['TrendUp3']  == 0) &
            (data['close']     < data['open']) &
            (data['BuyPut']    == 1)
        ).astype(int)

        data[['linearreg','lri_slope','lri_intercept',
              'lri_curve','lri_angle','ema26']] = \
            data[['linearreg','lri_slope','lri_intercept',
                  'lri_curve','lri_angle','ema26']].round(4)

        # dayhigh / daylow — cumulative since 09:15 today only
        today_open = (pd.Timestamp.now(tz='Asia/Kolkata').normalize()
                      + pd.Timedelta(hours=9, minutes=15))
        today_mask  = pd.to_datetime(data['datetime']).dt.tz_localize('Asia/Kolkata') >= today_open
        data['dayhigh'] = np.where(today_mask,
                                   data['high'].where(today_mask).expanding().max(), np.nan)
        data['daylow']  = np.where(today_mask,
                                   data['low'].where(today_mask).expanding().min(),  np.nan)
        data[['dayhigh','daylow']] = data[['dayhigh','daylow']].round(2)
        return data

    # ── DB save ───────────────────────────────────────────────────────────────

    async def save_indicators_to_db(self, pool, data, num_issues):
        data = [[None if pd.isna(x) else x for x in row] for row in data]
        non_zero_data = [row for row in data
                         if any(row[i] not in (0, None) for i in [1, 2, 3, 4])]
        rows_to_write = non_zero_data[-min(len(non_zero_data), num_issues + 10):]

        replace_query = '''
            REPLACE INTO indicators_data (
                datetime, open, high, low, close, ohlc4,
                linearreg, lri_slope, lri_intercept, lri_curve, lri_angle, ema26,
                hma26_5m,
                BuyCall, BuyPut, Bull, Bear,
                ATR, VStop2, VStop3, TrendUp2, TrendUp3, Max, Min,
                cup_vstop2, cup_vstop3, cdn_vstop2, cdn_vstop3,
                dayhigh, daylow,
                st_avg, st_max, st_min, supertrend, st_dir
            ) VALUES (
                %s,%s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,%s,
                %s,
                %s,%s,%s,%s,
                %s,%s,%s,%s,%s,%s,%s,
                %s,%s,%s,%s,
                %s,%s,
                %s,%s,%s,%s,%s
            )
        '''
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.executemany(replace_query, rows_to_write)

    # ── Signal pipeline ───────────────────────────────────────────────────────

    async def get_signal(self, pool):
        num_issues = await self.check_missing_or_duplicate_keys(pool)
        if num_issues == 0:
            return

        ohlc_1m = await self.fetch_ohlctick_1mdata(pool)
        if len(ohlc_1m) < 252:
            return

        ohlc_5m = await self.fetch_ohlctick_5mdata(pool)

        indicator_data = await self.calculate_vstop(ohlc_1m)
        indicator_data = await self.calculate_supertrend(indicator_data)
        indicator_data = await self.calculate_additional_indicators(indicator_data)

        # Merge 5-min HMA onto 1-min frame
        if len(ohlc_5m) >= 26:
            indicator_data = self.merge_hma26_5m(indicator_data, ohlc_5m)
        else:
            indicator_data['hma26_5m'] = np.nan

        cols = [
            'datetime', 'open', 'high', 'low', 'close', 'ohlc4',
            'linearreg', 'lri_slope', 'lri_intercept', 'lri_curve', 'lri_angle', 'ema26',
            'hma26_5m',
            'BuyCall', 'BuyPut', 'Bull', 'Bear',
            'ATR', 'VStop2', 'VStop3', 'TrendUp2', 'TrendUp3', 'Max', 'Min',
            'cup_vstop2', 'cup_vstop3', 'cdn_vstop2', 'cdn_vstop3',
            'dayhigh', 'daylow',
            'st_avg', 'st_max', 'st_min', 'supertrend', 'st_dir',
        ]
        await self.save_indicators_to_db(pool, indicator_data[cols].to_numpy(), num_issues)

    # ── Main loop ─────────────────────────────────────────────────────────────

    async def run(self):
        print(f"Indicator Update started at {datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S')} IST")

        now = datetime.now(IST)
        if not self.is_business_day(now):
            print("Not a trading day. Exiting.")
            return

        market_close = now.replace(hour=15, minute=30, second=0, microsecond=0)
        if now > market_close:
            print(f"Market already closed ({now.strftime('%H:%M:%S')} IST). Exiting.")
            return

        market_open = now.replace(hour=9, minute=15, second=0, microsecond=0)
        if now < market_open:
            wait_secs = (market_open - now).total_seconds()
            print(f"Market opens in {wait_secs:.0f}s. Waiting...")
            await asyncio.sleep(wait_secs)

        pool = await self.get_mysql_pool()
        await self.create_tables_if_not_exists(pool)

        try:
            print("Market open. Starting indicator update loop...")
            while self.is_market_open():
                await self.get_signal(pool)

                current_time      = pd.Timestamp.now(tz='Asia/Kolkata')
                period_now        = pd.Period.now('1min')
                next_period_start = (period_now + 1).start_time.tz_localize('Asia/Kolkata')
                next_execution    = next_period_start + pd.Timedelta(seconds=4)
                sleep_till        = (next_execution - current_time).total_seconds()
                if 0 < sleep_till < 61:
                    await asyncio.sleep(sleep_till)

            print(f"Market closed at {datetime.now(IST).strftime('%H:%M:%S')} IST. Exiting.")
        except Exception as e:
            print(f"Error in indicator update loop: {e}")
            raise
        finally:
            pool.close()
            await pool.wait_closed()


if __name__ == "__main__":
    indicator_update = IndicatorUpdate()
    asyncio.run(indicator_update.run())
```

### 📄 scripts/indicatordata_all.py

```py
import asyncio
import aiomysql
import pytz
import pandas as pd
import talib
import numpy as np
import os
import ssl
from datetime import datetime

IST = pytz.timezone('Asia/Kolkata')

holidays = [
    # 2025
    "2025-02-26", "2025-03-14", "2025-03-31", "2025-04-10",
    "2025-04-14", "2025-04-18", "2025-05-01", "2025-08-15",
    "2025-08-27", "2025-10-02", "2025-10-21", "2025-10-22",
    "2025-11-05", "2025-11-15", "2025-12-25",
    # 2026
    "2026-01-26", "2026-03-03", "2026-03-26", "2026-03-31",
    "2026-04-03", "2026-04-14", "2026-05-01", "2026-05-28",
    "2026-06-26", "2026-09-14", "2026-10-02", "2026-10-20",
    "2026-11-10", "2026-11-24", "2026-12-25",
]

db_config = {
    "host":     os.getenv("DB_HOST"),
    "port":     int(os.getenv("DB_PORT", 3306)),
    "user":     os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
}


class IndicatorAllData:

    def __init__(self):
        pass

    # ── DB pool ───────────────────────────────────────────────────────────────

    async def get_mysql_pool(self):
        ssl_ctx = None
        ca_path = os.path.join(os.path.dirname(__file__), '..', 'ca.pem')
        if os.path.exists(ca_path):
            ssl_ctx = ssl.create_default_context(cafile=ca_path)

        pool = await aiomysql.create_pool(
            host=db_config['host'],
            port=db_config['port'],
            user=db_config['user'],
            password=db_config['password'],
            db=db_config['database'],
            autocommit=True,
            ssl=ssl_ctx,
            minsize=2,
            maxsize=10,
        )
        return pool

    # ── DDL ───────────────────────────────────────────────────────────────────

    async def create_tables_if_not_exists(self, pool):
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute('''
                    CREATE TABLE IF NOT EXISTS indicators_data (
                        datetime      DATETIME PRIMARY KEY,
                        open          DOUBLE, high  DOUBLE, low   DOUBLE, close DOUBLE,
                        ohlc4         DOUBLE,
                        linearreg     DOUBLE,
                        lri_slope     DOUBLE, lri_intercept DOUBLE,
                        lri_curve     DOUBLE, lri_angle     DOUBLE,
                        ema26         DOUBLE,
                        hma26_5m      DOUBLE,
                        BuyCall       INTEGER, BuyPut  INTEGER,
                        Bull          INTEGER, Bear    INTEGER,
                        ATR           DOUBLE,
                        VStop2        DOUBLE,  VStop3        DOUBLE,
                        TrendUp2      INTEGER, TrendUp3      INTEGER,
                        Max           DOUBLE,  Min           DOUBLE,
                        cup_vstop2    DOUBLE,  cup_vstop3    DOUBLE,
                        cdn_vstop2    DOUBLE,  cdn_vstop3    DOUBLE,
                        dayhigh       DOUBLE,  daylow        DOUBLE,
                        st_avg        DOUBLE,  st_max        DOUBLE,
                        st_min        DOUBLE,  supertrend    DOUBLE,
                        st_dir        INTEGER
                    )
                ''')

    async def truncate_table(self, pool):
        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute('TRUNCATE TABLE indicators_data')
            await conn.commit()
        print("indicators_data truncated.")

    # ── Data fetch ────────────────────────────────────────────────────────────

    async def fetch_ohlctick_1mdata(self, pool):
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT * FROM ohlctick_1mdata ORDER BY datetime")
                result  = await cur.fetchall()
                columns = [col[0] for col in cur.description]
                data    = pd.DataFrame(result, columns=columns)
                data.sort_values(by='datetime', inplace=True)
                return data

    async def fetch_ohlctick_5mdata(self, pool):
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT * FROM ohlctick_5mdata ORDER BY datetime")
                result  = await cur.fetchall()
                columns = [col[0] for col in cur.description]
                data    = pd.DataFrame(result, columns=columns)
                data.sort_values(by='datetime', inplace=True)
                return data
    def _hma(self, series: pd.Series, length: int) -> pd.Series:
        half  = max(1, length // 2)
        sqrtn = max(1, int(np.floor(np.sqrt(length))))
        wma_half = talib.WMA(series, timeperiod=half)
        wma_full = talib.WMA(series, timeperiod=length)
        diff     = 2 * wma_half - wma_full
        return talib.WMA(diff, timeperiod=sqrtn)
    # ── HMA (5-min) → forward-fill onto 1-min index ──────────────────────────

    def merge_hma26_5m(self, data_1m, data_5m):
        df5 = data_5m.copy()
        df5['datetime'] = pd.to_datetime(df5['datetime'])
        df5.set_index('datetime', inplace=True)
        df5['hma26_5m'] = self._hma(df5['hlc3'], 26).round(2)

        df1 = data_1m.copy()
        df1['datetime'] = pd.to_datetime(df1['datetime'])
        df1.set_index('datetime', inplace=True)
        df1['hma26_5m'] = df5['hma26_5m'].reindex(df1.index, method='ffill')
        df1.reset_index(inplace=True)
        return df1

    # ── Indicator calculations ────────────────────────────────────────────────

    async def calculate_vstop(self, data):
        data['ATR']      = talib.ATR(data['high'], data['low'], data['close'], timeperiod=252)
        data['VStop2']   = np.nan; data['VStop3']   = np.nan
        data['TrendUp2'] = True;   data['TrendUp3'] = True
        data['Max']      = data['close']; data['Min'] = data['close']

        cup_vstop2 = np.nan; cup_vstop3 = np.nan
        cdn_vstop2 = np.nan; cdn_vstop3 = np.nan
        cup_vstop2_arr = np.full(len(data), np.nan)
        cup_vstop3_arr = np.full(len(data), np.nan)
        cdn_vstop2_arr = np.full(len(data), np.nan)
        cdn_vstop3_arr = np.full(len(data), np.nan)

        for i in range(252, len(data)):
            src    = data['close'].iloc[i]
            atr_m2 = data['ATR'].iloc[i] * 2
            atr_m3 = data['ATR'].iloc[i] * 3

            data.at[i, 'Max'] = max(data['Max'].iloc[i-1], src)
            data.at[i, 'Min'] = min(data['Min'].iloc[i-1], src)

            prev2 = data['VStop2'].iloc[i-1]
            if data['TrendUp2'].iloc[i-1]:
                data.at[i, 'VStop2'] = max(prev2 if not np.isnan(prev2) else src,
                                           data['Max'].iloc[i] - atr_m2)
            else:
                data.at[i, 'VStop2'] = min(prev2 if not np.isnan(prev2) else src,
                                           data['Min'].iloc[i] + atr_m2)
            data.at[i, 'TrendUp2'] = src >= data['VStop2'].iloc[i]
            if data['TrendUp2'].iloc[i] != data['TrendUp2'].iloc[i-1]:
                data.at[i, 'Max']    = src; data.at[i, 'Min']    = src
                data.at[i, 'VStop2'] = (data['Max'].iloc[i] - atr_m2
                                        if data['TrendUp2'].iloc[i]
                                        else data['Min'].iloc[i] + atr_m2)

            prev3 = data['VStop3'].iloc[i-1]
            if data['TrendUp3'].iloc[i-1]:
                data.at[i, 'VStop3'] = max(prev3 if not np.isnan(prev3) else src,
                                           data['Max'].iloc[i] - atr_m3)
            else:
                data.at[i, 'VStop3'] = min(prev3 if not np.isnan(prev3) else src,
                                           data['Min'].iloc[i] + atr_m3)
            data.at[i, 'TrendUp3'] = src >= data['VStop3'].iloc[i]
            if data['TrendUp3'].iloc[i] != data['TrendUp3'].iloc[i-1]:
                data.at[i, 'Max']    = src; data.at[i, 'Min']    = src
                data.at[i, 'VStop3'] = (data['Max'].iloc[i] - atr_m3
                                        if data['TrendUp3'].iloc[i]
                                        else data['Min'].iloc[i] + atr_m3)

            v2_now  = data['VStop2'].iloc[i];  v3_now  = data['VStop3'].iloc[i]
            v2_prev = data['VStop2'].iloc[i-1]; v3_prev = data['VStop3'].iloc[i-1]
            if not (np.isnan(v2_now) or np.isnan(v3_now) or
                    np.isnan(v2_prev) or np.isnan(v3_prev)):
                if v2_prev <= v3_prev and v2_now > v3_now:
                    cup_vstop2 = v2_now; cup_vstop3 = v3_now
                elif v2_prev >= v3_prev and v2_now < v3_now:
                    cdn_vstop2 = v2_now; cdn_vstop3 = v3_now

            cup_vstop2_arr[i] = cup_vstop2; cup_vstop3_arr[i] = cup_vstop3
            cdn_vstop2_arr[i] = cdn_vstop2; cdn_vstop3_arr[i] = cdn_vstop3

        data['cup_vstop2'] = cup_vstop2_arr; data['cup_vstop3'] = cup_vstop3_arr
        data['cdn_vstop2'] = cdn_vstop2_arr; data['cdn_vstop3'] = cdn_vstop3_arr
        data[['ATR','VStop2','VStop3',
              'cup_vstop2','cup_vstop3',
              'cdn_vstop2','cdn_vstop3']] = \
            data[['ATR','VStop2','VStop3',
                  'cup_vstop2','cup_vstop3',
                  'cdn_vstop2','cdn_vstop3']].round(2)
        return data

    async def calculate_supertrend(self, data, atr_period=63, atr_mult=3.0):
        atr  = talib.ATR(data['high'], data['low'], data['close'],
                         timeperiod=atr_period) * atr_mult
        hl2  = (data['high'] + data['low']) / 2
        n    = len(data)
        upper_arr  = np.full(n, np.nan); lower_arr  = np.full(n, np.nan)
        os_arr     = np.zeros(n, dtype=int)
        spt_arr    = np.full(n, np.nan)
        st_max_arr = np.full(n, np.nan); st_min_arr = np.full(n, np.nan)

        close = data['close'].to_numpy()
        hl2v  = hl2.to_numpy()
        atrv  = atr.to_numpy()

        for i in range(1, n):
            if np.isnan(atrv[i]):
                continue
            up = hl2v[i] + atrv[i]; dn = hl2v[i] - atrv[i]

            prev_upper = upper_arr[i-1]
            upper_arr[i] = (min(up, prev_upper) if not np.isnan(prev_upper)
                            and close[i-1] < prev_upper else up)

            prev_lower = lower_arr[i-1]
            lower_arr[i] = (max(dn, prev_lower) if not np.isnan(prev_lower)
                            and close[i-1] > prev_lower else dn)

            if   close[i] > upper_arr[i]: os_arr[i] = 1
            elif close[i] < lower_arr[i]: os_arr[i] = 0
            else:                         os_arr[i] = os_arr[i-1]

            spt_arr[i] = lower_arr[i] if os_arr[i] == 1 else upper_arr[i]

            prev_spt = spt_arr[i-1]
            crossed  = (not np.isnan(prev_spt) and
                        ((close[i-1] <= prev_spt and close[i] > spt_arr[i]) or
                         (close[i-1] >= prev_spt and close[i] < spt_arr[i])))

            prev_stmax = st_max_arr[i-1]
            if   np.isnan(prev_stmax):  st_max_arr[i] = close[i]
            elif crossed:               st_max_arr[i] = max(prev_stmax, close[i])
            elif os_arr[i] == 1:        st_max_arr[i] = max(close[i], prev_stmax)
            else:                       st_max_arr[i] = min(spt_arr[i], prev_stmax)

            prev_stmin = st_min_arr[i-1]
            if   np.isnan(prev_stmin):  st_min_arr[i] = close[i]
            elif crossed:               st_min_arr[i] = min(prev_stmin, close[i])
            elif os_arr[i] == 0:        st_min_arr[i] = min(close[i], prev_stmin)
            else:                       st_min_arr[i] = max(spt_arr[i], prev_stmin)

        data['supertrend'] = spt_arr;  data['st_dir']  = os_arr
        data['st_max']     = st_max_arr; data['st_min'] = st_min_arr
        data['st_avg']     = np.where(
            ~np.isnan(st_max_arr) & ~np.isnan(st_min_arr),
            (st_max_arr + st_min_arr) / 2, np.nan)
        data[['supertrend','st_max','st_min','st_avg']] = \
            data[['supertrend','st_max','st_min','st_avg']].round(2)
        return data

    async def calculate_additional_indicators(self, data):
        data['linearreg']     = talib.LINEARREG(data['close'], timeperiod=63)
        data['lri_slope']     = talib.LINEARREG_SLOPE(data['close'], timeperiod=63)
        data['lri_intercept'] = talib.LINEARREG_INTERCEPT(data['close'], timeperiod=63)
        data['lri_curve']     = data['linearreg'] - data['lri_intercept']
        data['lri_angle']     = talib.LINEARREG_ANGLE(data['close'], timeperiod=63)
        data['ema26']         = talib.EMA(data['close'], timeperiod=26)

        data['BuyCall'] = (data['lri_slope'] > 0).astype(int)
        data['BuyPut']  = (data['lri_slope'] < 0).astype(int)

        data['Bull'] = (
            (data['close']     > data['linearreg']) &
            (data['lri_slope'] > 0) &
            (data['TrendUp3']  == 1) &
            (data['close']     > data['open']) &
            (data['BuyCall']   == 1)
        ).astype(int)
        data['Bear'] = (
            (data['close']     < data['linearreg']) &
            (data['lri_slope'] < 0) &
            (data['TrendUp3']  == 0) &
            (data['close']     < data['open']) &
            (data['BuyPut']    == 1)
        ).astype(int)

        data[['linearreg','lri_slope','lri_intercept',
              'lri_curve','lri_angle','ema26']] = \
            data[['linearreg','lri_slope','lri_intercept',
                  'lri_curve','lri_angle','ema26']].round(4)

        # dayhigh / daylow — correct multi-day cumulative per calendar day
        data['_dt']       = pd.to_datetime(data['datetime'])
        data['_day_open'] = data['_dt'].dt.normalize() + pd.Timedelta(hours=9, minutes=15)
        mask = data['_dt'] >= data['_day_open']
        data['dayhigh'] = data['high'].where(mask).groupby(data['_dt'].dt.date).cummax()
        data['daylow']  = data['low'].where(mask).groupby(data['_dt'].dt.date).cummin()
        data[['dayhigh','daylow']] = data[['dayhigh','daylow']].round(2)
        data.drop(columns=['_dt','_day_open'], inplace=True)
        return data

    # ── DB save ───────────────────────────────────────────────────────────────

    async def save_indicators_to_db(self, pool, data):
        data = [[None if pd.isna(x) else x for x in row] for row in data]
        non_zero_data = [row for row in data
                         if any(row[i] not in (0, None) for i in [1, 2, 3, 4])]

        replace_query = '''
            REPLACE INTO indicators_data (
                datetime, open, high, low, close, ohlc4,
                linearreg, lri_slope, lri_intercept, lri_curve, lri_angle, ema26,
                hma26_5m,
                BuyCall, BuyPut, Bull, Bear,
                ATR, VStop2, VStop3, TrendUp2, TrendUp3, Max, Min,
                cup_vstop2, cup_vstop3, cdn_vstop2, cdn_vstop3,
                dayhigh, daylow,
                st_avg, st_max, st_min, supertrend, st_dir
            ) VALUES (
                %s,%s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,%s,
                %s,
                %s,%s,%s,%s,
                %s,%s,%s,%s,%s,%s,%s,
                %s,%s,%s,%s,
                %s,%s,
                %s,%s,%s,%s,%s
            )
        '''
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.executemany(replace_query, non_zero_data)

    # ── Entry point ───────────────────────────────────────────────────────────

    async def run(self):
        pool = await self.get_mysql_pool()
        await self.create_tables_if_not_exists(pool)
        await self.truncate_table(pool)

        try:
            ohlc_1m = await self.fetch_ohlctick_1mdata(pool)
            if len(ohlc_1m) < 252:
                print(f"Not enough 1-min data: {len(ohlc_1m)} rows (need 252+). Exiting.")
                return

            ohlc_5m = await self.fetch_ohlctick_5mdata(pool)
            if len(ohlc_5m) < 26:
                print(f"Not enough 5-min data: {len(ohlc_5m)} rows (need 26+). Exiting.")
                return

            indicator_data = await self.calculate_vstop(ohlc_1m)
            indicator_data = await self.calculate_supertrend(indicator_data)
            indicator_data = await self.calculate_additional_indicators(indicator_data)
            indicator_data = self.merge_hma26_5m(indicator_data, ohlc_5m)

            cols = [
                'datetime', 'open', 'high', 'low', 'close', 'ohlc4',
                'linearreg', 'lri_slope', 'lri_intercept', 'lri_curve', 'lri_angle', 'ema26',
                'hma26_5m',
                'BuyCall', 'BuyPut', 'Bull', 'Bear',
                'ATR', 'VStop2', 'VStop3', 'TrendUp2', 'TrendUp3', 'Max', 'Min',
                'cup_vstop2', 'cup_vstop3', 'cdn_vstop2', 'cdn_vstop3',
                'dayhigh', 'daylow',
                'st_avg', 'st_max', 'st_min', 'supertrend', 'st_dir',
            ]
            await self.save_indicators_to_db(pool, indicator_data[cols].to_numpy())
            print(f"Indicator calculation complete — "
                  f"{datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S')} IST")

        except Exception as e:
            print(f"Error during indicator calculation: {e}")
            raise
        finally:
            pool.close()
            await pool.wait_closed()


if __name__ == "__main__":
    indicator_alldata = IndicatorAllData()
    asyncio.run(indicator_alldata.run())
```

### 📄 scripts/optionbuying.py

```py
import os
import ssl
import asyncio
import aiomysql
import pytz
import pandas as pd
import numpy as np
import math
from datetime import datetime, time, timedelta
from breeze_connect import BreezeConnect

IST = pytz.timezone('Asia/Kolkata')

# ── NSE Holidays ────────────────────────────────────────────────────────────
holidays = [
    # 2025
    "2025-02-26", "2025-03-14", "2025-03-31", "2025-04-10", "2025-04-14",
    "2025-04-18", "2025-05-01", "2025-08-15", "2025-08-27", "2025-10-02",
    "2025-10-21", "2025-10-22", "2025-11-05", "2025-11-15", "2025-12-25",
    # 2026 — NSE Circular NSE/CMTR/71775
    "2026-01-26", "2026-03-03", "2026-03-26", "2026-03-31", "2026-04-03",
    "2026-04-14", "2026-05-01", "2026-05-28", "2026-06-26", "2026-09-14",
    "2026-10-02", "2026-10-20", "2026-11-10", "2026-11-24", "2026-12-25",
]

# ── Config from environment ──────────────────────────────────────────────────
db_config = {
    "host":     os.getenv("DB_HOST"),
    "port":     int(os.getenv("DB_PORT", 3306)),
    "user":     os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
}

def get_last_wednesday(year, month, timezone):
    """Last Wednesday of a given month."""
    if month == 12:
        last_day = timezone.localize(datetime(year, month, 31))
    else:
        first_day_next_month = timezone.localize(datetime(year, month + 1, 1))
        last_day = first_day_next_month - timedelta(days=1)
    offset = (last_day.weekday() - 2) % 7  # 2 = Wednesday
    return last_day - timedelta(days=offset)


def get_monthly_expiry():
    """
    Auto-calculate Bank Nifty monthly expiry.
    Last Wednesday of current month; rolls to next month if today is past it.
    Rolls back one day if Wednesday falls on an NSE holiday.
    """
    today = datetime.now(IST)
    year, month = today.year, today.month

    expiry = get_last_wednesday(year, month, IST)
    while expiry.strftime('%Y-%m-%d') in holidays:
        expiry -= timedelta(days=1)

    if today.date() > expiry.date():
        month = month + 1 if month < 12 else 1
        year  = year + 1 if month == 1 else year
        expiry = get_last_wednesday(year, month, IST)
        while expiry.strftime('%Y-%m-%d') in holidays:
            expiry -= timedelta(days=1)

    return expiry.strftime('%Y-%m-%d')


# Auto-calculated at job startup — no manual EXPIRY_DATE env var needed
expiry_date = get_monthly_expiry()


def get_breeze_api():
    """Initialise Breeze API using credentials from environment."""
    api_key       = os.getenv("API_KEY")
    api_secret    = os.getenv("API_SECRET")
    session_token = os.getenv("SESSION_TOKEN")

    if not all([api_key, api_secret, session_token]):
        raise EnvironmentError(
            "Missing Breeze credentials: API_KEY, API_SECRET, SESSION_TOKEN must be set."
        )

    api = BreezeConnect(api_key=api_key)
    api.generate_session(api_secret=api_secret, session_token=str(session_token))
    return api


# ── MySQL pool ───────────────────────────────────────────────────────────────
async def get_mysql_pool():
    ssl_ctx = None
    ca_path = os.path.join(os.path.dirname(__file__), '..', 'ca.pem')
    if os.path.exists(ca_path):
        ssl_ctx = ssl.create_default_context(cafile=ca_path)

    pool = await aiomysql.create_pool(
        host=db_config['host'],
        port=db_config['port'],
        user=db_config['user'],
        password=db_config['password'],
        db=db_config['database'],
        autocommit=True,
        ssl=ssl_ctx,
        minsize=2,
        maxsize=10,
    )
    return pool


def safe(v):
    """Replace NaN/inf with None for MySQL compatibility."""
    if v is None:
        return None
    try:
        if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
            return None
    except Exception:
        pass
    return v


# ════════════════════════════════════════════════════════════════════════════
class OptionBuying:

    def __init__(self):
        self.api = None   # Initialised lazily — only when placing orders

    # ── Market helpers ───────────────────────────────────────────────────────
    def is_market_open(self):
        now = datetime.now(IST)
        open_time  = datetime.combine(now.date(), time(9, 15)).replace(tzinfo=IST)
        close_time = datetime.combine(now.date(), time(15, 30)).replace(tzinfo=IST)
        return open_time <= now <= close_time

    def is_business_day(self, date):
        return date.weekday() < 5 and date.strftime('%Y-%m-%d') not in holidays

    # ── Data fetch ───────────────────────────────────────────────────────────
    async def fetch_indicators_data(self, pool):
        """
        Fetch last 1 day of indicators_data.
        dayhigh/daylow are pre-calculated per candle by indicator_update.py
        as cumulative high/low since 09:15 — no recalculation needed here.
        """
        query = (
            "SELECT * FROM `indicators_data` "
            "WHERE `datetime` >= NOW() - INTERVAL 1 DAY "
            "ORDER BY `datetime`"
        )
        async with pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(query)
                result = await cur.fetchall()
        if not result:
            return pd.DataFrame()
        data = pd.DataFrame(result)
        data['datetime'] = pd.to_datetime(data['datetime'])
        data = data.sort_values(by='datetime', ascending=True).reset_index(drop=True)
        return data

    # ── Signal calculation ───────────────────────────────────────────────────
    async def get_sma_cross_data(self, data):
        required = [
            'lri_slope', 'lri_angle', 'TrendUp2', 'TrendUp3',
            'BuyCall', 'BuyPut', 'close', 'open',
        ]
        missing = [c for c in required if c not in data.columns]
        if missing:
            raise ValueError(f"Missing columns in indicators_data: {missing}")

        trendup_crossover = (
            ((data['TrendUp2'] == 1) & (data['TrendUp2'].shift(1) == 0) &
             ((data['lri_slope'] > 0) | (data['BuyCall'] == 1))) |
            ((data['TrendUp3'] == 1) & (data['TrendUp3'].shift(1) == 0)) |
            ((data['TrendUp2'] == 1) & (data['TrendUp3'] == 1) &
             (data['TrendUp3'].shift(1) == 0) & (data['TrendUp2'].shift(1) == 0))
        )

        trendup_crossunder = (
            ((data['TrendUp2'] == 0) & (data['TrendUp2'].shift(1) == 1) &
             ((data['lri_slope'] < 0) | (data['BuyPut'] == 1))) |
            ((data['TrendUp3'] == 0) & (data['TrendUp3'].shift(1) == 1)) |
            ((data['TrendUp2'] == 0) & (data['TrendUp3'] == 0) &
             (data['TrendUp3'].shift(1) == 1) & (data['TrendUp2'].shift(1) == 1))
        )

        bullish_trend = (data['close'] > data['open']) & (data['TrendUp3'] == 1) & (data['TrendUp2'] == 1)
        bearish_trend = (data['close'] < data['open']) & (data['TrendUp3'] == 0) & (data['TrendUp2'] == 0)

        crossover_data  = data.loc[trendup_crossover & bullish_trend].copy()
        crossunder_data = data.loc[trendup_crossunder & bearish_trend].copy()

        return crossover_data, crossunder_data

    async def get_entry_trigger(self, crossover_data, crossunder_data):
        if crossover_data.empty or crossunder_data.empty:
            return None, None, None, None

        if len(crossover_data) < 2 or len(crossunder_data) < 2:
            return None, None, None, None

        co_row  = crossover_data.iloc[-1]
        cu_row  = crossunder_data.iloc[-1]
        co_pre  = crossover_data.iloc[-2]
        cu_pre  = crossunder_data.iloc[-2]

        call_entry_trigger = call_sl_trigger = None
        put_entry_trigger  = put_sl_trigger  = None

        max_dt     = max(co_row['datetime'],  cu_row['datetime'])
        max_pre_dt = max(co_pre['datetime'],  cu_pre['datetime'])

        if (max_dt - max_pre_dt) <= timedelta(minutes=2):
            if max_pre_dt == co_pre['datetime']:
                call_entry_trigger = co_pre['close']
                call_sl_trigger    = min(co_pre['VStop2'], co_pre['VStop3'])
            if max_pre_dt == cu_pre['datetime']:
                put_entry_trigger  = cu_pre['close']
                put_sl_trigger     = max(cu_pre['VStop2'], cu_pre['VStop3'])

        if max_dt == co_row['datetime']:
            call_entry_trigger = co_row['close']
            call_sl_trigger    = min(co_row['VStop2'], co_row['VStop3'])

        if max_dt == cu_row['datetime']:
            put_entry_trigger  = cu_row['close']
            put_sl_trigger     = max(cu_row['VStop2'], cu_row['VStop3'])

        return call_entry_trigger, call_sl_trigger, put_entry_trigger, put_sl_trigger

    async def get_strike_prices(self, call_entry_trigger, put_entry_trigger):
        strike_price = None
        option_type  = None
        if call_entry_trigger:
            strike_price = int((call_entry_trigger - call_entry_trigger % 100) - 100)
            option_type  = "call"
        if put_entry_trigger:
            strike_price = int((put_entry_trigger - put_entry_trigger % 100) + 100)
            if option_type is None:
                option_type = "put"
        return strike_price, option_type

    async def get_entry_signal(self, call_entry_trigger, put_entry_trigger, data):
        callbuy_signal = putbuy_signal = None

        close_last  = data['close'].iloc[-1]
        last_bull   = data['Bull'].iloc[-1]
        last_bear   = data['Bear'].iloc[-1]
        ohlc4_last  = data['ohlc4'].iloc[-1]

        now              = datetime.now(IST)
        trade_start      = datetime.combine(now.date(), time(9, 20)).replace(tzinfo=IST)
        trade_end        = datetime.combine(now.date(), time(15, 7)).replace(tzinfo=IST)
        in_trading_hours = trade_start < now < trade_end

        if (call_entry_trigger is not None and put_entry_trigger is None and
                last_bull == 1 and last_bear == 0 and in_trading_hours and
                (ohlc4_last - call_entry_trigger) <= 100):
            callbuy_signal = int(close_last > call_entry_trigger)

        if (put_entry_trigger is not None and call_entry_trigger is None and
                last_bear == 1 and last_bull == 0 and in_trading_hours and
                (put_entry_trigger - ohlc4_last) <= 100):
            putbuy_signal = int(close_last < put_entry_trigger)

        return callbuy_signal, putbuy_signal

    # ── Order placement ──────────────────────────────────────────────────────
    async def fetch_order_placed_flag(self, pool):
        today = datetime.now(IST).strftime('%Y-%m-%d')
        query = """
            SELECT * FROM `order_notification`
            WHERE `orderReference` IS NOT NULL
              AND DATE(`datetime`) = %s
            ORDER BY `datetime` DESC
        """
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query, (today,))
                result = await cur.fetchall()
                if not result:
                    return False
                columns    = [col[0] for col in cur.description]
                order_data = pd.DataFrame(result, columns=columns)
                order_data.sort_values(by='datetime', inplace=True)
                valid      = order_data[order_data['orderReference'].notna()]
                last_flow  = valid['orderFlow'].iloc[-1]
                last_qty   = valid['executedQuantity'].iloc[-1]
                flag = (last_flow == 'Buy' and last_qty > 0) or \
                       (last_flow == 'Sell' and last_qty == 0)
                return flag

    async def place_order(self, strike_price, option_type,
                          callbuy_signal, putbuy_signal, order_placed_flag):
        if not strike_price or not option_type:
            return None
        if order_placed_flag or not (callbuy_signal or putbuy_signal):
            return None

        # Initialise Breeze API only when actually placing an order
        if self.api is None:
            self.api = get_breeze_api()

        try:
            response = self.api.place_order(
                stock_code="CNXBAN",
                exchange_code="NFO",
                product="options",
                action="BUY",
                order_type="market",
                stoploss="",
                quantity="15",
                price="",
                validity="day",
                validity_date=datetime.now(IST).strftime('%Y-%m-%d'),
                disclosed_quantity='0',
                expiry_date=expiry_date,
                right=option_type,
                strike_price=strike_price,
            )
            if response.get('Status') == 200:
                order_id = response['Success']['order_id']
                print(f"Order placed successfully: order_id={order_id}")
                return order_id
            else:
                print(f"Order failed: {response}")
                return None
        except Exception as e:
            print(f"Order placement error: {e}")
            return None

    # ── DB write ─────────────────────────────────────────────────────────────
    async def create_placeorder_track_table(self, pool):
        query = """
            CREATE TABLE IF NOT EXISTS `placeorder_track` (
                `datetime`           DATETIME     PRIMARY KEY,
                `order_id`           BIGINT       DEFAULT NULL,
                `call_entry_trigger` DOUBLE       DEFAULT NULL,
                `call_sl_trigger`    DOUBLE       DEFAULT NULL,
                `put_entry_trigger`  DOUBLE       DEFAULT NULL,
                `put_sl_trigger`     DOUBLE       DEFAULT NULL,
                `strike_price`       INT          DEFAULT NULL,
                `option_type`        VARCHAR(10)  DEFAULT NULL,
                `Bull`               INT          DEFAULT NULL,
                `Bear`               INT          DEFAULT NULL,
                `callbuy_signal`     INT          DEFAULT NULL,
                `putbuy_signal`      INT          DEFAULT NULL,
                `dayhigh`            DOUBLE       DEFAULT NULL,
                `daylow`             DOUBLE       DEFAULT NULL
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query)
                await conn.commit()

    async def insert_order_data(self, pool, row: dict):
        query = """
            INSERT INTO placeorder_track (
                datetime, order_id,
                call_entry_trigger, call_sl_trigger,
                put_entry_trigger,  put_sl_trigger,
                strike_price, option_type,
                Bull, Bear, callbuy_signal, putbuy_signal,
                dayhigh, daylow
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON DUPLICATE KEY UPDATE
                order_id            = VALUES(order_id),
                call_entry_trigger  = VALUES(call_entry_trigger),
                call_sl_trigger     = VALUES(call_sl_trigger),
                put_entry_trigger   = VALUES(put_entry_trigger),
                put_sl_trigger      = VALUES(put_sl_trigger),
                strike_price        = VALUES(strike_price),
                option_type         = VALUES(option_type),
                Bull                = VALUES(Bull),
                Bear                = VALUES(Bear),
                callbuy_signal      = VALUES(callbuy_signal),
                putbuy_signal       = VALUES(putbuy_signal),
                dayhigh             = VALUES(dayhigh),
                daylow              = VALUES(daylow);
        """
        values = (
            row['datetime'], row['order_id'],
            row['call_entry_trigger'], row['call_sl_trigger'],
            row['put_entry_trigger'],  row['put_sl_trigger'],
            row['strike_price'],       row['option_type'],
            row['Bull'],               row['Bear'],
            row['callbuy_signal'],     row['putbuy_signal'],
            row['dayhigh'],            row['daylow'],
        )
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query, values)
                await conn.commit()

    # ── Core run cycle ───────────────────────────────────────────────────────
    async def run_once(self, pool):
        """One full evaluation cycle — fetch signals, check entry, place order."""
        data = await self.fetch_indicators_data(pool)
        if data.empty or len(data) < 2:
            return

        crossover_data, crossunder_data = await self.get_sma_cross_data(data)
        order_placed_flag = await self.fetch_order_placed_flag(pool)

        call_entry, call_sl, put_entry, put_sl = await self.get_entry_trigger(
            crossover_data, crossunder_data)
        callbuy_signal, putbuy_signal = await self.get_entry_signal(
            call_entry, put_entry, data)
        strike_price, option_type = await self.get_strike_prices(call_entry, put_entry)

        order_id = await self.place_order(
            strike_price, option_type,
            callbuy_signal, putbuy_signal, order_placed_flag)

        # dayhigh/daylow: read from last row of indicators_data.
        # Pre-calculated per candle as cumulative max/min since 09:15 by indicator_update.py.
        last_row = data.iloc[-1]
        dayhigh  = last_row.get('dayhigh') if 'dayhigh' in data.columns else None
        daylow   = last_row.get('daylow')  if 'daylow'  in data.columns else None

        # Use the last closed candle's datetime as the row key.
        # This ensures ON DUPLICATE KEY UPDATE deduplicates correctly —
        # one row per candle regardless of how many times run_once() fires
        # within the same minute (retries, overlaps, etc.).
        candle_dt = data['datetime'].iloc[-1].strftime('%Y-%m-%d %H:%M:%S')

        row = {
            'datetime':           candle_dt,
            'order_id':           order_id,
            'call_entry_trigger': call_entry,
            'call_sl_trigger':    call_sl,
            'put_entry_trigger':  put_entry,
            'put_sl_trigger':     put_sl,
            'strike_price':       strike_price,
            'option_type':        option_type,
            'Bull':               int(data['Bull'].iloc[-1]),
            'Bear':               int(data['Bear'].iloc[-1]),
            'callbuy_signal':     callbuy_signal,
            'putbuy_signal':      putbuy_signal,
            'dayhigh':            dayhigh,
            'daylow':             daylow,
        }

        row = {k: safe(v) for k, v in row.items()}
        await self.insert_order_data(pool, row)

    # ── Entry points ─────────────────────────────────────────────────────────
    async def run(self):
        """On-demand: single evaluation cycle."""
        print(f"OptionBuying on-demand run at {datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S')} IST")
        pool = await get_mysql_pool()
        await self.create_placeorder_track_table(pool)
        try:
            await self.run_once(pool)
        except Exception as e:
            print(f"Error: {e}")
            raise
        finally:
            pool.close()
            await pool.wait_closed()

    async def run_scheduled(self):
        """Scheduled: runs every minute during market hours, exits at close."""
        print(f"OptionBuying scheduled job started at "
              f"{datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S')} IST")

        now = datetime.now(IST)

        if not self.is_business_day(now):
            print("Not a trading day. Exiting.")
            return

        market_close = now.replace(hour=15, minute=30, second=0, microsecond=0)
        if now > market_close:
            print(f"Market already closed ({now.strftime('%H:%M:%S')} IST). Exiting.")
            return

        market_open = now.replace(hour=9, minute=15, second=0, microsecond=0)
        if now < market_open:
            wait_secs = (market_open - now).total_seconds()
            print(f"Market opens in {wait_secs:.0f}s. Waiting...")
            await asyncio.sleep(wait_secs)

        pool = await get_mysql_pool()
        await self.create_placeorder_track_table(pool)

        try:
            print("Market open. Starting option buying loop...")
            while self.is_market_open():
                await self.run_once(pool)

                current_time      = pd.Timestamp.now(tz='Asia/Kolkata')
                period_now        = pd.Period.now('1min')
                next_period_start = (period_now + 1).start_time.tz_localize('Asia/Kolkata')
                next_execution    = next_period_start + pd.Timedelta(seconds=6)
                sleep_secs        = (next_execution - current_time).total_seconds()

                if 0 < sleep_secs < 64:
                    print(f"Sleeping {sleep_secs:.1f}s until next candle...")
                    await asyncio.sleep(sleep_secs)

            print(f"Market closed at {datetime.now(IST).strftime('%H:%M:%S')} IST. "
                  f"OptionBuying job complete. Exiting.")
        except Exception as e:
            print(f"Error in option buying loop: {e}")
            raise
        finally:
            pool.close()
            await pool.wait_closed()


# ── Run mode ─────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import sys
    mode = sys.argv[1] if len(sys.argv) > 1 else "scheduled"

    ob = OptionBuying()
    if mode == "once":
        asyncio.run(ob.run())
    else:
        asyncio.run(ob.run_scheduled())
```

### 📄 scripts/order_stream.py

```py
import os
import ssl
import asyncio
import aiomysql
import pytz
import pandas as pd
from datetime import datetime, time, timedelta
from breeze_connect import BreezeConnect

IST = pytz.timezone('Asia/Kolkata')

# ── NSE Holidays ─────────────────────────────────────────────────────────────
holidays = [
    # 2025
    "2025-02-26", "2025-03-14", "2025-03-31", "2025-04-10", "2025-04-14",
    "2025-04-18", "2025-05-01", "2025-08-15", "2025-08-27", "2025-10-02",
    "2025-10-21", "2025-10-22", "2025-11-05", "2025-11-15", "2025-12-25",
    # 2026 — NSE Circular NSE/CMTR/71775
    "2026-01-26", "2026-03-03", "2026-03-26", "2026-03-31", "2026-04-03",
    "2026-04-14", "2026-05-01", "2026-05-28", "2026-06-26", "2026-09-14",
    "2026-10-02", "2026-10-20", "2026-11-10", "2026-11-24", "2026-12-25",
]

# ── Config from environment ───────────────────────────────────────────────────
db_config = {
    "host":     os.getenv("DB_HOST"),
    "port":     int(os.getenv("DB_PORT", 3306)),
    "user":     os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
}

REQUIRED_COLUMNS = [
    'datetime', 'orderExchangeCode', 'stockCode', 'productType', 'optionType',
    'strikePrice', 'expiryDate', 'orderValidDate', 'orderFlow', 'limitMarketFlag',
    'orderType', 'limitRate', 'orderStatus', 'orderReference', 'executedQuantity',
]


# ════════════════════════════════════════════════════════════════════════════
class OrderNotification:

    def __init__(self):
        self.api  = None   # Initialised in run() after env vars confirmed
        self.pool = None   # Shared pool — created once in run()
        self._loop = None  # Event loop reference for WebSocket callback

    # ── Market helpers ────────────────────────────────────────────────────────
    def is_market_open(self):
        now        = datetime.now(IST)
        open_time  = datetime.combine(now.date(), time(9, 15)).replace(tzinfo=IST)
        close_time = datetime.combine(now.date(), time(15, 30)).replace(tzinfo=IST)
        is_open    = open_time <= now <= close_time
        # print(f"Market open: {is_open} | IST: {now.strftime('%H:%M:%S')}")
        return is_open

    def is_business_day(self, date):
        return date.weekday() < 5 and date.strftime('%Y-%m-%d') not in holidays

    # ── Breeze API ────────────────────────────────────────────────────────────
    def init_breeze(self):
        api_key       = os.getenv("API_KEY")
        api_secret    = os.getenv("API_SECRET")
        session_token = os.getenv("SESSION_TOKEN")

        if not all([api_key, api_secret, session_token]):
            raise EnvironmentError(
                "Missing Breeze credentials: API_KEY, API_SECRET, SESSION_TOKEN must be set."
            )

        self.api = BreezeConnect(api_key=api_key)
        self.api.generate_session(
            api_secret=api_secret,
            session_token=str(session_token)
        )
        # print("Breeze API session initialised.")

    # ── MySQL pool ────────────────────────────────────────────────────────────
    async def init_pool(self):
        ssl_ctx = None
        ca_path = os.path.join(os.path.dirname(__file__), '..', 'ca.pem')
        if os.path.exists(ca_path):
            ssl_ctx = ssl.create_default_context(cafile=ca_path)

        self.pool = await aiomysql.create_pool(
            host=db_config['host'],
            port=db_config['port'],
            user=db_config['user'],
            password=db_config['password'],
            db=db_config['database'],
            autocommit=True,
            ssl=ssl_ctx,
            minsize=2,
            maxsize=10,
        )
        # print("MySQL pool created.")

    async def create_tables_if_not_exists(self):
        query = '''
            CREATE TABLE IF NOT EXISTS `order_notification` (
                `datetime`          DATETIME     NOT NULL,
                `orderExchangeCode` TEXT,
                `stockCode`         TEXT,
                `productType`       TEXT,
                `optionType`        TEXT,
                `strikePrice`       INT          DEFAULT NULL,
                `expiryDate`        TEXT,
                `orderValidDate`    TEXT,
                `orderFlow`         TEXT,
                `limitMarketFlag`   TEXT,
                `orderType`         TEXT,
                `limitRate`         DOUBLE       DEFAULT NULL,
                `orderStatus`       TEXT,
                `orderReference`    BIGINT       DEFAULT NULL,
                `executedQuantity`  INT          DEFAULT NULL,
                PRIMARY KEY (`datetime`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        '''
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query)
        # print("Table verified: order_notification")

    # ── Tick processing ───────────────────────────────────────────────────────
    async def insert_order_notification(self, record: dict):
        """Insert a single order notification record into the DB."""
        query = '''
            REPLACE INTO `order_notification` (
                `datetime`, `orderExchangeCode`, `stockCode`,
                `productType`, `optionType`, `strikePrice`, `expiryDate`,
                `orderValidDate`, `orderFlow`, `limitMarketFlag`,
                `orderType`, `limitRate`, `orderStatus`,
                `orderReference`, `executedQuantity`
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        '''
        try:
            # ICICI sends strikePrice and limitRate multiplied by 100
            strike = int(float(record.get('strikePrice', 0) or 0) / 100)
            rate   = float(float(record.get('limitRate', 0) or 0) / 100)

            values = (
                datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S'),
                record.get('orderExchangeCode'),
                record.get('stockCode'),
                record.get('productType'),
                record.get('optionType'),
                strike,
                record.get('expiryDate'),
                record.get('orderValidDate'),
                record.get('orderFlow'),
                record.get('limitMarketFlag'),
                record.get('orderType'),
                rate,
                record.get('orderStatus'),
                record.get('orderReference'),
                record.get('executedQuantity'),
            )

            async with self.pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(query, values)
                    await conn.commit()
            # print(f"Order notification saved: {record.get('orderReference')} "
            #       f"| {record.get('orderFlow')} | {record.get('orderStatus')}")

        except Exception as e:
            print(f"Error inserting order notification: {e} | record: {record}")

    async def process_tick(self, tick):
        """Validate and insert a single tick from the WebSocket callback."""
        print(f"Tick received: {tick}")
        if not tick:
            return

        # Normalise to list
        records = [tick] if isinstance(tick, dict) else tick

        for record in records:
            if not isinstance(record, dict):
                # print(f"Unexpected tick format: {record}")
                continue

            # Check required columns
            missing = [c for c in REQUIRED_COLUMNS if c not in record and c != 'datetime']
            if missing:
                # print(f"Skipping tick — missing columns: {missing}")
                continue

            await self.insert_order_notification(record)

    def on_ticks(self, tick):
        """
        Sync callback registered with Breeze WebSocket.
        Schedules the async handler onto the running event loop.
        """
        if self._loop and not self._loop.is_closed():
            asyncio.run_coroutine_threadsafe(self.process_tick(tick), self._loop)
        else:
            print("Event loop not available — tick dropped.")

    # ── WebSocket connection ──────────────────────────────────────────────────
    async def connect_websocket(self):
        print("Connecting to Breeze WebSocket...")
        self.api.ws_connect()
        self.api.on_ticks = self.on_ticks
        self.api.subscribe_feeds(get_order_notification=True)
        print("Subscribed to order notifications feed.")

    async def disconnect_websocket(self):
        print("Disconnecting from Breeze WebSocket...")
        try:
            self.api.unsubscribe_feeds(get_order_notification=True)
            disconnected = self.api.ws_disconnect()
            print("WebSocket disconnected." if disconnected else "WebSocket disconnect error.")
        except Exception as e:
            print(f"Error during WebSocket disconnect: {e}")

    # ── Main run ──────────────────────────────────────────────────────────────
    async def run(self):
        print(f"OrderNotification job started at "
              f"{datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S')} IST")

        now = datetime.now(IST)

        # Exit if not a business day
        if not self.is_business_day(now):
            print("Not a trading day. Exiting.")
            return

        # Exit if market already closed
        market_close = now.replace(hour=15, minute=30, second=0, microsecond=0)
        if now > market_close:
            print(f"Market already closed ({now.strftime('%H:%M:%S')} IST). Exiting.")
            return

        # Wait if before market open
        market_open = now.replace(hour=9, minute=15, second=0, microsecond=0)
        if now < market_open:
            wait_secs = (market_open - now).total_seconds()
            print(f"Market opens in {wait_secs:.0f}s. Waiting...")
            await asyncio.sleep(wait_secs)

        # Initialise Breeze API and DB pool
        self.init_breeze()
        await self.init_pool()
        await self.create_tables_if_not_exists()

        # Store event loop reference for WebSocket callback thread
        self._loop = asyncio.get_event_loop()

        try:
            await self.connect_websocket()
            # print("Listening for order notifications...")

            # Keep alive until market closes — check every second
            while self.is_market_open():
                await asyncio.sleep(1)

            await self.disconnect_websocket()
            print(f"Market closed at {datetime.now(IST).strftime('%H:%M:%S')} IST. "
                  f"OrderNotification job complete. Exiting.")

        except Exception as e:
            print(f"Error in run loop: {e}")
            raise
        finally:
            await self.disconnect_websocket()
            if self.pool:
                self.pool.close()
                await self.pool.wait_closed()


# ── Entry point ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    order_stream = OrderNotification()
    asyncio.run(order_stream.run())
```

### 📄 scripts/tvdata.py

```py
from tvDatafeed import TvDatafeed, Interval
import pandas as pd
import asyncio
import aiomysql
import os
import ssl

holidays = [
    # 2025
    "2025-02-26", "2025-03-14", "2025-03-31", "2025-04-10",
    "2025-04-14", "2025-04-18", "2025-05-01", "2025-08-15",
    "2025-08-27", "2025-10-02", "2025-10-21", "2025-10-22",
    "2025-11-05", "2025-11-15", "2025-12-25",
    # 2026
    "2026-01-26", "2026-03-03", "2026-03-26", "2026-03-31",
    "2026-04-03", "2026-04-14", "2026-05-01", "2026-05-28",
    "2026-06-26", "2026-09-14", "2026-10-02", "2026-10-20",
    "2026-11-10", "2026-11-24", "2026-12-25",
]

db_config = {
    "host":     os.getenv("DB_HOST"),
    "port":     int(os.getenv("DB_PORT", 3306)),
    "user":     os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
}


class TvDataAll:

    def __init__(self):
        self.tv = TvDatafeed()

    # ── DB pool ───────────────────────────────────────────────────────────────

    async def get_mysql_pool(self):
        ssl_ctx = None
        ca_path = os.path.join(os.path.dirname(__file__), '..', 'ca.pem')
        if os.path.exists(ca_path):
            ssl_ctx = ssl.create_default_context(cafile=ca_path)

        pool = await aiomysql.create_pool(
            host=db_config['host'],
            port=db_config['port'],
            user=db_config['user'],
            password=db_config['password'],
            db=db_config['database'],
            autocommit=True,
            ssl=ssl_ctx,
            minsize=2,
            maxsize=10,
        )
        return pool

    # ── DDL ───────────────────────────────────────────────────────────────────

    async def create_tables_if_not_exists(self, pool):
        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute('''
                    CREATE TABLE IF NOT EXISTS ohlctick_1mdata (
                        datetime DATETIME PRIMARY KEY,
                        open     FLOAT,
                        high     FLOAT,
                        low      FLOAT,
                        close    FLOAT,
                        ohlc4    FLOAT
                    )
                ''')
                await cursor.execute('''
                    CREATE TABLE IF NOT EXISTS ohlctick_5mdata (
                        datetime DATETIME PRIMARY KEY,
                        open     FLOAT,
                        high     FLOAT,
                        low      FLOAT,
                        close    FLOAT,
                        ohlc4    FLOAT,
                        hlc3     FLOAT
                    )
                ''')
            await conn.commit()

    async def truncate_tables(self, pool):
        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute('TRUNCATE TABLE ohlctick_1mdata')
                await cursor.execute('TRUNCATE TABLE ohlctick_5mdata')
            await conn.commit()
        print("Tables truncated.")

    # ── Fetch & clean ─────────────────────────────────────────────────────────

    def _clean_tv_df(self, raw_data):
        """Convert raw TvDatafeed output to clean IST-aware, tz-naive DataFrame."""
        df = pd.DataFrame(raw_data)
        df.index = pd.to_datetime(df.index, errors='coerce')
        df.reset_index(inplace=True)
        df.rename(columns={'index': 'datetime'}, inplace=True)
        df['datetime'] = (
            pd.to_datetime(df['datetime'], utc=True)
              .dt.tz_convert('Asia/Kolkata')
              .dt.tz_localize(None)
        )
        df.rename(columns={
            'Open': 'open', 'High': 'high',
            'Low':  'low',  'Close': 'close',
        }, inplace=True)
        if not {'open', 'high', 'low', 'close'}.issubset(df.columns):
            print("Missing expected OHLC columns. Available:", df.columns.tolist())
            return pd.DataFrame()
        df['ohlc4'] = ((df['open'] + df['high'] +
                        df['low']  + df['close']) / 4).round(2)
        return df

    def fetch_1m(self, n_bars=1000):
        print(f"Fetching {n_bars} bars of 1-min data...")
        raw = self.tv.get_hist(
            symbol='BANKNIFTY', exchange='NSE',
            interval=Interval.in_1_minute,
            n_bars=n_bars,
        )
        if raw is None or raw.empty:
            print("1-min fetch returned no data.")
            return pd.DataFrame()
        df = self._clean_tv_df(raw)
        if df.empty:
            return df
        return df[['datetime', 'open', 'high', 'low', 'close', 'ohlc4']]

    def fetch_5m(self, n_bars=200):
        print(f"Fetching {n_bars} bars of 5-min data...")
        raw = self.tv.get_hist(
            symbol='BANKNIFTY', exchange='NSE',
            interval=Interval.in_5_minute,
            n_bars=n_bars,
        )
        if raw is None or raw.empty:
            print("5-min fetch returned no data.")
            return pd.DataFrame()
        df = self._clean_tv_df(raw)
        if df.empty:
            return df
        df['hlc3'] = ((df['high'] + df['low'] + df['close']) / 3).round(2)
        return df[['datetime', 'open', 'high', 'low', 'close', 'ohlc4', 'hlc3']]

    # ── DB insert ─────────────────────────────────────────────────────────────

    async def insert_dataframe(self, pool, df, table):
        if df.empty:
            print(f"[{table}] Nothing to insert.")
            return
        # Drop rows where all OHLC are zero — bad bars
        df = df[(df[['open', 'high', 'low', 'close']] != 0).all(axis=1)]
        if df.empty:
            print(f"[{table}] All rows were zero-OHLC, skipping.")
            return

        cols         = df.columns.tolist()
        col_names    = ', '.join(f'`{c}`' for c in cols)
        placeholders = ', '.join(['%s'] * len(cols))
        sql          = f'REPLACE INTO `{table}` ({col_names}) VALUES ({placeholders})'

        records = [tuple(row) for row in df.itertuples(index=False)]
        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                try:
                    await cursor.executemany(sql, records)
                    await conn.commit()
                    print(f"[{table}] Inserted/replaced {len(records)} rows.")
                except Exception as e:
                    print(f"[{table}] Insert error: {e}")
                    await conn.rollback()

    # ── Entry point ───────────────────────────────────────────────────────────

    async def run(self):
        pool = await self.get_mysql_pool()
        await self.create_tables_if_not_exists(pool)
        await self.truncate_tables(pool)

        df_1m = self.fetch_1m(n_bars=1000)
        df_5m = self.fetch_5m(n_bars=200)

        await self.insert_dataframe(pool, df_1m, 'ohlctick_1mdata')
        await self.insert_dataframe(pool, df_5m, 'ohlctick_5mdata')

        pool.close()
        await pool.wait_closed()
        print("Done.")


if __name__ == "__main__":
    tvdata = TvDataAll()
    asyncio.run(tvdata.run())
```

### 📄 scripts/tvdata_update.py

```py
from tvDatafeed import TvDatafeed, Interval
import pandas as pd
import asyncio
import aiomysql
import os
import ssl
import pytz
from datetime import datetime, timedelta

IST = pytz.timezone('Asia/Kolkata')

holidays = [
    # 2025
    "2025-02-26", "2025-03-14", "2025-03-31", "2025-04-10",
    "2025-04-14", "2025-04-18", "2025-05-01", "2025-08-15",
    "2025-08-27", "2025-10-02", "2025-10-21", "2025-10-22",
    "2025-11-05", "2025-11-15", "2025-12-25",
    # 2026
    "2026-01-26", "2026-03-03", "2026-03-26", "2026-03-31",
    "2026-04-03", "2026-04-14", "2026-05-01", "2026-05-28",
    "2026-06-26", "2026-09-14", "2026-10-02", "2026-10-20",
    "2026-11-10", "2026-11-24", "2026-12-25",
]

db_config = {
    "host":     os.getenv("DB_HOST"),
    "port":     int(os.getenv("DB_PORT", 3306)),
    "user":     os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
}


class TvDataUpdate:

    def __init__(self):
        self.tv = None  # TvDatafeed instance — created once in run()

    # ── DB pool ───────────────────────────────────────────────────────────────

    async def get_mysql_pool(self):
        ssl_ctx = None
        ca_path = os.path.join(os.path.dirname(__file__), '..', 'ca.pem')
        if os.path.exists(ca_path):
            ssl_ctx = ssl.create_default_context(cafile=ca_path)

        pool = await aiomysql.create_pool(
            host=db_config['host'],
            port=db_config['port'],
            user=db_config['user'],
            password=db_config['password'],
            db=db_config['database'],
            autocommit=True,
            ssl=ssl_ctx,
            minsize=2,
            maxsize=10,
        )
        return pool

    # ── DDL ───────────────────────────────────────────────────────────────────

    async def create_tables_if_not_exists(self, pool):
        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute('''
                    CREATE TABLE IF NOT EXISTS ohlctick_1mdata (
                        datetime DATETIME PRIMARY KEY,
                        open     FLOAT,
                        high     FLOAT,
                        low      FLOAT,
                        close    FLOAT,
                        ohlc4    FLOAT
                    )
                ''')
                await cursor.execute('''
                    CREATE TABLE IF NOT EXISTS ohlctick_5mdata (
                        datetime DATETIME PRIMARY KEY,
                        open     FLOAT,
                        high     FLOAT,
                        low      FLOAT,
                        close    FLOAT,
                        ohlc4    FLOAT,
                        hlc3     FLOAT
                    )
                ''')
            await conn.commit()

    # ── Gap / duplicate checker ───────────────────────────────────────────────

    async def check_missing_or_duplicate_keys(self, pool, timeframe='1m'):
        table    = 'ohlctick_1mdata' if timeframe == '1m' else 'ohlctick_5mdata'
        interval = 'INTERVAL 1 MINUTE' if timeframe == '1m' else 'INTERVAL 5 MINUTE'

        now              = pd.Timestamp.now(tz='Asia/Kolkata')
        open_time_dt     = now.replace(hour=9,  minute=15, second=0, microsecond=0)
        close_time_dt    = now.replace(hour=15, minute=30, second=0, microsecond=0)
        period_now       = pd.Period.now('1min')
        previous_candle  = (period_now - 1).start_time.tz_localize('Asia/Kolkata')
        min_datetime     = min(close_time_dt, previous_candle)

        open_str = open_time_dt.strftime('%Y-%m-%d %H:%M:%S')
        min_str  = min_datetime.strftime('%Y-%m-%d %H:%M:%S')

        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                query = f"""
                    WITH RECURSIVE datetime_sequence AS (
                        SELECT '{open_str}' AS dt
                        UNION ALL
                        SELECT DATE_ADD(dt, {interval})
                        FROM datetime_sequence
                        WHERE dt < '{min_str}'
                    )
                    SELECT COUNT(*) AS num_issues
                    FROM (
                        SELECT ds.dt AS missing_or_dup
                        FROM datetime_sequence ds
                        LEFT JOIN (
                            SELECT `datetime`, COUNT(*) AS cnt
                            FROM `{table}`
                            WHERE `datetime` >= '{open_str}' AND `datetime` <= '{min_str}'
                            GROUP BY `datetime`
                        ) t ON ds.dt = t.`datetime`
                        LEFT JOIN `{table}` id ON ds.dt = id.`datetime`
                        WHERE t.`datetime` IS NULL
                           OR t.cnt > 1
                           OR COALESCE(id.open, id.high, id.low, id.close) = 0
                           OR (id.open = id.close AND id.open = id.high AND id.open = id.low)
                    ) AS issues;
                """
                await cursor.execute(query)
                result     = await cursor.fetchone()
                num_issues = result[0] if result else 0
                if num_issues:
                    print(f"[{timeframe}] Missing/duplicate candles in {table}: {num_issues}")
                return num_issues

    # ── TvDatafeed fetch & clean ──────────────────────────────────────────────

    def _clean_tv_df(self, raw_data):
        """Convert raw TvDatafeed output to a clean, IST-aware, tz-naive DataFrame."""
        df = pd.DataFrame(raw_data)
        df.index = pd.to_datetime(df.index, errors='coerce')
        df.reset_index(inplace=True)
        df.rename(columns={'index': 'datetime'}, inplace=True)
        df['datetime'] = (
            pd.to_datetime(df['datetime'], utc=True)
              .dt.tz_convert('Asia/Kolkata')
              .dt.tz_localize(None)
        )
        df.rename(columns={
            'Open': 'open', 'High': 'high',
            'Low':  'low',  'Close': 'close',
        }, inplace=True)
        df['ohlc4'] = ((df['open'] + df['high'] +
                        df['low']  + df['close']) / 4).round(2)
        return df[['datetime', 'open', 'high', 'low', 'close', 'ohlc4']]

    async def fetch_tv_data_1m(self, pool):
        num_issues = await self.check_missing_or_duplicate_keys(pool, '1m')
        raw = self.tv.get_hist(
            symbol='BANKNIFTY', exchange='NSE',
            interval=Interval.in_1_minute,
            n_bars=num_issues + 750,
        )
        if raw is None or raw.empty:
            print("[1m] TvDatafeed returned no data.")
            return pd.DataFrame()
        return self._clean_tv_df(raw)

    async def fetch_tv_data_5m(self, pool):
        num_issues = await self.check_missing_or_duplicate_keys(pool, '5m')
        raw = self.tv.get_hist(
            symbol='BANKNIFTY', exchange='NSE',
            interval=Interval.in_5_minute,
            n_bars=num_issues + 75,
        )
        if raw is None or raw.empty:
            print("[5m] TvDatafeed returned no data.")
            return pd.DataFrame()
        df = self._clean_tv_df(raw)
        df['hlc3'] = ((df['high'] + df['low'] + df['close']) / 3).round(2)
        return df[['datetime', 'open', 'high', 'low', 'close', 'ohlc4', 'hlc3']]

    # ── DB insert ─────────────────────────────────────────────────────────────

    async def insert_tick_dataframe(self, pool, df, table):
        if df.empty:
            return
        cols        = df.columns.tolist()
        placeholders = ', '.join(['%s'] * len(cols))
        col_names    = ', '.join(f'`{c}`' for c in cols)
        sql          = f'REPLACE INTO `{table}` ({col_names}) VALUES ({placeholders})'

        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                try:
                    records = [tuple(row) for row in df.itertuples(index=False)]
                    await cursor.executemany(sql, records)
                    await conn.commit()
                except Exception as e:
                    print(f"[{table}] Insert error: {e}")
                    await conn.rollback()

    # ── Market calendar helpers ───────────────────────────────────────────────

    def is_market_open(self):
        now = pd.Timestamp.now(tz='Asia/Kolkata')
        return (now.replace(hour=9,  minute=15, second=0, microsecond=0)
                <= now <=
                now.replace(hour=15, minute=30, second=0, microsecond=0))

    def is_business_day(self, date):
        return (date.weekday() < 5 and
                date.strftime('%Y-%m-%d') not in holidays)

    # ── Main loop ─────────────────────────────────────────────────────────────

    async def run(self):
        now = datetime.now(IST)
        print(f"TvDataUpdate started at {now.strftime('%Y-%m-%d %H:%M:%S')} IST")

        if not self.is_business_day(now):
            print("Not a trading day. Exiting.")
            return

        market_close = now.replace(hour=15, minute=30, second=0, microsecond=0)
        if now > market_close:
            print(f"Market already closed ({now.strftime('%H:%M:%S')} IST). Exiting.")
            return

        # Wait if started before market open
        market_open = now.replace(hour=9, minute=15, second=0, microsecond=0)
        if now < market_open:
            wait_secs = (market_open - now).total_seconds()
            print(f"Market opens in {wait_secs:.0f}s. Waiting...")
            await asyncio.sleep(wait_secs)

        # Single TvDatafeed instance for the entire session
        self.tv = TvDatafeed()

        pool = await self.get_mysql_pool()
        await self.create_tables_if_not_exists(pool)

        try:
            print("Market open. Starting data pipeline...")
            while self.is_market_open():
                df_1m = await self.fetch_tv_data_1m(pool)
                df_5m = await self.fetch_tv_data_5m(pool)
                await self.insert_tick_dataframe(pool, df_1m, 'ohlctick_1mdata')
                await self.insert_tick_dataframe(pool, df_5m, 'ohlctick_5mdata')

                # Sleep until 5s after the next 1-min candle close
                period_now     = pd.Period.now('1min')
                next_execution = (
                    (period_now + 1).start_time.tz_localize('Asia/Kolkata')
                    + pd.Timedelta(seconds=5)
                )
                sleep_till = (next_execution - pd.Timestamp.now(tz='Asia/Kolkata')).total_seconds()
                if 0 < sleep_till < 61:
                    await asyncio.sleep(sleep_till)

            print(f"Market closed at {datetime.now(IST).strftime('%H:%M:%S')} IST. Exiting.")

        except Exception as e:
            print(f"Pipeline error: {e}")
            raise
        finally:
            pool.close()
            await pool.wait_closed()


if __name__ == "__main__":
    tvdata_update = TvDataUpdate()
    asyncio.run(tvdata_update.run())
```

