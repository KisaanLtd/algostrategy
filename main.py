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