const express = require('express');
const { Client } = require('ssh2');
const cron = require('node-cron');
const { WebSocketServer } = require('ws');
const http = require('http');
const path = require('path');

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

const PORT = process.env.PORT || 3000;

// ─── State ──────────────────────────────────────────────
const state = {
  replit: {
    label: 'Replit NixOS',
    status: 'disconnected',
    lastSeen: null,
    lastOutput: '',
    reconnects: 0,
    error: null,
    conn: null,
    shell: null,
  },
  ubuntu: {
    label: 'Ubuntu VM (QEMU)',
    status: 'disconnected',
    lastSeen: null,
    lastOutput: '',
    reconnects: 0,
    error: null,
    conn: null,
    shell: null,
  },
  logs: [],
};

// ─── Logging ────────────────────────────────────────────
function log(source, level, msg) {
  const entry = {
    time: new Date().toISOString(),
    source,
    level,
    msg,
  };
  state.logs.unshift(entry);
  if (state.logs.length > 500) state.logs.pop();
  console.log(`[${entry.time}] [${source}] [${level}] ${msg}`);
  broadcast({ type: 'log', entry });
}

// ─── WebSocket broadcast ─────────────────────────────────
function broadcast(data) {
  const str = JSON.stringify(data);
  wss.clients.forEach((client) => {
    if (client.readyState === 1) client.send(str);
  });
}

function broadcastState() {
  broadcast({
    type: 'state',
    replit: sanitize(state.replit),
    ubuntu: sanitize(state.ubuntu),
  });
}

function sanitize(s) {
  return {
    label: s.label,
    status: s.status,
    lastSeen: s.lastSeen,
    lastOutput: s.lastOutput,
    reconnects: s.reconnects,
    error: s.error,
  };
}

// ─── SSH Config ──────────────────────────────────────────
const REPLIT_HOST = (process.env.REPLIT_SSH_HOST || '').replace(/^-/, '');
const REPLIT_USER = process.env.REPLIT_SSH_USER || '';
const SSH_PRIVATE_KEY = (process.env.SSH_PRIVATE_KEY || '').replace(/\\n/g, '\n');
const UBUNTU_PASS = process.env.UBUNTU_SSH_PASSWORD || '';

// ─── Keep-alive ping every 30s ───────────────────────────
function sendKeepAlive(key) {
  const s = state[key];
  if (s.shell && s.status === 'connected') {
    try {
      s.shell.write('echo keepalive_ping_$(date +%s)\n');
    } catch (e) {
      log(key, 'WARN', `Keepalive write failed: ${e.message}`);
    }
  }
}

// ─── Connect to Replit ───────────────────────────────────
function connectReplit() {
  const key = 'replit';
  const s = state[key];

  if (s.conn) {
    try { s.conn.end(); } catch (_) {}
    s.conn = null;
    s.shell = null;
  }

  if (!REPLIT_HOST || !REPLIT_USER || !SSH_PRIVATE_KEY) {
    s.status = 'error';
    s.error = 'Missing SSH credentials (REPLIT_SSH_HOST / REPLIT_SSH_USER / SSH_PRIVATE_KEY)';
    log(key, 'ERROR', s.error);
    broadcastState();
    return;
  }

  s.status = 'connecting';
  s.error = null;
  broadcastState();
  log(key, 'INFO', `Connecting to ${REPLIT_HOST} as ${REPLIT_USER}...`);

  const conn = new Client();
  s.conn = conn;

  conn.on('ready', () => {
    log(key, 'INFO', 'SSH handshake OK — opening shell...');
    conn.shell({ term: 'xterm' }, (err, stream) => {
      if (err) {
        s.status = 'error';
        s.error = err.message;
        log(key, 'ERROR', `Shell open failed: ${err.message}`);
        broadcastState();
        scheduleReconnect(key, connectReplit);
        return;
      }

      s.shell = stream;
      s.status = 'connected';
      s.lastSeen = new Date().toISOString();
      s.reconnects++;
      broadcastState();
      log(key, 'INFO', 'Shell open — connection established ✓');

      stream.on('data', (data) => {
        const txt = data.toString().trim();
        if (txt) {
          s.lastOutput = txt.slice(-500);
          s.lastSeen = new Date().toISOString();
          broadcast({ type: 'output', source: key, data: txt });
        }
      });

      stream.stderr.on('data', (data) => {
        log(key, 'WARN', `stderr: ${data.toString().trim()}`);
      });

      stream.on('close', () => {
        s.status = 'disconnected';
        s.shell = null;
        broadcastState();
        log(key, 'WARN', 'Shell closed — scheduling reconnect...');
        scheduleReconnect(key, connectReplit);
      });
    });
  });

  conn.on('error', (err) => {
    s.status = 'error';
    s.error = err.message;
    s.shell = null;
    broadcastState();
    log(key, 'ERROR', `Connection error: ${err.message}`);
    scheduleReconnect(key, connectReplit);
  });

  conn.on('end', () => {
    if (s.status === 'connected') {
      s.status = 'disconnected';
      s.shell = null;
      broadcastState();
      log(key, 'WARN', 'Connection ended');
    }
  });

  conn.connect({
    host: REPLIT_HOST,
    port: 22,
    username: REPLIT_USER,
    privateKey: SSH_PRIVATE_KEY,
    keepaliveInterval: 20000,
    keepaliveCountMax: 10,
    readyTimeout: 30000,
  });
}

// ─── Connect to Ubuntu VM (through Replit jump) ──────────
function connectUbuntu() {
  const key = 'ubuntu';
  const s = state[key];
  const r = state.replit;

  if (r.status !== 'connected' || !r.conn) {
    s.status = 'waiting';
    s.error = 'Waiting for Replit connection first...';
    broadcastState();
    log(key, 'INFO', 'Waiting for Replit tunnel before connecting Ubuntu...');
    setTimeout(() => connectUbuntu(), 10000);
    return;
  }

  if (s.conn) {
    try { s.conn.end(); } catch (_) {}
    s.conn = null;
    s.shell = null;
  }

  s.status = 'connecting';
  s.error = null;
  broadcastState();
  log(key, 'INFO', 'Connecting to Ubuntu VM via Replit jump host...');

  r.conn.forwardOut('127.0.0.1', 0, '127.0.0.1', 2222, (err, stream) => {
    if (err) {
      s.status = 'error';
      s.error = `Port forward failed: ${err.message}`;
      broadcastState();
      log(key, 'ERROR', s.error);
      scheduleReconnect(key, connectUbuntu);
      return;
    }

    const conn = new Client();
    s.conn = conn;

    conn.on('ready', () => {
      log(key, 'INFO', 'Ubuntu SSH handshake OK — opening shell...');
      conn.shell({ term: 'xterm' }, (err2, sh) => {
        if (err2) {
          s.status = 'error';
          s.error = err2.message;
          broadcastState();
          log(key, 'ERROR', `Ubuntu shell failed: ${err2.message}`);
          scheduleReconnect(key, connectUbuntu);
          return;
        }

        s.shell = sh;
        s.status = 'connected';
        s.lastSeen = new Date().toISOString();
        s.reconnects++;
        broadcastState();
        log(key, 'INFO', 'Ubuntu shell open — connection established ✓');

        sh.on('data', (data) => {
          const txt = data.toString().trim();
          if (txt) {
            s.lastOutput = txt.slice(-500);
            s.lastSeen = new Date().toISOString();
            broadcast({ type: 'output', source: key, data: txt });
          }
        });

        sh.stderr.on('data', (data) => {
          log(key, 'WARN', `ubuntu stderr: ${data.toString().trim()}`);
        });

        sh.on('close', () => {
          s.status = 'disconnected';
          s.shell = null;
          broadcastState();
          log(key, 'WARN', 'Ubuntu shell closed — scheduling reconnect...');
          scheduleReconnect(key, connectUbuntu);
        });
      });
    });

    conn.on('error', (err3) => {
      s.status = 'error';
      s.error = err3.message;
      s.shell = null;
      broadcastState();
      log(key, 'ERROR', `Ubuntu connection error: ${err3.message}`);
      scheduleReconnect(key, connectUbuntu);
    });

    conn.connect({
      sock: stream,
      username: 'ubuntu',
      password: UBUNTU_PASS,
      keepaliveInterval: 20000,
      keepaliveCountMax: 10,
      readyTimeout: 30000,
    });
  });
}

// ─── Reconnect scheduler ─────────────────────────────────
const reconnectTimers = {};
function scheduleReconnect(key, fn) {
  if (reconnectTimers[key]) clearTimeout(reconnectTimers[key]);
  const delay = 15000;
  log(key, 'INFO', `Reconnecting in ${delay / 1000}s...`);
  reconnectTimers[key] = setTimeout(fn, delay);
}

// ─── Keepalive cron (every 30s) ──────────────────────────
cron.schedule('*/30 * * * * *', () => {
  sendKeepAlive('replit');
  sendKeepAlive('ubuntu');
});

// ─── Web: serve dashboard ────────────────────────────────
app.use(express.static(path.join(__dirname, 'public')));
app.use(express.json());

app.get('/api/status', (req, res) => {
  res.json({
    replit: sanitize(state.replit),
    ubuntu: sanitize(state.ubuntu),
    logs: state.logs.slice(0, 100),
  });
});

app.post('/api/command', (req, res) => {
  const { target, cmd } = req.body;
  const s = state[target];
  if (!s || !s.shell || s.status !== 'connected') {
    return res.status(400).json({ error: 'Target not connected' });
  }
  try {
    s.shell.write(cmd + '\n');
    log(target, 'CMD', `> ${cmd}`);
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.get('/health', (req, res) => {
  res.json({ status: 'ok', uptime: process.uptime() });
});

// ─── WebSocket ───────────────────────────────────────────
wss.on('connection', (ws) => {
  ws.send(JSON.stringify({
    type: 'init',
    replit: sanitize(state.replit),
    ubuntu: sanitize(state.ubuntu),
    logs: state.logs.slice(0, 100),
  }));

  ws.on('message', (raw) => {
    try {
      const msg = JSON.parse(raw);
      if (msg.type === 'command') {
        const s = state[msg.target];
        if (s?.shell && s.status === 'connected') {
          s.shell.write(msg.cmd + '\n');
          log(msg.target, 'CMD', `> ${msg.cmd}`);
        }
      }
    } catch (_) {}
  });
});

// ─── Start ───────────────────────────────────────────────
server.listen(PORT, () => {
  log('system', 'INFO', `Server listening on port ${PORT}`);
  log('system', 'INFO', `Dashboard: http://localhost:${PORT}`);
  setTimeout(() => {
    connectReplit();
    setTimeout(() => connectUbuntu(), 8000);
  }, 2000);
});
