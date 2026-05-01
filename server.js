const express = require('express');
const { Client } = require('ssh2');
const cron = require('node-cron');
const { WebSocketServer } = require('ws');
const http = require('http');
const path = require('path');
const net = require('net');

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

const PORT = process.env.PORT || 3000;

// ─── State ────────────────────────────────────────────────
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
  startTime: Date.now(),
};

// ─── Logging ──────────────────────────────────────────────
function log(source, level, msg) {
  const entry = { time: new Date().toISOString(), source, level, msg };
  state.logs.unshift(entry);
  if (state.logs.length > 500) state.logs.pop();
  console.log(`[${entry.time}] [${source}] [${level}] ${msg}`);
  broadcast({ type: 'log', entry });
}

function broadcast(data) {
  const str = JSON.stringify(data);
  wss.clients.forEach((c) => { if (c.readyState === 1) c.send(str); });
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

function broadcastState() {
  broadcast({ type: 'state', replit: sanitize(state.replit), ubuntu: sanitize(state.ubuntu) });
}

// ─── SSH Config ───────────────────────────────────────────
const REPLIT_HOST = (process.env.REPLIT_SSH_HOST || '').replace(/^-/, '');
const REPLIT_USER = process.env.REPLIT_SSH_USER || '';
const UBUNTU_PASS = process.env.UBUNTU_SSH_PASSWORD || '';

function parsePrivateKey(raw) {
  if (!raw) return '';
  let key = raw.replace(/\\n/g, '\n');
  if (!key.includes('\n') || key.split('\n').length < 3) {
    const headerMatch = key.match(/(-----BEGIN [^-]+ KEY-----)/);
    const footerMatch = key.match(/(-----END [^-]+ KEY-----)/);
    if (headerMatch && footerMatch) {
      const header = headerMatch[1];
      const footer = footerMatch[1];
      let body = key.replace(header, '').replace(footer, '').trim().replace(/\s+/g, '');
      const lines = body.match(/.{1,70}/g) || [];
      key = `${header}\n${lines.join('\n')}\n${footer}\n`;
    }
  }
  return key;
}

const SSH_PRIVATE_KEY = parsePrivateKey(process.env.SSH_PRIVATE_KEY);

// ─── Keepalive ────────────────────────────────────────────
function sendKeepAlive(key) {
  const s = state[key];
  if (s.shell && s.status === 'connected') {
    try {
      s.shell.write('echo __keepalive_$(date +%s)__\n');
    } catch (e) {
      log(key, 'WARN', `Keepalive failed: ${e.message}`);
    }
  }
}

// ─── Reconnect scheduler ──────────────────────────────────
const reconnectTimers = {};
function scheduleReconnect(key, fn, delay = 15000) {
  if (reconnectTimers[key]) clearTimeout(reconnectTimers[key]);
  log(key, 'INFO', `Reconnecting in ${delay / 1000}s...`);
  reconnectTimers[key] = setTimeout(fn, delay);
}

// ─── Connect to Replit ────────────────────────────────────
function connectReplit() {
  const s = state.replit;
  if (s.conn) { try { s.conn.end(); } catch (_) {} s.conn = null; s.shell = null; }

  if (!REPLIT_HOST || !REPLIT_USER || !SSH_PRIVATE_KEY) {
    s.status = 'error';
    s.error = 'Missing REPLIT_SSH_HOST / REPLIT_SSH_USER / SSH_PRIVATE_KEY';
    log('replit', 'ERROR', s.error);
    broadcastState();
    return;
  }

  s.status = 'connecting';
  s.error = null;
  broadcastState();
  log('replit', 'INFO', `Connecting to ${REPLIT_HOST} as ${REPLIT_USER}...`);

  const conn = new Client();
  s.conn = conn;

  conn.on('ready', () => {
    log('replit', 'INFO', 'SSH ready — opening shell...');
    conn.shell({ term: 'xterm' }, (err, stream) => {
      if (err) {
        s.status = 'error'; s.error = err.message;
        broadcastState();
        log('replit', 'ERROR', `Shell failed: ${err.message}`);
        scheduleReconnect('replit', connectReplit);
        return;
      }
      s.shell = stream;
      s.status = 'connected';
      s.lastSeen = new Date().toISOString();
      s.reconnects++;
      broadcastState();
      log('replit', 'INFO', '✅ Replit connected and shell open');

      stream.on('data', (data) => {
        const txt = data.toString();
        if (txt.trim()) {
          s.lastOutput = txt.slice(-500);
          s.lastSeen = new Date().toISOString();
          if (!txt.includes('__keepalive_')) {
            broadcast({ type: 'output', source: 'replit', data: txt });
          }
        }
      });

      stream.stderr.on('data', (d) => log('replit', 'WARN', `stderr: ${d.toString().trim()}`));

      stream.on('close', () => {
        s.status = 'disconnected'; s.shell = null; s.conn = null;
        broadcastState();
        log('replit', 'WARN', 'Shell closed — reconnecting...');
        scheduleReconnect('replit', connectReplit);
      });

      // After Replit is ready, connect Ubuntu
      setTimeout(() => connectUbuntu(), 3000);
    });
  });

  conn.on('error', (err) => {
    s.status = 'error'; s.error = err.message; s.shell = null;
    broadcastState();
    log('replit', 'ERROR', `Connection error: ${err.message}`);
    scheduleReconnect('replit', connectReplit);
  });

  conn.on('end', () => {
    if (s.status === 'connected') {
      s.status = 'disconnected'; s.shell = null;
      broadcastState();
      log('replit', 'WARN', 'Connection ended');
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

// ─── Connect to Ubuntu VM via forwardOut ──────────────────
function connectUbuntu() {
  const s = state.ubuntu;
  const r = state.replit;

  if (s.conn) { try { s.conn.end(); } catch (_) {} s.conn = null; s.shell = null; }

  if (r.status !== 'connected' || !r.conn) {
    s.status = 'waiting';
    s.error = 'Waiting for Replit connection...';
    broadcastState();
    log('ubuntu', 'INFO', 'Replit not ready yet, will retry in 10s...');
    scheduleReconnect('ubuntu', connectUbuntu, 10000);
    return;
  }

  s.status = 'connecting';
  s.error = null;
  broadcastState();
  log('ubuntu', 'INFO', 'Opening port-forward tunnel to Ubuntu VM (localhost:2222)...');

  // Use forwardOut to create a real TCP tunnel through the SSH connection
  // This is the correct way - same as "ssh -L local:remote" but done programmatically
  r.conn.forwardOut('127.0.0.1', 0, '127.0.0.1', 2222, (err, stream) => {
    if (err) {
      s.status = 'error';
      s.error = `Port forward failed: ${err.message}`;
      broadcastState();
      log('ubuntu', 'ERROR', s.error);
      scheduleReconnect('ubuntu', connectUbuntu, 20000);
      return;
    }

    log('ubuntu', 'INFO', 'Tunnel open — authenticating to Ubuntu VM...');
    const ubuntuConn = new Client();
    s.conn = ubuntuConn;

    ubuntuConn.on('ready', () => {
      log('ubuntu', 'INFO', 'Ubuntu SSH ready — opening shell...');
      ubuntuConn.shell({ term: 'xterm' }, (err2, sh) => {
        if (err2) {
          s.status = 'error'; s.error = err2.message;
          broadcastState();
          log('ubuntu', 'ERROR', `Ubuntu shell failed: ${err2.message}`);
          scheduleReconnect('ubuntu', connectUbuntu, 20000);
          return;
        }

        s.shell = sh;
        s.status = 'connected';
        s.lastSeen = new Date().toISOString();
        s.reconnects++;
        broadcastState();
        log('ubuntu', 'INFO', '✅ Ubuntu VM connected and shell open');

        sh.on('data', (data) => {
          const txt = data.toString();
          if (txt.trim()) {
            s.lastOutput = txt.slice(-500);
            s.lastSeen = new Date().toISOString();
            if (!txt.includes('__keepalive_')) {
              broadcast({ type: 'output', source: 'ubuntu', data: txt });
            }
          }
        });

        sh.stderr.on('data', (d) => log('ubuntu', 'WARN', `stderr: ${d.toString().trim()}`));

        sh.on('close', () => {
          s.status = 'disconnected'; s.shell = null; s.conn = null;
          broadcastState();
          log('ubuntu', 'WARN', 'Ubuntu shell closed — reconnecting...');
          scheduleReconnect('ubuntu', connectUbuntu, 15000);
        });
      });
    });

    ubuntuConn.on('error', (err3) => {
      s.status = 'error'; s.error = err3.message; s.shell = null;
      broadcastState();
      log('ubuntu', 'ERROR', `Ubuntu error: ${err3.message}`);
      scheduleReconnect('ubuntu', connectUbuntu, 20000);
    });

    ubuntuConn.connect({
      sock: stream,           // The tunnel stream — key difference vs naive approach
      username: 'ubuntu',
      password: UBUNTU_PASS,
      keepaliveInterval: 20000,
      keepaliveCountMax: 10,
      readyTimeout: 30000,
    });
  });
}

// ─── Cron: keepalive every 30s ────────────────────────────
cron.schedule('*/30 * * * * *', () => {
  sendKeepAlive('replit');
  sendKeepAlive('ubuntu');
});

// ─── Cron: health check every 60s ────────────────────────
cron.schedule('*/60 * * * * *', () => {
  if (state.replit.status !== 'connected') {
    log('system', 'WARN', 'Replit not connected — forcing reconnect...');
    connectReplit();
  } else if (state.ubuntu.status !== 'connected' && state.ubuntu.status !== 'connecting') {
    log('system', 'WARN', 'Ubuntu not connected — forcing reconnect...');
    connectUbuntu();
  }
});

// ─── REST API ─────────────────────────────────────────────
app.use(express.static(path.join(__dirname, 'public')));
app.use(express.json());

app.get('/api/status', (req, res) => {
  res.json({
    uptime: Math.floor((Date.now() - state.startTime) / 1000),
    replit: sanitize(state.replit),
    ubuntu: sanitize(state.ubuntu),
    logs: state.logs.slice(0, 100),
  });
});

app.post('/api/command', (req, res) => {
  const { target, cmd } = req.body;
  const s = state[target];
  if (!s || !s.shell || s.status !== 'connected') {
    return res.status(400).json({ error: `${target} is not connected` });
  }
  try {
    s.shell.write(cmd + '\n');
    log(target, 'CMD', `> ${cmd}`);
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.get('/health', (_req, res) => {
  res.json({
    ok: true,
    uptime: Math.floor((Date.now() - state.startTime) / 1000),
    replit: state.replit.status,
    ubuntu: state.ubuntu.status,
  });
});

// ─── WebSocket ────────────────────────────────────────────
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

// ─── Start ────────────────────────────────────────────────
server.listen(PORT, () => {
  log('system', 'INFO', `Server started on port ${PORT}`);
  log('system', 'INFO', `Replit host: ${REPLIT_HOST || '(not set)'}`);
  setTimeout(connectReplit, 2000);
});
