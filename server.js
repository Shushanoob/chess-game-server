'use strict';

const http = require('http');
const { WebSocketServer, WebSocket } = require('ws');

const PORT = Number(process.env.PORT) || 10000;
const HOST = '0.0.0.0';
const MAX_NAME = 14;
const ROOM_TTL_MS = 10 * 60 * 1000;
const PAUSE_LIMIT_MS = 2 * 60 * 1000;

const rooms = new Map();
const queues = new Map(); // time -> Set<WebSocket>

const send = (ws, msg) => {
  if (ws && ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(msg));
};
const info = ws => ({
  name: String(ws.name || 'Игрок').slice(0, MAX_NAME),
  elo: Number.isFinite(Number(ws.elo)) ? Number(ws.elo) : 800,
});

function queueRemove(ws) {
  for (const [time, set] of queues) {
    set.delete(ws);
    if (!set.size) queues.delete(time);
  }
  ws.queuedTime = null;
}

function roomRemove(ws) {
  for (const [code, room] of rooms) if (room.host === ws) rooms.delete(code);
}

function pair(a, b, time) {
  queueRemove(a); queueRemove(b);
  a.peer = b; b.peer = a;
  a.matchId = b.matchId = Math.random().toString(36).slice(2, 10);
  a.gameEnded = b.gameEnded = false;
  a.pauseUsed = b.pauseUsed = false;
  a.pausedUntil = b.pausedUntil = 0;
  const [ca, cb] = Math.random() < 0.5 ? ['w', 'b'] : ['b', 'w'];
  send(a, { type: 'start', matchId: a.matchId, color: ca, opponent: info(b), time });
  send(b, { type: 'start', matchId: b.matchId, color: cb, opponent: info(a), time });
}

function waitingPlayer(time) {
  const set = queues.get(time);
  if (!set) return null;
  for (const ws of set) {
    if (ws.readyState === WebSocket.OPEN && !ws.peer && ws.queuedTime === time) return ws;
    set.delete(ws);
  }
  if (!set.size) queues.delete(time);
  return null;
}

function cleanupWaiting() {
  const now = Date.now();
  for (const [code, room] of rooms) {
    if (now - room.createdAt > ROOM_TTL_MS || room.host.readyState !== WebSocket.OPEN) rooms.delete(code);
  }
  for (const [time, set] of queues) {
    for (const ws of set) if (ws.readyState !== WebSocket.OPEN || ws.queuedTime !== time || ws.peer) set.delete(ws);
    if (!set.size) queues.delete(time);
  }
}

function leave(ws, notifyPeer = true) {
  queueRemove(ws);
  roomRemove(ws);
  const peer = ws.peer;
  ws.peer = null;
  if (peer && peer.peer === ws) {
    peer.peer = null;
    if (notifyPeer && !peer.gameEnded) send(peer, { type: 'left', matchId: ws.matchId });
  }
}

const httpServer = http.createServer((req, res) => {
  if (req.url === '/' || req.url === '/health') {
    res.writeHead(200, { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store' });
    return res.end(JSON.stringify({ ok: true, service: 'chess-online', time: Date.now() }));
  }
  res.writeHead(404); res.end('Not found');
});

const wss = new WebSocketServer({ server: httpServer });

wss.on('connection', ws => {
  ws.isAlive = true; ws.peer = null; ws.queuedTime = null; ws.matchId = null;
  ws.gameEnded = false; ws.pauseUsed = false; ws.pausedUntil = 0;
  ws.on('pong', () => { ws.isAlive = true; });
  ws.on('error', () => {});

  ws.on('message', raw => {
    let m; try { m = JSON.parse(raw.toString()); } catch { return send(ws, { type: 'error', text: 'Некорректное сообщение' }); }

    if (['quick', 'create', 'join'].includes(m.type)) {
      if (ws.peer) return send(ws, { type: 'error', text: 'Партия уже началась' });
      ws.name = String(m.name || 'Игрок').slice(0, MAX_NAME);
      ws.elo = Number(m.elo) || 800;
      ws.time = [3, 5, 10].includes(Number(m.time)) ? Number(m.time) : 5;
    }

    if (m.type === 'quick') {
      queueRemove(ws);
      const other = waitingPlayer(ws.time);
      if (other && other !== ws) return pair(other, ws, ws.time);
      let set = queues.get(ws.time); if (!set) queues.set(ws.time, set = new Set());
      set.add(ws); ws.queuedTime = ws.time;
      return send(ws, { type: 'waiting', requestId: m.requestId || null });
    }

    if (m.type === 'create') {
      queueRemove(ws); roomRemove(ws);
      const code = Math.random().toString(36).slice(2, 8).toUpperCase();
      rooms.set(code, { host: ws, createdAt: Date.now() });
      return send(ws, { type: 'waiting', code, requestId: m.requestId || null });
    }

    if (m.type === 'join') {
      const code = String(m.code || '').trim().toUpperCase();
      const room = rooms.get(code);
      if (!room || room.host.readyState !== WebSocket.OPEN) { rooms.delete(code); return send(ws, { type: 'error', text: 'Комната не найдена' }); }
      if (room.host === ws) return send(ws, { type: 'error', text: 'Нельзя войти в свою комнату' });
      rooms.delete(code); pair(room.host, ws, room.host.time || ws.time || 5); return;
    }

    if (m.type === 'cancel') {
      queueRemove(ws); roomRemove(ws);
      return send(ws, { type: 'cancelled', requestId: m.requestId || null });
    }

    if (!ws.peer) return;
    if (m.matchId && m.matchId !== ws.matchId) return;

    if (m.type === 'move') {
      if (ws.pausedUntil > Date.now()) return;
      const mv = m.move || {};
      return send(ws.peer, { type: 'move', matchId: ws.matchId, move: { from: Number(mv.from), to: Number(mv.to), promo: typeof mv.promo === 'string' ? mv.promo : null } });
    }

    if (m.type === 'resign' || m.type === 'timeout') {
      if (ws.gameEnded) return;
      ws.gameEnded = true; ws.peer.gameEnded = true;
      return send(ws.peer, { type: m.type, matchId: ws.matchId });
    }

    if (m.type === 'pause_request') {
      if (ws.pauseUsed || ws.pausedUntil > Date.now() || ws.peer.pausePending) return send(ws, { type: 'pause_error', text: 'Пауза сейчас недоступна.' });
      const seconds = Math.max(30, Math.min(120, Number(m.seconds) || 60));
      ws.peer.pausePending = true;
      return send(ws.peer, { type: 'pause_request', matchId: ws.matchId, seconds, from: info(ws) });
    }

    if (m.type === 'pause_response') {
      if (!ws.pausePending) return;
      ws.pausePending = false;
      ws.peer.pausePending = false;
      const accepted = !!m.accept;
      if (!accepted) return send(ws.peer, { type: 'pause_declined', matchId: ws.matchId });
      const seconds = Math.max(30, Math.min(120, Number(m.seconds) || 60));
      const until = Date.now() + seconds * 1000;
      ws.peer.pauseUsed = true;
      ws.pausedUntil = ws.peer.pausedUntil = until;
      send(ws, { type: 'pause_state', matchId: ws.matchId, until, seconds });
      return send(ws.peer, { type: 'pause_state', matchId: ws.matchId, until, seconds });
    }
  });

  ws.on('close', () => leave(ws));
});

const heartbeat = setInterval(() => {
  cleanupWaiting();
  for (const ws of wss.clients) {
    if (ws.isAlive === false) { ws.terminate(); continue; }
    ws.isAlive = false; ws.ping();
  }
}, 30000);

wss.on('close', () => clearInterval(heartbeat));
httpServer.listen(PORT, HOST, () => console.log(`Chess online server listening on ${HOST}:${PORT}`));

function shutdown() {
  clearInterval(heartbeat);
  for (const ws of wss.clients) ws.close(1001, 'Server restarting');
  wss.close(() => httpServer.close(() => process.exit(0)));
  setTimeout(() => process.exit(0), 5000).unref();
}
process.on('SIGTERM', shutdown); process.on('SIGINT', shutdown);
