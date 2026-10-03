'use strict';

const http = require('http');
const { WebSocketServer, WebSocket } = require('ws');

const PORT = Number(process.env.PORT) || 10000;
const HOST = '0.0.0.0';
const MAX_NAME = 14;
const ROOM_TTL_MS = 10 * 60 * 1000;
const RECONNECT_GRACE_MS = 60 * 1000;
const PAUSE_LIMIT_MS = 2 * 60 * 1000;
const CHAT_LIMIT = 50;
const CHAT_MAX_CHARS = 300;

const rooms = new Map();
const queues = new Map();
const matches = new Map();

const send = (ws, msg) => {
  if (ws && ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(msg));
};
const info = ws => ({
  name: String(ws?.name || 'Игрок').slice(0, MAX_NAME),
  elo: Number.isFinite(Number(ws?.elo)) ? Number(ws.elo) : 800,
});
const token = () => `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 12)}`;
const matchId = () => Math.random().toString(36).slice(2, 10);

function queueRemove(ws) {
  for (const [time, set] of queues) {
    set.delete(ws);
    if (!set.size) queues.delete(time);
  }
  ws.queuedTime = null; ws.queueRequestId = null;
}
function roomRemove(ws) {
  for (const [code, room] of rooms) if (room.host === ws) rooms.delete(code);
}
function activeMatchFor(ws) {
  return ws.matchId ? matches.get(ws.matchId) : null;
}
function playerFor(match, ws) {
  return Object.values(match.players).find(p => p.ws === ws) || null;
}
function playerByToken(match, t) { return match.players[t] || null; }

function currentClock(match) {
  const clocks = { ...match.clocks };
  if (match.pausedUntil > Date.now() || match.frozenUntil > Date.now()) return clocks;
  if (match.ended) return clocks;
  clocks[match.turn] = Math.max(0, clocks[match.turn] - (Date.now() - match.clockAt));
  return clocks;
}
function settleClock(match) {
  if (match.pausedUntil > Date.now() || match.frozenUntil > Date.now() || match.ended) return;
  const now = Date.now();
  match.clocks[match.turn] = Math.max(0, match.clocks[match.turn] - (now - match.clockAt));
  match.clockAt = now;
}
function playerSnapshot(p) {
  return { token: p.token, color: p.color, name: p.name, elo: p.elo, connected: !!p.ws };
}
function syncPayload(match, p) {
  return {
    type: 'sync',
    matchId: match.id,
    color: p.color,
    opponent: playerSnapshot(match.players[p.opponentToken]),
    time: match.time,
    moves: match.moves,
    clocks: currentClock(match),
    turn: match.turn,
    pausedUntil: match.pausedUntil || 0,
    pauseUsed: p.pauseUsed,
    opponentConnected: !!match.players[p.opponentToken].ws,
    frozenUntil: match.frozenUntil || 0,
    chat: match.chat.slice(-CHAT_LIMIT),
    reconnectLeft: match.players[p.opponentToken].disconnectedAt ? Math.max(0, RECONNECT_GRACE_MS - (Date.now() - match.players[p.opponentToken].disconnectedAt)) : 0,
  };
}

function endMatch(match, winnerToken, reason) {
  if (!match || match.ended) return;
  settleClock(match);
  match.ended = true; match.winnerToken = winnerToken || null; match.endReason = reason;
  if (match.disconnectTimers) for (const t of Object.values(match.disconnectTimers)) clearTimeout(t);
  const winner = winnerToken ? match.players[winnerToken] : null;
  for (const p of Object.values(match.players)) {
    if (p.ws) send(p.ws, { type: 'match_end', matchId: match.id, result: winner ? (p.token === winner.token ? 'win' : 'loss') : 'draw', reason, clocks: match.clocks });
  }
  setTimeout(() => matches.delete(match.id), 2 * 60 * 1000).unref();
}

function pair(a, b, time) {
  const requestA = a.queueRequestId || null, requestB = b.queueRequestId || null;
  queueRemove(a); queueRemove(b);
  const id = matchId();
  const [ca, cb] = Math.random() < 0.5 ? ['w', 'b'] : ['b', 'w'];
  const base = {
    id, time: Number(time) || 5, moves: [], turn: 'w', clocks: { w: (Number(time) || 5) * 60000, b: (Number(time) || 5) * 60000 },
    clockAt: Date.now(), pausedUntil: 0, frozenUntil: 0, ended: false, pausePending: null, disconnectTimers: {},
    players: {},
    chat: [],
  };
  const pa = { token: token(), ws: a, color: ca, name: String(a.name || 'Игрок').slice(0, MAX_NAME), elo: Number(a.elo) || 800, opponentToken: null, pauseUsed: false, disconnectedAt: 0 };
  const pb = { token: token(), ws: b, color: cb, name: String(b.name || 'Игрок').slice(0, MAX_NAME), elo: Number(b.elo) || 800, opponentToken: pa.token, pauseUsed: false, disconnectedAt: 0 };
  pa.opponentToken = pb.token;
  base.players[pa.token] = pa; base.players[pb.token] = pb;
  matches.set(id, base);
  a.matchId = b.matchId = id; a.matchToken = pa.token; b.matchToken = pb.token; a.peer = b; b.peer = a;
  send(a, { type: 'start', matchId: id, token: pa.token, color: ca, opponent: { name: pb.name, elo: pb.elo }, time: base.time, clocks: base.clocks });
  send(b, { type: 'start', matchId: id, token: pb.token, color: cb, opponent: { name: pa.name, elo: pa.elo }, time: base.time, clocks: base.clocks });
}

function waitingPlayer(time) {
  const set = queues.get(time);
  if (!set) return null;
  for (const ws of set) {
    if (ws.readyState === WebSocket.OPEN && !ws.peer && ws.queuedTime === time && ws.queueRequestId) return ws;
    set.delete(ws);
  }
  if (!set.size) queues.delete(time);
  return null;
}

function disconnectPlayer(ws) {
  const match = activeMatchFor(ws);
  if (!match || match.ended) return;
  const p = playerFor(match, ws);
  if (!p) return;
  p.ws = null;
  p.disconnectedAt = Date.now();
  match.frozenUntil = Math.max(match.frozenUntil || 0, Date.now() + RECONNECT_GRACE_MS);
  ws.peer = null;
  const opponent = match.players[p.opponentToken];
  if (opponent?.ws) send(opponent.ws, { type: 'opponent_disconnected', matchId: match.id, seconds: 60 });
  clearTimeout(match.disconnectTimers[p.token]);
  match.disconnectTimers[p.token] = setTimeout(() => {
    const latest = matches.get(match.id);
    const stillAway = latest && latest.players[p.token] && !latest.players[p.token].ws && !latest.ended;
    if (!stillAway) return;
    endMatch(latest, opponent?.token || null, 'disconnect_timeout');
  }, RECONNECT_GRACE_MS);
}

function cleanupWaiting() {
  const now = Date.now();
  for (const [code, room] of rooms) if (now - room.createdAt > ROOM_TTL_MS || room.host.readyState !== WebSocket.OPEN) rooms.delete(code);
  for (const [time, set] of queues) {
    for (const ws of set) if (ws.readyState !== WebSocket.OPEN || ws.queuedTime !== time || ws.peer) set.delete(ws);
    if (!set.size) queues.delete(time);
  }
  for (const [id, match] of matches) {
    if (match.ended && now - match.clockAt > 5 * 60 * 1000) matches.delete(id);
  }
}

function cancelWaiting(ws) {
  queueRemove(ws); roomRemove(ws);
  if (ws.matchId) return;
  send(ws, { type: 'cancelled' });
}

const httpServer = http.createServer((req, res) => {
  if (req.url === '/' || req.url === '/health') {
    res.writeHead(200, { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store' });
    return res.end(JSON.stringify({ ok: true, service: 'chess-online', reconnectGrace: 60, time: Date.now() }));
  }
  res.writeHead(404); res.end('Not found');
});
const wss = new WebSocketServer({ server: httpServer });

wss.on('connection', ws => {
  ws.isAlive = true; ws.peer = null; ws.queuedTime = null; ws.matchId = null; ws.matchToken = null;
  ws.name = 'Игрок'; ws.elo = 800; ws.queueRequestId = null; ws.on('pong', () => { ws.isAlive = true; }); ws.on('error', () => {});

  ws.on('message', raw => {
    let m; try { m = JSON.parse(raw.toString()); } catch { return send(ws, { type: 'error', text: 'Некорректное сообщение' }); }

    if (m.type === 'reconnect') {
      const match = matches.get(String(m.matchId || ''));
      const p = match && playerByToken(match, String(m.token || ''));
      if (!match || !p) return send(ws, { type: 'reconnect_failed', text: 'Время на переподключение истекло.' });
      if (match.ended) return send(ws, { type: 'match_end', matchId: match.id, result: match.winnerToken === p.token ? 'win' : match.winnerToken ? 'loss' : 'draw', reason: match.endReason || 'server' });
      clearTimeout(match.disconnectTimers[p.token]);
      p.ws = ws; p.disconnectedAt = 0; ws.matchId = match.id; ws.matchToken = p.token; ws.name = p.name; ws.elo = p.elo;
      const opponent = match.players[p.opponentToken]; ws.peer = opponent.ws || null;
      if (opponent.ws) opponent.ws.peer = ws;
      if (Object.values(match.players).every(x => x.ws)) { match.frozenUntil = 0; match.clockAt = Date.now(); }
      send(ws, syncPayload(match, p));
      if (opponent.ws) send(opponent.ws, { type: 'opponent_reconnected', matchId: match.id });
      return;
    }

    if (['quick', 'create', 'join'].includes(m.type)) {
      if (ws.matchId && activeMatchFor(ws) && !activeMatchFor(ws).ended) return send(ws, { type: 'error', text: 'Партия уже началась' });
      ws.name = String(m.name || 'Игрок').slice(0, MAX_NAME); ws.elo = Number(m.elo) || 800; ws.time = [3, 5, 10].includes(Number(m.time)) ? Number(m.time) : 5;
    }
    if (m.type === 'quick') {
      const requestId = String(m.requestId || token());
      queueRemove(ws);
      const other = waitingPlayer(ws.time);
      if (other && other !== ws) {
        // Preserve each player's own request id so stale UI messages cannot open a different match.
        return pair(other, ws, ws.time);
      }
      let set = queues.get(ws.time); if (!set) queues.set(ws.time, set = new Set());
      set.add(ws); ws.queuedTime = ws.time; ws.queueRequestId = requestId;
      return send(ws, { type: 'waiting', requestId });
    }
    if (m.type === 'create') {
      queueRemove(ws); roomRemove(ws); const requestId = String(m.requestId || token()); ws.queueRequestId = requestId; const code = Math.random().toString(36).slice(2, 8).toUpperCase();
      rooms.set(code, { host: ws, createdAt: Date.now(), requestId }); return send(ws, { type: 'waiting', code, requestId });
    }
    if (m.type === 'join') {
      const code = String(m.code || '').trim().toUpperCase(); const room = rooms.get(code);
      if (!room || room.host.readyState !== WebSocket.OPEN) { rooms.delete(code); return send(ws, { type: 'error', text: 'Комната не найдена' }); }
      rooms.delete(code); ws.queueRequestId = String(m.requestId || token()); room.host.queueRequestId = room.requestId || room.host.queueRequestId || null; return pair(room.host, ws, room.host.time || ws.time || 5);
    }
    if (m.type === 'cancel') {
      const id = String(m.requestId || '');
      if (ws.queueRequestId && id && ws.queueRequestId !== id) return;
      cancelWaiting(ws);
      return send(ws, { type: 'cancelled', requestId: id || null });
    }

    const match = activeMatchFor(ws);
    if (!match || match.ended) return;
    const p = playerFor(match, ws);
    if (!p) return;
    if (m.matchId && m.matchId !== match.id) return;
    const opponent = match.players[p.opponentToken];

    if (m.type === 'chat') {
      const text = String(m.text || '').replace(/\s+/g, ' ').trim().slice(0, CHAT_MAX_CHARS);
      if (!text) return;
      const message = { id: token(), name: p.name, elo: p.elo, text, at: Date.now() };
      match.chat.push(message);
      if (match.chat.length > CHAT_LIMIT) match.chat.splice(0, match.chat.length - CHAT_LIMIT);
      for (const pl of Object.values(match.players)) if (pl.ws) send(pl.ws, { type: 'chat', matchId: match.id, message });
      return;
    }
    if (m.type === 'move') {
      if (match.pausedUntil > Date.now() || match.turn !== p.color) return;
      settleClock(match);
      if (match.clocks[p.color] <= 0) return endMatch(match, opponent.token, 'timeout');
      const mv = m.move || {};
      match.moves.push({ from: Number(mv.from), to: Number(mv.to), promo: typeof mv.promo === 'string' ? mv.promo : null });
      match.turn = match.turn === 'w' ? 'b' : 'w'; match.clockAt = Date.now();
      const clocks = currentClock(match);
      if (p.ws) send(p.ws, { type: 'move_ack', matchId: match.id, move: match.moves[match.moves.length - 1], clocks, turn: match.turn });
      if (opponent.ws) send(opponent.ws, { type: 'move', matchId: match.id, move: match.moves[match.moves.length - 1], clocks, turn: match.turn });
      return;
    }
    if (m.type === 'resign') return endMatch(match, opponent.token, 'resign');
    if (m.type === 'timeout') { settleClock(match); return endMatch(match, opponent.token, 'timeout'); }

    if (m.type === 'pause_request') {
      if (p.pauseUsed || match.pausedUntil > Date.now() || match.pausePending) return send(ws, { type: 'pause_error', text: 'Пауза сейчас недоступна.' });
      const seconds = Math.max(30, Math.min(120, Number(m.seconds) || 60));
      match.pausePending = { token: p.token, seconds };
      if (opponent.ws) send(opponent.ws, { type: 'pause_request', matchId: match.id, seconds, from: info(ws) });
      else { match.pausePending = null; send(ws, { type: 'pause_error', text: 'Соперник сейчас отключён.' }); }
      return;
    }
    if (m.type === 'pause_response') {
      if (!match.pausePending || match.pausePending.token !== opponent.token) return;
      const accepted = !!m.accept; const seconds = match.pausePending.seconds; match.pausePending = null;
      if (!accepted) return opponent.ws && send(opponent.ws, { type: 'pause_declined', matchId: match.id });
      settleClock(match); const until = Date.now() + seconds * 1000; match.pausedUntil = until; p.pauseUsed = true; opponent.pauseUsed = true; match.frozenUntil = 0; match.clockAt = until;
      const payload = { type: 'pause_state', matchId: match.id, until, seconds };
      send(ws, payload); send(opponent.ws, payload); return;
    }
  });

  ws.on('close', () => {
    queueRemove(ws); roomRemove(ws);
    if (ws.matchId) disconnectPlayer(ws);
  });
});

const heartbeat = setInterval(() => {
  cleanupWaiting();
  for (const ws of wss.clients) { if (ws.isAlive === false) { ws.terminate(); continue; } ws.isAlive = false; ws.ping(); }
}, 30000);
wss.on('close', () => clearInterval(heartbeat));
httpServer.listen(PORT, HOST, () => console.log(`Chess online server listening on ${HOST}:${PORT}`));
function shutdown() { clearInterval(heartbeat); for (const ws of wss.clients) ws.close(1001, 'Server restarting'); wss.close(() => httpServer.close(() => process.exit(0))); setTimeout(() => process.exit(0), 5000).unref(); }
process.on('SIGTERM', shutdown); process.on('SIGINT', shutdown);
