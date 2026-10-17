// Servidor de Poolrooms: sirve index.html y reenvía mensajes entre jugadores por WebSocket.
const http = require('http'), fs = require('fs'), path = require('path');
const { WebSocketServer } = require('ws');

const srv = http.createServer((q, r) => {
  const u = q.url.split('?')[0];
  if (u === '/' || u === '/index.html') {
    r.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    r.end(fs.readFileSync(path.join(__dirname, 'index.html')));
  } else { r.writeHead(404); r.end('No encontrado'); }
});

const wss = new WebSocketServer({ server: srv, maxPayload: 8192 });
const clients = new Map();
let counter = 1;

const send = (ws, o) => { if (ws.readyState === 1) ws.send(JSON.stringify(o)); };
const broadcast = (o, except) => { for (const c of clients.values()) if (c !== except) send(c, o); };
const num = v => (typeof v === 'number' && isFinite(v)) ? v : 0;
const str = (v, n) => String(v == null ? '' : v).slice(0, n);

function cleanPresence(p) {
  if (!p || typeof p !== 'object') return {};
  return { x: num(p.x), z: num(p.z), yaw: num(p.yaw), lvl: p.lvl ? 1 : 0, n: str(p.n, 14), v: p.v ? 1 : 0 };
}

wss.on('connection', ws => {
  if (clients.size >= 30) { ws.close(); return; }
  const id = 'p' + (counter++).toString(36) + Math.random().toString(36).slice(2, 6);
  ws.id = id; ws.p = {}; ws.count = 0; ws.alive = true;
  send(ws, { t: 'hello', id, peers: [...clients.values()].map(c => ({ id: c.id, p: c.p })) });
  clients.set(id, ws);
  broadcast({ t: 'join', id }, ws);

  const tick = setInterval(() => { ws.count = 0; }, 1000);
  ws.on('pong', () => { ws.alive = true; });

  ws.on('message', raw => {
    if (++ws.count > 80) return;               // límite de mensajes por segundo
    let m; try { m = JSON.parse(raw); } catch (e) { return; }
    if (m.t === 'p') {
      ws.p = cleanPresence(m.p);
      broadcast({ t: 'p', id, p: ws.p }, ws);
    } else if (m.t === 'e') {
      if (m.topic === 'chat') {
        const d = m.data || {};
        broadcast({ t: 'e', id, topic: 'chat', data: { n: str(d.n, 14), t: str(d.t, 120) } });
      } else if (m.topic === 'rtc') {
        if (m.data && typeof m.data === 'object') broadcast({ t: 'e', id, topic: 'rtc', data: m.data });
      }
    }
  });

  ws.on('close', () => {
    clearInterval(tick);
    clients.delete(id);
    broadcast({ t: 'left', id });
  });
  ws.on('error', () => {});
});

// mantiene viva la conexión y limpia las muertas
setInterval(() => {
  for (const c of clients.values()) {
    if (!c.alive) { c.terminate(); continue; }
    c.alive = false; try { c.ping(); } catch (e) {}
  }
}, 25000);

srv.listen(process.env.PORT || 3000, () => console.log('Poolrooms listo'));
