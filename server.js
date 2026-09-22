const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const STUDENT_PASS = process.env.STUDENT_PASS || 'Pass@123';
const STAFF_PASS = process.env.STAFF_PASS || 'Staff@123';   // change this!
const SAMPLE_RATE = 16000;
const MAX_SECONDS = 60;      // max one transmission
const MAX_HISTORY = 200;     // keep last 200 recordings

const DATA = path.join(__dirname, 'data');
const REC = path.join(DATA, 'rec');
const META = path.join(DATA, 'history.json');
fs.mkdirSync(REC, { recursive: true });

let history = [];
try { history = JSON.parse(fs.readFileSync(META, 'utf8')); } catch (e) {}
const saveMeta = () => fs.writeFile(META, JSON.stringify(history), () => {});

// ---------- HTTP (serves page + recordings) ----------
function serveFile(req, res, file, type) {
  fs.stat(file, (err, st) => {
    if (err) { res.writeHead(404); return res.end('Not found'); }
    const range = req.headers.range;
    if (range) {
      const m = /bytes=(\d*)-(\d*)/.exec(range);
      let start = m && m[1] ? parseInt(m[1], 10) : 0;
      let end = m && m[2] ? parseInt(m[2], 10) : st.size - 1;
      end = Math.min(end, st.size - 1);
      if (start >= st.size || end < start) {
        res.writeHead(416, { 'Content-Range': `bytes */${st.size}` });
        return res.end();
      }
      res.writeHead(206, {
        'Content-Type': type,
        'Content-Range': `bytes ${start}-${end}/${st.size}`,
        'Accept-Ranges': 'bytes',
        'Content-Length': end - start + 1
      });
      fs.createReadStream(file, { start, end }).pipe(res);
    } else {
      res.writeHead(200, { 'Content-Type': type, 'Content-Length': st.size, 'Accept-Ranges': 'bytes' });
      fs.createReadStream(file).pipe(res);
    }
  });
}

const server = http.createServer((req, res) => {
  const url = req.url.split('?')[0];
  if (url === '/' || url === '/index.html') {
    return serveFile(req, res, path.join(__dirname, 'index.html'), 'text/html; charset=utf-8');
  }
  const m = /^\/rec\/([\w-]+)\.wav$/.exec(url);
  if (m) return serveFile(req, res, path.join(REC, m[1] + '.wav'), 'audio/wav');
  if (url === '/manifest.json') return serveFile(req, res, path.join(__dirname, 'manifest.json'), 'application/manifest+json');
  if (url === '/sw.js') return serveFile(req, res, path.join(__dirname, 'sw.js'), 'application/javascript');
  if (url === '/icon-192.png') return serveFile(req, res, path.join(__dirname, 'icon-192.png'), 'image/png');
  if (url === '/icon-512.png') return serveFile(req, res, path.join(__dirname, 'icon-512.png'), 'image/png');
  res.writeHead(404); res.end('Not found');
});

// ---------- WAV writer ----------
function makeWav(pcm) {
  const h = Buffer.alloc(44);
  h.write('RIFF', 0); h.writeUInt32LE(36 + pcm.length, 4); h.write('WAVE', 8);
  h.write('fmt ', 12); h.writeUInt32LE(16, 16); h.writeUInt16LE(1, 20); h.writeUInt16LE(1, 22);
  h.writeUInt32LE(SAMPLE_RATE, 24); h.writeUInt32LE(SAMPLE_RATE * 2, 28);
  h.writeUInt16LE(2, 32); h.writeUInt16LE(16, 34);
  h.write('data', 36); h.writeUInt32LE(pcm.length, 40);
  return Buffer.concat([h, pcm]);
}

// ---------- WebSocket radio ----------
const wss = new WebSocketServer({ server, maxPayload: 1 << 20 });
let nextCid = 1;
let speaker = null; // { ws, user, chunks, bytes, timer }

const clean = (s) => String(s || '').trim().slice(0, 60);
const sendJSON = (ws, obj) => { if (ws.readyState === 1) ws.send(JSON.stringify(obj)); };
const broadcast = (obj) => wss.clients.forEach((c) => { if (c.user) sendJSON(c, obj); });
const onlineCount = () => [...wss.clients].filter((c) => c.user).length;
const broadcastOnline = () => broadcast({ type: 'online', count: onlineCount() });

function endSpeaker() {
  if (!speaker) return;
  const s = speaker;
  speaker = null;
  clearTimeout(s.timer);

  const pcm = Buffer.concat(s.chunks);
  const duration = pcm.length / 2 / SAMPLE_RATE;
  if (duration >= 0.4) {
    const id = Date.now() + '-' + Math.random().toString(36).slice(2, 8);
    fs.writeFileSync(path.join(REC, id + '.wav'), makeWav(pcm));
    const log = { id, user: s.user, ts: Date.now(), duration: Math.round(duration * 10) / 10 };
    history.push(log);
    while (history.length > MAX_HISTORY) {
      const old = history.shift();
      fs.unlink(path.join(REC, old.id + '.wav'), () => {});
    }
    saveMeta();
    broadcast({ type: 'new_log', log });
  }
  broadcast({ type: 'idle' });
}

wss.on('connection', (ws) => {
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });

  ws.on('message', (data, isBinary) => {
    // Audio frames
    if (isBinary) {
      if (!speaker || speaker.ws !== ws || data.length % 2 !== 0) return;
      speaker.chunks.push(Buffer.from(data));
      speaker.bytes += data.length;
      wss.clients.forEach((c) => {
        if (c !== ws && c.user && c.readyState === 1) c.send(data, { binary: true });
      });
      return;
    }

    let msg;
    try { msg = JSON.parse(data.toString()); } catch (e) { return; }

    if (msg.type === 'login') {
      let user;
      if (msg.role === 'lecturer') {
        if (msg.pass !== STAFF_PASS) return sendJSON(ws, { type: 'error', msg: 'Wrong lecturer password' });
        user = { role: 'lecturer', name: clean(msg.name), subject: clean(msg.subject), staffId: clean(msg.staffId) };
        if (!user.name || !user.subject || !user.staffId) return sendJSON(ws, { type: 'error', msg: 'Fill all fields' });
      } else {
        if (msg.pass !== STUDENT_PASS) return sendJSON(ws, { type: 'error', msg: 'Wrong password' });
        user = { role: 'student', name: clean(msg.name), uucms: clean(msg.uucms), uClass: clean(msg.uClass) };
        if (!user.name || !user.uucms || !user.uClass) return sendJSON(ws, { type: 'error', msg: 'Fill all fields' });
      }
      user.cid = nextCid++;
      ws.user = user;
      sendJSON(ws, {
        type: 'welcome',
        myId: user.cid,
        history,
        speaker: speaker ? speaker.user : null,
        online: onlineCount()
      });
      broadcastOnline();
      return;
    }

    if (!ws.user) return;

    if (msg.type === 'ptt_start') {
      if (speaker && speaker.ws !== ws) return sendJSON(ws, { type: 'denied' });
      if (!speaker) {
        speaker = { ws, user: ws.user, chunks: [], bytes: 0, timer: setTimeout(endSpeaker, MAX_SECONDS * 1000) };
        broadcast({ type: 'speaking', user: ws.user });
      }
      sendJSON(ws, { type: 'granted' });
    } else if (msg.type === 'ptt_end') {
      if (speaker && speaker.ws === ws) endSpeaker();
    } else if (msg.type === 'delete_log') {
      // Any signed-in user (student or lecturer) can delete a recording
      const id = String(msg.id || '');
      if (!/^[\w-]+$/.test(id)) return;
      const idx = history.findIndex((l) => l.id === id);
      if (idx === -1) return;
      history.splice(idx, 1);
      fs.unlink(path.join(REC, id + '.wav'), () => {});
      saveMeta();
      broadcast({ type: 'log_deleted', id });
    }
  });

  ws.on('close', () => {
    if (speaker && speaker.ws === ws) endSpeaker();
    if (ws.user) broadcastOnline();
  });
  ws.on('error', () => {});
});

setInterval(() => {
  wss.clients.forEach((ws) => {
    if (!ws.isAlive) return ws.terminate();
    ws.isAlive = false;
    ws.ping();
  });
}, 30000);

server.listen(PORT, () => console.log('GFGC Radio running on port ' + PORT));
