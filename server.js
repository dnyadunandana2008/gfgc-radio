cat > /mnt/user-data/outputs/server.js << 'SERVEREOF'
const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const STUDENT_PASS = process.env.STUDENT_PASS || 'Pass@123';
const STAFF_PASS = process.env.STAFF_PASS || 'Staff@123';   // change this!
const SAMPLE_RATE = 16000;
const MAX_SECONDS = 60;      // max one transmission
const MAX_HISTORY = 200;     // keep last 200 recordings (Main channel only)

const DATA = path.join(__dirname, 'data');
const REC = path.join(DATA, 'rec');
const META = path.join(DATA, 'history.json');
const DIR_FILE = path.join(DATA, 'directory.json');
const GROUPS_FILE = path.join(DATA, 'groups.json');
fs.mkdirSync(REC, { recursive: true });

let history = [];
try { history = JSON.parse(fs.readFileSync(META, 'utf8')); } catch (e) {}
const saveMeta = () => fs.writeFile(META, JSON.stringify(history), () => {});

let directory = {}; // persistId -> {pid, role, name, uClass/subject, uucms/staffId, lastSeen}
try { directory = JSON.parse(fs.readFileSync(DIR_FILE, 'utf8')); } catch (e) {}
const saveDirectory = () => fs.writeFile(DIR_FILE, JSON.stringify(directory), () => {});

let groups = {}; // groupId -> {id, name, creatorId, members:[pid...], createdAt}
try { groups = JSON.parse(fs.readFileSync(GROUPS_FILE, 'utf8')); } catch (e) {}
const saveGroups = () => fs.writeFile(GROUPS_FILE, JSON.stringify(groups), () => {});

// ---------- HTTP (serves page + recordings + PWA files) ----------
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
const channelLocks = {}; // key -> { ws, user, target, chunks, bytes, timer }

const clean = (s) => String(s || '').trim().slice(0, 60);
const sendJSON = (ws, obj) => { if (ws.readyState === 1) ws.send(JSON.stringify(obj)); };
const onlineCount = () => [...wss.clients].filter((c) => c.user).length;

function persistId(u) {
  return u.role === 'lecturer' ? 'staff:' + u.staffId.toLowerCase() : 'stu:' + u.uucms.toLowerCase();
}
function onlineList() {
  return [...wss.clients].filter((c) => c.user).map((c) => ({ pid: c.pid, ...c.user }));
}
function myGroups(pid) {
  return Object.values(groups).filter((g) => g.members.includes(pid));
}
function broadcastAll(obj) {
  wss.clients.forEach((c) => { if (c.user) sendJSON(c, obj); });
}
function broadcastOnlineList() {
  broadcastAll({ type: 'online', count: onlineCount(), list: onlineList() });
}
function targetKey(t) {
  return t.type === 'main' ? 'main' : t.type + ':' + t.id;
}
function targetRecipients(t, senderWs) {
  if (t.type === 'main') return [...wss.clients].filter((c) => c.user);
  if (t.type === 'group') {
    const g = groups[t.id]; if (!g) return [];
    return [...wss.clients].filter((c) => c.user && g.members.includes(c.pid));
  }
  if (t.type === 'user') {
    return [...wss.clients].filter((c) => c.user && (c.pid === t.id || c === senderWs));
  }
  return [];
}
function notifyGroup(id) {
  const g = groups[id]; if (!g) return;
  [...wss.clients].forEach((c) => { if (c.user && g.members.includes(c.pid)) sendJSON(c, { type: 'group_update', group: g }); });
}
function notifyGroupRemoved(pids, id) {
  [...wss.clients].forEach((c) => { if (c.user && pids.includes(c.pid)) sendJSON(c, { type: 'group_removed', id }); });
}

function endSpeaker(key) {
  const lock = channelLocks[key];
  if (!lock) return;
  delete channelLocks[key];
  clearTimeout(lock.timer);

  const pcm = Buffer.concat(lock.chunks);
  const duration = pcm.length / 2 / SAMPLE_RATE;

  if (lock.target.type === 'main' && duration >= 0.4) {
    const id = Date.now() + '-' + Math.random().toString(36).slice(2, 8);
    fs.writeFileSync(path.join(REC, id + '.wav'), makeWav(pcm));
    const log = { id, user: lock.user, ts: Date.now(), duration: Math.round(duration * 10) / 10 };
    history.push(log);
    while (history.length > MAX_HISTORY) {
      const old = history.shift();
      fs.unlink(path.join(REC, old.id + '.wav'), () => {});
    }
    saveMeta();
    broadcastAll({ type: 'new_log', log });
  }

  targetRecipients(lock.target, lock.ws).forEach((c) => sendJSON(c, { type: 'idle', key, target: lock.target }));
}

wss.on('connection', (ws) => {
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });

  ws.on('message', (data, isBinary) => {
    if (isBinary) {
      const active = Object.entries(channelLocks).find(([, l]) => l.ws === ws);
      if (!active || data.length % 2 !== 0) return;
      const [, lock] = active;
      lock.chunks.push(Buffer.from(data));
      lock.bytes += data.length;
      targetRecipients(lock.target, ws).forEach((c) => {
        if (c !== ws && c.readyState === 1) c.send(data, { binary: true });
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
      const pid = persistId(user);
      user.pid = pid;
      ws.user = user;
      ws.pid = pid;

      directory[pid] = { pid, role: user.role, name: user.name, uClass: user.uClass, subject: user.subject, staffId: user.staffId, uucms: user.uucms, lastSeen: Date.now() };
      saveDirectory();

      sendJSON(ws, {
        type: 'welcome',
        myId: user.cid,
        myPid: pid,
        history,
        online: onlineCount(),
        onlineList: onlineList(),
        directory: Object.values(directory),
        groups: myGroups(pid)
      });
      broadcastOnlineList();
      broadcastAll({ type: 'directory_update', entry: directory[pid] });
      return;
    }

    if (!ws.user) return;

    if (msg.type === 'create_group') {
      const name = clean(msg.name) || 'Untitled Group';
      let members = Array.isArray(msg.members) ? msg.members.filter((id) => directory[id]) : [];
      members = [...new Set([...members, ws.pid])];
      if (members.length < 2) return sendJSON(ws, { type: 'error', msg: 'Pick at least one other person' });
      const id = 'g_' + Date.now() + '_' + Math.random().toString(36).slice(2, 6);
      groups[id] = { id, name, creatorId: ws.pid, members, createdAt: Date.now() };
      saveGroups();
      notifyGroup(id);
    } else if (msg.type === 'edit_group') {
      const g = groups[msg.groupId];
      if (!g || g.creatorId !== ws.pid) return;
      const before = [...g.members];
      if (typeof msg.name === 'string' && clean(msg.name)) g.name = clean(msg.name);
      if (Array.isArray(msg.addMembers)) {
        msg.addMembers.forEach((id) => { if (directory[id] && !g.members.includes(id)) g.members.push(id); });
      }
      if (Array.isArray(msg.removeMembers)) {
        g.members = g.members.filter((id) => id === g.creatorId || !msg.removeMembers.includes(id));
      }
      saveGroups();
      const removedPids = before.filter((id) => !g.members.includes(id));
      if (removedPids.length) notifyGroupRemoved(removedPids, g.id);
      notifyGroup(g.id);
    } else if (msg.type === 'delete_group') {
      const g = groups[msg.groupId];
      if (!g || g.creatorId !== ws.pid) return;
      const members = g.members;
      delete groups[msg.groupId];
      saveGroups();
      notifyGroupRemoved(members, msg.groupId);
    } else if (msg.type === 'ptt_start') {
      const target = msg.target && msg.target.type ? msg.target : { type: 'main' };
      if (target.type === 'group') {
        const g = groups[target.id];
        if (!g || !g.members.includes(ws.pid)) return sendJSON(ws, { type: 'denied' });
      } else if (target.type === 'user') {
        if (!directory[target.id] || target.id === ws.pid) return sendJSON(ws, { type: 'denied' });
      }
      const key = targetKey(target);
      if (channelLocks[key] && channelLocks[key].ws !== ws) return sendJSON(ws, { type: 'denied' });
      if (!channelLocks[key]) {
        channelLocks[key] = { ws, user: ws.user, target, chunks: [], bytes: 0, timer: setTimeout(() => endSpeaker(key), MAX_SECONDS * 1000) };
        targetRecipients(target, ws).forEach((c) => sendJSON(c, { type: 'speaking', user: ws.user, target, key }));
      }
      sendJSON(ws, { type: 'granted', target, key });
    } else if (msg.type === 'ptt_end') {
      const active = Object.entries(channelLocks).find(([, l]) => l.ws === ws);
      if (active) endSpeaker(active[0]);
    } else if (msg.type === 'delete_log') {
      const id = String(msg.id || '');
      if (!/^[\w-]+$/.test(id)) return;
      const idx = history.findIndex((l) => l.id === id);
      if (idx === -1) return;
      history.splice(idx, 1);
      fs.unlink(path.join(REC, id + '.wav'), () => {});
      saveMeta();
      broadcastAll({ type: 'log_deleted', id });
    }
  });

  ws.on('close', () => {
    const active = Object.entries(channelLocks).find(([, l]) => l.ws === ws);
    if (active) endSpeaker(active[0]);
    if (ws.user) broadcastOnlineList();
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
SERVEREOF
node --check /mnt/user-data/outputs/server.js && echo "server.js OK"
Output

server.js OK