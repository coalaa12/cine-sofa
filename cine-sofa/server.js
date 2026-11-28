const express = require('express');
const http = require('http');
const fs = require('fs');
const crypto = require('crypto');
const { Server } = require('socket.io');

const app = express();
app.set('trust proxy', true);
const server = http.createServer(app);
const io = new Server(server, { maxHttpBufferSize: 1e5 });

const MAX_ROOMS = 300, MAX_USERS = 40, MAX_VOICE = 15;
const EMOJIS = ['😂', '😱', '❤️', '👏', '🔥'];
const SFX = ['aplauso', 'suspense', 'sino'];
const INDEX = fs.readFileSync(__dirname + '/public/index.html', 'utf8');

app.use((req, res, next) => {
  res.set({ 'X-Content-Type-Options': 'nosniff', 'Referrer-Policy': 'strict-origin-when-cross-origin', 'X-Frame-Options': 'SAMEORIGIN' });
  next();
});
app.get('/healthz', (req, res) => res.send('ok'));
app.get('/', (req, res) => {
  const host = /^[a-z0-9.\-:]{1,100}$/i.test(req.get('host') || '') ? req.get('host') : 'localhost';
  res.type('html').send(INDEX.replace(/__ORIGIN__/g, req.protocol + '://' + host));
});
app.use(express.static(__dirname + '/public'));

const rooms = {};
const hash = p => crypto.createHash('sha256').update(String(p)).digest();

/* ---------- limites de uso ---------- */
function limit(s, key, max, ms) {
  const now = Date.now(), b = (s.rl = s.rl || {}), a = (b[key] = (b[key] || []).filter(t => now - t < ms));
  if (a.length >= max) return false;
  a.push(now); return true;
}
const ipHits = new Map();
function limitIp(ip, key, max, ms) {
  const k = ip + '|' + key, now = Date.now(), a = (ipHits.get(k) || []).filter(t => now - t < ms);
  const ok = a.length < max; if (ok) a.push(now);
  ipHits.set(k, a); return ok;
}
setInterval(() => ipHits.clear(), 10 * 60 * 1000).unref();
const ipOf = s => String(s.handshake.headers['x-forwarded-for'] || s.handshake.address || '').split(',')[0].trim();
const HEAVY = new Set(['signal', 'vsignal', 'ctl', 'lag']);
const EXTRA = { suggest: [5, 60000], 'queue-add': [10, 60000], react: [15, 5000], sfx: [4, 10000], count: [2, 30000], break: [3, 60000], 'quick-new': [4, 60000], chat: [8, 10000] };

/* ---------- validacao ---------- */
const cleanName = n => String(n || '').replace(/[\u0000-\u001f<>]/g, '').trim().slice(0, 24) || 'Visitante';
function uniqueName(room, n) {
  const taken = new Set(users(room).map(u => u.name.toLowerCase()));
  let c = n, i = 2; while (taken.has(c.toLowerCase())) c = n.slice(0, 20) + ' (' + i++ + ')';
  return c;
}
function cleanSrc(src, allowFile) {
  if (!src || typeof src !== 'object') return null;
  if (src.kind === 'yt' && /^[\w-]{11}$/.test(String(src.id))) return { kind: 'yt', id: String(src.id) };
  if (src.kind === 'url') { const u = String(src.url || '').trim().slice(0, 500); return /^https?:\/\/\S+$/i.test(u) ? { kind: 'url', url: u } : null; }
  if (allowFile && src.kind === 'file') { const n = String(src.name || '').replace(/[\r\n]/g, ' ').slice(0, 120); return n ? { kind: 'file', name: n } : null; }
  return null;
}

function users(room) {
  return [...(io.sockets.adapter.rooms.get(room) || [])].map(id => ({ id, name: io.sockets.sockets.get(id).name }));
}
const pub = r => ({ host: r.host, locked: r.locked });
const pl = r => r.poll.map(o => ({ id: o.id, title: o.title, by: o.by, n: o.voters.length, voters: o.voters }));
const qv = r => ({ ...r.quick, counts: r.quick.opts.map((_, i) => Object.values(r.quick.votes).filter(v => v === i).length), score: r.score });
const can = s => { const r = rooms[s.room]; return r && (!r.locked || r.host === s.id); };

io.on('connection', s => {
  s.use(([ev], next) => {
    const x = EXTRA[ev];
    if (x && !limit(s, ev, x[0], x[1])) return next(new Error('rate'));
    if (!limit(s, HEAVY.has(ev) ? 'heavy' : 'all', HEAVY.has(ev) ? 300 : 40, 5000)) return next(new Error('rate'));
    next();
  });

  s.on('join', ({ room, name, pass } = {}) => {
    if (s.room) return;
    room = String(room || '').toLowerCase();
    if (!/^[a-z0-9_-]{1,30}$/.test(room)) return s.emit('bad-room');
    const ip = ipOf(s);
    if (!limitIp(ip, 'join', 30, 60000)) return s.emit('denied', 'limit');
    pass = String(pass || '').slice(0, 30);
    const ex = rooms[room];
    if (ex && ex.pass) {
      if (!limitIp(ip, 'pw:' + room, 8, 60000)) return s.emit('denied', 'limit');
      if (!crypto.timingSafeEqual(ex.pass, hash(pass))) return s.emit('denied');
    }
    if (!ex && Object.keys(rooms).length >= MAX_ROOMS) return s.emit('busy');
    if (ex && users(room).length >= MAX_USERS) return s.emit('busy');
    s.name = uniqueName(room, cleanName(name));
    s.room = room; s.join(room);
    const r = (rooms[room] = ex || {
      state: null, host: s.id, locked: false, chat: [], queue: [], poll: [], pass: pass ? hash(pass) : null,
      quick: null, score: {}, stats: { react: {}, msgs: {}, watched: [] }, sharer: null, voice: new Set(), lastCtl: null,
    });
    let state = r.state;
    if (state && state.playing) state = { ...state, time: state.time + (Date.now() - state.at) / 1000 };
    s.emit('joined', { id: s.id, state, users: users(room), host: r.host, locked: r.locked, chat: r.chat, queue: r.queue, poll: r.poll, quick: r.quick ? qv(r) : null, voice: [...r.voice] });
    s.to(room).emit('user-joined', { id: s.id, name: s.name });
  });

  s.on('chat', t => {
    const r = rooms[s.room]; t = String(t || '').trim().slice(0, 500);
    if (!r || !t) return;
    const m = { name: s.name, text: t };
    r.chat.push(m); r.stats.msgs[s.name] = (r.stats.msgs[s.name] || 0) + 1; r.chat = r.chat.slice(-30);
    io.to(s.room).emit('chat', m);
  });

  s.on('react', e => {
    const r = rooms[s.room]; if (!r || !EMOJIS.includes(e)) return;
    r.stats.react[e] = (r.stats.react[e] || 0) + 1; io.to(s.room).emit('react', e);
  });
  s.on('sfx', k => { if (s.room && SFX.includes(k)) io.to(s.room).emit('sfx', k); });
  s.on('count', () => { if (can(s)) io.to(s.room).emit('count'); });
  s.on('lag', on => s.room && s.to(s.room).emit('lag', { id: s.id, on: !!on }));

  s.on('lock', v => {
    const r = rooms[s.room]; if (!r || r.host !== s.id) return;
    r.locked = !!v; io.to(s.room).emit('host', pub(r));
  });
  s.on('kick', id => {
    const r = rooms[s.room]; if (!r || r.host !== s.id || id === s.id) return;
    const t = io.sockets.sockets.get(id);
    if (t && t.room === s.room) { t.emit('kicked'); t.disconnect(true); }
  });
  s.on('make-host', id => {
    const r = rooms[s.room], t = io.sockets.sockets.get(id);
    if (r && r.host === s.id && t && t.room === s.room) { r.host = id; io.to(s.room).emit('host', pub(r)); }
  });

  s.on('source', src => {
    if (!can(s)) return;
    src = cleanSrc(src, true); if (!src) return;
    const r = rooms[s.room];
    r.state = { src, playing: false, time: 0, at: Date.now() }; r.stats.watched.push(src);
    s.to(s.room).emit('source', src);
  });
  s.on('ctl', c => {
    if (!can(s) || !c || typeof c !== 'object') return;
    const r = rooms[s.room], now = Date.now(), playing = !!c.playing, time = Math.max(0, Math.min(+c.time || 0, 1e6));
    // dois comandos opostos quase juntos: vale o primeiro
    if (r.lastCtl && r.lastCtl.by !== s.id && now - r.lastCtl.at < 250 && r.lastCtl.playing !== playing) return;
    r.lastCtl = { by: s.id, at: now, playing };
    r.state = Object.assign(r.state || {}, { playing, time, at: now });
    s.to(s.room).emit('ctl', { playing, time, type: String(c.type || '').slice(0, 8) });
  });

  s.on('suggest', t => {
    const r = rooms[s.room]; t = String(t || '').trim().slice(0, 80);
    if (!r || !t || r.poll.length >= 12) return;
    r.poll.push({ id: Date.now() + Math.random().toString(36).slice(2, 5), title: t, by: s.name, voters: [] });
    io.to(s.room).emit('poll', pl(r));
  });
  s.on('vote', id => {
    const r = rooms[s.room]; if (!r) return;
    const o = r.poll.find(x => x.id === id); if (!o) return;
    const i = o.voters.indexOf(s.id); if (i < 0) o.voters.push(s.id); else o.voters.splice(i, 1);
    io.to(s.room).emit('poll', pl(r));
  });
  s.on('poll-clear', () => { const r = rooms[s.room]; if (r && r.host === s.id) { r.poll = []; io.to(s.room).emit('poll', []); } });

  s.on('queue-add', src => {
    const r = rooms[s.room]; src = cleanSrc(src, false);
    if (!r || !src || r.queue.length >= 20) return;
    r.queue.push(src); io.to(s.room).emit('queue', r.queue);
  });
  s.on('queue-next', () => {
    if (!can(s)) return;
    const r = rooms[s.room], src = r.queue.shift(); if (!src) return;
    r.state = { src, playing: false, time: 0, at: Date.now() };
    r.stats.watched.push(src); io.to(s.room).emit('source', src); io.to(s.room).emit('queue', r.queue);
  });

  s.on('break', ({ secs, label } = {}) => {
    if (can(s)) io.to(s.room).emit('break', { secs: Math.max(0, Math.min(+secs || 0, 7200)), label: String(label || '').slice(0, 40) });
  });
  s.on('quick-new', q => {
    const r = rooms[s.room]; if (!r || !q || !Array.isArray(q.opts)) return;
    if (q.scored && r.host !== s.id) return;
    const opts = q.opts.slice(0, 6).map(x => String(x).slice(0, 40)).filter(Boolean);
    if (opts.length < 2) return;
    r.quick = { q: String(q.q).slice(0, 100), opts, votes: {}, scored: !!q.scored, answer: null };
    io.to(s.room).emit('quick', qv(r));
  });
  s.on('quick-vote', i => {
    const r = rooms[s.room]; if (!r || !r.quick || r.quick.answer !== null || r.quick.opts[i] === undefined) return;
    r.quick.votes[s.id] = i; io.to(s.room).emit('quick', qv(r));
  });
  s.on('quick-reveal', i => {
    const r = rooms[s.room]; if (!r || !r.quick || r.host !== s.id || r.quick.answer !== null || r.quick.opts[i] === undefined) return;
    r.quick.answer = i;
    if (r.quick.scored) Object.entries(r.quick.votes).forEach(([id, v]) => {
      const t = io.sockets.sockets.get(id); if (t && v === i) r.score[t.name] = (r.score[t.name] || 0) + 1;
    });
    io.to(s.room).emit('quick', qv(r));
  });
  s.on('recap', () => {
    const r = rooms[s.room]; if (r && r.host === s.id) io.to(s.room).emit('recap', { stats: r.stats, score: r.score });
  });

  /* ---------- transmissao de tela/arquivo: so uma pessoa por vez ---------- */
  s.on('share-start', () => {
    const r = rooms[s.room]; if (!r) return;
    if ((r.sharer && r.sharer !== s.id) || !can(s)) return s.emit('share-denied');
    r.sharer = s.id;
  });
  s.on('share-stop', () => {
    const r = rooms[s.room];
    if (r && r.sharer === s.id) { r.sharer = null; s.to(s.room).emit('share-stop'); }
  });
  s.on('signal', ({ to, data } = {}) => {
    const r = rooms[s.room], t = io.sockets.sockets.get(to);
    if (!r || !t || t.room !== s.room || !data || typeof data !== 'object') return;
    if (data.sdp && data.sdp.type === 'offer' && r.sharer !== s.id) return; // so quem transmite pode ofertar
    t.emit('signal', { from: s.id, data });
  });

  /* ---------- voz ---------- */
  s.on('voice-join', () => {
    const r = rooms[s.room]; if (!r) return;
    if (r.voice.size >= MAX_VOICE && !r.voice.has(s.id)) return s.emit('voice-full');
    const list = [...r.voice].filter(id => id !== s.id); r.voice.add(s.id);
    s.emit('voice-list', list); s.to(s.room).emit('voice-state', { id: s.id, on: true });
  });
  s.on('voice-leave', () => {
    const r = rooms[s.room]; if (!r) return;
    r.voice.delete(s.id); s.to(s.room).emit('voice-state', { id: s.id, on: false });
  });
  s.on('vsignal', ({ to, data } = {}) => {
    const r = rooms[s.room], t = io.sockets.sockets.get(to);
    if (!r || !t || t.room !== s.room || !data || typeof data !== 'object' || !r.voice.has(s.id)) return;
    t.emit('vsignal', { from: s.id, data });
  });

  s.on('disconnect', () => {
    if (!s.room) return;
    s.to(s.room).emit('user-left', s.id);
    const r = rooms[s.room], left = users(s.room);
    if (!left.length) { delete rooms[s.room]; return; }
    if (!r) return;
    r.voice.delete(s.id);
    if (r.sharer === s.id) { r.sharer = null; s.to(s.room).emit('share-stop'); }
    if (r.host === s.id) { r.host = left[0].id; io.to(s.room).emit('host', pub(r)); }
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log('Cine Sofá rodando em http://localhost:' + PORT));
