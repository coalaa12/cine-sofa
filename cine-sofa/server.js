const express = require('express');
const http = require('http');
const fs = require('fs');
const crypto = require('crypto');
const { Server } = require('socket.io');
let Pool; try { ({ Pool } = require('pg')); } catch (e) { Pool = null; }
const AVATARS = ['🎬', '🍿', '🎥', '📽️', '🦊', '🐼', '🐙', '🦄', '🐸', '🐯', '🦉', '🐧', '🚀', '🎮', '🎧', '⭐', '🔥', '🌙', '🌈', '🍕', '🧛', '🤖', '👻', '🐳'];
const SCHEMA = [ // so cria o que nao existe; nunca apaga nada
  `CREATE TABLE IF NOT EXISTS users (id SERIAL PRIMARY KEY, username TEXT NOT NULL, username_lc TEXT NOT NULL UNIQUE, email TEXT NOT NULL UNIQUE,
    pass_hash TEXT NOT NULL, avatar TEXT NOT NULL DEFAULT '0', bio TEXT NOT NULL DEFAULT '', status TEXT NOT NULL DEFAULT '',
    msg_count INTEGER NOT NULL DEFAULT 0, created_at TIMESTAMPTZ NOT NULL DEFAULT now())`,
  `CREATE TABLE IF NOT EXISTS sessions (token_hash TEXT PRIMARY KEY, user_id INTEGER NOT NULL REFERENCES users(id), expires_at TIMESTAMPTZ NOT NULL)`,
  `CREATE TABLE IF NOT EXISTS favorites (user_id INTEGER NOT NULL REFERENCES users(id), room TEXT NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT now(), PRIMARY KEY (user_id, room))`,
  `CREATE TABLE IF NOT EXISTS history (id SERIAL PRIMARY KEY, user_id INTEGER NOT NULL REFERENCES users(id), room TEXT NOT NULL, joined_at TIMESTAMPTZ NOT NULL DEFAULT now(), left_at TIMESTAMPTZ)`,
];
async function initDb(pool) { for (const sql of SCHEMA) await pool.query(sql); }

const ql = r => r.queue.map(x => ({ qid: x.qid, kind: x.src.kind, id: x.src.id, url: x.src.url, title: x.title, by: x.by, byId: x.byId, voters: x.voters }));
function titleFromUrl(u) {
  try { const n = decodeURIComponent(new URL(u).pathname.split('/').filter(Boolean).pop() || ''); return (n || new URL(u).host).replace(/[\u0000-\u001f]/g, ' ').slice(0, 100); }
  catch (e) { return 'Vídeo'; }
}
async function ytTitle(id) { // so consulta o endereco fixo do YouTube, com o codigo ja validado (11 caracteres)
  try {
    const r = await fetch('https://www.youtube.com/oembed?format=json&url=' + encodeURIComponent('https://www.youtube.com/watch?v=' + id), { signal: AbortSignal.timeout(3000) });
    if (!r.ok) return '';
    return String((await r.json()).title || '').replace(/[\u0000-\u001f]/g, ' ').slice(0, 100);
  } catch (e) { return ''; }
}

function build(pool) {

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
const EXTRA = { suggest: [5, 60000], 'queue-add': [10, 60000], react: [15, 5000], sfx: [4, 10000], count: [2, 30000], break: [3, 60000], 'quick-new': [4, 60000], chat: [8, 10000], 'queue-vote': [20, 10000], 'queue-move': [20, 10000], 'queue-remove': [10, 10000], 'queue-play': [5, 10000] };

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
  return [...(io.sockets.adapter.rooms.get(room) || [])].map(id => {
    const x = io.sockets.sockets.get(id);
    return { id, name: x.name, av: x.user ? AVATARS[+x.user.avatar] || AVATARS[0] : '', acct: !!x.user };
  });
}
const pub = r => ({ host: r.host, locked: r.locked });
const pl = r => r.poll.map(o => ({ id: o.id, title: o.title, by: o.by, n: o.voters.length, voters: o.voters }));
const qv = r => ({ ...r.quick, counts: r.quick.opts.map((_, i) => Object.values(r.quick.votes).filter(v => v === i).length), score: r.score });
const can = s => { const r = rooms[s.room]; return r && (!r.locked || r.host === s.id); };


  /* ---------- contas (PostgreSQL). Sem banco configurado, o site segue so com visitantes ---------- */
  let dbOk = false;
  const ready = pool ? initDb(pool).then(() => { dbOk = true; }).catch(e => console.error('Banco de dados indisponivel:', e.message)) : Promise.resolve();
  const sha = t => crypto.createHash('sha256').update(t).digest('hex');
  const q = (t, p) => pool.query(t, p);
  const pubUser = u => ({ id: u.id, username: u.username, avatar: AVATARS[+u.avatar] || AVATARS[0], avatarIdx: +u.avatar || 0, bio: u.bio, status: u.status });
  async function userFromCookie(h) {
    const m = String(h || '').match(/(?:^|;\s*)cs_sid=([a-f0-9]{64})/);
    if (!m || !dbOk) return null;
    const r = await q('SELECT u.* FROM sessions s JOIN users u ON u.id = s.user_id WHERE s.token_hash = $1 AND s.expires_at > $2', [sha(m[1]), new Date()]);
    return r.rows[0] ? { ...r.rows[0], tok: m[1] } : null;
  }
  const hashPw = pw => new Promise((ok, no) => {
    const salt = crypto.randomBytes(16);
    crypto.scrypt(pw, salt, 64, (e, k) => e ? no(e) : ok('scrypt$' + salt.toString('hex') + '$' + k.toString('hex')));
  });
  const checkPw = (pw, stored) => new Promise(ok => {
    const [, salt, h] = String(stored).split('$');
    crypto.scrypt(pw, Buffer.from(salt || '', 'hex'), 64, (e, k) => {
      let good = false; try { good = !e && crypto.timingSafeEqual(k, Buffer.from(h || '', 'hex')); } catch (x) {}
      ok(good);
    });
  });
  const DUMMY = 'scrypt$' + '00'.repeat(16) + '$' + '00'.repeat(64);
  const wrap = fn => (req, res, next) => fn(req, res, next).catch(e => { console.error('api:', e.message); res.status(500).json({ error: 'Erro interno.' }); });
  async function startSession(req, res, uid) {
    const tok = crypto.randomBytes(32).toString('hex');
    await q('INSERT INTO sessions(token_hash, user_id, expires_at) VALUES($1, $2, $3)', [sha(tok), uid, new Date(Date.now() + 30 * 864e5)]);
    res.append('Set-Cookie', 'cs_sid=' + tok + '; HttpOnly; SameSite=Lax; Path=/; Max-Age=2592000' + (req.secure ? '; Secure' : ''));
  }
  const clearCookie = res => res.append('Set-Cookie', 'cs_sid=; HttpOnly; SameSite=Lax; Path=/; Max-Age=0');
  async function stats(uid, msgs) {
    const h = (await q('SELECT room, joined_at, left_at FROM history WHERE user_id = $1 ORDER BY joined_at DESC LIMIT 5000', [uid])).rows;
    const ms = h.reduce((a, x) => a + (x.left_at ? Math.max(0, new Date(x.left_at) - new Date(x.joined_at)) : 0), 0);
    return { sessions: h.length, minutes: Math.round(ms / 60000), rooms: new Set(h.map(x => x.room)).size, messages: msgs };
  }
  const txt = (v, n) => String(v || '').replace(/[\u0000-\u001f]/g, ' ').trim().slice(0, n);
  const ROOM = /^[a-z0-9_-]{1,30}$/;

  const api = express.Router();
  api.use(express.json({ limit: '10kb' }));
  api.use((req, res, next) => { // protecao contra CSRF: cabecalho proprio + mesma origem
    if (req.method !== 'GET') {
      const o = req.get('origin'); let bad = req.get('x-cs') !== '1';
      if (o) { try { bad = bad || new URL(o).host !== req.get('host'); } catch (e) { bad = true; } }
      if (bad) return res.status(403).json({ error: 'Requisição recusada.' });
    }
    next();
  });
  api.get('/config', wrap(async (req, res) => { await ready; res.json({ accounts: dbOk, avatars: AVATARS }); }));
  api.use(wrap(async (req, res, next) => {
    await ready;
    if (!dbOk) return res.status(503).json({ error: 'Contas indisponíveis no momento.' });
    req.user = await userFromCookie(req.headers.cookie); next();
  }));
  const need = (req, res, next) => req.user ? next() : res.status(401).json({ error: 'Faça login.' });

  api.post('/register', wrap(async (req, res) => {
    if (!limitIp(req.ip, 'reg', 20, 3600000)) return res.status(429).json({ error: 'Muitas contas criadas por aqui. Tente mais tarde.' });
    const b = req.body || {}, username = String(b.username || '').trim(), email = String(b.email || '').trim().toLowerCase(), pw = String(b.password || '');
    if (!/^[a-zA-Z0-9_]{3,20}$/.test(username)) return res.status(400).json({ error: 'Nome de usuário: 3 a 20 letras, números ou _.' });
    if (email.length > 254 || !/^[^\s@]+@[^\s@]+\.[^\s@]{2,}$/.test(email)) return res.status(400).json({ error: 'Email inválido.' });
    if (pw.length < 8 || pw.length > 128) return res.status(400).json({ error: 'A senha precisa ter de 8 a 128 caracteres.' });
    const dup = await q('SELECT username_lc FROM users WHERE username_lc = $1 OR email = $2', [username.toLowerCase(), email]);
    if (dup.rows.length) return res.status(409).json({ error: dup.rows[0].username_lc === username.toLowerCase() ? 'Esse nome de usuário já existe.' : 'Esse email já está cadastrado.' });
    const r = await q('INSERT INTO users(username, username_lc, email, pass_hash) VALUES($1, $2, $3, $4) RETURNING *', [username, username.toLowerCase(), email, await hashPw(pw)]);
    await startSession(req, res, r.rows[0].id);
    res.json({ user: pubUser(r.rows[0]) });
  }));
  api.post('/login', wrap(async (req, res) => {
    const b = req.body || {}, login = String(b.login || '').trim().toLowerCase().slice(0, 254), pw = String(b.password || '').slice(0, 128);
    if (!limitIp(req.ip, 'login', 30, 600000) || !limitIp(login, 'acct', 10, 900000)) return res.status(429).json({ error: 'Muitas tentativas. Espere alguns minutos.' });
    const u = (await q('SELECT * FROM users WHERE username_lc = $1 OR email = $1', [login])).rows[0];
    const ok = await checkPw(pw, u ? u.pass_hash : DUMMY);
    if (!u || !ok) return res.status(401).json({ error: 'Usuário ou senha incorretos.' });
    await startSession(req, res, u.id);
    res.json({ user: pubUser(u) });
  }));
  api.post('/logout', wrap(async (req, res) => {
    if (req.user) await q('DELETE FROM sessions WHERE token_hash = $1', [sha(req.user.tok)]);
    clearCookie(res); res.json({ ok: true });
  }));
  api.get('/me', need, wrap(async (req, res) => {
    res.json({ user: { ...pubUser(req.user), email: req.user.email, since: req.user.created_at }, stats: await stats(req.user.id, req.user.msg_count) });
  }));
  api.patch('/me', need, wrap(async (req, res) => {
    const b = req.body || {}, av = +b.avatar;
    if (!Number.isInteger(av) || av < 0 || av >= AVATARS.length) return res.status(400).json({ error: 'Avatar inválido.' });
    const r = await q('UPDATE users SET bio = $1, status = $2, avatar = $3 WHERE id = $4 RETURNING *', [txt(b.bio, 160), txt(b.status, 40), String(av), req.user.id]);
    res.json({ user: pubUser(r.rows[0]) });
  }));
  api.get('/me/favorites', need, wrap(async (req, res) => {
    res.json({ rooms: (await q('SELECT room FROM favorites WHERE user_id = $1 ORDER BY created_at DESC LIMIT 100', [req.user.id])).rows.map(x => x.room) });
  }));
  api.post('/me/favorites', need, wrap(async (req, res) => {
    const room = String((req.body || {}).code || '').toLowerCase();
    if (!ROOM.test(room)) return res.status(400).json({ error: 'Código de sala inválido.' });
    if ((await q('SELECT room FROM favorites WHERE user_id = $1', [req.user.id])).rows.length >= 100) return res.status(400).json({ error: 'Limite de 100 favoritas.' });
    await q('INSERT INTO favorites(user_id, room) VALUES($1, $2) ON CONFLICT DO NOTHING', [req.user.id, room]);
    res.json({ ok: true });
  }));
  api.delete('/me/favorites', need, wrap(async (req, res) => {
    await q('DELETE FROM favorites WHERE user_id = $1 AND room = $2', [req.user.id, String((req.body || {}).code || '').toLowerCase()]);
    res.json({ ok: true });
  }));
  api.get('/me/history', need, wrap(async (req, res) => {
    res.json({ items: (await q('SELECT room, joined_at, left_at FROM history WHERE user_id = $1 ORDER BY joined_at DESC LIMIT 50', [req.user.id])).rows });
  }));
  api.get('/me/export', need, wrap(async (req, res) => { // direito de acesso aos dados (LGPD)
    const u = req.user, fav = (await q('SELECT room, created_at FROM favorites WHERE user_id = $1', [u.id])).rows;
    const his = (await q('SELECT room, joined_at, left_at FROM history WHERE user_id = $1 ORDER BY joined_at DESC LIMIT 5000', [u.id])).rows;
    res.set('Content-Disposition', 'attachment; filename="cine-sofa-meus-dados.json"');
    res.json({ conta: { usuario: u.username, email: u.email, avatar: AVATARS[+u.avatar] || AVATARS[0], bio: u.bio, status: u.status, mensagens_enviadas: u.msg_count, criada_em: u.created_at }, favoritas: fav, historico: his });
  }));
  api.delete('/me', need, wrap(async (req, res) => { // exclusao da conta (LGPD)
    if (!limitIp(req.ip, 'del', 5, 600000)) return res.status(429).json({ error: 'Muitas tentativas. Espere alguns minutos.' });
    if (!(await checkPw(String((req.body || {}).password || '').slice(0, 128), req.user.pass_hash))) return res.status(401).json({ error: 'Senha incorreta.' });
    for (const t of ['sessions', 'favorites', 'history']) await q('DELETE FROM ' + t + ' WHERE user_id = $1', [req.user.id]);
    await q('DELETE FROM users WHERE id = $1', [req.user.id]);
    clearCookie(res); res.json({ ok: true });
  }));
  api.use((err, req, res, next) => res.status(400).json({ error: 'Requisição inválida.' }));
  app.use('/api', api);

  io.use(async (socket, next) => {
    try { await ready; socket.user = await userFromCookie(socket.handshake.headers.cookie); } catch (e) {}
    next();
  });

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
    s.name = uniqueName(room, s.user ? s.user.username : cleanName(name));
    s.room = room; s.join(room);
    const r = (rooms[room] = ex || {
      state: null, host: s.id, locked: false, chat: [], queue: [], poll: [], pass: pass ? hash(pass) : null,
      quick: null, score: {}, stats: { react: {}, msgs: {}, watched: [] }, sharer: null, voice: new Set(), lastCtl: null,
    });
    let state = r.state;
    if (state && state.playing) state = { ...state, time: state.time + (Date.now() - state.at) / 1000 };
    s.emit('joined', { id: s.id, state, users: users(room), host: r.host, locked: r.locked, chat: r.chat, queue: ql(r), poll: r.poll, quick: r.quick ? qv(r) : null, voice: [...r.voice] });
    s.to(room).emit('user-joined', { id: s.id, name: s.name, av: s.user ? AVATARS[+s.user.avatar] || AVATARS[0] : '', acct: !!s.user });
    if (s.user && pool) pool.query('INSERT INTO history(user_id, room) VALUES($1, $2) RETURNING id', [s.user.id, room]).then(r => { s.hid = r.rows[0].id; }).catch(() => {});
  });

  s.on('chat', t => {
    const r = rooms[s.room]; t = String(t || '').trim().slice(0, 500);
    if (!r || !t) return;
    const m = { name: s.name, text: t, av: s.user ? AVATARS[+s.user.avatar] || AVATARS[0] : '' };
    if (s.user) s.msgs = (s.msgs || 0) + 1;
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

  const live = x => x.voters.filter(id => { const t = io.sockets.sockets.get(id); return t && t.room === s.room; }).length;
  const playFrom = (r, i) => {
    const [it] = r.queue.splice(i, 1), src = it.src;
    r.state = { src, playing: false, time: 0, at: Date.now() };
    r.stats.watched.push(src); io.to(s.room).emit('source', src); io.to(s.room).emit('queue', ql(r));
  };
  s.on('queue-add', src => {
    const r = rooms[s.room]; src = cleanSrc(src, false);
    if (!r || !src || r.queue.length >= 20) return;
    const it = { qid: Math.random().toString(36).slice(2, 9), src, title: src.kind === 'url' ? titleFromUrl(src.url) : '', by: s.name, byId: s.id, voters: [] };
    r.queue.push(it); io.to(s.room).emit('queue', ql(r));
    if (src.kind === 'yt') ytTitle(src.id).then(t => { if (t && r.queue.includes(it)) { it.title = t; io.to(s.room).emit('queue', ql(r)); } });
  });
  s.on('queue-vote', qid => {
    const r = rooms[s.room], it = r && r.queue.find(x => x.qid === qid); if (!it) return;
    const i = it.voters.indexOf(s.id); if (i < 0) it.voters.push(s.id); else it.voters.splice(i, 1);
    io.to(s.room).emit('queue', ql(r));
  });
  s.on('queue-move', ({ qid, dir } = {}) => {
    const r = rooms[s.room]; if (!r || !can(s)) return;
    const i = r.queue.findIndex(x => x.qid === qid), j = i + (dir === 'up' ? -1 : dir === 'down' ? 1 : 0);
    if (i < 0 || j < 0 || j >= r.queue.length || i === j) return;
    [r.queue[i], r.queue[j]] = [r.queue[j], r.queue[i]]; io.to(s.room).emit('queue', ql(r));
  });
  s.on('queue-remove', qid => {
    const r = rooms[s.room]; if (!r) return;
    const i = r.queue.findIndex(x => x.qid === qid); if (i < 0) return;
    if (r.host !== s.id && r.queue[i].byId !== s.id) return; // so o anfitriao ou quem adicionou
    r.queue.splice(i, 1); io.to(s.room).emit('queue', ql(r));
  });
  s.on('queue-play', qid => {
    const r = rooms[s.room]; if (!r || !can(s)) return;
    const i = r.queue.findIndex(x => x.qid === qid); if (i >= 0) playFrom(r, i);
  });
  s.on('queue-next', () => { // toca o mais votado; empate ou sem votos: o primeiro da fila
    const r = rooms[s.room]; if (!r || !can(s) || !r.queue.length) return;
    let best = 0, bv = -1; r.queue.forEach((x, i) => { const v = live(x); if (v > bv) { bv = v; best = i; } });
    playFrom(r, best);
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
    if (pool && s.user) {
      if (s.hid) pool.query('UPDATE history SET left_at = now() WHERE id = $1', [s.hid]).catch(() => {});
      if (s.msgs) pool.query('UPDATE users SET msg_count = msg_count + $1 WHERE id = $2', [s.msgs, s.user.id]).catch(() => {});
    }
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


  return server;
}

if (require.main === module) {
  let pool = null;
  const url = process.env.DATABASE_URL;
  if (url && Pool) {
    pool = new Pool({ connectionString: url, ssl: /localhost|127\.0\.0\.1/.test(url) ? false : { rejectUnauthorized: false }, max: 5 });
    pool.on('error', e => console.error('pg:', e.message));
  } else console.log('Contas desativadas (sem DATABASE_URL ou sem o pacote pg).');
  const PORT = process.env.PORT || 3000;
  build(pool).listen(PORT, () => console.log('Cine Sofá rodando em http://localhost:' + PORT));
}
module.exports = build;
