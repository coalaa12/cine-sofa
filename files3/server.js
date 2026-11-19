const express = require('express');
const http = require('http');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);
const io = new Server(server);
app.use(express.static(__dirname + '/public'));

const rooms = {}; // codigo -> { state, host, locked, chat }
const EMOJIS = ['😂', '😱', '❤️', '👏', '🔥'];

function users(room) {
  return [...(io.sockets.adapter.rooms.get(room) || [])].map(id => ({ id, name: io.sockets.sockets.get(id).name }));
}
const pub = r => ({ host: r.host, locked: r.locked });
const pl = r => r.poll.map(o => ({ id: o.id, title: o.title, by: o.by, n: o.voters.length, voters: o.voters }));
const can = s => { const r = rooms[s.room]; return r && (!r.locked || r.host === s.id); };

io.on('connection', s => {
  s.on('join', ({ room, name, pass }) => {
    room = String(room || '').toLowerCase().slice(0, 20);
    if (!room) return;
    pass = String(pass || '').slice(0, 30);
    const ex = rooms[room];
    if (ex && ex.pass && ex.pass !== pass) { s.emit('denied'); return; }
    s.room = room;
    s.name = String(name || 'Visitante').slice(0, 24);
    s.join(room);
    const r = (rooms[room] = rooms[room] || { state: null, host: s.id, locked: false, chat: [], queue: [], poll: [], pass });
    let state = r.state;
    if (state && state.playing) state = { ...state, time: state.time + (Date.now() - state.at) / 1000 };
    s.emit('joined', { id: s.id, state, users: users(room), host: r.host, locked: r.locked, chat: r.chat, queue: r.queue, poll: r.poll });
    s.to(room).emit('user-joined', { id: s.id, name: s.name });
  });

  s.on('chat', t => {
    const r = rooms[s.room]; if (!r) return;
    const m = { name: s.name, text: String(t).slice(0, 500) };
    r.chat.push(m); r.chat = r.chat.slice(-30);
    io.to(s.room).emit('chat', m);
  });

  s.on('react', e => { if (s.room && EMOJIS.includes(e)) io.to(s.room).emit('react', e); });

  s.on('lock', v => {
    const r = rooms[s.room]; if (!r || r.host !== s.id) return;
    r.locked = !!v; io.to(s.room).emit('host', pub(r));
  });

  s.on('kick', id => {
    const r = rooms[s.room]; if (!r || r.host !== s.id || id === s.id) return;
    const t = io.sockets.sockets.get(id);
    if (t && t.room === s.room) { t.emit('kicked'); t.disconnect(true); }
  });

  s.on('source', src => {
    if (!can(s)) return;
    rooms[s.room].state = { src, playing: false, time: 0, at: Date.now() };
    s.to(s.room).emit('source', src);
  });

  s.on('ctl', c => {
    if (!can(s)) return;
    const r = rooms[s.room];
    r.state = Object.assign(r.state || {}, { playing: !!c.playing, time: +c.time || 0, at: Date.now() });
    s.to(s.room).emit('ctl', c);
  });

  s.on('suggest', t => {
    const r = rooms[s.room]; if (!r || r.poll.length >= 12) return;
    r.poll.push({ id: Date.now() + Math.random().toString(36).slice(2, 5), title: String(t).slice(0, 80), by: s.name, voters: [] });
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
    const r = rooms[s.room]; if (!r || r.queue.length >= 20 || !src) return;
    r.queue.push(src.kind === 'yt' ? { kind: 'yt', id: String(src.id).slice(0, 11) } : { kind: 'url', url: String(src.url).slice(0, 500) });
    io.to(s.room).emit('queue', r.queue);
  });
  s.on('queue-next', () => {
    if (!can(s)) return;
    const r = rooms[s.room], src = r.queue.shift(); if (!src) return;
    r.state = { src, playing: false, time: 0, at: Date.now() };
    io.to(s.room).emit('source', src); io.to(s.room).emit('queue', r.queue);
  });
  s.on('sfx', k => { if (s.room && ['aplauso', 'suspense', 'sino'].includes(k)) io.to(s.room).emit('sfx', k); });

  s.on('signal', ({ to, data }) => io.to(to).emit('signal', { from: s.id, data }));
  s.on('count', () => s.room && io.to(s.room).emit('count'));
  s.on('share-stop', () => s.room && s.to(s.room).emit('share-stop'));

  s.on('disconnect', () => {
    if (!s.room) return;
    s.to(s.room).emit('user-left', s.id);
    const r = rooms[s.room], left = users(s.room);
    if (!left.length) { delete rooms[s.room]; return; }
    if (r && r.host === s.id) { r.host = left[0].id; io.to(s.room).emit('host', pub(r)); }
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log('Cine Sofá rodando em http://localhost:' + PORT));
