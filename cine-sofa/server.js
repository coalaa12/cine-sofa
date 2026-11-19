const express = require('express');
const http = require('http');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);
const io = new Server(server);
app.use(express.static(__dirname + '/public'));

const rooms = {}; // codigo -> { state: { src, playing, time, at } }

function users(room) {
  return [...(io.sockets.adapter.rooms.get(room) || [])].map(id => ({
    id,
    name: io.sockets.sockets.get(id).name,
  }));
}

io.on('connection', s => {
  s.on('join', ({ room, name }) => {
    room = String(room || '').toLowerCase().slice(0, 20);
    if (!room) return;
    s.room = room;
    s.name = String(name || 'Visitante').slice(0, 24);
    s.join(room);
    const r = (rooms[room] = rooms[room] || { state: null });
    let state = r.state;
    if (state && state.playing) {
      state = { ...state, time: state.time + (Date.now() - state.at) / 1000 };
    }
    s.emit('joined', { id: s.id, state, users: users(room) });
    s.to(room).emit('user-joined', { id: s.id, name: s.name });
  });

  s.on('chat', t => {
    if (!s.room) return;
    io.to(s.room).emit('chat', { name: s.name, text: String(t).slice(0, 500) });
  });

  s.on('source', src => {
    if (!s.room) return;
    rooms[s.room].state = { src, playing: false, time: 0, at: Date.now() };
    s.to(s.room).emit('source', src);
  });

  s.on('ctl', c => {
    const r = rooms[s.room];
    if (!r) return;
    r.state = Object.assign(r.state || {}, { playing: !!c.playing, time: +c.time || 0, at: Date.now() });
    s.to(s.room).emit('ctl', c);
  });

  s.on('signal', ({ to, data }) => io.to(to).emit('signal', { from: s.id, data }));
  s.on('count', () => s.room && io.to(s.room).emit('count'));
  s.on('share-stop', () => s.room && s.to(s.room).emit('share-stop'));

  s.on('disconnect', () => {
    if (!s.room) return;
    s.to(s.room).emit('user-left', s.id);
    if (!users(s.room).length) delete rooms[s.room];
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log('Cine Sofá rodando em http://localhost:' + PORT));
