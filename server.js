'use strict';

/**
 * WatchSync sync server
 * -------------------------------------------------------------
 * A tiny WebSocket relay that keeps two (or a few) players in sync.
 * It does NOT host video. Each phone plays the video locally; this
 * server only passes around three things — play, pause, seek — plus
 * a couple of room/housekeeping messages.
 *
 * Run:  node server.js     (listens on ws://0.0.0.0:8080 by default)
 */

const http = require('http');
const { WebSocketServer } = require('ws');
const crypto = require('crypto');

// ---- Config ---------------------------------------------------
const PORT = process.env.PORT || 8080;
const RATE_LIMIT_MAX = 25;          // max inbound messages...
const RATE_LIMIT_WINDOW_MS = 2000;  // ...per this rolling window (per client)
const ROOM_CODE_LENGTH = 6;
const MAX_CLIENTS_PER_ROOM = 8;     // it's for friends, not a broadcast

const rooms = new Map(); // code -> Room

// ---- Helpers --------------------------------------------------
function makeRoomCode() {
  const alphabet = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789'; // no 0/O/1/I confusion
  let code;
  do {
    code = '';
    const bytes = crypto.randomBytes(ROOM_CODE_LENGTH);
    for (let i = 0; i < ROOM_CODE_LENGTH; i++) code += alphabet[bytes[i] % alphabet.length];
  } while (rooms.has(code));
  return code;
}

function send(ws, obj) {
  if (ws.readyState === ws.OPEN) ws.send(JSON.stringify(obj));
}

function broadcast(room, obj, exceptWs = null) {
  for (const client of room.clients) if (client !== exceptWs) send(client, obj);
}

// Estimate the live playhead (so a late joiner lands at the right spot).
function currentTime(room) {
  if (room.state.playing) return room.state.time + (Date.now() - room.state.updatedAt) / 1000;
  return room.state.time;
}

function needRoom(ws) {
  send(ws, { type: 'error', code: 'no_room', message: 'Join a room first.' });
}

// If auto-pause is on and anyone is unstable, force everyone to pause.
function evaluateStability(room) {
  const blocked = room.autoPause && room.unstable.size > 0;
  if (blocked && room.state.playing) {
    room.state.time = currentTime(room);
    room.state.playing = false;
    room.state.updatedAt = Date.now();
    broadcast(room, { type: 'forcePause', reason: 'network_unstable', time: room.state.time });
  }
  return blocked;
}

function leaveRoom(ws) {
  const room = ws.room;
  if (!room) return;
  room.clients.delete(ws);
  room.unstable.delete(ws);
  ws.room = null;

  if (room.clients.size === 0) { rooms.delete(room.code); return; }

  broadcast(room, { type: 'peerLeft', peers: room.clients.size });

  // A disconnect counts as a connection problem: pause the rest if enabled.
  if (room.autoPause && room.state.playing) {
    room.state.time = currentTime(room);
    room.state.playing = false;
    room.state.updatedAt = Date.now();
    broadcast(room, { type: 'forcePause', reason: 'peer_disconnected', time: room.state.time });
  }
}

// ---- Message handling -----------------------------------------
function handle(ws, msg) {
  switch (msg.type) {
    case 'create': {
      if (ws.room) leaveRoom(ws);
      const code = makeRoomCode();
      const room = {
        code,
        clients: new Set([ws]),
        state: { videoUrl: msg.url || null, playing: false, time: 0, updatedAt: Date.now() },
        autoPause: msg.autoPause !== undefined ? !!msg.autoPause : true, // strict vs relaxed
        roomName: typeof msg.roomName === 'string' ? msg.roomName.slice(0, 60) : null,
        unstable: new Set(),
      };
      rooms.set(code, room);
      ws.room = room;
      send(ws, { type: 'created', room: code, roomName: room.roomName, autoPause: room.autoPause });
      break;
    }

    case 'join': {
      const code = (msg.room || '').toUpperCase();
      const room = rooms.get(code);
      if (!room) return send(ws, { type: 'error', code: 'no_room', message: 'Room not found.' });
      if (room.clients.size >= MAX_CLIENTS_PER_ROOM)
        return send(ws, { type: 'error', code: 'room_full', message: 'Room is full.' });
      if (ws.room) leaveRoom(ws);
      room.clients.add(ws);
      ws.room = room;
      // Snapshot the newcomer onto the current state.
      send(ws, {
        type: 'joined',
        room: code,
        roomName: room.roomName,
        peers: room.clients.size,
        autoPause: room.autoPause,
        video: room.state.videoUrl,
        playing: room.state.playing,
        time: currentTime(room),
      });
      broadcast(room, { type: 'peerJoined', peers: room.clients.size }, ws);
      break;
    }

    case 'loadVideo': {
      const room = ws.room; if (!room) return needRoom(ws);
      room.state.videoUrl = msg.url || null;
      room.state.time = 0;
      room.state.playing = false;
      room.state.updatedAt = Date.now();
      broadcast(room, { type: 'video', url: room.state.videoUrl }, ws);
      break;
    }

    case 'play': {
      const room = ws.room; if (!room) return needRoom(ws);
      if (evaluateStability(room)) {
        return send(ws, { type: 'forcePause', reason: 'network_unstable', time: currentTime(room) });
      }
      room.state.playing = true;
      room.state.time = typeof msg.time === 'number' ? msg.time : currentTime(room);
      room.state.updatedAt = Date.now();
      broadcast(room, { type: 'play', time: room.state.time }, ws);
      break;
    }

    case 'pause': {
      const room = ws.room; if (!room) return needRoom(ws);
      room.state.playing = false;
      room.state.time = typeof msg.time === 'number' ? msg.time : currentTime(room);
      room.state.updatedAt = Date.now();
      broadcast(room, { type: 'pause', time: room.state.time }, ws);
      break;
    }

    case 'seek': {
      const room = ws.room; if (!room) return needRoom(ws);
      room.state.time = typeof msg.time === 'number' ? msg.time : 0;
      room.state.updatedAt = Date.now();
      broadcast(room, { type: 'seek', time: room.state.time }, ws);
      break;
    }

    case 'netStatus': {
      const room = ws.room; if (!room) return needRoom(ws);
      if (msg.stable === false) room.unstable.add(ws);
      else room.unstable.delete(ws);
      evaluateStability(room);
      break;
    }

    case 'setAutoPause': {
      const room = ws.room; if (!room) return needRoom(ws);
      room.autoPause = !!msg.enabled;
      broadcast(room, { type: 'autoPause', enabled: room.autoPause });
      send(ws, { type: 'autoPause', enabled: room.autoPause });
      break;
    }

    case 'reaction': {
      const room = ws.room; if (!room) return needRoom(ws);
      const emoji = typeof msg.emoji === 'string' ? msg.emoji.slice(0, 8) : '';
      if (!emoji) return;
      broadcast(room, { type: 'reaction', emoji, from: msg.from || 'peer' }, ws);
      break;
    }

    case 'requestSync': {
      // Used by Relaxed mode's "Re-sync" button: catch the asker up to current state.
      const room = ws.room; if (!room) return needRoom(ws);
      send(ws, {
        type: 'syncState',
        video: room.state.videoUrl,
        playing: room.state.playing,
        time: currentTime(room),
      });
      break;
    }

    case 'ping':
      send(ws, { type: 'pong', t: Date.now() });
      break;

    default:
      send(ws, { type: 'error', code: 'unknown_type', message: `Unknown type: ${msg.type}` });
  }
}

// ---- Page -> video resolver ----------------------------------
// Best-effort: fetch a page and try to find a playable video URL.
// Works for HTML5 <video>/<source>, Open Graph / Twitter video meta,
// JSON-LD VideoObject, embedded YouTube, or a direct media URL in the HTML.
// Does NOT defeat DRM or run a site's JavaScript.

function absolutize(maybeUrl, baseUrl) {
  if (!maybeUrl) return null;
  try { return new URL(maybeUrl, baseUrl).toString(); } catch { return null; }
}

function youTubeId(url) {
  let m = url.match(/[?&]v=([\w-]{11})/); if (m) return m[1];
  m = url.match(/youtu\.be\/([\w-]{11})/); if (m) return m[1];
  m = url.match(/youtube\.com\/(?:embed|shorts)\/([\w-]{11})/); if (m) return m[1];
  return null;
}

// Pull the first plausible video out of raw HTML. Pure function -> easy to test.
function extractVideo(html, baseUrl) {
  if (!html) return { ok: false, reason: 'empty page' };

  const metaPatterns = [
    /<meta[^>]+property=["']og:video:secure_url["'][^>]+content=["']([^"']+)["']/i,
    /<meta[^>]+property=["']og:video:url["'][^>]+content=["']([^"']+)["']/i,
    /<meta[^>]+property=["']og:video["'][^>]+content=["']([^"']+)["']/i,
    /<meta[^>]+name=["']twitter:player:stream["'][^>]+content=["']([^"']+)["']/i,
    // content-first ordering variants
    /<meta[^>]+content=["']([^"']+)["'][^>]+property=["']og:video(?::secure_url|:url)?["']/i,
  ];
  for (const re of metaPatterns) {
    const m = html.match(re);
    if (m) {
      const u = absolutize(m[1].replace(/&amp;/g, '&'), baseUrl);
      const yt = u && youTubeId(u);
      if (yt) return { ok: true, type: 'youtube', videoId: yt, url: u, via: 'og:video' };
      if (u) return { ok: true, type: 'direct', url: u, via: 'og:video' };
    }
  }

  // JSON-LD VideoObject contentUrl
  let m = html.match(/"contentUrl"\s*:\s*"([^"]+\.(?:mp4|m3u8|webm)[^"]*)"/i);
  if (m) { const u = absolutize(m[1].replace(/\\\//g, '/'), baseUrl); if (u) return { ok: true, type: 'direct', url: u, via: 'json-ld' }; }

  // <video src> or <source src>
  m = html.match(/<video[^>]+src=["']([^"']+)["']/i)
    || html.match(/<source[^>]+src=["']([^"']+\.(?:mp4|m3u8|webm)[^"']*)["']/i);
  if (m) { const u = absolutize(m[1], baseUrl); if (u) return { ok: true, type: 'direct', url: u, via: 'video-tag' }; }

  // Embedded YouTube iframe
  m = html.match(/(?:src|href)=["']([^"']*youtube(?:-nocookie)?\.com\/embed\/[\w-]{11}[^"']*)["']/i);
  if (m) { const yt = youTubeId(m[1]); if (yt) return { ok: true, type: 'youtube', videoId: yt, url: absolutize(m[1], baseUrl), via: 'iframe' }; }

  // Any direct media URL anywhere in the HTML (last resort)
  m = html.match(/https?:\/\/[^"'\s<>]+\.(?:mp4|m3u8|webm)(?:\?[^"'\s<>]*)?/i);
  if (m) { return { ok: true, type: 'direct', url: m[0].replace(/\\\//g, '/'), via: 'inline' }; }

  return { ok: false, reason: 'no playable video found on that page' };
}

async function resolveVideo(pageUrl) {
  const url = (pageUrl || '').trim();
  if (!/^https?:\/\//i.test(url)) return { ok: false, reason: 'link must start with http(s)://' };

  // Already a direct video or a YouTube link? Hand it straight back.
  const yt = youTubeId(url);
  if (yt) return { ok: true, type: 'youtube', videoId: yt, url, via: 'direct' };
  if (/\.(mp4|m3u8|webm|mov|mkv|m4v)(\?.*)?$/i.test(url)) return { ok: true, type: 'direct', url, via: 'direct' };

  // Fetch the page (Node 18+ has global fetch).
  const controller = new AbortController();
  const t = setTimeout(() => controller.abort(), 8000);
  try {
    const resp = await fetch(url, {
      signal: controller.signal,
      redirect: 'follow',
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml',
      },
    });
    const ctype = resp.headers.get('content-type') || '';
    if (/^(video|application\/(?:x-mpegurl|vnd\.apple\.mpegurl|octet-stream))/i.test(ctype)) {
      return { ok: true, type: 'direct', url, via: 'content-type' }; // the link WAS the video
    }
    if (!/text\/html|xml/i.test(ctype)) {
      return { ok: false, reason: `that link is a ${ctype.split(';')[0] || 'non-page'} resource, not a video page` };
    }
    const html = (await resp.text()).slice(0, 600000); // cap
    return extractVideo(html, resp.url || url);
  } catch (e) {
    return { ok: false, reason: e.name === 'AbortError' ? 'page took too long to load' : 'could not fetch that page' };
  } finally {
    clearTimeout(t);
  }
}

// ---- Server (shared HTTP + WebSocket) ------------------------
const httpServer = http.createServer((req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'POST, GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') { res.writeHead(204); return res.end(); }

  if (req.method === 'POST' && req.url === '/resolve') {
    let body = '';
    req.on('data', (c) => { body += c; if (body.length > 100000) req.destroy(); });
    req.on('end', async () => {
      let pageUrl;
      try { pageUrl = JSON.parse(body).url; }
      catch { res.writeHead(400, { 'Content-Type': 'application/json' }); return res.end(JSON.stringify({ ok: false, reason: 'bad request' })); }
      const result = await resolveVideo(pageUrl);
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(result));
    });
    return;
  }

  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('WatchSync server is running.');
});

const wss = new WebSocketServer({ server: httpServer });

wss.on('connection', (ws) => {
  ws.room = null;
  ws.rate = [];
  send(ws, { type: 'hello', message: 'connected' });

  ws.on('message', (data) => {
    // Rolling-window rate limit (per connection).
    const now = Date.now();
    ws.rate = ws.rate.filter((t) => now - t < RATE_LIMIT_WINDOW_MS);
    if (ws.rate.length >= RATE_LIMIT_MAX) {
      return send(ws, { type: 'error', code: 'rate_limited', message: 'Too many messages, slow down.' });
    }
    ws.rate.push(now);

    let msg;
    try { msg = JSON.parse(data.toString()); }
    catch { return send(ws, { type: 'error', code: 'bad_json', message: 'Invalid message.' }); }
    if (!msg || typeof msg.type !== 'string') {
      return send(ws, { type: 'error', code: 'bad_msg', message: 'Missing message type.' });
    }
    handle(ws, msg);
  });

  ws.on('close', () => leaveRoom(ws));
  ws.on('error', () => { /* a close event will follow; nothing to do */ });
});

const portIsNumber = /^\d+$/.test(String(PORT));
if (portIsNumber) {
  httpServer.listen(Number(PORT), '0.0.0.0', () => {
    console.log(`WatchSync server listening on http+ws://0.0.0.0:${PORT}`);
  });
} else {
  // Some hosts (e.g. Passenger) pass a socket path instead of a port number.
  httpServer.listen(PORT, () => {
    console.log(`WatchSync server listening on ${PORT}`);
  });
}

module.exports = { wss, resolveVideo, extractVideo };
