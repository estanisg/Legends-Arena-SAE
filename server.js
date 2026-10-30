// Legends Arena · servidor de cuentas y ranking
// Sin dependencias externas: solo Node.js >= 18.
//
// Variables de entorno (todas opcionales):
//   PORT         puerto (por defecto 3000)
//   DATA_DIR     carpeta donde se guarda db.json (por defecto ./data). En hosting
//                usa un disco/volumen persistente o los datos se perderán al reiniciar.
//   CORS_ORIGIN  origen permitido si el juego se sirve desde otro dominio (por defecto *)
//   SESSION_DAYS duración de la sesión en días (por defecto 60)
//   UPSTASH_REDIS_REST_URL / UPSTASH_REDIS_REST_TOKEN
//                si existen, los datos se guardan en Upstash Redis (gratis, persistente)
//                en lugar de en un archivo. Ideal para hostings gratuitos sin disco.

'use strict';
const http = require('http');
const fs = require('fs');
const path = require('path');
const zlib = require('zlib');
const crypto = require('crypto');

const PORT = +process.env.PORT || 3000;
const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, 'data');
const CORS_ORIGIN = process.env.CORS_ORIGIN || '*';
const SESSION_MS = (+process.env.SESSION_DAYS || 60) * 864e5;
const PUBLIC_DIR = path.join(__dirname, 'public');
const DB_FILE = path.join(DATA_DIR, 'db.json');

const USER_RE = /^[A-Za-z0-9_]{3,12}$/; // 12 = límite del campo de nombre online del juego
const MAX_SCORE = 2_000_000;
const MAX_WAVE = 999;

// ---------------------------------------------------------------- base de datos
const REDIS_URL = (process.env.UPSTASH_REDIS_REST_URL || '').replace(/\/+$/, '');
const REDIS_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN || '';
const USE_REDIS = !!(REDIS_URL && REDIS_TOKEN);
const REDIS_KEY = 'legends-arena:db';
async function redis(cmd) {
  const r = await fetch(REDIS_URL, {
    method: 'POST',
    headers: { Authorization: 'Bearer ' + REDIS_TOKEN, 'Content-Type': 'application/json' },
    body: JSON.stringify(cmd)
  });
  const d = await r.json().catch(() => ({}));
  if (!r.ok || d.error) throw new Error(d.error || 'HTTP ' + r.status);
  return d.result;
}
if (!USE_REDIS) fs.mkdirSync(DATA_DIR, { recursive: true });
let db = { users: {}, sessions: {} };
async function loadDb() {
  if (USE_REDIS) {
    const raw = await redis(['GET', REDIS_KEY]); // si falla, el servidor no arranca (así nunca se pisan datos)
    if (raw) db = Object.assign(db, JSON.parse(raw));
    console.log('Datos en Upstash Redis · usuarios:', Object.keys(db.users).length);
  } else {
    try { db = Object.assign(db, JSON.parse(fs.readFileSync(DB_FILE, 'utf8'))); } catch (_) {}
    console.log('Datos en ' + DB_FILE + ' · usuarios:', Object.keys(db.users).length);
  }
}
let saveTimer = null, saving = Promise.resolve();
function saveNow() {
  clearTimeout(saveTimer); saveTimer = null;
  const data = JSON.stringify(db);
  if (USE_REDIS) {
    saving = saving.then(() => redis(['SET', REDIS_KEY, data])).catch(e => { console.error('Error guardando en Redis:', e.message); save(5000); });
  } else {
    const tmp = DB_FILE + '.tmp';
    fs.writeFileSync(tmp, data);
    fs.renameSync(tmp, DB_FILE); // escritura atómica
  }
  return saving;
}
function save(ms) { if (!saveTimer) saveTimer = setTimeout(saveNow, ms || 300); }
function cleanSessions() {
  const now = Date.now();
  for (const k of Object.keys(db.sessions)) if (db.sessions[k].exp < now) delete db.sessions[k];
}
setInterval(() => { cleanSessions(); save(); }, 36e5).unref();
for (const sig of ['SIGINT', 'SIGTERM']) process.on(sig, async () => { try { await Promise.race([saveNow(), new Promise(r => setTimeout(r, 4000))]); } catch (_) {} process.exit(0); });

// ---------------------------------------------------------------- utilidades
const sha = s => crypto.createHash('sha256').update(s).digest('hex');
function hashPw(pw, salt) { return crypto.scryptSync(pw, salt, 64).toString('hex'); }
function newSession(key) {
  const tok = crypto.randomBytes(32).toString('hex');
  db.sessions[sha(tok)] = { u: key, exp: Date.now() + SESSION_MS };
  save();
  return tok;
}
function authUser(req) {
  const h = req.headers.authorization || '';
  const tok = h.startsWith('Bearer ') ? h.slice(7).trim() : '';
  if (!tok) return null;
  const s = db.sessions[sha(tok)];
  if (!s || s.exp < Date.now()) return null;
  const u = db.users[s.u];
  return u ? { key: s.u, u, tokHash: sha(tok) } : null;
}
const pub = u => ({ name: u.name, best: u.best || 0, wave: u.wave || 0 });
function ranking() {
  return Object.values(db.users)
    .filter(u => u.best > 0)
    .sort((a, b) => b.best - a.best || a.bestAt - b.bestAt);
}

// Límite de intentos (en memoria)
const hits = new Map();
function limited(key, max, windowMs) {
  const now = Date.now();
  const arr = (hits.get(key) || []).filter(t => now - t < windowMs);
  if (arr.length >= max) { hits.set(key, arr); return true; }
  arr.push(now); hits.set(key, arr); return false;
}
setInterval(() => { const now = Date.now(); for (const [k, v] of hits) if (!v.some(t => now - t < 6e4)) hits.delete(k); }, 6e4).unref();
const ipOf = req => (req.headers['x-forwarded-for'] || '').split(',')[0].trim() || req.socket.remoteAddress || '?';

function send(res, code, obj) {
  const body = code === 204 ? '' : JSON.stringify(obj);
  res.writeHead(code, {
    'Content-Type': 'application/json; charset=utf-8',
    'Cache-Control': 'no-store',
    'Access-Control-Allow-Origin': CORS_ORIGIN,
    'Access-Control-Allow-Headers': 'Content-Type, Authorization',
    'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
    'Vary': 'Origin'
  });
  res.end(body);
}
function readBody(req) {
  return new Promise((resolve, reject) => {
    let n = 0; const chunks = [];
    req.on('data', c => { n += c.length; if (n > 4096) { reject(new Error('big')); req.destroy(); } else chunks.push(c); });
    req.on('end', () => { try { resolve(JSON.parse(Buffer.concat(chunks).toString() || '{}')); } catch (e) { reject(e); } });
    req.on('error', reject);
  });
}

// ---------------------------------------------------------------- API
async function api(req, res, url) {
  const route = url.pathname;
  if (req.method === 'OPTIONS') return send(res, 204, {});

  if (route === '/api/health') return send(res, 200, { ok: true });

  if (route === '/api/register' && req.method === 'POST') {
    if (limited('reg:' + ipOf(req), 5, 36e5)) return send(res, 429, { error: 'Demasiados registros desde tu red. Prueba más tarde.' });
    let b; try { b = await readBody(req); } catch (_) { return send(res, 400, { error: 'Solicitud inválida' }); }
    const name = String(b.username || '').trim(), pw = String(b.password || '');
    if (!USER_RE.test(name)) return send(res, 400, { error: 'Usuario: 3 a 12 caracteres (letras, números o _)' });
    if (pw.length < 6 || pw.length > 64) return send(res, 400, { error: 'La contraseña debe tener entre 6 y 64 caracteres' });
    const key = name.toLowerCase();
    if (db.users[key]) return send(res, 409, { error: 'Ese nombre de usuario ya existe' });
    const salt = crypto.randomBytes(16).toString('hex');
    db.users[key] = { name, salt, hash: hashPw(pw, salt), best: 0, wave: 0, bestAt: 0, created: Date.now(), lastScoreAt: 0 };
    const token = newSession(key); save();
    return send(res, 201, { token, user: pub(db.users[key]) });
  }

  if (route === '/api/login' && req.method === 'POST') {
    let b; try { b = await readBody(req); } catch (_) { return send(res, 400, { error: 'Solicitud inválida' }); }
    const name = String(b.username || '').trim(), pw = String(b.password || '');
    const key = name.toLowerCase();
    if (limited('log:' + ipOf(req), 20, 6e4) || limited('logu:' + key, 8, 6e4))
      return send(res, 429, { error: 'Demasiados intentos. Espera un minuto.' });
    const u = db.users[key];
    // se calcula el hash aunque el usuario no exista (evita revelar cuentas por tiempo de respuesta)
    const salt = u ? u.salt : 'x'.repeat(32);
    const h = Buffer.from(hashPw(pw.slice(0, 64), salt), 'hex');
    const ok = u && crypto.timingSafeEqual(h, Buffer.from(u.hash, 'hex'));
    if (!ok) return send(res, 401, { error: 'Usuario o contraseña incorrectos' });
    return send(res, 200, { token: newSession(key), user: pub(u) });
  }

  if (route === '/api/logout' && req.method === 'POST') {
    const a = authUser(req);
    if (a) { delete db.sessions[a.tokHash]; save(); }
    return send(res, 200, { ok: true });
  }

  if (route === '/api/me' && req.method === 'GET') {
    const a = authUser(req);
    if (!a) return send(res, 401, { error: 'Sesión caducada' });
    return send(res, 200, { user: pub(a.u) });
  }

  if (route === '/api/score' && req.method === 'POST') {
    const a = authUser(req);
    if (!a) return send(res, 401, { error: 'Sesión caducada' });
    let b; try { b = await readBody(req); } catch (_) { return send(res, 400, { error: 'Solicitud inválida' }); }
    const score = Math.floor(+b.score), wave = Math.floor(+b.wave) || 0;
    if (!Number.isFinite(score) || score < 0 || score > MAX_SCORE || wave < 0 || wave > MAX_WAVE)
      return send(res, 400, { error: 'Puntuación no válida' });
    if (Date.now() - (a.u.lastScoreAt || 0) < 3000) return send(res, 429, { error: 'Demasiado rápido' });
    a.u.lastScoreAt = Date.now();
    let improved = false;
    if (score > (a.u.best || 0)) { a.u.best = score; a.u.wave = wave; a.u.bestAt = Date.now(); improved = true; }
    save();
    const rank = ranking().findIndex(u => u.name === a.u.name) + 1;
    return send(res, 200, { improved, best: a.u.best, wave: a.u.wave, rank: rank || null });
  }

  if (route === '/api/leaderboard' && req.method === 'GET') {
    const limit = Math.max(1, Math.min(100, +url.searchParams.get('limit') || 50));
    const all = ranking();
    const top = all.slice(0, limit).map((u, i) => ({ rank: i + 1, name: u.name, score: u.best, wave: u.wave || 0 }));
    const a = authUser(req);
    let me = null;
    if (a && a.u.best > 0) { const i = all.indexOf(a.u); me = { rank: i + 1, name: a.u.name, score: a.u.best, wave: a.u.wave || 0 }; }
    return send(res, 200, { top, me, total: all.length });
  }

  return send(res, 404, { error: 'No encontrado' });
}

// ---------------------------------------------------------------- archivos estáticos (el juego)
const gzCache = new Map();
function serveGame(req, res, file) {
  fs.stat(file, (err, st) => {
    if (err || !st.isFile()) { res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' }); return res.end('Falta el juego: sube index.html junto a server.js'); }
    const type = file.endsWith('.html') ? 'text/html; charset=utf-8' : 'application/octet-stream';
    const etag = '"' + st.size + '-' + st.mtimeMs + '"';
    if (req.headers['if-none-match'] === etag) { res.writeHead(304); return res.end(); }
    const head = { 'Content-Type': type, 'ETag': etag, 'Cache-Control': 'no-cache' };
    const wantsGz = /\bgzip\b/.test(req.headers['accept-encoding'] || '');
    if (!wantsGz) { res.writeHead(200, head); return fs.createReadStream(file).pipe(res); }
    const c = gzCache.get(file);
    if (c && c.etag === etag) { res.writeHead(200, { ...head, 'Content-Encoding': 'gzip' }); return res.end(c.buf); }
    fs.readFile(file, (e, data) => {
      if (e) { res.writeHead(500); return res.end(); }
      zlib.gzip(data, { level: 6 }, (e2, buf) => {
        if (e2) { res.writeHead(500); return res.end(); }
        gzCache.set(file, { etag, buf });
        res.writeHead(200, { ...head, 'Content-Encoding': 'gzip' }); res.end(buf);
      });
    });
  });
}

const server = http.createServer(async (req, res) => {
  try {
    const url = new URL(req.url, 'http://x');
    if (url.pathname.startsWith('/api/')) return await api(req, res, url);
    if (req.method !== 'GET' && req.method !== 'HEAD') { res.writeHead(405); return res.end(); }
    const pf = path.join(PUBLIC_DIR, 'index.html');
    return serveGame(req, res, fs.existsSync(pf) ? pf : path.join(__dirname, 'index.html'));
  } catch (e) {
    console.error(e);
    if (!res.headersSent) send(res, 500, { error: 'Error del servidor' }); else res.end();
  }
});
loadDb().then(() => { cleanSessions(); server.listen(PORT, () => console.log('Legends Arena server en http://localhost:' + PORT)); })
  .catch(e => { console.error('No se pudo cargar la base de datos:', e.message); process.exit(1); });
