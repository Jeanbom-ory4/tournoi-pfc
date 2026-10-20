// API unique du tournoi PFC (Vercel Serverless Function) — Redis (Upstash)
const { Redis } = require('@upstash/redis');
const crypto = require('crypto');

const redis = new Redis({
  url: process.env.UPSTASH_REDIS_REST_URL || process.env.KV_REST_API_URL,
  token: process.env.UPSTASH_REDIS_REST_TOKEN || process.env.KV_REST_API_TOKEN,
});
const SECRET = process.env.SESSION_SECRET || 'changez-moi';
const ADMIN = process.env.ADMIN_KEY;
const BEATS = { r: 'c', f: 'r', c: 'f' };
const CHARS = 'ABCDEFGHJKMNPQRSTUVWXYZ23456789';
const DELAY = (Number(process.env.DEADLINE_HOURS) || 48) * 3600e3; // délai pour jouer après le coup de l'adversaire
const MAX_TRIES = Number(process.env.MAX_TRIES) || 7;                  // mauvais codes avant blocage
const LOCK = (Number(process.env.LOCK_MINUTES) || 3) * 60e3;           // durée du blocage d'un joueur
const lockKey = (cfg, id) => 'pfc:lock:' + (cfg.tid || '') + ':' + id;
const hrs = (ms) => Math.round(ms / 3600e3 * 100) / 100;
const mk = (r, i) => `m${r}_${i}`;
const hash = (s) => crypto.createHash('sha256').update(s + SECRET).digest('hex');
// Le jeton est lié à l'identifiant du tournoi (tid) : après une réinitialisation, les anciens jetons ne sont plus valables
const sign = (id, tid) => `${id}.${tid}.` + crypto.createHmac('sha256', SECRET).update(`${id}.${tid}`).digest('hex').slice(0, 32);
const who = (t, tid) => { const id = String(t || '').split('.')[0]; return id && sign(id, tid || '') === t ? id : null; };
const isAdmin = (k) => !!ADMIN && String(k || '').trim() === ADMIN.trim();
const cnt = (cfg, r) => cfg.counts ? cfg.counts[r] : cfg.size / 2 ** (r + 1);   // nombre de matchs du tour r (ancien format : puissance de 2)
const ids = (cfg) => { const o = []; for (let r = 0; r < cfg.rounds; r++) for (let i = 0; i < cnt(cfg, r); i++) o.push(mk(r, i)); if (cfg.rounds >= 2) o.push('third'); return o; };
// Où va le gagnant du match i du tour r (dans le tour r+1). Quand un tour a un nombre impair de joueurs, un joueur est exempté ;
// l'exempté alterne entre la fin (tours pairs) et le début (tours impairs) du tableau, pour ne pas favoriser toujours le même joueur.
const route = (cfg, r, i) => {
  if (cnt(cfg, r) % 2 === 1 && (r + 1) % 2 === 1) return i === 0 ? { t: 0, side: 'a' } : { t: ((i - 1) >> 1) + 1, side: (i - 1) % 2 ? 'b' : 'a' };
  return { t: i >> 1, side: i % 2 ? 'b' : 'a' };
};

async function load() {
  const cfg = await redis.get('pfc:cfg');
  if (!cfg) return { cfg: null, matches: {} };
  const list = ids(cfg), ms = await redis.mget(...list.map((i) => 'pfc:m:' + i)), matches = {};
  list.forEach((id, k) => { if (ms[k]) matches[id] = ms[k]; });
  return { cfg, matches };
}
// Vue publique : ni PIN, et le coup en attente n'est JAMAIS renvoyé (seulement qui a joué)
const pub = ({ cfg, matches }) => ({
  cfg: cfg && { players: cfg.players.map(({ id, name }) => ({ id, name })), rounds: cfg.rounds, size: cfg.size || null, counts: cfg.counts || null, hours: hrs(DELAY), started: cfg.started !== false, tid: cfg.tid || '', startAt: cfg.startAt || null },
  matches: Object.fromEntries(Object.entries(matches).map(([k, { rem, ...m }]) => [k, { ...m, pend: m.pend ? { by: m.pend.by } : null }])),
});
const out = (st) => ({ ...pub(st), now: Date.now() });
// Présence « en ligne » : chaque appli ouverte envoie un signal toutes les ~20 s ; la clé expire seule après 90 s.
// Un joueur qui a choisi « Apparaître hors ligne » (b.hid) n'écrit rien. On ne renvoie que les joueurs des matchs en cours.
const seenKey = (cfg, id) => 'pfc:seen:' + (cfg.tid || '') + ':' + id;
async function presence(cfg, matches, b) {
  const me = who(b.token, cfg.tid || '');
  if (me && cfg.players.some((p) => p.id === me)) {
    if (b.hid) await redis.del(seenKey(cfg, me)); else await redis.set(seenKey(cfg, me), 1, { ex: 90 });
  }
  const want = new Set();
  for (const m of Object.values(matches)) if (m && m.a && m.b && !m.w) { want.add(m.a); want.add(m.b); }
  const l = [...want];
  if (!l.length) return [];
  const v = await redis.mget(...l.map((i) => seenKey(cfg, i)));
  return l.filter((_, k) => v[k]);
}
// Sondage anonyme : une réponse par joueur, rangée sous un code haché (le nom n'est jamais stocké ni affiché).
// Réponse possible seulement quand le joueur n'a plus de match à jouer : éliminé, ou vainqueur de la finale / petite finale.
const svKey = (cfg) => 'pfc:sv:' + (cfg.tid || '');   // les réponses sont propres au tournoi en cours : la réinitialisation les efface
const svId = (cfg, id) => crypto.createHmac('sha256', SECRET).update('sv:' + (cfg.tid || '') + ':' + id).digest('hex').slice(0, 24);
const svOk = (cfg, matches, id) => {
  const fin = matches[mk(cfg.rounds - 1, 0)], th = matches.third;
  if (fin && fin.w === id) return true;
  if (th && th.w === id) return true;
  return Object.values(matches).some((m) => m && m.w && m.a && m.b && (m.a === id || m.b === id) && m.w !== id);
};
async function svResults(cfg) {
  const all = (await redis.hgetall(svKey(cfg))) || {}, rows = Object.values(all).map((v) => (typeof v === 'string' ? JSON.parse(v) : v));
  const dist = Array(10).fill(0); let sum = 0, n = 0;
  rows.forEach((r) => { if (r.s >= 1 && r.s <= 10) { dist[r.s - 1]++; sum += r.s; n++; } });
  const ideas = rows.filter((r) => r.i).sort((x, y) => y.t - x.t).map((r) => ({ i: r.i, t: r.t }));
  return { total: rows.length, n, avg: n ? Math.round(sum / n * 10) / 10 : null, dist, ideas };
}
async function wipe() {
  const cfg = await redis.get('pfc:cfg');
  if (cfg) await Promise.all([...ids(cfg).map((i) => redis.del('pfc:m:' + i)), ...cfg.players.map((p) => redis.del(seenKey(cfg, p.id))), redis.del(svKey(cfg))]);   // matchs, présence et sondage
  await redis.del('pfc:cfg', 'pfc:pins', 'pfc:emails'); // 'pfc:emails' : nettoyage d'anciennes données
}
// Termine un match : désigne le vainqueur (ff = joueur éliminé pour dépassement de délai) et le fait avancer
async function finish(cfg, m, win, ff) {
  m.w = win; m.pend = null; m.due = null;
  if (ff) m.ff = ff;
  if (m.r + 1 < cfg.rounds && m.id !== 'third') {
    const { t, side } = route(cfg, m.r, m.i), nk = 'pfc:m:' + mk(m.r + 1, t), nm = await redis.get(nk);
    nm[side] = win;
    if (nm.bye) await finish(cfg, nm, win, null); // match « exempté » : le joueur passe directement au tour suivant
    else await redis.set(nk, nm);
  }
  // Le perdant d'une demi-finale rejoint la petite finale (sauf s'il est éliminé pour dépassement de délai)
  if (cfg.rounds >= 2 && m.id !== 'third' && m.r === cfg.rounds - 2) {
    const loser = win === m.a ? m.b : m.a, tk = 'pfc:m:third', t = await redis.get(tk);
    t[m.i % 2 ? 'b' : 'a'] = ff ? null : loser;
    t.got = (t.got || 0) + 1;
    if (t.got >= 2 && (!t.a || !t.b) && !t.w) t.w = t.a || t.b || 'none'; // un seul demi-finaliste : 3e place d'office
    await redis.set(tk, t);
  }
  await redis.set('pfc:m:' + (m.id || mk(m.r, m.i)), m);
}
// Applique les éliminations dont le délai est dépassé (appelé à chaque lecture de l'état et à chaque coup)
async function sweep(st) {
  if (!st.cfg) return st;
  let changed = false;
  for (const m of Object.values(st.matches)) {
    if (!m.w && m.pend && m.due && Date.now() > m.due) {
      const byA = m.pend.by === 'a';
      await finish(st.cfg, m, byA ? m.a : m.b, byA ? m.b : m.a);
      changed = true;
    }
  }
  return changed ? load() : st;
}
const newPin = () => Array.from({ length: 4 }, () => CHARS[crypto.randomInt(CHARS.length)]).join('');
// Construit le tableau à partir de la liste des joueurs : tous les joueurs jouent dès le tour 1 (par paires) ;
// si le nombre de joueurs d'un tour est impair, un joueur est exempté et passe directement au tour suivant.
function build(players, doShuffle) {
  const order = players.map((p) => p.id);
  if (doShuffle) for (let i = order.length - 1; i > 0; i--) { const j = crypto.randomInt(i + 1); [order[i], order[j]] = [order[j], order[i]]; }
  const counts = []; for (let p = order.length; ;) { const m = Math.ceil(p / 2); counts.push(m); if (m === 1) break; p = m; }
  const rounds = counts.length, M = {};
  for (let r = 0; r < rounds; r++) for (let i = 0; i < counts[r]; i++)
    M[mk(r, i)] = { id: mk(r, i), r, i, a: null, b: null, sa: 0, sb: 0, hist: [], pend: null, w: null, need: r === rounds - 1 ? 5 : 3 };
  if (rounds >= 2) M.third = { id: 'third', r: rounds - 1, i: 0, a: null, b: null, sa: 0, sb: 0, hist: [], pend: null, w: null, need: 3, got: 0 };
  order.forEach((id, k) => { M[mk(0, k >> 1)][k % 2 ? 'b' : 'a'] = id; });
  for (let r = 0; r < rounds; r++) {
    const p = r === 0 ? order.length : counts[r - 1];
    if (p % 2 === 1) M[mk(r, r % 2 === 1 ? 0 : counts[r] - 1)].bye = true;
  }
  const cfg = { rounds, counts };
  for (let i = 0; i < counts[0]; i++) {   // exempté(s) du tour 1 : ils avancent tout de suite
    let m = M[mk(0, i)];
    while (m && m.bye && !m.w && (m.a || m.b)) {
      m.w = m.a || m.b;
      if (rounds >= 2 && m.r === rounds - 2) M.third.got++;   // demi-finale exemptée : pas de perdant
      if (m.r + 1 >= rounds) break;
      const { t, side } = route(cfg, m.r, m.i), nm = M[mk(m.r + 1, t)];
      nm[side] = m.w; m = nm;
    }
  }
  return { rounds, counts, M };
}

module.exports = async (req, res) => {
  try {
    const b = req.body || {}, a = b.action;
    if (!(process.env.UPSTASH_REDIS_REST_URL || process.env.KV_REST_API_URL) || !(process.env.UPSTASH_REDIS_REST_TOKEN || process.env.KV_REST_API_TOKEN))
      return res.status(500).json({ error: 'Redis non configuré : ajoute une base Upstash Redis au projet Vercel puis redéploie.' });
    if (a === 'state') {
      const st = await sweep(await load()), o = out(st);
      if (b.ping && st.cfg) { try { o.on = await presence(st.cfg, st.matches, b); } catch (e) { /* la présence est facultative */ } }
      return res.json(o);
    }

    if (a === 'login') {
      const cfg = await redis.get('pfc:cfg'), p = cfg && cfg.players.find((x) => x.id === b.id);
      if (!p) return res.status(401).json({ error: 'pin' });
      const lk = lockKey(cfg, p.id), st = (await redis.get(lk)) || { n: 0, until: 0 }, ex = Math.ceil(LOCK / 1000) + 1800;
      if (st.until > Date.now()) return res.status(429).json({ error: 'locked', retry: Math.ceil((st.until - Date.now()) / 1000) }); // bloqué : même le bon code est refusé
      if (hash(String(b.pin).toUpperCase() + p.id) !== p.h) {
        st.n = (st.n || 0) + 1;
        if (st.n >= MAX_TRIES) { st.until = Date.now() + LOCK; st.n = 0; await redis.set(lk, st, { ex }); return res.status(429).json({ error: 'locked', retry: Math.ceil(LOCK / 1000) }); }
        await redis.set(lk, st, { ex });
        return res.status(401).json({ error: 'pin', left: MAX_TRIES - st.n });
      }
      await redis.del(lk); // bon code : le compteur d'essais est remis à zéro
      return res.json({ token: sign(p.id, cfg.tid || '') });
    }

    if (a === 'quit') {   // abandon volontaire : le joueur est éliminé, son adversaire passe au tour suivant
      const c0 = await redis.get('pfc:cfg'), me = c0 && who(b.token, c0.tid);
      if (!me || !c0.players.some((p) => p.id === me)) return res.status(401).json({ error: 'auth' });
      if (c0.started === false) return res.status(409).json({ error: 'notstarted' });
      const st = await sweep(await load()), m = st.matches[b.id];
      if (!m || m.w || !m.a || !m.b || (m.a !== me && m.b !== me)) return res.status(400).json({ error: 'match' });
      m.ab = me;
      await finish(st.cfg, m, m.a === me ? m.b : m.a, me);
      return res.json(out(await load()));
    }

    if (a === 'surveyget' || a === 'survey') {   // sondage (joueur identifié par son jeton)
      const cfg = await redis.get('pfc:cfg'), me = cfg && who(b.token, cfg.tid || '');
      if (!me || !cfg.players.some((p) => p.id === me)) return res.status(401).json({ error: 'auth' });
      const st = await load();
      if (!svOk(cfg, st.matches, me)) return res.status(409).json({ error: 'notyet' });
      const f = svId(cfg, me);
      if (a === 'surveyget') { const v = await redis.hget(svKey(cfg), f); return res.json({ r: v ? (typeof v === 'string' ? JSON.parse(v) : { s: v.s, i: v.i }) : null }); }
      const sc = b.score === null || b.score === undefined || b.score === '' ? null : Number(b.score), idea = String(b.idea || '').replace(/\s+/g, ' ').trim().slice(0, 500);
      if (sc !== null && !(Number.isInteger(sc) && sc >= 1 && sc <= 10)) return res.status(400).json({ error: 'note' });
      if (sc === null && !idea) return res.status(400).json({ error: 'vide' });
      await redis.hset(svKey(cfg), { [f]: { s: sc, i: idea, t: Date.now() } });
      return res.json({ ok: 1 });
    }

    if (a === 'play') {
      const c0 = await redis.get('pfc:cfg'), me = c0 && who(b.token, c0.tid);
      if (!me || !c0.players.some((p) => p.id === me)) return res.status(401).json({ error: 'auth' });
      if (!['r', 'f', 'c'].includes(b.mv)) return res.status(400).json({ error: 'coup' });
      if (c0.started === false) return res.status(409).json({ error: 'notstarted' });
      const st = await sweep(await load()), cfg = st.cfg, m = st.matches[b.id], key = 'pfc:m:' + b.id;
      if (m && m.w && m.ff === me) return res.status(409).json({ error: 'deadline' });
      if (!m || m.w || !m.a || !m.b || (m.a !== me && m.b !== me)) return res.status(400).json({ error: 'match' });
      const side = m.a === me ? 'a' : 'b';
      if (!m.pend) {
        m.pend = { by: side, mv: b.mv }; m.due = Date.now() + DELAY;
        await redis.set(key, m);
      } else if (m.pend.by !== side) {
        const o = m.pend.mv, ma = side === 'a' ? b.mv : o, mb = side === 'a' ? o : b.mv;
        const w = ma === mb ? '=' : BEATS[ma] === mb ? 'a' : 'b';
        if (w === 'a') m.sa++; if (w === 'b') m.sb++;
        m.hist.push({ a: ma, b: mb, w });
        m.pend = null;
        m.due = null;
        const need = m.need || 3, win = m.sa >= need ? m.a : m.sb >= need ? m.b : null;
        if (win) await finish(cfg, m, win, null); else await redis.set(key, m);
      }
      return res.json(out(await load()));
    }

    // --- Actions organisateur (clé ADMIN_KEY) ---
    if (['create', 'reset', 'admin', 'extend', 'forfeit', 'start', 'unlock', 'schedule', 'addplayers', 'removeplayer', 'renameplayer'].includes(a)) {
      if (!ADMIN) return res.status(500).json({ error: 'Variable ADMIN_KEY absente sur ce déploiement : vérifie son nom exact, coche Production, puis redéploie.' });
      if (!isAdmin(b.key)) return res.status(401).json({ error: 'admin' });
      if (a === 'start') {
        const cfg = await redis.get('pfc:cfg');
        if (!cfg) return res.status(400).json({ error: 'tournoi' });
        cfg.started = true; await redis.set('pfc:cfg', cfg);
        return res.json({ ok: 1 });
      }
      if (a === 'extend' || a === 'forfeit') {
        const cfg = await redis.get('pfc:cfg'), key = 'pfc:m:' + b.id, m = await redis.get(key);
        if (!cfg || !m || m.w) return res.status(400).json({ error: 'match' });
        if (a === 'extend') {
          if (!m.pend) return res.status(400).json({ error: 'match' });
          m.due = Math.max(m.due || 0, Date.now()) + 24 * 3600e3; await redis.set(key, m);
        } else {
          if (b.loser !== m.a && b.loser !== m.b) return res.status(400).json({ error: 'joueur' });
          await finish(cfg, m, b.loser === m.a ? m.b : m.a, b.loser);
        }
        return res.json({ ok: 1 });
      }
      if (a === 'schedule') {
        const cfg = await redis.get('pfc:cfg');
        if (!cfg) return res.status(400).json({ error: 'tournoi' });
        const at = b.at ? Number(b.at) : null;
        if (b.at && !Number.isFinite(at)) return res.status(400).json({ error: 'date' });
        cfg.startAt = at; await redis.set('pfc:cfg', cfg);
        return res.json({ ok: 1 });
      }
      if (a === 'renameplayer') {
        const cfg = await redis.get('pfc:cfg');
        if (!cfg) return res.status(400).json({ error: 'tournoi' });
        if (cfg.started !== false) return res.status(409).json({ error: 'started' });
        const p = cfg.players.find((x) => x.id === b.id);
        if (!p) return res.status(400).json({ error: 'joueur' });
        const name = String(b.name || '').replace(/\s+/g, ' ').trim().slice(0, 60);
        if (!name) return res.status(400).json({ error: 'vide' });
        if (cfg.players.some((x) => x.id !== p.id && x.name.trim().toLowerCase() === name.toLowerCase())) return res.status(400).json({ error: 'doublon' });
        p.name = name; // l'identifiant, le code et le tirage au sort ne changent pas
        await redis.set('pfc:cfg', cfg);
        return res.json({ ok: 1 });
      }
      if (a === 'removeplayer') {
        const cfg = await redis.get('pfc:cfg');
        if (!cfg) return res.status(400).json({ error: 'tournoi' });
        if (cfg.started !== false) return res.status(409).json({ error: 'started' });
        if (!cfg.players.some((p) => p.id === b.id)) return res.status(400).json({ error: 'joueur' });
        if (cfg.players.length <= 2) return res.status(400).json({ error: 'min2' });
        const players = cfg.players.filter((p) => p.id !== b.id), pins = (await redis.get('pfc:pins')) || {};
        delete pins[b.id];
        const nextId = Math.max(cfg.nextId || 0, ...cfg.players.map((p) => Number(p.id.slice(1)) + 1)); // l'identifiant retiré ne sera jamais réattribué
        const { rounds, counts, M } = build(players, true); // nouveau tirage au sort avec les joueurs restants ; leurs codes ne changent pas
        const { size: _ancien, ...cfgRest } = cfg;
        await Promise.all(ids(cfg).map((i) => redis.del('pfc:m:' + i)));
        await Promise.all([
          redis.del(lockKey(cfg, b.id)), redis.set('pfc:cfg', { ...cfgRest, players, rounds, counts, nextId }), redis.set('pfc:pins', pins),
          ...Object.entries(M).map(([id, m]) => redis.set('pfc:m:' + id, m)),
        ]);
        return res.json({ ok: 1 });
      }
      if (a === 'addplayers') {
        const cfg = await redis.get('pfc:cfg');
        if (!cfg) return res.status(400).json({ error: 'tournoi' });
        if (cfg.started !== false) return res.status(409).json({ error: 'started' });
        const have = new Set(cfg.players.map((p) => p.name.trim().toLowerCase())), seen = new Set(), add = [];
        for (const name of String(b.text || '').split('\n').map((x) => x.trim()).filter(Boolean)) {
          const k = name.toLowerCase();
          if (!have.has(k) && !seen.has(k)) { seen.add(k); add.push(name); }
        }
        if (!add.length) return res.status(400).json({ error: 'none' });
        const pins = (await redis.get('pfc:pins')) || {}, players = [...cfg.players];
        let n = Math.max(cfg.nextId || 0, players.reduce((m, p) => Math.max(m, Number(p.id.slice(1)) + 1), 0)); // un identifiant retiré n'est jamais réutilisé
        for (const name of add) { const id = 'p' + n++, pin = newPin(); pins[id] = pin; players.push({ id, name, h: hash(pin + id) }); }
        const { rounds, counts, M } = build(players, true);
        const { size: _ancien, ...cfgRest } = cfg; // nouveau tirage au sort ; les codes et connexions existants restent valables
        await Promise.all(ids(cfg).map((i) => redis.del('pfc:m:' + i)));
        await Promise.all([
          redis.set('pfc:cfg', { ...cfgRest, players, rounds, counts, nextId: n }), redis.set('pfc:pins', pins),
          ...Object.entries(M).map(([id, m]) => redis.set('pfc:m:' + id, m)),
        ]);
        return res.json({ ok: 1, added: add.length });
      }
      if (a === 'unlock') {
        const cfg = await redis.get('pfc:cfg');
        if (!cfg || !cfg.players.some((p) => p.id === b.id)) return res.status(400).json({ error: 'joueur' });
        await redis.del(lockKey(cfg, b.id));
        return res.json({ ok: 1 });
      }
      if (a === 'admin') {
        const cfg = await redis.get('pfc:cfg'), locks = {};
        if (cfg) for (const p of cfg.players) { const st = await redis.get(lockKey(cfg, p.id)); if (st && st.until > Date.now()) locks[p.id] = st.until; }
        return res.json({ pins: (await redis.get('pfc:pins')) || {}, locks, survey: cfg ? await svResults(cfg) : null });
      }
      if (a === 'reset') { await wipe(); return res.json({ ok: 1 }); }

      const seen = new Set(), names = [];
      for (const name of String(b.text || '').split('\n').map((s) => s.trim()).filter(Boolean)) {
        if (!seen.has(name)) { seen.add(name); names.push({ name }); }
      }
      if (names.length < 2) return res.status(400).json({ error: 'min2' });
      await wipe();
      for (let i = names.length - 1; i > 0; i--) { const j = crypto.randomInt(i + 1); [names[i], names[j]] = [names[j], names[i]]; }
      const players = [], pins = {};
      names.forEach((n, i) => { const id = 'p' + i, pin = newPin(); pins[id] = pin; players.push({ id, name: n.name, h: hash(pin + id) }); });
      const { rounds, counts, M } = build(players, false);
      await Promise.all([
        redis.set('pfc:cfg', { players, rounds, counts, nextId: players.length, started: false, tid: crypto.randomBytes(4).toString('hex') }), redis.set('pfc:pins', pins),
        ...Object.entries(M).map(([id, m]) => redis.set('pfc:m:' + id, m)),
      ]);
      return res.json({ ok: 1 });
    }
    res.status(400).json({ error: 'action' });
  } catch (e) {
    res.status(500).json({ error: String(e.message || e) });
  }
};
