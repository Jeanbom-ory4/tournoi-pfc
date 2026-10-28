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
const DELAY = (Number(process.env.DEADLINE_HOURS) || 24) * 3600e3; // délai pour jouer après le coup de l'adversaire (le week-end ne compte pas : voir dueFrom)
// Échéance « week-end exclu » : on compte `ms` de temps ouvré (lundi à vendredi, heure de Paris) ; le samedi et le dimanche, l'horloge est arrêtée et reprend le lundi à 0 h.
const PARIS = new Intl.DateTimeFormat('en-GB', { timeZone: 'Europe/Paris', weekday: 'short', hour: 'numeric', minute: 'numeric', second: 'numeric', hourCycle: 'h23' });
const parisAt = (t) => { const o = {}; for (const x of PARIS.formatToParts(new Date(t))) o[x.type] = x.value; return { wk: o.weekday === 'Sat' || o.weekday === 'Sun', since: ((+o.hour * 60 + +o.minute) * 60 + +o.second) * 1000 }; };
function dueFrom(from, ms) {
  let t = from, rem = ms;
  for (let i = 0; i < 400 && rem > 0; i++) {
    const { wk, since } = parisAt(t), toMid = Math.max(1000, 86400000 - since);
    if (wk) { t += toMid; continue; }
    const step = Math.min(rem, toMid); t += step; rem -= step;
  }
  return t;
}
const MAX_TRIES = Number(process.env.MAX_TRIES) || 7;                  // mauvais codes avant blocage
const LOCK = (Number(process.env.LOCK_MINUTES) || 3) * 60e3;           // durée du blocage d'un joueur
const lockKey = (cfg, id) => 'pfc:lock:' + (cfg.tid || '') + ':' + id;
const helpKey = (cfg, id) => 'pfc:help:' + (cfg.tid || '') + ':' + id;   // « appel à l'aide » d'un joueur qui a perdu son code (expire seul après 7 jours)
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
// Lot du vainqueur de la finale : il choisit une seule fois entre « recevoir » et « offrir » ; seul l'organisateur voit le choix.
const pzKey = (cfg) => 'pfc:prize:' + (cfg.tid || '');
// Textes du lot : jamais présents dans la page, envoyés seulement au vainqueur de la finale (et à l'organisateur pour son résumé)
const PZ_UI = {
  intro: "Pour fêter ta victoire, l'organisateur est heureux de t'offrir une carte cadeau de 20 €.",
  ask: '🎁 Que souhaites-tu faire de ton lot ?',
  take: { label: '🛍️ Recevoir ma carte cadeau (20 €)', quote: '« Merci pour ce tournoi ! Je récupère mon lot. »', confirm: 'Confirmer : tu souhaites recevoir ta carte cadeau de 20 € ?\n\nTon choix sera définitif.', done: "✅ C'est noté : tu as choisi de recevoir ta carte cadeau (20 €).", sub: "L'organisateur te la remettra.", admin: '🛍️ souhaite <b>recevoir</b> la carte cadeau (20 €)' },
  give: { label: '🤝 Offrir mon lot pour le prochain tournoi', quote: "« Je choisis d'offrir cette valeur pour enrichir la dotation du prochain vainqueur (cagnotte portée à 40 €) ! »", confirm: 'Confirmer : tu souhaites offrir ton lot pour le prochain tournoi (cagnotte portée à 40 €) ?\n\nTon choix sera définitif.', done: "✅ Merci pour ce beau geste ! Tu as choisi d'offrir ton lot.", sub: 'La cagnotte du prochain tournoi sera portée à 40 €.', admin: '🤝 <b>offre</b> son lot : cagnotte du prochain tournoi portée à 40 €' },
};
// Petits lots : perdant de la finale (friandise précisée par lui, eau, ou rien) et vainqueur de la petite finale (eau ou rien). Mêmes règles : textes jamais dans la page, choix définitif.
const pzKey2 = (cfg) => 'pfc:prize:' + (cfg.tid || '') + ':2', pzKey3 = (cfg) => 'pfc:prize:' + (cfg.tid || '') + ':3';
const nothing = { k: 'none', label: '🙂 Rien, le plaisir de participer', quote: '« Participer était déjà un beau cadeau ! »', confirm: 'Confirmer : tu ne souhaites aucun lot, juste le plaisir de participer ?\n\nTon choix sera définitif.', done: '✅ C\'est noté : le plaisir de participer te suffit.', sub: 'Merci d\'avoir joué !', admin: '🙂 ne souhaite <b>rien</b> (le plaisir de participer)' };
const water = { k: 'water', label: '💧 Une bouteille d\'eau', quote: '« Une petite pause rafraîchissante, merci ! »', confirm: 'Confirmer : tu souhaites une bouteille d\'eau ?\n\nTon choix sera définitif.', done: '✅ C\'est noté : tu as choisi une bouteille d\'eau.', sub: 'L\'organisateur te l\'offrira.', admin: '💧 souhaite une <b>bouteille d\'eau</b>' };
const PZ2_UI = {
  intro: 'Pour te remercier d\'avoir joué jusqu\'au bout, l\'organisateur te propose un petit lot.',
  ask: '🎁 Que souhaites-tu choisir ?',
  opts: [
    { k: 'snack', label: '🍫 Une friandise au distributeur', quote: '« Une petite douceur, s\'il vous plaît ! »', input: 'Précise ta friandise (ex. : barre chocolatée…)', confirm: 'Confirmer : tu souhaites cette friandise au distributeur ?\n\nTon choix sera définitif.', done: '✅ C\'est noté : tu as choisi une friandise au distributeur.', sub: 'L\'organisateur te l\'offrira.', admin: '🍫 souhaite une <b>friandise au distributeur</b>' },
    water, nothing,
  ],
};
const PZ3_UI = {
  intro: 'Pour fêter ce podium, l\'organisateur te propose un petit lot.',
  ask: '🎁 Que souhaites-tu choisir ?',
  opts: [water, nothing],
};
const PZX = { second: { key: pzKey2, ui: PZ2_UI }, third: { key: pzKey3, ui: PZ3_UI } };
const admPz = async (cfg, role) => { const X = PZX[role], p = await redis.get(X.key(cfg)), o = p && X.ui.opts.find((x) => x.k === p.choice); return p ? { ...p, label: o ? o.admin : '' } : null; };
async function wipe() {
  const cfg = await redis.get('pfc:cfg');
  if (cfg) await Promise.all([...ids(cfg).map((i) => redis.del('pfc:m:' + i)), ...cfg.players.map((p) => redis.del(seenKey(cfg, p.id))), ...cfg.players.map((p) => redis.del(helpKey(cfg, p.id))), redis.del(svKey(cfg)), redis.del(pzKey(cfg)), redis.del(pzKey2(cfg)), redis.del(pzKey3(cfg))]);   // matchs, présence et sondage
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
      await Promise.all([redis.del(lk), redis.del(helpKey(cfg, p.id))]); // bon code : le compteur d'essais est remis à zéro et l'éventuel appel à l'aide est effacé
      return res.json({ token: sign(p.id, cfg.tid || '') });
    }
    if (a === 'help') {   // « J'ai perdu mon code » : prévient l'organisateur (visible dans Orga) ; ne change rien d'autre
      const cfg = await redis.get('pfc:cfg'), p = cfg && cfg.players.find((x) => x.id === b.id);
      if (!p) return res.status(400).json({ error: 'joueur' });
      const hk = helpKey(cfg, p.id);
      if (!(await redis.get(hk))) await redis.set(hk, Date.now(), { ex: 7 * 86400 });
      return res.json({ ok: 1 });
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

    if (a === 'hb') {   // simple signal de présence (rubrique « À propos ») : 1 seule commande, sans lire le tournoi
      const [hid0, tid0] = String(b.token || '').split('.');
      if (!hid0 || sign(hid0, tid0 || '') !== b.token) return res.status(401).json({ error: 'auth' });
      const k = 'pfc:seen:' + (tid0 || '') + ':' + hid0;
      if (b.hid) await redis.del(k); else await redis.set(k, 1, { ex: 90 });
      return res.json({ ok: 1 });
    }

    if (a === 'prizeget' || a === 'prize') {   // lots : champion, perdant de la finale, vainqueur de la petite finale (jeton obligatoire)
      const cfg = await redis.get('pfc:cfg'), me = cfg && who(b.token, cfg.tid || '');
      if (!me || !cfg.players.some((p) => p.id === me)) return res.status(401).json({ error: 'auth' });
      const st = await load(), fin = st.matches[mk(cfg.rounds - 1, 0)], th = st.matches.third;
      const role = fin && fin.w === me ? 'champ' : fin && fin.w && (fin.a === me || fin.b === me) ? 'second' : th && th.w === me ? 'third' : null;
      if (!role) return res.status(409).json({ error: 'notchampion' });
      if (role === 'champ') {
        const cur = await redis.get(pzKey(cfg));
        if (a === 'prizeget') return res.json({ role, choice: cur ? cur.choice : null, ui: PZ_UI });
        if (cur) return res.json({ role, choice: cur.choice, already: 1 });   // choix définitif
        if (!['take', 'give'].includes(b.choice)) return res.status(400).json({ error: 'choix' });
        await redis.set(pzKey(cfg), { by: me, choice: b.choice, t: Date.now() });
        return res.json({ role, choice: b.choice });
      }
      const X = PZX[role], cur = await redis.get(X.key(cfg));
      if (a === 'prizeget') return res.json({ role, choice: cur ? cur.choice : null, snack: cur ? cur.snack || '' : '', ui: X.ui });
      if (cur) return res.json({ role, choice: cur.choice, snack: cur.snack || '', already: 1 });   // choix définitif
      const o = X.ui.opts.find((x) => x.k === b.choice);
      if (!o) return res.status(400).json({ error: 'choix' });
      const snack = o.input ? String(b.snack || '').replace(/\s+/g, ' ').trim().slice(0, 60) : '';
      if (o.input && !snack) return res.status(400).json({ error: 'precise' });
      await redis.set(X.key(cfg), { by: me, choice: o.k, snack, t: Date.now() });
      return res.json({ role, choice: o.k, snack });
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
        m.pend = { by: side, mv: b.mv }; m.due = dueFrom(Date.now(), DELAY);
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
    if (['create', 'reset', 'admin', 'extend', 'forfeit', 'start', 'unlock', 'schedule', 'addplayers', 'removeplayer', 'renameplayer', 'newpin', 'reorder'].includes(a)) {
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
          m.due = dueFrom(Math.max(m.due || 0, Date.now()), 24 * 3600e3); await redis.set(key, m);
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
      if (a === 'reorder') {   // place des joueurs au tour 1 (avant le lancement) : ordre choisi, ou nouveau tirage au sort
        const cfg = await redis.get('pfc:cfg');
        if (!cfg) return res.status(400).json({ error: 'tournoi' });
        if (cfg.started !== false) return res.status(409).json({ error: 'started' });
        let players = cfg.players, doShuffle = true;
        if (!b.shuffle) {
          const order = Array.isArray(b.order) ? b.order.map(String) : [], byId = new Map(cfg.players.map((p) => [p.id, p]));
          if (order.length !== byId.size || new Set(order).size !== order.length || !order.every((id) => byId.has(id))) return res.status(400).json({ error: 'ordre' });
          players = order.map((id) => byId.get(id)); doShuffle = false;
        }
        const { rounds, counts, M } = build(players, doShuffle);
        if (rounds !== cfg.rounds) return res.status(500).json({ error: 'structure' });
        await Promise.all(ids(cfg).map((i) => redis.del('pfc:m:' + i)));
        await Promise.all(Object.entries(M).map(([id, m]) => redis.set('pfc:m:' + id, m)));
        return res.json({ ok: 1 });
      }
      if (a === 'renameplayer') {
        const cfg = await redis.get('pfc:cfg');
        if (!cfg) return res.status(400).json({ error: 'tournoi' });
        const p = cfg.players.find((x) => x.id === b.id);   // modifiable à tout moment, même tournoi lancé (remplaçant d'un joueur absent)
        if (!p) return res.status(400).json({ error: 'joueur' });
        const name = String(b.name || '').replace(/\s+/g, ' ').trim().slice(0, 60);
        if (!name) return res.status(400).json({ error: 'vide' });
        if (cfg.players.some((x) => x.id !== p.id && x.name.trim().toLowerCase() === name.toLowerCase())) return res.status(400).json({ error: 'doublon' });
        p.name = name; // l'identifiant, le code, les matchs et le tirage au sort ne changent pas
        await redis.set('pfc:cfg', cfg);
        return res.json({ ok: 1 });
      }
      if (a === 'newpin') {   // nouveau code secret pour un joueur (même tournoi lancé) : l'ancien code ne permet plus de se reconnecter ; les matchs ne changent pas
        const cfg = await redis.get('pfc:cfg');
        if (!cfg) return res.status(400).json({ error: 'tournoi' });
        const p = cfg.players.find((x) => x.id === b.id);
        if (!p) return res.status(400).json({ error: 'joueur' });
        const pin = newPin(), pins = (await redis.get('pfc:pins')) || {};
        p.h = hash(pin + p.id); pins[p.id] = pin;
        await Promise.all([redis.set('pfc:cfg', cfg), redis.set('pfc:pins', pins), redis.del(lockKey(cfg, p.id)), redis.del(helpKey(cfg, p.id))]);
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
        const hv = cfg ? await redis.mget(...cfg.players.map((p) => helpKey(cfg, p.id))) : [], help = {};
        if (cfg) cfg.players.forEach((p, i) => { if (hv[i]) help[p.id] = hv[i]; });
        return res.json({ pins: (await redis.get('pfc:pins')) || {}, locks, help, survey: cfg ? await svResults(cfg) : null, prize: cfg ? await (async () => { const p = await redis.get(pzKey(cfg)); return p ? { ...p, label: PZ_UI[p.choice] ? PZ_UI[p.choice].admin : '' } : null; })() : null,
          prizes: cfg ? { second: await admPz(cfg, 'second'), third: await admPz(cfg, 'third') } : null });
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
