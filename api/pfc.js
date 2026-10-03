// API unique du tournoi PFC (Vercel Serverless Function) — Redis (Upstash) + e-mails Resend
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
const REM = (Number(process.env.REMINDER_HOURS) || 24) * 3600e3;   // rappel envoyé X heures après le 1er e-mail
const hrs = (ms) => Math.round(ms / 3600e3 * 100) / 100;
const mk = (r, i) => `m${r}_${i}`;
const hash = (s) => crypto.createHash('sha256').update(s + SECRET).digest('hex');
const sign = (id) => id + '.' + crypto.createHmac('sha256', SECRET).update(id).digest('hex').slice(0, 32);
const who = (t) => { const id = String(t || '').split('.')[0]; return id && sign(id) === t ? id : null; };
const isAdmin = (k) => !!ADMIN && String(k || '').trim() === ADMIN.trim();
const ids = (cfg) => { const o = []; for (let r = 0; r < cfg.rounds; r++) for (let i = 0; i < cfg.size / 2 ** (r + 1); i++) o.push(mk(r, i)); return o; };

async function load() {
  const cfg = await redis.get('pfc:cfg');
  if (!cfg) return { cfg: null, matches: {} };
  const list = ids(cfg), ms = await redis.mget(...list.map((i) => 'pfc:m:' + i)), matches = {};
  list.forEach((id, k) => { if (ms[k]) matches[id] = ms[k]; });
  return { cfg, matches };
}
// Vue publique : ni PIN, ni e-mails, et le coup en attente n'est JAMAIS renvoyé (seulement qui a joué)
const pub = ({ cfg, matches }) => ({
  cfg: cfg && { players: cfg.players.map(({ id, name }) => ({ id, name })), rounds: cfg.rounds, size: cfg.size, hours: hrs(DELAY) },
  matches: Object.fromEntries(Object.entries(matches).map(([k, { rem, ...m }]) => [k, { ...m, pend: m.pend ? { by: m.pend.by } : null }])),
});
const out = (st) => ({ ...pub(st), now: Date.now() });
async function wipe() {
  const cfg = await redis.get('pfc:cfg');
  if (cfg) await Promise.all(ids(cfg).map((i) => redis.del('pfc:m:' + i)));
  await redis.del('pfc:cfg', 'pfc:pins', 'pfc:emails');
}
async function cancelMail(id) {
  if (!id || !process.env.BREVO_API_KEY) return;
  try { await fetch('https://api.brevo.com/v3/smtp/email/' + encodeURIComponent(id), { method: 'DELETE', headers: { 'api-key': process.env.BREVO_API_KEY } }); } catch (e) {}
}
// Termine un match : annule le rappel, désigne le vainqueur (ff = joueur éliminé pour dépassement de délai) et le fait avancer
async function finish(cfg, m, win, ff) {
  await cancelMail(m.rem);
  m.w = win; m.pend = null; m.due = null; m.rem = null;
  if (ff) m.ff = ff;
  if (m.r + 1 < cfg.rounds) {
    const nk = 'pfc:m:' + mk(m.r + 1, m.i >> 1), nm = await redis.get(nk);
    nm[m.i % 2 ? 'b' : 'a'] = win;
    await redis.set(nk, nm);
  }
  await redis.set('pfc:m:' + mk(m.r, m.i), m);
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
// E-mail de notification (+ rappel programmé chez Brevo). Renvoie l'identifiant du rappel pour pouvoir l'annuler.
async function mail(req, cfg, m, from, to) {
  const em = ((await redis.get('pfc:emails')) || {})[to];
  const brevo = process.env.BREVO_API_KEY, resend = process.env.RESEND_API_KEY;
  if (!em || !(brevo || resend)) return null;
  const name = (id) => cfg.players.find((p) => p.id === id).name;
  const link = process.env.APP_URL || `https://${req.headers.host}`;
  const until = new Date(m.due).toLocaleString('fr-FR', { timeZone: 'Europe/Paris', weekday: 'long', day: 'numeric', month: 'long', hour: '2-digit', minute: '2-digit' });
  const send = async (subject, text, at) => {
    try {
      let r;
      if (brevo) {
        const body = { sender: { name: process.env.MAIL_FROM_NAME || 'Tournoi PFC', email: process.env.MAIL_FROM_EMAIL }, to: [{ email: em }], subject, textContent: text };
        if (at) body.scheduledAt = new Date(at).toISOString();
        r = await fetch('https://api.brevo.com/v3/smtp/email', { method: 'POST', headers: { 'api-key': brevo, 'Content-Type': 'application/json', accept: 'application/json' }, body: JSON.stringify(body) });
      } else {
        if (at) return null; // rappel programmé : Brevo uniquement
        r = await fetch('https://api.resend.com/emails', { method: 'POST', headers: { Authorization: 'Bearer ' + resend, 'Content-Type': 'application/json' }, body: JSON.stringify({ from: process.env.MAIL_FROM || 'Tournoi PFC <onboarding@resend.dev>', to: [em], subject, text }) });
      }
      if (!r.ok) { console.error('Envoi e-mail refusé', r.status, await r.text()); return null; }
      const j = await r.json().catch(() => ({}));
      return j.messageId || (j.messageIds && j.messageIds[0]) || j.batchId || null;
    } catch (e) { console.error('Envoi e-mail impossible', e.message); return null; }
  };
  await send("Tournoi PFC — C'est ton tour de jouer !",
    `Bonjour ${name(to)}, ${name(from)} a joué son coup ! Clique sur ce lien pour jouer ton tour : ${link}\n\nTu as ${hrs(DELAY)} h pour jouer (jusqu'au ${until}), sinon tu seras éliminé(e) du tournoi.`);
  if (brevo && REM < DELAY && REM <= 72 * 3600e3)
    return send(`Rappel — Tournoi PFC : il te reste ${hrs(DELAY - REM)} h pour jouer !`,
      `Bonjour ${name(to)}, ${name(from)} attend toujours ton coup ! Il te reste environ ${hrs(DELAY - REM)} h (jusqu'au ${until}) pour jouer, sinon tu seras éliminé(e) du tournoi. Clique sur ce lien : ${link}`, m.pendAt + REM);
  return null;
}

module.exports = async (req, res) => {
  try {
    const b = req.body || {}, a = b.action;
    if (!(process.env.UPSTASH_REDIS_REST_URL || process.env.KV_REST_API_URL) || !(process.env.UPSTASH_REDIS_REST_TOKEN || process.env.KV_REST_API_TOKEN))
      return res.status(500).json({ error: 'Redis non configuré : ajoutez une base Upstash Redis au projet Vercel puis redéployez.' });
    if (a === 'state') return res.json(out(await sweep(await load())));

    if (a === 'login') {
      const cfg = await redis.get('pfc:cfg'), p = cfg && cfg.players.find((x) => x.id === b.id);
      if (!p || hash(String(b.pin).toUpperCase() + p.id) !== p.h) return res.status(401).json({ error: 'pin' });
      return res.json({ token: sign(p.id) });
    }

    if (a === 'play') {
      const me = who(b.token);
      if (!me) return res.status(401).json({ error: 'auth' });
      if (!['r', 'f', 'c'].includes(b.mv)) return res.status(400).json({ error: 'coup' });
      const st = await sweep(await load()), cfg = st.cfg, m = st.matches[b.id], key = 'pfc:m:' + b.id;
      if (m && m.w && m.ff === me) return res.status(409).json({ error: 'deadline' });
      if (!m || m.w || !m.a || !m.b || (m.a !== me && m.b !== me)) return res.status(400).json({ error: 'match' });
      const side = m.a === me ? 'a' : 'b', opp = side === 'a' ? m.b : m.a;
      if (!m.pend) {
        const t = Date.now();
        m.pend = { by: side, mv: b.mv }; m.pendAt = t; m.due = t + DELAY;
        await redis.set(key, m);
        const rid = await mail(req, cfg, m, me, opp);
        if (rid) { m.rem = rid; await redis.set(key, m); }
      } else if (m.pend.by !== side) {
        const o = m.pend.mv, ma = side === 'a' ? b.mv : o, mb = side === 'a' ? o : b.mv;
        const w = ma === mb ? '=' : BEATS[ma] === mb ? 'a' : 'b';
        if (w === 'a') m.sa++; if (w === 'b') m.sb++;
        m.hist.push({ a: ma, b: mb, w });
        m.pend = null;
        await cancelMail(m.rem); m.rem = null; m.due = null;
        const win = m.sa >= 3 ? m.a : m.sb >= 3 ? m.b : null;
        if (win) await finish(cfg, m, win, null); else await redis.set(key, m);
      }
      return res.json(out(await load()));
    }

    // --- Actions organisateur (clé ADMIN_KEY) ---
    if (['create', 'reset', 'admin', 'extend', 'forfeit'].includes(a)) {
      if (!ADMIN) return res.status(500).json({ error: 'Variable ADMIN_KEY absente sur ce déploiement : vérifiez son nom exact, cochez Production, puis redéployez.' });
      if (!isAdmin(b.key)) return res.status(401).json({ error: 'admin' });
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
      if (a === 'admin') return res.json({ pins: (await redis.get('pfc:pins')) || {}, emails: (await redis.get('pfc:emails')) || {} });
      if (a === 'reset') { await wipe(); return res.json({ ok: 1 }); }

      const seen = new Set(), names = [];
      for (const l of String(b.text || '').split('\n').map((s) => s.trim()).filter(Boolean)) {
        const p = l.split(/[,;\t]/).map((x) => x.trim()), name = p[0];
        const em = p.slice(1).find((x) => /^\S+@\S+\.\S+$/.test(x)) || '';
        if (name && !seen.has(name)) { seen.add(name); names.push({ name, em }); }
      }
      if (names.length < 2) return res.status(400).json({ error: 'min2' });
      await wipe();
      for (let i = names.length - 1; i > 0; i--) { const j = crypto.randomInt(i + 1); [names[i], names[j]] = [names[j], names[i]]; }
      let size = 2; while (size < names.length) size *= 2;
      const rounds = Math.log2(size), players = [], pins = {}, emails = {};
      names.forEach((n, i) => {
        const id = 'p' + i, pin = Array.from({ length: 4 }, () => CHARS[crypto.randomInt(CHARS.length)]).join('');
        pins[id] = pin; if (n.em) emails[id] = n.em;
        players.push({ id, name: n.name, h: hash(pin + id) });
      });
      const slots = Array(size).fill(null);
      players.forEach((p, k) => { slots[k < size / 2 ? 2 * k : 2 * (k - size / 2) + 1] = p.id; });
      const M = {};
      for (let r = 0; r < rounds; r++) for (let i = 0; i < size / 2 ** (r + 1); i++)
        M[mk(r, i)] = { r, i, a: r ? null : slots[2 * i], b: r ? null : slots[2 * i + 1], sa: 0, sb: 0, hist: [], pend: null, w: null };
      for (let i = 0; i < size / 2; i++) {
        const m = M[mk(0, i)];
        if (!m.a || !m.b) { m.w = m.a || m.b; if (rounds > 1) M[mk(1, i >> 1)][i % 2 ? 'b' : 'a'] = m.w; }
      }
      await Promise.all([
        redis.set('pfc:cfg', { players, rounds, size }), redis.set('pfc:pins', pins), redis.set('pfc:emails', emails),
        ...Object.entries(M).map(([id, m]) => redis.set('pfc:m:' + id, m)),
      ]);
      return res.json({ ok: 1 });
    }
    res.status(400).json({ error: 'action' });
  } catch (e) {
    res.status(500).json({ error: String(e.message || e) });
  }
};
