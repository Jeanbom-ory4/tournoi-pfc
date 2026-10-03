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
  cfg: cfg && { players: cfg.players.map(({ id, name }) => ({ id, name })), rounds: cfg.rounds, size: cfg.size },
  matches: Object.fromEntries(Object.entries(matches).map(([k, m]) => [k, { ...m, pend: m.pend ? { by: m.pend.by } : null }])),
});
async function wipe() {
  const cfg = await redis.get('pfc:cfg');
  if (cfg) await Promise.all(ids(cfg).map((i) => redis.del('pfc:m:' + i)));
  await redis.del('pfc:cfg', 'pfc:pins', 'pfc:emails');
}
async function mail(req, cfg, m, from, to) {
  const em = ((await redis.get('pfc:emails')) || {})[to];
  if (!em || !process.env.RESEND_API_KEY) return;
  const name = (id) => cfg.players.find((p) => p.id === id).name;
  const link = process.env.APP_URL || `https://${req.headers.host}`;
  await fetch('https://api.resend.com/emails', {
    method: 'POST',
    headers: { Authorization: 'Bearer ' + process.env.RESEND_API_KEY, 'Content-Type': 'application/json' },
    body: JSON.stringify({
      from: process.env.MAIL_FROM || 'Tournoi PFC <onboarding@resend.dev>',
      to: [em],
      subject: "Tournoi PFC — C'est ton tour de jouer !",
      text: `Bonjour ${name(to)}, ${name(from)} a joué son coup ! Clique sur ce lien pour jouer ton tour : ${link}`,
    }),
  }).catch(() => {});
}

module.exports = async (req, res) => {
  try {
    const b = req.body || {}, a = b.action;
    if (!(process.env.UPSTASH_REDIS_REST_URL || process.env.KV_REST_API_URL) || !(process.env.UPSTASH_REDIS_REST_TOKEN || process.env.KV_REST_API_TOKEN))
      return res.status(500).json({ error: 'Redis non configuré : ajoutez une base Upstash Redis au projet Vercel puis redéployez.' });
    if (a === 'state') return res.json(pub(await load()));

    if (a === 'login') {
      const cfg = await redis.get('pfc:cfg'), p = cfg && cfg.players.find((x) => x.id === b.id);
      if (!p || hash(String(b.pin).toUpperCase() + p.id) !== p.h) return res.status(401).json({ error: 'pin' });
      return res.json({ token: sign(p.id) });
    }

    if (a === 'play') {
      const me = who(b.token);
      if (!me) return res.status(401).json({ error: 'auth' });
      if (!['r', 'f', 'c'].includes(b.mv)) return res.status(400).json({ error: 'coup' });
      const cfg = await redis.get('pfc:cfg'), key = 'pfc:m:' + b.id, m = await redis.get(key);
      if (!m || m.w || !m.a || !m.b || (m.a !== me && m.b !== me)) return res.status(400).json({ error: 'match' });
      const side = m.a === me ? 'a' : 'b', opp = side === 'a' ? m.b : m.a;
      if (!m.pend) {
        m.pend = { by: side, mv: b.mv };
        await redis.set(key, m);
        await mail(req, cfg, m, me, opp);
      } else if (m.pend.by !== side) {
        const o = m.pend.mv, ma = side === 'a' ? b.mv : o, mb = side === 'a' ? o : b.mv;
        const w = ma === mb ? '=' : BEATS[ma] === mb ? 'a' : 'b';
        if (w === 'a') m.sa++; if (w === 'b') m.sb++;
        m.hist.push({ a: ma, b: mb, w });
        m.pend = null;
        const win = m.sa >= 3 ? m.a : m.sb >= 3 ? m.b : null;
        if (win) {
          m.w = win;
          if (m.r + 1 < cfg.rounds) {
            const nk = 'pfc:m:' + mk(m.r + 1, m.i >> 1), nm = await redis.get(nk);
            nm[m.i % 2 ? 'b' : 'a'] = win;
            await redis.set(nk, nm);
          }
        }
        await redis.set(key, m);
      }
      return res.json(pub(await load()));
    }

    // --- Actions organisateur (clé ADMIN_KEY) ---
    if (['create', 'reset', 'admin'].includes(a)) {
      if (!ADMIN) return res.status(500).json({ error: 'Variable ADMIN_KEY absente sur ce déploiement : vérifiez son nom exact, cochez Production, puis redéployez.' });
      if (!isAdmin(b.key)) return res.status(401).json({ error: 'admin' });
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
