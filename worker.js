"use strict";

const express = require("express");
const { Pool } = require("pg");

if (typeof fetch !== "function") {
  throw new Error("Global fetch not found. Use Node 18+.");
}

const POLYGON_KEY       = process.env.POLYGON_KEY;
const DATABASE_URL      = process.env.DATABASE_URL;
const BASE44_INGEST_URL = process.env.BASE44_INGEST_URL || "";
const BASE44_API_KEY    = process.env.BASE44_API_KEY    || "";
const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN || "";
const TELEGRAM_CHAT_ID   = process.env.TELEGRAM_CHAT_ID   || "";

if (!POLYGON_KEY)   console.error("Missing env var: POLYGON_KEY");
if (!DATABASE_URL)  console.error("Missing env var: DATABASE_URL");

// ===== Config =====
const SCAN_INTERVAL_MS      = Number(process.env.SCAN_INTERVAL_MS      || 15000);
const PRICE_MIN              = Number(process.env.PRICE_MIN              || 1);
const PRICE_MAX              = Number(process.env.PRICE_MAX              || 12);
const MAX_FLOAT              = Number(process.env.MAX_FLOAT              || 5_000_000);
const AVG_VOL_DAYS           = Number(process.env.AVG_VOL_DAYS           || 30);
const ALERT_COOLDOWN_MIN     = Number(process.env.ALERT_COOLDOWN_MIN     || 90);
const MAX_CANDIDATES         = Number(process.env.MAX_CANDIDATES         || 500);
const CONCURRENCY            = Number(process.env.CONCURRENCY            || 4);
const NEWS_LOOKBACK_MIN      = Number(process.env.NEWS_LOOKBACK_MIN      || 1440);
const SCAN_START_HOUR_PT     = Number(process.env.SCAN_START_HOUR_PT     || 4);
const SCAN_END_HOUR_PT       = Number(process.env.SCAN_END_HOUR_PT       || 12);
const MIN_MOMENTUM_SCORE     = Number(process.env.MIN_MOMENTUM_SCORE     || 70);
const TOP_ALERTS_PER_SCAN    = Number(process.env.TOP_ALERTS_PER_SCAN    || 3);
const LOW_FLOAT_THRESHOLD    = Number(process.env.LOW_FLOAT_THRESHOLD    || 2_000_000);
const MID_FLOAT_THRESHOLD    = Number(process.env.MID_FLOAT_THRESHOLD    || 5_000_000);
const EARLY_ALERTS_ENABLED   = process.env.EARLY_ALERTS_ENABLED !== "false";
const EARLY_MIN_PERCENT_CHANGE = Number(process.env.EARLY_MIN_PERCENT_CHANGE || 5);
const EARLY_MIN_RVOL         = Number(process.env.EARLY_MIN_RVOL         || 3);
const EARLY_MIN_ACCEL        = Number(process.env.EARLY_MIN_ACCEL        || 2);
const RUNNER_ENABLED         = process.env.RUNNER_ENABLED !== "false";
const RUNNER_REQUIRE_NEWS    = process.env.RUNNER_REQUIRE_NEWS === "true";
const MIN_PERCENT_CHANGE     = Number(process.env.MIN_PERCENT_CHANGE     || 8);
const MIN_RVOL               = Number(process.env.MIN_RVOL               || 5);
const RUNNER_MIN_VOL         = Number(process.env.RUNNER_MIN_VOL         || 300_000);
const PREMARKET_ENABLED      = process.env.PREMARKET_ENABLED !== "false";
const PREMARKET_MIN_GAP      = Number(process.env.PREMARKET_MIN_GAP      || 8);
const PREMARKET_MIN_VOL      = Number(process.env.PREMARKET_MIN_VOL      || 50_000);
const PREMARKET_REQUIRE_NEWS = process.env.PREMARKET_REQUIRE_NEWS === "true";
const PREMARKET_REQUIRE_SPIKE= process.env.PREMARKET_REQUIRE_SPIKE === "true";
const VOLUME_SPIKE_MULTIPLIER= Number(process.env.VOLUME_SPIKE_MULTIPLIER|| 2);
const VOLUME_LOOKBACK_MIN    = Number(process.env.VOLUME_LOOKBACK_MIN    || 5);
const VOLUME_BASELINE_MIN    = Number(process.env.VOLUME_BASELINE_MIN    || 30);
const POLYGON_RETRY_ATTEMPTS = Number(process.env.POLYGON_RETRY_ATTEMPTS || 3);
const POLYGON_RETRY_DELAY_MS = Number(process.env.POLYGON_RETRY_DELAY_MS || 500);

// ===== DB =====
const pool = new Pool({
  connectionString: DATABASE_URL,
  ssl: { rejectUnauthorized: false },
  max: 10,                   // FIX: limit pool size for Railway's pg limits
  idleTimeoutMillis: 30_000,
  connectionTimeoutMillis: 5_000,
});

// ===== Express =====
const app = express();
app.use(express.json());

// ===== Runtime state =====
// FIX: stats now accumulate across scans instead of resetting each run
let isScanning          = false;
let lastError           = null;
let lastLoopAt          = null;
let lastScanStartedAt   = null;
let lastScanFinishedAt  = null;
let lastScanDurationMs  = null;
let totalTickersFetched = 0;
let totalAlertsCreated  = 0;
let scanRuns            = 0;

// Per-scan stats (for logging, not health endpoint confusion)
let _scanStats = {};

// ===== Routes =====
app.get("/", (_req, res) => res.send("Quantum Scan Worker is running"));

app.get("/health", async (req, res) => {
  try {
    await pool.query("SELECT 1");
    res.json({
      ok: true,
      db: "connected",
      isScanning,
      scanIntervalMs: SCAN_INTERVAL_MS,
      lastError,
      lastLoopAt,
      lastScanStartedAt,
      lastScanFinishedAt,
      lastScanDurationMs,
      totalTickersFetched,
      totalAlertsCreated,
      scanRuns,
      lastScanStats: _scanStats,
      telegramEnabled: Boolean(TELEGRAM_BOT_TOKEN && TELEGRAM_CHAT_ID),
      base44Enabled:   Boolean(BASE44_INGEST_URL && BASE44_API_KEY),
      config: {
        PRICE_MIN, PRICE_MAX, MAX_FLOAT, AVG_VOL_DAYS,
        ALERT_COOLDOWN_MIN, MAX_CANDIDATES, CONCURRENCY, NEWS_LOOKBACK_MIN,
        SCAN_START_HOUR_PT, SCAN_END_HOUR_PT, MIN_MOMENTUM_SCORE,
        TOP_ALERTS_PER_SCAN, LOW_FLOAT_THRESHOLD, MID_FLOAT_THRESHOLD,
        EARLY_ALERTS_ENABLED, EARLY_MIN_PERCENT_CHANGE, EARLY_MIN_RVOL, EARLY_MIN_ACCEL,
        RUNNER_ENABLED, RUNNER_REQUIRE_NEWS, MIN_PERCENT_CHANGE, MIN_RVOL, RUNNER_MIN_VOL,
        PREMARKET_ENABLED, PREMARKET_MIN_GAP, PREMARKET_MIN_VOL,
        PREMARKET_REQUIRE_NEWS, PREMARKET_REQUIRE_SPIKE,
        VOLUME_SPIKE_MULTIPLIER, VOLUME_LOOKBACK_MIN, VOLUME_BASELINE_MIN,
      },
    });
  } catch (e) {
    res.status(500).json({ ok: false, db: "error", error: e.message });
  }
});

app.get("/alerts", async (_req, res) => {
  try {
    const result = await pool.query(
      `SELECT * FROM alerts ORDER BY created_at DESC LIMIT 200`
    );
    res.json(result.rows);
  } catch (err) {
    console.error("GET /alerts error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

app.get("/test", async (_req, res) => {
  try {
    const inserted = await insertAlert({
      ticker: "TEST", price: 5.25, percent_change: 12.5,
      rvol: 5.1, float: 2_000_000, news: true,
      alert_type: "TEST",
      meta: JSON.stringify({ score: 88, source: "manual_test" }),
    });
    await pushToBase44(inserted);
    await pushToTelegram(formatTelegram(inserted));
    res.json({ success: true, alert: inserted });
  } catch (err) {
    console.error("GET /test error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

app.get("/telegram_test", async (_req, res) => {
  try {
    const result = await pushToTelegram(
      `✅ Quantum Scan Telegram test\nTime: ${new Date().toISOString()}`,
      { returnDebug: true }
    );
    res.json({ ok: true, result });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// ===== DB Helpers =====
async function insertAlert({ ticker, price, percent_change, rvol, float, news, alert_type, meta }) {
  const result = await pool.query(
    `INSERT INTO alerts (ticker, price, percent_change, rvol, float, news, created_at, alert_type, meta)
     VALUES ($1,$2,$3,$4,$5,$6,now(),$7,$8) RETURNING *`,
    [ticker, price, percent_change, rvol, float, news, alert_type || null, meta || null]
  );
  return result.rows[0];
}

async function wasAlertedRecently(ticker, alertType) {
  // FIX: in-memory cooldown is checked first (caller's responsibility);
  // this DB check is only for post-restart deduplication
  const result = await pool.query(
    `SELECT 1 FROM alerts
     WHERE ticker = $1
       AND COALESCE(alert_type,'') = COALESCE($2,'')
       AND created_at >= now() - ($3 || ' minutes')::interval
     LIMIT 1`,
    [ticker, alertType || null, String(ALERT_COOLDOWN_MIN)]
  );
  return result.rowCount > 0;
}

// ===== Base44 =====
async function pushToBase44(alert) {
  if (!BASE44_INGEST_URL || !BASE44_API_KEY) return;
  try {
    const resp = await fetch(BASE44_INGEST_URL, {
      method: "POST",
      headers: { "Content-Type": "application/json", api_key: BASE44_API_KEY },
      body: JSON.stringify(alert),
    });
    if (!resp.ok) {
      const text = await resp.text().catch(() => "");
      console.error("Base44 ingest failed:", resp.status, text.slice(0, 400));
    }
  } catch (e) {
    console.error("Base44 ingest error:", e.message);
  }
}

// ===== Telegram =====
async function pushToTelegram(text, opts = {}) {
  if (!TELEGRAM_BOT_TOKEN || !TELEGRAM_CHAT_ID) {
    const msg = "Telegram not configured";
    if (opts.returnDebug) return { sent: false, reason: msg };
    return false;
  }

  const url = `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`;
  try {
    const resp = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        chat_id: TELEGRAM_CHAT_ID,
        text,
        parse_mode: "HTML",                    // FIX: enable HTML formatting
        disable_web_page_preview: true,
      }),
    });
    const bodyText = await resp.text().catch(() => "");
    if (!resp.ok) {
      console.error("Telegram send failed:", resp.status, bodyText);
      if (opts.returnDebug) return { sent: false, status: resp.status, body: bodyText };
      return false;
    }
    console.log("Telegram sent:", text.split("\n")[0]);
    if (opts.returnDebug) return { sent: true, status: resp.status };
    return true;
  } catch (e) {
    console.error("Telegram send error:", e.message);
    if (opts.returnDebug) return { sent: false, error: e.message };
    return false;
  }
}

// FIX: HTML-formatted Telegram messages (cleaner on mobile)
function formatTelegram(a) {
  const tv  = `https://www.tradingview.com/symbols/${encodeURIComponent(a.ticker)}/`;
  const pct = Number(a.percent_change || 0).toFixed(2);
  const rvol  = a.rvol  != null ? Number(a.rvol).toFixed(2)  : "—";
  const fl    = a.float ? Number(a.float).toLocaleString()    : "—";
  const price = a.price != null ? Number(a.price).toFixed(2) : "—";
  const news  = a.news ? "✅" : "❌";
  const type  = a.alert_type || "ALERT";

  let scoreText = "";
  try {
    const meta = a.meta ? JSON.parse(a.meta) : null;
    if (meta?.score != null) scoreText = `Score: <b>${Math.round(meta.score)}</b>\n`;
  } catch {}

  return (
    `🚀 <b>Quantum Scan (${type})</b>\n` +
    `<b>${a.ticker}</b>  $${price}  (<b>${pct}%</b>)\n` +
    `RVOL: ${rvol}   Float: ${fl}\n` +
    `News: ${news}\n` +
    `${scoreText}<a href="${tv}">Chart →</a>`
  );
}

function formatEarlyTelegram({ ticker, price, pct, rvol, float, volumeAccel, score }) {
  const tv = `https://www.tradingview.com/symbols/${encodeURIComponent(ticker)}/`;
  return (
    `🔥 <b>Quantum Scan (EARLY)</b>\n` +
    `<b>${ticker}</b>  $${Number(price).toFixed(2)}  (<b>${Number(pct).toFixed(2)}%</b>)\n` +
    `RVOL: ${Number(rvol).toFixed(2)}   Float: ${Number(float).toLocaleString()}\n` +
    `Accel: ${Number(volumeAccel).toFixed(2)}\n` +
    `Score: <b>${Math.round(score)}</b>\n` +
    `<a href="${tv}">Chart →</a>`
  );
}

// ===== Polygon / Cache =====
const cache = {
  avgVol:     new Map(),
  float:      new Map(),
  news:       new Map(),
  minuteAggs: new Map(),
};

function getCache(map, key) {
  const entry = map.get(key);
  if (!entry) return null;
  if (Date.now() > entry.expiresAt) { map.delete(key); return null; }
  return entry.value;
}

function setCache(map, key, value, ttlMs) {
  map.set(key, { value, expiresAt: Date.now() + ttlMs });
}

// FIX: retry wrapper for transient Polygon 429/503 errors
async function polygonJson(url, attempt = 0) {
  let resp;
  try {
    resp = await fetch(url);
  } catch (e) {
    if (attempt < POLYGON_RETRY_ATTEMPTS - 1) {
      await sleep(POLYGON_RETRY_DELAY_MS * (attempt + 1));
      return polygonJson(url, attempt + 1);
    }
    throw e;
  }

  if (resp.status === 429 || resp.status >= 500) {
    if (attempt < POLYGON_RETRY_ATTEMPTS - 1) {
      const retryAfter = Number(resp.headers.get("retry-after") || 0) * 1000
        || POLYGON_RETRY_DELAY_MS * (attempt + 1);
      await sleep(retryAfter);
      return polygonJson(url, attempt + 1);
    }
  }

  if (!resp.ok) {
    const text = await resp.text().catch(() => "");
    throw new Error(`Polygon ${resp.status}: ${text.slice(0, 400)}`);
  }
  return resp.json();
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function fetchAllSnapshotTickers() {
  const all = [];
  let url = `https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers?limit=1000&apiKey=${POLYGON_KEY}`;

  for (let page = 0; page < 200 && url; page++) {
    const data = await polygonJson(url);
    const pageTickers = Array.isArray(data?.tickers) ? data.tickers : [];
    all.push(...pageTickers);

    const nextUrl = data?.next_url;
    if (!nextUrl) break;
    url = nextUrl.includes("apiKey=") ? nextUrl
      : `${nextUrl}${nextUrl.includes("?") ? "&" : "?"}apiKey=${POLYGON_KEY}`;
  }

  return all;
}

// FIX: explicit prevClose computation — more reliable premarket
function computePercentChange(t) {
  const prevClose = Number(t?.prevDay?.c || 0);
  if (prevClose <= 0) return 0;

  const price =
    Number(t?.lastTrade?.p) ||
    Number(t?.day?.c)       ||
    prevClose;

  return ((price - prevClose) / prevClose) * 100;
}

async function getAvgDailyVolume(ticker) {
  const cached = getCache(cache.avgVol, ticker);
  if (cached != null) return cached;

  const lookbackCalendarDays = Math.max(AVG_VOL_DAYS * 2, 40);
  const to   = new Date().toISOString().slice(0, 10);
  const from = new Date(Date.now() - lookbackCalendarDays * 86_400_000).toISOString().slice(0, 10);

  const url =
    `https://api.polygon.io/v2/aggs/ticker/${encodeURIComponent(ticker)}` +
    `/range/1/day/${from}/${to}?adjusted=true&sort=desc&limit=50000&apiKey=${POLYGON_KEY}`;

  const data = await polygonJson(url);
  const vols = (data?.results || [])
    .map(r => Number(r?.v || 0))
    .filter(v => Number.isFinite(v) && v > 0)
    .slice(0, AVG_VOL_DAYS);

  const avg = vols.length
    ? Math.round(vols.reduce((a, b) => a + b, 0) / vols.length)
    : 0;

  setCache(cache.avgVol, ticker, avg, 6 * 3_600_000);
  return avg;
}

async function getFloatInfo(ticker) {
  const cached = getCache(cache.float, ticker);
  if (cached != null) return cached;

  const url =
    `https://api.polygon.io/v3/reference/tickers/${encodeURIComponent(ticker)}?apiKey=${POLYGON_KEY}`;

  const data  = await polygonJson(url);
  const res   = data?.results || {};
  const payload = {
    float:             Number(res?.float || 0),
    sharesOutstanding: Number(res?.share_class_shares_outstanding || res?.weighted_shares_outstanding || 0),
  };

  setCache(cache.float, ticker, payload, 24 * 3_600_000);
  return payload;
}

async function hasRecentNews(ticker, lookbackMin) {
  const key    = `${ticker}:${lookbackMin}`;
  const cached = getCache(cache.news, key);
  if (cached != null) return cached;

  const sinceIso = new Date(Date.now() - lookbackMin * 60_000).toISOString();
  const url =
    `https://api.polygon.io/v2/reference/news?ticker=${encodeURIComponent(ticker)}` +
    `&published_utc.gte=${encodeURIComponent(sinceIso)}&limit=5&apiKey=${POLYGON_KEY}`;

  let ok = false;
  try {
    const data = await polygonJson(url);
    ok = Array.isArray(data?.results) && data.results.length > 0;
  } catch { ok = false; }

  setCache(cache.news, key, ok, 2 * 60_000);
  return ok;
}

async function getMinuteAggs(ticker, minutesBack) {
  const cacheKey = `${ticker}:${minutesBack}`;
  const cached   = getCache(cache.minuteAggs, cacheKey);
  if (cached) return cached;

  const cutoff = Date.now() - minutesBack * 60_000;
  const to     = new Date().toISOString().slice(0, 10);
  const from   = new Date(cutoff).toISOString().slice(0, 10);

  const url =
    `https://api.polygon.io/v2/aggs/ticker/${encodeURIComponent(ticker)}` +
    `/range/1/minute/${from}/${to}?adjusted=true&sort=desc&limit=50000&apiKey=${POLYGON_KEY}`;

  const data    = await polygonJson(url);
  const filtered = (data?.results || [])
    .filter(r => Number(r?.t || 0) >= cutoff)
    .map(r => ({ t: Number(r.t), v: Number(r.v || 0), h: Number(r.h || 0), c: Number(r.c || 0) }));

  setCache(cache.minuteAggs, cacheKey, filtered, 20_000);
  return filtered;
}

async function computePremarketVolumeAndSpike(ticker) {
  const bars = await getMinuteAggs(
    ticker,
    Math.max(VOLUME_BASELINE_MIN, VOLUME_LOOKBACK_MIN) + 10
  );

  const totalVol  = bars.reduce((sum, b) => sum + b.v, 0);
  const sortedAsc = [...bars].sort((a, b) => a.t - b.t);
  const lastN     = sortedAsc.slice(-VOLUME_LOOKBACK_MIN);
  const base      = sortedAsc.slice(0, Math.max(0, sortedAsc.length - VOLUME_LOOKBACK_MIN));

  const lastAvg = lastN.length ? lastN.reduce((s, b) => s + b.v, 0) / lastN.length : 0;
  const baseSlice = base.slice(-VOLUME_BASELINE_MIN);
  const baseAvg   = baseSlice.length ? baseSlice.reduce((s, b) => s + b.v, 0) / baseSlice.length : 0;

  const spike  = baseAvg > 0 ? lastAvg / baseAvg : 0;
  const pmHigh = sortedAsc.length ? Math.max(...sortedAsc.map(b => b.h || 0)) : 0;

  return { totalVol, spikeMultiplier: spike, pmHigh };
}

// FIX: rebalanced score — rvol capped lower, float bonus more nuanced
function computeMomentumScore({ pct, rvol, volumeAccel, float, news, breakout }) {
  let score = 0;

  // RVOL: cap at 30 (was 40) so a single 8x RVOL spike can't dominate
  score += Math.min(rvol * 4, 30);

  // % change: cap at 20
  score += Math.min(pct * 0.8, 20);

  // Volume acceleration
  score += volumeAccel > 4 ? 20
         : volumeAccel > 3 ? 15
         : volumeAccel > 2 ? 10
         : volumeAccel > 1.25 ? 5 : 0;

  // Float tiers (unchanged)
  score += float < LOW_FLOAT_THRESHOLD ? 15
         : float < MID_FLOAT_THRESHOLD ? 8
         : float < MAX_FLOAT           ? 3 : 0;

  if (news)     score += 10;
  if (breakout) score += 15;

  return score;
}

// ===== Cooldown =====
const cooldownMap = new Map();

function inCooldown(key) {
  const exp = cooldownMap.get(key);
  if (!exp) return false;
  if (Date.now() > exp) { cooldownMap.delete(key); return false; }
  return true;
}

function setCooldown(key) {
  cooldownMap.set(key, Date.now() + ALERT_COOLDOWN_MIN * 60_000);
}

// ===== Concurrency helper =====
async function runWithConcurrency(items, limit, workerFn) {
  const results = new Array(items.length);
  let idx = 0;

  async function runner() {
    while (idx < items.length) {
      const i = idx++;
      results[i] = await workerFn(items[i], i);
    }
  }

  await Promise.all(Array.from({ length: Math.max(1, limit) }, runner));
  return results;
}

// ===== Scanner =====
async function scan() {
  lastLoopAt = new Date().toISOString();   // FIX: always update heartbeat

  // Check trading window
  const pacific = new Date(
    new Date().toLocaleString("en-US", { timeZone: "America/Los_Angeles" })
  );
  const hour = pacific.getHours();

  if (hour < SCAN_START_HOUR_PT || hour >= SCAN_END_HOUR_PT) {
    console.log(`Outside scan window (${SCAN_START_HOUR_PT}–${SCAN_END_HOUR_PT} PT). Skipping.`);
    return;
  }

  if (isScanning) {
    console.log("Previous scan still running, skipping.");
    return;
  }

  isScanning = true;
  scanRuns  += 1;

  const started = Date.now();
  lastError = null;
  lastScanStartedAt = new Date().toISOString();

  // Per-scan counters
  let tickersFetched   = 0;
  let candidatesFound  = 0;
  let runnerCandidates = 0;
  let gapperCandidates = 0;
  let earlyCandidates  = 0;
  let deepChecked      = 0;
  let alertsCreated    = 0;

  try {
    console.log(`[Scan #${scanRuns}] Starting...`);

    const tickers = await fetchAllSnapshotTickers();
    tickersFetched = tickers.length;
    totalTickersFetched += tickersFetched;

    let raw = tickers
      .map(t => {
        const symbol = t?.ticker;
        const price  =
          Number(t?.lastTrade?.p) ||
          Number(t?.day?.c)       ||
          Number(t?.prevDay?.c)   || 0;
        const pct    = computePercentChange(t);   // FIX: use improved fn
        const dayVol = Number(t?.day?.v || 0);
        return { symbol, price, pct, dayVol };
      })
      .filter(x => x.symbol && x.price > 0 && x.price >= PRICE_MIN && x.price <= PRICE_MAX);

    candidatesFound = raw.length;
    raw.sort((a, b) => Math.abs(b.pct) - Math.abs(a.pct));
    if (MAX_CANDIDATES > 0) raw = raw.slice(0, MAX_CANDIDATES);

    console.log(`Fetched: ${tickersFetched} | In range: ${candidatesFound} | Deep-checking: ${raw.length}`);

    const scoredAlerts = [];

    await runWithConcurrency(raw, CONCURRENCY, async (c) => {
      const ticker = c.symbol;
      try {
        const floatInfo   = await getFloatInfo(ticker);
        const trueFloat   = Number(floatInfo?.float || 0);
        const sharesOut   = Number(floatInfo?.sharesOutstanding || 0);
        const effectiveFloat = trueFloat > 0 ? trueFloat : sharesOut;

        if (!effectiveFloat || effectiveFloat > MAX_FLOAT) return;

        const avgVol = await getAvgDailyVolume(ticker);
        if (!avgVol || avgVol <= 0) return;

        const rvol = c.dayVol / avgVol;

        let volumeAccel = 0, breakout = false, pmHigh = 0, totalVol = 0, spikeMultiplier = 0;
        try {
          const pm = await computePremarketVolumeAndSpike(ticker);
          volumeAccel     = Number(pm.spikeMultiplier || 0);
          spikeMultiplier = volumeAccel;
          pmHigh          = Number(pm.pmHigh || 0);
          totalVol        = Number(pm.totalVol || 0);
          breakout        = pmHigh > 0 && c.price >= pmHigh;
        } catch { /* non-fatal */ }

        const newsOk = await hasRecentNews(ticker, NEWS_LOOKBACK_MIN);

        const baseScore = computeMomentumScore({
          pct: c.pct, rvol, volumeAccel, float: effectiveFloat, news: newsOk, breakout,
        });

        // ── EARLY ALERT ────────────────────────────────────────────────
        if (
          EARLY_ALERTS_ENABLED &&
          c.pct >= EARLY_MIN_PERCENT_CHANGE &&
          rvol  >= EARLY_MIN_RVOL &&
          volumeAccel >= EARLY_MIN_ACCEL
        ) {
          const key = `${ticker}:EARLY`;
          if (!inCooldown(key) && !(await wasAlertedRecently(ticker, "EARLY"))) {
            earlyCandidates++;
            await pushToTelegram(
              formatEarlyTelegram({ ticker, price: c.price, pct: c.pct, rvol,
                float: Math.round(effectiveFloat), volumeAccel, score: baseScore })
            );
            setCooldown(key);
            console.log(`[EARLY] ${ticker} pct=${c.pct.toFixed(2)} rvol=${rvol.toFixed(2)} accel=${volumeAccel.toFixed(2)} score=${Math.round(baseScore)}`);
          }
        }

        // ── RUNNER ─────────────────────────────────────────────────────
        if (
          RUNNER_ENABLED &&
          c.pct    >= MIN_PERCENT_CHANGE &&
          c.dayVol >= RUNNER_MIN_VOL &&
          rvol     >= MIN_RVOL
        ) {
          if (RUNNER_REQUIRE_NEWS && !newsOk) return;
          if (baseScore < MIN_MOMENTUM_SCORE) return;

          runnerCandidates++;

          let type = "RUNNER";
          if (breakout) type = "PM_BREAKOUT";
          if (effectiveFloat < LOW_FLOAT_THRESHOLD && c.pct >= 15 && rvol >= 8) type = "LOW_FLOAT_SQUEEZE";

          const key = `${ticker}:${type}`;
          if (inCooldown(key)) return;
          if (await wasAlertedRecently(ticker, type)) { setCooldown(key); return; }

          deepChecked++;
          scoredAlerts.push({
            ticker, price: c.price,
            percent_change: Number(c.pct.toFixed(2)),
            rvol:  Number(rvol.toFixed(2)),
            float: Math.round(effectiveFloat),
            news:  Boolean(newsOk),
            alert_type: type,
            score: baseScore,
            meta: JSON.stringify({
              score: Number(baseScore.toFixed(2)),
              volumeAccel: Number(volumeAccel.toFixed(2)),
              breakout, pmHigh: Number(pmHigh.toFixed(2)),
              trueFloat: Math.round(trueFloat), sharesOutstanding: Math.round(sharesOut),
            }),
            cooldownKey: key,
          });
          return;
        }

        // ── GAPPER ─────────────────────────────────────────────────────
        if (PREMARKET_ENABLED && c.pct >= PREMARKET_MIN_GAP) {
          const gapBreakout = pmHigh > 0 && c.price >= pmHigh;
          if (totalVol < PREMARKET_MIN_VOL) return;
          if (PREMARKET_REQUIRE_SPIKE && spikeMultiplier < VOLUME_SPIKE_MULTIPLIER) return;
          if (PREMARKET_REQUIRE_NEWS && !newsOk) return;

          const score = computeMomentumScore({
            pct: c.pct, rvol, volumeAccel: spikeMultiplier,
            float: effectiveFloat, news: newsOk, breakout: gapBreakout,
          });
          if (score < MIN_MOMENTUM_SCORE) return;

          gapperCandidates++;
          const key = `${ticker}:GAPPER`;
          if (inCooldown(key)) return;
          if (await wasAlertedRecently(ticker, "GAPPER")) { setCooldown(key); return; }

          deepChecked++;
          scoredAlerts.push({
            ticker, price: c.price,
            percent_change: Number(c.pct.toFixed(2)),
            rvol:  Number(rvol.toFixed(2)),
            float: Math.round(effectiveFloat),
            news:  Boolean(newsOk),
            alert_type: "GAPPER",
            score,
            meta: JSON.stringify({
              score: Number(score.toFixed(2)),
              premarket_vol_window: Math.round(totalVol),
              volume_spike: Number(spikeMultiplier.toFixed(2)),
              breakout: gapBreakout, pmHigh: Number(pmHigh || 0),
              trueFloat: Math.round(trueFloat), sharesOutstanding: Math.round(sharesOut),
            }),
            cooldownKey: key,
          });
        }

      } catch (e) {
        console.error(`[${ticker}] check error:`, e.message);
      }
    });

    // Send top N only
    scoredAlerts.sort((a, b) => b.score - a.score);
    const topAlerts = scoredAlerts.slice(0, TOP_ALERTS_PER_SCAN);

    for (const payload of topAlerts) {
      const inserted = await insertAlert(payload);
      await pushToBase44(inserted);
      await pushToTelegram(formatTelegram(inserted));
      setCooldown(payload.cooldownKey);
      alertsCreated++;
      totalAlertsCreated++;
      console.log(`[ALERT][${payload.alert_type}] ${payload.ticker} pct=${payload.percent_change} rvol=${payload.rvol} float=${payload.float} score=${Math.round(payload.score)}`);
    }

  } catch (err) {
    lastError = err.message;
    console.error("Scan error:", err.message);
  } finally {
    lastScanFinishedAt  = new Date().toISOString();
    lastScanDurationMs  = Date.now() - started;
    isScanning          = false;

    // Store last scan stats for /health
    _scanStats = { tickersFetched, candidatesFound, runnerCandidates, gapperCandidates, earlyCandidates, deepChecked, alertsCreated };
    console.log(`[Scan #${scanRuns}] Done in ${lastScanDurationMs}ms | alerts=${alertsCreated} runner=${runnerCandidates} gap=${gapperCandidates} early=${earlyCandidates}`);
  }
}

// ===== Start =====
const PORT = process.env.PORT || 8080;
app.listen(PORT, "0.0.0.0", () => {
  console.log(`Server on port ${PORT} | Telegram: ${Boolean(TELEGRAM_BOT_TOKEN && TELEGRAM_CHAT_ID)} | Base44: ${Boolean(BASE44_INGEST_URL && BASE44_API_KEY)}`);
});

scan();
setInterval(scan, SCAN_INTERVAL_MS);
