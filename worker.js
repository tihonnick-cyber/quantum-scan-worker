"use strict";

const express = require("express");
const { Pool } = require("pg");

if (typeof fetch !== "function") {
  throw new Error("Global fetch not found. Use Node 18+.");
}

const POLYGON_KEY        = process.env.POLYGON_KEY;
const MASSIVE_API_KEY    = process.env.MASSIVE_API_KEY || POLYGON_KEY || "";
const MASSIVE_API_BASE   = process.env.MASSIVE_API_BASE || "https://api.massive.com";
const DATABASE_URL       = process.env.DATABASE_URL;
const BASE44_INGEST_URL  = process.env.BASE44_INGEST_URL || "";
const BASE44_API_KEY     = process.env.BASE44_API_KEY    || "";
const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN || "";
const TELEGRAM_CHAT_ID   = process.env.TELEGRAM_CHAT_ID   || "";

if (!POLYGON_KEY)     console.error("Missing env var: POLYGON_KEY");
if (!MASSIVE_API_KEY) console.error("Missing env var: MASSIVE_API_KEY (or POLYGON_KEY fallback)");
if (!DATABASE_URL)    console.error("Missing env var: DATABASE_URL");

// ===== EXISTING QUANTUM SCANNER MASTER SWITCH =====
// Set QUANTUM_ENABLED=false in Railway to pause ONLY the old Quantum scanner.
// The separate Top-20 Scalp scanner is controlled by TOP20_ENABLED.
const QUANTUM_ENABLED = process.env.QUANTUM_ENABLED !== "false";

// ===== EXISTING SCANNER CONFIG =====
const SCAN_INTERVAL_MS         = Number(process.env.SCAN_INTERVAL_MS         || 10000);
const PRICE_MIN                = Number(process.env.PRICE_MIN                || 1);
const PRICE_MAX                = Number(process.env.PRICE_MAX                || 12);
const MAX_FLOAT                = Number(process.env.MAX_FLOAT                || 5_000_000);
const AVG_VOL_DAYS             = Number(process.env.AVG_VOL_DAYS             || 30);
const ALERT_COOLDOWN_MIN       = Number(process.env.ALERT_COOLDOWN_MIN       || 90);
const MAX_CANDIDATES           = Number(process.env.MAX_CANDIDATES           || 500);
const CONCURRENCY              = Number(process.env.CONCURRENCY              || 8);
const NEWS_LOOKBACK_MIN        = Number(process.env.NEWS_LOOKBACK_MIN        || 1440);
const SCAN_START_HOUR_PT       = Number(process.env.SCAN_START_HOUR_PT       || 4);
const SCAN_END_HOUR_PT         = Number(process.env.SCAN_END_HOUR_PT         || 12);
const MIN_MOMENTUM_SCORE       = Number(process.env.MIN_MOMENTUM_SCORE       || 78);
const TOP_ALERTS_PER_SCAN      = Number(process.env.TOP_ALERTS_PER_SCAN      || 2);
const LOW_FLOAT_THRESHOLD      = Number(process.env.LOW_FLOAT_THRESHOLD      || 2_000_000);
const MID_FLOAT_THRESHOLD      = Number(process.env.MID_FLOAT_THRESHOLD      || 5_000_000);
const EARLY_ALERTS_ENABLED     = process.env.EARLY_ALERTS_ENABLED !== "false";
const EARLY_MIN_PERCENT_CHANGE = Number(process.env.EARLY_MIN_PERCENT_CHANGE || 3);
const EARLY_MIN_RVOL           = Number(process.env.EARLY_MIN_RVOL           || 50);
const EARLY_MIN_ACCEL          = Number(process.env.EARLY_MIN_ACCEL          || 2);
const RUNNER_ENABLED           = process.env.RUNNER_ENABLED !== "false";
const RUNNER_REQUIRE_NEWS      = process.env.RUNNER_REQUIRE_NEWS === "true";
const MIN_PERCENT_CHANGE       = Number(process.env.MIN_PERCENT_CHANGE       || 5);
const RUNNER_MAX_PERCENT_CHANGE= Number(process.env.RUNNER_MAX_PERCENT_CHANGE|| 80);
const MIN_RVOL                 = Number(process.env.MIN_RVOL                 || 15);
const RUNNER_MIN_VOL           = Number(process.env.RUNNER_MIN_VOL           || 500_000);
const MIN_VOLUME_TREND         = Number(process.env.MIN_VOLUME_TREND         || 1.0);
const PREMARKET_ENABLED        = process.env.PREMARKET_ENABLED !== "false";
const PREMARKET_MIN_GAP        = Number(process.env.PREMARKET_MIN_GAP        || 8);
const PREMARKET_MIN_VOL        = Number(process.env.PREMARKET_MIN_VOL        || 150_000);
const GAPPER_MIN_RVOL          = Number(process.env.GAPPER_MIN_RVOL          || 10);
const PREMARKET_REQUIRE_NEWS   = process.env.PREMARKET_REQUIRE_NEWS === "true";
const PREMARKET_REQUIRE_SPIKE  = process.env.PREMARKET_REQUIRE_SPIKE === "true";
const VOLUME_SPIKE_MULTIPLIER  = Number(process.env.VOLUME_SPIKE_MULTIPLIER  || 2);
const VOLUME_LOOKBACK_MIN      = Number(process.env.VOLUME_LOOKBACK_MIN      || 5);
const VOLUME_BASELINE_MIN      = Number(process.env.VOLUME_BASELINE_MIN      || 30);
const POLYGON_RETRY_ATTEMPTS   = Number(process.env.POLYGON_RETRY_ATTEMPTS   || 3);
const POLYGON_RETRY_DELAY_MS   = Number(process.env.POLYGON_RETRY_DELAY_MS   || 200);

// ===== NEW TOP-20 TECHNICAL SCANNER CONFIG =====
// This scanner sends Telegram alerts and also exposes its latest calculated state at GET /top20 for the Base44 visual dashboard.
// It still does NOT write Top-20 rows into the existing alerts DB.
const TOP20_ENABLED            = process.env.TOP20_ENABLED !== "false";
const TOP20_SCAN_INTERVAL_MS   = Number(process.env.TOP20_SCAN_INTERVAL_MS   || 10000);
const TOP20_START_HOUR_PT      = Number(process.env.TOP20_START_HOUR_PT      || 6);
const TOP20_END_HOUR_PT        = Number(process.env.TOP20_END_HOUR_PT        || 14);
const TOP20_MIN_SCORE          = Number(process.env.TOP20_MIN_SCORE          || 4);
const TOP20_MIN_VOLUME         = Number(process.env.TOP20_MIN_VOLUME         || 200_000);
const TOP20_CROSS_LOOKBACK     = Number(process.env.TOP20_CROSS_LOOKBACK     || 5);
const TOP20_SMA_TREND_LOOKBACK = Number(process.env.TOP20_SMA_TREND_LOOKBACK || 5);
const TOP20_REARM_MIN          = Number(process.env.TOP20_REARM_MIN          || 5);
const TOP20_CONCURRENCY        = Number(process.env.TOP20_CONCURRENCY        || 10);
const TOP20_BAR_LOOKBACK_MIN   = Number(process.env.TOP20_BAR_LOOKBACK_MIN   || 360);
const TOP20_HISTORY_CALENDAR_DAYS = Number(process.env.TOP20_HISTORY_CALENDAR_DAYS || 7);
const TOP20_MIN_BARS           = Number(process.env.TOP20_MIN_BARS           || 110);
const TOP20_REQUIRE_HIGHER_CLOSES = process.env.TOP20_REQUIRE_HIGHER_CLOSES !== "false";

// ===== DB =====
const pool = new Pool({
  connectionString: DATABASE_URL,
  ssl: { rejectUnauthorized: false },
  max: 10,
  idleTimeoutMillis: 30_000,
  connectionTimeoutMillis: 5_000,
});

// ===== EXPRESS =====
const app = express();
app.use(express.json());

// Allow the Base44 web app to read Railway dashboard endpoints directly.
// Only simple GET/OPTIONS access is exposed here; scanner calculations stay on Railway.
app.use((req, res, next) => {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET,OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
  if (req.method === "OPTIONS") return res.sendStatus(204);
  next();
});

// ===== RUNTIME STATE =====
let isScanning         = false;
let lastError          = null;
let lastLoopAt         = null;
let lastScanStartedAt  = null;
let lastScanFinishedAt = null;
let lastScanDurationMs = null;
let totalTickersFetched= 0;
let totalAlertsCreated = 0;
let scanRuns           = 0;
let _scanStats         = {};

let top20IsScanning         = false;
let top20LastError          = null;
let top20LastStartedAt      = null;
let top20LastFinishedAt     = null;
let top20LastDurationMs     = null;
let top20ScanRuns           = 0;
let top20AlertsSent         = 0;
let top20LastLeaders        = [];
let top20LastResults        = [];
let top20LastStats          = {};

// ===== ROUTES =====
app.get("/", (_req, res) => res.send("Quantum Scan Worker is running"));

app.get("/health", async (_req, res) => {
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
      base44Enabled: Boolean(BASE44_INGEST_URL && BASE44_API_KEY),
      massiveEnabled: Boolean(MASSIVE_API_KEY),
      quantumEnabled: QUANTUM_ENABLED,
      config: {
        QUANTUM_ENABLED,
        PRICE_MIN, PRICE_MAX, MAX_FLOAT, AVG_VOL_DAYS,
        ALERT_COOLDOWN_MIN, MAX_CANDIDATES, CONCURRENCY, NEWS_LOOKBACK_MIN,
        SCAN_START_HOUR_PT, SCAN_END_HOUR_PT, MIN_MOMENTUM_SCORE,
        TOP_ALERTS_PER_SCAN, LOW_FLOAT_THRESHOLD, MID_FLOAT_THRESHOLD,
        EARLY_ALERTS_ENABLED, EARLY_MIN_PERCENT_CHANGE, EARLY_MIN_RVOL, EARLY_MIN_ACCEL,
        RUNNER_ENABLED, RUNNER_REQUIRE_NEWS, MIN_PERCENT_CHANGE,
        RUNNER_MAX_PERCENT_CHANGE, MIN_RVOL, RUNNER_MIN_VOL, MIN_VOLUME_TREND,
        PREMARKET_ENABLED, PREMARKET_MIN_GAP, PREMARKET_MIN_VOL, GAPPER_MIN_RVOL,
        PREMARKET_REQUIRE_NEWS, PREMARKET_REQUIRE_SPIKE,
        VOLUME_SPIKE_MULTIPLIER, VOLUME_LOOKBACK_MIN, VOLUME_BASELINE_MIN,
      },
      top20Scalp: {
        name: "TOP 20 SCALP",
        enabled: TOP20_ENABLED,
        isScanning: top20IsScanning,
        scanIntervalMs: TOP20_SCAN_INTERVAL_MS,
        lastError: top20LastError,
        lastStartedAt: top20LastStartedAt,
        lastFinishedAt: top20LastFinishedAt,
        lastDurationMs: top20LastDurationMs,
        scanRuns: top20ScanRuns,
        alertsSent: top20AlertsSent,
        lastLeaders: top20LastLeaders,
        latestResultCount: top20LastResults.length,
        lastStats: top20LastStats,
        config: {
          TOP20_START_HOUR_PT,
          TOP20_END_HOUR_PT,
          TOP20_MIN_SCORE,
          TOP20_MIN_VOLUME,
          TOP20_CROSS_LOOKBACK,
          TOP20_SMA_TREND_LOOKBACK,
          TOP20_REARM_MIN,
          TOP20_CONCURRENCY,
          TOP20_BAR_LOOKBACK_MIN,
          TOP20_HISTORY_CALENDAR_DAYS,
          TOP20_MIN_BARS,
          TOP20_REQUIRE_HIGHER_CLOSES,
        },
      },
    });
  } catch (e) {
    res.status(500).json({ ok: false, db: "error", error: e.message });
  }
});

function getTop20ScannerStatus() {
  if (!TOP20_ENABLED) return "OFF";

  const hour = pacificHourNow();
  if (hour < TOP20_START_HOUR_PT || hour >= TOP20_END_HOUR_PT) {
    return "OUTSIDE_HOURS";
  }

  if (top20LastError && top20LastResults.length === 0) return "ERROR";
  if (top20IsScanning) return "SCANNING";
  return "ACTIVE";
}

// Latest Top-20 Scalp state for the Base44 visual dashboard.
// This route only returns data already calculated by Railway; it does NOT trigger a new Massive scan.
app.get("/top20", (_req, res) => {
  res.set("Cache-Control", "no-store, no-cache, must-revalidate, proxy-revalidate");
  res.set("Pragma", "no-cache");
  res.set("Expires", "0");
  res.json({
    ok: !top20LastError || top20LastResults.length > 0,
    scanner: "TOP 20 SCALP",
    status: getTop20ScannerStatus(),
    enabled: TOP20_ENABLED,
    isScanning: top20IsScanning,
    updatedAt: top20LastFinishedAt,
    lastStartedAt: top20LastStartedAt,
    lastError: top20LastError,
    scanIntervalMs: TOP20_SCAN_INTERVAL_MS,
    activeWindowPT: {
      startHour: TOP20_START_HOUR_PT,
      endHour: TOP20_END_HOUR_PT,
    },
    count: top20LastResults.length,
    stocks: top20LastResults,
  });
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

app.get("/top20_test", async (_req, res) => {
  try {
    const result = await pushToTelegram(
      `🔥 <b>TOP 20 TECHNICAL — TEST 5/5</b>\n` +
      `<b>TEST</b>  $5.25  (<b>+22.50%</b>)\n` +
      `Gainer Rank: <b>#1</b>\n` +
      `Volume: 1,250,000\n\n` +
      `✅ MACD Positive\n` +
      `✅ 10 SMA > 100 SMA\n` +
      `✅ Volume ≥ 200K\n` +
      `✅ 3 Green 1m Candles + Higher Closes\n` +
      `✅ 100 SMA Trending Up\n\n` +
      `⚡ <b>BONUS:</b> Fresh 10/100 SMA crossover 2m ago\n\n` +
      `⭐ <b>PERFECT SETUP — 5/5 + CROSSOVER BONUS</b>`,
      { returnDebug: true }
    );
    res.json({ ok: true, result });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// ===== DB HELPERS =====
async function insertAlert({ ticker, price, percent_change, rvol, float, news, alert_type, meta }) {
  const result = await pool.query(
    `INSERT INTO alerts (ticker, price, percent_change, rvol, float, news, created_at, alert_type, meta)
     VALUES ($1,$2,$3,$4,$5,$6,now(),$7,$8) RETURNING *`,
    [ticker, price, percent_change, rvol, float, news, alert_type || null, meta || null]
  );
  return result.rows[0];
}

// Existing scanner cross-type cooldown — any DB alert on ticker blocks all existing alert types.
// The new TOP20 scanner intentionally does NOT use this function, so it remains independent.
async function wasAlertedRecently(ticker) {
  const result = await pool.query(
    `SELECT 1 FROM alerts
     WHERE ticker = $1
       AND created_at >= now() - ($2 || ' minutes')::interval
     LIMIT 1`,
    [ticker, String(ALERT_COOLDOWN_MIN)]
  );
  return result.rowCount > 0;
}

// ===== BASE44 =====
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

// ===== TELEGRAM =====
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
        parse_mode: "HTML",
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

function formatTelegram(a) {
  const tv    = `https://www.tradingview.com/symbols/${encodeURIComponent(a.ticker)}/`;
  const pct   = Number(a.percent_change || 0).toFixed(2);
  const rvol  = a.rvol  != null ? Number(a.rvol).toFixed(2)  : "—";
  const fl    = a.float ? Number(a.float).toLocaleString()    : "—";
  const price = a.price != null ? Number(a.price).toFixed(2) : "—";
  const news  = a.news ? "✅" : "❌";
  const type  = a.alert_type || "ALERT";

  let scoreText = "";
  let trendText = "";
  try {
    const meta = a.meta ? JSON.parse(a.meta) : null;
    if (meta?.score       != null) scoreText = `Score: <b>${Math.round(meta.score)}</b>\n`;
    if (meta?.volumeTrend != null) trendText = `Trend: ${Number(meta.volumeTrend).toFixed(2)}\n`;
  } catch {}

  return (
    `🚀 <b>Quantum Scan (${type})</b>\n` +
    `<b>${a.ticker}</b>  $${price}  (<b>${pct}%</b>)\n` +
    `RVOL: ${rvol}   Float: ${fl}\n` +
    `News: ${news}\n` +
    `${trendText}${scoreText}<a href="${tv}">Chart →</a>`
  );
}

function formatEarlyTelegram({ ticker, price, pct, rvol, float, volumeAccel, volumeTrend, score }) {
  const tv = `https://www.tradingview.com/symbols/${encodeURIComponent(ticker)}/`;
  return (
    `🔥 <b>Quantum Scan (EARLY)</b>\n` +
    `<b>${ticker}</b>  $${Number(price).toFixed(2)}  (<b>${Number(pct).toFixed(2)}%</b>)\n` +
    `RVOL: ${Number(rvol).toFixed(2)}   Float: ${Number(float).toLocaleString()}\n` +
    `Accel: ${Number(volumeAccel).toFixed(2)}  Trend: ${Number(volumeTrend).toFixed(2)}\n` +
    `Score: <b>${Math.round(score)}</b>\n` +
    `<a href="${tv}">Chart →</a>`
  );
}

function formatTop20Telegram(result) {
  const {
    ticker, rank, price, pct, volume, score, criteria, bonus,
    macdLine, macdSignal, sma10, sma100, crossBarsAgo, sma100SlopePct,
  } = result;

  const tv = `https://www.tradingview.com/symbols/${encodeURIComponent(ticker)}/`;
  const title = score >= 5
    ? `🔥 <b>TOP 20 SCALP — 5/5</b>`
    : `🟠 <b>TOP 20 SCALP — 4/5</b>`;

  const bonusLine = bonus?.freshSmaCross
    ? `⚡ <b>BONUS:</b> Fresh 10/100 SMA crossover ${crossBarsAgo === 0 ? "now" : `${crossBarsAgo}m ago`}\n`
    : `➖ Bonus: No fresh 10/100 crossover in last ${TOP20_CROSS_LOOKBACK}m\n`;

  return (
    `${title}\n` +
    `<b>${ticker}</b>  $${Number(price).toFixed(2)}  (<b>${Number(pct).toFixed(2)}%</b>)\n` +
    `Gainer Rank: <b>#${rank}</b>\n` +
    `Volume: ${Math.round(volume).toLocaleString()}\n\n` +
    `${criteria.macdPositive ? "✅" : "❌"} MACD Positive ` +
      `(${Number(macdLine).toFixed(3)} / ${Number(macdSignal).toFixed(3)})\n` +
    `${criteria.sma10Above100 ? "✅" : "❌"} 10 SMA > 100 SMA\n` +
    `${criteria.volume ? "✅" : "❌"} Volume ≥ ${TOP20_MIN_VOLUME.toLocaleString()}\n` +
    `${criteria.threeGreen ? "✅" : "❌"} 3 Green 1m Candles + Bullish Progression\n` +
    `${criteria.sma100Up ? "✅" : "❌"} 100 SMA Trending Up (${Number(sma100SlopePct).toFixed(3)}%)\n\n` +
    `${bonusLine}\n` +
    `SMA10: ${Number(sma10).toFixed(3)}   SMA100: ${Number(sma100).toFixed(3)}\n` +
    (score >= 5
      ? (bonus?.freshSmaCross
          ? `⭐ <b>PERFECT SETUP — 5/5 + CROSSOVER BONUS</b>\n`
          : `⭐ <b>PERFECT SETUP — 5/5</b>\n`)
      : (bonus?.freshSmaCross
          ? `Setup Score: <b>${score}/5 + ⚡ crossover bonus</b>\n`
          : `Setup Score: <b>${score}/5</b>\n`)) +
    `<a href="${tv}">Chart →</a>`
  );
}

// ===== CACHE =====
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

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function pacificHourNow() {
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone: "America/Los_Angeles",
    hour: "2-digit",
    hour12: false,
  }).formatToParts(new Date());
  return Number(parts.find(p => p.type === "hour")?.value || 0);
}

// ===== POLYGON (EXISTING SCANNER) =====
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
      const retryAfter =
        Number(resp.headers.get("retry-after") || 0) * 1000 ||
        POLYGON_RETRY_DELAY_MS * (attempt + 1);
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

async function fetchAllSnapshotTickers() {
  const all = [];
  let url = `https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers?limit=1000&apiKey=${POLYGON_KEY}`;

  for (let page = 0; page < 200 && url; page++) {
    const data = await polygonJson(url);
    const pageTickers = Array.isArray(data?.tickers) ? data.tickers : [];
    all.push(...pageTickers);

    const nextUrl = data?.next_url;
    if (!nextUrl) break;
    url = nextUrl.includes("apiKey=")
      ? nextUrl
      : `${nextUrl}${nextUrl.includes("?") ? "&" : "?"}apiKey=${POLYGON_KEY}`;
  }

  return all;
}

function computePercentChange(t) {
  const prevClose = Number(t?.prevDay?.c || 0);
  if (prevClose <= 0) return 0;
  const price =
    Number(t?.lastTrade?.p) ||
    Number(t?.day?.c)       ||
    prevClose;
  return ((price - prevClose) / prevClose) * 100;
}

// SPEED: use prevDay.v from snapshot as avg vol baseline — skips API call
async function getAvgDailyVolume(ticker, prevDayVol = 0) {
  if (prevDayVol > 0) return prevDayVol;

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

  try {
    const data = await polygonJson(url);
    const res  = data?.results || {};
    const payload = {
      float: Number(res?.float || 0),
      sharesOutstanding: Number(
        res?.share_class_shares_outstanding ||
        res?.weighted_shares_outstanding || 0
      ),
    };
    setCache(cache.float, ticker, payload, 24 * 3_600_000);
    return payload;
  } catch (e) {
    if (!e.message.includes("404")) {
      console.error(`[${ticker}] getFloatInfo error:`, e.message);
    }
    const fallback = { float: 0, sharesOutstanding: 0 };
    setCache(cache.float, ticker, fallback, 60 * 60_000);
    return fallback;
  }
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

  const data     = await polygonJson(url);
  const filtered = (data?.results || [])
    .filter(r => Number(r?.t || 0) >= cutoff)
    .map(r => ({
      t: Number(r.t),
      o: Number(r.o || 0),
      h: Number(r.h || 0),
      l: Number(r.l || 0),
      c: Number(r.c || 0),
      v: Number(r.v || 0),
    }));

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

  const lastAvg   = lastN.length
    ? lastN.reduce((s, b) => s + b.v, 0) / lastN.length : 0;
  const baseSlice = base.slice(-VOLUME_BASELINE_MIN);
  const baseAvg   = baseSlice.length
    ? baseSlice.reduce((s, b) => s + b.v, 0) / baseSlice.length : 0;

  const spike  = baseAvg > 0 ? lastAvg / baseAvg : 0;
  const pmHigh = sortedAsc.length
    ? Math.max(...sortedAsc.map(b => b.h || 0)) : 0;

  const recentBars  = sortedAsc.slice(-3);
  const earlierBars = sortedAsc.slice(-15, -3);
  const recentAvg   = recentBars.length
    ? recentBars.reduce((s, b)  => s + b.v, 0) / recentBars.length  : 0;
  const earlierAvg  = earlierBars.length
    ? earlierBars.reduce((s, b) => s + b.v, 0) / earlierBars.length : 0;
  const volumeTrend = earlierAvg > 0 ? recentAvg / earlierAvg : 1;

  return { totalVol, spikeMultiplier: spike, pmHigh, volumeTrend };
}

// ===== MOMENTUM SCORE =====
function computeMomentumScore({ pct, rvol, volumeAccel, volumeTrend, float, news, breakout }) {
  let score = 0;

  score += rvol >= 200 ? 40
         : rvol >= 100 ? 35
         : rvol >= 50  ? 28
         : rvol >= 20  ? 20
         : rvol >= 10  ? 12
         : rvol >= 5   ? 6 : 0;

  score += pct < 5   ? 0
         : pct < 15  ? Math.min(pct * 1.2, 18)
         : pct < 30  ? 15
         : pct < 60  ? 10
         : pct < 100 ? 5
         : 0;

  score += volumeAccel > 4    ? 20
         : volumeAccel > 3    ? 15
         : volumeAccel > 2    ? 10
         : volumeAccel > 1.25 ? 5 : 0;

  score += volumeTrend > 2.0 ? 15
         : volumeTrend > 1.5 ? 10
         : volumeTrend > 1.0 ? 5
         : volumeTrend < 0.7 ? -15
         : 0;

  score += float < LOW_FLOAT_THRESHOLD ? 15
         : float < MID_FLOAT_THRESHOLD ? 8
         : float < MAX_FLOAT           ? 3 : 0;

  if (news)     score += 10;
  if (breakout) score += 15;

  return score;
}

// ===== EXISTING SCANNER COOLDOWN =====
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

function tickerInAnyCooldown(ticker) {
  const types = ["EARLY", "RUNNER", "GAPPER", "PM_BREAKOUT", "LOW_FLOAT_SQUEEZE"];
  return types.some(t => inCooldown(`${ticker}:${t}`));
}

// ===== CONCURRENCY =====
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

// ============================================================================
// NEW TOP-20 TECHNICAL SCANNER (MASSIVE -> RAILWAY -> TELEGRAM ONLY)
// ============================================================================

async function massiveJson(pathOrUrl, attempt = 0) {
  let url = pathOrUrl.startsWith("http")
    ? pathOrUrl
    : `${MASSIVE_API_BASE}${pathOrUrl}`;

  if (!url.includes("apiKey=")) {
    url += `${url.includes("?") ? "&" : "?"}apiKey=${encodeURIComponent(MASSIVE_API_KEY)}`;
  }

  let resp;
  try {
    resp = await fetch(url);
  } catch (e) {
    if (attempt < POLYGON_RETRY_ATTEMPTS - 1) {
      await sleep(POLYGON_RETRY_DELAY_MS * (attempt + 1));
      return massiveJson(pathOrUrl, attempt + 1);
    }
    throw e;
  }

  if (resp.status === 429 || resp.status >= 500) {
    if (attempt < POLYGON_RETRY_ATTEMPTS - 1) {
      const retryAfter =
        Number(resp.headers.get("retry-after") || 0) * 1000 ||
        POLYGON_RETRY_DELAY_MS * (attempt + 1);
      await sleep(retryAfter);
      return massiveJson(pathOrUrl, attempt + 1);
    }
  }

  if (!resp.ok) {
    const text = await resp.text().catch(() => "");
    throw new Error(`Massive ${resp.status}: ${text.slice(0, 400)}`);
  }

  return resp.json();
}

async function fetchTop20Gainers() {
  const data = await massiveJson(
    "/v2/snapshot/locale/us/markets/stocks/gainers"
  );

  const tickers = Array.isArray(data?.tickers) ? data.tickers : [];

  return tickers.slice(0, 20).map((t, index) => {
    const price =
      Number(t?.lastTrade?.p) ||
      Number(t?.day?.c) ||
      Number(t?.prevDay?.c) || 0;

    const pct = Number.isFinite(Number(t?.todaysChangePerc))
      ? Number(t.todaysChangePerc)
      : computePercentChange(t);

    return {
      rank: index + 1,
      ticker: String(t?.ticker || "").toUpperCase(),
      price,
      pct,
      volume: Number(t?.day?.v || 0),
    };
  }).filter(x => x.ticker && x.price > 0 && /^[A-Z]{1,5}$/.test(x.ticker));
}

// Cache technical bars until the next completed 1-minute candle.
// This keeps the scanner fast without re-downloading 100+ bars every 10 seconds.
const top20TechnicalBarCache = new Map();

function completedMinuteKey() {
  return Math.floor(Date.now() / 60_000) - 1;
}

async function getTop20MinuteBars(ticker) {
  const minuteKey = completedMinuteKey();
  const cacheKey = `${ticker}:${minuteKey}`;
  const cached = top20TechnicalBarCache.get(cacheKey);
  if (cached) return cached;

  // IMPORTANT:
  // The 100-SMA needs at least 100 completed 1-minute bars. At 6:00 AM PT,
  // thin premarket stocks may not have 110 bars from the current day alone.
  // So fetch several CALENDAR days and use the most recent completed bars,
  // which automatically carries the indicator history across prior sessions,
  // weekends, and holidays.
  const currentMinuteStart = Math.floor(Date.now() / 60_000) * 60_000;
  const historyStart = Date.now() - TOP20_HISTORY_CALENDAR_DAYS * 86_400_000;
  const from = new Date(historyStart).toISOString().slice(0, 10);
  const to   = new Date().toISOString().slice(0, 10);

  const path =
    `/v2/aggs/ticker/${encodeURIComponent(ticker)}` +
    `/range/1/minute/${from}/${to}` +
    `?adjusted=true&sort=asc&limit=50000`;

  const data = await massiveJson(path);
  let bars = (data?.results || [])
    .map(r => ({
      t: Number(r?.t || 0),
      o: Number(r?.o || 0),
      h: Number(r?.h || 0),
      l: Number(r?.l || 0),
      c: Number(r?.c || 0),
      v: Number(r?.v || 0),
    }))
    .filter(b =>
      b.t < currentMinuteStart &&
      b.o > 0 && b.h > 0 && b.l > 0 && b.c > 0
    )
    .sort((a, b) => a.t - b.t);

  // Keep enough history for the 100-SMA, trend comparison, recent cross,
  // MACD warm-up, and the latest candle pattern without carrying thousands
  // of old bars through every calculation.
  const minimumNeeded = Math.max(
    TOP20_MIN_BARS,
    100 + TOP20_SMA_TREND_LOOKBACK + TOP20_CROSS_LOOKBACK + 10,
    60
  );
  const barsToKeep = Math.max(minimumNeeded + 100, 250);
  if (bars.length > barsToKeep) bars = bars.slice(-barsToKeep);

  // Remove older cache keys for this ticker so memory stays small.
  for (const key of top20TechnicalBarCache.keys()) {
    if (key.startsWith(`${ticker}:`) && key !== cacheKey) {
      top20TechnicalBarCache.delete(key);
    }
  }

  top20TechnicalBarCache.set(cacheKey, bars);
  return bars;
}

function sma(values, period, endExclusive = values.length) {
  if (period <= 0 || endExclusive < period) return null;
  const start = endExclusive - period;
  let sum = 0;
  for (let i = start; i < endExclusive; i++) sum += values[i];
  return sum / period;
}

function emaSeries(values, period) {
  if (!Array.isArray(values) || values.length < period || period <= 0) return [];

  const out = new Array(values.length).fill(null);
  const seed = values.slice(0, period).reduce((a, b) => a + b, 0) / period;
  out[period - 1] = seed;

  const k = 2 / (period + 1);
  for (let i = period; i < values.length; i++) {
    out[i] = values[i] * k + out[i - 1] * (1 - k);
  }
  return out;
}

function computeMacd(values, fastPeriod = 12, slowPeriod = 26, signalPeriod = 9) {
  if (values.length < slowPeriod + signalPeriod) return null;

  const fast = emaSeries(values, fastPeriod);
  const slow = emaSeries(values, slowPeriod);
  const macd = values.map((_, i) =>
    fast[i] != null && slow[i] != null ? fast[i] - slow[i] : null
  );

  const validMacd = macd.filter(v => v != null);
  if (validMacd.length < signalPeriod) return null;

  const signalValid = emaSeries(validMacd, signalPeriod);
  const signal = new Array(values.length).fill(null);
  let validIndex = 0;
  for (let i = 0; i < macd.length; i++) {
    if (macd[i] != null) {
      signal[i] = signalValid[validIndex];
      validIndex++;
    }
  }

  const last = values.length - 1;
  if (macd[last] == null || signal[last] == null) return null;

  return {
    macd: macd[last],
    signal: signal[last],
    histogram: macd[last] - signal[last],
  };
}

function findRecentSmaCross(closes, fastPeriod, slowPeriod, lookbackBars) {
  if (closes.length < slowPeriod + lookbackBars + 1) {
    return { passed: false, barsAgo: null, smaFast: null, smaSlow: null };
  }

  const currentFast = sma(closes, fastPeriod);
  const currentSlow = sma(closes, slowPeriod);
  if (currentFast == null || currentSlow == null || currentFast <= currentSlow) {
    return { passed: false, barsAgo: null, smaFast: currentFast, smaSlow: currentSlow };
  }

  const maxLookback = Math.min(lookbackBars, closes.length - slowPeriod - 1);

  for (let barsAgo = 0; barsAgo <= maxLookback; barsAgo++) {
    const currEnd = closes.length - barsAgo;
    const prevEnd = currEnd - 1;
    const currFast = sma(closes, fastPeriod, currEnd);
    const currSlow = sma(closes, slowPeriod, currEnd);
    const prevFast = sma(closes, fastPeriod, prevEnd);
    const prevSlow = sma(closes, slowPeriod, prevEnd);

    if (
      currFast != null && currSlow != null &&
      prevFast != null && prevSlow != null &&
      currFast > currSlow && prevFast <= prevSlow
    ) {
      return {
        passed: true,
        barsAgo,
        smaFast: currentFast,
        smaSlow: currentSlow,
      };
    }
  }

  return {
    passed: false,
    barsAgo: null,
    smaFast: currentFast,
    smaSlow: currentSlow,
  };
}

function evaluateThreeGreenBullish(bars) {
  if (bars.length < 3) return false;
  const [a, b, c] = bars.slice(-3);

  const allGreen = a.c > a.o && b.c > b.o && c.c > c.o;
  if (!allGreen) return false;

  if (!TOP20_REQUIRE_HIGHER_CLOSES) return true;

  // Bullish progression: each green candle closes higher than the prior candle.
  return a.c < b.c && b.c < c.c;
}

function evaluateTop20Technical(leader, bars) {
  if (!bars || bars.length < TOP20_MIN_BARS) {
    return {
      ...leader,
      insufficientBars: true,
      barsAvailable: bars?.length || 0,
      score: 0,
    };
  }

  const closes = bars.map(b => b.c);
  const macd = computeMacd(closes);
  const cross = findRecentSmaCross(closes, 10, 100, TOP20_CROSS_LOOKBACK);

  const currentSma10 = sma(closes, 10);
  const currentSma100 = sma(closes, 100);
  const pastEnd = closes.length - TOP20_SMA_TREND_LOOKBACK;
  const pastSma100 = sma(closes, 100, pastEnd);

  const sma100SlopePct =
    currentSma100 != null && pastSma100 != null && pastSma100 !== 0
      ? ((currentSma100 - pastSma100) / pastSma100) * 100
      : 0;

  // Five CORE criteria determine the 4/5 or 5/5 setup score.
  // The fresh 10/100 crossover is intentionally a BONUS only.
  const criteria = {
    macdPositive: Boolean(macd && macd.macd > macd.signal && macd.histogram > 0),
    sma10Above100: Boolean(
      currentSma10 != null &&
      currentSma100 != null &&
      currentSma10 > currentSma100
    ),
    volume: Number(leader.volume || 0) >= TOP20_MIN_VOLUME,
    threeGreen: evaluateThreeGreenBullish(bars),
    sma100Up: Boolean(
      currentSma100 != null &&
      pastSma100 != null &&
      currentSma100 > pastSma100
    ),
  };

  const bonus = {
    freshSmaCross: Boolean(cross.passed),
  };

  const score = Object.values(criteria).filter(Boolean).length;

  return {
    ...leader,
    score,
    criteria,
    bonus,
    macdLine: macd?.macd ?? 0,
    macdSignal: macd?.signal ?? 0,
    macdHistogram: macd?.histogram ?? 0,
    sma10: currentSma10 ?? 0,
    sma100: currentSma100 ?? 0,
    crossBarsAgo: cross.barsAgo,
    sma100SlopePct,
    barsAvailable: bars.length,
    insufficientBars: false,
  };
}

function serializeTop20Result(result) {
  const c = result.criteria || {};
  const b = result.bonus || {};

  return {
    rank: Number(result.rank || 0),
    ticker: String(result.ticker || ""),
    price: Number(Number(result.price || 0).toFixed(4)),
    percentChange: Number(Number(result.pct || 0).toFixed(2)),
    volume: Math.round(Number(result.volume || 0)),
    score: Number(result.score || 0),

    // The five core criteria shown in Base44.
    macdPositive: Boolean(c.macdPositive),
    sma10Above100: Boolean(c.sma10Above100),
    volumePass: Boolean(c.volume),
    threeGreen: Boolean(c.threeGreen),
    sma100Rising: Boolean(c.sma100Up),

    // Fresh crossover is a BONUS, not part of the 5-point score.
    freshCross: Boolean(b.freshSmaCross),
    crossMinutesAgo: result.crossBarsAgo != null ? Number(result.crossBarsAgo) : null,

    // Extra values for detail cards / troubleshooting.
    sma10: Number(Number(result.sma10 || 0).toFixed(4)),
    sma100: Number(Number(result.sma100 || 0).toFixed(4)),
    sma100SlopePct: Number(Number(result.sma100SlopePct || 0).toFixed(4)),
    macdLine: Number(Number(result.macdLine || 0).toFixed(6)),
    macdSignal: Number(Number(result.macdSignal || 0).toFixed(6)),
    macdHistogram: Number(Number(result.macdHistogram || 0).toFixed(6)),
    barsAvailable: Number(result.barsAvailable || 0),
    insufficientBars: Boolean(result.insufficientBars),
  };
}

// Separate state so TOP20 alerts do not interfere with your existing scanner cooldowns.
const top20AlertState = new Map();

function shouldSendTop20Alert(result) {
  const now = Date.now();
  const ticker = result.ticker;
  const freshBonus = Boolean(result.bonus?.freshSmaCross);
  const state = top20AlertState.get(ticker) || {
    lastScore: 0,
    lastAlertScore: 0,
    lastAlertAt: 0,
    belowThresholdSince: 0,
    lastBonus: false,
  };

  const score = result.score;

  if (score < TOP20_MIN_SCORE) {
    if (!state.belowThresholdSince) state.belowThresholdSince = now;

    // Rearm after it has been below the qualifying threshold for the configured time.
    if (now - state.belowThresholdSince >= TOP20_REARM_MIN * 60_000) {
      state.lastAlertScore = 0;
      state.lastAlertAt = 0;
      state.lastBonus = false;
    }

    state.lastScore = score;
    top20AlertState.set(ticker, state);
    return false;
  }

  state.belowThresholdSince = 0;

  // First qualifying 4/5 or 5/5 alert.
  if (state.lastAlertScore < TOP20_MIN_SCORE) {
    state.lastScore = score;
    state.lastAlertScore = score;
    state.lastAlertAt = now;
    state.lastBonus = freshBonus;
    top20AlertState.set(ticker, state);
    return true;
  }

  // Upgrade from 4/5 to 5/5 immediately.
  if (score === 5 && state.lastAlertScore < 5) {
    state.lastScore = score;
    state.lastAlertScore = 5;
    state.lastAlertAt = now;
    state.lastBonus = freshBonus;
    top20AlertState.set(ticker, state);
    return true;
  }

  // If the setup already qualified and a NEW fresh crossover appears later,
  // send one bonus alert. The crossover does not change the 4/5 or 5/5 score.
  if (freshBonus && !state.lastBonus) {
    state.lastScore = score;
    state.lastAlertScore = Math.max(state.lastAlertScore, score);
    state.lastAlertAt = now;
    state.lastBonus = true;
    top20AlertState.set(ticker, state);
    return true;
  }

  state.lastScore = score;
  state.lastBonus = freshBonus;
  top20AlertState.set(ticker, state);
  return false;
}

async function scanTop20Technicals() {
  if (!TOP20_ENABLED) return;

  const hour = pacificHourNow();
  if (hour < TOP20_START_HOUR_PT || hour >= TOP20_END_HOUR_PT) return;

  if (top20IsScanning) return;
  top20IsScanning = true;
  top20ScanRuns++;
  top20LastStartedAt = new Date().toISOString();
  top20LastError = null;
  const started = Date.now();

  let leadersFetched = 0;
  let analyzed = 0;
  let insufficientBars = 0;
  let qualifying4 = 0;
  let qualifying5 = 0;
  let alertsThisRun = 0;

  try {
    const leaders = await fetchTop20Gainers();
    leadersFetched = leaders.length;
    top20LastLeaders = leaders.map(x => ({
      rank: x.rank,
      ticker: x.ticker,
      price: Number(x.price.toFixed(4)),
      pct: Number(x.pct.toFixed(2)),
      volume: Math.round(x.volume),
    }));

    const evaluated = await runWithConcurrency(
      leaders,
      TOP20_CONCURRENCY,
      async leader => {
        try {
          const bars = await getTop20MinuteBars(leader.ticker);
          const result = evaluateTop20Technical(leader, bars);
          return result;
        } catch (e) {
          console.error(`[TOP20][${leader.ticker}] error:`, e.message);
          return null;
        }
      }
    );

    // Publish the complete current Top-20 state for Base44, including 0/5–5/5 names.
    // Sort by technical score first, then original gainer rank, matching the app design.
    top20LastResults = evaluated
      .filter(Boolean)
      .map(serializeTop20Result)
      .sort((a, b) => (b.score - a.score) || (a.rank - b.rank));

    for (const result of evaluated.filter(Boolean)) {
      if (result.insufficientBars) {
        insufficientBars++;
        continue;
      }

      analyzed++;
      if (result.score === 4) qualifying4++;
      if (result.score === 5) qualifying5++;

      if (result.score >= TOP20_MIN_SCORE && shouldSendTop20Alert(result)) {
        await pushToTelegram(formatTop20Telegram(result));
        alertsThisRun++;
        top20AlertsSent++;
        console.log(
          `[TOP20][ALERT] #${result.rank} ${result.ticker} ` +
          `score=${result.score}/5 pct=${result.pct.toFixed(2)} vol=${Math.round(result.volume)}`
        );
      } else if (result.score < TOP20_MIN_SCORE) {
        // Update/rearm state for non-qualifying names.
        shouldSendTop20Alert(result);
      }
    }
  } catch (e) {
    top20LastError = e.message;
    console.error("TOP20 scan error:", e.message);
  } finally {
    top20LastFinishedAt = new Date().toISOString();
    top20LastDurationMs = Date.now() - started;
    top20LastStats = {
      leadersFetched,
      analyzed,
      insufficientBars,
      qualifying4,
      qualifying5,
      alertsThisRun,
    };
    top20IsScanning = false;
  }
}

// ============================================================================
// EXISTING SCANNER
// ============================================================================
async function scan() {
  lastLoopAt = new Date().toISOString();

  // Master switch for the existing Quantum scanner only.
  if (!QUANTUM_ENABLED) {
    return;
  }

  const hour = pacificHourNow();

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
    tickersFetched       = tickers.length;
    totalTickersFetched += tickersFetched;

    let raw = tickers
      .map(t => {
        const symbol     = t?.ticker;
        const price      =
          Number(t?.lastTrade?.p) ||
          Number(t?.day?.c)       ||
          Number(t?.prevDay?.c)   || 0;
        const pct        = computePercentChange(t);
        const dayVol     = Number(t?.day?.v     || 0);
        const prevDayVol = Number(t?.prevDay?.v || 0);
        return { symbol, price, pct, dayVol, prevDayVol };
      })
      .filter(x => x.symbol && x.price > 0 && x.price >= PRICE_MIN && x.price <= PRICE_MAX)
      .filter(x => /^[A-Z]{1,5}$/.test(x.symbol));

    candidatesFound = raw.length;
    raw.sort((a, b) => Math.abs(b.pct) - Math.abs(a.pct));
    if (MAX_CANDIDATES > 0) raw = raw.slice(0, MAX_CANDIDATES);

    console.log(`Fetched: ${tickersFetched} | In range: ${candidatesFound} | Deep-checking: ${raw.length}`);

    const scoredAlerts = [];

    await runWithConcurrency(raw, CONCURRENCY, async (c) => {
      const ticker = c.symbol;
      try {
        if (tickerInAnyCooldown(ticker)) return;

        const floatInfo      = await getFloatInfo(ticker);
        const trueFloat      = Number(floatInfo?.float || 0);
        const sharesOut      = Number(floatInfo?.sharesOutstanding || 0);
        const effectiveFloat = trueFloat > 0 ? trueFloat : sharesOut;

        if (!effectiveFloat || effectiveFloat > MAX_FLOAT) return;

        const avgVol = await getAvgDailyVolume(ticker, c.prevDayVol);
        if (!avgVol || avgVol <= 0) return;

        const rvol = c.dayVol / avgVol;

        let volumeAccel = 0, volumeTrend = 1, breakout = false;
        let pmHigh = 0, totalVol = 0, spikeMultiplier = 0;

        try {
          const pm    = await computePremarketVolumeAndSpike(ticker);
          volumeAccel     = Number(pm.spikeMultiplier || 0);
          spikeMultiplier = volumeAccel;
          pmHigh          = Number(pm.pmHigh || 0);
          totalVol        = Number(pm.totalVol || 0);
          volumeTrend     = Number(pm.volumeTrend || 1);
          breakout        = pmHigh > 0 && c.price >= pmHigh;
        } catch { /* non-fatal */ }

        const newsOk    = await hasRecentNews(ticker, NEWS_LOOKBACK_MIN);
        const baseScore = computeMomentumScore({
          pct: c.pct, rvol, volumeAccel, volumeTrend,
          float: effectiveFloat, news: newsOk, breakout,
        });

        // ── EARLY ALERT ──────────────────────────────────────────────
        if (
          EARLY_ALERTS_ENABLED &&
          c.pct       >= EARLY_MIN_PERCENT_CHANGE &&
          rvol        >= EARLY_MIN_RVOL &&
          volumeAccel >= EARLY_MIN_ACCEL
        ) {
          const key = `${ticker}:EARLY`;
          if (!inCooldown(key) && !(await wasAlertedRecently(ticker))) {
            earlyCandidates++;
            await pushToTelegram(
              formatEarlyTelegram({
                ticker, price: c.price, pct: c.pct, rvol,
                float: Math.round(effectiveFloat),
                volumeAccel, volumeTrend, score: baseScore,
              })
            );
            setCooldown(key);
            console.log(`[EARLY] ${ticker} pct=${c.pct.toFixed(2)} rvol=${rvol.toFixed(2)} accel=${volumeAccel.toFixed(2)} trend=${volumeTrend.toFixed(2)} score=${Math.round(baseScore)}`);

            // Do not also send RUNNER/GAPPER for this same ticker in the same pass.
            // The cross-type cooldown will keep the old Quantum scanner from duplicating it afterward.
            return;
          }
        }

        // ── RUNNER ───────────────────────────────────────────────────
        if (
          RUNNER_ENABLED &&
          c.pct    >= MIN_PERCENT_CHANGE &&
          c.pct    <= RUNNER_MAX_PERCENT_CHANGE &&
          c.dayVol >= RUNNER_MIN_VOL &&
          rvol     >= MIN_RVOL
        ) {
          if (RUNNER_REQUIRE_NEWS && !newsOk) return;
          if (volumeTrend < MIN_VOLUME_TREND) return;
          if (baseScore < MIN_MOMENTUM_SCORE) return;

          runnerCandidates++;

          let type = "RUNNER";
          if (breakout) type = "PM_BREAKOUT";
          if (effectiveFloat < LOW_FLOAT_THRESHOLD && c.pct >= 15 && rvol >= 8) {
            type = "LOW_FLOAT_SQUEEZE";
          }

          const key = `${ticker}:${type}`;
          if (inCooldown(key)) return;
          if (await wasAlertedRecently(ticker)) { setCooldown(key); return; }

          deepChecked++;
          scoredAlerts.push({
            ticker, price: c.price,
            percent_change: Number(c.pct.toFixed(2)),
            rvol: Number(rvol.toFixed(2)),
            float: Math.round(effectiveFloat),
            news: Boolean(newsOk),
            alert_type: type,
            score: baseScore,
            meta: JSON.stringify({
              score: Number(baseScore.toFixed(2)),
              volumeAccel: Number(volumeAccel.toFixed(2)),
              volumeTrend: Number(volumeTrend.toFixed(2)),
              breakout,
              pmHigh: Number(pmHigh.toFixed(2)),
              trueFloat: Math.round(trueFloat),
              sharesOutstanding: Math.round(sharesOut),
            }),
            cooldownKey: key,
          });
          return;
        }

       // ── GAPPER ───────────────────────────────────────────────────
        if (PREMARKET_ENABLED && c.pct >= PREMARKET_MIN_GAP) {
          const gapBreakout = pmHigh > 0 && c.price >= pmHigh;

          if (totalVol < PREMARKET_MIN_VOL) return;
          if (rvol < GAPPER_MIN_RVOL) return;
          if (volumeTrend < MIN_VOLUME_TREND) return;
          if (PREMARKET_REQUIRE_SPIKE && spikeMultiplier < VOLUME_SPIKE_MULTIPLIER) return;
          if (PREMARKET_REQUIRE_NEWS && !newsOk) return;

          const score = computeMomentumScore({
            pct: c.pct, rvol, volumeAccel: spikeMultiplier, volumeTrend,
            float: effectiveFloat, news: newsOk, breakout: gapBreakout,
          });
          if (score < MIN_MOMENTUM_SCORE) return;

          gapperCandidates++;
          const key = `${ticker}:GAPPER`;
          if (inCooldown(key)) return;
          if (await wasAlertedRecently(ticker)) { setCooldown(key); return; }

          deepChecked++;
          scoredAlerts.push({
            ticker, price: c.price,
            percent_change: Number(c.pct.toFixed(2)),
            rvol: Number(rvol.toFixed(2)),
            float: Math.round(effectiveFloat),
            news: Boolean(newsOk),
            alert_type: "GAPPER",
            score,
            meta: JSON.stringify({
              score: Number(score.toFixed(2)),
              premarket_vol_window: Math.round(totalVol),
              volume_spike: Number(spikeMultiplier.toFixed(2)),
              volumeTrend: Number(volumeTrend.toFixed(2)),
              breakout: gapBreakout,
              pmHigh: Number(pmHigh || 0),
              trueFloat: Math.round(trueFloat),
              sharesOutstanding: Math.round(sharesOut),
            }),
            cooldownKey: key,
          });
        }

      } catch (e) {
        if (!e.message.includes("404")) {
          console.error(`[${ticker}] check error:`, e.message);
        }
      }
    });

    scoredAlerts.sort((a, b) => b.score - a.score);
    const topAlerts = scoredAlerts.slice(0, TOP_ALERTS_PER_SCAN);

    for (const payload of topAlerts) {
      const inserted = await insertAlert(payload);
      await pushToBase44(inserted);
      await pushToTelegram(formatTelegram(inserted));
      setCooldown(payload.cooldownKey);
      alertsCreated++;
      totalAlertsCreated++;
      console.log(`[ALERT][${payload.alert_type}] ${payload.ticker} pct=${payload.percent_change} rvol=${payload.rvol} float=${payload.float} trend=${JSON.parse(payload.meta).volumeTrend} score=${Math.round(payload.score)}`);
    }

  } catch (err) {
    lastError = err.message;
    console.error("Scan error:", err.message);
  } finally {
    lastScanFinishedAt = new Date().toISOString();
    lastScanDurationMs = Date.now() - started;
    isScanning = false;
    _scanStats = {
      tickersFetched, candidatesFound,
      runnerCandidates, gapperCandidates,
      earlyCandidates, deepChecked, alertsCreated,
    };
    console.log(`[Scan #${scanRuns}] Done in ${lastScanDurationMs}ms | alerts=${alertsCreated} runner=${runnerCandidates} gap=${gapperCandidates} early=${earlyCandidates}`);
  }
}

// ===== START =====
const PORT = process.env.PORT || 8080;
app.listen(PORT, "0.0.0.0", () => {
  console.log(
    `Server on port ${PORT} | ` +
    `Telegram: ${Boolean(TELEGRAM_BOT_TOKEN && TELEGRAM_CHAT_ID)} | ` +
    `Base44: ${Boolean(BASE44_INGEST_URL && BASE44_API_KEY)} | ` +
    `Quantum: ${QUANTUM_ENABLED} | ` +
    `Top20Scalp: ${TOP20_ENABLED}`
  );
});

// Existing scanner
scan();
setInterval(scan, SCAN_INTERVAL_MS);

// New Top-20 Scalp scanner
scanTop20Technicals();
setInterval(scanTop20Technicals, TOP20_SCAN_INTERVAL_MS);
