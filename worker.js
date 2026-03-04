"use strict";

const express = require("express");
const { Pool } = require("pg");

if (typeof fetch !== "function") {
  throw new Error("Global fetch not found. Use Node 18+ on Railway.");
}

/**
 * REQUIRED ENV VARS
 * POLYGON_KEY
 * DATABASE_URL
 *
 * OPTIONAL
 * BASE44_INGEST_URL
 * BASE44_API_KEY
 *
 * TELEGRAM_BOT_TOKEN
 * TELEGRAM_CHAT_ID
 */

const POLYGON_KEY = process.env.POLYGON_KEY;
const DATABASE_URL = process.env.DATABASE_URL;

const BASE44_INGEST_URL = process.env.BASE44_INGEST_URL || "";
const BASE44_API_KEY = process.env.BASE44_API_KEY || "";

// ===== Criteria (scan rules) =====
const SCAN_INTERVAL_MS = Number(process.env.SCAN_INTERVAL_MS || 60000);

const PRICE_MIN = Number(process.env.PRICE_MIN || 2);
const PRICE_MAX = Number(process.env.PRICE_MAX || 20);

const MIN_PERCENT_CHANGE = Number(process.env.MIN_PERCENT_CHANGE || 10);
const MIN_RVOL = Number(process.env.MIN_RVOL || 5);
const MAX_FLOAT = Number(process.env.MAX_FLOAT || 5000000);

const AVG_VOL_DAYS = Number(process.env.AVG_VOL_DAYS || 30);
const NEWS_LOOKBACK_MIN = Number(process.env.NEWS_LOOKBACK_MIN || 1440);

const ALERT_COOLDOWN_MIN = Number(process.env.ALERT_COOLDOWN_MIN || 30);
const MAX_CANDIDATES = Number(process.env.MAX_CANDIDATES || 300);
const CONCURRENCY = Number(process.env.CONCURRENCY || 4);

// ===== Premarket gappers =====
const PREMARKET_ENABLED = String(process.env.PREMARKET_ENABLED || "true").toLowerCase() === "true";
const PREMARKET_MIN_GAP = Number(process.env.PREMARKET_MIN_GAP || 5);
const PREMARKET_MIN_VOL = Number(process.env.PREMARKET_MIN_VOL || 20000);
const PREMARKET_REQUIRE_NEWS =
  String(process.env.PREMARKET_REQUIRE_NEWS || "false").toLowerCase() === "true";

// ===== Runners (volume spike ignition) =====
const RUNNER_ENABLED = String(process.env.RUNNER_ENABLED || "false").toLowerCase() === "true";
const EARLY_MIN_PERCENT_CHANGE = Number(process.env.EARLY_MIN_PERCENT_CHANGE || 3);
const RUNNER_MIN_VOL = Number(process.env.RUNNER_MIN_VOL || 20000);
const VOLUME_SPIKE_MULTIPLIER = Number(process.env.VOLUME_SPIKE_MULTIPLIER || 3);
const VOLUME_LOOKBACK_MIN = Number(process.env.VOLUME_LOOKBACK_MIN || 5);
const VOLUME_BASELINE_MIN = Number(process.env.VOLUME_BASELINE_MIN || 30);

// ===== Telegram =====
const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN || "";
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID || "";
const TELEGRAM_ENABLED = Boolean(TELEGRAM_BOT_TOKEN && TELEGRAM_CHAT_ID);

if (!POLYGON_KEY) console.error("Missing env var: POLYGON_KEY");
if (!DATABASE_URL) console.error("Missing env var: DATABASE_URL");

// ===== Postgres pool =====
const pool = new Pool({
  connectionString: DATABASE_URL,
  ssl: { rejectUnauthorized: false },
});

// ===== Express =====
const app = express();
app.use(express.json());

// =========================
// RUNTIME STATE (/health)
// =========================
let isScanning = false;
let lastError = null;

let lastLoopAt = null;
let lastScanStartedAt = null;
let lastScanFinishedAt = null;
let lastScanDurationMs = null;

let tickersFetched = 0;
let candidatesFound = 0;
let deepChecked = 0;
let alertsCreated = 0;
let scanRuns = 0;

let premarketCandidates = 0;
let runnerCandidates = 0;
let runnerChecked = 0;

// =========================
// ROUTES
// =========================
app.get("/", (req, res) => res.send("Quantum Scan Worker is running"));

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
      tickersFetched,
      candidatesFound,
      deepChecked,
      alertsCreated,
      scanRuns,
      premarketCandidates,
      runnerCandidates,
      runnerChecked,
      telegramEnabled: TELEGRAM_ENABLED,
      base44Enabled: Boolean(BASE44_INGEST_URL && BASE44_API_KEY),
      criteria: {
        PRICE_MIN,
        PRICE_MAX,
        MIN_PERCENT_CHANGE,
        MIN_RVOL,
        MAX_FLOAT,
        AVG_VOL_DAYS,
        NEWS_LOOKBACK_MIN,
        ALERT_COOLDOWN_MIN,
        MAX_CANDIDATES,
        CONCURRENCY,
        PREMARKET_ENABLED,
        PREMARKET_MIN_GAP,
        PREMARKET_MIN_VOL,
        PREMARKET_REQUIRE_NEWS,
        RUNNER_ENABLED,
        EARLY_MIN_PERCENT_CHANGE,
        RUNNER_MIN_VOL,
        VOLUME_SPIKE_MULTIPLIER,
        VOLUME_LOOKBACK_MIN,
        VOLUME_BASELINE_MIN,
      },
    });
  } catch (e) {
    res.status(500).json({ ok: false, db: "error", error: e.message });
  }
});

app.get("/alerts", async (req, res) => {
  try {
    const result = await pool.query(
      `SELECT * FROM alerts ORDER BY created_at DESC LIMIT 200;`
    );
    res.json(result.rows);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get("/test", async (req, res) => {
  try {
    const inserted = await insertAlert({
      ticker: "TEST",
      price: 5.25,
      percent_change: 12.5,
      rvol: 5.1,
      float: 2000000,
      news: true,
      alert_type: "TEST",
    });

    await pushToBase44(inserted);
    await sendTelegramAlert(inserted);

    res.json({ success: true, alert: inserted });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// =========================
// DB HELPERS
// =========================
async function insertAlert({
  ticker,
  price,
  percent_change,
  rvol,
  float,
  news,
  alert_type = "SCAN",
  meta = null,
}) {
  // If your table doesn't have alert_type/meta columns, either:
  // 1) add them, or 2) remove these fields from the query.
  const result = await pool.query(
    `
    INSERT INTO alerts (
      ticker,
      price,
      percent_change,
      rvol,
      float,
      news,
      alert_type,
      meta,
      created_at
    )
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8, now())
    RETURNING *;
    `,
    [ticker, price, percent_change, rvol, float, news, alert_type, meta]
  );
  return result.rows[0];
}

async function wasAlertedRecently(ticker) {
  const minutes = ALERT_COOLDOWN_MIN;
  const result = await pool.query(
    `
    SELECT 1
    FROM alerts
    WHERE ticker = $1
      AND created_at >= now() - ($2 || ' minutes')::interval
    LIMIT 1;
    `,
    [ticker, String(minutes)]
  );
  return result.rowCount > 0;
}

// =========================
// BASE44 PUSH (optional)
// =========================
async function pushToBase44(alert) {
  if (!BASE44_INGEST_URL || !BASE44_API_KEY) return;

  try {
    const resp = await fetch(BASE44_INGEST_URL, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        api_key: BASE44_API_KEY,
      },
      body: JSON.stringify(alert),
    });

    if (!resp.ok) {
      const text = await resp.text().catch(() => "");
      console.error("Base44 ingest failed:", resp.status, text.slice(0, 200));
    }
  } catch (e) {
    console.error("Base44 ingest error:", e.message);
  }
}

// =========================
// TELEGRAM
// =========================
async function sendTelegram(text) {
  if (!TELEGRAM_ENABLED) return;
  const url = `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`;

  try {
    const resp = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        chat_id: TELEGRAM_CHAT_ID,
        text,
        disable_web_page_preview: true,
      }),
    });

    if (!resp.ok) {
      const t = await resp.text().catch(() => "");
      console.error("Telegram send failed:", resp.status, t.slice(0, 200));
    }
  } catch (e) {
    console.error("Telegram send error:", e.message);
  }
}

function formatAlertText(a) {
  const type = a.alert_type || "SCAN";
  const px = Number(a.price || 0).toFixed(2);
  const chg = Number(a.percent_change || 0).toFixed(2);
  const rvol = a.rvol != null ? Number(a.rvol).toFixed(2) : "n/a";
  const flt = a.float ? Math.round(Number(a.float)) : 0;
  const news = a.news ? "Yes" : "No";

  return `Quantum Scan (${type})
${a.ticker}  $${px}  ${chg}%
RVOL: ${rvol}x   Float: ${flt}
News: ${news}`;
}

async function sendTelegramAlert(alertRow) {
  await sendTelegram(formatAlertText(alertRow));
}

// =========================
// POLYGON HELPERS + CACHES
// =========================
const cache = {
  avgVol: new Map(),
  float: new Map(),
  news: new Map(),
};

function getCache(map, key) {
  const entry = map.get(key);
  if (!entry) return null;
  if (Date.now() > entry.expiresAt) {
    map.delete(key);
    return null;
  }
  return entry.value;
}

function setCache(map, key, value, ttlMs) {
  map.set(key, { value, expiresAt: Date.now() + ttlMs });
}

async function polygonJson(url) {
  const resp = await fetch(url);
  if (!resp.ok) {
    const text = await resp.text().catch(() => "");
    throw new Error(`Polygon ${resp.status}: ${text.slice(0, 200)}`);
  }
  return resp.json();
}

async function fetchAllSnapshotTickers() {
  let all = [];
  let url =
    `https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers?apiKey=${POLYGON_KEY}`;

  const MAX_PAGES = 20;

  for (let page = 0; page < MAX_PAGES; page++) {
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

async function getAvgDailyVolume(ticker) {
  const cached = getCache(cache.avgVol, ticker);
  if (cached != null) return cached;

  const lookbackCalendarDays = Math.max(AVG_VOL_DAYS * 2, 40);
  const to = new Date();
  const from = new Date(Date.now() - lookbackCalendarDays * 24 * 60 * 60 * 1000);

  const toStr = to.toISOString().slice(0, 10);
  const fromStr = from.toISOString().slice(0, 10);

  const url =
    `https://api.polygon.io/v2/aggs/ticker/${encodeURIComponent(ticker)}` +
    `/range/1/day/${fromStr}/${toStr}?adjusted=true&sort=desc&limit=50000&apiKey=${POLYGON_KEY}`;

  const data = await polygonJson(url);

  const results = Array.isArray(data?.results) ? data.results : [];
  const vols = results
    .map((r) => Number(r?.v || 0))
    .filter((v) => Number.isFinite(v) && v > 0)
    .slice(0, AVG_VOL_DAYS);

  if (vols.length === 0) {
    setCache(cache.avgVol, ticker, 0, 6 * 60 * 60 * 1000);
    return 0;
  }

  const avg = Math.round(vols.reduce((a, b) => a + b, 0) / vols.length);
  setCache(cache.avgVol, ticker, avg, 6 * 60 * 60 * 1000);
  return avg;
}

async function getFloatOrSharesOutstanding(ticker) {
  const cached = getCache(cache.float, ticker);
  if (cached != null) return cached;

  const url =
    `https://api.polygon.io/v3/reference/tickers/${encodeURIComponent(ticker)}` +
    `?apiKey=${POLYGON_KEY}`;

  const data = await polygonJson(url);
  const res = data?.results || {};

  const floatLike =
    Number(res?.float) ||
    Number(res?.share_class_shares_outstanding) ||
    Number(res?.weighted_shares_outstanding) ||
    0;

  setCache(cache.float, ticker, floatLike, 24 * 60 * 60 * 1000);
  return floatLike;
}

async function hasRecentNews(ticker) {
  const cached = getCache(cache.news, ticker);
  if (cached != null) return cached;

  const now = Date.now();
  const lookbackMs = NEWS_LOOKBACK_MIN * 60 * 1000;
  const sinceIso = new Date(now - lookbackMs).toISOString();

  const url =
    `https://api.polygon.io/v2/reference/news?ticker=${encodeURIComponent(ticker)}` +
    `&published_utc.gte=${encodeURIComponent(sinceIso)}` +
    `&limit=5&apiKey=${POLYGON_KEY}`;

  let ok = false;
  try {
    const data = await polygonJson(url);
    const results = Array.isArray(data?.results) ? data.results : [];
    ok = results.length > 0;
  } catch (e) {
    ok = false;
  }

  setCache(cache.news, ticker, ok, 5 * 60 * 1000);
  return ok;
}

// Minute bars for runner spike detection
async function getMinuteBars(ticker, minutesBack) {
  const to = new Date();
  const from = new Date(Date.now() - minutesBack * 60 * 1000);

  const toStr = to.toISOString();
  const fromStr = from.toISOString();

  const url =
    `https://api.polygon.io/v2/aggs/ticker/${encodeURIComponent(ticker)}` +
    `/range/1/minute/${encodeURIComponent(fromStr)}/${encodeURIComponent(toStr)}` +
    `?adjusted=true&sort=asc&limit=50000&apiKey=${POLYGON_KEY}`;

  const data = await polygonJson(url);
  return Array.isArray(data?.results) ? data.results : [];
}

// =========================
// COOLdown + concurrency
// =========================
const cooldownMap = new Map();

function inCooldown(ticker) {
  const exp = cooldownMap.get(ticker);
  if (!exp) return false;
  if (Date.now() > exp) {
    cooldownMap.delete(ticker);
    return false;
  }
  return true;
}

function setCooldown(ticker) {
  cooldownMap.set(ticker, Date.now() + ALERT_COOLDOWN_MIN * 60 * 1000);
}

async function runWithConcurrency(items, limit, workerFn) {
  const results = [];
  let idx = 0;

  async function runner() {
    while (idx < items.length) {
      const currentIndex = idx++;
      results[currentIndex] = await workerFn(items[currentIndex], currentIndex);
    }
  }

  const runners = Array.from({ length: Math.max(1, limit) }, () => runner());
  await Promise.all(runners);
  return results;
}

// =========================
// Time helpers (premarket)
// =========================
function isPremarketET(now = new Date()) {
  // Premarket: 4:00 AM ET to 9:29 AM ET
  const fmt = new Intl.DateTimeFormat("en-US", {
    timeZone: "America/New_York",
    hour12: false,
    hour: "2-digit",
    minute: "2-digit",
  });
  const parts = fmt.formatToParts(now);
  const hh = Number(parts.find(p => p.type === "hour")?.value || 0);
  const mm = Number(parts.find(p => p.type === "minute")?.value || 0);
  const mins = hh * 60 + mm;
  return mins >= 4 * 60 && mins < 9 * 60 + 30;
}

// =========================
// SCAN LOOP
// =========================
async function scan() {
  lastLoopAt = new Date().toISOString();
  if (isScanning) return;

  isScanning = true;
  scanRuns += 1;

  const started = Date.now();
  lastError = null;
  lastScanStartedAt = new Date().toISOString();

  tickersFetched = 0;
  candidatesFound = 0;
  deepChecked = 0;
  alertsCreated = 0;
  premarketCandidates = 0;
  runnerCandidates = 0;
  runnerChecked = 0;

  try {
    console.log("Scanning market...");

    const tickers = await fetchAllSnapshotTickers();
    tickersFetched = tickers.length;

    const mapped = tickers
      .map((t) => {
        const symbol = t?.ticker;
        const price = Number(t?.lastTrade?.p || 0);
        const percentChange = Number(t?.todaysChangePerc || 0);
        const dayVol = Number(t?.day?.v || 0);
        const prevClose = Number(t?.prevDay?.c || 0);
        return { symbol, price, percentChange, dayVol, prevClose };
      })
      .filter((t) => t.symbol && t.price > 0);

    // --- Premarket gappers list (fast prefilter) ---
    const premarketNow = PREMARKET_ENABLED && isPremarketET(new Date());
    let premarketList = [];
    if (premarketNow) {
      premarketList = mapped
        .filter((t) => t.price >= PRICE_MIN && t.price <= PRICE_MAX)
        .filter((t) => t.percentChange >= PREMARKET_MIN_GAP)
        .filter((t) => t.dayVol >= PREMARKET_MIN_VOL)
        .sort((a, b) => b.percentChange - a.percentChange);

      premarketCandidates = premarketList.length;
      if (MAX_CANDIDATES > 0) premarketList = premarketList.slice(0, MAX_CANDIDATES);
    }

    // --- Normal scan candidates (fast prefilter) ---
    let candidates = mapped
      .filter((t) => t.price >= PRICE_MIN && t.price <= PRICE_MAX)
      .filter((t) => t.percentChange >= MIN_PERCENT_CHANGE)
      .sort((a, b) => b.dayVol - a.dayVol);

    candidatesFound = candidates.length;
    if (MAX_CANDIDATES > 0) candidates = candidates.slice(0, MAX_CANDIDATES);

    // --- Runner candidates (fast prefilter) ---
    let runnerList = [];
    if (RUNNER_ENABLED) {
      runnerList = mapped
        .filter((t) => t.price >= PRICE_MIN && t.price <= PRICE_MAX)
        .filter((t) => t.percentChange >= EARLY_MIN_PERCENT_CHANGE)
        .filter((t) => t.dayVol >= RUNNER_MIN_VOL)
        .sort((a, b) => b.dayVol - a.dayVol);

      runnerCandidates = runnerList.length;
      // keep runner checks small to reduce API cost
      runnerList = runnerList.slice(0, Math.min(80, MAX_CANDIDATES > 0 ? MAX_CANDIDATES : 80));
    }

    console.log(
      `Tickers: ${tickersFetched} | Scan candidates: ${candidates.length} | Premarket: ${premarketList.length} | Runners: ${runnerList.length}`
    );

    // Merge lists with a tag (so we know what triggered it)
    const work = [
      ...premarketList.map(x => ({ ...x, mode: "PREMARKET" })),
      ...candidates.map(x => ({ ...x, mode: "SCAN" })),
      ...runnerList.map(x => ({ ...x, mode: "RUNNER" })),
    ];

    // Deduplicate by symbol, prioritize PREMARKET > SCAN > RUNNER
    const rank = { PREMARKET: 3, SCAN: 2, RUNNER: 1 };
    const bySymbol = new Map();
    for (const item of work) {
      const prev = bySymbol.get(item.symbol);
      if (!prev || rank[item.mode] > rank[prev.mode]) bySymbol.set(item.symbol, item);
    }
    const jobs = Array.from(bySymbol.values());

    await runWithConcurrency(jobs, CONCURRENCY, async (c) => {
      const ticker = c.symbol;
      try {
        if (inCooldown(ticker)) return;

        const recently = await wasAlertedRecently(ticker);
        if (recently) {
          setCooldown(ticker);
          return;
        }

        deepChecked += 1;

        // Float check first (fast-ish; cached)
        const floatVal = await getFloatOrSharesOutstanding(ticker);
        if (!floatVal || floatVal <= 0) return;
        if (floatVal > MAX_FLOAT) return;

        // News check (cached)
        const newsOk = await hasRecentNews(ticker);

        // RVOL check (normal scan & premarket should use it)
        const avgVol = await getAvgDailyVolume(ticker);
        if (!avgVol || avgVol <= 0) return;

        const rvol = c.dayVol / avgVol;

        // ---- MODE RULES ----
        if (c.mode === "SCAN") {
          if (rvol < MIN_RVOL) return;
          // require news for SCAN? keep it required by default behavior (you can change later)
          if (!newsOk) return;

          const payload = {
            ticker,
            price: c.price,
            percent_change: c.percentChange,
            rvol: Number(rvol.toFixed(2)),
            float: Math.round(floatVal),
            news: true,
            alert_type: "SCAN",
            meta: JSON.stringify({ mode: "SCAN" }),
          };

          const row = await insertAlert(payload);
          await pushToBase44(row);
          await sendTelegramAlert(row);

          alertsCreated += 1;
          setCooldown(ticker);
          console.log(`ALERT(SCAN): ${ticker} $${c.price} chg=${c.percentChange}% rvol=${rvol.toFixed(2)} float=${Math.round(floatVal)} news=true`);
          return;
        }

        if (c.mode === "PREMARKET") {
          // premarket gappers: allow no-news if configured
          if (rvol < MIN_RVOL) return;
          if (PREMARKET_REQUIRE_NEWS && !newsOk) return;

          const payload = {
            ticker,
            price: c.price,
            percent_change: c.percentChange,
            rvol: Number(rvol.toFixed(2)),
            float: Math.round(floatVal),
            news: Boolean(newsOk),
            alert_type: "GAPPER",
            meta: JSON.stringify({ mode: "PREMARKET" }),
          };

          const row = await insertAlert(payload);
          await pushToBase44(row);
          await sendTelegramAlert(row);

          alertsCreated += 1;
          setCooldown(ticker);
          console.log(`ALERT(GAPPER): ${ticker} $${c.price} chg=${c.percentChange}% rvol=${rvol.toFixed(2)} float=${Math.round(floatVal)} news=${newsOk}`);
          return;
        }

        if (c.mode === "RUNNER") {
          // runners: volume spike ignition (does NOT require news)
          runnerChecked += 1;

          // quick minute-bars spike check
          const bars = await getMinuteBars(ticker, Math.max(VOLUME_BASELINE_MIN, VOLUME_LOOKBACK_MIN) + 2);
          if (!bars || bars.length < (VOLUME_LOOKBACK_MIN + 2)) return;

          const vols = bars.map(b => Number(b?.v || 0)).filter(v => Number.isFinite(v));
          if (vols.length < (VOLUME_LOOKBACK_MIN + 2)) return;

          // baseline = average of older part of window
          const baselineWindow = vols.slice(0, Math.max(1, vols.length - VOLUME_LOOKBACK_MIN));
          const baselineAvg = baselineWindow.reduce((a,b)=>a+b,0) / Math.max(1, baselineWindow.length);

          const recentWindow = vols.slice(-VOLUME_LOOKBACK_MIN);
          const recentAvg = recentWindow.reduce((a,b)=>a+b,0) / Math.max(1, recentWindow.length);

          if (baselineAvg <= 0) return;
          const spike = recentAvg / baselineAvg;

          if (spike < VOLUME_SPIKE_MULTIPLIER) return;

          const payload = {
            ticker,
            price: c.price,
            percent_change: c.percentChange,
            rvol: Number(rvol.toFixed(2)),
            float: Math.round(floatVal),
            news: Boolean(newsOk),
            alert_type: "RUNNER",
            meta: JSON.stringify({ mode: "RUNNER", spike: Number(spike.toFixed(2)) }),
          };

          const row = await insertAlert(payload);
          await pushToBase44(row);
          await sendTelegramAlert(row);

          alertsCreated += 1;
          setCooldown(ticker);
          console.log(`ALERT(RUNNER): ${ticker} $${c.price} chg=${c.percentChange}% spike=${spike.toFixed(2)} rvol=${rvol.toFixed(2)} float=${Math.round(floatVal)}`);
          return;
        }
      } catch (e) {
        console.error(`Ticker check error (${ticker}):`, e.message);
      }
    });
  } catch (err) {
    lastError = err.message;
    console.error("Scan error:", err.message);
  } finally {
    lastScanFinishedAt = new Date().toISOString();
    lastScanDurationMs = Date.now() - started;
    console.log("Scan duration (ms):", lastScanDurationMs);
    isScanning = false;
  }
}

// =========================
// START SERVER + LOOP
// =========================
const PORT = process.env.PORT || 8080;

app.listen(PORT, "0.0.0.0", () => {
  console.log("Server running on port", PORT);
  console.log("Scan interval (ms):", SCAN_INTERVAL_MS);
  console.log("Telegram enabled:", TELEGRAM_ENABLED);
  console.log("Base44 enabled:", Boolean(BASE44_INGEST_URL && BASE44_API_KEY));
});

scan();
setInterval(scan, SCAN_INTERVAL_MS);
