"use strict";

const express = require("express");
const { Pool } = require("pg");

if (typeof fetch !== "function") {
  throw new Error("Global fetch not found. Use Node 18+.");
}

/**
 * REQUIRED:
 * - POLYGON_KEY
 * - DATABASE_URL
 *
 * OPTIONAL (Base44):
 * - BASE44_INGEST_URL
 * - BASE44_API_KEY
 *
 * OPTIONAL (Telegram):
 * - TELEGRAM_BOT_TOKEN
 * - TELEGRAM_CHAT_ID
 *
 * Recommended tighter defaults:
 * - SCAN_INTERVAL_MS=15000
 * - PRICE_MIN=1
 * - PRICE_MAX=20
 * - MAX_FLOAT=3000000
 * - AVG_VOL_DAYS=30
 * - ALERT_COOLDOWN_MIN=60
 * - MAX_CANDIDATES=400
 * - CONCURRENCY=4
 * - NEWS_LOOKBACK_MIN=1440
 *
 * Runner:
 * - RUNNER_ENABLED=true
 * - RUNNER_REQUIRE_NEWS=false
 * - MIN_PERCENT_CHANGE=8
 * - MIN_RVOL=5
 * - RUNNER_MIN_VOL=300000
 *
 * Premarket gappers:
 * - PREMARKET_ENABLED=true
 * - PREMARKET_MIN_GAP=8
 * - PREMARKET_MIN_VOL=50000
 * - PREMARKET_REQUIRE_NEWS=false
 * - PREMARKET_REQUIRE_SPIKE=false
 *
 * Volume spike:
 * - VOLUME_SPIKE_MULTIPLIER=2
 * - VOLUME_LOOKBACK_MIN=5
 * - VOLUME_BASELINE_MIN=30
 *
 * Scan window (Pacific Time):
 * - SCAN_START_HOUR_PT=4
 * - SCAN_END_HOUR_PT=12
 */

const POLYGON_KEY = process.env.POLYGON_KEY;
const DATABASE_URL = process.env.DATABASE_URL;

const BASE44_INGEST_URL = process.env.BASE44_INGEST_URL || "";
const BASE44_API_KEY = process.env.BASE44_API_KEY || "";

const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN || "";
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID || "";

if (!POLYGON_KEY) console.error("Missing env var: POLYGON_KEY");
if (!DATABASE_URL) console.error("Missing env var: DATABASE_URL");

// ===== Criteria =====
const SCAN_INTERVAL_MS = Number(process.env.SCAN_INTERVAL_MS || 15000);

const PRICE_MIN = Number(process.env.PRICE_MIN || 1);
const PRICE_MAX = Number(process.env.PRICE_MAX || 20);
const MAX_FLOAT = Number(process.env.MAX_FLOAT || 3000000);

const AVG_VOL_DAYS = Number(process.env.AVG_VOL_DAYS || 30);
const ALERT_COOLDOWN_MIN = Number(process.env.ALERT_COOLDOWN_MIN || 60);

const MAX_CANDIDATES = Number(process.env.MAX_CANDIDATES || 400);
const CONCURRENCY = Number(process.env.CONCURRENCY || 4);

const NEWS_LOOKBACK_MIN = Number(process.env.NEWS_LOOKBACK_MIN || 1440);

// Scan window (Pacific Time)
const SCAN_START_HOUR_PT = Number(process.env.SCAN_START_HOUR_PT || 4);
const SCAN_END_HOUR_PT = Number(process.env.SCAN_END_HOUR_PT || 12);

// Runner
const RUNNER_ENABLED = String(process.env.RUNNER_ENABLED || "true") === "true";
const RUNNER_REQUIRE_NEWS = String(process.env.RUNNER_REQUIRE_NEWS || "false") === "true";
const MIN_PERCENT_CHANGE = Number(process.env.MIN_PERCENT_CHANGE || 8);
const MIN_RVOL = Number(process.env.MIN_RVOL || 5);
const RUNNER_MIN_VOL = Number(process.env.RUNNER_MIN_VOL || 300000);

// Premarket gappers
const PREMARKET_ENABLED = String(process.env.PREMARKET_ENABLED || "true") === "true";
const PREMARKET_MIN_GAP = Number(process.env.PREMARKET_MIN_GAP || 8);
const PREMARKET_MIN_VOL = Number(process.env.PREMARKET_MIN_VOL || 50000);
const PREMARKET_REQUIRE_NEWS = String(process.env.PREMARKET_REQUIRE_NEWS || "false") === "true";
const PREMARKET_REQUIRE_SPIKE = String(process.env.PREMARKET_REQUIRE_SPIKE || "false") === "true";

// Volume spike
const VOLUME_SPIKE_MULTIPLIER = Number(process.env.VOLUME_SPIKE_MULTIPLIER || 2);
const VOLUME_LOOKBACK_MIN = Number(process.env.VOLUME_LOOKBACK_MIN || 5);
const VOLUME_BASELINE_MIN = Number(process.env.VOLUME_BASELINE_MIN || 30);

// ===== Postgres pool =====
const pool = new Pool({
  connectionString: DATABASE_URL,
  ssl: { rejectUnauthorized: false },
});

// ===== Express =====
const app = express();
app.use(express.json());

// ===== Runtime state =====
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

let runnerCandidates = 0;
let gapperCandidates = 0;

// ===== Routes =====
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
      runnerCandidates,
      gapperCandidates,
      deepChecked,
      alertsCreated,
      scanRuns,
      telegramEnabled: Boolean(TELEGRAM_BOT_TOKEN && TELEGRAM_CHAT_ID),
      base44Enabled: Boolean(BASE44_INGEST_URL && BASE44_API_KEY),
      config: {
        PRICE_MIN,
        PRICE_MAX,
        MAX_FLOAT,
        AVG_VOL_DAYS,
        ALERT_COOLDOWN_MIN,
        MAX_CANDIDATES,
        CONCURRENCY,
        NEWS_LOOKBACK_MIN,
        SCAN_START_HOUR_PT,
        SCAN_END_HOUR_PT,

        RUNNER_ENABLED,
        RUNNER_REQUIRE_NEWS,
        MIN_PERCENT_CHANGE,
        MIN_RVOL,
        RUNNER_MIN_VOL,

        PREMARKET_ENABLED,
        PREMARKET_MIN_GAP,
        PREMARKET_MIN_VOL,
        PREMARKET_REQUIRE_NEWS,
        PREMARKET_REQUIRE_SPIKE,

        VOLUME_SPIKE_MULTIPLIER,
        VOLUME_LOOKBACK_MIN,
        VOLUME_BASELINE_MIN,
      },
    });
  } catch (e) {
    res.status(500).json({
      ok: false,
      db: "error",
      error: e.message,
    });
  }
});

app.get("/alerts", async (req, res) => {
  try {
    const result = await pool.query(
      `SELECT * FROM alerts ORDER BY created_at DESC LIMIT 200;`
    );
    res.json(result.rows);
  } catch (err) {
    console.error("GET /alerts error:", err.message);
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
      meta: null,
    });

    await pushToBase44(inserted);
    console.log("Sending Telegram for TEST (TEST)");
    await pushToTelegram(formatTelegram(inserted));

    res.json({ success: true, alert: inserted });
  } catch (err) {
    console.error("GET /test error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

app.get("/telegram_test", async (req, res) => {
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

// ===== DB helpers =====
async function insertAlert({
  ticker,
  price,
  percent_change,
  rvol,
  float,
  news,
  alert_type,
  meta,
}) {
  const result = await pool.query(
    `
    INSERT INTO alerts (
      ticker, price, percent_change, rvol, float, news, created_at, alert_type, meta
    )
    VALUES ($1,$2,$3,$4,$5,$6, now(), $7, $8)
    RETURNING *;
    `,
    [
      ticker,
      price,
      percent_change,
      rvol,
      float,
      news,
      alert_type || null,
      meta || null,
    ]
  );
  return result.rows[0];
}

async function wasAlertedRecently(ticker, alertType) {
  const result = await pool.query(
    `
    SELECT 1
    FROM alerts
    WHERE ticker = $1
      AND COALESCE(alert_type, '') = COALESCE($2, '')
      AND created_at >= now() - ($3 || ' minutes')::interval
    LIMIT 1;
    `,
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
      headers: {
        "Content-Type": "application/json",
        api_key: BASE44_API_KEY,
      },
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
    console.error(msg);
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
        disable_web_page_preview: true,
      }),
    });

    const bodyText = await resp.text().catch(() => "");

    if (!resp.ok) {
      console.error("Telegram send failed:", resp.status, bodyText);
      if (opts.returnDebug) {
        return { sent: false, status: resp.status, body: bodyText };
      }
      return false;
    }

    console.log("Telegram sent successfully");
    if (opts.returnDebug) {
      return { sent: true, status: resp.status, body: bodyText };
    }
    return true;
  } catch (e) {
    console.error("Telegram send error:", e.message);
    if (opts.returnDebug) return { sent: false, error: e.message };
    return false;
  }
}

function formatTelegram(a) {
  const tv = `https://www.tradingview.com/symbols/${encodeURIComponent(a.ticker)}/`;
  const pct = Number(a.percent_change || 0).toFixed(2);
  const rvol = a.rvol != null ? Number(a.rvol).toFixed(2) : "—";
  const fl = a.float ? Number(a.float).toLocaleString() : "—";
  const price = a.price != null ? Number(a.price).toFixed(2) : "—";
  const news = a.news ? "✅" : "❌";
  const type = a.alert_type || "ALERT";

  return `✅ Quantum Scan (${type})
${a.ticker}  $${price}  (${pct}%)
RVOL: ${rvol}   Float: ${fl}
News: ${news}
${tv}`;
}

// ===== Polygon helpers / caches =====
const cache = {
  avgVol: new Map(),
  float: new Map(),
  news: new Map(),
  minuteAggs: new Map(),
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
    throw new Error(`Polygon ${resp.status}: ${text.slice(0, 400)}`);
  }
  return resp.json();
}

async function fetchAllSnapshotTickers() {
  const all = [];
  let url = `https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers?limit=1000&apiKey=${POLYGON_KEY}`;
  const MAX_PAGES = 200;

  for (let page = 0; page < MAX_PAGES && url; page++) {
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

function computePercentChangeFromSnapshot(t) {
  const price =
    Number(t?.lastTrade?.p) ||
    Number(t?.day?.c) ||
    Number(t?.prevDay?.c) ||
    0;

  const prevClose = Number(t?.prevDay?.c || 0);

  if (price > 0 && prevClose > 0) {
    return ((price - prevClose) / prevClose) * 100;
  }

  return Number(t?.todaysChangePerc || 0);
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
    `https://api.polygon.io/v3/reference/tickers/${encodeURIComponent(ticker)}?apiKey=${POLYGON_KEY}`;

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

async function hasRecentNews(ticker, lookbackMin) {
  const key = `${ticker}:${lookbackMin}`;
  const cached = getCache(cache.news, key);
  if (cached != null) return cached;

  const sinceIso = new Date(Date.now() - lookbackMin * 60 * 1000).toISOString();

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

  setCache(cache.news, key, ok, 2 * 60 * 1000);
  return ok;
}

async function getMinuteAggs(ticker, minutesBack) {
  const cacheKey = `${ticker}:${minutesBack}`;
  const cached = getCache(cache.minuteAggs, cacheKey);
  if (cached) return cached;

  const to = new Date();
  const from = new Date(Date.now() - minutesBack * 60 * 1000);

  const toStr = to.toISOString().slice(0, 10);
  const fromStr = from.toISOString().slice(0, 10);

  const url =
    `https://api.polygon.io/v2/aggs/ticker/${encodeURIComponent(ticker)}` +
    `/range/1/minute/${fromStr}/${toStr}?adjusted=true&sort=desc&limit=50000&apiKey=${POLYGON_KEY}`;

  const data = await polygonJson(url);
  const results = Array.isArray(data?.results) ? data.results : [];

  const cutoff = Date.now() - minutesBack * 60 * 1000;

  const filtered = results
    .filter((r) => Number(r?.t || 0) >= cutoff)
    .map((r) => ({
      t: Number(r.t),
      v: Number(r.v || 0),
    }));

  setCache(cache.minuteAggs, cacheKey, filtered, 20 * 1000);
  return filtered;
}

async function computePremarketVolumeAndSpike(ticker) {
  const bars = await getMinuteAggs(
    ticker,
    Math.max(VOLUME_BASELINE_MIN, VOLUME_LOOKBACK_MIN) + 2
  );

  const totalVol = bars.reduce((sum, b) => sum + (b.v || 0), 0);

  const sortedAsc = [...bars].sort((a, b) => a.t - b.t);
  const lastN = sortedAsc.slice(-VOLUME_LOOKBACK_MIN);
  const base = sortedAsc.slice(0, Math.max(0, sortedAsc.length - VOLUME_LOOKBACK_MIN));

  const lastAvg = lastN.length
    ? lastN.reduce((sum, b) => sum + b.v, 0) / lastN.length
    : 0;

  const baseUse = base.slice(-VOLUME_BASELINE_MIN);
  const baseAvg = baseUse.length
    ? baseUse.reduce((sum, b) => sum + b.v, 0) / baseUse.length
    : 0;

  const spike = baseAvg > 0 ? lastAvg / baseAvg : 0;

  return { totalVol, spikeMultiplier: spike };
}

// ===== Cooldown =====
const cooldownMap = new Map();

function inCooldown(key) {
  const exp = cooldownMap.get(key);
  if (!exp) return false;
  if (Date.now() > exp) {
    cooldownMap.delete(key);
    return false;
  }
  return true;
}

function setCooldown(key) {
  cooldownMap.set(key, Date.now() + ALERT_COOLDOWN_MIN * 60 * 1000);
}

// ===== Concurrency =====
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

// ===== Scanner =====
async function scan() {
  // ===== Trading session window (Pacific Time) =====
  const now = new Date();
  const pacific = new Date(
    now.toLocaleString("en-US", { timeZone: "America/Los_Angeles" })
  );

  const hour = pacific.getHours();

  if (hour < SCAN_START_HOUR_PT || hour >= SCAN_END_HOUR_PT) {
    console.log(
      `Outside trading scan window (${SCAN_START_HOUR_PT}:00–${SCAN_END_HOUR_PT}:00 PT). Skipping scan.`
    );
    return;
  }

  lastLoopAt = new Date().toISOString();
  if (isScanning) return;

  isScanning = true;
  scanRuns += 1;

  const started = Date.now();
  lastError = null;
  lastScanStartedAt = new Date().toISOString();

  tickersFetched = 0;
  candidatesFound = 0;
  runnerCandidates = 0;
  gapperCandidates = 0;
  deepChecked = 0;
  alertsCreated = 0;

  try {
    console.log("Scanning market...");

    const tickers = await fetchAllSnapshotTickers();
    tickersFetched = tickers.length;

    let raw = tickers
      .map((t) => {
        const symbol = t?.ticker;

        const price =
          Number(t?.lastTrade?.p) ||
          Number(t?.day?.c) ||
          Number(t?.prevDay?.c) ||
          0;

        const pct = computePercentChangeFromSnapshot(t);
        const dayVol = Number(t?.day?.v || 0);
        const prevClose = Number(t?.prevDay?.c || 0);

        return { symbol, price, pct, dayVol, prevClose };
      })
      .filter((x) => x.symbol);

    raw = raw.filter((x) => x.price > 0);
    raw = raw.filter((x) => x.price >= PRICE_MIN && x.price <= PRICE_MAX);

    candidatesFound = raw.length;

    raw.sort((a, b) => Math.abs(b.pct) - Math.abs(a.pct));

    if (MAX_CANDIDATES > 0) {
      raw = raw.slice(0, MAX_CANDIDATES);
    }

    console.log(
      `Tickers fetched: ${tickersFetched} | Snapshot candidates: ${candidatesFound} | Deep-checking: ${raw.length}`
    );

    await runWithConcurrency(raw, CONCURRENCY, async (c) => {
      const ticker = c.symbol;

      try {
        const floatVal = await getFloatOrSharesOutstanding(ticker);
        if (!floatVal || floatVal <= 0 || floatVal > MAX_FLOAT) return;

        // ===== Premarket gapper path =====
        if (PREMARKET_ENABLED && c.pct >= PREMARKET_MIN_GAP) {
          const key = `${ticker}:GAPPER`;

          if (inCooldown(key)) return;
          if (await wasAlertedRecently(ticker, "GAPPER")) {
            setCooldown(key);
            return;
          }

          const { totalVol, spikeMultiplier } = await computePremarketVolumeAndSpike(ticker);

          if (totalVol < PREMARKET_MIN_VOL) return;

          if (PREMARKET_REQUIRE_SPIKE && spikeMultiplier < VOLUME_SPIKE_MULTIPLIER) return;

          const newsOk = PREMARKET_REQUIRE_NEWS
            ? await hasRecentNews(ticker, NEWS_LOOKBACK_MIN)
            : true;

          if (!newsOk) return;

          deepChecked += 1;

          const alertPayload = {
            ticker,
            price: c.price,
            percent_change: Number(c.pct.toFixed(2)),
            rvol: null,
            float: Math.round(floatVal),
            news: Boolean(newsOk),
            alert_type: "GAPPER",
            meta: JSON.stringify({
              premarket_vol_window: Math.round(totalVol),
              volume_spike: Number(spikeMultiplier.toFixed(2)),
            }),
          };

          const inserted = await insertAlert(alertPayload);
          await pushToBase44(inserted);
          console.log(`Sending Telegram for ${ticker} (GAPPER)`);
          await pushToTelegram(formatTelegram(inserted));

          alertsCreated += 1;
          gapperCandidates += 1;
          setCooldown(key);

          console.log(
            `ALERT(GAPPER): ${ticker} pct=${c.pct.toFixed(2)} volWindow=${Math.round(
              totalVol
            )} spike=${spikeMultiplier.toFixed(2)} float=${Math.round(floatVal)}`
          );

          return;
        }

        // ===== Runner path =====
        if (RUNNER_ENABLED && c.pct >= MIN_PERCENT_CHANGE && c.dayVol >= RUNNER_MIN_VOL) {
          const key = `${ticker}:RUNNER`;

          if (inCooldown(key)) return;
          if (await wasAlertedRecently(ticker, "RUNNER")) {
            setCooldown(key);
            return;
          }

          const avgVol = await getAvgDailyVolume(ticker);
          if (!avgVol || avgVol <= 0) return;

          const rvol = c.dayVol / avgVol;
          if (rvol < MIN_RVOL) return;

          const newsOk = await hasRecentNews(ticker, NEWS_LOOKBACK_MIN);
          if (RUNNER_REQUIRE_NEWS && !newsOk) return;

          deepChecked += 1;

          const alertPayload = {
            ticker,
            price: c.price,
            percent_change: Number(c.pct.toFixed(2)),
            rvol: Number(rvol.toFixed(2)),
            float: Math.round(floatVal),
            news: Boolean(newsOk),
            alert_type: "RUNNER",
            meta: null,
          };

          const inserted = await insertAlert(alertPayload);
          await pushToBase44(inserted);
          console.log(`Sending Telegram for ${ticker} (RUNNER)`);
          await pushToTelegram(formatTelegram(inserted));

          alertsCreated += 1;
          runnerCandidates += 1;
          setCooldown(key);

          console.log(
            `ALERT(RUNNER): ${ticker} pct=${c.pct.toFixed(2)} rvol=${rvol.toFixed(
              2
            )} float=${Math.round(floatVal)}`
          );
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

// ===== Start =====
const PORT = process.env.PORT || 8080;

app.listen(PORT, "0.0.0.0", () => {
  console.log("Server running on port", PORT);
  console.log("Telegram enabled:", Boolean(TELEGRAM_BOT_TOKEN && TELEGRAM_CHAT_ID));
  console.log("Base44 enabled:", Boolean(BASE44_INGEST_URL && BASE44_API_KEY));
});

scan();
setInterval(scan, SCAN_INTERVAL_MS);
