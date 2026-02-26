"use strict";

const express = require("express");
const { Pool } = require("pg");

// Node 18+ has fetch globally (Railway Node 20/22 does)
if (typeof fetch !== "function") {
  throw new Error("Global fetch not found. Use Node 18+ on Railway.");
}

/**
 * =========================
 * REQUIRED ENV VARS (Railway -> Variables)
 * =========================
 * POLYGON_KEY
 * DATABASE_URL
 *
 * =========================
 * OPTIONAL ENV VARS
 * =========================
 * BASE44_INGEST_URL=https://quantum-scan-pro.base44.app/api/functions/ingestAlert
 * BASE44_API_KEY=xxxxxxxxxxxxxxxx
 *
 * SCAN_INTERVAL_MS=15000
 * PRICE_MIN=2
 * PRICE_MAX=20
 * MIN_PERCENT_CHANGE=10
 * MIN_RVOL=5
 * MAX_FLOAT=5000000
 * AVG_VOL_DAYS=30
 * NEWS_LOOKBACK_MIN=1440          (24h)
 * ALERT_COOLDOWN_MIN=30           (dedupe per ticker)
 * MAX_CANDIDATES=60               (cap to avoid rate limit)
 * CONCURRENCY=6                   (how many per-ticker checks at once)
 */

const POLYGON_KEY = process.env.POLYGON_KEY;
const DATABASE_URL = process.env.DATABASE_URL;

const BASE44_INGEST_URL = process.env.BASE44_INGEST_URL || "";
const BASE44_API_KEY = process.env.BASE44_API_KEY || "";

if (!POLYGON_KEY) console.error("Missing env var: POLYGON_KEY");
if (!DATABASE_URL) console.error("Missing env var: DATABASE_URL");

// ===== Criteria (your 5 rules) =====
const SCAN_INTERVAL_MS = Number(process.env.SCAN_INTERVAL_MS || 15000);

const PRICE_MIN = Number(process.env.PRICE_MIN || 2);
const PRICE_MAX = Number(process.env.PRICE_MAX || 20);
const MIN_PERCENT_CHANGE = Number(process.env.MIN_PERCENT_CHANGE || 10);

const MIN_RVOL = Number(process.env.MIN_RVOL || 5);
const MAX_FLOAT = Number(process.env.MAX_FLOAT || 5000000);

const AVG_VOL_DAYS = Number(process.env.AVG_VOL_DAYS || 30);
const NEWS_LOOKBACK_MIN = Number(process.env.NEWS_LOOKBACK_MIN || 1440);

const ALERT_COOLDOWN_MIN = Number(process.env.ALERT_COOLDOWN_MIN || 30);

const MAX_CANDIDATES = Number(process.env.MAX_CANDIDATES || 60);
const CONCURRENCY = Number(process.env.CONCURRENCY || 6);

// ===== Postgres pool (Railway typically needs ssl rejectUnauthorized false) =====
const pool = new Pool({
  connectionString: DATABASE_URL,
  ssl: { rejectUnauthorized: false },
});

// ===== Express =====
const app = express();
app.use(express.json());

// =========================
// RUNTIME STATE
// =========================
let isScanning = false;
let inFlight = false;
let lastScanAt = null;
let lastError = null;

// This is what your UI can show:
let stocksScanned = 0; // number of candidates deeply evaluated (after cheap filter)
let stocksScannedTotal = 0; // total tickers pulled from polygon (whole market snapshot)

// =========================
// ROUTES
// =========================
app.get("/", (req, res) => {
  res.send("Quantum Scan Worker is running");
});

app.get("/health", async (req, res) => {
  try {
    await pool.query("SELECT 1");
    res.json({
      ok: true,
      db: "connected",
      lastScanAt,
      isScanning,
      inFlight,
      stocksScanned,
      stocksScannedTotal,
      scanIntervalMs: SCAN_INTERVAL_MS,
      lastError,
    });
  } catch (e) {
    res.status(500).json({
      ok: false,
      db: "error",
      error: e.message,
      lastScanAt,
      isScanning,
      inFlight,
      stocksScanned,
      stocksScannedTotal,
      scanIntervalMs: SCAN_INTERVAL_MS,
      lastError,
    });
  }
});

app.get("/alerts", async (req, res) => {
  try {
    const result = await pool.query(
      `SELECT * FROM alerts
       ORDER BY created_at DESC
       LIMIT 200;`
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
    });

    await pushToBase44({
      ticker: inserted.ticker,
      price: inserted.price,
      percent_change: inserted.percent_change,
      rvol: inserted.rvol,
      float: inserted.float,
      news: inserted.news,
    });

    res.json({ success: true, alert: inserted });
  } catch (err) {
    console.error("GET /test error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// =========================
// DB HELPERS
// =========================
async function insertAlert({ ticker, price, percent_change, rvol, float, news }) {
  const result = await pool.query(
    `
    INSERT INTO alerts (
      ticker,
      price,
      percent_change,
      rvol,
      float,
      news,
      created_at
    )
    VALUES ($1, $2, $3, $4, $5, $6, now())
    RETURNING *;
    `,
    [ticker, price, percent_change, rvol, float, news]
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
// POLYGON HELPERS + CACHES
// =========================
const cache = {
  avgVol: new Map(), // ticker -> { value, expiresAt }
  float: new Map(), // ticker -> { value, expiresAt }
  news: new Map(), // ticker -> { value, expiresAt }
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

/**
 * Fetch ALL snapshot pages using next_url.
 * This is what makes "scan whole market" actually happen.
 */
async function fetchAllSnapshots() {
  let url = `https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers?apiKey=${POLYGON_KEY}`;
  const all = [];

  while (url) {
    const data = await polygonJson(url);

    if (Array.isArray(data?.tickers)) {
      all.push(...data.tickers);
    }

    if (data?.next_url) {
      // Polygon next_url sometimes doesn't include apiKey
      url = data.next_url.includes("apiKey=")
        ? data.next_url
        : `${data.next_url}&apiKey=${POLYGON_KEY}`;
    } else {
      url = null;
    }
  }

  return all;
}

/**
 * 30-day average daily volume (Aggs)
 */
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
  setCache(cache.avgVol, ticker, avg, 6 * 60 * 60 * 1000); // 6 hours
  return avg;
}

/**
 * Shares/float-ish (best available)
 */
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

  setCache(cache.float, ticker, floatLike, 24 * 60 * 60 * 1000); // 24 hours
  return floatLike;
}

/**
 * News catalyst: any news within lookback minutes
 */
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
    console.error(`News check failed for ${ticker}:`, e.message);
    ok = false;
  }

  setCache(cache.news, ticker, ok, 5 * 60 * 1000); // 5 minutes
  return ok;
}

// =========================
// SCANNER (loop)
// =========================
const cooldownMap = new Map(); // ticker -> expiresAt

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

/**
 * Simple concurrency limiter (no extra libs)
 */
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

async function scan() {
  // Prevent overlap
  if (inFlight) return;
  inFlight = true;

  if (isScanning) {
    inFlight = false;
    return;
  }

  isScanning = true;

  const started = Date.now();
  lastError = null;

  try {
    console.log("Scanning market (ALL pages) ...");

    // ✅ FULL MARKET SNAPSHOT (pagination)
    const tickers = await fetchAllSnapshots();
    stocksScannedTotal = tickers.length;

    // Cheap prefilter:
    // - price $2-$20
    // - up 10%+ today
    const candidates = tickers
      .map((t) => {
        const symbol = t?.ticker;
        const price = Number(t?.lastTrade?.p || 0);
        const percentChange = Number(t?.todaysChangePerc || 0);
        const dayVol = Number(t?.day?.v || 0);
        return { symbol, price, percentChange, dayVol };
      })
      .filter((t) => t.symbol && t.price >= PRICE_MIN && t.price <= PRICE_MAX)
      .filter((t) => t.percentChange >= MIN_PERCENT_CHANGE)
      .sort((a, b) => b.dayVol - a.dayVol)
      .slice(0, MAX_CANDIDATES);

    // This is how many you “deep check” each cycle
    stocksScanned = candidates.length;

    console.log(
      `Total tickers pulled: ${stocksScannedTotal} | Candidates after prefilter: ${candidates.length}`
    );

    await runWithConcurrency(candidates, CONCURRENCY, async (c) => {
      const ticker = c.symbol;

      try {
        if (inCooldown(ticker)) return;

        const recently = await wasAlertedRecently(ticker);
        if (recently) {
          setCooldown(ticker);
          return;
        }

        // 1) RVOL
        const avgVol = await getAvgDailyVolume(ticker);
        if (!avgVol || avgVol <= 0) return;

        const rvol = c.dayVol / avgVol;
        if (rvol < MIN_RVOL) return;

        // 2) News catalyst
        const newsOk = await hasRecentNews(ticker);
        if (!newsOk) return;

        // 3) Float/shares <= 5M
        const floatVal = await getFloatOrSharesOutstanding(ticker);
        if (!floatVal || floatVal <= 0) return;
        if (floatVal > MAX_FLOAT) return;

        // ✅ PASSES ALL 5
        const alertPayload = {
          ticker,
          price: c.price,
          percent_change: c.percentChange,
          rvol: Number(rvol.toFixed(2)),
          float: Math.round(floatVal),
          news: true,
        };

        await insertAlert(alertPayload);
        await pushToBase44(alertPayload);

        setCooldown(ticker);

        console.log(
          `ALERT: ${ticker} price=${c.price.toFixed(2)} change=${c.percentChange.toFixed(
            2
          )}% rvol=${rvol.toFixed(2)} float=${Math.round(floatVal)} news=true`
        );
      } catch (e) {
        console.error(`Ticker check error (${ticker}):`, e.message);
      }
    });

    lastScanAt = new Date().toISOString();
  } catch (err) {
    lastError = err.message;
    console.error("Scan error:", err.message);
  } finally {
    console.log("Scan duration (ms):", Date.now() - started);
    isScanning = false;
    inFlight = false;
  }
}

// =========================
// START SERVER + LOOP
// =========================
const PORT = process.env.PORT || 8080;

app.listen(PORT, "0.0.0.0", () => {
  console.log("Server running on port", PORT);
  console.log("Scan interval (ms):", SCAN_INTERVAL_MS);
  console.log("Criteria:", {
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
    BASE44_ENABLED: Boolean(BASE44_INGEST_URL && BASE44_API_KEY),
  });
});

scan();
setInterval(scan, SCAN_INTERVAL_MS);const POLYGON_KEY = process.env.POLYGON_KEY;
const DATABASE_URL = process.env.DATABASE_URL;

const BASE44_INGEST_URL = process.env.BASE44_INGEST_URL || "";
const BASE44_API_KEY = process.env.BASE44_API_KEY || "";

if (!POLYGON_KEY) console.error("Missing env var: POLYGON_KEY");
if (!DATABASE_URL) console.error("Missing env var: DATABASE_URL");

// ===== Criteria (your 5 rules) =====
// 1) RVOL >= 5x (today volume / avg daily volume over last N days)
// 2) Up >= 10% today
// 3) Has news event (within lookback window)
// 4) Price between $2 and $20
// 5) Supply: shares/float <= 5,000,000

const SCAN_INTERVAL_MS = Number(process.env.SCAN_INTERVAL_MS || 15000);

const PRICE_MIN = Number(process.env.PRICE_MIN || 2);
const PRICE_MAX = Number(process.env.PRICE_MAX || 20);
const MIN_PERCENT_CHANGE = Number(process.env.MIN_PERCENT_CHANGE || 10);

const MIN_RVOL = Number(process.env.MIN_RVOL || 5);
const MAX_FLOAT = Number(process.env.MAX_FLOAT || 5000000);

const AVG_VOL_DAYS = Number(process.env.AVG_VOL_DAYS || 30);
const NEWS_LOOKBACK_MIN = Number(process.env.NEWS_LOOKBACK_MIN || 1440);

const ALERT_COOLDOWN_MIN = Number(process.env.ALERT_COOLDOWN_MIN || 30);

const MAX_CANDIDATES = Number(process.env.MAX_CANDIDATES || 60);
const CONCURRENCY = Number(process.env.CONCURRENCY || 6);

// ===== Postgres pool (Railway typically needs ssl rejectUnauthorized false) =====
const pool = new Pool({
  connectionString: DATABASE_URL,
  ssl: { rejectUnauthorized: false },
});

// ===== Express =====
const app = express();
app.use(express.json());

// =========================
// ROUTES
// =========================
let isScanning = false;
let lastScanAt = null;

app.get("/", (req, res) => {
  res.send("Quantum Scan Worker is running");
});

app.get("/health", async (req, res) => {
  try {
    await pool.query("SELECT 1");
    res.json({
      ok: true,
      db: "connected",
      lastScanAt,
      isScanning,
      scanIntervalMs: SCAN_INTERVAL_MS,
    });
  } catch (e) {
    res.status(500).json({ ok: false, db: "error", error: e.message });
  }
});

app.get("/alerts", async (req, res) => {
  try {
    const result = await pool.query(
      `SELECT * FROM alerts
       ORDER BY created_at DESC
       LIMIT 200;`
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
    });

    // optional: also send to Base44
    await pushToBase44({
      ticker: inserted.ticker,
      price: inserted.price,
      percent_change: inserted.percent_change,
      rvol: inserted.rvol,
      float: inserted.float,
      news: inserted.news,
    });

    res.json({ success: true, alert: inserted });
  } catch (err) {
    console.error("GET /test error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// =========================
// DB HELPERS
// =========================
async function insertAlert({ ticker, price, percent_change, rvol, float, news }) {
  const result = await pool.query(
    `
    INSERT INTO alerts (
      ticker,
      price,
      percent_change,
      rvol,
      float,
      news,
      created_at
    )
    VALUES ($1, $2, $3, $4, $5, $6, now())
    RETURNING *;
    `,
    [ticker, price, percent_change, rvol, float, news]
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
// POLYGON HELPERS + CACHES
// =========================
const cache = {
  avgVol: new Map(), // ticker -> { value, expiresAt }
  float: new Map(),  // ticker -> { value, expiresAt }
  news: new Map(),   // ticker -> { value, expiresAt }
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

/**
 * 30-day average daily volume (Aggs)
 */
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
  setCache(cache.avgVol, ticker, avg, 6 * 60 * 60 * 1000); // 6 hours
  return avg;
}

/**
 * Shares/float-ish (best available)
 * NOTE: Depending on your Polygon plan, true float may not exist.
 */
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

  setCache(cache.float, ticker, floatLike, 24 * 60 * 60 * 1000); // 24 hours
  return floatLike;
}

/**
 * News catalyst: any news within lookback minutes
 */
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
    // If endpoint blocked by plan, news will fail -> ok stays false (as your criteria requires).
    console.error(`News check failed for ${ticker}:`, e.message);
    ok = false;
  }

  setCache(cache.news, ticker, ok, 5 * 60 * 1000); // 5 minutes
  return ok;
}

// =========================
// SCANNER (15s loop)
// =========================
const cooldownMap = new Map(); // ticker -> expiresAt

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

/**
 * Simple concurrency limiter (no extra libs)
 */
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

async function scan() {
  if (isScanning) return;
  isScanning = true;

  const started = Date.now();

  try {
    console.log("Scanning market...");

    const url =
      `https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers?apiKey=${POLYGON_KEY}`;

    const data = await polygonJson(url);
    const tickers = Array.isArray(data?.tickers) ? data.tickers : [];

    // Cheap prefilter:
    // - price $2-$20
    // - up 10%+ today
    const candidates = tickers
      .map((t) => {
        const symbol = t?.ticker;
        const price = Number(t?.lastTrade?.p || 0);
        const percentChange = Number(t?.todaysChangePerc || 0);
        const dayVol = Number(t?.day?.v || 0);
        return { symbol, price, percentChange, dayVol };
      })
      .filter((t) => t.symbol && t.price >= PRICE_MIN && t.price <= PRICE_MAX)
      .filter((t) => t.percentChange >= MIN_PERCENT_CHANGE)
      .sort((a, b) => b.dayVol - a.dayVol)
      .slice(0, MAX_CANDIDATES);

    console.log(`Candidates after prefilter: ${candidates.length}`);

    await runWithConcurrency(candidates, CONCURRENCY, async (c) => {
      const ticker = c.symbol;

      try {
        // local cooldown
        if (inCooldown(ticker)) return;

        // DB cooldown (survives restarts)
        const recently = await wasAlertedRecently(ticker);
        if (recently) {
          setCooldown(ticker);
          return;
        }

        // 1) RVOL = today volume / avg daily volume (30 trading days)
        const avgVol = await getAvgDailyVolume(ticker);
        if (!avgVol || avgVol <= 0) return;

        const rvol = c.dayVol / avgVol;
        if (rvol < MIN_RVOL) return;

        // 2) News catalyst
        const newsOk = await hasRecentNews(ticker);
        if (!newsOk) return;

        // 3) Float/shares <= 5M
        const floatVal = await getFloatOrSharesOutstanding(ticker);
        if (!floatVal || floatVal <= 0) return;
        if (floatVal > MAX_FLOAT) return;

        // ✅ PASSES ALL 5
        const alertPayload = {
          ticker,
          price: c.price,
          percent_change: c.percentChange,
          rvol: Number(rvol.toFixed(2)),
          float: Math.round(floatVal),
          news: true,
        };

        const inserted = await insertAlert(alertPayload);

        // Push to Base44 (optional)
        await pushToBase44(alertPayload);

        setCooldown(ticker);

        console.log(
          `ALERT: ${ticker} price=${c.price.toFixed(2)} change=${c.percentChange.toFixed(
            2
          )}% rvol=${rvol.toFixed(2)} float=${Math.round(floatVal)} news=true`
        );

        return inserted;
      } catch (e) {
        console.error(`Ticker check error (${ticker}):`, e.message);
      }
    });

    lastScanAt = new Date().toISOString();
  } catch (err) {
    console.error("Scan error:", err.message);
  } finally {
    console.log("Scan duration (ms):", Date.now() - started);
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
  console.log("Criteria:", {
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
    BASE44_ENABLED: Boolean(BASE44_INGEST_URL && BASE44_API_KEY),
  });
});

scan();
setInterval(scan, SCAN_INTERVAL_MS);
