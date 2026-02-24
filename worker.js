"use strict";

const express = require("express");
const { Pool } = require("pg");

// Node 18+ has fetch globally (Railway Node 22 does)
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

if (!POLYGON_KEY) console.error("Missing env var: POLYGON_KEY");
if (!DATABASE_URL) console.error("Missing env var: DATABASE_URL");

// Criteria
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

// Postgres pool (Railway typically needs ssl rejectUnauthorized false)
const pool = new Pool({
  connectionString: DATABASE_URL,
  ssl: { rejectUnauthorized: false },
});

const app = express();
app.use(express.json());

/**
 * =========================
 * ROUTES
 * =========================
 */

app.get("/", (req, res) => {
  res.send("Quantum Scan Worker is running");
});

app.get("/health", async (req, res) => {
  try {
    await pool.query("SELECT 1");
    res.json({ ok: true, db: "connected", lastScanAt });
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
    // Inserts a sample alert so you can confirm DB works.
    const inserted = await insertAlert({
      ticker: "TEST",
      price: 5.25,
      percent_change: 12.5,
      rvol: 5.1,
      float: 2000000,
      news: true,
    });

    res.json({ success: true, alert: inserted });
  } catch (err) {
    console.error("GET /test error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

/**
 * =========================
 * DB HELPERS
 * =========================
 */

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

/**
 * Optional: DB-based dedupe (prevents inserts if we already alerted recently).
 * This keeps your DB from filling with the same ticker every 15s.
 */
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

/**
 * =========================
 * POLYGON HELPERS + CACHES
 * =========================
 *
 * We cache expensive per-ticker data to avoid rate-limit problems:
 * - avg volume (30 day) cached for 6 hours
 * - float/shares cached for 24 hours
 * - news presence cached for 5 minutes
 */

const cache = {
  avgVol: new Map(),   // key: ticker -> { value, expiresAt }
  float: new Map(),    // key: ticker -> { value, expiresAt }
  news: new Map(),     // key: ticker -> { value (boolean), expiresAt }
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
 * Get 30-day average daily volume using Aggs.
 * Endpoint:
 * /v2/aggs/ticker/{ticker}/range/1/day/{from}/{to}
 *
 * We compute average volume over the returned days.
 */
async function getAvgDailyVolume(ticker) {
  const cached = getCache(cache.avgVol, ticker);
  if (cached != null) return cached;

  // Look back enough calendar days to capture ~AVG_VOL_DAYS trading days
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
  // results are daily bars; "v" = volume
  const vols = results
    .map((r) => Number(r?.v || 0))
    .filter((v) => Number.isFinite(v) && v > 0)
    .slice(0, AVG_VOL_DAYS);

  if (vols.length === 0) {
    setCache(cache.avgVol, ticker, 0, 6 * 60 * 60 * 1000);
    return 0;
  }

  const avg = Math.round(vols.reduce((a, b) => a + b, 0) / vols.length);

  // cache 6 hours
  setCache(cache.avgVol, ticker, avg, 6 * 60 * 60 * 1000);
  return avg;
}

/**
 * Float / shares available:
 * Polygon does not always give "float" directly in all plans.
 * We try to get the best available:
 * - share_class_shares_outstanding (often present)
 * - weighted_shares_outstanding
 *
 * IMPORTANT: this is shares outstanding, not true float, unless Polygon provides float.
 * If your plan exposes a float field later, we can swap to that immediately.
 */
async function getFloatOrSharesOutstanding(ticker) {
  const cached = getCache(cache.float, ticker);
  if (cached != null) return cached;

  const url =
    `https://api.polygon.io/v3/reference/tickers/${encodeURIComponent(ticker)}` +
    `?apiKey=${POLYGON_KEY}`;

  const data = await polygonJson(url);
  const res = data?.results || {};

  // Try common fields
  const floatLike =
    Number(res?.float) ||
    Number(res?.share_class_shares_outstanding) ||
    Number(res?.weighted_shares_outstanding) ||
    0;

  // cache 24 hours
  setCache(cache.float, ticker, floatLike, 24 * 60 * 60 * 1000);
  return floatLike;
}

/**
 * News catalyst:
 * Check if Polygon has at least 1 news item for ticker within lookback minutes.
 */
async function hasRecentNews(ticker) {
  const cached = getCache(cache.news, ticker);
  if (cached != null) return cached;

  const now = Date.now();
  const lookbackMs = NEWS_LOOKBACK_MIN * 60 * 1000;
  const sinceIso = new Date(now - lookbackMs).toISOString();

  // Polygon news endpoint (v2 reference news is common)
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
    // If news endpoint not available on your plan, this may 403.
    // In that case, news check will always fail (as it should for your criteria).
    console.error(`News check failed for ${ticker}:`, e.message);
    ok = false;
  }

  // cache 5 minutes
  setCache(cache.news, ticker, ok, 5 * 60 * 1000);
  return ok;
}

/**
 * =========================
 * SCANNER
 * =========================
 */

let isScanning = false;
let lastScanAt = null;

// In-memory cooldown (extra layer)
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

    // PRE-FILTER (cheap) to reduce per-ticker calls:
    // Criteria we can check from snapshot:
    // - Price between 2 and 20
    // - Up 10%+ today
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
      // Optional: reduce noise (keeps rate under control)
      .sort((a, b) => b.dayVol - a.dayVol)
      .slice(0, MAX_CANDIDATES);

    console.log(`Candidates after prefilter: ${candidates.length}`);

    // Now validate the other 3 criteria (RVOL, NEWS, FLOAT) for candidates only.
    await runWithConcurrency(candidates, CONCURRENCY, async (c) => {
      const ticker = c.symbol;

      try {
        // Cooldown / dedupe (fast)
        if (inCooldown(ticker)) return;

        // DB dedupe (prevents spam even across restarts)
        const recently = await wasAlertedRecently(ticker);
        if (recently) {
          setCooldown(ticker);
          return;
        }

        // 1) RVOL = today volume / average daily volume (last 30 trading days)
        const avgVol = await getAvgDailyVolume(ticker);
        if (!avgVol || avgVol <= 0) return;

        const rvol = c.dayVol / avgVol;
        if (rvol < MIN_RVOL) return;

        // 2) News catalyst
        const newsOk = await hasRecentNews(ticker);
        if (!newsOk) return;

        // 3) Float < 5M (best available from Polygon reference)
        const floatVal = await getFloatOrSharesOutstanding(ticker);
        if (!floatVal || floatVal <= 0) return;
        if (floatVal > MAX_FLOAT) return;

        // ✅ PASSES ALL 5:
        // 1) RVOL >= 5
        // 2) Up 10%+
        // 3) News exists
        // 4) Price 2-20
        // 5) Float < 5M

        const inserted = await insertAlert({
          ticker,
          price: c.price,
          percent_change: c.percentChange,
          rvol: Number(rvol.toFixed(2)),
          float: Math.round(floatVal),
          news: true,
        });

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

/**
 * =========================
 * START SERVER + LOOP
 * =========================
 */

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
  });
});

scan();
setInterval(scan, SCAN_INTERVAL_MS);
