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
const TOP20_QUALITY_ENABLED       = process.env.TOP20_QUALITY_ENABLED !== "false";
const TOP20_QUALITY_STRONG        = Number(process.env.TOP20_QUALITY_STRONG || 75);
const TOP20_QUALITY_CAUTION       = Number(process.env.TOP20_QUALITY_CAUTION || 55);

// ===== TOP-20 MOMENTUM SAFETY FILTER =====
// These filters do NOT change the 5 core criteria. They decide whether a
// qualifying 4/5 or 5/5 setup is safe enough to send to Telegram.
const TOP20_SAFETY_FILTER_ENABLED       = process.env.TOP20_SAFETY_FILTER_ENABLED !== "false";
const TOP20_REQUIRE_PRICE_ABOVE_SMA10   = process.env.TOP20_REQUIRE_PRICE_ABOVE_SMA10 !== "false";
const TOP20_REQUIRE_SMA10_RISING        = process.env.TOP20_REQUIRE_SMA10_RISING !== "false";
const TOP20_SMA10_TREND_LOOKBACK        = Number(process.env.TOP20_SMA10_TREND_LOOKBACK || 3);
const TOP20_MAX_RED_VOLUME_RATIO        = Number(process.env.TOP20_MAX_RED_VOLUME_RATIO || 0.65);
const TOP20_RECENT_MOMENTUM_LOOKBACK    = Number(process.env.TOP20_RECENT_MOMENTUM_LOOKBACK || 3);
const TOP20_MIN_RECENT_MOMENTUM_PCT     = Number(process.env.TOP20_MIN_RECENT_MOMENTUM_PCT || 0);
const TOP20_MAX_UPPER_WICK_RATIO        = Number(process.env.TOP20_MAX_UPPER_WICK_RATIO || 0.55);
const TOP20_MAX_DISTANCE_ABOVE_SMA10_PCT = Number(process.env.TOP20_MAX_DISTANCE_ABOVE_SMA10_PCT || 8);
const TOP20_REQUIRE_MACD_STRENGTHENING  = process.env.TOP20_REQUIRE_MACD_STRENGTHENING === "true";

// ===== TOP-20 LEVEL 1 BUY-PRESSURE CONFIRMATION =====
// This is a bonus/confirmation layer. It does NOT change the core 4/5 or 5/5 score.
const TOP20_LEVEL1_ENABLED          = process.env.TOP20_LEVEL1_ENABLED !== "false";
const TOP20_LEVEL1_BULL_RATIO       = Number(process.env.TOP20_LEVEL1_BULL_RATIO || 1.5);
const TOP20_LEVEL1_BEAR_RATIO       = Number(process.env.TOP20_LEVEL1_BEAR_RATIO || 0.75);
const TOP20_LEVEL1_MAX_SPREAD_PCT   = Number(process.env.TOP20_LEVEL1_MAX_SPREAD_PCT || 1.0);
const TOP20_LEVEL1_MAX_QUOTE_AGE_SEC = Number(process.env.TOP20_LEVEL1_MAX_QUOTE_AGE_SEC || 30);

// ===== TOP-20 UPTREND + PREVIOUS-CANDLE VOLUME TRIGGER =====
// The expensive/full technical scan runs only after this gate passes.
// Trigger = confirmed uptrend + newest completed 1m candle volume >= N times
// the immediately previous completed 1m candle.
const TOP20_TRIGGER_ENABLED = process.env.TOP20_TRIGGER_ENABLED !== "false";
const TOP20_TRIGGER_VOLUME_MULTIPLIER = Number(process.env.TOP20_TRIGGER_VOLUME_MULTIPLIER || 5);
const TOP20_TRIGGER_COMPLETION_GRACE_MS = Number(process.env.TOP20_TRIGGER_COMPLETION_GRACE_MS || 2000);
const TOP20_TRIGGER_REQUIRE_GREEN = process.env.TOP20_TRIGGER_REQUIRE_GREEN !== "false";
const TOP20_TRIGGER_REQUIRE_HIGHER_CLOSE = process.env.TOP20_TRIGGER_REQUIRE_HIGHER_CLOSE !== "false";
const TOP20_TRIGGER_REQUIRE_PRICE_ABOVE_SMA10 = process.env.TOP20_TRIGGER_REQUIRE_PRICE_ABOVE_SMA10 !== "false";
const TOP20_TRIGGER_REQUIRE_SMA10_RISING = process.env.TOP20_TRIGGER_REQUIRE_SMA10_RISING !== "false";
const TOP20_TRIGGER_REQUIRE_SMA100_RISING = process.env.TOP20_TRIGGER_REQUIRE_SMA100_RISING !== "false";

// ===== TOP-20 RELIABILITY / LEARNING LAYER =====
// Defaults are conservative; all can be tuned later from Railway.
const TOP20_MAX_BAR_AGE_SEC            = Number(process.env.TOP20_MAX_BAR_AGE_SEC || 150);
const TOP20_MAX_ZERO_VOL_RATIO         = Number(process.env.TOP20_MAX_ZERO_VOL_RATIO || 0.35);
const TOP20_CIRCUIT_BREAKER_FAILURES   = Number(process.env.TOP20_CIRCUIT_BREAKER_FAILURES || 3);
const TOP20_SHADOW_MODE                = process.env.TOP20_SHADOW_MODE !== "false";
const TOP20_OUTCOME_TRACKING           = process.env.TOP20_OUTCOME_TRACKING !== "false";
const TOP20_SELF_TEST_ENABLED          = process.env.TOP20_SELF_TEST_ENABLED !== "false";
const TOP20_SELF_TEST_HOUR_PT          = Number(process.env.TOP20_SELF_TEST_HOUR_PT || 5);
const TOP20_SELF_TEST_MINUTE_PT        = Number(process.env.TOP20_SELF_TEST_MINUTE_PT || 55);

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

let top20FeedFailureCount      = 0;
let top20CircuitOpen           = false;
let top20CircuitNotified       = false;
let top20LastFeedRecoveryAt    = null;
let top20LastSelfTestDate      = null;
let top20LastSelfTest          = null;
let top20ShadowSignals         = 0;
let top20TrackedSignals        = 0;
let top20OutcomeUpdates        = 0;
const top20ShadowState         = new Map();

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
        reliability: {
          feedFailureCount: top20FeedFailureCount,
          circuitOpen: top20CircuitOpen,
          lastFeedRecoveryAt: top20LastFeedRecoveryAt,
          lastSelfTest: top20LastSelfTest,
          shadowSignals: top20ShadowSignals,
          trackedSignals: top20TrackedSignals,
          outcomeUpdates: top20OutcomeUpdates,
        },
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
          TOP20_QUALITY_ENABLED,
          TOP20_QUALITY_STRONG,
          TOP20_QUALITY_CAUTION,
          TOP20_SAFETY_FILTER_ENABLED,
          TOP20_REQUIRE_PRICE_ABOVE_SMA10,
          TOP20_REQUIRE_SMA10_RISING,
          TOP20_SMA10_TREND_LOOKBACK,
          TOP20_MAX_RED_VOLUME_RATIO,
          TOP20_RECENT_MOMENTUM_LOOKBACK,
          TOP20_MIN_RECENT_MOMENTUM_PCT,
          TOP20_MAX_UPPER_WICK_RATIO,
          TOP20_MAX_DISTANCE_ABOVE_SMA10_PCT,
          TOP20_REQUIRE_MACD_STRENGTHENING,
          TOP20_LEVEL1_ENABLED,
          TOP20_LEVEL1_BULL_RATIO,
          TOP20_LEVEL1_BEAR_RATIO,
          TOP20_LEVEL1_MAX_SPREAD_PCT,
          TOP20_LEVEL1_MAX_QUOTE_AGE_SEC,
          TOP20_TRIGGER_ENABLED,
          TOP20_TRIGGER_VOLUME_MULTIPLIER,
          TOP20_TRIGGER_COMPLETION_GRACE_MS,
          TOP20_TRIGGER_REQUIRE_GREEN,
          TOP20_TRIGGER_REQUIRE_HIGHER_CLOSE,
          TOP20_TRIGGER_REQUIRE_PRICE_ABOVE_SMA10,
          TOP20_TRIGGER_REQUIRE_SMA10_RISING,
          TOP20_TRIGGER_REQUIRE_SMA100_RISING,
          TOP20_MAX_BAR_AGE_SEC,
          TOP20_MAX_ZERO_VOL_RATIO,
          TOP20_CIRCUIT_BREAKER_FAILURES,
          TOP20_SHADOW_MODE,
          TOP20_OUTCOME_TRACKING,
          TOP20_SELF_TEST_ENABLED,
          TOP20_SELF_TEST_HOUR_PT,
          TOP20_SELF_TEST_MINUTE_PT,
        },
      },
    });
  } catch (e) {
    res.status(500).json({ ok: false, db: "error", error: e.message });
  }
});

function getTop20ScannerStatus() {
  if (!TOP20_ENABLED) return "OFF";
  if (top20CircuitOpen) return "FEED_PAUSED";

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
    reliability: {
      feedFailureCount: top20FeedFailureCount,
      circuitOpen: top20CircuitOpen,
      lastFeedRecoveryAt: top20LastFeedRecoveryAt,
      lastSelfTest: top20LastSelfTest,
      shadowSignals: top20ShadowSignals,
      trackedSignals: top20TrackedSignals,
      outcomeUpdates: top20OutcomeUpdates,
    },
    count: top20LastResults.length,
    stocks: top20LastResults,
  });
});


app.get("/top20_signals", async (_req, res) => {
  try {
    await ensureTop20TrackingTable();
    const result = await pool.query(`
      SELECT id, ticker, signal_kind, score, quality_score, price, percent_change,
             gainer_rank, detected_at, block_reasons,
             outcome_1m_pct, outcome_3m_pct, outcome_5m_pct, outcome_10m_pct
      FROM top20_signal_log
      ORDER BY detected_at DESC
      LIMIT 200
    `);
    res.json({ ok: true, signals: result.rows });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
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


// ===== TOP-20 SIGNAL LEARNING / OUTCOME DB =====
let top20TrackingInitPromise = null;

async function ensureTop20TrackingTable() {
  if (top20TrackingInitPromise) return top20TrackingInitPromise;

  top20TrackingInitPromise = (async () => {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS top20_signal_log (
        id BIGSERIAL PRIMARY KEY,
        ticker TEXT NOT NULL,
        signal_kind TEXT NOT NULL,
        score INTEGER NOT NULL,
        quality_score INTEGER,
        price DOUBLE PRECISION NOT NULL,
        percent_change DOUBLE PRECISION,
        gainer_rank INTEGER,
        detected_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        block_reasons JSONB NOT NULL DEFAULT '[]'::jsonb,
        snapshot JSONB,
        outcome_1m_pct DOUBLE PRECISION,
        outcome_3m_pct DOUBLE PRECISION,
        outcome_5m_pct DOUBLE PRECISION,
        outcome_10m_pct DOUBLE PRECISION,
        outcome_1m_at TIMESTAMPTZ,
        outcome_3m_at TIMESTAMPTZ,
        outcome_5m_at TIMESTAMPTZ,
        outcome_10m_at TIMESTAMPTZ
      )
    `);
    await pool.query(`
      CREATE INDEX IF NOT EXISTS idx_top20_signal_log_pending
      ON top20_signal_log (detected_at DESC)
    `);
  })().catch(err => {
    top20TrackingInitPromise = null;
    throw err;
  });

  return top20TrackingInitPromise;
}

async function recordTop20Signal(result, kind, reasons = []) {
  if (!TOP20_OUTCOME_TRACKING) return null;
  try {
    await ensureTop20TrackingTable();
    const snapshot = serializeTop20Result(result);
    const q = Number(result.momentumQuality?.score || 0);
    const inserted = await pool.query(
      `INSERT INTO top20_signal_log
       (ticker, signal_kind, score, quality_score, price, percent_change, gainer_rank, detected_at, block_reasons, snapshot)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb,$10::jsonb)
       RETURNING id`,
      [
        result.ticker,
        kind,
        Number(result.score || 0),
        q,
        Number(result.price || 0),
        Number(result.pct || 0),
        Number(result.rank || 0),
        new Date(result.detectedAtMs || Date.now()).toISOString(),
        JSON.stringify(reasons || []),
        JSON.stringify(snapshot),
      ]
    );
    top20TrackedSignals++;
    return inserted.rows[0]?.id || null;
  } catch (e) {
    console.error('[TOP20][TRACKING] record error:', e.message);
    return null;
  }
}

async function fetchMassiveTickerPrice(ticker) {
  const data = await massiveJson(
    `/v2/snapshot/locale/us/markets/stocks/tickers/${encodeURIComponent(ticker)}`
  );
  const t = data?.ticker || {};
  return Number(t?.lastTrade?.p) || Number(t?.day?.c) || Number(t?.min?.c) || 0;
}

async function updateTop20Outcomes() {
  if (!TOP20_OUTCOME_TRACKING || top20CircuitOpen) return;
  try {
    await ensureTop20TrackingTable();
    const pending = await pool.query(`
      SELECT id, ticker, price, detected_at,
             outcome_1m_pct, outcome_3m_pct, outcome_5m_pct, outcome_10m_pct
      FROM top20_signal_log
      WHERE detected_at >= now() - interval '20 minutes'
        AND (
          outcome_1m_pct IS NULL OR outcome_3m_pct IS NULL OR
          outcome_5m_pct IS NULL OR outcome_10m_pct IS NULL
        )
      ORDER BY detected_at ASC
      LIMIT 50
    `);

    const now = Date.now();
    const due = [];
    for (const row of pending.rows) {
      const ageMin = (now - new Date(row.detected_at).getTime()) / 60000;
      const horizons = [
        [1, 'outcome_1m_pct', 'outcome_1m_at'],
        [3, 'outcome_3m_pct', 'outcome_3m_at'],
        [5, 'outcome_5m_pct', 'outcome_5m_at'],
        [10, 'outcome_10m_pct', 'outcome_10m_at'],
      ];
      for (const [mins, pctField, atField] of horizons) {
        if (ageMin >= mins && row[pctField] == null) {
          due.push({ row, mins, pctField, atField });
          break; // one snapshot can satisfy all currently-due horizons for this signal
        }
      }
    }

    await runWithConcurrency(due, Math.min(4, TOP20_CONCURRENCY), async item => {
      const { row } = item;
      const currentPrice = await fetchMassiveTickerPrice(row.ticker);
      if (!(currentPrice > 0) || !(Number(row.price) > 0)) return;
      const ageMin = (Date.now() - new Date(row.detected_at).getTime()) / 60000;
      const pct = ((currentPrice - Number(row.price)) / Number(row.price)) * 100;
      const sets = [];
      const vals = [];
      let p = 1;
      for (const mins of [1, 3, 5, 10]) {
        const pctField = `outcome_${mins}m_pct`;
        const atField = `outcome_${mins}m_at`;
        if (ageMin >= mins && row[pctField] == null) {
          sets.push(`${pctField} = $${p++}`, `${atField} = now()`);
          vals.push(Number(pct.toFixed(4)));
        }
      }
      if (!sets.length) return;
      vals.push(row.id);
      await pool.query(
        `UPDATE top20_signal_log SET ${sets.join(', ')} WHERE id = $${p}`,
        vals
      );
      top20OutcomeUpdates += sets.length / 2;
    });
  } catch (e) {
    console.error('[TOP20][OUTCOME] error:', e.message);
  }
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

function formatTop20Telegram(result, timing = {}) {
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

  const trigger = result.trigger || {};
  const triggerLine = trigger.passed
    ? (
        `🚨 <b>UPTREND + 1M VOLUME JUMP TRIGGER</b> — ` +
        `${Number(trigger.multiplier || 0).toFixed(2)}x previous candle\n` +
        `Current 1m: ${Number(trigger.currentCandleVolume || 0).toLocaleString()} ` +
        `vs previous 1m: ${Number(trigger.previousCandleVolume || 0).toLocaleString()}\n`
      )
    : "";

  const q = result.momentumQuality || {};
  const qualityIcon =
    q.label === "STRONG" ? "🟢" :
    q.label === "GOOD" ? "🔵" :
    q.label === "CAUTION" ? "🟡" : "🔴";

  const qualityLine =
    `${qualityIcon} Momentum Quality: <b>${Math.round(q.score || 0)}/100 ${q.label || "UNAVAILABLE"}</b>\n`;

  const riskLine = Array.isArray(q.riskFlags) && q.riskFlags.length
    ? `⚠️ Risk: ${q.riskFlags.join(", ").replaceAll("_", " ")}\n`
    : `✅ Risk checks: Clean\n`;
  const safety = result.safety || {};
  const safetyLine = safety.passed
    ? `🛡️ Safety Filter: <b>PASS</b>\n`
    : `🛑 Safety Filter: <b>BLOCKED</b> — ${(safety.reasons || []).join(", ").replaceAll("_", " ")}\n`;

  const l1 = result.level1 || {};
  const l1Icon =
    l1.pressure === "BUY_PRESSURE" ? "🟢" :
    l1.pressure === "SELL_PRESSURE" ? "🔴" :
    l1.pressure === "WIDE_SPREAD" ? "🟡" :
    l1.pressure === "STALE" ? "⚪" : "🟡";

  const l1Label =
    l1.pressure === "BUY_PRESSURE" ? "BUY PRESSURE CONFIRMED" :
    l1.pressure === "SELL_PRESSURE" ? "SELL PRESSURE" :
    l1.pressure === "WIDE_SPREAD" ? "WIDE SPREAD / NEUTRAL" :
    l1.pressure === "STALE" ? "STALE QUOTE" :
    l1.pressure === "ERROR" ? "QUOTE ERROR" :
    l1.pressure === "DISABLED" ? "DISABLED" :
    "NEUTRAL";
  const level1Lines =
    TOP20_LEVEL1_ENABLED && l1.available
      ? (
          `📊 <b>LEVEL 1</b> — ${l1Icon} <b>${l1Label}</b>\n` +
          `Bid: $${Number(l1.bid || 0).toFixed(2)} x ${Number(l1.bidSize || 0).toLocaleString()}   ` +
          `Ask: $${Number(l1.ask || 0).toFixed(2)} x ${Number(l1.askSize || 0).toLocaleString()}\n` +
          `Bid/Ask Ratio: ${l1.ratio != null ? Number(l1.ratio).toFixed(2) : "—"}x   ` +
          `Spread: ${l1.spreadPct != null ? Number(l1.spreadPct).toFixed(2) : "—"}%   ` +
          `Quote age: ${l1.quoteAgeSec != null ? Number(l1.quoteAgeSec).toFixed(1) : "—"}s\n`
        )
      : (
          TOP20_LEVEL1_ENABLED
            ? `📊 <b>LEVEL 1</b> — ⚪ Quote unavailable\n`
            : `📊 <b>LEVEL 1</b> — Disabled\n`
        );

  const detectedAtMs = Number(result.detectedAtMs || timing.detectedAtMs || Date.now());
  const scanStartedAtMs = Number(timing.scanStartedAtMs || detectedAtMs);
  const signalLatencyMs = Math.max(0, detectedAtMs - scanStartedAtMs);
  const dataBarMs = Number(result.lastCompletedBarAtMs || 0);
  const timingLines =
    `⏱ Detected: ${formatPacificTime(detectedAtMs)} PT\n` +
    `⚙️ Scan-to-signal: ${signalLatencyMs} ms\n` +
    (dataBarMs ? `🕯 Latest completed 1m bar: ${formatPacificTime(dataBarMs)} PT\n` : "");

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
    `${triggerLine}${bonusLine}` +
    `${qualityLine}${riskLine}${safetyLine}` +
    `${level1Lines}\n` +
    `SMA10: ${Number(sma10).toFixed(3)}   SMA100: ${Number(sma100).toFixed(3)}\n` +
    (score >= 5
      ? (
          bonus?.freshSmaCross
            ? `⭐ <b>PERFECT SETUP — 5/5 + CROSSOVER BONUS</b>\n`
            : `⭐ <b>PERFECT SETUP — 5/5</b>\n`
        )
      : (bonus?.freshSmaCross
          ? `Setup Score: <b>${score}/5 + ⚡ crossover bonus</b>\n`
          : `Setup Score: <b>${score}/5</b>\n`)) +
    `${timingLines}` +
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

function formatPacificTime(msOrDate = Date.now()) {
  const d = msOrDate instanceof Date ? msOrDate : new Date(msOrDate);
  return new Intl.DateTimeFormat("en-US", {
    timeZone: "America/Los_Angeles",
    hour: "numeric",
    minute: "2-digit",
    second: "2-digit",
    hour12: true,
  }).format(d);
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
async function getTop20Level1Quote(ticker) {
  if (!TOP20_LEVEL1_ENABLED) {
    return {
      enabled: false,
      available: false,
      pressure: "DISABLED",
      bullish: false,
      bearish: false,
      neutral: true,
    };
  }

  try {
    const data = await massiveJson(`/v2/last/nbbo/${encodeURIComponent(ticker)}`);
    const q = data?.results || {};

    const bid = Number(q.p || 0);
    const ask = Number(q.P || 0);
    const bidSize = Number(q.s || 0);
    const askSize = Number(q.S || 0);
    // Massive's stock NBBO timestamp is nanoseconds.
    const quoteTimestampNs = Number(q.t || q.y || q.f || 0);
    const quoteTimestampMs =
      quoteTimestampNs > 1e15
        ? Math.floor(quoteTimestampNs / 1e6)
        : quoteTimestampNs > 1e12
          ? Math.floor(quoteTimestampNs / 1e3)
          : quoteTimestampNs > 0
            ? quoteTimestampNs
            : 0;

    const quoteAgeSec =
      quoteTimestampMs > 0
        ? Math.max(0, (Date.now() - quoteTimestampMs) / 1000)
        : null;

    const valid =
      bid > 0 &&
      ask > 0 &&
      ask >= bid &&
      bidSize >= 0 &&
      askSize >= 0;

    if (!valid) {
      return {
        enabled: true,
        available: false,
        pressure: "UNAVAILABLE",
        bullish: false,
        bearish: false,
        neutral: true,
        bid,
        ask,
        bidSize,
        askSize,
        ratio: null,
        spreadPct: null,
        quoteAgeSec,
      };
    }

    const mid = (bid + ask) / 2;
    const spreadPct = mid > 0 ? ((ask - bid) / mid) * 100 : 0;
    const ratio =
      askSize > 0
        ? bidSize / askSize
        : bidSize > 0
          ? Infinity
          : 1;

    const stale =
      quoteAgeSec != null &&
      quoteAgeSec > TOP20_LEVEL1_MAX_QUOTE_AGE_SEC;

    const wideSpread = spreadPct > TOP20_LEVEL1_MAX_SPREAD_PCT;

    let pressure = "NEUTRAL";
    let bullish = false;
    let bearish = false;

    if (stale) {
      pressure = "STALE";
    } else if (ratio <= TOP20_LEVEL1_BEAR_RATIO) {
      pressure = "SELL_PRESSURE";
      bearish = true;
    } else if (ratio >= TOP20_LEVEL1_BULL_RATIO && !wideSpread) {
      pressure = "BUY_PRESSURE";
      bullish = true;
    } else if (wideSpread) {
      pressure = "WIDE_SPREAD";
    }

    return {
      enabled: true,
      available: true,
      pressure,
      bullish,
      bearish,
      neutral: !bullish && !bearish,
      bid,
      ask,
      bidSize,
      askSize,
      ratio: Number.isFinite(ratio) ? Number(ratio.toFixed(3)) : 999,
      spreadPct: Number(spreadPct.toFixed(3)),
      quoteAgeSec: quoteAgeSec != null ? Number(quoteAgeSec.toFixed(1)) : null,
      stale,
      wideSpread,
      quoteTimestampMs,
    };
  } catch (e) {
    console.error(`[TOP20][LEVEL1][${ticker}] error:`, e.message);
    return {
      enabled: true,
      available: false,
      pressure: "ERROR",
      bullish: false,
      bearish: false,
      neutral: true,
      error: e.message,
    };
  }
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

  const histogram = macd[last] - signal[last];
  const prevIndex = Math.max(0, last - 3);
  const previousHistogram =
    macd[prevIndex] != null && signal[prevIndex] != null
      ? macd[prevIndex] - signal[prevIndex]
      : histogram;

  return {
    macd: macd[last],
    signal: signal[last],
    histogram,
    previousHistogram,
    strengthening: histogram > previousHistogram,
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


function clamp(value, min, max) {
  return Math.max(min, Math.min(max, value));
}

function computeTop20MomentumQuality({ leader, bars, macd, sma10, sma100, sma100Up }) {
  if (!TOP20_QUALITY_ENABLED || !bars?.length || !sma10 || !sma100) {
    return { score: 0, label: "UNAVAILABLE", riskFlags: [], metrics: {} };
  }

  const last = bars[bars.length - 1];
  const price = Number(leader.price || last.c || 0);

  let trendScore = 0;
  if (sma10 > sma100) trendScore += 10;
  if (sma100Up) trendScore += 10;
  let macdScore = 0;
  if (macd?.macd > macd?.signal && macd?.histogram > 0) macdScore += 10;
  if (macd?.strengthening) macdScore += 10;

  const recent3 = bars.slice(-3);
  const prior10 = bars.slice(-13, -3);
  const recent3Avg = recent3.length
    ? recent3.reduce((s, b) => s + b.v, 0) / recent3.length : 0;
  const prior10Avg = prior10.length
    ? prior10.reduce((s, b) => s + b.v, 0) / prior10.length : 0;
  const volumeAcceleration = prior10Avg > 0 ? recent3Avg / prior10Avg : 1;

  const volumeScore =
    volumeAcceleration >= 1.5 ? 20 :
    volumeAcceleration >= 1.0 ? 12 :
    volumeAcceleration >= 0.7 ? 5 : 0;

  const distanceAboveSma10Pct = sma10 > 0 ? ((price - sma10) / sma10) * 100 : 0;
  let extensionScore =
    distanceAboveSma10Pct <= 1 ? 15 :
    distanceAboveSma10Pct <= 3 ? 12 :
    distanceAboveSma10Pct <= 6 ? 7 :
    distanceAboveSma10Pct <= 10 ? 2 : 0;
  if (price < sma10) extensionScore = 3;

  const prior20 = bars.slice(-21, -1);
  const recentHigh = prior20.length ? Math.max(...prior20.map(b => b.h)) : last.h;
  const highRatio = recentHigh > 0 ? price / recentHigh : 0;
  const breakoutScore =
    highRatio >= 1.0 ? 10 :
    highRatio >= 0.995 ? 8 :
    highRatio >= 0.98 ? 5 : 0;

  const last5 = bars.slice(-5);
  const totalVol5 = last5.reduce((s, b) => s + b.v, 0);
  const redVol5 = last5
    .filter(b => b.c < b.o)
    .reduce((s, b) => s + b.v, 0);
  const redVolumeRatio = totalVol5 > 0 ? redVol5 / totalVol5 : 0;

  const sellingPressureScore =
    redVolumeRatio <= 0.35 ? 10 :
    redVolumeRatio <= 0.50 ? 7 :
    redVolumeRatio <= 0.65 ? 3 : 0;

  const recentForReclaim = bars.slice(-5);
  const hadPullback = recentForReclaim.slice(0, -1).some(b => b.l <= sma10);
  const reclaim = hadPullback && last.c > sma10 && last.c > last.o;
  const reclaimScore = reclaim ? 5 : 0;

  const riskFlags = [];
  if (distanceAboveSma10Pct > 6) riskFlags.push("EXTENDED_FROM_10SMA");
  if (volumeAcceleration < 0.7) riskFlags.push("VOLUME_FADING");
  if (macd && !macd.strengthening) riskFlags.push("MACD_WEAKENING");
  if (redVolumeRatio > 0.65) riskFlags.push("HEAVY_RED_VOLUME");
  if (Number(leader.pct || 0) >= 80) riskFlags.push("DAY_MOVE_EXTENDED");
  if (price < sma10) riskFlags.push("BELOW_10SMA");

  const rawScore =
    trendScore + macdScore + volumeScore + extensionScore +
    breakoutScore + sellingPressureScore + reclaimScore;

  // Risk penalties prevent contradictory labels such as
  // "75/100 STRONG" while also showing HEAVY RED VOLUME.
  let penalty = 0;
  if (riskFlags.includes("HEAVY_RED_VOLUME")) penalty += 25;
  if (riskFlags.includes("BELOW_10SMA")) penalty += 25;
  if (riskFlags.includes("VOLUME_FADING")) penalty += 15;
  if (riskFlags.includes("MACD_WEAKENING")) penalty += 10;
  if (riskFlags.includes("DAY_MOVE_EXTENDED")) penalty += 10;
  if (riskFlags.includes("EXTENDED_FROM_10SMA")) penalty += 10;

  const score = Math.round(clamp(rawScore - penalty, 0, 100));

  let label =
    score >= TOP20_QUALITY_STRONG ? "STRONG" :
    score >= TOP20_QUALITY_CAUTION ? "GOOD" :
    score >= 40 ? "CAUTION" : "WEAK";

  // Hard-risk conditions can never display as STRONG/GOOD.
  if (
    riskFlags.includes("HEAVY_RED_VOLUME") ||
    riskFlags.includes("BELOW_10SMA")
  ) {
    label = score >= 40 ? "CAUTION" : "WEAK";
  }

  return {
    score,
    rawScore: Math.round(rawScore),
    penalty,
    label,
    riskFlags,
    metrics: {
      volumeAcceleration: Number(volumeAcceleration.toFixed(3)),
      distanceAboveSma10Pct: Number(distanceAboveSma10Pct.toFixed(3)),
      redVolumeRatio: Number(redVolumeRatio.toFixed(3)),
      recentHigh: Number(recentHigh.toFixed(4)),
      nearRecentHigh: highRatio >= 0.98,
      breakout: highRatio >= 1.0,
      reclaim10Sma: reclaim,
      macdStrengthening: Boolean(macd?.strengthening),
    },
  };
}

function evaluateTop20Safety({ leader, bars, macd, sma10, momentumQuality }) {
  if (!TOP20_SAFETY_FILTER_ENABLED) {
    return {
      passed: true,
      reasons: [],
      metrics: {},
    };
  }

  const last = bars[bars.length - 1];
  const closes = bars.map(b => b.c);
  const price = Number(leader.price || last.c || 0);

  const sma10PastEnd = closes.length - TOP20_SMA10_TREND_LOOKBACK;
  const pastSma10 = sma(closes, 10, sma10PastEnd);
  const sma10SlopePct =
    sma10 != null && pastSma10 != null && pastSma10 !== 0
      ? ((sma10 - pastSma10) / pastSma10) * 100
      : 0;
  const momentumLookback = Math.max(1, TOP20_RECENT_MOMENTUM_LOOKBACK);
  const oldIndex = Math.max(0, closes.length - 1 - momentumLookback);
  const oldClose = closes[oldIndex] || price;
  const recentMomentumPct =
    oldClose > 0 ? ((last.c - oldClose) / oldClose) * 100 : 0;

  const last3 = bars.slice(-3);
  const upperWickRatios = last3.map(b => {
    const range = Math.max(0, b.h - b.l);
    if (range <= 0) return 0;
    const upperWick = Math.max(0, b.h - Math.max(b.o, b.c));
    return upperWick / range;
  });
  const avgUpperWickRatio = upperWickRatios.length
    ? upperWickRatios.reduce((a, b) => a + b, 0) / upperWickRatios.length
    : 0;

  const q = momentumQuality?.metrics || {};
  const redVolumeRatio = Number(q.redVolumeRatio || 0);
  const distanceAboveSma10Pct = Number(q.distanceAboveSma10Pct || 0);
  const reasons = [];

  if (TOP20_REQUIRE_PRICE_ABOVE_SMA10 && !(price > sma10)) {
    reasons.push("PRICE_BELOW_10SMA");
  }

  if (TOP20_REQUIRE_SMA10_RISING && !(sma10SlopePct > 0)) {
    reasons.push("SMA10_NOT_RISING");
  }

  if (redVolumeRatio > TOP20_MAX_RED_VOLUME_RATIO) {
    reasons.push("HEAVY_RED_VOLUME");
  }

  if (recentMomentumPct < TOP20_MIN_RECENT_MOMENTUM_PCT) {
    reasons.push("RECENT_MOMENTUM_WEAK");
  }

  if (avgUpperWickRatio > TOP20_MAX_UPPER_WICK_RATIO) {
    reasons.push("REJECTION_WICKS");
  }

  if (distanceAboveSma10Pct > TOP20_MAX_DISTANCE_ABOVE_SMA10_PCT) {
    reasons.push("TOO_EXTENDED_FROM_10SMA");
  }

  if (TOP20_REQUIRE_MACD_STRENGTHENING && !macd?.strengthening) {
    reasons.push("MACD_NOT_STRENGTHENING");
  }

  return {
    passed: reasons.length === 0,
    reasons,
    metrics: {
      priceAboveSma10: price > sma10,
      sma10SlopePct: Number(sma10SlopePct.toFixed(4)),
      recentMomentumPct: Number(recentMomentumPct.toFixed(4)),
      avgUpperWickRatio: Number(avgUpperWickRatio.toFixed(4)),
    },
  };
}

function evaluateTop20Trigger(bars) {
  const empty = {
    enabled: TOP20_TRIGGER_ENABLED,
    passed: false,
    uptrendConfirmed: false,
    volumeJumpPassed: false,
    multiplier: 0,
    currentCandleVolume: 0,
    previousCandleVolume: 0,
    triggerCandleTimestampMs: null,
    triggerCandleGreen: false,
    higherClose: false,
    priceAboveSma10: false,
    sma10Above100: false,
    sma10Rising: false,
    sma100Rising: false,
    sma10: null,
    sma100: null,
    reason: "UNAVAILABLE",
  };

  if (!TOP20_TRIGGER_ENABLED) return { ...empty, reason: "DISABLED" };
  if (!Array.isArray(bars) || bars.length < 110) {
    return { ...empty, reason: "INSUFFICIENT_BARS" };
  }

  // Bar timestamp is the start of the minute. Only use fully closed candles.
  const cutoff =
    Date.now() - 60_000 - Math.max(0, TOP20_TRIGGER_COMPLETION_GRACE_MS);

  const completed = bars
    .filter(b =>
      Number.isFinite(b?.t) &&
      Number.isFinite(b?.v) &&
      Number.isFinite(b?.o) &&
      Number.isFinite(b?.c) &&
      b.t <= cutoff
    )
    .sort((a, b) => a.t - b.t);

  if (completed.length < 110) {
    return { ...empty, reason: "INSUFFICIENT_COMPLETED_BARS" };
  }

  const current = completed[completed.length - 1];
  const previous = completed[completed.length - 2];
  const closes = completed.map(b => Number(b.c));

  const currentSma10 = sma(closes, 10);
  const currentSma100 = sma(closes, 100);

  const sma10PastEnd = Math.max(10, closes.length - TOP20_SMA10_TREND_LOOKBACK);
  const sma100PastEnd = Math.max(100, closes.length - TOP20_SMA_TREND_LOOKBACK);

  const pastSma10 = sma(closes, 10, sma10PastEnd);
  const pastSma100 = sma(closes, 100, sma100PastEnd);

  const priceAboveSma10 =
    currentSma10 != null && Number(current.c) > currentSma10;

  const sma10Above100 =
    currentSma10 != null &&
    currentSma100 != null &&
    currentSma10 > currentSma100;

  const sma10Rising =
    currentSma10 != null &&
    pastSma10 != null &&
    currentSma10 > pastSma10;

  const sma100Rising =
    currentSma100 != null &&
    pastSma100 != null &&
    currentSma100 > pastSma100;

  const triggerCandleGreen = Number(current.c) > Number(current.o);
  const higherClose = Number(current.c) > Number(previous.c);

  const multiplier =
    Number(previous.v || 0) > 0
      ? Number(current.v || 0) / Number(previous.v)
      : 0;
  const volumeJumpPassed =
    multiplier >= TOP20_TRIGGER_VOLUME_MULTIPLIER;

  const uptrendConfirmed = Boolean(
    sma10Above100 &&
    (!TOP20_TRIGGER_REQUIRE_PRICE_ABOVE_SMA10 || priceAboveSma10) &&
    (!TOP20_TRIGGER_REQUIRE_SMA10_RISING || sma10Rising) &&
    (!TOP20_TRIGGER_REQUIRE_SMA100_RISING || sma100Rising)
  );

  const passed = Boolean(
    uptrendConfirmed &&
    volumeJumpPassed &&
    (!TOP20_TRIGGER_REQUIRE_GREEN || triggerCandleGreen) &&
    (!TOP20_TRIGGER_REQUIRE_HIGHER_CLOSE || higherClose)
  );

  let reason = "PASS";
  if (!uptrendConfirmed) reason = "UPTREND_NOT_CONFIRMED";
  else if (!volumeJumpPassed) reason = "NO_5X_PREVIOUS_CANDLE_JUMP";
  else if (TOP20_TRIGGER_REQUIRE_GREEN && !triggerCandleGreen) {
    reason = "TRIGGER_CANDLE_NOT_GREEN";
  } else if (TOP20_TRIGGER_REQUIRE_HIGHER_CLOSE && !higherClose) {
    reason = "TRIGGER_CANDLE_NOT_HIGHER_CLOSE";
  }

  return {
    enabled: true,
    passed,
    uptrendConfirmed,
    volumeJumpPassed,
    multiplier: Number(multiplier.toFixed(3)),
    currentCandleVolume: Math.round(Number(current.v || 0)),
    previousCandleVolume: Math.round(Number(previous.v || 0)),
    triggerCandleTimestampMs: Number(current.t),
    triggerCandleGreen,
    higherClose,
    priceAboveSma10,
    sma10Above100,
    sma10Rising,
    sma100Rising,
    sma10: currentSma10 != null ? Number(currentSma10.toFixed(6)) : null,
    sma100: currentSma100 != null ? Number(currentSma100.toFixed(6)) : null,
    reason,
  };
}


function evaluateTop20DataQuality(bars) {
  const reasons = [];
  const metrics = {
    barAgeSec: null,
    duplicateBars: 0,
    zeroVolumeRatio: 0,
    malformedBars: 0,
  };

  if (!Array.isArray(bars) || bars.length < TOP20_MIN_BARS) {
    reasons.push('INSUFFICIENT_BARS');
    return { passed: false, reasons, metrics };
  }

  let malformed = 0;
  let duplicates = 0;
  const seen = new Set();
  for (const b of bars) {
    if (
      !Number.isFinite(b.t) || !Number.isFinite(b.o) || !Number.isFinite(b.h) ||
      !Number.isFinite(b.l) || !Number.isFinite(b.c) || !Number.isFinite(b.v) ||
      b.o <= 0 || b.h <= 0 || b.l <= 0 || b.c <= 0 || b.h < b.l
    ) malformed++;
    if (seen.has(b.t)) duplicates++;
    seen.add(b.t);
  }

  metrics.malformedBars = malformed;
  metrics.duplicateBars = duplicates;
  if (malformed > 0) reasons.push('MALFORMED_BARS');
  if (duplicates > 0) reasons.push('DUPLICATE_BARS');

  const latest = bars[bars.length - 1];
  const barAgeSec = latest?.t ? (Date.now() - latest.t) / 1000 : Infinity;
  metrics.barAgeSec = Number.isFinite(barAgeSec) ? Number(barAgeSec.toFixed(1)) : null;
  if (!Number.isFinite(barAgeSec) || barAgeSec > TOP20_MAX_BAR_AGE_SEC) {
    reasons.push('STALE_DATA');
  }

  const recent = bars.slice(-20);
  const zeroVol = recent.filter(b => !(b.v > 0)).length;
  const zeroVolumeRatio = recent.length ? zeroVol / recent.length : 1;
  metrics.zeroVolumeRatio = Number(zeroVolumeRatio.toFixed(3));
  if (zeroVolumeRatio > TOP20_MAX_ZERO_VOL_RATIO) reasons.push('TOO_MANY_ZERO_VOLUME_BARS');

  return { passed: reasons.length === 0, reasons, metrics };
}

function evaluateTop20Technical(leader, bars) {
  const dataQuality = evaluateTop20DataQuality(bars);
  if (!bars || bars.length < TOP20_MIN_BARS) {
    return {
      ...leader,
      insufficientBars: true,
      barsAvailable: bars?.length || 0,
      score: 0,
      dataQuality,
      detectedAtMs: Date.now(),
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
  const momentumQuality = computeTop20MomentumQuality({
    leader,
    bars,
    macd,
    sma10: currentSma10,
    sma100: currentSma100,
    sma100Up: criteria.sma100Up,
  });

  const safety = evaluateTop20Safety({
    leader,
    bars,
    macd,
    sma10: currentSma10,
    momentumQuality,
  });

  // Contradiction guard: core bullish score cannot override clearly bearish immediate conditions.
  const contradictionReasons = [];
  if (criteria.sma10Above100 && !safety.metrics?.priceAboveSma10) contradictionReasons.push('PRICE_BELOW_10SMA');
  if (criteria.macdPositive && !momentumQuality.metrics?.macdStrengthening) contradictionReasons.push('MACD_WEAKENING');
  if (momentumQuality.metrics?.redVolumeRatio > TOP20_MAX_RED_VOLUME_RATIO) contradictionReasons.push('HEAVY_RED_VOLUME');
  if (safety.metrics?.recentMomentumPct < TOP20_MIN_RECENT_MOMENTUM_PCT) contradictionReasons.push('RECENT_MOMENTUM_WEAK');

  if (!dataQuality.passed) {
    safety.passed = false;
    safety.reasons = [...new Set([...(safety.reasons || []), ...dataQuality.reasons])];
  }
  if (contradictionReasons.length) {
    safety.passed = false;
    safety.reasons = [...new Set([...(safety.reasons || []), ...contradictionReasons])];
  }

  const detectedAtMs = Date.now();
  const lastCompletedBarAtMs = bars[bars.length - 1]?.t || 0;

  return {
    ...leader,
    score,
    criteria,
    bonus,
    momentumQuality,
    safety,
    dataQuality,
    detectedAtMs,
    lastCompletedBarAtMs,
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

    // Bonuses do not change the core 5-point score.
    freshCross: Boolean(b.freshSmaCross),
    crossMinutesAgo: result.crossBarsAgo != null ? Number(result.crossBarsAgo) : null,

    triggerPassed: Boolean(result.trigger?.passed),
    triggerUptrendConfirmed: Boolean(result.trigger?.uptrendConfirmed),
    triggerVolumeJumpPassed: Boolean(result.trigger?.volumeJumpPassed),
    triggerVolumeMultiplier: Number(result.trigger?.multiplier || 0),
    triggerCurrentCandleVolume: Number(result.trigger?.currentCandleVolume || 0),
    triggerPreviousCandleVolume: Number(result.trigger?.previousCandleVolume || 0),
    triggerCandleGreen: Boolean(result.trigger?.triggerCandleGreen),
    triggerHigherClose: Boolean(result.trigger?.higherClose),
    triggerPriceAboveSma10: Boolean(result.trigger?.priceAboveSma10),
    triggerSma10Above100: Boolean(result.trigger?.sma10Above100),
    triggerSma10Rising: Boolean(result.trigger?.sma10Rising),
    triggerSma100Rising: Boolean(result.trigger?.sma100Rising),
    triggerReason: String(result.trigger?.reason || "UNAVAILABLE"),
    triggerCandleAt: result.trigger?.triggerCandleTimestampMs
      ? new Date(result.trigger.triggerCandleTimestampMs).toISOString()
      : null,

    momentumQuality: Number(result.momentumQuality?.score || 0),
    momentumQualityLabel: String(result.momentumQuality?.label || "UNAVAILABLE"),
    riskFlags: Array.isArray(result.momentumQuality?.riskFlags) ? result.momentumQuality.riskFlags : [],
    volumeAcceleration: Number(result.momentumQuality?.metrics?.volumeAcceleration || 0),
    distanceAboveSma10Pct: Number(result.momentumQuality?.metrics?.distanceAboveSma10Pct || 0),
    redVolumeRatio: Number(result.momentumQuality?.metrics?.redVolumeRatio || 0),
    recentHigh: Number(result.momentumQuality?.metrics?.recentHigh || 0),
    nearRecentHigh: Boolean(result.momentumQuality?.metrics?.nearRecentHigh),
    breakingRecentHigh: Boolean(result.momentumQuality?.metrics?.breakout),
    reclaim10Sma: Boolean(result.momentumQuality?.metrics?.reclaim10Sma),
    macdStrengthening: Boolean(result.momentumQuality?.metrics?.macdStrengthening),

    // Momentum safety filter state.
    safetyPass: Boolean(result.safety?.passed),
    safetyReasons: Array.isArray(result.safety?.reasons) ? result.safety.reasons : [],
    priceAboveSma10: Boolean(result.safety?.metrics?.priceAboveSma10),
    sma10SlopePct: Number(result.safety?.metrics?.sma10SlopePct || 0),
    recentMomentumPct: Number(result.safety?.metrics?.recentMomentumPct || 0),
    avgUpperWickRatio: Number(result.safety?.metrics?.avgUpperWickRatio || 0),

    // Level 1 NBBO buy-pressure confirmation (bonus only).
    level1Enabled: Boolean(result.level1?.enabled),
    level1Available: Boolean(result.level1?.available),
    level1Pressure: String(result.level1?.pressure || "UNAVAILABLE"),
    level1Bullish: Boolean(result.level1?.bullish),
    level1Bearish: Boolean(result.level1?.bearish),
    level1Bid: Number(result.level1?.bid || 0),
    level1Ask: Number(result.level1?.ask || 0),
    level1BidSize: Number(result.level1?.bidSize || 0),
    level1AskSize: Number(result.level1?.askSize || 0),
    level1BidAskRatio: result.level1?.ratio != null ? Number(result.level1.ratio) : null,
    level1SpreadPct: result.level1?.spreadPct != null ? Number(result.level1.spreadPct) : null,
    level1QuoteAgeSec: result.level1?.quoteAgeSec != null ? Number(result.level1.quoteAgeSec) : null,

    // Data freshness / quality guard.
    dataQualityPass: Boolean(result.dataQuality?.passed),
    dataQualityReasons: Array.isArray(result.dataQuality?.reasons) ? result.dataQuality.reasons : [],
    barAgeSec: result.dataQuality?.metrics?.barAgeSec ?? null,
    duplicateBars: Number(result.dataQuality?.metrics?.duplicateBars || 0),
    zeroVolumeRatio: Number(result.dataQuality?.metrics?.zeroVolumeRatio || 0),

    // Timing fields so Base44 can show exactly how fresh the signal is.
    detectedAt: result.detectedAtMs ? new Date(result.detectedAtMs).toISOString() : null,
    lastCompletedBarAt: result.lastCompletedBarAtMs
      ? new Date(result.lastCompletedBarAtMs).toISOString() : null,

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
  const safetyPass = result.safety?.passed !== false;
  const qualifiesForAlert = score >= TOP20_MIN_SCORE && safetyPass;

  if (!qualifiesForAlert) {
    if (!state.belowThresholdSince) state.belowThresholdSince = now;

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

  if (state.lastAlertScore < TOP20_MIN_SCORE) {
    state.lastScore = score;
    state.lastAlertScore = score;
    state.lastAlertAt = now;
    state.lastBonus = freshBonus;
    top20AlertState.set(ticker, state);
    return true;
  }

  if (score === 5 && state.lastAlertScore < 5) {
    state.lastScore = score;
    state.lastAlertScore = 5;
    state.lastAlertAt = now;
    state.lastBonus = freshBonus;
    top20AlertState.set(ticker, state);
    return true;
  }

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

async function handleTop20FeedFailure(error) {
  top20FeedFailureCount++;
  if (top20FeedFailureCount >= TOP20_CIRCUIT_BREAKER_FAILURES) {
    top20CircuitOpen = true;
    if (!top20CircuitNotified) {
      top20CircuitNotified = true;
      await pushToTelegram(
        `⚠️ <b>TOP 20 SCALP PAUSED</b>\n` +
        `Massive data feed failed ${top20FeedFailureCount} consecutive scans.\n` +
        `Alerts are paused rather than using unreliable data.\n` +
        `Last error: ${String(error?.message || error).slice(0, 250)}`
      );
    }
  }
}

async function handleTop20FeedSuccess() {
  const wasOpen = top20CircuitOpen;
  top20FeedFailureCount = 0;
  top20CircuitOpen = false;
  if (wasOpen) {
    top20LastFeedRecoveryAt = new Date().toISOString();
    top20CircuitNotified = false;
    await pushToTelegram(
      `🟢 <b>TOP 20 SCALP DATA FEED RECOVERED</b>\n` +
      `Massive data is responding again. Scanner alerts resumed.`
    );
  }
}

function shouldLogShadowSignal(result) {
  if (!TOP20_SHADOW_MODE || result.score < TOP20_MIN_SCORE || result.safety?.passed) return false;
  const key = result.ticker;
  const now = Date.now();
  const reasonsKey = (result.safety?.reasons || []).slice().sort().join('|');
  const prev = top20ShadowState.get(key);
  if (!prev || prev.reasonsKey !== reasonsKey || now - prev.at >= TOP20_REARM_MIN * 60_000) {
    top20ShadowState.set(key, { at: now, reasonsKey });
    return true;
  }
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
  let tickerErrors = 0;
  let shadowThisRun = 0;

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

          // Trigger gate: full analysis runs only after a confirmed uptrend
          // prints a >=5x volume jump versus the immediately previous
          // fully completed 1-minute candle.
          const trigger = evaluateTop20Trigger(bars);
          if (!trigger.passed) return null;

          const result = evaluateTop20Technical(leader, bars);
          if (result) result.trigger = trigger;
          return result;
        } catch (e) {
          tickerErrors++;
          console.error(`[TOP20][${leader.ticker}] error:`, e.message);
          return null;
        }
      }
    );

    // Scan-level feed health. A main request failure is caught below; excessive per-ticker
    // failures are also treated as degraded data so alerts do not continue blindly.
    if (leadersFetched === 0 || (leadersFetched > 0 && tickerErrors / leadersFetched > 0.5)) {
      throw new Error(`Top20 feed degraded: leaders=${leadersFetched}, tickerErrors=${tickerErrors}`);
    }
    await handleTop20FeedSuccess();

    for (const result of evaluated.filter(Boolean)) {
      if (result.insufficientBars) {
        insufficientBars++;
        continue;
      }

      analyzed++;
      if (result.score === 4) qualifying4++;
      if (result.score === 5) qualifying5++;

      // Level 1 is fetched only for 4/5 or 5/5 setups (including shadow candidates).
      // This keeps the scanner fast and avoids 20 extra quote calls every 10 seconds.
      if (TOP20_LEVEL1_ENABLED && result.score >= TOP20_MIN_SCORE) {
        result.level1 = await getTop20Level1Quote(result.ticker);
      } else {
        result.level1 = {
          enabled: TOP20_LEVEL1_ENABLED,
          available: false,
          pressure: result.score >= TOP20_MIN_SCORE ? "UNAVAILABLE" : "NOT_CHECKED",
          bullish: false,
          bearish: false,
          neutral: true,
        };
      }

      if (shouldLogShadowSignal(result)) {
        shadowThisRun++;
        top20ShadowSignals++;
        await recordTop20Signal(result, "SHADOW", result.safety?.reasons || []);
        console.log(
          `[TOP20][SHADOW] #${result.rank} ${result.ticker} score=${result.score}/5 ` +
          `blocked=${(result.safety?.reasons || []).join(",")}`
        );
      }

      const shouldAlert = shouldSendTop20Alert(result);

      if (
        result.score >= TOP20_MIN_SCORE &&
        result.safety?.passed &&
        shouldAlert
      ) {
        const telegramStartedAt = Date.now();
        await pushToTelegram(
          formatTop20Telegram(result, {
            scanStartedAtMs: started,
            detectedAtMs: result.detectedAtMs,
          })
        );
        const telegramApiMs = Date.now() - telegramStartedAt;

        await recordTop20Signal(result, "ALERT", []);
        alertsThisRun++;
        top20AlertsSent++;
        console.log(
          `[TOP20][ALERT] #${result.rank} ${result.ticker} ` +
          `score=${result.score}/5 quality=${result.momentumQuality?.score || 0}/100 ` +
          `scanToSignalMs=${Math.max(0, result.detectedAtMs - started)} ` +
          `telegramApiMs=${telegramApiMs} ` +
          `pct=${result.pct.toFixed(2)} vol=${Math.round(result.volume)}`
        );
      }
    }

    // Publish the complete current Top-20 state for Base44 AFTER Level 1 enrichment.
    top20LastResults = evaluated
      .filter(Boolean)
      .map(serializeTop20Result)
      .sort((a, b) => (b.score - a.score) || (a.rank - b.rank));

  } catch (e) {
    top20LastError = e.message;
    console.error("TOP20 scan error:", e.message);
    await handleTop20FeedFailure(e);
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
      tickerErrors,
      shadowThisRun,
      circuitOpen: top20CircuitOpen,
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


function pacificDateKey(date = new Date()) {
  return new Intl.DateTimeFormat("en-CA", {
    timeZone: "America/Los_Angeles",
    year: "numeric", month: "2-digit", day: "2-digit",
  }).format(date);
}

function pacificHourMinuteNow() {
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone: "America/Los_Angeles",
    hour: "2-digit", minute: "2-digit", hour12: false,
  }).formatToParts(new Date());
  return {
    hour: Number(parts.find(p => p.type === "hour")?.value || 0),
    minute: Number(parts.find(p => p.type === "minute")?.value || 0),
  };
}

async function runTop20DailySelfTest() {
  if (!TOP20_SELF_TEST_ENABLED || !TOP20_ENABLED) return;
  const { hour, minute } = pacificHourMinuteNow();
  if (hour !== TOP20_SELF_TEST_HOUR_PT || minute < TOP20_SELF_TEST_MINUTE_PT) return;

  const day = pacificDateKey();
  if (top20LastSelfTestDate === day) return;
  top20LastSelfTestDate = day;

  const checks = { database: false, massive: false, telegramConfigured: false };
  const errors = [];
  try { await pool.query('SELECT 1'); checks.database = true; } catch (e) { errors.push(`DB: ${e.message}`); }
  try {
    const leaders = await fetchTop20Gainers();
    checks.massive = Array.isArray(leaders) && leaders.length > 0;
    if (!checks.massive) errors.push('Massive: no top gainers returned');
  } catch (e) { errors.push(`Massive: ${e.message}`); }
  checks.telegramConfigured = Boolean(TELEGRAM_BOT_TOKEN && TELEGRAM_CHAT_ID);
  if (!checks.telegramConfigured) errors.push('Telegram: not configured');

  const ok = checks.database && checks.massive && checks.telegramConfigured;
  top20LastSelfTest = { at: new Date().toISOString(), ok, checks, errors };

  const lines = [
    ok ? '🟢 <b>TOP 20 SCALP READY</b>' : '⚠️ <b>TOP 20 SCALP SELF-TEST WARNING</b>',
    `Database: ${checks.database ? '✅' : '❌'}`,
    `Massive: ${checks.massive ? '✅' : '❌'}`,
    `Telegram: ${checks.telegramConfigured ? '✅' : '❌'}`,
  ];
  if (errors.length) lines.push(`Issues: ${errors.join(' | ').slice(0, 500)}`);
  await pushToTelegram(lines.join('\n'));
}

async function top20MaintenanceLoop() {
  await Promise.allSettled([
    runTop20DailySelfTest(),
    updateTop20Outcomes(),
  ]);
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
ensureTop20TrackingTable().catch(e => console.error("Top20 tracking init error:", e.message));
scanTop20Technicals();
setInterval(scanTop20Technicals, TOP20_SCAN_INTERVAL_MS);

// Reliability / learning maintenance: self-test + 1/3/5/10-minute outcome tracking.
top20MaintenanceLoop();
setInterval(top20MaintenanceLoop, 15_000);
