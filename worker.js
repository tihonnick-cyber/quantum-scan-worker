const http = require("http");
const WebSocket = require("ws");

const API_KEY = process.env.MASSIVE_API_KEY;
const PORT = Number(process.env.PORT || 8080);

const TOP_GAINERS_LIMIT = Number(process.env.TOP_GAINERS_LIMIT || 50);
const PRICE_MIN = Number(process.env.PRICE_MIN || 0);
const PRICE_MAX = Number(process.env.PRICE_MAX || 10);
const VOLUME_MULTIPLIER = Number(process.env.VOLUME_MULTIPLIER || 3);
const MIN_TRIGGER_VOLUME = Number(process.env.MIN_TRIGGER_VOLUME || 50000);
const SCAN_START_HOUR_PT = Number(process.env.SCAN_START_HOUR_PT || 6);
const SCAN_END_HOUR_PT = Number(process.env.SCAN_END_HOUR_PT || 13);
const ALERT_COOLDOWN_SEC = Number(process.env.ALERT_COOLDOWN_SEC || 0);
const RANK_REFRESH_MS = Number(process.env.RANK_REFRESH_MS || 1000);
const WS_URL = process.env.MASSIVE_WS_URL || "wss://socket.massive.com/stocks";

const TG_TOKEN = process.env.TELEGRAM_BOT_TOKEN || "";
const TG_CHAT = process.env.TELEGRAM_CHAT_ID || "";

if (!API_KEY) throw new Error("Set MASSIVE_API_KEY in Railway");

let ws = null;
let prevClose = new Map();
let market = new Map();
let top50 = new Set();
let top50Info = new Map();
let prevCloseDate = null;
let lastRankAt = 0;

const currentBars = new Map();   // sym -> current 30s bar
const previousBars = new Map();  // sym -> previous completed 30s bar
const lastAlert = new Map();

function pt(ts = Date.now()) {
  return Object.fromEntries(
    new Intl.DateTimeFormat("en-US", {
      timeZone: "America/Los_Angeles",
      hour12: false,
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
    }).formatToParts(new Date(ts)).map(x => [x.type, x.value])
  );
}

function active(ts = Date.now()) {
  const h = Number(pt(ts).hour);
  return h >= SCAN_START_HOUR_PT && h < SCAN_END_HOUR_PT;
}

function bucket30s(ts) {
  return Math.floor(ts / 30000) * 30000;
}

function dateStringUTC(ms) {
  return new Date(ms).toISOString().slice(0, 10);
}

async function fetchJson(url) {
  const r = await fetch(url);
  const body = await r.text();
  if (!r.ok) throw new Error(`${r.status} ${body.slice(0, 500)}`);
  try {
    return JSON.parse(body);
  } catch {
    throw new Error(`Invalid JSON: ${body.slice(0, 250)}`);
  }
}

// Uses Daily Market Summary, not any Snapshot endpoint.
// Massive documents this endpoint as included in all Stocks plans.
async function loadPreviousCloses() {
  const now = Date.now();

  for (let daysBack = 1; daysBack <= 10; daysBack++) {
    const date = dateStringUTC(now - daysBack * 86400000);
    const url =
      `https://api.massive.com/v2/aggs/grouped/locale/us/market/stocks/${date}` +
      `?adjusted=true&include_otc=false&apiKey=${encodeURIComponent(API_KEY)}`;

    try {
      const d = await fetchJson(url);
      const rows = Array.isArray(d?.results) ? d.results : [];

      if (rows.length > 100) {
        const map = new Map();
        for (const r of rows) {
          const ticker = String(r?.T || "").toUpperCase();
          const close = Number(r?.c || 0);
          if (ticker && close > 0) map.set(ticker, close);
        }

        if (map.size > 100) {
          prevClose = map;
          prevCloseDate = date;
          console.log(`[PREVCLOSE] Loaded ${map.size} tickers from ${date}`);
          return;
        }
      }
    } catch (e) {
      console.error(`[PREVCLOSE] ${date}: ${e.message}`);
    }
  }

  throw new Error("Could not load a previous trading-day market summary.");
}

function rebuildTop50() {
  const ranked = [];

  for (const [ticker, x] of market) {
    const prev = prevClose.get(ticker);
    if (!(prev > 0)) continue;
    if (!(x.price >= PRICE_MIN && x.price <= PRICE_MAX)) continue;

    const pct = ((x.price - prev) / prev) * 100;
    if (!(pct > 0) || !Number.isFinite(pct)) continue;

    ranked.push({
      ticker,
      price: x.price,
      pct,
      dayVolume: x.dayVolume || 0,
    });
  }

  ranked.sort((a, b) => b.pct - a.pct);
  const leaders = ranked.slice(0, TOP_GAINERS_LIMIT);

  top50 = new Set(leaders.map(x => x.ticker));
  top50Info = new Map(leaders.map((x, i) => [x.ticker, { ...x, rank: i + 1 }]));
  lastRankAt = Date.now();

  if (leaders.length) {
    console.log(
      `[TOP50] ${leaders.length}: ` +
      leaders.slice(0, 10)
        .map((x, i) => `#${i + 1} ${x.ticker} +${x.pct.toFixed(1)}%`)
        .join(" | ")
    );
  }
}

function updateRankingMaybe() {
  if (Date.now() - lastRankAt >= RANK_REFRESH_MS) rebuildTop50();
}

function onSecondAggregate(m) {
  const sym = String(m?.sym || "").toUpperCase();
  const ts = Number(m?.s || Date.now());
  const price = Number(m?.c || 0);

  if (!sym || !(price > 0)) return;

  const dayVolume = Number(m?.av || 0);
  market.set(sym, { price, dayVolume, updatedAt: ts });

  updateRankingMaybe();

  if (!active(ts)) return;
  if (!prevClose.has(sym)) return;
  if (!(price >= PRICE_MIN && price <= PRICE_MAX)) return;

  const start = bucket30s(ts);
  let bar = currentBars.get(sym);

  if (!bar || bar.start !== start) {
    // Finalize the previous current bar when the symbol moves into a new 30s bucket.
    if (bar) {
      evaluateCompletedBar(sym, bar).catch(e =>
        console.error(`[EVAL][${sym}]`, e.message)
      );
      previousBars.set(sym, bar);
    }

    bar = {
      start,
      open: Number(m?.o || price),
      high: Number(m?.h || price),
      low: Number(m?.l || price),
      close: price,
      volume: Number(m?.v || 0),
    };
    currentBars.set(sym, bar);
  } else {
    bar.high = Math.max(bar.high, Number(m?.h || price));
    bar.low = Math.min(bar.low, Number(m?.l || price));
    bar.close = price;
    bar.volume += Number(m?.v || 0);
  }
}

async function evaluateCompletedBar(sym, bar) {
  const prev = previousBars.get(sym);
  if (!prev || prev.start !== bar.start - 30000) {
    console.log(`[BASELINE] ${sym} V=${Math.round(bar.volume)}`);
    return;
  }

  const info = top50Info.get(sym);
  const isTop50 = top50.has(sym);
  const green = bar.close > bar.open;
  const ratio = prev.volume > 0 ? bar.volume / prev.volume : 0;

  console.log(
    `[30S] ${sym} top50=${isTop50} rank=${info?.rank || "?"} ` +
    `O=${bar.open} C=${bar.close} V=${Math.round(bar.volume)} ` +
    `prev=${Math.round(prev.volume)} x=${ratio.toFixed(2)} ` +
    `green=${green} minVolPass=${bar.volume >= MIN_TRIGGER_VOLUME}`
  );

  // ONLY alert rules.
  if (!isTop50) return;
  if (!green) return;
  if (bar.volume < MIN_TRIGGER_VOLUME) return;
  if (prev.volume <= 0) return;
  if (ratio < VOLUME_MULTIPLIER) return;

  const now = Date.now();
  const last = lastAlert.get(sym) || 0;
  if (
    ALERT_COOLDOWN_SEC > 0 &&
    now - last < ALERT_COOLDOWN_SEC * 1000
  ) return;

  lastAlert.set(sym, now);

  const q = pt(bar.start);
  const text =
`🚨 TOP-50 30-SEC VOLUME SPIKE
${sym} | Rank #${info?.rank || "?"}
Price: $${bar.close.toFixed(2)}
Day: +${Number(info?.pct || 0).toFixed(2)}%
30s Volume: ${Math.round(bar.volume).toLocaleString()}
Previous 30s: ${Math.round(prev.volume).toLocaleString()}
Spike: ${ratio.toFixed(2)}x
GREEN candle
${q.hour}:${q.minute}:${q.second} PT`;

  console.log("[ALERT]", text.replace(/\n/g, " | "));

  if (TG_TOKEN && TG_CHAT) {
    const r = await fetch(
      `https://api.telegram.org/bot${TG_TOKEN}/sendMessage`,
      {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ chat_id: TG_CHAT, text }),
      }
    );

    if (!r.ok) {
      console.error(
        "[TELEGRAM]",
        r.status,
        (await r.text()).slice(0, 300)
      );
    }
  }
}

function connectWebSocket() {
  ws = new WebSocket(WS_URL);

  ws.on("open", () => {
    ws.send(JSON.stringify({
      action: "auth",
      params: API_KEY,
    }));
  });

  ws.on("message", raw => {
    let messages;
    try {
      messages = JSON.parse(raw.toString());
    } catch {
      return;
    }

    if (!Array.isArray(messages)) messages = [messages];

    for (const m of messages) {
      if (m.ev === "status") {
        console.log("[WS]", m.status, m.message || "");

        if (m.status === "auth_success") {
          // Subscribe to all stock per-second aggregates.
          // Massive documents A.* as supported.
          ws.send(JSON.stringify({
            action: "subscribe",
            params: "A.*",
          }));
          console.log("[WS] subscribing to A.*");
        }
      } else if (m.ev === "A") {
        onSecondAggregate(m);
      }
    }
  });

  ws.on("error", e => console.error("[WS]", e.message));

  ws.on("close", () => {
    console.log("[WS] closed; reconnecting in 3 seconds");
    setTimeout(connectWebSocket, 3000);
  });
}

http.createServer((req, res) => {
  res.setHeader("content-type", "application/json");

  if (req.url === "/health") {
    res.end(JSON.stringify({
      ok: true,
      scanner: "TOP50_30SEC_NO_SNAPSHOTS",
      previousCloseDate: prevCloseDate,
      previousCloseTickers: prevClose.size,
      liveTickersSeen: market.size,
      top50Count: top50.size,
      websocket:
        ws?.readyState === WebSocket.OPEN ? "open" : "closed",
      config: {
        TOP_GAINERS_LIMIT,
        PRICE_MIN,
        PRICE_MAX,
        VOLUME_MULTIPLIER,
        MIN_TRIGGER_VOLUME,
        SCAN_START_HOUR_PT,
        SCAN_END_HOUR_PT,
        ALERT_COOLDOWN_SEC,
        RANK_REFRESH_MS,
      }
    }));
    return;
  }

  res.statusCode = 404;
  res.end(JSON.stringify({ error: "not found" }));
}).listen(PORT, "0.0.0.0", () => {
  console.log(`[HTTP] ${PORT}`);
});

async function start() {
  console.log("TOP-50 / 30-SECOND / NO-SNAPSHOT SCANNER");
  console.log(
    `Rules: Top ${TOP_GAINERS_LIMIT}, price ${PRICE_MIN}-${PRICE_MAX}, ` +
    `green 30s candle, volume >= ${MIN_TRIGGER_VOLUME}, ` +
    `spike >= ${VOLUME_MULTIPLIER}x, ${SCAN_START_HOUR_PT}:00-${SCAN_END_HOUR_PT}:00 PT`
  );

  await loadPreviousCloses();
  connectWebSocket();
}

start().catch(e => {
  console.error("[FATAL]", e.message);
  process.exit(1);
});
