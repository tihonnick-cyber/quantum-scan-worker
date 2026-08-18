const http = require("http");
const WebSocket = require("ws");

const API_KEY = process.env.MASSIVE_API_KEY;
const PORT = Number(process.env.PORT || 8080);
const LIMIT = Math.min(Number(process.env.TOP_GAINERS_LIMIT || 20), 20);
const PRICE_MIN = Number(process.env.PRICE_MIN || 0);
const PRICE_MAX = Number(process.env.PRICE_MAX || 10);
const MULT = Number(process.env.VOLUME_MULTIPLIER || 3);
const MIN_VOL = Number(process.env.MIN_TRIGGER_VOLUME || 50000);
const START_PT = Number(process.env.SCAN_START_HOUR_PT || 6);
const END_PT = Number(process.env.SCAN_END_HOUR_PT || 13);
const REFRESH_MS = Number(process.env.UNIVERSE_REFRESH_MS || 15000);
const COOLDOWN = Number(process.env.ALERT_COOLDOWN_SEC || 0);
const WS_URL = process.env.MASSIVE_WS_URL || "wss://socket.massive.com/stocks";
const TG_TOKEN = process.env.TELEGRAM_BOT_TOKEN || "";
const TG_CHAT = process.env.TELEGRAM_CHAT_ID || "";

if (!API_KEY) throw new Error("Set MASSIVE_API_KEY in Railway");

let universe = new Set(), info = new Map(), ws = null, subscribed = new Set();
const bars = new Map(), lastBar = new Map(), lastAlert = new Map();

function pt(ts = Date.now()) {
  return Object.fromEntries(new Intl.DateTimeFormat("en-US", {
    timeZone: "America/Los_Angeles", hour12: false,
    year: "numeric", month: "2-digit", day: "2-digit",
    hour: "2-digit", minute: "2-digit", second: "2-digit"
  }).formatToParts(new Date(ts)).map(x => [x.type, x.value]));
}
function active(ts = Date.now()) {
  const h = Number(pt(ts).hour);
  return h >= START_PT && h < END_PT;
}
function bucket30s(ts) { return Math.floor(ts / 30000) * 30000; }
function snapshotPrice(t) {
  return Number(t?.lastTrade?.p || t?.min?.c || t?.day?.c || t?.prevDay?.c || 0);
}
function snapshotPct(t) {
  if (Number.isFinite(Number(t?.todaysChangePerc))) return Number(t.todaysChangePerc);
  const p = snapshotPrice(t), prev = Number(t?.prevDay?.c || 0);
  return p > 0 && prev > 0 ? ((p - prev) / prev) * 100 : NaN;
}
async function fetchJson(url) {
  const r = await fetch(url);
  const body = await r.text();
  if (!r.ok) throw new Error(`${r.status} ${body.slice(0, 500)}`);
  return JSON.parse(body);
}

async function refreshUniverse() {
  if (!active()) return;
  try {
    const url = `https://api.massive.com/v2/snapshot/locale/us/markets/stocks/gainers?include_otc=false&apiKey=${encodeURIComponent(API_KEY)}`;
    const d = await fetchJson(url);
    const rows = Array.isArray(d?.tickers) ? d.tickers : [];
    const ranked = rows.map(t => ({
      ticker: String(t?.ticker || "").toUpperCase(),
      price: snapshotPrice(t),
      pct: snapshotPct(t)
    }))
    .filter(x => x.ticker && x.price >= PRICE_MIN && x.price <= PRICE_MAX && Number.isFinite(x.pct) && x.pct > 0)
    .sort((a,b) => b.pct - a.pct)
    .slice(0, LIMIT)
    .map((x,i) => ({...x, rank:i+1}));

    universe = new Set(ranked.map(x => x.ticker));
    info = new Map(ranked.map(x => [x.ticker, x]));
    updateSubscriptions();

    console.log(`[GAINERS] ${ranked.length}: ${ranked.map(x => `#${x.rank} ${x.ticker} +${x.pct.toFixed(1)}%`).join(" | ")}`);
  } catch (e) {
    console.error("[GAINERS]", e.message);
  }
}

function updateSubscriptions() {
  if (!ws || ws.readyState !== WebSocket.OPEN) return;
  const add = [...universe].filter(x => !subscribed.has(x));
  const del = [...subscribed].filter(x => !universe.has(x));
  if (del.length) {
    ws.send(JSON.stringify({action:"unsubscribe", params:del.map(x => `A.${x}`).join(",")}));
    del.forEach(x => subscribed.delete(x));
  }
  if (add.length) {
    ws.send(JSON.stringify({action:"subscribe", params:add.map(x => `A.${x}`).join(",")}));
    add.forEach(x => subscribed.add(x));
  }
}

function processSecondAggregate(m) {
  const sym = String(m?.sym || "").toUpperCase();
  const ts = Number(m?.s || Date.now());
  if (!sym || !universe.has(sym) || !active(ts)) return;

  const start = bucket30s(ts), key = `${sym}:${start}`;
  let b = bars.get(key);
  if (!b) {
    b = {sym, start, open:Number(m.o||0), high:Number(m.h||0), low:Number(m.l||0),
         close:Number(m.c||0), volume:Number(m.v||0), done:false};
    bars.set(key, b);
  } else {
    b.high = Math.max(b.high, Number(m.h || b.high));
    b.low = Math.min(b.low, Number(m.l || b.low));
    b.close = Number(m.c || b.close);
    b.volume += Number(m.v || 0);
  }

  const old = bars.get(`${sym}:${start - 30000}`);
  if (old && !old.done) {
    old.done = true;
    evaluateCompletedBar(old).catch(e => console.error(`[EVAL][${sym}]`, e.message));
  }

  for (const [k,v] of bars) {
    if (v.sym === sym && v.start < start - 120000) bars.delete(k);
  }
}

async function evaluateCompletedBar(b) {
  const prev = lastBar.get(b.sym);
  lastBar.set(b.sym, b);

  if (!prev || prev.start !== b.start - 30000) {
    console.log(`[BASELINE] ${b.sym} V=${Math.round(b.volume)}`);
    return;
  }

  const ratio = prev.volume > 0 ? b.volume / prev.volume : 0;
  const green = b.close > b.open;
  const i = info.get(b.sym);

  console.log(`[30S] ${b.sym} rank=${i?.rank || "?"} O=${b.open} C=${b.close} V=${Math.round(b.volume)} prev=${Math.round(prev.volume)} x=${ratio.toFixed(2)} green=${green} minVolPass=${b.volume >= MIN_VOL}`);

  if (!green) return;
  if (b.volume < MIN_VOL) return;
  if (prev.volume <= 0) return;
  if (ratio < MULT) return;

  const now = Date.now(), la = lastAlert.get(b.sym) || 0;
  if (COOLDOWN > 0 && now - la < COOLDOWN * 1000) return;
  lastAlert.set(b.sym, now);

  const q = pt(b.start);
  const text = `🚨 30-SEC VOLUME SPIKE
${b.sym} | Gainer Rank #${i?.rank || "?"}
Price: $${b.close.toFixed(2)}
Day: +${Number(i?.pct || 0).toFixed(2)}%
30s Volume: ${Math.round(b.volume).toLocaleString()}
Previous 30s: ${Math.round(prev.volume).toLocaleString()}
Spike: ${ratio.toFixed(2)}x
GREEN candle
${q.hour}:${q.minute}:${q.second} PT`;

  console.log("[ALERT]", text.replace(/\n/g, " | "));

  if (TG_TOKEN && TG_CHAT) {
    const r = await fetch(`https://api.telegram.org/bot${TG_TOKEN}/sendMessage`, {
      method:"POST", headers:{"content-type":"application/json"},
      body:JSON.stringify({chat_id:TG_CHAT, text})
    });
    if (!r.ok) console.error("[TELEGRAM]", r.status, (await r.text()).slice(0,300));
  }
}

function connectWebSocket() {
  ws = new WebSocket(WS_URL);

  ws.on("open", () => {
    subscribed.clear();
    ws.send(JSON.stringify({action:"auth", params:API_KEY}));
  });

  ws.on("message", raw => {
    let messages;
    try { messages = JSON.parse(raw.toString()); } catch { return; }
    if (!Array.isArray(messages)) messages = [messages];

    for (const m of messages) {
      if (m.ev === "status") {
        console.log("[WS]", m.status, m.message || "");
        if (m.status === "auth_success") updateSubscriptions();
      } else if (m.ev === "A") {
        processSecondAggregate(m);
      }
    }
  });

  ws.on("error", e => console.error("[WS]", e.message));
  ws.on("close", () => {
    subscribed.clear();
    console.log("[WS] closed; reconnecting in 3 seconds");
    setTimeout(connectWebSocket, 3000);
  });
}

http.createServer((req,res) => {
  res.setHeader("content-type","application/json");
  if (req.url === "/health") {
    res.end(JSON.stringify({
      ok:true,
      scanner:"SIMPLE_GAINERS_30SEC",
      universeCount:universe.size,
      websocket:ws?.readyState===WebSocket.OPEN?"open":"closed",
      config:{
        TOP_GAINERS_LIMIT:LIMIT, PRICE_MIN, PRICE_MAX,
        VOLUME_MULTIPLIER:MULT, MIN_TRIGGER_VOLUME:MIN_VOL,
        SCAN_START_HOUR_PT:START_PT, SCAN_END_HOUR_PT:END_PT,
        UNIVERSE_REFRESH_MS:REFRESH_MS, ALERT_COOLDOWN_SEC:COOLDOWN
      },
      note:"Massive official gainers endpoint is limited to top 20."
    }));
    return;
  }
  res.statusCode = 404;
  res.end(JSON.stringify({error:"not found"}));
}).listen(PORT, "0.0.0.0", () => console.log(`[HTTP] ${PORT}`));

console.log("SIMPLE GAINERS / 30-SECOND / VOLUME SPIKE SCANNER");
connectWebSocket();
refreshUniverse();
setInterval(refreshUniverse, REFRESH_MS);
