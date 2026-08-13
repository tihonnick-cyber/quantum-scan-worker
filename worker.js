const http = require("http");
const WebSocket = require("ws");

const API_KEY = process.env.POLYGON_API_KEY || process.env.MASSIVE_API_KEY;
const PORT = Number(process.env.PORT || 3000);
const LIMIT = Number(process.env.TOP_GAINERS_LIMIT || 50);
const PRICE_MIN = Number(process.env.PRICE_MIN || 0);
const PRICE_MAX = Number(process.env.PRICE_MAX || 10);
const MULT = Number(process.env.VOLUME_MULTIPLIER || 3);
const MIN_VOL = Number(process.env.MIN_TRIGGER_VOLUME || 0);
const START_PT = Number(process.env.SCAN_START_HOUR_PT || 6);
const END_PT = Number(process.env.SCAN_END_HOUR_PT || 13);
const REFRESH_MS = Number(process.env.UNIVERSE_REFRESH_MS || 15000);
const COOLDOWN = Number(process.env.ALERT_COOLDOWN_SEC || 0);
const WS_URL = process.env.MASSIVE_WS_URL || "wss://socket.massive.com/stocks";
const TG_TOKEN = process.env.TELEGRAM_BOT_TOKEN || "";
const TG_CHAT = process.env.TELEGRAM_CHAT_ID || "";

if (!API_KEY) throw new Error("Set POLYGON_API_KEY or MASSIVE_API_KEY");

let universe = new Set(), info = new Map(), ws, subscribed = new Set();
const bars = new Map(), lastBar = new Map(), lastAlert = new Map();

function pt(ts=Date.now()) {
  return Object.fromEntries(new Intl.DateTimeFormat("en-US", {
    timeZone:"America/Los_Angeles", hour12:false, year:"numeric", month:"2-digit",
    day:"2-digit", hour:"2-digit", minute:"2-digit", second:"2-digit"
  }).formatToParts(new Date(ts)).map(x=>[x.type,x.value]));
}
function active(ts=Date.now()) { const h=+pt(ts).hour; return h>=START_PT && h<END_PT; }
function bucket(ts) { return Math.floor(ts/30000)*30000; }
function price(t) { return +(t?.lastTrade?.p || t?.min?.c || t?.day?.c || 0); }
function pct(t) {
  const p=price(t), prev=+(t?.prevDay?.c||0);
  return p&&prev ? (p-prev)/prev*100 : NaN;
}
async function json(url) {
  const r=await fetch(url);
  if(!r.ok) throw new Error(`${r.status} ${await r.text()}`);
  return r.json();
}

async function refresh() {
  if(!active()) return;
  try {
    const d=await json(`https://api.massive.com/v2/snapshot/locale/us/markets/stocks/tickers?include_otc=false&apiKey=${encodeURIComponent(API_KEY)}`);
    const ranked=(d.tickers||[]).map(t=>({ticker:t.ticker,price:price(t),pct:pct(t)}))
      .filter(x=>x.ticker && x.price>=PRICE_MIN && x.price<=PRICE_MAX && Number.isFinite(x.pct) && x.pct>0)
      .sort((a,b)=>b.pct-a.pct).slice(0,LIMIT)
      .map((x,i)=>({...x,rank:i+1}));
    universe=new Set(ranked.map(x=>x.ticker));
    info=new Map(ranked.map(x=>[x.ticker,x]));
    subscriptions();
    console.log(`[TOP50] ${ranked.length}: ${ranked.slice(0,10).map(x=>`${x.ticker} +${x.pct.toFixed(1)}%`).join(" | ")}`);
  } catch(e) { console.error("[TOP50]",e.message); }
}

function subscriptions() {
  if(!ws || ws.readyState!==WebSocket.OPEN) return;
  const add=[...universe].filter(x=>!subscribed.has(x));
  const del=[...subscribed].filter(x=>!universe.has(x));
  if(del.length) {
    ws.send(JSON.stringify({action:"unsubscribe",params:del.map(x=>`A.${x}`).join(",")}));
    del.forEach(x=>subscribed.delete(x));
  }
  if(add.length) {
    ws.send(JSON.stringify({action:"subscribe",params:add.map(x=>`A.${x}`).join(",")}));
    add.forEach(x=>subscribed.add(x));
  }
}

function second(m) {
  if(!universe.has(m.sym) || !active(m.s||Date.now())) return;
  const start=bucket(+m.s), key=`${m.sym}:${start}`;
  let b=bars.get(key);
  if(!b) {
    b={sym:m.sym,start,open:+m.o,high:+m.h,low:+m.l,close:+m.c,volume:+(m.v||0)};
    bars.set(key,b);
  } else {
    b.high=Math.max(b.high,+m.h); b.low=Math.min(b.low,+m.l);
    b.close=+m.c; b.volume+=+(m.v||0);
  }
  const old=bars.get(`${m.sym}:${start-30000}`);
  if(old && !old.done) { old.done=true; evaluate(old); }
  for(const [k,v] of bars) if(v.sym===m.sym && v.start<start-120000) bars.delete(k);
}

async function evaluate(b) {
  const prev=lastBar.get(b.sym);
  lastBar.set(b.sym,b);
  if(!prev || prev.start!==b.start-30000) {
    console.log(`[BASELINE] ${b.sym} V=${b.volume}`);
    return;
  }
  const ratio=prev.volume>0 ? b.volume/prev.volume : 0;
  const green=b.close>b.open;
  const i=info.get(b.sym);
  console.log(`[30S] ${b.sym} rank=${i?.rank||"?"} O=${b.open} C=${b.close} V=${b.volume} prev=${prev.volume} x=${ratio.toFixed(2)} green=${green}`);

  if(!green || b.volume<MIN_VOL || prev.volume<=0 || ratio<MULT) return;
  const now=Date.now(), la=lastAlert.get(b.sym)||0;
  if(COOLDOWN>0 && now-la<COOLDOWN*1000) return;
  lastAlert.set(b.sym,now);

  const q=pt(b.start);
  const text=`🚨 30-SEC VOLUME SPIKE
${b.sym} | Rank #${i?.rank||"?"}
Price: $${b.close.toFixed(2)}
Day: +${(i?.pct||0).toFixed(2)}%
Volume: ${Math.round(b.volume).toLocaleString()}
Previous: ${Math.round(prev.volume).toLocaleString()}
Spike: ${ratio.toFixed(2)}x
GREEN candle
${q.hour}:${q.minute}:${q.second} PT`;
  console.log("[ALERT]",text.replace(/\n/g," | "));
  if(TG_TOKEN && TG_CHAT) {
    try {
      await fetch(`https://api.telegram.org/bot${TG_TOKEN}/sendMessage`,{
        method:"POST",headers:{"content-type":"application/json"},
        body:JSON.stringify({chat_id:TG_CHAT,text})
      });
    } catch(e) { console.error("[TELEGRAM]",e.message); }
  }
}

function connect() {
  ws=new WebSocket(WS_URL);
  ws.on("open",()=>{ subscribed.clear(); ws.send(JSON.stringify({action:"auth",params:API_KEY})); });
  ws.on("message",raw=>{
    let a; try{a=JSON.parse(raw.toString())}catch{return}
    if(!Array.isArray(a)) a=[a];
    for(const m of a) {
      if(m.ev==="status") {
        console.log("[WS]",m.status,m.message||"");
        if(m.status==="auth_success") subscriptions();
      } else if(m.ev==="A") second(m);
    }
  });
  ws.on("error",e=>console.error("[WS]",e.message));
  ws.on("close",()=>{ subscribed.clear(); console.log("[WS] reconnect in 3 sec"); setTimeout(connect,3000); });
}

http.createServer((req,res)=>{
  res.setHeader("content-type","application/json");
  if(req.url==="/health") return res.end(JSON.stringify({
    ok:true,scanner:"SIMPLE_TOP50_30SEC_3X",top50:universe.size,
    websocket:ws?.readyState===WebSocket.OPEN?"open":"closed",
    LIMIT,PRICE_MIN,PRICE_MAX,MULT,MIN_VOL,START_PT,END_PT
  }));
  res.statusCode=404; res.end(JSON.stringify({error:"not found"}));
}).listen(PORT,()=>console.log(`[HTTP] ${PORT}`));

console.log("SIMPLE TOP-50 / 30-SECOND / VOLUME SPIKE SCANNER");
connect(); refresh(); setInterval(refresh,REFRESH_MS);
