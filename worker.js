const express = require("express");
const { Pool } = require("pg");

const POLYGON_KEY = process.env.POLYGON_KEY;
const DATABASE_URL = process.env.DATABASE_URL;

const pool = new Pool({
  connectionString: DATABASE_URL,
  ssl: { rejectUnauthorized: false },
});

const app = express();
let isScanning = false;

app.use(express.json());

/* =========================
   BASIC STATUS ROUTE
========================= */
app.get("/", (req, res) => {
  res.send("Quantum Scan Worker is running");
});

/* =========================
   TEST INSERT ROUTE
========================= */
app.get("/test", async (req, res) => {
  try {
    const result = await pool.query(`
      INSERT INTO alerts (ticker, price, percent_change, rvol, float, news, created_at)
      VALUES ('TEST', 5.25, 12.5, 300000, 2000000, true, now())
      RETURNING *;
    `);

    res.json({
      success: true,
      alert: result.rows[0],
    });

  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

/* =========================
   GET ALERTS ROUTE
========================= */
app.get("/alerts", async (req, res) => {
  try {
    const result = await pool.query(
      "SELECT * FROM alerts ORDER BY created_at DESC LIMIT 100"
    );

    res.json(result.rows);

  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

/* =========================
   SAVE ALERT FUNCTION
========================= */
async function saveAlert(ticker, price, change, volume) {
  try {

    await pool.query(`
      INSERT INTO alerts (
        ticker,
        price,
        percent_change,
        rvol,
        float,
        news,
        created_at
      )
      VALUES ($1, $2, $3, $4, 0, false, now())
      ON CONFLICT DO NOTHING;
    `, [ticker, price, change, volume]);

  } catch (err) {
    console.error("DB save error:", err.message);
  }
}

/* =========================
   MARKET SCANNER
========================= */
async function scan() {
  if (isScanning) return;
  isScanning = true;
  const started = Date.now();

  try {

    console.log("Scanning market...");

    const response = await fetch(
      `https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers?apiKey=${POLYGON_KEY}`
    );

    const data = await response.json();

    const tickers = data?.tickers || [];

    const matches = tickers.filter((ticker) => {

      const price = ticker?.lastTrade?.p ?? 0;
      const change = ticker?.todaysChangePerc ?? 0;
      const volume = ticker?.day?.v ?? 0;

      return (
        price >= 2 &&
        price <= 20 &&
        change >= 10 &&
        volume >= 500000
      );

    });

    console.log("Matches:", matches.map(t => t.ticker));

    for (const ticker of matches) {

      await saveAlert(
        ticker.ticker,
        ticker.lastTrade?.p ?? 0,
        ticker.todaysChangePerc ?? 0,
        ticker.day?.v ?? 0
      );

    
  } catch (err) {
  console.error("Scan error:", err.message);
} finally {
  console.log("Scan duration (ms):", Date.now() - started);
  isScanning = false;
}

}

/* =========================
   RUN SCANNER EVERY 15s
========================= */
scan();
setInterval(scan, 15000);

/* =========================
   START SERVER
========================= */
const PORT = process.env.PORT || 8080;

app.listen(PORT, "0.0.0.0", () => {
  console.log("Server running on port", PORT);
});
