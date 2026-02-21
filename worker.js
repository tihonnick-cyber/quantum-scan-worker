const express = require("express");
const { Pool } = require("pg");

const POLYGON_KEY = process.env.POLYGON_KEY;
const DATABASE_URL = process.env.DATABASE_URL;

const pool = new Pool({
  connectionString: DATABASE_URL,
  ssl: {
    rejectUnauthorized: false,
  },
});

const app = express();

app.get("/", (req, res) => {
  res.send("Quantum Scan Worker is running ✅");
});

app.get("/alerts", async (req, res) => {
  try {
    const result = await pool.query(
      "SELECT * FROM alerts ORDER BY created_at DESC LIMIT 50"
    );
    res.json(result.rows);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

async function saveAlert(ticker, price, change, volume) {
  try {
    await pool.query(
      `
      INSERT INTO alerts (ticker, price, change_percent, volume)
      VALUES ($1, $2, $3, $4)
      ON CONFLICT DO NOTHING
      `,
      [ticker, price, change, volume]
    );
  } catch (err) {
    console.error("DB save error:", err.message);
  }
}

async function scan() {
  try {
    console.log("Scanning market...");

    const response = await fetch(
      `https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers?apiKey=${POLYGON_KEY}`
    );

    const data = await response.json();

    const tickers = data?.tickers || [];

    const results = tickers.filter((ticker) => {
      const price = ticker?.lastTrade?.p ?? 0;
      const change = ticker?.todaysChangePerc ?? 0;
      const volume = ticker?.day?.v ?? 0;

      return (
        price >= 2 &&
        price <= 4 &&
        change >= 10 &&
        volume >= 500000
      );
    });

    console.log("Matches:", results.map(r => r.ticker));

    for (const ticker of results) {
      await saveAlert(
        ticker.ticker,
        ticker.lastTrade?.p ?? 0,
        ticker.todaysChangePerc ?? 0,
        ticker.day?.v ?? 0
      );
    }

  } catch (err) {
    console.error("Scan error:", err.message);
  }
}

scan();
setInterval(scan, 60000);

const PORT = process.env.PORT || 8080;

app.listen(PORT, "0.0.0.0", () => {
  console.log("Server running on port", PORT);
});
