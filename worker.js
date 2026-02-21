import express from "express";
import pkg from "pg";

const { Pool } = pkg;

const POLYGON_KEY = process.env.POLYGON_KEY;
const DATABASE_URL = process.env.DATABASE_URL;

const pool = new Pool({
  connectionString: DATABASE_URL,
  ssl: { rejectUnauthorized: false }
});

const app = express();
app.use(express.json());

async function scan() {
  try {
    console.log("Scanning market...");

    const response = await fetch(
      `https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers?apiKey=${POLYGON_KEY}`
    );

    const data = await response.json();

    if (!data.tickers) {
      console.log("No ticker data received");
      return;
    }

    const matches = data.tickers.filter(ticker => {
      const price = ticker.lastTrade?.p || 0;
      const change = ticker.todaysChangePerc || 0;
      const volume = ticker.day?.v || 0;

      return (
        price >= 2 &&
        price <= 4 &&
        change >= 10 &&
        volume >= 500000
      );
    });

    console.log("Matches:", matches.map(t => t.ticker));

    for (const ticker of matches) {
      await pool.query(
        `INSERT INTO alerts (ticker, price, change_percent, volume, created_at)
         VALUES ($1, $2, $3, $4, NOW())`,
        [
          ticker.ticker,
          ticker.lastTrade?.p || 0,
          ticker.todaysChangePerc || 0,
          ticker.day?.v || 0
        ]
      );
    }

  } catch (err) {
    console.error("Scan error:", err);
  }
}

setInterval(scan, 60000);
scan();

app.get("/alerts", async (req, res) => {
  const result = await pool.query(
    "SELECT * FROM alerts ORDER BY created_at DESC LIMIT 50"
  );
  res.json(result.rows);
});

const PORT = process.env.PORT || 8080;
app.listen(PORT, () => {
  console.log("Server running on port", PORT);
});
