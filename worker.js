const express = require("express");

const POLYGON_KEY = process.env.POLYGON_KEY;

const app = express();

app.get("/", (req, res) => {
  res.status(200).send("Quantum Scan Worker is running ✅");
});

app.get("/health", (req, res) => {
  res.status(200).json({ ok: true });
});

// Temporary endpoint so Railway has something to hit
app.get("/alerts", (req, res) => {
  res.status(200).json([]);
});

async function scan() {
  try {
    console.log("Scanning market...");

    if (!POLYGON_KEY) {
      console.log("Missing POLYGON_KEY env var");
      return;
    }

    const url =
      `https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers?apiKey=${POLYGON_KEY}`;

    const response = await fetch(url);
    const data = await response.json();

    const tickers = Array.isArray(data?.tickers) ? data.tickers : [];

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
  } catch (err) {
    console.error("Scan error:", err);
  }
}

// Run once on boot, then every 60s
scan();
setInterval(scan, 60000);

// IMPORTANT: listen on Railway's port
const PORT = process.env.PORT || 8080;
app.listen(PORT, "0.0.0.0", () => {
  console.log(`Server listening on port ${PORT}`);
});
