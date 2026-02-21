const POLYGON_KEY = process.env.POLYGON_KEY;

async function scan() {
  try {
    console.log("Scanning market...");

    const response = await fetch(
      `https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers?apiKey=${POLYGON_KEY}`
    );

    const data = await response.json();

    const results = (data.tickers || []).filter(ticker => {
      const price = ticker.lastTrade?.p || 0;
      const change = ticker.todaysChangePerc || 0;
      const volume = ticker.day?.v || 0;
      const float = ticker.shareClassSharesOutstanding || 999999999;

      return (
        price >= 2 &&
        price <= 4 &&
        change >= 10 &&
        volume >= 500000 &&
        float <= 5000000
      );
    });

    console.log("Matches:", results.map(r => r.ticker));

  } catch (err) {
    console.error(err);
  }
}

setInterval(scan, 60000);
scan();
