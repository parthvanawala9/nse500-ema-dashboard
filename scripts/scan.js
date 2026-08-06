#!/usr/bin/env node
'use strict';

// ============================================================================
// NSE 500 SMA 44/100/200 Scanner
//
// Standalone replacement for the n8n workflow. Run by a GitHub Actions
// cron job (see .github/workflows/scan.yml) - no server, no n8n, no cost.
//
// Reads/writes data/scanner.json and data/history.json directly in the
// checked-out repo; the Actions workflow commits + pushes them afterward.
// ============================================================================

const fs = require('fs');
const path = require('path');

const ROOT = path.join(__dirname, '..');
const DATA_DIR = path.join(ROOT, 'data');
const SCANNER_PATH = path.join(DATA_DIR, 'scanner.json');
const HISTORY_PATH = path.join(DATA_DIR, 'history.json');

const HEADERS_CSV = { 'User-Agent': 'Mozilla/5.0', Accept: 'text/csv' };
const HEADERS_JSON = { 'User-Agent': 'Mozilla/5.0', Accept: 'application/json' };

const BATCH_SIZE = 5;
const BATCH_DELAY_MS = 4000;
const YAHOO_RANGE = '300d';
const MIN_BARS = 200; // true minimum: SMA200 needs 200 bars
const MIN_OK_STOCKS = 20;
const MAX_HISTORY_DAYS = 365;

// ---------------------------------------------------------------------------
// Fetch helpers
// ---------------------------------------------------------------------------

async function fetchText(url, headers) {
  try {
    const res = await fetch(url, { headers });
    if (!res.ok) return null;
    return await res.text();
  } catch (e) {
    return null;
  }
}

async function fetchJson(url, headers) {
  try {
    const res = await fetch(url, { headers });
    if (!res.ok) return null;
    return await res.json();
  } catch (e) {
    return null;
  }
}

// ---------------------------------------------------------------------------
// CSV parsing (same shape as the old "Parse CSV1" / parseSymbols nodes)
// ---------------------------------------------------------------------------

function parseNifty500Csv(csv) {
  const rows = csv.trim().split('\n');
  const out = [];
  for (let i = 1; i < rows.length; i++) {
    const cols = rows[i].split(',');
    if (cols.length >= 3) {
      out.push({
        company: cols[0].replace(/"/g, ''),
        industry: cols[1].replace(/"/g, ''),
        symbol: cols[2].replace(/"/g, '').trim() + '.NS',
      });
    }
  }
  return out;
}

function parseSymbolSet(csv) {
  const set = new Set();
  if (!csv) return set;
  const rows = csv.trim().split('\n');
  for (let i = 1; i < rows.length; i++) {
    const cols = rows[i].split(',');
    if (cols.length >= 3) {
      set.add(cols[2].replace(/"/g, '').trim() + '.NS');
    }
  }
  return set;
}

// ---------------------------------------------------------------------------
// Signal calculation (same logic as the n8n "Signal Calculation" node)
// ---------------------------------------------------------------------------

function sma(closes, period, offsetFromEnd) {
  const n = closes.length - 1;
  const end = n - offsetFromEnd;
  const start = end - period + 1;
  if (start < 0) return null;
  let sum = 0;
  for (let i = start; i <= end; i++) sum += closes[i];
  return Number((sum / period).toFixed(2));
}

async function computeSignal(stockMeta) {
  const { symbol, company, industry } = stockMeta;
  const url = `https://query1.finance.yahoo.com/v8/finance/chart/${symbol}?range=${YAHOO_RANGE}&interval=1d`;
  const data = await fetchJson(url, HEADERS_JSON);

  if (!data || !data.chart) {
    return { error: { symbol, reason: 'No chart data returned' } };
  }
  const result = data.chart.result && data.chart.result[0];
  if (!result) {
    return { error: { symbol, reason: 'Empty chart.result from Yahoo Finance' } };
  }

  const quote = result.indicators && result.indicators.quote && result.indicators.quote[0];
  const rawClose = (quote && quote.close) || [];
  const rawOpen = (quote && quote.open) || [];
  const rawLow = (quote && quote.low) || [];

  const closes = [];
  const opens = [];
  const lows = [];
  for (let i = 0; i < rawClose.length; i++) {
    if (rawClose[i] != null && rawOpen[i] != null && rawLow[i] != null) {
      closes.push(rawClose[i]);
      opens.push(rawOpen[i]);
      lows.push(rawLow[i]);
    }
  }

  if (closes.length < MIN_BARS) {
    return { error: { symbol, reason: `Insufficient history (${closes.length} bars, need ${MIN_BARS}+)` } };
  }

  const n = closes.length - 1;
  const latestClose = closes[n];
  const latestOpen = opens[n];
  const latestLow = lows[n];
  const prevClose = closes[n - 1];

  const sma44 = sma(closes, 44, 0);
  const sma44Prev = sma(closes, 44, 1);
  const sma44_10dAgo = sma(closes, 44, 10);
  const sma100 = sma(closes, 100, 0);
  const sma200 = sma(closes, 200, 0);

  if (sma44 === null || sma44Prev === null || sma44_10dAgo === null || sma100 === null || sma200 === null) {
    return { error: { symbol, reason: 'Could not compute one or more SMAs (not enough history)' } };
  }

  const buyConditions =
    latestClose > sma44 &&
    latestLow < sma44 &&
    sma44 > sma44_10dAgo &&
    sma44 > sma100 &&
    sma100 > sma200 &&
    latestClose > latestOpen;

  const sellConditions = prevClose > sma44Prev && latestClose < sma44;

  let signal = 'HOLD';
  if (buyConditions) signal = 'BUY';
  else if (sellConditions) signal = 'SELL';

  return {
    stock: {
      SYMBOLL: (result.meta && result.meta.symbol) || symbol,
      COMPANY: company,
      INDUSTRY: industry,
      PRICE: latestClose,
      OPEN: latestOpen,
      LOW: latestLow,
      SMA44: sma44,
      SMA44_10DAGO: sma44_10dAgo,
      SMA100: sma100,
      SMA200: sma200,
      Signal: signal,
      'Market Cap': '',
      'Last Updated': new Date().toLocaleString('en-IN'),
    },
  };
}

async function processBatched(items, worker, batchSize, delayMs) {
  const results = [];
  for (let i = 0; i < items.length; i += batchSize) {
    const batch = items.slice(i, i + batchSize);
    const batchResults = await Promise.all(batch.map(worker));
    results.push(...batchResults);
    if (i + batchSize < items.length) {
      await new Promise((r) => setTimeout(r, delayMs));
    }
    if ((i / batchSize) % 10 === 0) {
      console.log(`  ...${Math.min(i + batchSize, items.length)}/${items.length} symbols processed`);
    }
  }
  return results;
}

// ---------------------------------------------------------------------------
// Scanner JSON building (same logic as "Create Scanner JSON1")
// ---------------------------------------------------------------------------

function organizeCapGroup(list, newBuySymbols, newSellSymbols) {
  function bucket(signal) {
    return list
      .filter((s) => s.Signal === signal)
      .sort((a, b) => {
        const aNew = newBuySymbols.has(a.SYMBOLL) || newSellSymbols.has(a.SYMBOLL) ? 1 : 0;
        const bNew = newBuySymbols.has(b.SYMBOLL) || newSellSymbols.has(b.SYMBOLL) ? 1 : 0;
        if (bNew !== aNew) return bNew - aNew;
        return a.SYMBOLL.localeCompare(b.SYMBOLL);
      });
  }
  return { buy: bucket('BUY'), hold: bucket('HOLD'), sell: bucket('SELL') };
}

function readJsonSafe(filePath, fallback) {
  try {
    const raw = fs.readFileSync(filePath, 'utf-8');
    return JSON.parse(raw);
  } catch (e) {
    return fallback;
  }
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main() {
  console.log('Fetching index lists (Nifty100 / Midcap150 / Nifty500)...');
  const [nifty100Csv, midcap150Csv, nifty500Csv] = await Promise.all([
    fetchText('https://archives.nseindia.com/content/indices/ind_nifty100list.csv', HEADERS_CSV),
    fetchText('https://archives.nseindia.com/content/indices/ind_niftymidcap150list.csv', HEADERS_CSV),
    fetchText('https://archives.nseindia.com/content/indices/ind_nifty500list.csv', HEADERS_CSV),
  ]);

  if (!nifty500Csv) {
    throw new Error('Failed to fetch the Nifty500 list - aborting run.');
  }

  const largeSet = parseSymbolSet(nifty100Csv);
  const midSet = parseSymbolSet(midcap150Csv);
  const stockList = parseNifty500Csv(nifty500Csv);

  console.log(`Fetched ${stockList.length} symbols. Fetching + computing signals (batched)...`);
  const results = await processBatched(stockList, computeSignal, BATCH_SIZE, BATCH_DELAY_MS);

  const stocks = [];
  const errors = [];
  for (const r of results) {
    if (r.stock) stocks.push(r.stock);
    else if (r.error) errors.push(r.error);
  }

  const okCount = stocks.length;
  const skippedCount = errors.length;
  console.log(`Scanner run: ${okCount} stocks OK, ${skippedCount} skipped.`);
  if (errors.length > 0) {
    console.log('Skipped symbols (first 20):', JSON.stringify(errors.slice(0, 20), null, 2));
  }

  if (okCount < MIN_OK_STOCKS) {
    throw new Error(
      `Aborting write: only ${okCount}/${okCount + skippedCount} stocks succeeded ` +
        `(need at least ${MIN_OK_STOCKS}). Not overwriting scanner.json/history.json. ` +
        `First few errors: ${JSON.stringify(errors.slice(0, 5))}`
    );
  }

  const stats = { buy: 0, sell: 0, hold: 0 };
  for (const s of stocks) {
    if (s.Signal === 'BUY') stats.buy++;
    else if (s.Signal === 'SELL') stats.sell++;
    else stats.hold++;
  }

  const marketBreadth = { sma44: 0, sma100: 0, sma200: 0 };
  for (const s of stocks) {
    if (s.PRICE > s.SMA44) marketBreadth.sma44++;
    if (s.PRICE > s.SMA100) marketBreadth.sma100++;
    if (s.PRICE > s.SMA200) marketBreadth.sma200++;
  }

  const prevScanner = readJsonSafe(SCANNER_PATH, null);
  const previousStocksBySymbol = {};
  if (prevScanner && Array.isArray(prevScanner.stocks)) {
    for (const s of prevScanner.stocks) {
      if (s && s.SYMBOLL) previousStocksBySymbol[s.SYMBOLL] = s;
    }
  }

  const newBuy = stocks.filter((s) => {
    const prev = previousStocksBySymbol[s.SYMBOLL];
    return s.Signal === 'BUY' && (!prev || prev.Signal !== 'BUY');
  });
  const newSell = stocks.filter((s) => {
    const prev = previousStocksBySymbol[s.SYMBOLL];
    return s.Signal === 'SELL' && (!prev || prev.Signal !== 'SELL');
  });
  const newBuySymbols = new Set(newBuy.map((s) => s.SYMBOLL));
  const newSellSymbols = new Set(newSell.map((s) => s.SYMBOLL));

  const largeCapRaw = [];
  const midCapRaw = [];
  const smallCapRaw = [];
  for (const s of stocks) {
    if (largeSet.has(s.SYMBOLL)) largeCapRaw.push(s);
    else if (midSet.has(s.SYMBOLL)) midCapRaw.push(s);
    else smallCapRaw.push(s);
  }

  const scanner = {
    updatedAt: new Date().toISOString(),
    stats,
    marketBreadth,
    newBuy,
    newSell,
    stocks,
    largeCap: organizeCapGroup(largeCapRaw, newBuySymbols, newSellSymbols),
    midCap: organizeCapGroup(midCapRaw, newBuySymbols, newSellSymbols),
    smallCap: organizeCapGroup(smallCapRaw, newBuySymbols, newSellSymbols),
    errors,
  };

  // -------------------------------------------------------------------
  // History (self-healing: checks history.json's own open positions,
  // not a comparison against the previous scanner snapshot)
  // -------------------------------------------------------------------
  const today = new Date().toISOString().slice(0, 10);
  let previousHistory = readJsonSafe(HISTORY_PATH, []);
  if (!Array.isArray(previousHistory)) previousHistory = [];

  previousHistory = previousHistory.filter((e) => e.date !== today);
  previousHistory.sort((a, b) => (a.date < b.date ? -1 : a.date > b.date ? 1 : 0));

  const openBuyBySymbol = {};
  for (const day of previousHistory) {
    for (const b of day.buy) openBuyBySymbol[b.symbol] = b;
    for (const s of day.sell) delete openBuyBySymbol[s.symbol];
  }

  const todayBuy = [];
  const todaySell = [];
  for (const s of stocks) {
    if (s.Signal === 'BUY' && !openBuyBySymbol[s.SYMBOLL]) {
      todayBuy.push({ symbol: s.SYMBOLL, price: s.PRICE });
    } else if (s.Signal === 'SELL' && openBuyBySymbol[s.SYMBOLL]) {
      todaySell.push({ symbol: s.SYMBOLL, price: s.PRICE });
    }
  }
  previousHistory.push({ date: today, buy: todayBuy, sell: todaySell });

  const openBuyFinal = {};
  for (const day of previousHistory) {
    for (const b of day.buy) openBuyFinal[b.symbol] = b;
    for (const s of day.sell) {
      const openBuy = openBuyFinal[s.symbol];
      if (openBuy) {
        s.entryPrice = openBuy.price;
        s.pnlPercent = Number((((s.price - openBuy.price) / openBuy.price) * 100).toFixed(2));
      }
      delete openBuyFinal[s.symbol];
    }
  }

  const latestPriceBySymbol = {};
  for (const s of stocks) latestPriceBySymbol[s.SYMBOLL] = s.PRICE;

  for (const day of previousHistory) {
    for (const b of day.buy) {
      delete b.currentPrice;
      delete b.pnlPercent;
      delete b.asOfDate;
    }
  }
  for (const symbol in openBuyFinal) {
    const b = openBuyFinal[symbol];
    if (latestPriceBySymbol[symbol] !== undefined) {
      b.currentPrice = latestPriceBySymbol[symbol];
      b.pnlPercent = Number((((b.currentPrice - b.price) / b.price) * 100).toFixed(2));
      b.asOfDate = today;
    }
  }

  if (previousHistory.length > MAX_HISTORY_DAYS) {
    previousHistory = previousHistory.slice(previousHistory.length - MAX_HISTORY_DAYS);
  }

  fs.mkdirSync(DATA_DIR, { recursive: true });
  fs.writeFileSync(SCANNER_PATH, JSON.stringify(scanner, null, 2));
  fs.writeFileSync(HISTORY_PATH, JSON.stringify(previousHistory, null, 2));

  console.log('Done. data/scanner.json and data/history.json written.');
}

main().catch((err) => {
  console.error('FATAL:', err.message);
  process.exit(1);
});
