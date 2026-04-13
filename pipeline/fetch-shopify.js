/**
 * Shopify â Firebase Data Pipeline
 *
 * Pulls orders, revenue, and product data from Shopify Admin API
 * and writes aggregated metrics to Firebase Realtime Database.
 *
 * Runs via GitHub Actions on a daily schedule.
 *
 * ENV VARS REQUIRED (set as GitHub Secrets):
 *   FIREBASE_DB_URL     - Firebase Realtime Database URL
 *   FIREBASE_API_KEY    - Firebase Web API key
 *   FIREBASE_EMAIL      - Firebase auth email (service account)
 *   FIREBASE_PASSWORD   - Firebase auth password
 *   BRANDS_CONFIG       - JSON string of brand configs with Shopify tokens
 *
 * BRANDS_CONFIG format:
 * {
 *   "uspa_au": {
 *     "shopifyStore": "e6c1b4-3.myshopify.com",
 *     "shopifyClientId": "your_client_id",
 *     "shopifyClientSecret": "shpss_xxxxx",
 *     "firebasePath": "uspa_au"
 *   }
 * }
 */

const https = require('https');

// ââ Config ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ

const FIREBASE_DB_URL = process.env.FIREBASE_DB_URL;
const FIREBASE_API_KEY = process.env.FIREBASE_API_KEY;
const FIREBASE_EMAIL = process.env.FIREBASE_EMAIL;
const FIREBASE_PASSWORD = process.env.FIREBASE_PASSWORD;
const BRANDS_CONFIG = JSON.parse(process.env.BRANDS_CONFIG || '{}');

// ââ HTTP Helper âââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ

function httpRequest(url, options = {}) {
  return new Promise((resolve, reject) => {
    const urlObj = new URL(url);
    const reqOptions = {
      hostname: urlObj.hostname,
      port: urlObj.port || 443,
      path: urlObj.pathname + urlObj.search,
      method: options.method || 'GET',
      headers: options.headers || {}
    };

    const req = https.request(reqOptions, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        try {
          resolve({ status: res.statusCode, data: JSON.parse(data), headers: res.headers });
        } catch (e) {
          resolve({ status: res.statusCode, data: data, headers: res.headers });
        }
      });
    });

    req.on('error', reject);
    if (options.body) req.write(typeof options.body === 'string' ? options.body : JSON.stringify(options.body));
    req.end();
  });
}

// ââ Firebase Auth âââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ

async function getFirebaseToken() {
  const res = await httpRequest(
    `https://identitytoolkit.googleapis.com/v1/accounts:signInWithPassword?key=${FIREBASE_API_KEY}`,
    {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ email: FIREBASE_EMAIL, password: FIREBASE_PASSWORD, returnSecureToken: true })
    }
  );
  if (!res.data.idToken) throw new Error('Firebase auth failed: ' + JSON.stringify(res.data));
  return res.data.idToken;
}

async function writeToFirebase(path, data, token) {
  const url = `${FIREBASE_DB_URL}/${path}.json?auth=${token}`;
  const res = await httpRequest(url, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(data)
  });
  if (res.status !== 200) throw new Error(`Firebase write failed (${res.status}): ${JSON.stringify(res.data)}`);
  return res.data;
}

// ââ Shopify OAuth (Client Credentials) ââââââââââââââââââââââââââââââââââââââ

async function getShopifyAccessToken(store, clientId, clientSecret) {
  console.log(`   Obtaining access token for ${store} via client credentials...`);
  const url = `https://${store}/admin/oauth/access_token`;
  const res = await httpRequest(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      client_id: clientId,
      client_secret: clientSecret,
      grant_type: 'client_credentials'
    })
  });
  if (res.status !== 200 || !res.data.access_token) {
    throw new Error(`Shopify OAuth failed (${res.status}): ${JSON.stringify(res.data)}`);
  }
  console.log(`   â Access token obtained`);
  return res.data.access_token;
}

// ââ Shopify API âââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ

async function shopifyGet(store, token, endpoint, params = {}) {
  const query = Object.entries(params).map(([k, v]) => `${k}=${encodeURIComponent(v)}`).join('&');
  const url = `https://${store}/admin/api/2024-01/${endpoint}.json${query ? '?' + query : ''}`;
  const res = await httpRequest(url, {
    headers: { 'X-Shopify-Access-Token': token, 'Content-Type': 'application/json' }
  });
  if (res.status !== 200) throw new Error(`Shopify API error (${res.status}) for ${endpoint}: ${JSON.stringify(res.data)}`);
  return res.data;
}

async function fetchAllOrders(store, token, sinceDate, untilDate) {
  let allOrders = [];
  let pageInfo = null;
  let hasMore = true;

  while (hasMore) {
    let params;
    if (pageInfo) {
      params = { limit: 250, page_info: pageInfo };
    } else {
      params = {
        limit: 250,
        status: 'any',
        financial_status: 'paid,partially_refunded',
        created_at_min: sinceDate,
        created_at_max: untilDate,
        fields: 'id,created_at,total_price,subtotal_price,total_discounts,line_items,financial_status,discount_codes,total_tax'
      };
    }

    const url = pageInfo
      ? `https://${store}/admin/api/2024-01/orders.json?limit=250&page_info=${pageInfo}`
      : `https://${store}/admin/api/2024-01/orders.json?${Object.entries(params).map(([k, v]) => `${k}=${encodeURIComponent(v)}`).join('&')}`;

    const res = await httpRequest(url, {
      headers: { 'X-Shopify-Access-Token': token, 'Content-Type': 'application/json' }
    });

    if (res.status !== 200) throw new Error(`Shopify orders error: ${res.status}`);

    const orders = res.data.orders || [];
    allOrders = allOrders.concat(orders);

    // Check for pagination
    const linkHeader = res.headers.link || '';
    const nextMatch = linkHeader.match(/<[^>]*page_info=([^>&]+)[^>]*>;\s*rel="next"/);
    if (nextMatch) {
      pageInfo = nextMatch[1];
    } else {
      hasMore = false;
    }

    // Rate limiting: Shopify allows 2 req/sec
    await new Promise(r => setTimeout(r, 600));
  }

  return allOrders;
}

// ââ Data Aggregation ââââââââââââââââââââââââââââââââââââââââââââââââââââââââ

function aggregateByMonth(orders) {
  const months = {};

  orders.forEach(order => {
    const date = new Date(order.created_at);
    const key = `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}`;

    if (!months[key]) {
      months[key] = {
        month: key,
        orders: 0,
        revenue: 0,
        units: 0,
        totalDiscount: 0,
        totalTax: 0,
        aovSum: 0
      };
    }

    const m = months[key];
    const revenue = parseFloat(order.total_price) || 0;
    const discount = parseFloat(order.total_discounts) || 0;
    const units = (order.line_items || []).reduce((sum, li) => sum + (li.quantity || 0), 0);

    m.orders++;
    m.revenue += revenue;
    m.units += units;
    m.totalDiscount += discount;
    m.totalTax += parseFloat(order.total_tax) || 0;
  });

  // Calculate derived metrics
  Object.values(months).forEach(m => {
    m.aov = m.orders > 0 ? Math.round((m.revenue / m.orders) * 100) / 100 : 0;
    m.upo = m.orders > 0 ? Math.round((m.units / m.orders) * 10) / 10 : 0;
    m.avgDiscount = m.revenue > 0 ? Math.round((m.totalDiscount / (m.revenue + m.totalDiscount)) * 1000) / 10 : 0;
    m.revenue = Math.round(m.revenue * 100) / 100;
    m.totalDiscount = Math.round(m.totalDiscount * 100) / 100;
  });

  return months;
}

function aggregateByWeek(orders) {
  const weeks = {};

  orders.forEach(order => {
    const date = new Date(order.created_at);
    // ISO week: Monday-based
    const dayOfWeek = date.getDay() || 7;
    const monday = new Date(date);
    monday.setDate(date.getDate() - dayOfWeek + 1);
    const key = monday.toISOString().slice(0, 10);

    if (!weeks[key]) {
      weeks[key] = { weekStart: key, orders: 0, revenue: 0, units: 0, totalDiscount: 0 };
    }

    const w = weeks[key];
    w.orders++;
    w.revenue += parseFloat(order.total_price) || 0;
    w.units += (order.line_items || []).reduce((sum, li) => sum + (li.quantity || 0), 0);
    w.totalDiscount += parseFloat(order.total_discounts) || 0;
  });

  Object.values(weeks).forEach(w => {
    w.aov = w.orders > 0 ? Math.round((w.revenue / w.orders) * 100) / 100 : 0;
    w.revenue = Math.round(w.revenue * 100) / 100;
  });

  return weeks;
}

function aggregateByDay(orders) {
  const days = {};

  orders.forEach(order => {
    const key = new Date(order.created_at).toISOString().slice(0, 10);

    if (!days[key]) {
      days[key] = { date: key, orders: 0, revenue: 0, units: 0 };
    }

    days[key].orders++;
    days[key].revenue += parseFloat(order.total_price) || 0;
    days[key].units += (order.line_items || []).reduce((sum, li) => sum + (li.quantity || 0), 0);
  });

  Object.values(days).forEach(d => {
    d.aov = d.orders > 0 ? Math.round((d.revenue / d.orders) * 100) / 100 : 0;
    d.revenue = Math.round(d.revenue * 100) / 100;
  });

  return days;
}

// ââ Main Pipeline âââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ

async function processBrand(brandKey, brandConfig, firebaseToken) {
  console.log(`\nââ Processing: ${brandKey} ââ`);
  console.log(`   Store: ${brandConfig.shopifyStore}`);

  const { shopifyStore, shopifyClientId, shopifyClientSecret, firebasePath } = brandConfig;

  // Get access token via OAuth client credentials
  const shopifyToken = await getShopifyAccessToken(shopifyStore, shopifyClientId, shopifyClientSecret);

  // Pull last 13 months of data (for year-over-year comparison)
  const now = new Date();
  const sinceDate = new Date(now.getFullYear() - 1, now.getMonth() - 1, 1).toISOString();
  const untilDate = now.toISOString();

  console.log(`   Fetching orders from ${sinceDate.slice(0, 10)} to ${untilDate.slice(0, 10)}...`);

  const orders = await fetchAllOrders(shopifyStore, shopifyToken, sinceDate, untilDate);
  console.log(`   Found ${orders.length} orders`);

  // Aggregate
  const monthlyData = aggregateByMonth(orders);
  const weeklyData = aggregateByWeek(orders);
  const dailyData = aggregateByDay(orders);

  // Summary stats
  const totalRevenue = Object.values(monthlyData).reduce((s, m) => s + m.revenue, 0);
  const totalOrders = Object.values(monthlyData).reduce((s, m) => s + m.orders, 0);
  const totalUnits = Object.values(monthlyData).reduce((s, m) => s + m.units, 0);

  const summary = {
    totalRevenue: Math.round(totalRevenue * 100) / 100,
    totalOrders,
    totalUnits,
    aov: totalOrders > 0 ? Math.round((totalRevenue / totalOrders) * 100) / 100 : 0,
    upo: totalOrders > 0 ? Math.round((totalUnits / totalOrders) * 10) / 10 : 0,
    lastUpdated: new Date().toISOString(),
    dataRange: { from: sinceDate.slice(0, 10), to: untilDate.slice(0, 10) }
  };

  console.log(`   Revenue: $${summary.totalRevenue.toLocaleString()} | Orders: ${totalOrders} | AOV: $${summary.aov}`);

  // Write to Firebase
  const payload = {
    summary,
    monthly: monthlyData,
    weekly: weeklyData,
    daily: dailyData
  };

  console.log(`   Writing to Firebase: ${firebasePath}/shopify_data ...`);
  await writeToFirebase(`${firebasePath}/shopify_data`, payload, firebaseToken);
  console.log(`   â Done`);

  return { brandKey, orders: orders.length, revenue: summary.totalRevenue };
}

async function main() {
  console.log('=== Shopify â Firebase Data Pipeline ===');
  console.log(`Date: ${new Date().toISOString()}`);

  // Validate config
  if (!FIREBASE_DB_URL || !FIREBASE_API_KEY || !FIREBASE_EMAIL || !FIREBASE_PASSWORD) {
    throw new Error('Missing Firebase env vars. Set FIREBASE_DB_URL, FIREBASE_API_KEY, FIREBASE_EMAIL, FIREBASE_PASSWORD');
  }

  const brandKeys = Object.keys(BRANDS_CONFIG);
  if (brandKeys.length === 0) {
    throw new Error('No brands configured. Set BRANDS_CONFIG env var.');
  }

  console.log(`Brands to process: ${brandKeys.join(', ')}`);

  // Auth with Firebase
  console.log('\nAuthenticating with Firebase...');
  const firebaseToken = await getFirebaseToken();
  console.log('â Firebase auth OK');

  // Process each brand
  const results = [];
  for (const brandKey of brandKeys) {
    try {
      const result = await processBrand(brandKey, BRANDS_CONFIG[brandKey], firebaseToken);
      results.push(result);
    } catch (err) {
      console.error(`â Error processing ${brandKey}:`, err.message);
      results.push({ brandKey, error: err.message });
    }
  }

  // Summary
  console.log('\n=== Pipeline Complete ===');
  results.forEach(r => {
    if (r.error) {
      console.log(`  â ${r.brandKey}: FAILED - ${r.error}`);
    } else {
      console.log(`  â ${r.brandKey}: ${r.orders} orders, $${r.revenue.toLocaleString()}`);
    }
  });

  // Exit with error if any brand failed
  if (results.some(r => r.error)) process.exit(1);
}

main().catch(err => {
  console.error('Pipeline failed:', err);
  process.exit(1);
});

