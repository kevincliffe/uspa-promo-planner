// fetch-klaviyo.js — Klaviyo → Firebase pipeline (multi-brand)
// Runs daily via GitHub Actions. Iterates every brand in BRANDS_CONFIG that
// has a `klaviyoPrivateKey` set and writes:
//   {firebasePath}/klaviyo_data/campaigns      — last 90 days of sent campaigns + performance
//   {firebasePath}/klaviyo_data/campaigns_upcoming — scheduled/draft campaigns (next 60 days)
//   {firebasePath}/klaviyo_data/flows          — flow list + 90-day performance
//   {firebasePath}/klaviyo_data/lists          — list growth snapshot
//   {firebasePath}/klaviyo_data/summary        — totals + last_updated
//
// Add a new brand = add it to BRANDS_CONFIG secret with its own klaviyoPrivateKey.
// No code changes required.

const KLAVIYO_REVISION = '2024-10-15';
const KLAVIYO_BASE = 'https://a.klaviyo.com/api';
const DAYS_BACK = 90;
const UPCOMING_DAYS = 60;

// ---------- env ----------
const BRANDS_CONFIG = JSON.parse(process.env.BRANDS_CONFIG || '{}');
const FIREBASE_DB_URL = process.env.FIREBASE_DB_URL;
const FIREBASE_API_KEY = process.env.FIREBASE_API_KEY;
const FIREBASE_EMAIL = process.env.FIREBASE_EMAIL;
const FIREBASE_PASSWORD = process.env.FIREBASE_PASSWORD;

if (!FIREBASE_DB_URL || !FIREBASE_API_KEY || !FIREBASE_EMAIL || !FIREBASE_PASSWORD) {
  console.error('Missing Firebase env vars'); process.exit(1);
}

// ---------- helpers ----------
const iso = (d) => new Date(d).toISOString();
const daysAgo = (n) => { const d = new Date(); d.setUTCDate(d.getUTCDate() - n); return d; };
const daysAhead = (n) => { const d = new Date(); d.setUTCDate(d.getUTCDate() + n); return d; };

async function klaviyo(path, key, opts = {}) {
  const url = path.startsWith('http') ? path : `${KLAVIYO_BASE}${path}`;
  const res = await fetch(url, {
    ...opts,
    headers: {
      'Authorization': `Klaviyo-API-Key ${key}`,
      'revision': KLAVIYO_REVISION,
      'accept': 'application/vnd.api+json',
      'content-type': 'application/vnd.api+json',
      ...(opts.headers || {})
    }
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Klaviyo ${res.status} on ${path}: ${text.substring(0, 300)}`);
  }
  return res.json();
}

async function klaviyoPaged(path, key) {
  let next = path;
  const out = [];
  while (next) {
    const j = await klaviyo(next, key);
    if (Array.isArray(j.data)) out.push(...j.data);
    next = j.links && j.links.next ? j.links.next : null;
  }
  return out;
}

// ---------- Firebase auth ----------
let firebaseIdToken = null;
async function firebaseSignIn() {
  const r = await fetch(
    `https://identitytoolkit.googleapis.com/v1/accounts:signInWithPassword?key=${FIREBASE_API_KEY}`,
    {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ email: FIREBASE_EMAIL, password: FIREBASE_PASSWORD, returnSecureToken: true })
    }
  );
  if (!r.ok) throw new Error('Firebase sign-in failed: ' + await r.text());
  const j = await r.json();
  firebaseIdToken = j.idToken;
}

async function firebasePut(path, data) {
  const url = `${FIREBASE_DB_URL}/${path}.json?auth=${firebaseIdToken}`;
  const r = await fetch(url, { method: 'PUT', body: JSON.stringify(data) });
  if (!r.ok) throw new Error(`Firebase PUT ${path} failed: ${r.status} ${await r.text()}`);
}

// ---------- per-brand fetchers ----------
async function fetchCampaignsSent(key) {
  const since = iso(daysAgo(DAYS_BACK));
  const filter = `and(greater-or-equal(created_at,${since}),equals(messages.channel,"email"),equals(status,"Sent"))`;
  const path =
    `/campaigns?filter=${encodeURIComponent(filter)}` +
    `&fields[campaign]=name,status,send_time,scheduled_at,created_at,updated_at` +
    `&include=campaign-messages` +
    `&fields[campaign-message]=definition.label,definition.content.subject,definition.content.preview_text`;
  return klaviyoPaged(path, key);
}

async function fetchCampaignsUpcoming(key) {
  const until = iso(daysAhead(UPCOMING_DAYS));
  const filter = `and(equals(messages.channel,"email"),any(status,["Scheduled","Draft","Sending","Queued without Recipients","Preparing to send"]),less-or-equal(scheduled_at,${until}))`;
  const path =
    `/campaigns?filter=${encodeURIComponent(filter)}` +
    `&fields[campaign]=name,status,send_time,scheduled_at,created_at` +
    `&include=campaign-messages` +
    `&fields[campaign-message]=definition.label,definition.content.subject,definition.content.preview_text`;
  return klaviyoPaged(path, key);
}

async function fetchCampaignReport(key, campaignIds) {
  // campaign-values-reports is a POST endpoint that returns aggregated stats per campaign
  if (!campaignIds.length) return {};
  const results = {};
  // Chunk to avoid oversized requests
  const chunks = [];
  for (let i = 0; i < campaignIds.length; i += 20) chunks.push(campaignIds.slice(i, i + 20));
  const stats = ['opens_unique','clicks_unique','delivered','bounced','unsubscribes','conversions','conversion_value','recipients'];
  for (const chunk of chunks) {
    try {
      const body = {
        data: {
          type: 'campaign-values-report',
          attributes: {
            statistics: stats,
            timeframe: { key: 'last_90_days' },
            conversion_metric_id: await getPlacedOrderMetricId(key),
            filter: `any(campaign_id,["${chunk.join('","')}"])`
          }
        }
      };
      const r = await klaviyo('/campaign-values-reports/', key, {
        method: 'POST',
        body: JSON.stringify(body)
      });
      const rows = r.data && r.data.attributes && r.data.attributes.results ? r.data.attributes.results : [];
      for (const row of rows) {
        results[row.groupings.campaign_id] = row.statistics;
      }
    } catch (e) {
      console.warn('campaign report chunk failed:', e.message);
    }
  }
  return results;
}

async function fetchFlows(key) {
  return klaviyoPaged(
    `/flows?fields[flow]=name,status,trigger_type,created,updated&filter=equals(archived,false)`,
    key
  );
}

async function fetchFlowReport(key, flowIds) {
  if (!flowIds.length) return {};
  const results = {};
  const chunks = [];
  for (let i = 0; i < flowIds.length; i += 20) chunks.push(flowIds.slice(i, i + 20));
  const stats = ['opens_unique','clicks_unique','delivered','conversions','conversion_value','recipients'];
  for (const chunk of chunks) {
    try {
      const body = {
        data: {
          type: 'flow-values-report',
          attributes: {
            statistics: stats,
            timeframe: { key: 'last_90_days' },
            conversion_metric_id: await getPlacedOrderMetricId(key),
            filter: `any(flow_id,["${chunk.join('","')}"])`
          }
        }
      };
      const r = await klaviyo('/flow-values-reports/', key, {
        method: 'POST',
        body: JSON.stringify(body)
      });
      const rows = r.data && r.data.attributes && r.data.attributes.results ? r.data.attributes.results : [];
      for (const row of rows) {
        results[row.groupings.flow_id] = row.statistics;
      }
    } catch (e) {
      console.warn('flow report chunk failed:', e.message);
    }
  }
  return results;
}

async function fetchLists(key) {
  return klaviyoPaged(`/lists?fields[list]=name,created,updated`, key);
}

// Cache Placed Order metric ID per key (needed for conversion reports)
const placedOrderCache = {};
async function getPlacedOrderMetricId(key) {
  if (placedOrderCache[key]) return placedOrderCache[key];
  const metrics = await klaviyoPaged(`/metrics?fields[metric]=name,integration`, key);
  const po = metrics.find((m) => m.attributes.name === 'Placed Order');
  if (!po) throw new Error('No Placed Order metric found in Klaviyo account');
  placedOrderCache[key] = po.id;
  return po.id;
}

// ---------- per-brand orchestrator ----------
async function processBrand(brandKey, cfg) {
  if (!cfg.klaviyoPrivateKey) {
    console.log(`[${brandKey}] no klaviyoPrivateKey configured, skipping`);
    return;
  }
  const key = cfg.klaviyoPrivateKey;
  const path = cfg.firebasePath || brandKey;
  console.log(`[${brandKey}] fetching Klaviyo data…`);

  const [sent, upcoming, flows, lists] = await Promise.all([
    fetchCampaignsSent(key),
    fetchCampaignsUpcoming(key),
    fetchFlows(key),
    fetchLists(key)
  ]);

  const sentIds = sent.map((c) => c.id);
  const flowIds = flows.map((f) => f.id);
  const [sentStats, flowStats] = await Promise.all([
    fetchCampaignReport(key, sentIds),
    fetchFlowReport(key, flowIds)
  ]);

  // Shape campaigns output
  const campaignsOut = {};
  for (const c of sent) {
    const msg = (c.relationships && c.relationships['campaign-messages'] && c.relationships['campaign-messages'].data || [])[0];
    const stats = sentStats[c.id] || {};
    campaignsOut[c.id] = {
      name: c.attributes.name,
      status: c.attributes.status,
      send_time: c.attributes.send_time,
      scheduled_at: c.attributes.scheduled_at,
      created_at: c.attributes.created_at,
      stats: {
        recipients: stats.recipients || 0,
        delivered: stats.delivered || 0,
        opens_unique: stats.opens_unique || 0,
        clicks_unique: stats.clicks_unique || 0,
        unsubscribes: stats.unsubscribes || 0,
        orders: stats.conversions || 0,
        revenue: stats.conversion_value || 0
      }
    };
  }

  const upcomingOut = {};
  for (const c of upcoming) {
    upcomingOut[c.id] = {
      name: c.attributes.name,
      status: c.attributes.status,
      send_time: c.attributes.send_time,
      scheduled_at: c.attributes.scheduled_at
    };
  }

  const flowsOut = {};
  for (const f of flows) {
    const stats = flowStats[f.id] || {};
    flowsOut[f.id] = {
      name: f.attributes.name,
      status: f.attributes.status,
      trigger_type: f.attributes.triggerType || f.attributes.trigger_type,
      stats: {
        recipients: stats.recipients || 0,
        delivered: stats.delivered || 0,
        opens_unique: stats.opens_unique || 0,
        clicks_unique: stats.clicks_unique || 0,
        orders: stats.conversions || 0,
        revenue: stats.conversion_value || 0
      }
    };
  }

  const listsOut = {};
  for (const l of lists) {
    listsOut[l.id] = { name: l.attributes.name, created: l.attributes.created, updated: l.attributes.updated };
  }

  // Summary totals
  const campaignRevenue = Object.values(campaignsOut).reduce((s, c) => s + (c.stats.revenue || 0), 0);
  const flowRevenue = Object.values(flowsOut).reduce((s, f) => s + (f.stats.revenue || 0), 0);
  const summary = {
    last_updated: new Date().toISOString(),
    window_days: DAYS_BACK,
    campaigns_sent: sent.length,
    campaigns_upcoming: upcoming.length,
    flows_live: flows.filter((f) => f.attributes.status === 'live').length,
    total_email_revenue: Math.round((campaignRevenue + flowRevenue) * 100) / 100,
    campaign_revenue: Math.round(campaignRevenue * 100) / 100,
    flow_revenue: Math.round(flowRevenue * 100) / 100
  };

  // Write to Firebase
  await firebasePut(`${path}/klaviyo_data/campaigns`, campaignsOut);
  await firebasePut(`${path}/klaviyo_data/campaigns_upcoming`, upcomingOut);
  await firebasePut(`${path}/klaviyo_data/flows`, flowsOut);
  await firebasePut(`${path}/klaviyo_data/lists`, listsOut);
  await firebasePut(`${path}/klaviyo_data/summary`, summary);

  console.log(`[${brandKey}] done:`, summary);
}

// ---------- main ----------
(async () => {
  const start = Date.now();
  try {
    await firebaseSignIn();
    const brands = Object.entries(BRANDS_CONFIG);
    if (!brands.length) { console.log('No brands configured'); return; }
    for (const [brandKey, cfg] of brands) {
      try {
        await processBrand(brandKey, cfg);
      } catch (e) {
        console.error(`[${brandKey}] FAILED:`, e.message);
      }
    }
    console.log(`Klaviyo pipeline complete in ${Math.round((Date.now() - start) / 1000)}s`);
  } catch (e) {
    console.error('Pipeline error:', e);
    process.exit(1);
  }
})();
