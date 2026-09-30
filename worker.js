export default {
  // =========================================================
  // HTTP 요청
  //
  // /                  → Bybit 실시간 미리보기
  // /latest            → D1 최신 데이터 1개
  // /history?hours=1   → D1 최근 1시간 데이터
  // /history?hours=6   → D1 최근 6시간 데이터
  // /history?hours=24  → D1 최근 24시간 데이터
  //
  // 브라우저 조회 자체는 DB에 새 데이터를 저장하지 않음
  // =========================================================
  async fetch(request, env) {
    try {
      const url = new URL(request.url);

      if (url.pathname === "/latest") {
        return await getLatest(env);
      }

      if (url.pathname === "/history") {
        return await getHistory(url, env);
      }

      if (url.pathname === "/") {
        const data = await collectBybitData();

        return json({
          ok: true,
          mode: "preview",
          message: "Bybit live preview - not saved to D1",
          data,
        });
      }

      return json(
        {
          ok: false,
          error: "Not found",
          available: ["/", "/latest", "/history?hours=1"],
        },
        404
      );
    } catch (error) {
      return json(
        {
          ok: false,
          error: String(error?.message || error),
        },
        500
      );
    }
  },

  // =========================================================
  // Cron Trigger
  // Cloudflare Cron이 실행될 때만 D1에 저장
  // =========================================================
  async scheduled(event, env, ctx) {
    ctx.waitUntil(saveSnapshot(env));
  },
};


// ===========================================================
// Bybit 데이터 수집
// ===========================================================
async function collectBybitData() {
  const symbol = "SOLUSDT";

  const [
    tickerResult,
    fundingResult,
    oiResult,
  ] = await Promise.allSettled([
    fetchJSON(
      `https://api.bybit.com/v5/market/tickers?category=linear&symbol=${symbol}`
    ),

    fetchJSON(
      `https://api.bybit.com/v5/market/funding/history?category=linear&symbol=${symbol}&limit=1`
    ),

    fetchJSON(
      `https://api.bybit.com/v5/market/open-interest?category=linear&symbol=${symbol}&intervalTime=5min&limit=1`
    ),
  ]);

  const ticker =
    tickerResult.status === "fulfilled"
      ? tickerResult.value?.result?.list?.[0]
      : null;

  const funding =
    fundingResult.status === "fulfilled"
      ? fundingResult.value?.result?.list?.[0]
      : null;

  const oi =
    oiResult.status === "fulfilled"
      ? oiResult.value?.result?.list?.[0]
      : null;

  const now = Date.now();

  return {
    symbol,

    timestamp: now,
    timestamp_iso: new Date(now).toISOString(),

    price: numberOrNull(ticker?.lastPrice),

    mark_price: numberOrNull(ticker?.markPrice),

    index_price: numberOrNull(ticker?.indexPrice),

    funding_rate: numberOrNull(
      ticker?.fundingRate ?? funding?.fundingRate
    ),

    next_funding_time:
      ticker?.nextFundingTime
        ? Number(ticker.nextFundingTime)
        : null,

    open_interest: numberOrNull(
      ticker?.openInterest ?? oi?.openInterest
    ),

    volume_24h: numberOrNull(ticker?.volume24h),

    turnover_24h: numberOrNull(ticker?.turnover24h),

    price_change_24h_pct:
      ticker?.price24hPcnt != null
        ? Number(ticker.price24hPcnt) * 100
        : null,

    high_24h: numberOrNull(ticker?.highPrice24h),

    low_24h: numberOrNull(ticker?.lowPrice24h),

    source: "Bybit",
  };
}


// ===========================================================
// D1 저장
// ===========================================================
async function saveSnapshot(env) {
  if (!env.DB) {
    throw new Error(
      "D1 binding 'DB' is not configured."
    );
  }

  const data = await collectBybitData();

  await ensureTable(env);

  await env.DB.prepare(`
    INSERT INTO sol_market_data (
      timestamp,
      timestamp_iso,
      symbol,
      price,
      mark_price,
      index_price,
      funding_rate,
      next_funding_time,
      open_interest,
      volume_24h,
      turnover_24h,
      price_change_24h_pct,
      high_24h,
      low_24h,
      source
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `)
    .bind(
      data.timestamp,
      data.timestamp_iso,
      data.symbol,
      data.price,
      data.mark_price,
      data.index_price,
      data.funding_rate,
      data.next_funding_time,
      data.open_interest,
      data.volume_24h,
      data.turnover_24h,
      data.price_change_24h_pct,
      data.high_24h,
      data.low_24h,
      data.source
    )
    .run();

  console.log(
    `Saved ${data.symbol} snapshot at ${data.timestamp_iso}`
  );
}


// ===========================================================
// 최신 데이터
// ===========================================================
async function getLatest(env) {
  if (!env.DB) {
    return json(
      {
        ok: false,
        error: "D1 binding 'DB' is not configured.",
      },
      500
    );
  }

  await ensureTable(env);

  const row = await env.DB.prepare(`
    SELECT *
    FROM sol_market_data
    ORDER BY timestamp DESC
    LIMIT 1
  `).first();

  return json({
    ok: true,
    data: row || null,
  });
}


// ===========================================================
// 과거 데이터
// ===========================================================
async function getHistory(url, env) {
  if (!env.DB) {
    return json(
      {
        ok: false,
        error: "D1 binding 'DB' is not configured.",
      },
      500
    );
  }

  await ensureTable(env);

  let hours = Number(url.searchParams.get("hours") || 1);

  if (!Number.isFinite(hours)) {
    hours = 1;
  }

  hours = Math.max(1, Math.min(hours, 168));

  const since = Date.now() - hours * 60 * 60 * 1000;

  const result = await env.DB.prepare(`
    SELECT *
    FROM sol_market_data
    WHERE timestamp >= ?
    ORDER BY timestamp ASC
  `)
    .bind(since)
    .all();

  return json({
    ok: true,
    hours,
    count: result.results?.length || 0,
    data: result.results || [],
  });
}


// ===========================================================
// D1 테이블 생성
// ===========================================================
async function ensureTable(env) {
  await env.DB.prepare(`
    CREATE TABLE IF NOT EXISTS sol_market_data (
      id INTEGER PRIMARY KEY AUTOINCREMENT,

      timestamp INTEGER NOT NULL,
      timestamp_iso TEXT NOT NULL,

      symbol TEXT,

      price REAL,
      mark_price REAL,
      index_price REAL,

      funding_rate REAL,
      next_funding_time INTEGER,

      open_interest REAL,

      volume_24h REAL,
      turnover_24h REAL,

      price_change_24h_pct REAL,

      high_24h REAL,
      low_24h REAL,

      source TEXT
    )
  `).run();

  await env.DB.prepare(`
    CREATE INDEX IF NOT EXISTS
    idx_sol_market_data_timestamp
    ON sol_market_data(timestamp)
  `).run();
}


// ===========================================================
// HTTP JSON 요청
// ===========================================================
async function fetchJSON(url) {
  const response = await fetch(url, {
    headers: {
      Accept: "application/json",
      "User-Agent": "sol-market-data-worker",
    },
  });

  if (!response.ok) {
    throw new Error(
      `HTTP ${response.status}: ${url}`
    );
  }

  const data = await response.json();

  if (
    data &&
    typeof data === "object" &&
    "retCode" in data &&
    data.retCode !== 0
  ) {
    throw new Error(
      `Bybit API error ${data.retCode}: ${data.retMsg}`
    );
  }

  return data;
}


// ===========================================================
// 숫자 변환
// ===========================================================
function numberOrNull(value) {
  if (
    value === undefined ||
    value === null ||
    value === ""
  ) {
    return null;
  }

  const n = Number(value);

  return Number.isFinite(n) ? n : null;
}


// ===========================================================
// JSON 응답
// ===========================================================
function json(data, status = 200) {
  return new Response(
    JSON.stringify(data, null, 2),
    {
      status,
      headers: {
        "content-type":
          "application/json; charset=UTF-8",

        "access-control-allow-origin": "*",

        "cache-control": "no-store",
      },
    }
  );
}
