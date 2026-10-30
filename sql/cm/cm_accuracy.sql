-- 気配から計算した IV の突き合わせ（#156）。手で流す読み取りだけのクエリ（docs/runbook.md の「気配から計算した IV の突き合わせ（#156）」）。
-- 範囲 [@start_at, @end_at) の最新版の気配の is_atm・bid_iv・ask_iv を NULL にした入力（IV・ATM の印を出さない取り込み元の形）で
-- 30 日固定の ATM IV を計算し、JPX の値（atm_cm_iv の行、is_atm・bid_iv・ask_iv）と比べる。結果は 1 行（件数と差の分位点）。
-- 計算（-- cm:begin 〜 -- cm:end）と一時関数（-- functions:begin 〜 -- functions:end）は sql/cm/atm_cm.sql と一字一句同じ
-- （tests/test_cm.py が検査）。書き込みはしない。プロジェクトとデータセットは render で入れる（runbook）。
-- スキャン量: option_price は UTC の日パーティションを丸ごと読む（1 日 約 112 MiB）。分位点は APPROX_QUANTILES（近似）。
-- 差は「計算 − JPX」。IV は %、F は円。
--   iv30: 両方ある回の差、計算だけ・表だけで出る回の数。month: 1 限月・2 限月の F・補間 ATM IV の差
--   k0: 気配の ATM（|C − P| 最小）と is_atm の行使価格の隔たり（行使価格の並びで 0 = 同じ、1 = 隣。none = 気配の ATM が出ない）
--   row_iv: 気配の ATM に近い 9 本の使える気配で、bid・ask から計算した IV と JPX の bid_iv・ask_iv（両方 0 より大きい行）の差。
--     no_iv = 計算した IV が NULL（裁定の範囲の外など）の行
-- functions:begin
-- Black-76（金利 0）の IV（%）。二分法（0〜500%、100 回）。値段が裁定の範囲の外か、500% を超えるなら NULL
CREATE TEMP FUNCTION black76_iv(is_call BOOL, price FLOAT64, f FLOAT64, k FLOAT64, t FLOAT64)
RETURNS FLOAT64 LANGUAGE js AS r"""
  if (price === null || f === null || k === null || t === null || !(t > 0) || !(f > 0) || !(k > 0)) return null;
  // 標準正規分布の累積分布関数（Marsaglia 2004 の級数）
  function cdf(x) {
    if (x < -10) return 0;
    if (x > 10) return 1;
    var s = x, v = x, b = x * x;
    for (var i = 1; i < 1000; i += 2) {
      v *= b / (i + 2);
      var n = s + v;
      if (n === s) break;
      s = n;
    }
    return 0.5 + s * Math.exp(-0.5 * b - 0.91893853320467274178);
  }
  function value(sigma) {
    var st = sigma * Math.sqrt(t);
    var d1 = (Math.log(f / k) + 0.5 * st * st) / st, d2 = d1 - st;
    return is_call ? f * cdf(d1) - k * cdf(d2) : k * cdf(-d2) - f * cdf(-d1);
  }
  var lower = is_call ? Math.max(f - k, 0) : Math.max(k - f, 0);
  var upper = is_call ? f : k;
  if (!(price > lower) || !(price < upper)) return null;
  var lo = 0, hi = 5;
  if (value(hi) < price) return null;
  for (var j = 0; j < 100; j++) {
    var mid = (lo + hi) / 2;
    if (value(mid) < price) lo = mid; else hi = mid;
  }
  return (lo + hi) / 2 * 100;
""";
-- functions:end
WITH
-- source:begin
jpx AS (
  SELECT created_at, last_trading_day, type, target_price, is_atm, bid, ask, bid_iv, ask_iv
  FROM `${project}.${dataset}.option_price_latest`(@start_at, @end_at)
),
-- 気配だけの形（is_atm・bid_iv・ask_iv を NULL にする）
quote AS (
  SELECT
    created_at,
    last_trading_day,
    type,
    target_price,
    CAST(NULL AS BOOL) AS is_atm,
    bid,
    ask,
    CAST(NULL AS FLOAT64) AS bid_iv,
    CAST(NULL AS FLOAT64) AS ask_iv
  FROM jpx
),
-- SQ の日と残存の起点の日を決める休業日（sql/cm/atm_cm.sql と同じ。範囲の日付の前日から。is_trading_day に関係なく全行。#149）
holiday AS (
  SELECT calendar_date
  FROM `${project}.${dataset}.trading_calendar_latest`
  WHERE calendar_date >= DATE_SUB(DATE(@start_at, 'Asia/Tokyo'), INTERVAL 1 DAY)
    AND calendar_date < DATE_ADD(DATE(@end_at, 'Asia/Tokyo'), INTERVAL 120 DAY)
),
-- 気配から作った行だけ（約定 IV からの行（basis = 'trade'。#158）は比べない）
stored AS (
  SELECT *
  FROM `${project}.${dataset}.atm_cm_iv`
  WHERE created_at >= @start_at AND created_at < @end_at AND basis IS NULL
),
-- source:end
-- cm:begin
-- 使える気配: bid・ask が両方 0 より大きく、スプレッド（ask − bid）が中値の 30% 以下（薄い時間帯の極端に低い bid・高い ask を除く）
cm_quote AS (
  SELECT *, bid > 0 AND ask >= bid AND (ask - bid) * 2 <= 0.3 * (ask + bid) AS ok
  FROM quote
),
-- 行使価格ごとに call（type = 1）・put（type = 2）の気配の中値と mid IV（使える気配で、bid_iv・ask_iv が両方 0 より大きいときだけ）。
-- atm = ATM の印。no_iv = ATM の印・bid_iv・ask_iv が無い（IV を出さない取り込み元。#156）。使える気配の bid・ask は no_iv の限月の
-- IV の計算に使う
cm_strike AS (
  SELECT
    created_at,
    last_trading_day,
    target_price AS k,
    LOGICAL_OR(is_atm) AS atm,
    LOGICAL_AND(is_atm IS NULL AND bid_iv IS NULL AND ask_iv IS NULL) AS no_iv,
    MAX(IF(type = 1 AND ok, (bid + ask) / 2, NULL)) AS call_mid,
    MAX(IF(type = 2 AND ok, (bid + ask) / 2, NULL)) AS put_mid,
    MAX(IF(type = 1 AND ok AND bid_iv > 0 AND ask_iv > 0, (bid_iv + ask_iv) / 2, NULL)) AS call_iv,
    MAX(IF(type = 2 AND ok AND bid_iv > 0 AND ask_iv > 0, (bid_iv + ask_iv) / 2, NULL)) AS put_iv,
    MAX(IF(type = 1 AND ok, bid, NULL)) AS call_bid,
    MAX(IF(type = 1 AND ok, ask, NULL)) AS call_ask,
    MAX(IF(type = 2 AND ok, bid, NULL)) AS put_bid,
    MAX(IF(type = 2 AND ok, ask, NULL)) AS put_ask
  FROM cm_quote
  GROUP BY created_at, last_trading_day, target_price
),
-- 限月ごとの ATM の行使価格 k0（近い行使価格を選ぶ起点）。is_atm の行使価格（2 行あれば小さい方）。
-- 限月の全行が no_iv（気配だけの限月）なら、call・put の両方に使える気配のある行使価格のうち |C − P| が最も小さいもの
-- （同じなら小さい方）。どちらも無ければ NULL（その限月は F・IV を出さない）
cm_atm AS (
  SELECT
    created_at,
    last_trading_day,
    LOGICAL_AND(no_iv) AS no_iv,
    IF(LOGICAL_AND(no_iv),
      ARRAY_AGG(IF(call_mid IS NOT NULL AND put_mid IS NOT NULL, k, NULL) IGNORE NULLS
        ORDER BY ABS(call_mid - put_mid), k LIMIT 1)[SAFE_OFFSET(0)],
      MIN(IF(atm, k, NULL))) AS k0
  FROM cm_strike
  GROUP BY created_at, last_trading_day
),
-- ATM に近い順の番号（同じ距離は行使価格の小さい方から）
cm_near AS (
  SELECT
    s.* EXCEPT (no_iv),
    a.k0,
    a.no_iv,
    ROW_NUMBER() OVER (PARTITION BY created_at, last_trading_day ORDER BY ABS(k - a.k0), k) AS rn
  FROM cm_strike AS s JOIN cm_atm AS a USING (created_at, last_trading_day)
  WHERE a.k0 IS NOT NULL
),
-- フォワード F: ATM に近い 5 本のうち call・put の両方に気配のある行使価格の C − P + K（put-call パリティ。金利は無視）の中央値。
-- 3 本に満たなければ出さない
cm_parity AS (
  SELECT created_at, last_trading_day, ARRAY_AGG(call_mid - put_mid + k ORDER BY call_mid - put_mid + k) AS xs
  FROM cm_near
  WHERE rn <= 5 AND call_mid IS NOT NULL AND put_mid IS NOT NULL
  GROUP BY created_at, last_trading_day
  HAVING COUNT(*) >= 3
),
cm_fwd AS (
  SELECT created_at, last_trading_day, (xs[OFFSET(DIV(ARRAY_LENGTH(xs) - 1, 2))] + xs[OFFSET(DIV(ARRAY_LENGTH(xs), 2))]) / 2 AS fwd
  FROM cm_parity
),
-- SQ = 最終取引日の次の営業日（土日・休業日を飛ばす）の 09:00 JST
cm_sq AS (
  SELECT m.last_trading_day, TIMESTAMP(DATETIME(MIN(d), TIME(9, 0, 0)), 'Asia/Tokyo') AS sq_at
  FROM (SELECT DISTINCT last_trading_day FROM quote) AS m,
    UNNEST(GENERATE_DATE_ARRAY(DATE_ADD(m.last_trading_day, INTERVAL 1 DAY), DATE_ADD(m.last_trading_day, INTERVAL 15 DAY))) AS d
  WHERE EXTRACT(DAYOFWEEK FROM d) NOT IN (1, 7) AND d NOT IN (SELECT calendar_date FROM holiday)
  GROUP BY m.last_trading_day
),
-- 気配だけの限月の IV の残存の起点の日 d0: 取り込みの時刻（JST）の 6 時間前の日付から数えて最初の現物の営業日（土日・休業日を
-- 飛ばす。夜間は始まった日、祝日取引の日は次の営業日。#114 の規則に営業日へ進めるのを足した）
cm_day AS (
  SELECT c.created_at, MIN(d) AS d0
  FROM (SELECT DISTINCT created_at, DATE(DATETIME_SUB(DATETIME(created_at, 'Asia/Tokyo'), INTERVAL 6 HOUR)) AS base FROM quote) AS c,
    UNNEST(GENERATE_DATE_ARRAY(c.base, DATE_ADD(c.base, INTERVAL 15 DAY))) AS d
  WHERE EXTRACT(DAYOFWEEK FROM d) NOT IN (1, 7) AND d NOT IN (SELECT calendar_date FROM holiday)
  GROUP BY c.created_at
),
-- ATM に近い 9 本の mid IV。JPX の形の限月は bid_iv・ask_iv の平均（cm_strike）。気配だけの限月は Black-76（金利 0、原資産は F、
-- 残存 t = (SQ の日 − d0) / 365 年）で bid・ask から IV を求めた平均（どちらかが裁定の範囲の外なら NULL）
cm_iv AS (
  SELECT
    created_at,
    last_trading_day,
    fwd,
    k,
    IF(no_iv, (black76_iv(TRUE, call_bid, fwd, k, t) + black76_iv(TRUE, call_ask, fwd, k, t)) / 2, call_iv) AS call_iv,
    IF(no_iv, (black76_iv(FALSE, put_bid, fwd, k, t) + black76_iv(FALSE, put_ask, fwd, k, t)) / 2, put_iv) AS put_iv
  FROM (
    SELECT n.*, f.fwd, DATE_DIFF(DATE(q.sq_at, 'Asia/Tokyo'), y.d0, DAY) / 365 AS t
    FROM cm_near AS n JOIN cm_fwd AS f USING (created_at, last_trading_day)
      LEFT JOIN cm_sq AS q USING (last_trading_day) LEFT JOIN cm_day AS y USING (created_at)
    WHERE n.rn <= 9
  )
),
-- 行使価格ごとの IV: call と put の mid IV の平均（片方だけならその値）
cm_smile AS (
  SELECT
    created_at,
    last_trading_day,
    fwd,
    k,
    IF(call_iv IS NOT NULL AND put_iv IS NOT NULL, (call_iv + put_iv) / 2, COALESCE(call_iv, put_iv)) AS s
  FROM cm_iv
),
-- F をはさむ 2 本（F 以下で最大・F より上で最小の、IV のある行使価格）
cm_bracket AS (
  SELECT
    created_at,
    last_trading_day,
    ANY_VALUE(fwd) AS fwd,
    ARRAY_AGG(IF(k <= fwd, STRUCT(k, s), NULL) IGNORE NULLS ORDER BY k DESC LIMIT 1)[SAFE_OFFSET(0)] AS lo,
    ARRAY_AGG(IF(k > fwd, STRUCT(k, s), NULL) IGNORE NULLS ORDER BY k LIMIT 1)[SAFE_OFFSET(0)] AS hi
  FROM cm_smile
  WHERE s IS NOT NULL
  GROUP BY created_at, last_trading_day
),
-- 限月ごとの補間 ATM IV: 2 本の IV を log(K / F) で線形に補間（片側が無ければ NULL）
cm_month AS (
  SELECT
    created_at,
    last_trading_day,
    fwd,
    IF(lo IS NULL OR hi IS NULL, NULL, lo.s + (hi.s - lo.s) * LN(fwd / lo.k) / LN(hi.k / lo.k)) AS iv
  FROM cm_bracket
),
-- SQ 前の限月を最終取引日の順に 1 限月・2 限月とする（ATM の印・F・IV が無い限月も数える。数えないと 2 限月が繰り上がる）。
-- days = SQ までの暦日の日数
cm_rank AS (
  SELECT
    created_at,
    last_trading_day,
    TIMESTAMP_DIFF(sq_at, created_at, SECOND) / 86400 AS days,
    fwd,
    iv,
    ROW_NUMBER() OVER (PARTITION BY created_at ORDER BY last_trading_day) AS rk
  FROM (SELECT DISTINCT created_at, last_trading_day FROM quote)
    JOIN cm_sq USING (last_trading_day) LEFT JOIN cm_month USING (created_at, last_trading_day)
  WHERE sq_at > created_at
),
cm_pair AS (
  SELECT
    created_at,
    MAX(IF(rk = 1, last_trading_day, NULL)) AS m1_last_trading_day,
    MAX(IF(rk = 1, days, NULL)) AS m1_days,
    MAX(IF(rk = 1, fwd, NULL)) AS m1_forward,
    MAX(IF(rk = 1, iv, NULL)) AS m1_iv,
    MAX(IF(rk = 2, last_trading_day, NULL)) AS m2_last_trading_day,
    MAX(IF(rk = 2, days, NULL)) AS m2_days,
    MAX(IF(rk = 2, fwd, NULL)) AS m2_forward,
    MAX(IF(rk = 2, iv, NULL)) AS m2_iv
  FROM cm_rank
  WHERE rk <= 2
  GROUP BY created_at
),
-- 30 日の IV: 1 限月の SQ まで 30 日以上なら 1 限月の IV（method = 'm1'。SQ の直後）、3〜30 日なら総分散 σ²T を T について
-- 線形に補間（method = 'interp'）、3 日を切ったら 2 限月の IV（method = 'm2'。SQ の直前）。境目で値がつながる
-- （30 日ちょうどで補間の重みが 1 限月だけ、3 日では 2 限月がほぼすべて。SQ の前後は同じ限月の IV）。どれにも当たらなければ NULL
cm_var AS (
  SELECT
    *,
    ((m2_days - 30) * m1_iv * m1_iv * m1_days + (30 - m1_days) * m2_iv * m2_iv * m2_days) / (m2_days - m1_days) AS var30
  FROM cm_pair
),
cm AS (
  SELECT
    created_at,
    CASE
      WHEN m1_days >= 30 THEN m1_iv
      WHEN m1_days >= 3 AND var30 > 0 THEN SQRT(var30 / 30)
      WHEN m1_days < 3 THEN m2_iv
    END AS iv30,
    CASE
      WHEN m1_days >= 30 AND m1_iv IS NOT NULL THEN 'm1'
      WHEN m1_days >= 3 AND m1_days < 30 AND var30 > 0 THEN 'interp'
      WHEN m1_days < 3 AND m2_iv IS NOT NULL THEN 'm2'
    END AS method,
    m1_last_trading_day,
    m1_days,
    m1_forward,
    m1_iv,
    m2_last_trading_day,
    m2_days,
    m2_forward,
    m2_iv
  FROM cm_var
)
-- cm:end
,
-- 取り込み 1 回ごとの差（計算 − 表）
diff AS (
  SELECT
    c.iv30 - s.iv30 AS iv30,
    c.iv30 IS NOT NULL AND s.iv30 IS NULL AS only_quote,
    c.iv30 IS NULL AND s.iv30 IS NOT NULL AS only_table,
    c.m1_forward - s.m1_forward AS f1,
    c.m2_forward - s.m2_forward AS f2,
    c.m1_iv - s.m1_iv AS s1,
    c.m2_iv - s.m2_iv AS s2
  FROM cm AS c FULL JOIN stored AS s USING (created_at)
),
-- 限月ごとの行使価格の並びでの位置
k_pos AS (
  SELECT created_at, last_trading_day, k, RANK() OVER (PARTITION BY created_at, last_trading_day ORDER BY k) AS pos
  FROM cm_strike
),
jpx_atm AS (
  SELECT created_at, last_trading_day, MIN(target_price) AS k
  FROM jpx
  WHERE is_atm
  GROUP BY created_at, last_trading_day
),
k0_gap AS (
  SELECT ABS(q.pos - j.pos) AS gap
  FROM jpx_atm AS a
    LEFT JOIN cm_atm AS c USING (created_at, last_trading_day)
    LEFT JOIN k_pos AS q ON q.created_at = a.created_at AND q.last_trading_day = a.last_trading_day AND q.k = c.k0
    LEFT JOIN k_pos AS j ON j.created_at = a.created_at AND j.last_trading_day = a.last_trading_day AND j.k = a.k
),
-- 行ごとの IV の差（F・t は cm と同じ）
row_iv AS (
  SELECT
    black76_iv(p.type = 1, p.bid, p.fwd, p.target_price, p.t) - p.bid_iv AS bid,
    black76_iv(p.type = 1, p.ask, p.fwd, p.target_price, p.t) - p.ask_iv AS ask
  FROM (
    SELECT j.*, f.fwd, DATE_DIFF(DATE(q.sq_at, 'Asia/Tokyo'), y.d0, DAY) / 365 AS t
    FROM cm_near AS n
      JOIN cm_fwd AS f USING (created_at, last_trading_day)
      JOIN jpx AS j ON j.created_at = n.created_at AND j.last_trading_day = n.last_trading_day AND j.target_price = n.k
      LEFT JOIN cm_sq AS q ON q.last_trading_day = n.last_trading_day
      LEFT JOIN cm_day AS y ON y.created_at = n.created_at
    WHERE n.rn <= 9 AND j.bid > 0 AND j.ask >= j.bid AND (j.ask - j.bid) * 2 <= 0.3 * (j.ask + j.bid)
      AND j.bid_iv > 0 AND j.ask_iv > 0
  ) AS p
)
SELECT
  (SELECT COUNT(*) FROM cm) AS snapshots,
  (
    SELECT AS STRUCT
      COUNTIF(iv30 IS NOT NULL) AS n,
      COUNTIF(only_quote) AS only_quote,
      COUNTIF(only_table) AS only_table,
      ROUND(APPROX_QUANTILES(ABS(iv30), 100)[OFFSET(50)], 4) AS p50,
      ROUND(APPROX_QUANTILES(ABS(iv30), 100)[OFFSET(90)], 4) AS p90,
      ROUND(APPROX_QUANTILES(ABS(iv30), 100)[OFFSET(99)], 4) AS p99,
      ROUND(MAX(ABS(iv30)), 4) AS max,
      ROUND(APPROX_QUANTILES(iv30, 100)[OFFSET(50)], 4) AS signed_p50,
      COUNTIF(ABS(iv30) > 0.3) AS over_0_3,
      COUNTIF(ABS(iv30) > 1) AS over_1
    FROM diff
  ) AS iv30,
  (
    SELECT AS STRUCT
      ROUND(APPROX_QUANTILES(ABS(f1), 100)[OFFSET(50)], 2) AS f1_p50,
      ROUND(APPROX_QUANTILES(ABS(f1), 100)[OFFSET(90)], 2) AS f1_p90,
      ROUND(APPROX_QUANTILES(ABS(f1), 100)[OFFSET(99)], 2) AS f1_p99,
      ROUND(APPROX_QUANTILES(ABS(f2), 100)[OFFSET(50)], 2) AS f2_p50,
      ROUND(APPROX_QUANTILES(ABS(f2), 100)[OFFSET(90)], 2) AS f2_p90,
      ROUND(APPROX_QUANTILES(ABS(f2), 100)[OFFSET(99)], 2) AS f2_p99,
      ROUND(APPROX_QUANTILES(ABS(s1), 100)[OFFSET(50)], 4) AS s1_p50,
      ROUND(APPROX_QUANTILES(ABS(s1), 100)[OFFSET(90)], 4) AS s1_p90,
      ROUND(APPROX_QUANTILES(ABS(s1), 100)[OFFSET(99)], 4) AS s1_p99,
      ROUND(APPROX_QUANTILES(ABS(s2), 100)[OFFSET(50)], 4) AS s2_p50,
      ROUND(APPROX_QUANTILES(ABS(s2), 100)[OFFSET(90)], 4) AS s2_p90,
      ROUND(APPROX_QUANTILES(ABS(s2), 100)[OFFSET(99)], 4) AS s2_p99
    FROM diff
  ) AS month,
  (
    SELECT AS STRUCT
      COUNT(*) AS n,
      COUNTIF(gap = 0) AS same,
      COUNTIF(gap = 1) AS next,
      COUNTIF(gap = 2) AS two,
      COUNTIF(gap > 2) AS farther,
      COUNTIF(gap IS NULL) AS none
    FROM k0_gap
  ) AS k0,
  (
    SELECT AS STRUCT
      COUNT(*) AS n,
      COUNTIF(bid IS NULL OR ask IS NULL) AS no_iv,
      ROUND(APPROX_QUANTILES(ABS(bid), 100)[OFFSET(50)], 4) AS bid_p50,
      ROUND(APPROX_QUANTILES(ABS(bid), 100)[OFFSET(90)], 4) AS bid_p90,
      ROUND(APPROX_QUANTILES(ABS(bid), 100)[OFFSET(99)], 4) AS bid_p99,
      ROUND(APPROX_QUANTILES(bid, 100)[OFFSET(50)], 4) AS bid_signed_p50,
      ROUND(APPROX_QUANTILES(ABS(ask), 100)[OFFSET(50)], 4) AS ask_p50,
      ROUND(APPROX_QUANTILES(ABS(ask), 100)[OFFSET(90)], 4) AS ask_p90,
      ROUND(APPROX_QUANTILES(ABS(ask), 100)[OFFSET(99)], 4) AS ask_p99,
      ROUND(APPROX_QUANTILES(ask, 100)[OFFSET(50)], 4) AS ask_signed_p50
    FROM row_iv
  ) AS row_iv
