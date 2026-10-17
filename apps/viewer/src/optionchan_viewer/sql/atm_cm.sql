-- /atm_iv_bars?n=cm（30 日固定の ATM IV、#100）: 取り込み 1 回ごとの 30 日の IV（iv30）を @step 秒（60 = 分足、3600 = 1 時間足）の
-- 足に束ねる。atm_cm_iv（定期クエリ sql/cm/atm_cm.sql が作る）の [@range_start, @fill_start) と、まだ表に入っていない分
-- （[@fill_start, @fill_end) の最新版の全行使価格の気配）を同じ計算（-- cm:begin〜end は sql/cm/atm_cm.sql と一字一句同じ。
-- tests/test_cm.py が検査）にしたものをつなぎ、[@range_start, @range_end) に入るものを使う。
--   足は UNIX 秒で切る（= JST の正時から）。open / close = 時刻が最も早い / 遅い NULL でない値、high / low = 最大 / 最小。
--   値が全部 NULL の足も行は出る（アプリが除く）。started_at 昇順
-- @fill_start はアプリが atm_cm_iv の最後の行の 1 秒後（最大 3 日前まで）にする。@fill_end が @fill_start より前（逆向き）なら
-- 生データを読まない（sql/atm_iv.sql と同じ）。SQ の日を決める休業日は [@holiday_start, @holiday_end)（補う分の日付から 120 日先まで）。
-- -- source:begin 〜 -- source:end は apps/viewer/tests/test_sql_bq.py が固定値の入力に差し替える印
WITH
-- source:begin
quote AS (
  SELECT created_at, last_trading_day, type, target_price, is_atm, bid, ask, bid_iv, ask_iv
  FROM `${project}.${dataset}.option_price_latest`(@fill_start, @fill_end)
),
holiday AS (
  SELECT calendar_date
  FROM `${project}.${dataset}.trading_calendar_latest`
  WHERE calendar_date >= @holiday_start AND calendar_date < @holiday_end
    AND NOT is_trading_day
),
stored AS (
  SELECT created_at, iv30
  FROM `${project}.${dataset}.atm_cm_iv`
  WHERE created_at >= @range_start AND created_at < @fill_start
),
-- source:end
-- cm:begin
-- 限月ごとの ATM の行使価格（is_atm。2 行あれば小さい方）。近い行使価格を選ぶ起点
cm_atm AS (
  SELECT created_at, last_trading_day, MIN(target_price) AS k0
  FROM quote
  WHERE is_atm
  GROUP BY created_at, last_trading_day
),
-- 使える気配: bid・ask が両方 0 より大きく、スプレッド（ask − bid）が中値の 30% 以下（薄い時間帯の極端に低い bid・高い ask を除く）
cm_quote AS (
  SELECT *, bid > 0 AND ask >= bid AND (ask - bid) * 2 <= 0.3 * (ask + bid) AS ok
  FROM quote
),
-- 行使価格ごとに call（type = 1）・put（type = 2）の気配の中値と mid IV（使える気配で、bid_iv・ask_iv が両方 0 より大きいときだけ）
cm_strike AS (
  SELECT
    created_at,
    last_trading_day,
    target_price AS k,
    ANY_VALUE(k0) AS k0,
    MAX(IF(type = 1 AND ok, (bid + ask) / 2, NULL)) AS call_mid,
    MAX(IF(type = 2 AND ok, (bid + ask) / 2, NULL)) AS put_mid,
    MAX(IF(type = 1 AND ok AND bid_iv > 0 AND ask_iv > 0, (bid_iv + ask_iv) / 2, NULL)) AS call_iv,
    MAX(IF(type = 2 AND ok AND bid_iv > 0 AND ask_iv > 0, (bid_iv + ask_iv) / 2, NULL)) AS put_iv
  FROM cm_quote JOIN cm_atm USING (created_at, last_trading_day)
  GROUP BY created_at, last_trading_day, target_price
),
-- ATM に近い順の番号（同じ距離は行使価格の小さい方から）
cm_near AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY created_at, last_trading_day ORDER BY ABS(k - k0), k) AS rn
  FROM cm_strike
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
-- 行使価格ごとの IV: call と put の mid IV の平均（片方だけならその値）。ATM に近い 9 本まで
cm_smile AS (
  SELECT
    created_at,
    last_trading_day,
    fwd,
    k,
    IF(call_iv IS NOT NULL AND put_iv IS NOT NULL, (call_iv + put_iv) / 2, COALESCE(call_iv, put_iv)) AS s
  FROM cm_near JOIN cm_fwd USING (created_at, last_trading_day)
  WHERE rn <= 9
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
-- SQ = 最終取引日の次の営業日（土日・休業日を飛ばす）の 09:00 JST
cm_sq AS (
  SELECT m.last_trading_day, TIMESTAMP(DATETIME(MIN(d), TIME(9, 0, 0)), 'Asia/Tokyo') AS sq_at
  FROM (SELECT DISTINCT last_trading_day FROM quote) AS m,
    UNNEST(GENERATE_DATE_ARRAY(DATE_ADD(m.last_trading_day, INTERVAL 1 DAY), DATE_ADD(m.last_trading_day, INTERVAL 15 DAY))) AS d
  WHERE EXTRACT(DAYOFWEEK FROM d) NOT IN (1, 7) AND d NOT IN (SELECT calendar_date FROM holiday)
  GROUP BY m.last_trading_day
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
points AS (
  SELECT created_at, iv30 FROM stored WHERE created_at < @range_end
  UNION ALL
  SELECT created_at, iv30 FROM cm WHERE created_at >= @range_start AND created_at < @range_end
)
SELECT
  ARRAY_AGG(iv30 IGNORE NULLS ORDER BY created_at)[SAFE_OFFSET(0)] AS open,
  MAX(iv30) AS high,
  MIN(iv30) AS low,
  ARRAY_AGG(iv30 IGNORE NULLS ORDER BY created_at DESC)[SAFE_OFFSET(0)] AS close,
  TIMESTAMP_SECONDS(DIV(UNIX_SECONDS(created_at), @step) * @step) AS started_at
FROM points
GROUP BY started_at
ORDER BY started_at
