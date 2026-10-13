-- 1 時間足（#49）の旧系との同値の確認（target.md 3.2「過去 30 日分で現行と同値」）。US で、旧系の option_price の
-- created_at が [@start_at, @end_at) の ATM の put に、定期クエリ（sql/ohlc/atm_hourly.sql）と同じ 1 時間足の計算を当て、
-- 旧系の atm_iv / atm_option_price / atm_target_price（started_at が同じ範囲）とキー (last_trading_day, time_frame, started_at) で比べる。
-- 範囲は UTC の正時にする。表ごとに 1 行:
--   recomputed / legacy_keys   計算し直した 1 時間足の数 / 旧系のキーの数
--   match                      値（open・high・low・close）が一致
--   late                       UTC 23 時台（08:00 JST）の値の違い・旧系に無いもの。旧系は 00:01 UTC に前日分を集計するので、
--                              JPX のページの遅れ（約 15 分）でその時点に届いていなかった 08:45〜08:59 JST の行が入っていない（説明済み）
--   differ / legacy_only / recomputed_only   23 時台以外の値の違い・旧系だけ・計算し直した側だけ（説明できない差分。0 であること）
--   legacy_dup_keys            旧系で 2 行以上あるキー（中身が同じ重複を含む）
--   legacy_conflict_keys       旧系で中身の違う行が 2 種類以上あるキー（比べずに数える）
--   samples                    説明できない差分の例（先頭 20 件）
-- 名前は runbook の render が（legacy_dataset = optionchan）、期間は bq のクエリパラメータが入れる。
-- -- source:begin 〜 -- source:end / -- result:begin 〜 -- result:end は tests/test_ohlc_bq.py が固定値の入力に差し替える印。
-- -- bars:begin 〜 -- bars:end は sql/ohlc/atm_hourly.sql と一字一句同じ（tests/test_ohlc.py が検査）。
DECLARE start_at TIMESTAMP DEFAULT @start_at;
DECLARE end_at TIMESTAMP DEFAULT @end_at;

IF start_at IS NULL OR end_at IS NULL OR start_at >= end_at THEN
  SELECT ERROR(FORMAT('legacy_check: invalid range [%t, %t)', start_at, end_at));
END IF;

-- result:begin
WITH
-- source:begin
atm AS (
  SELECT created_at, last_trading_day, target_price, iv, price
  FROM `${project}.${legacy_dataset}.option_price`
  WHERE created_at >= start_at AND created_at < end_at
    AND is_atm AND type = 2
),
legacy AS (
  SELECT 'atm_iv' AS tbl, open, high, low, close, last_trading_day, time_frame, started_at
  FROM `${project}.${legacy_dataset}.atm_iv`
  WHERE started_at >= start_at AND started_at < end_at
  UNION ALL
  SELECT 'atm_option_price', open, high, low, close, last_trading_day, time_frame, started_at
  FROM `${project}.${legacy_dataset}.atm_option_price`
  WHERE started_at >= start_at AND started_at < end_at
  UNION ALL
  SELECT 'atm_target_price', open, high, low, close, last_trading_day, time_frame, started_at
  FROM `${project}.${legacy_dataset}.atm_target_price`
  WHERE started_at >= start_at AND started_at < end_at
),
-- source:end
-- bars:begin
bars AS (
  SELECT
    last_trading_day,
    3600 AS time_frame,
    TIMESTAMP_TRUNC(created_at, HOUR) AS started_at,
    ARRAY_AGG(iv IGNORE NULLS ORDER BY created_at, target_price, iv)[SAFE_OFFSET(0)] AS iv_open,
    MAX(iv) AS iv_high,
    MIN(iv) AS iv_low,
    ARRAY_AGG(iv IGNORE NULLS ORDER BY created_at DESC, target_price DESC, iv DESC)[SAFE_OFFSET(0)] AS iv_close,
    CAST(ARRAY_AGG(price IGNORE NULLS ORDER BY created_at, target_price, price)[SAFE_OFFSET(0)] AS FLOAT64) AS price_open,
    CAST(MAX(price) AS FLOAT64) AS price_high,
    CAST(MIN(price) AS FLOAT64) AS price_low,
    CAST(ARRAY_AGG(price IGNORE NULLS ORDER BY created_at DESC, target_price DESC, price DESC)[SAFE_OFFSET(0)] AS FLOAT64) AS price_close,
    CAST(ARRAY_AGG(target_price IGNORE NULLS ORDER BY created_at, target_price)[SAFE_OFFSET(0)] AS FLOAT64) AS target_price_open,
    CAST(MAX(target_price) AS FLOAT64) AS target_price_high,
    CAST(MIN(target_price) AS FLOAT64) AS target_price_low,
    CAST(ARRAY_AGG(target_price IGNORE NULLS ORDER BY created_at DESC, target_price DESC)[SAFE_OFFSET(0)] AS FLOAT64) AS target_price_close
  FROM atm
  GROUP BY last_trading_day, started_at
),
-- bars:end
recomputed AS (
  SELECT 'atm_iv' AS tbl, iv_open AS open, iv_high AS high, iv_low AS low, iv_close AS close, last_trading_day, time_frame, started_at
  FROM bars
  UNION ALL
  SELECT 'atm_option_price', price_open, price_high, price_low, price_close, last_trading_day, time_frame, started_at FROM bars
  UNION ALL
  SELECT 'atm_target_price', target_price_open, target_price_high, target_price_low, target_price_close, last_trading_day, time_frame, started_at
  FROM bars
),
legacy_keys AS (
  SELECT
    tbl, last_trading_day, time_frame, started_at,
    COUNT(*) AS n,
    COUNT(DISTINCT TO_JSON_STRING(STRUCT(open, high, low, close))) AS versions,
    ANY_VALUE(STRUCT(open, high, low, close)) AS v
  FROM legacy
  GROUP BY tbl, last_trading_day, time_frame, started_at
),
joined AS (
  SELECT
    tbl, last_trading_day, time_frame, started_at,
    r.tbl IS NOT NULL AS has_recomputed,
    l.tbl IS NOT NULL AS has_legacy,
    COALESCE(l.n, 0) AS n,
    COALESCE(l.versions, 0) AS versions,
    r.open IS NOT DISTINCT FROM l.v.open AND r.high IS NOT DISTINCT FROM l.v.high
      AND r.low IS NOT DISTINCT FROM l.v.low AND r.close IS NOT DISTINCT FROM l.v.close AS same,
    EXTRACT(HOUR FROM started_at) = 23 AS late_hour,
    TO_JSON_STRING(STRUCT(r.open, r.high, r.low, r.close)) AS recomputed_value,
    TO_JSON_STRING(l.v) AS legacy_value
  FROM recomputed AS r
  FULL JOIN legacy_keys AS l USING (tbl, last_trading_day, time_frame, started_at)
),
classified AS (
  SELECT
    *,
    CASE
      WHEN versions > 1 THEN 'conflict'
      WHEN has_recomputed AND has_legacy AND same THEN 'match'
      WHEN late_hour AND has_recomputed THEN 'late'
      WHEN has_recomputed AND has_legacy THEN 'differ'
      WHEN has_legacy THEN 'legacy_only'
      ELSE 'recomputed_only'
    END AS kind
  FROM joined
)
SELECT
  tbl,
  start_at,
  end_at,
  COUNTIF(has_recomputed) AS recomputed,
  COUNTIF(has_legacy) AS legacy_keys,
  COUNTIF(kind = 'match') AS match,
  COUNTIF(kind = 'late') AS late,
  COUNTIF(kind = 'differ') AS differ,
  COUNTIF(kind = 'legacy_only') AS legacy_only,
  COUNTIF(kind = 'recomputed_only') AS recomputed_only,
  COUNTIF(n > 1) AS legacy_dup_keys,
  COUNTIF(kind = 'conflict') AS legacy_conflict_keys,
  ARRAY_AGG(
    IF(kind IN ('differ', 'legacy_only', 'recomputed_only'),
      STRUCT(kind, last_trading_day, started_at, recomputed_value, legacy_value), NULL)
    IGNORE NULLS ORDER BY started_at, last_trading_day LIMIT 20
  ) AS samples
FROM UNNEST(['atm_iv', 'atm_option_price', 'atm_target_price']) AS tbl
LEFT JOIN classified USING (tbl)
GROUP BY tbl
-- result:end
ORDER BY tbl;
