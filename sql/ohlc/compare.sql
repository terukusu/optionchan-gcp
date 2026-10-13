-- 1 時間足（#49）の新旧の突き合わせ（並行稼働中の日）。東京で、started_at が [@start_at, @end_at) の新系の 3 表と、
-- 旧系の同じ範囲（sql/ohlc/export.sql を US で流した結果の JSON。クエリパラメータ legacy_bars）をキー
-- (last_trading_day, time_frame, started_at) で比べる。範囲は UTC の正時にし、export.sql と同じ値を渡す。表ごとに 1 行:
--   legacy_keys / new_keys   旧系 / 新系のキーの数
--   match / differ           値（open・high・low・close）が一致 / 違う。新旧はスナップショットの版が違うことがある（#40 の「版の差」）ので、
--                            differ は説明できない差分とは限らない。max_abs_diff（値の差の絶対値の最大）と samples で中身を見る
--   legacy_only / new_only   片方にだけある 1 時間足（新系の取りこぼし・新系だけの時間。旧系の UTC 23 時台は旧系の集計が早く欠けることがある）
--   legacy_dup_keys          旧系で 2 行以上あるキー
--   conflict_keys            どちらかで中身の違う行が 2 種類以上あるキー（比べずに数える）
--   samples                  differ・legacy_only・new_only の例（先頭 20 件）
-- 名前は runbook の render が、期間と JSON は bq のクエリパラメータが入れる（値を SQL に埋め込まない）。
-- -- source:begin 〜 -- source:end / -- result:begin 〜 -- result:end は tests/test_ohlc_bq.py が固定値の入力に差し替える印。
DECLARE start_at TIMESTAMP DEFAULT @start_at;
DECLARE end_at TIMESTAMP DEFAULT @end_at;
DECLARE legacy_json STRING DEFAULT @legacy_bars;

IF start_at IS NULL OR end_at IS NULL OR start_at >= end_at THEN
  SELECT ERROR(FORMAT('ohlc_compare: invalid range [%t, %t)', start_at, end_at));
END IF;

IF legacy_json IS NULL THEN
  SELECT ERROR('ohlc_compare: legacy_bars is NULL (export.sql found no rows?)');
END IF;

-- result:begin
WITH
-- source:begin
new_rows AS (
  SELECT 'atm_iv' AS tbl, open, high, low, close, last_trading_day, time_frame, started_at
  FROM `${project}.${dataset}.atm_iv`
  WHERE started_at >= start_at AND started_at < end_at
  UNION ALL
  SELECT 'atm_option_price', open, high, low, close, last_trading_day, time_frame, started_at
  FROM `${project}.${dataset}.atm_option_price`
  WHERE started_at >= start_at AND started_at < end_at
  UNION ALL
  SELECT 'atm_target_price', open, high, low, close, last_trading_day, time_frame, started_at
  FROM `${project}.${dataset}.atm_target_price`
  WHERE started_at >= start_at AND started_at < end_at
),
-- source:end
legacy_rows AS (
  SELECT *
  FROM (
    SELECT
      JSON_VALUE(b, '$.tbl') AS tbl,
      CAST(JSON_VALUE(b, '$.open') AS FLOAT64) AS open,
      CAST(JSON_VALUE(b, '$.high') AS FLOAT64) AS high,
      CAST(JSON_VALUE(b, '$.low') AS FLOAT64) AS low,
      CAST(JSON_VALUE(b, '$.close') AS FLOAT64) AS close,
      CAST(JSON_VALUE(b, '$.last_trading_day') AS DATE) AS last_trading_day,
      CAST(JSON_VALUE(b, '$.time_frame') AS INT64) AS time_frame,
      CAST(JSON_VALUE(b, '$.started_at') AS TIMESTAMP) AS started_at,
      CAST(JSON_VALUE(b, '$.n') AS INT64) AS n
    FROM UNNEST(JSON_QUERY_ARRAY(legacy_json)) AS b
  )
  WHERE started_at >= start_at AND started_at < end_at
),
legacy_keys AS (
  SELECT
    tbl, last_trading_day, time_frame, started_at,
    SUM(n) AS n,
    COUNT(*) AS versions,
    ANY_VALUE(STRUCT(open, high, low, close)) AS v
  FROM legacy_rows
  GROUP BY tbl, last_trading_day, time_frame, started_at
),
new_keys AS (
  SELECT
    tbl, last_trading_day, time_frame, started_at,
    COUNT(DISTINCT TO_JSON_STRING(STRUCT(open, high, low, close))) AS versions,
    ANY_VALUE(STRUCT(open, high, low, close)) AS v
  FROM new_rows
  GROUP BY tbl, last_trading_day, time_frame, started_at
),
classified AS (
  SELECT
    tbl, last_trading_day, started_at,
    l.tbl IS NOT NULL AS has_legacy,
    w.tbl IS NOT NULL AS has_new,
    COALESCE(l.n, 0) AS legacy_n,
    CASE
      WHEN COALESCE(l.versions, 0) > 1 OR COALESCE(w.versions, 0) > 1 THEN 'conflict'
      WHEN l.tbl IS NULL THEN 'new_only'
      WHEN w.tbl IS NULL THEN 'legacy_only'
      WHEN l.v.open IS NOT DISTINCT FROM w.v.open AND l.v.high IS NOT DISTINCT FROM w.v.high
        AND l.v.low IS NOT DISTINCT FROM w.v.low AND l.v.close IS NOT DISTINCT FROM w.v.close THEN 'match'
      ELSE 'differ'
    END AS kind,
    GREATEST(
      COALESCE(ABS(l.v.open - w.v.open), 0), COALESCE(ABS(l.v.high - w.v.high), 0),
      COALESCE(ABS(l.v.low - w.v.low), 0), COALESCE(ABS(l.v.close - w.v.close), 0)) AS abs_diff,
    TO_JSON_STRING(l.v) AS legacy_value,
    TO_JSON_STRING(w.v) AS new_value
  FROM legacy_keys AS l
  FULL JOIN new_keys AS w USING (tbl, last_trading_day, time_frame, started_at)
)
SELECT
  tbl,
  start_at,
  end_at,
  COUNTIF(has_legacy) AS legacy_keys,
  COUNTIF(has_new) AS new_keys,
  COUNTIF(kind = 'match') AS match,
  COUNTIF(kind = 'differ') AS differ,
  COUNTIF(kind = 'legacy_only') AS legacy_only,
  COUNTIF(kind = 'new_only') AS new_only,
  COUNTIF(legacy_n > 1) AS legacy_dup_keys,
  COUNTIF(kind = 'conflict') AS conflict_keys,
  MAX(IF(kind = 'differ', abs_diff, NULL)) AS max_abs_diff,
  ARRAY_AGG(
    IF(kind IN ('differ', 'legacy_only', 'new_only'), STRUCT(kind, last_trading_day, started_at, legacy_value, new_value), NULL)
    IGNORE NULLS ORDER BY started_at, last_trading_day LIMIT 20
  ) AS samples
FROM UNNEST(['atm_iv', 'atm_option_price', 'atm_target_price']) AS tbl
LEFT JOIN classified USING (tbl)
GROUP BY tbl
-- result:end
ORDER BY tbl;
