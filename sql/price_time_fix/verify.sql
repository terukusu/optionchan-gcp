-- 過去データの補正（#4）の検証。東京で、created_at が [@start_at, @end_at) の範囲について表ごとに 1 行を返す（fix.sql の推定は使わない
-- 独立の検査）。補正の前にも流して基準値（new_rows・legacy_rows）を控え、補正の後に次がすべて 0 であることを見る:
--   out_of_scope_rows    補正の印（parser_version = 版）があるのに、旧系でない・@fixed_at 以降・price_time が NULL の行（新系の行に触れていない）
--   bad_raw_rows         補正の印がある行のうち、price_time_raw（元の値、'%F %T'）が読めない・時刻（時:分:秒）が price_time と違う
--   future_rows          旧系の行のうち、price_time が created_at（JST）+ 10 分より後（推定が未来に出ていない）
--   sunday_changed_rows  option・future の旧系の行のうち、日曜の日付で、補正で動かした行（日曜は立会が無い）
--   off_calendar_rows    補正で動かした行のうち、新しい日時が取引カレンダー上で立会中でない（spot は東証の休業日・土日）
--   spot_closed_rows     補正の印がある spot の行のうち、日付が東証の休業日・土日
--   shift_out_of_range_rows  補正の印がある行のうち、ずらした日数（元の日付 − 新しい日付）が −1〜31 日の外
-- 参考（0 でなくてよい）: unmarked_rows（未補正 = 一意に決まらず元のままの行。fix.sql の dryrun の rows_kept の合計と同じ）、
-- shifted_rows（日付を動かした行）、forward_rows（1 日後へ動かした行。表示が更新時刻より数分先の約定）、sunday_kept_rows（元の
-- ままの日曜の行）、new_rows（旧系でない行の数。補正の前後で変わらないこと）。
-- 立会の判定は fix.sql と同じ関数（-- functions:begin 〜 -- functions:end。tests/test_price_time_fix.py が一致を見る）と取引カレンダー。
-- 名前は runbook の render が、範囲は bq のクエリパラメータが入れる。fix_version は fix.sql・rollback.sql と同じ値。
DECLARE start_at TIMESTAMP DEFAULT @start_at;
DECLARE end_at TIMESTAMP DEFAULT @end_at;
DECLARE fixed_at TIMESTAMP DEFAULT @fixed_at;
DECLARE fix_version STRING DEFAULT 'legacy-fix4-1';
DECLARE closed_trading ARRAY<DATE> DEFAULT (
  SELECT IFNULL(ARRAY_AGG(calendar_date), []) FROM `${project}.${dataset}.trading_calendar_latest` WHERE NOT is_trading_day
);
DECLARE closed_cash ARRAY<DATE> DEFAULT (
  SELECT IFNULL(ARRAY_AGG(calendar_date), []) FROM `${project}.${dataset}.trading_calendar_latest`
);

-- functions:begin
-- 取引日（土日と休業日以外）
CREATE TEMP FUNCTION is_open(d DATE, closed ARRAY<DATE>) AS (
  EXTRACT(DAYOFWEEK FROM d) BETWEEN 2 AND 6 AND d NOT IN UNNEST(closed)
);
-- x（JST）が立会中か。cash = TRUE は spot（東証の営業日の日中だけ）
CREATE TEMP FUNCTION in_session(x DATETIME, closed ARRAY<DATE>, cash BOOL) AS (
  CASE
    WHEN cash THEN is_open(DATE(x), closed)
    WHEN TIME(x) BETWEEN '08:45:00' AND '15:45:00' OR TIME(x) >= '16:30:00' THEN is_open(DATE(x), closed)
    WHEN TIME(x) <= '06:00:00' THEN is_open(DATE_SUB(DATE(x), INTERVAL 1 DAY), closed)
    ELSE FALSE
  END
);
-- 保存値 b と同じ時刻で、upper 以下・立会中の最も新しい日時（b + 1 日から 31 日前まで）。無ければ NULL
CREATE TEMP FUNCTION latest_valid(b DATETIME, upper DATETIME, closed ARRAY<DATE>, cash BOOL) AS ((
  SELECT x
  FROM UNNEST(GENERATE_ARRAY(-1, 31)) AS k, UNNEST([DATETIME_SUB(b, INTERVAL k DAY)]) AS x
  WHERE x <= upper AND in_session(x, closed, cash)
  ORDER BY k
  LIMIT 1
));
-- a より前で、同じ時刻・立会中の最も新しい日時（次の候補）
CREATE TEMP FUNCTION previous_valid(a DATETIME, closed ARRAY<DATE>, cash BOOL) AS ((
  SELECT x
  FROM UNNEST(GENERATE_ARRAY(1, 31)) AS k, UNNEST([DATETIME_SUB(a, INTERVAL k DAY)]) AS x
  WHERE in_session(x, closed, cash)
  ORDER BY k
  LIMIT 1
));
-- functions:end

-- result:begin
WITH
-- source:begin
rows_all AS (
  SELECT 'spot_price' AS tbl, created_at, price_time, price_time_raw, ingest_run_id, parser_version
  FROM `${project}.${dataset}.spot_price`
  WHERE created_at >= start_at AND created_at < end_at
  UNION ALL
  SELECT 'future_price', created_at, price_time, price_time_raw, ingest_run_id, parser_version
  FROM `${project}.${dataset}.future_price`
  WHERE created_at >= start_at AND created_at < end_at
  UNION ALL
  SELECT 'option_price', created_at, price_time, price_time_raw, ingest_run_id, parser_version
  FROM `${project}.${dataset}.option_price`
  WHERE created_at >= start_at AND created_at < end_at
),
-- source:end
flagged AS (
  SELECT
    *,
    IFNULL(STARTS_WITH(ingest_run_id, 'legacy-'), FALSE) AS legacy,
    IFNULL(parser_version = fix_version, FALSE) AS marked,
    IF(IFNULL(parser_version = fix_version, FALSE), SAFE.PARSE_DATETIME('%F %T', price_time_raw), NULL) AS orig
  FROM rows_all
),
target AS (
  SELECT
    *,
    legacy AND created_at < fixed_at AND price_time IS NOT NULL AS in_scope,
    marked AND orig IS NOT NULL AND orig != price_time AS moved
  FROM flagged
)
SELECT
  tbl,
  COUNTIF(in_scope) AS legacy_rows,
  COUNTIF(in_scope AND NOT marked) AS unmarked_rows,
  COUNTIF(marked AND NOT in_scope) AS out_of_scope_rows,
  COUNTIF(marked AND (orig IS NULL OR TIME(orig) != TIME(price_time))) AS bad_raw_rows,
  COUNTIF(in_scope AND price_time > DATETIME_ADD(DATETIME(created_at, 'Asia/Tokyo'), INTERVAL 10 MINUTE)) AS future_rows,
  COUNTIF(in_scope AND tbl != 'spot_price' AND EXTRACT(DAYOFWEEK FROM price_time) = 1
    AND marked AND orig IS NOT NULL AND orig != price_time) AS sunday_changed_rows,
  COUNTIF(in_scope AND tbl != 'spot_price' AND EXTRACT(DAYOFWEEK FROM price_time) = 1
    AND NOT (marked AND orig IS NOT NULL AND orig != price_time)) AS sunday_kept_rows,
  COUNTIF(moved AND NOT in_session(price_time, IF(tbl = 'spot_price', closed_cash, closed_trading), tbl = 'spot_price'))
    AS off_calendar_rows,
  COUNTIF(marked AND tbl = 'spot_price' AND NOT is_open(DATE(price_time), closed_cash)) AS spot_closed_rows,
  COUNTIF(marked AND (orig IS NULL OR DATE_DIFF(DATE(orig), DATE(price_time), DAY) NOT BETWEEN -1 AND 31))
    AS shift_out_of_range_rows,
  COUNTIF(marked AND orig IS NOT NULL AND orig != price_time) AS shifted_rows,
  COUNTIF(marked AND orig IS NOT NULL AND orig < price_time) AS forward_rows,
  COUNTIF(NOT legacy) AS new_rows
FROM target
GROUP BY tbl
-- result:end
ORDER BY tbl;
