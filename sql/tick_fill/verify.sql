-- 過去データの補完（#111）の検証。東京で、created_at が [@start_at, @end_at) の option_price について 1 行を返す（fill.sql の推定は
-- 使わない独立の検査）。補完の前にも流して基準値（target_rows・volume_null_rows・new_rows）を控え、補完の後に次がすべて 0 であることを見る:
--   out_of_scope_rows         補完の印（parser_version = 版）があるのに、旧系でない・欠け 2（gap_start 〜 gap_end）の外・volume が NULL の行
--   null_marked_rows          補完の印がある行のうち、price か price_time が NULL
--   future_rows               補完の印がある行のうち、price_time が created_at（JST）+ 10 分より後
--   off_session_rows          補完の印がある行のうち、price_time が立会の外（取引カレンダー。#4 の in_session と同じ）
--   wrong_trade_day_rows      補完の印がある行のうち、price_time の取引日がスナップショットの取引日と違う
--   volume_null_changed_rows  欠け 2 の旧系の volume が NULL の行のうち、price・price_time・parser_version のどれかがある（表示が空の行に触れていない）
-- 参考（0 でなくてよい）: marked_rows（補完した行）、target_rows（未補完の対象 = 旧系の欠け 2 の volume がある NULL の行。補完の後は
-- fill.sql の dryrun の rows_kept の合計と同じ）、volume_null_rows・new_rows（旧系の volume が NULL の行・旧系でない行。前後で同じ）、
-- base_checked_rows / base_inconsistent_rows（補完の印がある行のうち前日比（diff）がある行と、その「price − diff」（前日の清算値）が
-- 系列・取引日ごとの最頻値と違う行。AC は base_inconsistent_rows / base_checked_rows <= 0.1%）。
-- 関数は fill.sql と同じ（-- functions:begin 〜 -- functions:end。tests/test_tick_fill.py が一致を見る）。
-- 名前は runbook の render が、範囲は bq のクエリパラメータが入れる。fill_version・gap_start・gap_end は fill.sql と同じ値。
DECLARE start_at TIMESTAMP DEFAULT @start_at;
DECLARE end_at TIMESTAMP DEFAULT @end_at;
DECLARE fill_version STRING DEFAULT 'legacy-fill111-1';
DECLARE gap_start TIMESTAMP DEFAULT TIMESTAMP '2023-04-25 04:42:00+00';
DECLARE gap_end TIMESTAMP DEFAULT TIMESTAMP '2023-05-08 07:31:00+00';
DECLARE closed_trading ARRAY<DATE> DEFAULT (
  SELECT IFNULL(ARRAY_AGG(calendar_date), []) FROM `${project}.${dataset}.trading_calendar_latest` WHERE NOT is_trading_day
);
DECLARE closed_cash ARRAY<DATE> DEFAULT (
  SELECT IFNULL(ARRAY_AGG(calendar_date), []) FROM `${project}.${dataset}.trading_calendar_latest`
);

IF start_at IS NULL OR end_at IS NULL OR start_at >= end_at THEN
  SELECT ERROR(FORMAT('tick_fill verify: invalid range [%t, %t)', start_at, end_at));
END IF;

-- functions:begin
-- 現物の営業日（土日と取引カレンダーの行（休業日・祝日取引の日）以外）。closed に closed_trading を渡すと先物・オプションの取引日
CREATE TEMP FUNCTION is_open(d DATE, closed ARRAY<DATE>) AS (
  EXTRACT(DAYOFWEEK FROM d) BETWEEN 2 AND 6 AND d NOT IN UNNEST(closed)
);
-- 日時 x（JST）の取引日: 16:30 以降なら翌日から、それより前なら当日から数えて最初の現物の営業日（DataCube の trade_date と同じ）
CREATE TEMP FUNCTION trade_day(x DATETIME, closed ARRAY<DATE>) AS ((
  SELECT d
  FROM UNNEST(GENERATE_DATE_ARRAY(
    DATE_ADD(DATE(x), INTERVAL IF(TIME(x) >= '16:30:00', 1, 0) DAY),
    DATE_ADD(DATE(x), INTERVAL 15 DAY))) AS d
  WHERE is_open(d, closed)
  ORDER BY d
  LIMIT 1
));
-- 約定日時（JST）。2022-07 以前の取引日の約定日は、夜間の 00:00〜08:00 の約定が夜間の始まりの日（前日）なので 1 日進める
CREATE TEMP FUNCTION exec_at(trade_date DATE, execution_date DATE, t TIME) AS (
  DATETIME(IF(trade_date < '2022-08-01' AND t < '08:00:00', DATE_ADD(execution_date, INTERVAL 1 DAY), execution_date), t)
);
-- 夜間のスナップショット（16:30 以降か 08:00 より前）
CREATE TEMP FUNCTION is_night(x DATETIME) AS (
  TIME(x) >= '16:30:00' OR TIME(x) < '08:00:00'
);
-- x（JST）が立会中か（日中 08:45〜15:45・夜間 16:30〜翌 06:00。#4 の in_session の先物・オプションの分）
CREATE TEMP FUNCTION in_session(x DATETIME, closed ARRAY<DATE>) AS (
  CASE
    WHEN TIME(x) BETWEEN '08:45:00' AND '15:45:00' OR TIME(x) >= '16:30:00' THEN is_open(DATE(x), closed)
    WHEN TIME(x) <= '06:00:00' THEN is_open(DATE_SUB(DATE(x), INTERVAL 1 DAY), closed)
    ELSE FALSE
  END
);
-- functions:end

-- result:begin
WITH
-- source:begin
rows_all AS (
  SELECT created_at, last_trading_day, type, target_price, price, price_time, volume, diff, ingest_run_id, parser_version
  FROM `${project}.${dataset}.option_price`
  WHERE created_at >= start_at AND created_at < end_at
),
-- source:end
flagged AS (
  SELECT
    *,
    IFNULL(STARTS_WITH(ingest_run_id, 'legacy-'), FALSE) AS legacy,
    IFNULL(parser_version = fill_version, FALSE) AS marked,
    created_at >= gap_start AND created_at < gap_end AS in_gap,
    DATETIME(created_at, 'Asia/Tokyo') AS c,
    trade_day(DATETIME(created_at, 'Asia/Tokyo'), closed_cash) AS td
  FROM rows_all
),
modes AS (
  SELECT last_trading_day, type, target_price, td, ARRAY_AGG(b ORDER BY n DESC, b LIMIT 1)[OFFSET(0)] AS mode_b
  FROM (
    SELECT last_trading_day, type, target_price, td, price - diff AS b, COUNT(*) AS n
    FROM flagged WHERE marked AND price IS NOT NULL AND diff IS NOT NULL
    GROUP BY 1, 2, 3, 4, 5
  )
  GROUP BY 1, 2, 3, 4
)
SELECT
  COUNTIF(f.marked) AS marked_rows,
  COUNTIF(f.legacy AND f.in_gap AND f.parser_version IS NULL AND f.price IS NULL AND f.price_time IS NULL
    AND f.volume IS NOT NULL) AS target_rows,
  COUNTIF(f.legacy AND f.volume IS NULL) AS volume_null_rows,
  COUNTIF(NOT f.legacy) AS new_rows,
  COUNTIF(f.marked AND (NOT f.legacy OR NOT f.in_gap OR f.volume IS NULL)) AS out_of_scope_rows,
  COUNTIF(f.marked AND (f.price IS NULL OR f.price_time IS NULL)) AS null_marked_rows,
  COUNTIF(f.marked AND f.price_time > DATETIME_ADD(f.c, INTERVAL 10 MINUTE)) AS future_rows,
  COUNTIF(f.marked AND NOT IFNULL(in_session(f.price_time, closed_trading), FALSE)) AS off_session_rows,
  COUNTIF(f.marked AND trade_day(f.price_time, closed_cash) IS DISTINCT FROM f.td) AS wrong_trade_day_rows,
  COUNTIF(f.legacy AND f.in_gap AND f.volume IS NULL
    AND (f.price IS NOT NULL OR f.price_time IS NOT NULL OR f.parser_version IS NOT NULL)) AS volume_null_changed_rows,
  COUNTIF(f.marked AND f.price IS NOT NULL AND f.diff IS NOT NULL) AS base_checked_rows,
  COUNTIF(f.marked AND f.price IS NOT NULL AND f.diff IS NOT NULL AND f.price - f.diff != o.mode_b) AS base_inconsistent_rows
FROM flagged AS f
LEFT JOIN modes AS o USING (last_trading_day, type, target_price, td)
-- result:end
;
