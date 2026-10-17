-- 過去データの補完（#111）の規則の一致率。正解の期間（旧系が price・price_time を持つ期間。既定の手順は 2023-05-08 16:31 〜 05-31 の
-- 取引日）の旧系の volume がある行に fill.sql と同じ推定（-- functions:begin 〜 -- functions:end と -- result:begin 〜 -- result:end の
-- source 以外は fill.sql と同じ。tests/test_tick_fill.py が一致を見る）を当て、推定の値と保存値を比べる。読み取りだけ。
-- 確かさ（basis）ごとに、行数 n・値段が一致 price_ok・時刻（時:分）が一致 hhmm_ok・日付込みで一致 datetime_ok・#4 の印のある行の数
-- n_fix4 とそのうち日付込みで一致 datetime_ok_fix4 を返す（#4 で日付を直さなかった行は保存値の日付がずれていることがある）。
-- volume_null_rows・volume_null_priced_rows は範囲の旧系の volume が NULL の行と、そのうち price か price_time がある行（規則の前提:
-- ページの表示が空の行は volume も空。0 であること）。
-- AC: matched の price_ok / n・hhmm_ok / n・datetime_ok_fix4 / n_fix4 が 99.9% 以上、matched 以外が 0 に近いこと。
-- 名前は runbook の render が、範囲は bq のクエリパラメータが入れる。fill_version は fill.sql と同じ値。
DECLARE start_at TIMESTAMP DEFAULT @start_at;
DECLARE end_at TIMESTAMP DEFAULT @end_at;
DECLARE fill_version STRING DEFAULT 'legacy-fill111-1';
DECLARE fix4_version STRING DEFAULT 'legacy-fix4-1';
DECLARE tick_from DATE DEFAULT DATE(start_at, 'Asia/Tokyo');
DECLARE tick_to DATE DEFAULT DATE_ADD(DATE(end_at, 'Asia/Tokyo'), INTERVAL 14 DAY);
DECLARE closed_trading ARRAY<DATE> DEFAULT (
  SELECT IFNULL(ARRAY_AGG(calendar_date), []) FROM `${project}.${dataset}.trading_calendar_latest` WHERE NOT is_trading_day
);
DECLARE closed_cash ARRAY<DATE> DEFAULT (
  SELECT IFNULL(ARRAY_AGG(calendar_date), []) FROM `${project}.${dataset}.trading_calendar_latest`
);
DECLARE volume_null STRUCT<n INT64, priced INT64> DEFAULT (
  SELECT AS STRUCT COUNT(*), COUNTIF(price IS NOT NULL OR price_time IS NOT NULL)
  FROM `${project}.${dataset}.option_price`
  WHERE created_at >= start_at AND created_at < end_at
    AND STARTS_WITH(ingest_run_id, 'legacy-') AND volume IS NULL
);

IF start_at IS NULL OR end_at IS NULL OR start_at >= end_at THEN
  SELECT ERROR(FORMAT('tick_fill accuracy: invalid range [%t, %t)', start_at, end_at));
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

CREATE TEMP TABLE est AS
-- result:begin
WITH
-- source:begin
tgt AS (
  -- 正解: 旧系の volume がある行（ページに約定値が出ていた行）の保存値。補完した行（fill_version）は正解ではないので除く
  SELECT created_at, last_trading_day, type, target_price, volume, price, price_time, diff, parser_version
  FROM `${project}.${dataset}.option_price`
  WHERE created_at >= start_at AND created_at < end_at
    AND STARTS_WITH(ingest_run_id, 'legacy-') AND volume IS NOT NULL
    AND IFNULL(parser_version, '') != fill_version
),
tick AS (
  -- 歩み値（日経225 の月次の限月だけ。Weekly は限月コードの MM が 12 より大きい）
  SELECT trade_date, execution_date, execution_time, contract_month, put_call_type, exercise_price, trade_price, trade_volume, seq_no
  FROM `${project}.${dataset}.dc_option_tick`
  WHERE trade_date >= tick_from AND trade_date <= tick_to
    AND index_type = '18' AND SAFE_CAST(SUBSTR(contract_month, 5, 2) AS INT64) <= 12
),
-- source:end
snaps AS (
  -- 推定はキー（スナップショット・系列・取引高）ごと。同じキーの重複行は同じ値になる
  SELECT DISTINCT
    FORMAT('%t|%t|%t|%t|%t', created_at, last_trading_day, type, target_price, volume) AS k,
    created_at, last_trading_day, type, target_price, volume
  FROM tgt
),
snap AS (
  SELECT
    *,
    DATETIME(created_at, 'Asia/Tokyo') AS c,
    trade_day(DATETIME(created_at, 'Asia/Tokyo'), closed_cash) AS td,
    is_night(DATETIME(created_at, 'Asia/Tokyo')) AS night,
    FORMAT_DATE('%Y%m', last_trading_day) AS cm,
    -- option_price.type: 1 = コール・2 = プット。DataCube の put_call_type: 1 = プット・2 = コール
    CASE type WHEN 1 THEN 2 WHEN 2 THEN 1 END AS pc
  FROM snaps
),
trades AS (
  SELECT
    contract_month, put_call_type, exercise_price, trade_date, seq_no, trade_price, trade_volume, x AS exec_at,
    -- 祝日取引の日中の約定（取引日は後の現物の営業日）
    DATE(x) != trade_date AND TIME(x) >= '08:00:00' AND TIME(x) < '16:30:00' AS holiday_day
  FROM tick, UNNEST([exec_at(trade_date, execution_date, execution_time)]) AS x
),
cum AS (
  SELECT
    *,
    SUM(trade_volume) OVER w AS cum_all,
    SUM(IF(holiday_day, 0, trade_volume)) OVER w AS cum_night
  FROM trades
  WINDOW w AS (
    PARTITION BY contract_month, put_call_type, exercise_price, trade_date
    ORDER BY exec_at, seq_no ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  )
),
days AS (
  SELECT DISTINCT trade_date FROM tick
),
hit AS (
  -- 累計が volume と等しくなった約定（夜間は祝日の日中の約定を除いた累計）。数量 0 の約定で同じ累計が続くときは最後の約定
  SELECT
    s.k,
    ARRAY_AGG(STRUCT(t.exec_at, t.trade_price) ORDER BY t.exec_at DESC, t.seq_no DESC LIMIT 1)[OFFSET(0)] AS m
  FROM snap AS s
  JOIN cum AS t
    ON t.contract_month = s.cm AND t.put_call_type = s.pc AND t.exercise_price = s.target_price AND t.trade_date = s.td
  WHERE IF(s.night, NOT t.holiday_day AND t.cum_night = s.volume, t.cum_all = s.volume)
  GROUP BY s.k
),
holiday AS (
  -- 夜間のスナップショットの + 60 秒より前の、祝日の日中の最後の約定
  SELECT s.k, MAX(t.exec_at) AS last_holiday
  FROM snap AS s
  JOIN trades AS t
    ON t.contract_month = s.cm AND t.put_call_type = s.pc AND t.exercise_price = s.target_price AND t.trade_date = s.td
  WHERE s.night AND t.holiday_day AND t.exec_at < DATETIME_ADD(s.c, INTERVAL 60 SECOND)
  GROUP BY s.k
),
est_keys AS (
  SELECT
    s.k, s.td, h.m,
    CASE
      WHEN s.td IS NULL OR d.trade_date IS NULL THEN 'no_tick_day'
      WHEN h.m IS NULL OR h.m.trade_price != TRUNC(h.m.trade_price) THEN 'no_match'
      WHEN s.night AND l.last_holiday > h.m.exec_at THEN 'ambiguous_holiday'
      WHEN h.m.exec_at > DATETIME_ADD(s.c, INTERVAL 10 MINUTE) THEN 'late_trade'
      ELSE 'matched'
    END AS basis
  FROM snap AS s
  LEFT JOIN hit AS h USING (k)
  LEFT JOIN holiday AS l USING (k)
  LEFT JOIN days AS d ON d.trade_date = s.td
)
SELECT
  t.*,
  e.td AS trade_day,
  e.basis,
  IF(e.basis = 'matched', CAST(e.m.trade_price AS INT64), NULL) AS new_price,
  IF(e.basis = 'matched', DATETIME_TRUNC(e.m.exec_at, MINUTE), NULL) AS new_price_time
FROM tgt AS t
JOIN est_keys AS e
  ON e.k = FORMAT('%t|%t|%t|%t|%t', t.created_at, t.last_trading_day, t.type, t.target_price, t.volume)
-- result:end
;

SELECT
  start_at, end_at, basis,
  COUNT(*) AS n,
  COUNTIF(new_price = price) AS price_ok,
  COUNTIF(FORMAT_DATETIME('%H:%M', new_price_time) = FORMAT_DATETIME('%H:%M', price_time)) AS hhmm_ok,
  COUNTIF(new_price_time = price_time) AS datetime_ok,
  COUNTIF(parser_version = fix4_version) AS n_fix4,
  COUNTIF(parser_version = fix4_version AND new_price_time = price_time) AS datetime_ok_fix4,
  volume_null.n AS volume_null_rows,
  volume_null.priced AS volume_null_priced_rows
FROM est
GROUP BY basis
ORDER BY basis;
