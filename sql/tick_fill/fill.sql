-- 過去データの補完（#111）: #110 の欠け 2（2023-04-25 13:42 〜 2023-05-08 16:31 JST）の旧系の option の行（ingest_run_id が 'legacy-'
-- で始まる行）のうち、price・price_time が NULL で volume がある行に、DataCube の歩み値（${dataset}.dc_option_tick）から約定値・約定時刻を
-- 入れる。東京で、created_at が [@start_at, @end_at) の範囲について流す。@mode = 'dryrun' は数えるだけ、'apply' は UPDATE する。
--
-- 規則（ページの取引高 volume は、その取引日の約定の累計の取引高と一致する。正解の期間で確かめた。runbook の節を参照）:
--   1. スナップショットの取引日 T: created_at（JST）が 16:30 以降なら翌日から、それより前なら当日から数えて最初の現物の営業日
--      （土日でも取引カレンダーの行（休業日・祝日取引の日）でもない日）。DataCube の trade_date と同じ（祝日取引の日は取引日にならない）
--   2. T の約定を約定日時（同時刻はシーケンス No）順に累計し、累計 = volume になった約定の値段を price、約定日時（JST、分に切り捨て）を
--      price_time に入れる。夜間のスナップショット（16:30 以降か 08:00 より前）は、祝日取引の日中（約定日 ≠ T かつ 08:00〜16:30）の
--      約定を累計から除く（ページの取引高は祝日の日中の約定を数えていない）
--   3. 確かさ（basis）: matched = 書く。ambiguous_holiday = 夜間で、照合した約定より後・スナップショット + 60 秒より前に祝日の日中の
--      約定がある（ページの約定値がどちらか決まらない）。late_trade = 照合した約定がスナップショット + 10 分より後。no_match = 累計の合う
--      約定が無い（値段が整数でない場合を含む）。no_tick_day = T の歩み値が無い（月の入れ忘れ。fill はこれがあると止まる）
--   ページの表示が空（volume が NULL）の行は対象に入れない（price が NULL の行と volume が NULL の行は正解の期間で完全に同じ）。
-- 2022-07 以前の歩み値の約定日（Make_Date）は夜間の 00:00〜08:00 の約定が前日の日付なので、exec_at で 1 日進める。
--
-- 書き込み（apply）: matched の未補完の行（parser_version が NULL）だけを UPDATE し、price・price_time と parser_version に版 fill_version を
-- 入れる（他の列・price_time_raw は変えない）。元の値はすべて NULL なので、印で戻せる（rollback.sql）。推定は補完済みの行も同じに計算する
-- ので、同じ範囲を流し直しても結果は同じで、2 回目は 0 行になる（冪等）。
-- 止める（ERROR）: 範囲が欠け 2（gap_start 〜 gap_end）の外、mode が不正、カレンダーが範囲の年を持たない、別の版の印がある、歩み値に
-- （trade_date, security_code, seq_no）の重複がある、対象の取引日の歩み値が無い。
-- 名前（${project} 等）は runbook の render が、範囲・モードは bq のクエリパラメータが入れる。
-- -- source:begin 〜 -- source:end / -- result:begin 〜 -- result:end / -- functions:begin 〜 -- functions:end は
-- tests/test_tick_fill_bq.py が固定値の入力に差し替える印。accuracy.sql は source 以外が同じ（tests/test_tick_fill.py が一致を見る）。
DECLARE start_at TIMESTAMP DEFAULT @start_at;
DECLARE end_at TIMESTAMP DEFAULT @end_at;
DECLARE mode STRING DEFAULT @mode;
DECLARE fill_version STRING DEFAULT 'legacy-fill111-1';
DECLARE gap_start TIMESTAMP DEFAULT TIMESTAMP '2023-04-25 04:42:00+00';
DECLARE gap_end TIMESTAMP DEFAULT TIMESTAMP '2023-05-08 07:31:00+00';
-- 歩み値を読む取引日の範囲（範囲の初日から、最後の日の 14 日後まで。取引日の最初の現物の営業日はその中にある）
DECLARE tick_from DATE DEFAULT DATE(start_at, 'Asia/Tokyo');
DECLARE tick_to DATE DEFAULT DATE_ADD(DATE(end_at, 'Asia/Tokyo'), INTERVAL 14 DAY);
DECLARE calendar_from DATE DEFAULT (
  SELECT DATE_TRUNC(MIN(calendar_date), YEAR) FROM `${project}.${dataset}.trading_calendar_latest`
);
DECLARE closed_trading ARRAY<DATE> DEFAULT (
  SELECT IFNULL(ARRAY_AGG(calendar_date), []) FROM `${project}.${dataset}.trading_calendar_latest` WHERE NOT is_trading_day
);
DECLARE closed_cash ARRAY<DATE> DEFAULT (
  SELECT IFNULL(ARRAY_AGG(calendar_date), []) FROM `${project}.${dataset}.trading_calendar_latest`
);
DECLARE missing_new_year ARRAY<INT64> DEFAULT (
  SELECT ARRAY_AGG(y ORDER BY y)
  FROM UNNEST(GENERATE_ARRAY(EXTRACT(YEAR FROM tick_from), EXTRACT(YEAR FROM tick_to))) AS y
  -- 版の行は平日だけなので、1/1〜1/3（年始の休業日。3 日のどれかは平日）のどれかがあればその年はある
  WHERE y NOT IN (
    SELECT EXTRACT(YEAR FROM calendar_date) FROM `${project}.${dataset}.trading_calendar_latest`
    WHERE EXTRACT(MONTH FROM calendar_date) = 1 AND EXTRACT(DAY FROM calendar_date) <= 3
  )
);
DECLARE other_versions INT64 DEFAULT (
  SELECT COUNT(*) FROM `${project}.${dataset}.option_price`
  WHERE created_at >= start_at AND created_at < end_at
    AND parser_version LIKE 'legacy-fill111-%' AND parser_version != fill_version
);
DECLARE tick_duplicates INT64 DEFAULT (
  SELECT COUNT(*) FROM (
    SELECT trade_date, security_code, seq_no FROM `${project}.${dataset}.dc_option_tick`
    WHERE trade_date >= tick_from AND trade_date <= tick_to
    GROUP BY trade_date, security_code, seq_no
    HAVING COUNT(*) > 1
  )
);
DECLARE missing_days ARRAY<DATE> DEFAULT [];
DECLARE updated_option INT64 DEFAULT 0;

IF start_at IS NULL OR end_at IS NULL OR start_at >= end_at THEN
  SELECT ERROR(FORMAT('tick_fill: invalid range [%t, %t)', start_at, end_at));
END IF;

IF start_at < gap_start OR end_at > gap_end THEN
  SELECT ERROR(FORMAT('tick_fill: outside the gap (#111): [%t, %t)', start_at, end_at));
END IF;

IF IFNULL(mode, '') NOT IN ('dryrun', 'apply') THEN
  SELECT ERROR(FORMAT('tick_fill: mode must be dryrun or apply: %s', IFNULL(mode, 'NULL')));
END IF;

IF calendar_from IS NULL OR calendar_from > tick_from THEN
  SELECT ERROR(FORMAT('tick_fill: trading calendar starts at %t, needs %t (#67)', calendar_from, tick_from));
END IF;

IF ARRAY_LENGTH(IFNULL(missing_new_year, [])) > 0 THEN
  SELECT ERROR(FORMAT('tick_fill: trading calendar has no new-year rows (1/1-1/3) for years %t (#67)', missing_new_year));
END IF;

-- 別の版の補完が残っている（版を上げるときは rollback.sql で戻してから流す）
IF other_versions > 0 THEN
  SELECT ERROR(FORMAT('tick_fill: %d rows filled by another version (not %s)', other_versions, fill_version));
END IF;

-- 同じ約定が 2 回入っている（bq load を --replace とパーティションの指定なしで流した等）
IF tick_duplicates > 0 THEN
  SELECT ERROR(FORMAT('tick_fill: dc_option_tick has %d duplicated (trade_date, security_code, seq_no)', tick_duplicates));
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

-- 1. 推定（範囲の対象の行ごと）
CREATE TEMP TABLE est AS
-- result:begin
WITH
-- source:begin
tgt AS (
  -- 対象: 旧系の、volume がある未補完の NULL の行と、補完済みの行（同じ推定に入れて冪等・不一致を数える）
  SELECT created_at, last_trading_day, type, target_price, volume, price, price_time, diff, parser_version
  FROM `${project}.${dataset}.option_price`
  WHERE created_at >= start_at AND created_at < end_at
    AND STARTS_WITH(ingest_run_id, 'legacy-') AND volume IS NOT NULL
    AND ((price IS NULL AND price_time IS NULL AND parser_version IS NULL) OR parser_version = fill_version)
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

-- 対象の取引日の歩み値が無い（その月を bq load していない）
SET missing_days = (SELECT ARRAY_AGG(DISTINCT IFNULL(trade_day, DATE '1970-01-01') ORDER BY IFNULL(trade_day, DATE '1970-01-01'))
                    FROM est WHERE basis = 'no_tick_day');
IF ARRAY_LENGTH(IFNULL(missing_days, [])) > 0 THEN
  SELECT ERROR(FORMAT('tick_fill: dc_option_tick has no rows for trade days %t (load the months first)', missing_days));
END IF;

-- 2. 書き込み（matched の未補完の旧系の行だけ。決まらない行・volume が NULL の行・新系の行には触れない（印も付けない））
IF mode = 'apply' THEN
  UPDATE `${project}.${dataset}.option_price` AS t
  SET price = e.new_price, price_time = e.new_price_time, parser_version = fill_version
  FROM (
    SELECT DISTINCT created_at, last_trading_day, type, target_price, volume, new_price, new_price_time
    FROM est WHERE basis = 'matched'
  ) AS e
  WHERE t.created_at >= start_at AND t.created_at < end_at
    AND STARTS_WITH(t.ingest_run_id, 'legacy-')
    AND t.price IS NULL AND t.price_time IS NULL AND t.volume IS NOT NULL AND t.parser_version IS NULL
    AND e.created_at = t.created_at AND e.last_trading_day = t.last_trading_day AND e.type = t.type
    AND e.target_price = t.target_price AND e.volume = t.volume;
  SET updated_option = @@row_count;
END IF;

-- 3. 内訳（確かさごと）。rows_pending は書く（書いた）行数で、apply では updated_option と同じになる。rows_kept は書かない行数。
--    rows_mismatch は補完済みの行で、今の値と推定が違う行。rows_base_inconsistent は書く行のうち前日比（diff）がある行で、
--    「約定値 − 前日比」（前日の清算値）が系列・取引日ごとの最頻値と違う行（独立の検算。0 に近いこと）
WITH
base AS (
  SELECT last_trading_day, type, target_price, trade_day, new_price - diff AS b
  FROM est WHERE basis = 'matched' AND diff IS NOT NULL
),
modes AS (
  SELECT last_trading_day, type, target_price, trade_day, ARRAY_AGG(b ORDER BY n DESC, b LIMIT 1)[OFFSET(0)] AS mode_b
  FROM (SELECT last_trading_day, type, target_price, trade_day, b, COUNT(*) AS n FROM base GROUP BY 1, 2, 3, 4, 5)
  GROUP BY 1, 2, 3, 4
)
SELECT
  mode, start_at, end_at, e.basis,
  COUNT(*) AS rows_total,
  COUNTIF(e.basis = 'matched' AND e.parser_version IS NULL) AS rows_pending,
  COUNTIF(e.basis != 'matched') AS rows_kept,
  COUNTIF(e.parser_version = fill_version
    AND (e.basis != 'matched' OR e.price != e.new_price OR e.price_time != e.new_price_time)) AS rows_mismatch,
  COUNTIF(e.basis = 'matched' AND e.diff IS NOT NULL) AS rows_base_checked,
  COUNTIF(e.basis = 'matched' AND e.diff IS NOT NULL AND e.new_price - e.diff != o.mode_b) AS rows_base_inconsistent,
  updated_option
FROM est AS e
LEFT JOIN modes AS o USING (last_trading_day, type, target_price, trade_day)
GROUP BY e.basis
ORDER BY e.basis;
