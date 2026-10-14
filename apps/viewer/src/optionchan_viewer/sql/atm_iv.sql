-- /atm_iv_data のクエリ 2: 1 時間足 atm_iv（#49）と、まだ 1 時間足になっていない分（[@fill_start, @fill_end) の最新版の
-- option_price の ATM の put）を #49 と同じ計算（-- bars:begin〜end は sql/ohlc/atm_hourly.sql と一字一句同じ。
-- tests/test_ohlc.py が検査）で 1 時間足にしたものをつなぐ。
-- 現行の atm_iv_data（functions_py/atm_iv/main.py query_atm_iv）と同じ意味:
--   限月 = 1 時間足の直近 10 日（started_at > @months_since = now − 10 日の UTC の 0 時）の last_trading_day のうち
--          @today（JST の今日）以降を昇順に並べた @n 番目（範囲外は NULL → 0 行）
--   足 = その限月の started_at >= @days_start（now − d 日の UTC の 0 時）の 1 時間足 + 補った足（同じ下端）、started_at 昇順
-- 現行から変えた点: 補う分の始まりが UTC の今日の 0 時 → 最後の 1 時間足の次（@fill_start。最大 3 日前。新系の定期クエリは
-- 毎日 07:40 JST に前日 07:00 JST までを作るため）。補った足の月も限月の候補に入れる。OFFSET → SAFE_OFFSET（範囲外で 500 にしない）。
-- 同じ created_at の行の順を target_price で決める（#49 と同じ）。
-- @hourly_start = LEAST(@days_start, @months_since)（1 時間足を読む下端。アプリが渡す）。
-- 期間の上端（#77）: 足は started_at < @range_end だけ返す（/atm_iv_data は now）。/atm_iv_bars で過去の範囲だけを読むとき
-- （@range_end <= @fill_start）はアプリが @fill_end を @fill_start より前にする（逆向きの区間はパーティションを読まず 0 行。
-- 同じ時刻の空の区間は 1 日分のパーティションを読む）。そのとき限月の候補は 1 時間足（直近 10 日）だけになる。
-- -- source:begin 〜 -- source:end は apps/viewer/tests/test_sql_bq.py が固定値の入力に差し替える印
WITH
-- source:begin
atm AS (
  SELECT created_at, last_trading_day, target_price, iv, price
  FROM `${project}.${dataset}.option_price_latest`(@fill_start, @fill_end)
  WHERE is_atm AND type = 2
),
hourly AS (
  SELECT open, high, low, close, last_trading_day, started_at
  FROM `${project}.${dataset}.atm_iv`
  WHERE started_at >= @hourly_start AND started_at < @fill_start
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
)
-- bars:end
,
month AS (
  SELECT
    ARRAY_AGG(DISTINCT last_trading_day IGNORE NULLS ORDER BY last_trading_day)[SAFE_OFFSET(@n)] AS last_trading_day
  FROM (
    SELECT last_trading_day FROM hourly WHERE started_at > @months_since
    UNION ALL
    SELECT last_trading_day FROM bars
  )
  WHERE last_trading_day >= @today
)
SELECT open, high, low, close, started_at
FROM (
  SELECT open, high, low, close, started_at
  FROM hourly
  WHERE last_trading_day = (SELECT last_trading_day FROM month) AND started_at >= @days_start AND started_at < @range_end
  UNION ALL
  SELECT iv_open AS open, iv_high AS high, iv_low AS low, iv_close AS close, started_at
  FROM bars
  WHERE last_trading_day = (SELECT last_trading_day FROM month) AND started_at >= @days_start AND started_at < @range_end
)
ORDER BY started_at
