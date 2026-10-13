-- 1 時間足（#49、target.md 3.2。旧系の insert_ohlc の置き換え）。BigQuery Data Transfer Service のスケジュールされたクエリ atm-hourly
-- （asia-northeast1、実行 SA sa-sq-dts、毎日 07:40 JST）の本文。プロジェクトとデータセットは Terraform の templatefile が入れる。
-- 窓は @run_time の JST の日付 D に対し [D-1 07:00, D 07:00) JST（日中 + 夜間の 1 日分。品質チェックと同じ）。
-- 最新版（option_price_latest）の ATM の put（is_atm AND type = 2）から、限月（last_trading_day）× 1 時間ごとの OHLC を
-- atm_iv（iv）・atm_option_price（price）・atm_target_price（target_price）に MERGE する。意味は旧系の insert_ohlc と同じ:
--   started_at = created_at を UTC の正時に切り捨て（= JST の正時。窓の境目も正時なので 1 時間足が窓をまたがない）、time_frame = 3600
--   open / close = created_at が最も早い / 遅い NULL でない値、high / low = 最大 / 最小、全部 NULL なら NULL（行は作る）
--   1 時間の中で ATM の行使価格が変わっても分けない
-- 旧系から変えた点: SAFE_OFFSET、@run_time 基準、生テーブルでなく最新版、INSERT でなく MERGE、created_at が同じ行の順序を
-- target_price で決める（旧系の重複を移した行・ATM が 2 行の版で値が不定にならないように）。
-- MERGE は窓の中を置き換える（再実行しても同じ結果。再処理・回収・移行で最新版が変わった日は手動実行で作り直す）。
-- 窓に ATM の行が 1 件も無ければ何もしない（休場日・週末と、データの無い過去の日の手動実行で既存の行を消さないため）。3 表は 1 つのトランザクションで書く。
-- -- source:begin 〜 -- source:end / -- result:begin 〜 -- result:end は tests/test_ohlc_bq.py が固定値の入力に差し替える印。
-- -- bars:begin 〜 -- bars:end（1 時間足の計算）は sql/ohlc/legacy_check.sql と一字一句同じ（tests/test_ohlc.py が検査）。
DECLARE win_end TIMESTAMP DEFAULT TIMESTAMP(DATETIME(DATE(@run_time, 'Asia/Tokyo'), TIME '07:00:00'), 'Asia/Tokyo');
DECLARE win_start TIMESTAMP DEFAULT TIMESTAMP_SUB(win_end, INTERVAL 1 DAY);

IF win_end > CURRENT_TIMESTAMP() THEN
  SELECT ERROR(FORMAT('atm_hourly: win_end %t is in the future (run_time %t)', win_end, @run_time));
END IF;

CREATE TEMP TABLE bars AS
-- result:begin
WITH
-- source:begin
atm AS (
  SELECT created_at, last_trading_day, target_price, iv, price
  FROM `${project}.${dataset}.option_price_latest`(win_start, win_end)
  WHERE is_atm AND type = 2
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
SELECT * FROM bars
-- result:end
;

IF EXISTS (SELECT 1 FROM bars) THEN
  BEGIN TRANSACTION;

  MERGE `${project}.${dataset}.atm_iv` AS t
  USING (SELECT iv_open AS open, iv_high AS high, iv_low AS low, iv_close AS close, last_trading_day, time_frame, started_at FROM bars) AS s
  ON t.last_trading_day = s.last_trading_day AND t.time_frame = s.time_frame AND t.started_at = s.started_at
    AND t.started_at >= win_start AND t.started_at < win_end
  WHEN MATCHED THEN UPDATE SET open = s.open, high = s.high, low = s.low, close = s.close
  WHEN NOT MATCHED THEN INSERT (open, high, low, close, last_trading_day, time_frame, started_at)
    VALUES (s.open, s.high, s.low, s.close, s.last_trading_day, s.time_frame, s.started_at)
  WHEN NOT MATCHED BY SOURCE AND t.started_at >= win_start AND t.started_at < win_end AND t.time_frame = 3600 THEN DELETE;

  MERGE `${project}.${dataset}.atm_option_price` AS t
  USING (SELECT price_open AS open, price_high AS high, price_low AS low, price_close AS close, last_trading_day, time_frame, started_at FROM bars) AS s
  ON t.last_trading_day = s.last_trading_day AND t.time_frame = s.time_frame AND t.started_at = s.started_at
    AND t.started_at >= win_start AND t.started_at < win_end
  WHEN MATCHED THEN UPDATE SET open = s.open, high = s.high, low = s.low, close = s.close
  WHEN NOT MATCHED THEN INSERT (open, high, low, close, last_trading_day, time_frame, started_at)
    VALUES (s.open, s.high, s.low, s.close, s.last_trading_day, s.time_frame, s.started_at)
  WHEN NOT MATCHED BY SOURCE AND t.started_at >= win_start AND t.started_at < win_end AND t.time_frame = 3600 THEN DELETE;

  MERGE `${project}.${dataset}.atm_target_price` AS t
  USING (SELECT target_price_open AS open, target_price_high AS high, target_price_low AS low, target_price_close AS close, last_trading_day, time_frame, started_at FROM bars) AS s
  ON t.last_trading_day = s.last_trading_day AND t.time_frame = s.time_frame AND t.started_at = s.started_at
    AND t.started_at >= win_start AND t.started_at < win_end
  WHEN MATCHED THEN UPDATE SET open = s.open, high = s.high, low = s.low, close = s.close
  WHEN NOT MATCHED THEN INSERT (open, high, low, close, last_trading_day, time_frame, started_at)
    VALUES (s.open, s.high, s.low, s.close, s.last_trading_day, s.time_frame, s.started_at)
  WHEN NOT MATCHED BY SOURCE AND t.started_at >= win_start AND t.started_at < win_end AND t.time_frame = 3600 THEN DELETE;

  COMMIT TRANSACTION;
END IF;
