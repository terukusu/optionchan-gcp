-- 新旧の突き合わせ（#40）の 1 段目: 旧系（US のデータセット optionchan）の前日分を US の作業用データセットに作り直す。
-- BigQuery Data Transfer Service のスケジュールされたクエリ legacy-stage（US、実行 SA sa-sq-dts、毎日 07:02 JST）の本文。
-- この後 legacy-copy（データセットコピー、07:10）が東京の作業用データセットへ運び、legacy-compare（07:30）が比べる。
-- 窓は compare と同じ [D-1 07:00, D 07:00) JST（D は @run_time の JST の日付）。列は旧スキーマのまま全部（値を比べるため）。
-- staged_window は compare がコピーの窓を確かめるための 1 行。旧系の取り込み（07:00 時点で前日の夜間分まで入っている）は変えない。
-- プロジェクトとデータセットは Terraform の templatefile が入れる。
DECLARE win_end TIMESTAMP DEFAULT TIMESTAMP(DATETIME(DATE(@run_time, 'Asia/Tokyo'), TIME '07:00:00'), 'Asia/Tokyo');
DECLARE win_start TIMESTAMP DEFAULT TIMESTAMP_SUB(win_end, INTERVAL 1 DAY);

IF win_end > CURRENT_TIMESTAMP() THEN
  SELECT ERROR(FORMAT('legacy_stage: win_end %t is in the future (run_time %t)', win_end, @run_time));
END IF;

CREATE OR REPLACE TABLE `${project}.${stage_dataset}.spot_price` AS
SELECT price, price_time, diff, diff_rate, hv, created_at
FROM `${project}.${legacy_dataset}.spot_price`
WHERE created_at >= win_start AND created_at < win_end;

CREATE OR REPLACE TABLE `${project}.${stage_dataset}.future_price` AS
SELECT price, price_time, diff, diff_rate, hv, contract_month, created_at
FROM `${project}.${legacy_dataset}.future_price`
WHERE created_at >= win_start AND created_at < win_end;

CREATE OR REPLACE TABLE `${project}.${stage_dataset}.option_price` AS
SELECT
  type, target_price, is_atm, price, price_time, diff, diff_rate, iv,
  bid, bid_volume, bid_iv, ask, ask_volume, ask_iv, volume, positions,
  quotation, quotation_date, delta, gamma, theta, vega, last_trading_day, created_at
FROM `${project}.${legacy_dataset}.option_price`
WHERE created_at >= win_start AND created_at < win_end;

-- 最後に作る（途中で失敗したら前回の窓のままになり、compare が窓の不一致で止まる）
CREATE OR REPLACE TABLE `${project}.${stage_dataset}.staged_window` AS
SELECT
  win_start AS window_start,
  win_end AS window_end,
  (SELECT COUNT(*) FROM `${project}.${stage_dataset}.spot_price`) AS spot_rows,
  (SELECT COUNT(*) FROM `${project}.${stage_dataset}.future_price`) AS future_rows,
  (SELECT COUNT(*) FROM `${project}.${stage_dataset}.option_price`) AS option_rows,
  CURRENT_TIMESTAMP() AS staged_at;
