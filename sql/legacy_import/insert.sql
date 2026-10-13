-- 過去データの移行（#12、target.md 6 章 3a）: 一時データセット ${import_dataset}（旧系の表を旧スキーマのまま東京へコピーしたもの）から、
-- created_at が [@start_at, @end_at) の旧系のスナップショット T（3 表の created_at）のうち、${dataset}.ingest_runs に
-- 同じ snapshot_at の記録が 1 件も無いものを ${dataset} へ追記する。新系の稼働前（〜2026-10-12 23:39 UTC）はすべて、
-- 並行稼働中は新系の取りこぼしだけが対象になる（新系の記録がある T は新系を正とし、同じ T に新旧を混ぜない）。
--
-- 移行行の見え方: T ごとに ingest_run_id = 'legacy-<T の UTC, YYYYMMDDTHHMMSSZ>' とし、ingest_runs に完了記録
-- （trigger = 'legacy_import'、parser_version = 'legacy'、source_object = 'legacy:optionchan'、started_at = finished_at = T）を書く。
-- 最新版のテーブル関数（sql/latest/）はそのまま移行行を返す。finished_at = T なので、後で同じ T を新系が再処理すれば新系が選ばれる。
-- データの表の追加列（page_updated_at・price_time_raw・parser_version・source_object）は NULL（元データのまま。#4 は別ステップ）。
--
-- 冪等性: 記録の無い T だけを選ぶので、同じ範囲を流し直しても重複しない。データの表 → ingest_runs の順に書くので、途中で失敗しても
-- テーブル関数からは見えず、流し直しの最初に残骸（完了記録の無い legacy- の行）を消す。
-- end_at は品質チェック済みの窓の終わり（quality_daily の最大の window_end）以下に限る（定期の品質チェック・突き合わせが移行行を見ないように）。
-- 名前（${project} 等）は runbook の render が、期間は bq のクエリパラメータ（--parameter=start_at:TIMESTAMP:...）が入れる。
-- -- source:begin 〜 -- source:end / -- result:begin 〜 -- result:end は tests/test_legacy_import_bq.py が固定値の入力に差し替える印。
DECLARE start_at TIMESTAMP DEFAULT @start_at;
DECLARE end_at TIMESTAMP DEFAULT @end_at;
DECLARE checked_until TIMESTAMP DEFAULT (SELECT MAX(window_end) FROM `${project}.${dataset}.quality_daily`);

IF start_at IS NULL OR end_at IS NULL OR start_at >= end_at THEN
  SELECT ERROR(FORMAT('legacy_import: invalid range [%t, %t)', start_at, end_at));
END IF;

IF checked_until IS NULL OR end_at > checked_until THEN
  SELECT ERROR(FORMAT('legacy_import: end_at %t is after the last checked window end %t', end_at, checked_until));
END IF;

-- 1. 前回の途中失敗の残骸（データの表に書けたが完了記録の無い legacy- の行）を消す
DELETE FROM `${project}.${dataset}.spot_price`
WHERE created_at >= start_at AND created_at < end_at
  AND STARTS_WITH(ingest_run_id, 'legacy-')
  AND ingest_run_id NOT IN (
    SELECT ingest_run_id FROM `${project}.${dataset}.ingest_runs` WHERE snapshot_at >= start_at AND snapshot_at < end_at
  );

DELETE FROM `${project}.${dataset}.future_price`
WHERE created_at >= start_at AND created_at < end_at
  AND STARTS_WITH(ingest_run_id, 'legacy-')
  AND ingest_run_id NOT IN (
    SELECT ingest_run_id FROM `${project}.${dataset}.ingest_runs` WHERE snapshot_at >= start_at AND snapshot_at < end_at
  );

DELETE FROM `${project}.${dataset}.option_price`
WHERE created_at >= start_at AND created_at < end_at
  AND STARTS_WITH(ingest_run_id, 'legacy-')
  AND ingest_run_id NOT IN (
    SELECT ingest_run_id FROM `${project}.${dataset}.ingest_runs` WHERE snapshot_at >= start_at AND snapshot_at < end_at
  );

-- 2. 移す T と、その完了記録の値
CREATE TEMP TABLE targets AS
-- result:begin
WITH
-- source:begin
legacy_spot AS (
  SELECT created_at, COUNT(*) AS n
  FROM `${project}.${import_dataset}.spot_price`
  WHERE created_at >= start_at AND created_at < end_at
  GROUP BY created_at
),
legacy_future AS (
  SELECT created_at, COUNT(*) AS n
  FROM `${project}.${import_dataset}.future_price`
  WHERE created_at >= start_at AND created_at < end_at
  GROUP BY created_at
),
legacy_option AS (
  SELECT created_at, COUNT(*) AS n
  FROM `${project}.${import_dataset}.option_price`
  WHERE created_at >= start_at AND created_at < end_at
  GROUP BY created_at
),
recorded AS (
  SELECT DISTINCT snapshot_at
  FROM `${project}.${dataset}.ingest_runs`
  WHERE snapshot_at >= start_at AND snapshot_at < end_at
),
-- source:end
snapshots AS (
  SELECT created_at, SUM(n_spot) AS rows_spot, SUM(n_future) AS rows_future, SUM(n_option) AS rows_option
  FROM (
    SELECT created_at, n AS n_spot, 0 AS n_future, 0 AS n_option FROM legacy_spot
    UNION ALL
    SELECT created_at, 0, n, 0 FROM legacy_future
    UNION ALL
    SELECT created_at, 0, 0, n FROM legacy_option
  )
  GROUP BY created_at
)
SELECT
  FORMAT('legacy-%s', FORMAT_TIMESTAMP('%Y%m%dT%H%M%SZ', s.created_at, 'UTC')) AS ingest_run_id,
  s.created_at AS snapshot_at,
  s.rows_spot,
  s.rows_future,
  s.rows_option
FROM snapshots AS s
WHERE s.created_at NOT IN (SELECT snapshot_at FROM recorded)
-- result:end
;

-- 秒未満の created_at があると id が重なる（2026-10 の実測では旧系の created_at はすべて分ちょうど）
IF EXISTS (SELECT 1 FROM targets GROUP BY ingest_run_id HAVING COUNT(*) > 1) THEN
  SELECT ERROR('legacy_import: duplicate ingest_run_id in targets (sub-second created_at)');
END IF;

-- 3. データの表（旧スキーマの列 + ingest_run_id。追加列は NULL）
INSERT INTO `${project}.${dataset}.spot_price` (price, price_time, diff, diff_rate, hv, created_at, ingest_run_id)
SELECT l.price, l.price_time, l.diff, l.diff_rate, l.hv, l.created_at, t.ingest_run_id
FROM `${project}.${import_dataset}.spot_price` AS l
JOIN targets AS t ON t.snapshot_at = l.created_at
WHERE l.created_at >= start_at AND l.created_at < end_at;

INSERT INTO `${project}.${dataset}.future_price` (price, price_time, diff, diff_rate, hv, contract_month, created_at, ingest_run_id)
SELECT l.price, l.price_time, l.diff, l.diff_rate, l.hv, l.contract_month, l.created_at, t.ingest_run_id
FROM `${project}.${import_dataset}.future_price` AS l
JOIN targets AS t ON t.snapshot_at = l.created_at
WHERE l.created_at >= start_at AND l.created_at < end_at;

INSERT INTO `${project}.${dataset}.option_price` (
  type, target_price, is_atm, price, price_time, diff, diff_rate, iv,
  bid, bid_volume, bid_iv, ask, ask_volume, ask_iv, volume, positions,
  quotation, quotation_date, delta, gamma, theta, vega, last_trading_day, created_at, ingest_run_id)
SELECT
  l.type, l.target_price, l.is_atm, l.price, l.price_time, l.diff, l.diff_rate, l.iv,
  l.bid, l.bid_volume, l.bid_iv, l.ask, l.ask_volume, l.ask_iv, l.volume, l.positions,
  l.quotation, l.quotation_date, l.delta, l.gamma, l.theta, l.vega, l.last_trading_day, l.created_at, t.ingest_run_id
FROM `${project}.${import_dataset}.option_price` AS l
JOIN targets AS t ON t.snapshot_at = l.created_at
WHERE l.created_at >= start_at AND l.created_at < end_at;

-- 4. 最後に完了記録（新系と同じ順。ここまで来なければテーブル関数からは見えない）
INSERT INTO `${project}.${dataset}.ingest_runs` (
  ingest_run_id, snapshot_at, page2_updated_at, source_object, source_generation, parser_version, trigger,
  rows_spot, rows_future, rows_option, started_at, finished_at)
SELECT
  ingest_run_id, snapshot_at, CAST(NULL AS TIMESTAMP), 'legacy:optionchan', CAST(NULL AS INT64), 'legacy', 'legacy_import',
  rows_spot, rows_future, rows_option, snapshot_at, snapshot_at
FROM targets;

SELECT
  start_at,
  end_at,
  COUNT(*) AS snapshots,
  COALESCE(SUM(rows_spot), 0) AS rows_spot,
  COALESCE(SUM(rows_future), 0) AS rows_future,
  COALESCE(SUM(rows_option), 0) AS rows_option
FROM targets;
