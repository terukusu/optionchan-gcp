-- 1 時間足（#49）の旧系の過去分の移行。東京で、一時データセット ${import_dataset}（#12 のデータセットコピー。旧系の atm_* も運ばれる）の
-- atm_iv / atm_option_price / atm_target_price のうち started_at が [@start_at, @end_at) の行を、${dataset} の同じ名前の表へ追記する。
--   範囲: end_at は C 以下に限る。C = 新系（trigger != 'legacy_import'）の最初のスナップショットを含む窓（JST 07:00 区切り）の始まり
--         （2026-10-13 時点で 2026-10-12 22:00 UTC）。C 以降の窓は定期クエリ（sql/ohlc/atm_hourly.sql）が作る
--   重複: 同じキー (last_trading_day, time_frame, started_at) の同じ中身の行は 1 行にする。中身の違う行が 2 種類以上あるキー
--         （旧 atm_target_price の 2019-07-01 の 44 キー）は移さずに数える（その窓は定期クエリの手動実行で作り直す）
--   冪等: 新系に既にあるキーは上書きしない（MERGE の WHEN NOT MATCHED だけ）。同じ範囲を流し直しても増えない。3 表は 1 つのトランザクション
-- 結果は表ごとに 1 行（candidate_keys・inserted・skipped_existing・conflict_keys・dup_rows_collapsed）。
-- 名前は runbook の render が、期間は bq のクエリパラメータが入れる。手順は docs/runbook.md「1 時間足（#49）」の「過去分の移行」。
-- -- source:begin 〜 -- source:end / -- result:begin 〜 -- result:end は tests/test_ohlc_bq.py が固定値の入力に差し替える印。
DECLARE start_at TIMESTAMP DEFAULT @start_at;
DECLARE end_at TIMESTAMP DEFAULT @end_at;
-- ingest_runs は小さい（1 分 1 行）ので期間で絞らずに最初の 1 件を探す
DECLARE first_new TIMESTAMP DEFAULT (
  SELECT MIN(snapshot_at) FROM `${project}.${dataset}.ingest_runs` WHERE trigger != 'legacy_import'
);
DECLARE cutover TIMESTAMP DEFAULT TIMESTAMP(
  DATETIME(DATE(TIMESTAMP_SUB(first_new, INTERVAL 7 HOUR), 'Asia/Tokyo'), TIME '07:00:00'), 'Asia/Tokyo');

IF start_at IS NULL OR end_at IS NULL OR start_at >= end_at THEN
  SELECT ERROR(FORMAT('ohlc_import: invalid range [%t, %t)', start_at, end_at));
END IF;

IF cutover IS NULL OR end_at > cutover THEN
  SELECT ERROR(FORMAT('ohlc_import: end_at %t is after the first window of the new system %t', end_at, cutover));
END IF;

CREATE TEMP TABLE src AS
-- result:begin
WITH
-- source:begin
legacy AS (
  SELECT 'atm_iv' AS tbl, open, high, low, close, last_trading_day, time_frame, started_at
  FROM `${project}.${import_dataset}.atm_iv`
  WHERE started_at >= start_at AND started_at < end_at
  UNION ALL
  SELECT 'atm_option_price', open, high, low, close, last_trading_day, time_frame, started_at
  FROM `${project}.${import_dataset}.atm_option_price`
  WHERE started_at >= start_at AND started_at < end_at
  UNION ALL
  SELECT 'atm_target_price', open, high, low, close, last_trading_day, time_frame, started_at
  FROM `${project}.${import_dataset}.atm_target_price`
  WHERE started_at >= start_at AND started_at < end_at
),
existing AS (
  SELECT DISTINCT 'atm_iv' AS tbl, last_trading_day, time_frame, started_at
  FROM `${project}.${dataset}.atm_iv`
  WHERE started_at >= start_at AND started_at < end_at
  UNION ALL
  SELECT DISTINCT 'atm_option_price', last_trading_day, time_frame, started_at
  FROM `${project}.${dataset}.atm_option_price`
  WHERE started_at >= start_at AND started_at < end_at
  UNION ALL
  SELECT DISTINCT 'atm_target_price', last_trading_day, time_frame, started_at
  FROM `${project}.${dataset}.atm_target_price`
  WHERE started_at >= start_at AND started_at < end_at
),
-- source:end
legacy_keys AS (
  SELECT
    tbl, last_trading_day, time_frame, started_at,
    COUNT(*) AS n,
    COUNT(DISTINCT TO_JSON_STRING(STRUCT(open, high, low, close))) AS versions,
    ANY_VALUE(STRUCT(open, high, low, close)) AS v
  FROM legacy
  GROUP BY tbl, last_trading_day, time_frame, started_at
)
SELECT
  k.tbl,
  k.v.open,
  k.v.high,
  k.v.low,
  k.v.close,
  k.last_trading_day,
  k.time_frame,
  k.started_at,
  k.n,
  k.versions,
  e.tbl IS NOT NULL AS existing
FROM legacy_keys AS k
LEFT JOIN existing AS e USING (tbl, last_trading_day, time_frame, started_at)
-- result:end
;

BEGIN TRANSACTION;

MERGE `${project}.${dataset}.atm_iv` AS t
USING (SELECT * FROM src WHERE tbl = 'atm_iv' AND versions = 1) AS s
ON t.last_trading_day = s.last_trading_day AND t.time_frame = s.time_frame AND t.started_at = s.started_at
  AND t.started_at >= start_at AND t.started_at < end_at
WHEN NOT MATCHED THEN INSERT (open, high, low, close, last_trading_day, time_frame, started_at)
  VALUES (s.open, s.high, s.low, s.close, s.last_trading_day, s.time_frame, s.started_at);

MERGE `${project}.${dataset}.atm_option_price` AS t
USING (SELECT * FROM src WHERE tbl = 'atm_option_price' AND versions = 1) AS s
ON t.last_trading_day = s.last_trading_day AND t.time_frame = s.time_frame AND t.started_at = s.started_at
  AND t.started_at >= start_at AND t.started_at < end_at
WHEN NOT MATCHED THEN INSERT (open, high, low, close, last_trading_day, time_frame, started_at)
  VALUES (s.open, s.high, s.low, s.close, s.last_trading_day, s.time_frame, s.started_at);

MERGE `${project}.${dataset}.atm_target_price` AS t
USING (SELECT * FROM src WHERE tbl = 'atm_target_price' AND versions = 1) AS s
ON t.last_trading_day = s.last_trading_day AND t.time_frame = s.time_frame AND t.started_at = s.started_at
  AND t.started_at >= start_at AND t.started_at < end_at
WHEN NOT MATCHED THEN INSERT (open, high, low, close, last_trading_day, time_frame, started_at)
  VALUES (s.open, s.high, s.low, s.close, s.last_trading_day, s.time_frame, s.started_at);

COMMIT TRANSACTION;

SELECT
  tbl,
  start_at,
  end_at,
  cutover,
  COUNT(src.tbl) AS candidate_keys,
  COUNTIF(versions = 1 AND NOT existing) AS inserted,
  COUNTIF(versions = 1 AND existing) AS skipped_existing,
  COUNTIF(versions > 1) AS conflict_keys,
  COALESCE(SUM(IF(versions = 1, n - 1, 0)), 0) AS dup_rows_collapsed
FROM UNNEST(['atm_iv', 'atm_option_price', 'atm_target_price']) AS tbl
LEFT JOIN src USING (tbl)
GROUP BY tbl
ORDER BY tbl;
