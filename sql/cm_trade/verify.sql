-- 30 日固定の ATM IV の約定 IV からの値（#158）の検証。東京で、[@start_at, @end_at) について 1 行を返す（fill.sql の作り方は使わない
-- 独立の検査。読むだけ）。fill.sql の前にも流して基準値（quote_rows）を控え、後に次がすべて 0 であることを見る:
--   duplicate_rows       atm_cm_iv の同じ created_at の 2 行目以降（印に関係なく）
--   basis_errors         basis が NULL でも 'trade' でもない行
--   outside_rows         basis = 'trade' の行のうち、時刻が #114 のスナップショット（ingest_runs の trigger = 'legacy_fill114'）でない
--   missing_snapshots    #114 のスナップショットのうち、atm_cm_iv に同じ時刻の行が無い
--   iv_range_rows        basis = 'trade' の行のうち、iv30 が 0 以下か 500 以上
--   method_errors        basis = 'trade' の行のうち、method と iv30・m1_days が合わない（iv30 が NULL なら method も NULL、m1 は
--                        m1_days が 30 以上、interp は 3 以上 30 未満、m2 は 3 未満）
--   forward_errors       basis = 'trade' の行のうち、m1_forward と m2_forward が両方あって違う（どちらも同じ先物の値）
-- 参考（0 でなくてよい）: snapshots（#114 のスナップショット）、trade_rows・trade_iv30_rows（basis = 'trade' の行・iv30 のある行。
-- trade_rows = snapshots になる）、quote_rows（basis が NULL の行。fill.sql の前後で、fill.sql が消した値の無い行の分だけ違う）、
-- trading_days・close_days・short_days（データのある取引日（土日と取引カレンダーの休業日（is_trading_day = false）を除き、JST 9:00〜15:00
-- に取り込みがある日）・引け（JST 9:00〜15:00 の最後の、iv30・m1_forward・m1_last_trading_day のある行）が 14:30 以降にある日・
-- そうでない日の一覧）。
-- 名前は runbook の render が、範囲は bq のクエリパラメータが入れる。
DECLARE start_at TIMESTAMP DEFAULT @start_at;
DECLARE end_at TIMESTAMP DEFAULT @end_at;

IF start_at IS NULL OR end_at IS NULL OR start_at >= end_at THEN
  SELECT ERROR(FORMAT('cm_trade verify: invalid range [%t, %t)', start_at, end_at));
END IF;

WITH
runs AS (
  SELECT snapshot_at, trigger
  FROM `${project}.${dataset}.ingest_runs`
  WHERE snapshot_at >= start_at AND snapshot_at < end_at
),
snaps AS (
  SELECT DISTINCT snapshot_at AS created_at FROM runs WHERE trigger = 'legacy_fill114'
),
cm AS (
  SELECT *
  FROM `${project}.${dataset}.atm_cm_iv`
  WHERE created_at >= start_at AND created_at < end_at
),
trade AS (
  SELECT * FROM cm WHERE basis = 'trade'
),
closed AS (
  SELECT calendar_date
  FROM `${project}.${dataset}.trading_calendar_latest`
  WHERE NOT is_trading_day
),
days AS (
  SELECT DISTINCT DATE(snapshot_at, 'Asia/Tokyo') AS d
  FROM runs
  WHERE TIME(snapshot_at, 'Asia/Tokyo') BETWEEN '09:00:00' AND '15:00:00'
),
closes AS (
  SELECT DATE(created_at, 'Asia/Tokyo') AS d, MAX(TIME(created_at, 'Asia/Tokyo')) AS last_at
  FROM cm
  WHERE TIME(created_at, 'Asia/Tokyo') BETWEEN '09:00:00' AND '15:00:00'
    AND iv30 IS NOT NULL AND m1_forward IS NOT NULL AND m1_last_trading_day IS NOT NULL
  GROUP BY d
),
trading AS (
  SELECT d, c.last_at
  FROM days LEFT JOIN closes AS c USING (d)
  WHERE EXTRACT(DAYOFWEEK FROM d) NOT IN (1, 7) AND d NOT IN (SELECT calendar_date FROM closed)
)
SELECT
  start_at, end_at,
  (SELECT COUNT(*) - COUNT(DISTINCT created_at) FROM cm) AS duplicate_rows,
  (SELECT COUNTIF(basis IS NOT NULL AND basis != 'trade') FROM cm) AS basis_errors,
  (SELECT COUNT(*) FROM trade WHERE created_at NOT IN (SELECT created_at FROM snaps)) AS outside_rows,
  (SELECT COUNT(*) FROM snaps WHERE created_at NOT IN (SELECT created_at FROM cm)) AS missing_snapshots,
  (SELECT COUNTIF(iv30 <= 0 OR iv30 >= 500) FROM trade) AS iv_range_rows,
  (SELECT COUNTIF(
    (iv30 IS NULL) != (method IS NULL)
    OR (method = 'm1' AND NOT m1_days >= 30)
    OR (method = 'interp' AND NOT (m1_days >= 3 AND m1_days < 30))
    OR (method = 'm2' AND NOT m1_days < 3)
    OR IFNULL(method NOT IN ('m1', 'interp', 'm2'), FALSE)) FROM trade) AS method_errors,
  (SELECT COUNTIF(m1_forward != m2_forward) FROM trade) AS forward_errors,
  (SELECT COUNT(*) FROM snaps) AS snapshots,
  (SELECT COUNT(*) FROM trade) AS trade_rows,
  (SELECT COUNTIF(iv30 IS NOT NULL) FROM trade) AS trade_iv30_rows,
  (SELECT COUNTIF(basis IS NULL) FROM cm) AS quote_rows,
  (SELECT COUNT(*) FROM trading) AS trading_days,
  (SELECT COUNTIF(last_at >= '14:30:00') FROM trading) AS close_days,
  ARRAY(SELECT d FROM trading WHERE IFNULL(last_at < '14:30:00', TRUE) ORDER BY d) AS short_days;
