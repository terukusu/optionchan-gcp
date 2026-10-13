-- 日次の品質チェック（#40、target.md 3.4）。BigQuery Data Transfer Service のスケジュールされたクエリ quality-daily
-- （asia-northeast1、実行 SA sa-sq-dts、毎日 07:15 JST。ingest の 07:00 の照合・回収の後）の本文。プロジェクトとデータセットは Terraform の templatefile が入れる。
-- 窓は @run_time の JST の日付 D に対し [D-1 07:00, D 07:00) JST（日中 + 夜間の 1 日分。ingest の 07:00 の照合と同じ）。
-- 項目ごとの違反件数を quality_daily に記録し（同じ日は置き換え）、違反が 1 件以上なら ERROR() で失敗させる（新系の M2 で通知）。
-- 休場日・週末はデータが無いので違反 0。祝日取引の日付は違反にしない。限月の入れ替わり（SQ）はスナップショットごとの検査なので影響しない。
-- -- source:begin 〜 -- source:end / -- result:begin 〜 -- result:end は tests/test_sql_bq.py が固定値の入力に差し替える印。
DECLARE win_end TIMESTAMP DEFAULT TIMESTAMP(DATETIME(DATE(@run_time, 'Asia/Tokyo'), TIME '07:00:00'), 'Asia/Tokyo');
DECLARE win_start TIMESTAMP DEFAULT TIMESTAMP_SUB(win_end, INTERVAL 1 DAY);

IF win_end > CURRENT_TIMESTAMP() THEN
  SELECT ERROR(FORMAT('quality: win_end %t is in the future (run_time %t)', win_end, @run_time));
END IF;

CREATE TEMP TABLE result AS
-- result:begin
WITH
-- source:begin
runs AS (
  SELECT ingest_run_id, rows_spot, rows_future, rows_option
  FROM `${project}.${dataset}.ingest_runs`
  WHERE snapshot_at >= win_start AND snapshot_at < win_end
),
latest_runs AS (
  SELECT snapshot_at FROM `${project}.${dataset}.latest_runs`(win_start, win_end)
),
spot_l AS (
  SELECT created_at, page_updated_at, price_time FROM `${project}.${dataset}.spot_price_latest`(win_start, win_end)
),
future_l AS (
  SELECT created_at, page_updated_at, price_time FROM `${project}.${dataset}.future_price_latest`(win_start, win_end)
),
option_l AS (
  SELECT created_at, page_updated_at, price_time, type, target_price, last_trading_day, is_atm
  FROM `${project}.${dataset}.option_price_latest`(win_start, win_end)
),
raw_counts AS (
  SELECT 'spot' AS tbl, ingest_run_id, COUNT(*) AS n
  FROM `${project}.${dataset}.spot_price`
  WHERE created_at >= win_start AND created_at < win_end
  GROUP BY ingest_run_id
  UNION ALL
  SELECT 'future', ingest_run_id, COUNT(*)
  FROM `${project}.${dataset}.future_price`
  WHERE created_at >= win_start AND created_at < win_end
  GROUP BY ingest_run_id
  UNION ALL
  SELECT 'option', ingest_run_id, COUNT(*)
  FROM `${project}.${dataset}.option_price`
  WHERE created_at >= win_start AND created_at < win_end
  GROUP BY ingest_run_id
),
-- source:end
run_counts AS (
  SELECT
    r.ingest_run_id,
    r.rows_spot != COALESCE(SUM(IF(c.tbl = 'spot', c.n, 0)), 0)
      OR r.rows_future != COALESCE(SUM(IF(c.tbl = 'future', c.n, 0)), 0)
      OR r.rows_option != COALESCE(SUM(IF(c.tbl = 'option', c.n, 0)), 0) AS mismatch
  FROM runs AS r
  LEFT JOIN raw_counts AS c USING (ingest_run_id)
  GROUP BY r.ingest_run_id, r.rows_spot, r.rows_future, r.rows_option
),
expected AS (
  SELECT s.snapshot_at AS created_at, tbl
  FROM latest_runs AS s
  CROSS JOIN UNNEST(['spot', 'future', 'option']) AS tbl
),
actual AS (
  SELECT DISTINCT created_at, 'spot' AS tbl FROM spot_l
  UNION ALL
  SELECT DISTINCT created_at, 'future' FROM future_l
  UNION ALL
  SELECT DISTINCT created_at, 'option' FROM option_l
),
all_rows AS (
  SELECT created_at, page_updated_at, price_time FROM spot_l
  UNION ALL
  SELECT created_at, page_updated_at, price_time FROM future_l
  UNION ALL
  SELECT created_at, page_updated_at, price_time FROM option_l
),
checks AS (
  SELECT
    (SELECT COUNT(*) FROM runs) AS runs,
    (SELECT COUNT(*) FROM latest_runs) AS snapshots,
    (SELECT COUNT(*) FROM option_l) AS option_rows,
    (SELECT COUNTIF(mismatch) FROM run_counts) AS rowcount_mismatch,
    (
      SELECT COUNT(*)
      FROM expected AS e
      FULL JOIN actual AS a ON a.created_at = e.created_at AND a.tbl = e.tbl
      WHERE e.created_at IS NULL OR a.created_at IS NULL
    ) AS missing_table,
    (
      (SELECT COUNT(*) FROM (SELECT created_at FROM spot_l GROUP BY created_at HAVING COUNT(*) > 1))
      + (SELECT COUNT(*) FROM (SELECT created_at FROM future_l GROUP BY created_at HAVING COUNT(*) > 1))
      + (
        SELECT COUNT(*)
        FROM (
          SELECT 1 FROM option_l
          GROUP BY created_at, type, target_price, last_trading_day
          HAVING COUNT(*) > 1
        )
      )
    ) AS dup_keys,
    (
      SELECT COUNT(*)
      FROM (SELECT created_at FROM option_l GROUP BY created_at HAVING COUNT(DISTINCT last_trading_day) != 2)
    ) AS months_not_2,
    (
      SELECT COUNT(*)
      FROM (
        SELECT 1 FROM option_l
        GROUP BY created_at, last_trading_day, type
        HAVING COUNTIF(is_atm) != 1
      )
    ) AS atm_not_1,
    (
      SELECT COUNTIF(price_time > DATETIME(COALESCE(page_updated_at, created_at), 'Asia/Tokyo')) FROM all_rows
    ) AS price_time_future,
    (
      SELECT COUNTIF(price_time < DATETIME_SUB(DATETIME(created_at, 'Asia/Tokyo'), INTERVAL 30 DAY)) FROM all_rows
    ) AS price_time_too_old,
    (
      SELECT COUNTIF(
        EXTRACT(DAYOFWEEK FROM price_time) = 1
        OR EXTRACT(DAYOFWEEK FROM DATETIME(created_at, 'Asia/Tokyo')) = 1
      )
      FROM all_rows
    ) AS sunday,
    (
      SELECT COALESCE(SUM(n), 0) FROM raw_counts
      WHERE ingest_run_id IS NULL OR ingest_run_id NOT IN (SELECT ingest_run_id FROM runs)
    ) AS orphan_rows
)
SELECT
  DATE(win_start, 'Asia/Tokyo') AS window_date,
  win_start AS window_start,
  win_end AS window_end,
  runs,
  snapshots,
  option_rows,
  rowcount_mismatch,
  missing_table,
  dup_keys,
  months_not_2,
  atm_not_1,
  price_time_future,
  price_time_too_old,
  sunday,
  orphan_rows,
  rowcount_mismatch + missing_table + dup_keys + months_not_2 + atm_not_1
    + price_time_future + price_time_too_old + sunday AS violations,
  CURRENT_TIMESTAMP() AS checked_at
FROM checks
-- result:end
;

-- 同じ日の行を置き換える（再実行しても 1 行。1 文なので途中で失敗しても半端にならない）
MERGE `${project}.${dataset}.quality_daily` AS t
USING result AS s
ON FALSE
WHEN NOT MATCHED BY SOURCE AND t.window_date = DATE(win_start, 'Asia/Tokyo') THEN DELETE
WHEN NOT MATCHED THEN INSERT ROW;

IF (SELECT violations FROM result) > 0 THEN
  SELECT ERROR(FORMAT('quality: %s', (SELECT TO_JSON_STRING(r) FROM result AS r)));
END IF;
