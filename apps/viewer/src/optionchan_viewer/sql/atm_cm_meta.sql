-- /atm_iv_bars?n=cm（#100）のクエリ 1: atm_cm_iv（定期クエリ sql/cm/atm_cm.sql）の [@start_at, @end_at)（アプリが now − 14 日〜now を
-- 渡す）の最後の行の時刻。アプリはこの 1 秒後（無ければ now − 3 日の正時。補う範囲は最大 3 日）からを生データで補う（sql/atm_cm.sql の @fill_start）
SELECT MAX(created_at) AS last_created_at
FROM `${project}.${dataset}.atm_cm_iv`
WHERE created_at >= @start_at AND created_at < @end_at
