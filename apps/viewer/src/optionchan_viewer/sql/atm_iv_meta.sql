-- /atm_iv_data のクエリ 1: 1 時間足 atm_iv（#49）の [@start_at, @end_at)（アプリが now − 14 日〜now を渡す）の最後の足の時刻。
-- アプリはこの次の 1 時間（無ければ now − 3 日の正時。補う範囲は最大 3 日）からを生データで補う（クエリ 2 の @fill_start）
SELECT MAX(started_at) AS last_started_at
FROM `${project}.${dataset}.atm_iv`
WHERE started_at >= @start_at AND started_at < @end_at
