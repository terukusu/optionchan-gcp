-- 取引カレンダーの最新版（#63）。version_at が最大の版の行だけを返す（列は schema/tokyo/trading_calendar.json と同じ）。
-- content_hash は読み手が版の内容を照合するために残す（ingest は再計算して一致しなければ使わない）。
-- 読み手（ingest・simulator 等）の契約: docs/architecture/target.md 3.2「取引カレンダー（#63）」
SELECT *
FROM `${project}.${dataset}.trading_calendar`
WHERE version_at = (SELECT MAX(version_at) FROM `${project}.${dataset}.trading_calendar`)
