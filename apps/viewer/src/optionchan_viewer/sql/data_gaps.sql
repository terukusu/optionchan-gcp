-- /data_gaps（#123）: 過去データの欠けの一覧の表 data_gaps の全体（数百行。表は docs/runbook.md の手順で sql/gaps/gaps.sql の
-- 結果を入れる。入れていなければ 0 行）。時刻は UTC の UNIX 秒、prev_at 昇順。no_change かつ none（欠けではない区間）は api が除く
SELECT UNIX_SECONDS(prev_at) AS prev_at, UNIX_SECONDS(next_at) AS next_at, cause, status
FROM `${project}.${dataset}.data_gaps`
ORDER BY prev_at
