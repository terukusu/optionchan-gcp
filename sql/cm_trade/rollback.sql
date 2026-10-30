-- 30 日固定の ATM IV の約定 IV からの値（#158）の切り戻し。東京で、[@start_at, @end_at) の atm_cm_iv の basis = 'trade' の行を消す。
-- 気配から作った行（basis が NULL）には触れない。流し直しても安全（2 回目は 0 行）。消した時刻は、cm_backfill.sql を流さない限り
-- 行が無い（fill.sql の前と同じ。fill.sql が消した値の無い気配の行は cm_backfill.sql で戻る）。
-- 名前は runbook の render が、範囲は bq のクエリパラメータが入れる。
DECLARE start_at TIMESTAMP DEFAULT @start_at;
DECLARE end_at TIMESTAMP DEFAULT @end_at;
DECLARE deleted INT64 DEFAULT 0;

IF start_at IS NULL OR end_at IS NULL OR start_at >= end_at THEN
  SELECT ERROR(FORMAT('cm_trade rollback: invalid range [%t, %t)', start_at, end_at));
END IF;

DELETE FROM `${project}.${dataset}.atm_cm_iv`
WHERE created_at >= start_at AND created_at < end_at AND basis = 'trade';
SET deleted = @@row_count;

SELECT start_at, end_at, deleted;
