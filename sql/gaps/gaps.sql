-- 過去データの欠けの一覧（#110）: 旧系（functions/jpx_loader.py、2018-10〜2026-10）のスナップショットが途切れた区間ごとに、
-- 立会中の分の数・原因・#114 で作った行のある分の数・今の状態を 1 行で返す。東京で、snapshot_at が [@start_at, @end_at) の範囲について流す。
-- 読み取りだけ（表を作らない・書かない）。結果は docs/data-gaps.md に要約する（生の結果はリポジトリ・Issue に貼らない）。
--
-- 区間: 旧系のスナップショット（ingest_runs の trigger = 'legacy_import'）の隣り合う 2 つ（prev_at・next_at。どちらも範囲の中）の間が
--   hole_gap_minutes（15 分。sql/tick_build/build.sql と同じ）以上あき、その間の立会中の分が hole_gap_minutes 以上ある区間。
--   範囲の外にはみ出す区間は出さないので、全期間（2018-10-01〜）で流す
-- 分: 区間の中の分の終わり c（prev_at < c < next_at。build.sql と同じ）ごとに、分 [c − 1 分, c) の始まりの JST の日付 d・時刻で数える
-- 立会中の分（先物の立会の時間。旧系は先物でページが変わるので、オプションの日中が 9:00 からだった 2021-09-17 までも 8:45 から数える）:
--   日中     d が取引日で 8:45〜15:15（2024-11-05 から 15:45）
--   夜間     夕方の日 e が取引日で、e の 16:30（2024-11-05 から 17:00）〜 翌日の 5:30（e が 2021-09-21 から 6:00）
--   取引日 = 土日でなく、trading_calendar_latest で休業日（is_trading_day = FALSE）でない日（祝日取引の日は取引日）。
--   2021-09-17（金）の夜間は J-GATE 3.0 への移行で市場全体が休み（suspended_nights。旧系の停止ではない）
-- 原因（この順に最初に当たったもの）:
--   bug_format  欠け 1。区間が 2022-09-14 15:16 〜 2023-04-25 13:35 JST（format_from・format_to。価格の表示が「値(MM/DD HH:MM)」に
--               変わり解析できなかった期間。修正 106ddad）と重なる
--   bug_comma   欠け 3。prev_at が 2024-08-06 12:25 JST（comma_fix_at。build.sql の range_end と同じ。修正 385d56b が効いた時刻）より前で、
--               前か後のスナップショットの現物・先物の前日比（diff）の絶対値の最大が comma_diff（700）以上。前日比が ±1,000 に届くと
--               カンマが付いて解析が落ちる。区間は 1,000 に届いて始まり、下がって終わるので前後は 700〜999 になりやすい（しきい値は推定）
--   no_change   仕様。前と後のスナップショットの先物の値（price）が同じ（旧系は値が変わらないと保存しない。夜間に多い）
--   unknown     それ以外（停止・取得の失敗など。前後の先物の行が無い区間も）
-- 今の状態（#114 の作った行。ingest_runs の trigger = 'legacy_fill114'、snapshot_at は分の終わり c）:
--   none     built_minutes = 0
--   filled   built_minutes が session_minutes の filled_ratio（90%）以上（#114 は約定のあった分だけ作るので、約定の無い分は残る。
--            欠け 1 の作っていない分（約 4%）は米国の祝日の夜などの約定の無い分）
--   partial  それ以外
-- 原因と状態は別に決める（unknown の区間も #114 が作っていれば filled になる）。欠け 2（2023-04-25 13:42 〜 2023-05-08 16:31 JST の
-- option の price・price_time の NULL）は行の欠けではないので出さない（#111 で埋めた。docs/data-gaps.md）。
-- 表に残さない理由: 旧系は停止していて（#14）過去の旧系の行はもう増えないので、一度流して docs に要約すれば足りる。
-- 読む列: ingest_runs の snapshot_at・trigger、spot_price・future_price の旧系の行の created_at・price・diff（option_price は読まない）。
-- 名前（${project} 等）は runbook の render が、範囲は bq のクエリパラメータが入れる。試験は tests/test_gaps.py・tests/test_gaps_bq.py。
DECLARE start_at TIMESTAMP DEFAULT @start_at;
DECLARE end_at TIMESTAMP DEFAULT @end_at;
DECLARE hole_gap_minutes INT64 DEFAULT 15;
DECLARE format_from TIMESTAMP DEFAULT TIMESTAMP '2022-09-14 06:16:00+00';
DECLARE format_to TIMESTAMP DEFAULT TIMESTAMP '2023-04-25 04:35:00+00';
DECLARE comma_fix_at TIMESTAMP DEFAULT TIMESTAMP '2024-08-06 03:25:00+00';
DECLARE comma_diff INT64 DEFAULT 700;
DECLARE filled_ratio FLOAT64 DEFAULT 0.9;
-- 立会の時間の変更日（日中の終わり 15:15 → 15:45・夜間の始まり 16:30 → 17:00 は d・e、夜間の終わり 5:30 → 6:00 は夕方の日 e で比べる）
DECLARE day_close_change DATE DEFAULT DATE '2024-11-05';
DECLARE night_close_change DATE DEFAULT DATE '2021-09-21';
DECLARE suspended_nights ARRAY<DATE> DEFAULT [DATE '2021-09-17'];
DECLARE calendar_from DATE DEFAULT (
  SELECT DATE_TRUNC(MIN(calendar_date), YEAR) FROM `${project}.${dataset}.trading_calendar_latest`
);
DECLARE closed_trading ARRAY<DATE> DEFAULT (
  SELECT IFNULL(ARRAY_AGG(calendar_date), []) FROM `${project}.${dataset}.trading_calendar_latest` WHERE NOT is_trading_day
);
DECLARE missing_new_year ARRAY<INT64> DEFAULT (
  SELECT ARRAY_AGG(y ORDER BY y)
  FROM UNNEST(GENERATE_ARRAY(EXTRACT(YEAR FROM DATE(start_at, 'Asia/Tokyo')), EXTRACT(YEAR FROM DATE(end_at, 'Asia/Tokyo')))) AS y
  -- 版の行は平日だけなので、1/1〜1/3（年始の休業日。3 日のどれかは平日）のどれかがあればその年はある（build.sql と同じ）
  WHERE y NOT IN (
    SELECT EXTRACT(YEAR FROM calendar_date) FROM `${project}.${dataset}.trading_calendar_latest`
    WHERE EXTRACT(MONTH FROM calendar_date) = 1 AND EXTRACT(DAY FROM calendar_date) <= 3
  )
);

IF start_at IS NULL OR end_at IS NULL OR start_at >= end_at THEN
  SELECT ERROR(FORMAT('gaps: invalid range [%t, %t)', start_at, end_at));
END IF;

IF calendar_from IS NULL OR calendar_from > DATE(start_at, 'Asia/Tokyo') THEN
  SELECT ERROR(FORMAT('gaps: trading calendar starts at %t, needs %t (#67)', calendar_from, DATE(start_at, 'Asia/Tokyo')));
END IF;

IF ARRAY_LENGTH(IFNULL(missing_new_year, [])) > 0 THEN
  SELECT ERROR(FORMAT('gaps: trading calendar has no new-year rows (1/1-1/3) for years %t (#67)', missing_new_year));
END IF;

WITH
runs AS (
  SELECT snapshot_at, trigger FROM `${project}.${dataset}.ingest_runs`
  WHERE snapshot_at >= start_at AND snapshot_at < end_at
    AND trigger IN ('legacy_import', 'legacy_fill114')
),
legacy AS (
  SELECT snapshot_at AS prev_at, LEAD(snapshot_at) OVER (ORDER BY snapshot_at) AS next_at
  FROM (SELECT DISTINCT snapshot_at FROM runs WHERE trigger = 'legacy_import')
),
holes AS (
  SELECT prev_at, next_at FROM legacy
  WHERE TIMESTAMP_DIFF(next_at, prev_at, MINUTE) >= hole_gap_minutes
),
minutes AS (
  SELECT prev_at, c, DATE(t) AS d, TIME(t) AS tm, DATE_SUB(DATE(t), INTERVAL 1 DAY) AS e
  FROM (
    SELECT h.prev_at, c, DATETIME(TIMESTAMP_SUB(c, INTERVAL 1 MINUTE), 'Asia/Tokyo') AS t
    FROM holes AS h,
      UNNEST(GENERATE_TIMESTAMP_ARRAY(TIMESTAMP_ADD(h.prev_at, INTERVAL 1 MINUTE), TIMESTAMP_SUB(h.next_at, INTERVAL 1 MINUTE),
        INTERVAL 1 MINUTE)) AS c
  )
),
session AS (
  SELECT prev_at, c FROM (
    SELECT prev_at, c, d, tm, e,
      EXTRACT(DAYOFWEEK FROM d) NOT IN (1, 7) AND d NOT IN UNNEST(closed_trading) AS d_open,
      EXTRACT(DAYOFWEEK FROM e) NOT IN (1, 7) AND e NOT IN UNNEST(closed_trading) AS e_open
    FROM minutes
  )
  -- 日中
  WHERE (d_open AND tm >= TIME '08:45:00' AND tm < IF(d < day_close_change, TIME '15:15:00', TIME '15:45:00'))
    -- 夜間の夕方（その日 d が夕方の日）
    OR (d_open AND d NOT IN UNNEST(suspended_nights) AND tm >= IF(d < day_close_change, TIME '16:30:00', TIME '17:00:00'))
    -- 夜間の翌朝（前の日 e が夕方の日）
    OR (e_open AND e NOT IN UNNEST(suspended_nights) AND tm < IF(e < night_close_change, TIME '05:30:00', TIME '06:00:00'))
),
counts AS (
  SELECT s.prev_at, COUNT(*) AS session_minutes, COUNTIF(b.snapshot_at IS NOT NULL) AS built_minutes
  FROM session AS s
  LEFT JOIN (SELECT DISTINCT snapshot_at FROM runs WHERE trigger = 'legacy_fill114') AS b ON b.snapshot_at = s.c
  GROUP BY s.prev_at
),
-- 旧系の行だけ（#114 の作った行も 'legacy-' で始まるので除く）。1 つのスナップショットに 1 行（重複は created_at ごとにまとめる。
-- 値は近い限月・値の小さい方の行で決める。ANY_VALUE だと複数行のスナップショットで流すたびに no_change が変わりうる）
fut AS (
  SELECT created_at, ARRAY_AGG(price ORDER BY contract_month, price LIMIT 1)[OFFSET(0)] AS price, MAX(ABS(diff)) AS abs_diff
  FROM `${project}.${dataset}.future_price`
  WHERE created_at >= start_at AND created_at < end_at
    AND STARTS_WITH(ingest_run_id, 'legacy-') AND NOT STARTS_WITH(ingest_run_id, 'legacy-fill114-')
  GROUP BY created_at
),
spot AS (
  SELECT created_at, MAX(ABS(diff)) AS abs_diff
  FROM `${project}.${dataset}.spot_price`
  WHERE created_at >= start_at AND created_at < end_at
    AND STARTS_WITH(ingest_run_id, 'legacy-') AND NOT STARTS_WITH(ingest_run_id, 'legacy-fill114-')
  GROUP BY created_at
),
listed AS (
  SELECT h.prev_at, h.next_at, n.session_minutes, n.built_minutes,
    (SELECT MAX(x) FROM UNNEST([fp.abs_diff, sp.abs_diff, fn.abs_diff, sn.abs_diff]) AS x) AS max_abs_diff,
    fp.price = fn.price AS same_price
  FROM holes AS h
  JOIN counts AS n USING (prev_at)
  LEFT JOIN fut AS fp ON fp.created_at = h.prev_at
  LEFT JOIN fut AS fn ON fn.created_at = h.next_at
  LEFT JOIN spot AS sp ON sp.created_at = h.prev_at
  LEFT JOIN spot AS sn ON sn.created_at = h.next_at
  WHERE n.session_minutes >= hole_gap_minutes
)
SELECT
  prev_at,
  next_at,
  DATETIME(prev_at, 'Asia/Tokyo') AS prev_jst,
  DATETIME(next_at, 'Asia/Tokyo') AS next_jst,
  TIMESTAMP_DIFF(next_at, prev_at, MINUTE) AS gap_minutes,
  session_minutes,
  built_minutes,
  CASE
    WHEN prev_at < format_to AND next_at > format_from THEN 'bug_format'
    WHEN prev_at < comma_fix_at AND IFNULL(max_abs_diff, 0) >= comma_diff THEN 'bug_comma'
    WHEN IFNULL(same_price, FALSE) THEN 'no_change'
    ELSE 'unknown'
  END AS cause,
  CASE
    WHEN built_minutes = 0 THEN 'none'
    WHEN built_minutes >= filled_ratio * session_minutes THEN 'filled'
    ELSE 'partial'
  END AS status,
  max_abs_diff
FROM listed
ORDER BY prev_at;
