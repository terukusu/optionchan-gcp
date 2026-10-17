-- 過去データの行の作成（#114）: #110 の欠け 1（2022-09-14 15:16 〜 2023-04-25 13:35 JST の全欠け）と欠け 3（前日比 ±1,000 円以上での欠け。
-- 2024-08-06 12:25 JST の修正の前）の、旧系の行が 1 つも無い区間に、DataCube の歩み値（${dataset}.dc_option_tick・dc_future_tick）から
-- option_price・future_price・ingest_runs の行を作る。東京で、created_at が [@start_at, @end_at) の範囲について流す。
-- @mode = 'dryrun' は数えるだけ、'apply' は INSERT する。spot_price は作らない（元のデータが無い）。
--
-- 埋める区間（hole）: 旧系のスナップショット（ingest_runs の trigger = 'legacy_import'）の隣り合う 2 つ（prev_at・next_at）の間が
--   hole_gap_minutes（15 分）以上あき、prev_at が range_end（2024-08-06 12:25 JST）より前で、その間（prev_at < c < next_at の分の終わり c）に
--   先物の約定のある分が hole_active_minutes（10 分）以上ある区間。歩み値の月を入れていない区間は約定が無いので埋めない
-- スナップショット: 区間の中で、先物の約定か、近い 2 限月のオプションの約定があった分 [m, m + 1 分) ごとに 1 つ。created_at = m + 1 分
--   （分の終わり。その分までの約定をすべて含む。旧系の created_at も分の境目）。ingest_run_id = 'legacy-fill114-' || created_at（UTC、
--   %Y%m%dT%H%M%SZ）、parser_version = build_version、source_object = 'datacube:tick'、ingest_runs の trigger = 'legacy_fill114'
-- option の行: スナップショットの取引日 T（#111 の fill.sql と同じ。16:30 以降は翌日から数えて最初の現物の営業日）の近い 2 限月
--   （最終取引日が T 以降の月次の限月の小さい方から 2 つ）の、T の約定が created_at までに 1 つ以上ある系列（行使価格 × コール / プット）。
--   price = 最後の約定の値段、price_time = その約定日時（JST、分に切り捨て）、volume = T の約定の累計の数量（ページの取引高と同じ。#111）、
--   iv = 最後の約定の時点の IV（下のモデル）、is_atm = その限月の行のうち、行使価格が created_at の原資産に最も近い行（同じ距離なら
--   小さい方。コール・プットの両方）。気配・ギリシャ指標・前日比は NULL。祝日取引の日の扱いは fill.sql と同じ（夜間のスナップショットは
--   祝日の日中の約定を累計・最後の約定から除く。祝日の日中のスナップショットは前の夜間からの全部）
-- future の行: T の直近の四半期限月（最終取引日が T 以降の 3・6・9・12 月の最も近い限月）の、T の created_at より前の最後の約定
-- IV のモデル（旧系の JPX の iv と約 65 万件で突き合わせて決めた。runbook の節）:
--   原資産 F: 限月 M の原資産は、M 以降で最も近い四半期限月 U(M) の先物の、約定の時点（同じ時刻は先物が先）の直前の先物の約定。
--   四半期でない M は、取引日ごとの差 offset を足す（プット・コール・パリティ: 行使価格が F ± 500 の、分ごとの最後のコールとプットの
--   K + C − P − (F_C + F_P) / 2 の中央値を 5 円に丸める。その日のサンプルが 5 未満なら、10 日前からその日までのサンプルを合わせた
--   中央値（2 限月めは 1 日のサンプルが数件しか無い日が多い）。合わせても 5 未満なら offset は 0（iv_basis = 'offset_zero'。
--   対照の窓で、0 にしたときの 2 限月めの IV の差は中央値 0.1 ポイント前後で 1 限月めと同じ程度））
--   残存 = (SQ − 約定日時（JST）の 6 時間前の日付) / 365（0〜6 時の夜間は前の日から数える）。金利 0、Black-76、二分法（JS の一時関数）
--   SQ = M の第 2 金曜（現物の営業日でなければその前の営業日）、最終取引日 = SQ の前の営業日（旧系の last_trading_day と 2018-12〜2024-08 の
--   55 か月で一致）。値段が裁定の範囲の外（内在価値以下か、F（プットは K）以上）なら NULL
--
-- 書き込み（apply）: まだ作っていないスナップショット（ingest_runs に同じ ingest_run_id が無い）の行だけを、3 表に 1 つのトランザクションで
-- INSERT する。既存の行（旧系・新系・作成済み）は変えない。同じ範囲を流し直しても 2 回目は 0 行（冪等）。戻すのは rollback.sql（印で消す）。
-- 止める（ERROR）: 範囲が range_start 〜 range_end の外・mode が不正・カレンダーが範囲の年を持たない・別の版の印がある・歩み値に重複がある・
-- 埋める区間が歩み値を入れた月と入れていない月にまたがる・入れた月の取引日の歩み値が無い。
-- 名前（${project} 等）は runbook の render が、範囲・モードは bq のクエリパラメータが入れる。
-- -- source:begin 〜 -- source:end / -- result:begin 〜 -- result:end / -- functions:begin 〜 -- functions:end は tests/test_tick_build_bq.py が
-- 固定値の入力に差し替える印。accuracy.sql は source 以外が同じ（tests/test_tick_build.py が一致を見る）。
DECLARE start_at TIMESTAMP DEFAULT @start_at;
DECLARE end_at TIMESTAMP DEFAULT @end_at;
DECLARE mode STRING DEFAULT @mode;
DECLARE build_version STRING DEFAULT 'legacy-fill114-1';
DECLARE range_start TIMESTAMP DEFAULT TIMESTAMP '2018-09-30 15:00:00+00';
DECLARE range_end TIMESTAMP DEFAULT TIMESTAMP '2024-08-06 03:25:00+00';
DECLARE hole_gap_minutes INT64 DEFAULT 15;
DECLARE hole_active_minutes INT64 DEFAULT 10;
-- accuracy.sql は TRUE（範囲のすべての分で作る）。build.sql は約定のあった分だけ
DECLARE every_minute BOOL DEFAULT FALSE;
-- 歩み値を読む取引日の範囲（範囲の初日から最後の日の 14 日後まで。offset の 10 日前までの取引日を含める）
DECLARE tick_from DATE DEFAULT DATE(start_at, 'Asia/Tokyo');
DECLARE tick_to DATE DEFAULT DATE_ADD(DATE(end_at, 'Asia/Tokyo'), INTERVAL 14 DAY);
DECLARE opt_from DATE DEFAULT DATE_SUB(tick_from, INTERVAL 10 DAY);
DECLARE calendar_from DATE DEFAULT (
  SELECT DATE_TRUNC(MIN(calendar_date), YEAR) FROM `${project}.${dataset}.trading_calendar_latest`
);
DECLARE closed_trading ARRAY<DATE> DEFAULT (
  SELECT IFNULL(ARRAY_AGG(calendar_date), []) FROM `${project}.${dataset}.trading_calendar_latest` WHERE NOT is_trading_day
);
DECLARE closed_cash ARRAY<DATE> DEFAULT (
  SELECT IFNULL(ARRAY_AGG(calendar_date), []) FROM `${project}.${dataset}.trading_calendar_latest`
);
DECLARE missing_new_year ARRAY<INT64> DEFAULT (
  SELECT ARRAY_AGG(y ORDER BY y)
  FROM UNNEST(GENERATE_ARRAY(EXTRACT(YEAR FROM opt_from), EXTRACT(YEAR FROM tick_to))) AS y
  -- 版の行は平日だけなので、1/1〜1/3（年始の休業日。3 日のどれかは平日）のどれかがあればその年はある
  WHERE y NOT IN (
    SELECT EXTRACT(YEAR FROM calendar_date) FROM `${project}.${dataset}.trading_calendar_latest`
    WHERE EXTRACT(MONTH FROM calendar_date) = 1 AND EXTRACT(DAY FROM calendar_date) <= 3
  )
);
-- 範囲と交わる区間の候補（あきが hole_gap_minutes 以上）。旧系のスナップショットは ingest_runs の小さい列だけを読む
DECLARE holes ARRAY<STRUCT<prev_at TIMESTAMP, next_at TIMESTAMP>> DEFAULT (
  SELECT IFNULL(ARRAY_AGG(STRUCT(prev_at, next_at) ORDER BY prev_at), [])
  FROM (
    SELECT LAG(snapshot_at) OVER (ORDER BY snapshot_at) AS prev_at, snapshot_at AS next_at
    FROM (
      SELECT DISTINCT snapshot_at FROM `${project}.${dataset}.ingest_runs`
      WHERE snapshot_at >= TIMESTAMP_SUB(range_start, INTERVAL 30 DAY) AND snapshot_at < TIMESTAMP_ADD(range_end, INTERVAL 30 DAY)
        AND trigger = 'legacy_import'
    )
  )
  WHERE TIMESTAMP_DIFF(next_at, prev_at, MINUTE) >= hole_gap_minutes AND prev_at < range_end
    AND prev_at < end_at AND next_at > start_at
);
-- 区間の約定の分を数える取引日の範囲（区間の全体。範囲の外にはみ出す区間も全体で数える）
DECLARE hole_from DATE DEFAULT LEAST(opt_from, IFNULL((SELECT DATE(MIN(prev_at), 'Asia/Tokyo') FROM UNNEST(holes)), opt_from));
DECLARE hole_to DATE DEFAULT GREATEST(tick_to,
  IFNULL((SELECT DATE_ADD(DATE(MAX(next_at), 'Asia/Tokyo'), INTERVAL 14 DAY) FROM UNNEST(holes)), tick_to));
DECLARE other_versions INT64 DEFAULT (
  SELECT COUNT(*) FROM `${project}.${dataset}.ingest_runs`
  WHERE snapshot_at >= start_at AND snapshot_at < end_at
    AND STARTS_WITH(ingest_run_id, 'legacy-fill114-') AND parser_version != build_version
);
DECLARE tick_duplicates INT64 DEFAULT (
  SELECT COUNT(*) FROM (
    SELECT trade_date, security_code, seq_no FROM `${project}.${dataset}.dc_option_tick`
    WHERE trade_date >= hole_from AND trade_date <= hole_to
    GROUP BY trade_date, security_code, seq_no
    HAVING COUNT(*) > 1
  )
);
DECLARE future_duplicates INT64 DEFAULT (
  SELECT COUNT(*) FROM (
    SELECT trade_date, security_code, seq_no FROM `${project}.${dataset}.dc_future_tick`
    WHERE trade_date >= hole_from AND trade_date <= hole_to
    GROUP BY trade_date, security_code, seq_no
    HAVING COUNT(*) > 1
  )
);
DECLARE mixed_holes ARRAY<TIMESTAMP> DEFAULT [];
DECLARE missing_days ARRAY<DATE> DEFAULT [];
DECLARE inserted_option INT64 DEFAULT 0;
DECLARE inserted_future INT64 DEFAULT 0;
DECLARE inserted_runs INT64 DEFAULT 0;

IF start_at IS NULL OR end_at IS NULL OR start_at >= end_at THEN
  SELECT ERROR(FORMAT('tick_build: invalid range [%t, %t)', start_at, end_at));
END IF;

IF start_at < range_start OR end_at > range_end THEN
  SELECT ERROR(FORMAT('tick_build: outside the legacy range (#114): [%t, %t)', start_at, end_at));
END IF;

IF IFNULL(mode, '') NOT IN ('dryrun', 'apply') THEN
  SELECT ERROR(FORMAT('tick_build: mode must be dryrun or apply: %s', IFNULL(mode, 'NULL')));
END IF;

IF calendar_from IS NULL OR calendar_from > opt_from THEN
  SELECT ERROR(FORMAT('tick_build: trading calendar starts at %t, needs %t (#67)', calendar_from, opt_from));
END IF;

IF ARRAY_LENGTH(IFNULL(missing_new_year, [])) > 0 THEN
  SELECT ERROR(FORMAT('tick_build: trading calendar has no new-year rows (1/1-1/3) for years %t (#67)', missing_new_year));
END IF;

-- 別の版で作った行が残っている（版を上げるときは rollback.sql で消してから流す）
IF other_versions > 0 THEN
  SELECT ERROR(FORMAT('tick_build: %d snapshots built by another version (not %s)', other_versions, build_version));
END IF;

-- 同じ約定が 2 回入っている（bq load を --replace とパーティションの指定なしで流した等）
IF tick_duplicates > 0 THEN
  SELECT ERROR(FORMAT('tick_build: dc_option_tick has %d duplicated (trade_date, security_code, seq_no)', tick_duplicates));
END IF;

IF future_duplicates > 0 THEN
  SELECT ERROR(FORMAT('tick_build: dc_future_tick has %d duplicated (trade_date, security_code, seq_no)', future_duplicates));
END IF;

-- functions:begin
-- 現物の営業日（土日と取引カレンダーの行（休業日・祝日取引の日）以外）。closed に closed_trading を渡すと先物・オプションの取引日
CREATE TEMP FUNCTION is_open(d DATE, closed ARRAY<DATE>) AS (
  EXTRACT(DAYOFWEEK FROM d) BETWEEN 2 AND 6 AND d NOT IN UNNEST(closed)
);
-- 日時 x（JST）の取引日: 16:30 以降なら翌日から、それより前なら当日から数えて最初の現物の営業日（DataCube の trade_date と同じ）
CREATE TEMP FUNCTION trade_day(x DATETIME, closed ARRAY<DATE>) AS ((
  SELECT d
  FROM UNNEST(GENERATE_DATE_ARRAY(
    DATE_ADD(DATE(x), INTERVAL IF(TIME(x) >= '16:30:00', 1, 0) DAY),
    DATE_ADD(DATE(x), INTERVAL 15 DAY))) AS d
  WHERE is_open(d, closed)
  ORDER BY d
  LIMIT 1
));
-- 約定日時（JST）。2022-07 以前の取引日の約定日は、夜間の 00:00〜08:00 の約定が夜間の始まりの日（前日）なので 1 日進める（先物も同じ）
CREATE TEMP FUNCTION exec_at(trade_date DATE, execution_date DATE, t TIME) AS (
  DATETIME(IF(trade_date < '2022-08-01' AND t < '08:00:00', DATE_ADD(execution_date, INTERVAL 1 DAY), execution_date), t)
);
-- 夜間のスナップショット（16:30 以降か 08:00 より前）
CREATE TEMP FUNCTION is_night(x DATETIME) AS (
  TIME(x) >= '16:30:00' OR TIME(x) < '08:00:00'
);
-- 約定日時 x（JST）の分の終わり（その約定を含む最初のスナップショットの created_at）
CREATE TEMP FUNCTION minute_end(x DATETIME) AS (
  TIMESTAMP_ADD(TIMESTAMP(DATETIME_TRUNC(x, MINUTE), 'Asia/Tokyo'), INTERVAL 1 MINUTE)
);
-- 限月 m（月の 1 日）の SQ: 第 2 金曜。現物の営業日でなければその前の営業日
CREATE TEMP FUNCTION sq_day(m DATE, closed ARRAY<DATE>) AS ((
  SELECT MAX(d)
  FROM UNNEST(GENERATE_DATE_ARRAY(
    DATE_ADD(m, INTERVAL MOD(13 - EXTRACT(DAYOFWEEK FROM m), 7) - 3 DAY),
    DATE_ADD(m, INTERVAL MOD(13 - EXTRACT(DAYOFWEEK FROM m), 7) + 7 DAY))) AS d
  WHERE is_open(d, closed)
));
-- 限月 m の最終取引日: SQ の前の現物の営業日
CREATE TEMP FUNCTION last_day_of(m DATE, closed ARRAY<DATE>) AS ((
  SELECT MAX(d)
  FROM UNNEST(GENERATE_DATE_ARRAY(DATE_SUB(sq_day(m, closed), INTERVAL 10 DAY), DATE_SUB(sq_day(m, closed), INTERVAL 1 DAY))) AS d
  WHERE is_open(d, closed)
));
-- 限月 m 以降で最も近い四半期限月（3・6・9・12 月）
CREATE TEMP FUNCTION quarter_month(m DATE) AS (
  DATE_ADD(DATE_TRUNC(m, QUARTER), INTERVAL 2 MONTH)
);
-- Black-76（金利 0）の IV（%）。二分法（0〜500%、100 回）。値段が裁定の範囲の外か、500% を超えるなら NULL
CREATE TEMP FUNCTION black76_iv(is_call BOOL, price FLOAT64, f FLOAT64, k FLOAT64, t FLOAT64)
RETURNS FLOAT64 LANGUAGE js AS r"""
  if (price === null || f === null || k === null || t === null || !(t > 0) || !(f > 0) || !(k > 0)) return null;
  // 標準正規分布の累積分布関数（Marsaglia 2004 の級数）
  function cdf(x) {
    if (x < -10) return 0;
    if (x > 10) return 1;
    var s = x, v = x, b = x * x;
    for (var i = 1; i < 1000; i += 2) {
      v *= b / (i + 2);
      var n = s + v;
      if (n === s) break;
      s = n;
    }
    return 0.5 + s * Math.exp(-0.5 * b - 0.91893853320467274178);
  }
  function value(sigma) {
    var st = sigma * Math.sqrt(t);
    var d1 = (Math.log(f / k) + 0.5 * st * st) / st, d2 = d1 - st;
    return is_call ? f * cdf(d1) - k * cdf(d2) : k * cdf(-d2) - f * cdf(-d1);
  }
  var lower = is_call ? Math.max(f - k, 0) : Math.max(k - f, 0);
  var upper = is_call ? f : k;
  if (!(price > lower) || !(price < upper)) return null;
  var lo = 0, hi = 5;
  if (value(hi) < price) return null;
  for (var j = 0; j < 100; j++) {
    var mid = (lo + hi) / 2;
    if (value(mid) < price) lo = mid; else hi = mid;
  }
  return (lo + hi) / 2 * 100;
""";
-- functions:end

-- 1. 埋める区間（範囲と交わる区間の候補ごとに、先物の約定のある分の数と、歩み値を入れた月か）
CREATE TEMP TABLE hole_stats AS
WITH
hole_minutes AS (
  SELECT h.prev_at, h.next_at, c, DATE(c, 'Asia/Tokyo') AS d, TIME(DATETIME(c, 'Asia/Tokyo')) >= '16:30:00' AS late
  FROM UNNEST(holes) AS h,
    UNNEST(GENERATE_TIMESTAMP_ARRAY(TIMESTAMP_ADD(h.prev_at, INTERVAL 1 MINUTE), TIMESTAMP_SUB(h.next_at, INTERVAL 1 MINUTE),
      INTERVAL 1 MINUTE)) AS c
),
day_map AS (
  -- 分の取引日は（日付, 16:30 以降か）で決まる
  SELECT d, late, trade_day(DATETIME(d, IF(late, TIME '16:30:00', TIME '00:00:00')), closed_cash) AS td
  FROM (SELECT DISTINCT d, late FROM hole_minutes)
),
fut_minutes AS (
  SELECT DISTINCT minute_end(exec_at(trade_date, execution_date, execution_time)) AS c
  FROM `${project}.${dataset}.dc_future_tick`
  WHERE trade_date >= hole_from AND trade_date <= hole_to AND index_type = '18'
),
fut_days AS (
  SELECT DISTINCT trade_date FROM `${project}.${dataset}.dc_future_tick`
  WHERE trade_date >= hole_from AND trade_date <= hole_to AND index_type = '18'
),
opt_days AS (
  SELECT DISTINCT trade_date FROM `${project}.${dataset}.dc_option_tick`
  WHERE trade_date >= hole_from AND trade_date <= hole_to AND index_type = '18'
),
hole_days AS (
  SELECT DISTINCT m.prev_at, m.next_at, p.td
  FROM hole_minutes AS m JOIN day_map AS p USING (d, late)
),
day_flags AS (
  SELECT
    h.prev_at, h.next_at, h.td,
    DATE_TRUNC(h.td, MONTH) IN (SELECT DATE_TRUNC(trade_date, MONTH) FROM fut_days) AS fut_month,
    DATE_TRUNC(h.td, MONTH) IN (SELECT DATE_TRUNC(trade_date, MONTH) FROM opt_days) AS opt_month,
    h.td IN (SELECT trade_date FROM fut_days) AS fut_day,
    h.td IN (SELECT trade_date FROM opt_days) AS opt_day
  FROM hole_days AS h
),
active AS (
  SELECT m.prev_at, m.next_at, COUNT(*) AS active_minutes
  FROM hole_minutes AS m JOIN fut_minutes AS f USING (c)
  GROUP BY m.prev_at, m.next_at
)
SELECT
  f.prev_at, f.next_at,
  IFNULL(a.active_minutes, 0) AS active_minutes,
  IFNULL(a.active_minutes, 0) >= hole_active_minutes AS qualified,
  -- 取引日の月が全部入っている / 一部だけ入っている（ERROR）
  LOGICAL_AND(f.fut_month AND f.opt_month) AS loaded,
  LOGICAL_OR(f.fut_month OR f.opt_month) AND NOT LOGICAL_AND(f.fut_month AND f.opt_month) AS mixed,
  ARRAY_AGG(IF(f.fut_month AND f.opt_month AND NOT (f.fut_day AND f.opt_day), f.td, NULL) IGNORE NULLS) AS missing_days
FROM day_flags AS f
LEFT JOIN active AS a USING (prev_at, next_at)
GROUP BY f.prev_at, f.next_at, a.active_minutes
;

-- 約定のある区間が、歩み値を入れた月と入れていない月にまたがる（月の入れ忘れ）
SET mixed_holes = (SELECT ARRAY_AGG(prev_at ORDER BY prev_at) FROM hole_stats WHERE qualified AND mixed);
IF ARRAY_LENGTH(IFNULL(mixed_holes, [])) > 0 THEN
  SELECT ERROR(FORMAT('tick_build: holes starting at %t span loaded and not-loaded months (load the months first)', mixed_holes));
END IF;

-- 入れた月の取引日の歩み値が無い（ファイルの欠け）
SET missing_days = (SELECT ARRAY_AGG(DISTINCT d ORDER BY d) FROM hole_stats, UNNEST(missing_days) AS d WHERE qualified);
IF ARRAY_LENGTH(IFNULL(missing_days, [])) > 0 THEN
  SELECT ERROR(FORMAT('tick_build: dc_option_tick or dc_future_tick has no rows for trade days %t', missing_days));
END IF;

-- 2. 行の作成（範囲の中の、埋める区間のスナップショットごと）
CREATE TEMP TABLE built AS
-- result:begin
WITH
-- source:begin
opt_tick AS (
  -- オプションの歩み値（日経225 の月次の限月だけ。Weekly は限月コードの MM が 12 より大きい）
  SELECT trade_date, execution_date, execution_time, contract_month, put_call_type, exercise_price, trade_price, trade_volume, seq_no
  FROM `${project}.${dataset}.dc_option_tick`
  WHERE trade_date >= opt_from AND trade_date <= tick_to
    AND index_type = '18' AND SAFE_CAST(SUBSTR(contract_month, 5, 2) AS INT64) <= 12
),
fut_tick AS (
  -- 先物の歩み値（日経225 先物（ラージ）。ファイルには区分 18 だけが入っている）
  SELECT trade_date, execution_date, execution_time, contract_month, trade_price, seq_no
  FROM `${project}.${dataset}.dc_future_tick`
  WHERE trade_date >= opt_from AND trade_date <= tick_to AND index_type = '18'
),
cand AS (
  -- スナップショットの候補（created_at = 分の終わり）: 埋める区間の中で、範囲の中
  SELECT c
  FROM hole_stats AS h,
    UNNEST(GENERATE_TIMESTAMP_ARRAY(TIMESTAMP_ADD(h.prev_at, INTERVAL 1 MINUTE), TIMESTAMP_SUB(h.next_at, INTERVAL 1 MINUTE),
      INTERVAL 1 MINUTE)) AS c
  WHERE h.qualified AND h.loaded AND c >= start_at AND c < end_at
),
-- source:end
ft AS (
  SELECT contract_month AS ucm, trade_date, seq_no, trade_price, exec_at(trade_date, execution_date, execution_time) AS x
  FROM fut_tick
),
ot AS (
  SELECT
    contract_month AS cm, put_call_type AS pc, CAST(exercise_price AS INT64) AS k, trade_date, seq_no, trade_price, trade_volume,
    x,
    -- 祝日取引の日中の約定（取引日は後の現物の営業日）
    DATE(x) != trade_date AND TIME(x) >= '08:00:00' AND TIME(x) < '16:30:00' AS holiday_day
  FROM opt_tick, UNNEST([exec_at(trade_date, execution_date, execution_time)]) AS x
),
cand_days AS (
  SELECT d, late, trade_day(DATETIME(d, IF(late, TIME '16:30:00', TIME '00:00:00')), closed_cash) AS td
  FROM (SELECT DISTINCT DATE(c, 'Asia/Tokyo') AS d, TIME(DATETIME(c, 'Asia/Tokyo')) >= '16:30:00' AS late FROM cand)
),
tdays AS (
  -- 取引日ごとの近い 2 限月（m1・m2）と先物の直近の四半期限月（q）
  SELECT
    td, m1, DATE_ADD(m1, INTERVAL 1 MONTH) AS m2,
    IF(last_day_of(q0, closed_cash) >= td, q0, DATE_ADD(q0, INTERVAL 3 MONTH)) AS q
  FROM (
    SELECT
      td,
      IF(last_day_of(DATE_TRUNC(td, MONTH), closed_cash) >= td, DATE_TRUNC(td, MONTH),
        DATE_ADD(DATE_TRUNC(td, MONTH), INTERVAL 1 MONTH)) AS m1,
      quarter_month(DATE_TRUNC(td, MONTH)) AS q0
    FROM (SELECT td FROM cand_days UNION DISTINCT SELECT trade_date FROM ot)
  )
),
line AS (
  -- 原資産の先物（U(M)）の約定とオプションの約定を時刻順に並べ、オプションの約定ごとに直前の先物の約定値を取る（同じ時刻は先物が先。
  -- 夜間の寄り付きで先物より先に約定したオプションは、前の取引日の最後の先物の約定）
  SELECT ucm, trade_date, x, 0 AS kind, seq_no, CAST(trade_price AS FLOAT64) AS fp,
    CAST(NULL AS STRING) AS cm, CAST(NULL AS INT64) AS pc, CAST(NULL AS INT64) AS k, CAST(NULL AS NUMERIC) AS trade_price,
    CAST(NULL AS INT64) AS trade_volume, CAST(NULL AS BOOL) AS holiday_day
  FROM ft
  UNION ALL
  SELECT FORMAT_DATE('%Y%m', quarter_month(PARSE_DATE('%Y%m', cm))), trade_date, x, 1, seq_no, NULL,
    cm, pc, k, trade_price, trade_volume, holiday_day
  FROM ot
),
oxf AS (
  SELECT * EXCEPT (kind, fp)
  FROM (
    SELECT *, LAST_VALUE(fp IGNORE NULLS) OVER (
      PARTITION BY ucm ORDER BY x, kind, seq_no ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS fx
    FROM line
  )
  WHERE kind = 1
),
pcp AS (
  -- プット・コール・パリティのサンプル: 四半期でない限月の、行使価格 × 分ごとの最後のコールと最後のプット
  SELECT
    trade_date, cm, k,
    ARRAY_AGG(IF(pc = 2, STRUCT(CAST(trade_price AS FLOAT64) AS p, fx), NULL) IGNORE NULLS
      ORDER BY x DESC, seq_no DESC LIMIT 1)[SAFE_OFFSET(0)] AS cl,
    ARRAY_AGG(IF(pc = 1, STRUCT(CAST(trade_price AS FLOAT64) AS p, fx), NULL) IGNORE NULLS
      ORDER BY x DESC, seq_no DESC LIMIT 1)[SAFE_OFFSET(0)] AS pu
  FROM oxf
  WHERE fx IS NOT NULL AND cm != ucm
  GROUP BY trade_date, cm, k, DATETIME_TRUNC(x, MINUTE)
),
pcp_sample AS (
  SELECT trade_date, cm, k + cl.p - pu.p - (cl.fx + pu.fx) / 2 AS s
  FROM pcp
  WHERE cl IS NOT NULL AND pu IS NOT NULL AND ABS(k - (cl.fx + pu.fx) / 2) <= 500
),
off_day AS (
  SELECT trade_date, cm, COUNT(*) AS n, ROUND(APPROX_QUANTILES(s, 2)[OFFSET(1)] / 5) * 5 AS off
  FROM pcp_sample
  GROUP BY trade_date, cm
),
mon AS (
  -- 取引日 × 近い 2 限月: 原資産の限月・SQ・最終取引日・offset。四半期限月は 0。四半期でない限月は、その日のサンプルが 5 以上なら
  -- その日の中央値、5 未満なら 10 日前からその日までのサンプルを合わせた中央値（合わせても 5 未満なら 0 にして off_zero）。5 円に丸める
  SELECT
    x.td, FORMAT_DATE('%Y%m', x.m) AS cm, FORMAT_DATE('%Y%m', quarter_month(x.m)) AS ucm,
    sq_day(x.m, closed_cash) AS sq, last_day_of(x.m, closed_cash) AS ltd,
    CASE
      WHEN x.m = quarter_month(x.m) THEN 0.0
      WHEN ANY_VALUE(o.n) >= 5 THEN ANY_VALUE(o.off)
      WHEN COUNT(w.s) >= 5 THEN ROUND(APPROX_QUANTILES(w.s, 2 IGNORE NULLS)[OFFSET(1)] / 5) * 5
      ELSE 0.0
    END AS off,
    x.m != quarter_month(x.m) AND IFNULL(ANY_VALUE(o.n), 0) < 5 AND COUNT(w.s) < 5 AS off_zero
  FROM (SELECT t.td, m FROM tdays AS t, UNNEST([t.m1, t.m2]) AS m) AS x
  LEFT JOIN off_day AS o ON o.cm = FORMAT_DATE('%Y%m', x.m) AND o.trade_date = x.td
  LEFT JOIN pcp_sample AS w
    ON w.cm = FORMAT_DATE('%Y%m', x.m) AND w.trade_date <= x.td AND w.trade_date >= DATE_SUB(x.td, INTERVAL 10 DAY)
  GROUP BY x.td, x.m
),
oiv AS (
  -- 近い 2 限月の約定と、その時点の IV
  SELECT
    o.cm, o.pc, o.k, o.trade_date, o.seq_no, o.trade_price, o.trade_volume, o.x, o.holiday_day,
    o.fx + m.off AS f,
    CASE
      WHEN o.fx IS NULL THEN 'no_future'
      WHEN v IS NULL THEN 'out_of_bounds'
      WHEN m.off_zero THEN 'offset_zero'
      ELSE 'ok'
    END AS iv_basis,
    ROUND(v, 2) AS iv
  FROM oxf AS o
  JOIN mon AS m ON m.td = o.trade_date AND m.cm = o.cm,
  UNNEST([black76_iv(o.pc = 2, CAST(o.trade_price AS FLOAT64), o.fx + m.off, o.k,
    DATE_DIFF(m.sq, DATE(DATETIME_SUB(o.x, INTERVAL 6 HOUR)), DAY) / 365)]) AS v
),
ost AS (
  -- 系列 × 取引日の約定の累計と最後の約定（夜間用は祝日の日中の約定を除く）
  SELECT
    cm, pc, k, trade_date, x, seq_no, minute_end(x) AS c,
    STRUCT(SUM(trade_volume) OVER w AS volume, trade_price AS p, x, iv, iv_basis, f) AS st_all,
    STRUCT(SUM(IF(holiday_day, 0, trade_volume)) OVER w AS volume,
      LAST_VALUE(IF(holiday_day, NULL, STRUCT(trade_price AS p, x, iv, iv_basis, f)) IGNORE NULLS) OVER w AS last) AS night
  FROM oiv
  WINDOW w AS (PARTITION BY cm, pc, k, trade_date ORDER BY x, seq_no ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
obk AS (
  -- 分の終わり（c）ごとの最後の状態
  SELECT
    cm, pc, k, trade_date, c, st_all,
    IF(night.last IS NULL, NULL,
      STRUCT(night.volume AS volume, night.last.p AS p, night.last.x AS x, night.last.iv AS iv, night.last.iv_basis AS iv_basis,
        night.last.f AS f)) AS st_night
  FROM ost
  WHERE TRUE
  QUALIFY ROW_NUMBER() OVER (PARTITION BY cm, pc, k, trade_date, c ORDER BY x DESC, seq_no DESC) = 1
),
active AS (
  -- 約定のあった分（先物はどの限月でも、オプションは近い 2 限月）
  SELECT minute_end(x) AS c FROM ft
  UNION DISTINCT
  SELECT c FROM obk
),
snap AS (
  SELECT k.c, d.td, is_night(DATETIME(k.c, 'Asia/Tokyo')) AS night
  FROM cand AS k
  JOIN cand_days AS d
    ON d.d = DATE(k.c, 'Asia/Tokyo') AND d.late = (TIME(DATETIME(k.c, 'Asia/Tokyo')) >= '16:30:00')
  WHERE every_minute OR k.c IN (SELECT c FROM active)
),
series AS (
  SELECT DISTINCT cm, pc, k, trade_date FROM obk
),
filled AS (
  -- スナップショット × その取引日に約定のある系列の、created_at までの最後の状態（同じ c の約定はスナップショットに入る）
  SELECT c, td, cm, pc, k, IF(night, st_night_f, st_all_f) AS st
  FROM (
    SELECT
      c, td, cm, pc, k, is_snap, night,
      LAST_VALUE(st_all IGNORE NULLS) OVER w AS st_all_f,
      LAST_VALUE(st_night IGNORE NULLS) OVER w AS st_night_f
    FROM (
      SELECT cm, pc, k, trade_date AS td, c, FALSE AS is_snap, CAST(NULL AS BOOL) AS night, st_all, st_night FROM obk
      UNION ALL
      SELECT r.cm, r.pc, r.k, s.td, s.c, TRUE, s.night, NULL, NULL
      FROM snap AS s JOIN series AS r ON r.trade_date = s.td
    )
    WINDOW w AS (PARTITION BY cm, pc, k, td ORDER BY c, is_snap ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
  )
  WHERE is_snap
),
orows AS (
  SELECT f.*, m.ucm, m.ltd, m.off
  FROM filled AS f
  JOIN mon AS m ON m.td = f.td AND m.cm = f.cm
  WHERE f.st IS NOT NULL
),
fbk AS (
  -- 先物の分の終わりごとの最後の約定
  SELECT ucm, trade_date, minute_end(x) AS c,
    ARRAY_AGG(STRUCT(trade_price AS p, x) ORDER BY x DESC, seq_no DESC LIMIT 1)[OFFSET(0)] AS st
  FROM ft
  GROUP BY ucm, trade_date, c
),
fneed AS (
  -- スナップショットごとに要る先物の限月（近い 2 限月の原資産と、直近の四半期限月）
  SELECT DISTINCT s.c, s.td, FORMAT_DATE('%Y%m', u) AS ucm
  FROM snap AS s
  JOIN tdays AS t ON t.td = s.td,
  UNNEST([quarter_month(t.m1), quarter_month(t.m2), t.q]) AS u
),
ffill AS (
  SELECT c, td, ucm, st
  FROM (
    SELECT c, td, ucm, is_snap, LAST_VALUE(st IGNORE NULLS) OVER (
      PARTITION BY ucm, td ORDER BY c, is_snap ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS st
    FROM (
      SELECT ucm, trade_date AS td, c, FALSE AS is_snap, st FROM fbk
      UNION ALL
      SELECT ucm, td, c, TRUE, NULL FROM fneed
    )
  )
  WHERE is_snap
),
atm AS (
  -- 限月ごとの ATM の行使価格: 行のある行使価格のうち、created_at の原資産（先物 + offset）に最も近いもの（同じ距離なら小さい方）
  SELECT r.c, r.cm, ARRAY_AGG(r.k ORDER BY ABS(r.k - (CAST(f.st.p AS FLOAT64) + IFNULL(r.off, 0))), r.k LIMIT 1)[OFFSET(0)] AS atm_k
  FROM (SELECT DISTINCT c, td, cm, k, ucm, off FROM orows) AS r
  JOIN ffill AS f ON f.c = r.c AND f.ucm = r.ucm
  WHERE f.st IS NOT NULL
  GROUP BY r.c, r.cm
)
SELECT
  'option' AS tbl, r.c AS created_at, r.td AS trade_day, PARSE_DATE('%Y%m', r.cm) AS contract_month, r.ltd AS last_trading_day,
  -- option_price.type: 1 = コール・2 = プット。DataCube の put_call_type: 1 = プット・2 = コール
  CASE r.pc WHEN 2 THEN 1 WHEN 1 THEN 2 END AS type,
  r.k AS target_price, IFNULL(r.k = a.atm_k, FALSE) AS is_atm,
  r.st.p AS price, DATETIME_TRUNC(r.st.x, MINUTE) AS price_time, r.st.volume AS volume, r.st.iv AS iv, r.st.iv_basis AS iv_basis
FROM orows AS r
LEFT JOIN atm AS a USING (c, cm)
UNION ALL
SELECT
  'future', f.c, f.td, PARSE_DATE('%Y%m', f.ucm), NULL, NULL, NULL, NULL,
  f.st.p, DATETIME_TRUNC(f.st.x, MINUTE), NULL, NULL, NULL
FROM ffill AS f
JOIN tdays AS t ON t.td = f.td AND f.ucm = FORMAT_DATE('%Y%m', t.q)
WHERE f.st IS NOT NULL
-- result:end
;

-- 3. スナップショット（ingest_runs の行）と、まだ作っていないか
CREATE TEMP TABLE built_runs AS
SELECT
  created_at, FORMAT('legacy-fill114-%s', FORMAT_TIMESTAMP('%Y%m%dT%H%M%SZ', created_at)) AS ingest_run_id,
  COUNTIF(tbl = 'option') AS rows_option, COUNTIF(tbl = 'future') AS rows_future
FROM built
GROUP BY created_at;

CREATE TEMP TABLE pending AS
SELECT created_at, ingest_run_id
FROM built_runs
WHERE ingest_run_id NOT IN (
  SELECT ingest_run_id FROM `${project}.${dataset}.ingest_runs`
  WHERE snapshot_at >= start_at AND snapshot_at < end_at AND STARTS_WITH(ingest_run_id, 'legacy-fill114-')
);

-- 4. 書き込み（まだ作っていないスナップショットだけ。3 表を 1 つのトランザクションで。既存の行には触れない）
IF mode = 'apply' THEN
  BEGIN TRANSACTION;

  INSERT INTO `${project}.${dataset}.option_price`
    (type, target_price, is_atm, price, price_time, iv, volume, last_trading_day, created_at, ingest_run_id, parser_version,
     source_object)
  SELECT
    b.type, b.target_price, b.is_atm, IF(b.price = TRUNC(b.price), CAST(b.price AS INT64), NULL), b.price_time, b.iv, b.volume,
    b.last_trading_day, b.created_at, p.ingest_run_id, build_version, 'datacube:tick'
  FROM built AS b JOIN pending AS p USING (created_at)
  WHERE b.tbl = 'option';
  SET inserted_option = @@row_count;

  INSERT INTO `${project}.${dataset}.future_price`
    (price, price_time, contract_month, created_at, ingest_run_id, parser_version, source_object)
  SELECT CAST(b.price AS FLOAT64), b.price_time, b.contract_month, b.created_at, p.ingest_run_id, build_version, 'datacube:tick'
  FROM built AS b JOIN pending AS p USING (created_at)
  WHERE b.tbl = 'future';
  SET inserted_future = @@row_count;

  INSERT INTO `${project}.${dataset}.ingest_runs`
    (ingest_run_id, snapshot_at, source_object, parser_version, trigger, rows_spot, rows_future, rows_option, started_at, finished_at)
  SELECT r.ingest_run_id, r.created_at, 'datacube:tick', build_version, 'legacy_fill114', 0, r.rows_future, r.rows_option,
    r.created_at, r.created_at
  FROM built_runs AS r JOIN pending AS p USING (created_at);
  SET inserted_runs = @@row_count;

  COMMIT TRANSACTION;
END IF;

-- 5. 内訳（表 × IV の確かさごと）。rows_pending・snapshots_pending は書く（書いた）行・スナップショットで、apply では inserted_* と同じ。
--    iv_basis: ok = IV あり、out_of_bounds = 値段が裁定の範囲の外（IV は NULL）、offset_zero = offset が決まらず 0 にした（IV あり）、no_future = 原資産の
--    先物の約定がまだ無い。holes_* は範囲と交わる区間の候補の数（built = 埋める、inactive = 約定のある分が少ない、unloaded = 歩み値の
--    月が無い）と、埋める区間の約定のある分の数
SELECT
  mode, start_at, end_at, b.tbl, b.iv_basis,
  COUNT(*) AS rows_total,
  COUNTIF(p.created_at IS NOT NULL) AS rows_pending,
  COUNT(DISTINCT b.created_at) AS snapshots,
  COUNT(DISTINCT p.created_at) AS snapshots_pending,
  COUNTIF(b.is_atm) AS rows_atm,
  (SELECT COUNTIF(qualified AND loaded) FROM hole_stats) AS holes_built,
  (SELECT COUNTIF(NOT qualified AND loaded) FROM hole_stats) AS holes_inactive,
  (SELECT COUNTIF(NOT loaded) FROM hole_stats) AS holes_unloaded,
  (SELECT SUM(IF(qualified AND loaded, active_minutes, 0)) FROM hole_stats) AS hole_active_minutes_total,
  inserted_option, inserted_future, inserted_runs
FROM built AS b
LEFT JOIN pending AS p USING (created_at)
GROUP BY b.tbl, b.iv_basis
ORDER BY b.tbl, b.iv_basis;
