-- 過去データの行の作成（#114）の一致率。旧系の行がある期間（対照の窓。runbook の手順は 2020-02・2021-11・2022-08・2024-02 の数日）に
-- build.sql と同じ作り方（-- functions:begin 〜 -- functions:end と -- result:begin 〜 -- result:end の source 以外は build.sql と同じ。
-- tests/test_tick_build.py が一致を見る）を、窓のすべての分（every_minute = TRUE）で当て、同じ created_at の旧系の行と比べる。読み取りだけ。
-- 旧系のスナップショットはページの取得の遅れ（1〜3 分）があるので、作った行を created_at − d 分（d = 0〜3）でも比べる。結果は 1 行:
--   option_rows             旧系の約定のある行（volume が NULL でない行。同じキーの重複は 1 つ）
--   built_rows              そのうち同じ created_at に作った行がある行。built_only_rows は作った行だけがある（旧系に約定の無い）行
--   price_ok・price_time_ok・volume_ok・all_ok   d = 0 で値段・約定時刻（分）・取引高・3 つとも一致した行
--   all_ok_lag              d = 0〜3 のどれかで 3 つとも一致した行（同じ約定）
--   iv_rows・iv_abs_p50・iv_abs_p90・iv_abs_p99   同じ約定（all_ok_lag の最小の d）で、旧系の iv > 0 かつ作った iv がある行の |差|（ポイント）
--   iv_null_rows            同じ約定で、旧系の iv > 0 なのに作った iv が NULL の行
--   atm_pairs・atm_ok       旧系のスナップショット × 限月（作った行もある組）と、ATM の行使価格が同じ組
--   month_pairs・month_ok   旧系のスナップショット × 限月（作った行のあるスナップショット）と、作った行に同じ限月がある組
--   future_rows・future_ok・future_ok_lag   旧系の future の行と、作った行の値段・限月が d = 0 / 0〜3 で一致した行
-- 名前は runbook の render が、窓は bq のクエリパラメータが入れる（分の境目）。build_version は build.sql と同じ値。
DECLARE start_at TIMESTAMP DEFAULT @start_at;
DECLARE end_at TIMESTAMP DEFAULT @end_at;
DECLARE build_version STRING DEFAULT 'legacy-fill114-1';
-- accuracy.sql は TRUE（範囲のすべての分で作る）。build.sql は約定のあった分だけ
DECLARE every_minute BOOL DEFAULT TRUE;
-- 歩み値を読む取引日の範囲（範囲の初日から最後の日の 14 日後まで。offset の 10 日前までの取引日を含める）
DECLARE tick_from DATE DEFAULT DATE(start_at, 'Asia/Tokyo');
DECLARE tick_to DATE DEFAULT DATE_ADD(DATE(end_at, 'Asia/Tokyo'), INTERVAL 14 DAY);
DECLARE opt_from DATE DEFAULT DATE_SUB(tick_from, INTERVAL 10 DAY);
DECLARE closed_trading ARRAY<DATE> DEFAULT (
  SELECT IFNULL(ARRAY_AGG(calendar_date), []) FROM `${project}.${dataset}.trading_calendar_latest` WHERE NOT is_trading_day
);
DECLARE closed_cash ARRAY<DATE> DEFAULT (
  SELECT IFNULL(ARRAY_AGG(calendar_date), []) FROM `${project}.${dataset}.trading_calendar_latest`
);

IF start_at IS NULL OR end_at IS NULL OR start_at >= end_at
  OR TIMESTAMP_TRUNC(start_at, MINUTE) != start_at OR TIMESTAMP_TRUNC(end_at, MINUTE) != end_at THEN
  SELECT ERROR(FORMAT('tick_build accuracy: invalid range [%t, %t)', start_at, end_at));
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
  -- スナップショットの候補（created_at = 分の終わり）: 窓のすべての分
  SELECT c FROM UNNEST(GENERATE_TIMESTAMP_ARRAY(start_at, TIMESTAMP_SUB(end_at, INTERVAL 1 MINUTE), INTERVAL 1 MINUTE)) AS c
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
  -- その日の中央値、5 未満なら 10 日前からその日までのサンプルを合わせた中央値（合わせても 5 未満なら NULL）。5 円に丸める
  SELECT
    x.td, FORMAT_DATE('%Y%m', x.m) AS cm, FORMAT_DATE('%Y%m', quarter_month(x.m)) AS ucm,
    sq_day(x.m, closed_cash) AS sq, last_day_of(x.m, closed_cash) AS ltd,
    CASE
      WHEN x.m = quarter_month(x.m) THEN 0.0
      WHEN ANY_VALUE(o.n) >= 5 THEN ANY_VALUE(o.off)
      WHEN COUNT(w.s) >= 5 THEN ROUND(APPROX_QUANTILES(w.s, 2 IGNORE NULLS)[OFFSET(1)] / 5) * 5
    END AS off
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
      WHEN m.off IS NULL THEN 'no_offset'
      WHEN v IS NULL THEN 'out_of_bounds'
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

WITH
legacy AS (
  -- 旧系の約定のある行（作った行・別の補完の印の行も旧系の値として比べる。#114 で作った行は除く）
  SELECT created_at, last_trading_day, type, target_price,
    ANY_VALUE(price) AS price, ANY_VALUE(price_time) AS price_time, ANY_VALUE(volume) AS volume, ANY_VALUE(iv) AS iv
  FROM `${project}.${dataset}.option_price`
  WHERE created_at >= start_at AND created_at < end_at
    AND STARTS_WITH(ingest_run_id, 'legacy-') AND NOT STARTS_WITH(ingest_run_id, 'legacy-fill114-') AND volume IS NOT NULL
  GROUP BY created_at, last_trading_day, type, target_price
),
legacy_atm AS (
  SELECT created_at, last_trading_day, MIN(target_price) AS atm_k
  FROM `${project}.${dataset}.option_price`
  WHERE created_at >= start_at AND created_at < end_at
    AND STARTS_WITH(ingest_run_id, 'legacy-') AND NOT STARTS_WITH(ingest_run_id, 'legacy-fill114-') AND is_atm
  GROUP BY created_at, last_trading_day
),
legacy_future AS (
  SELECT created_at, ANY_VALUE(price) AS price, ANY_VALUE(contract_month) AS contract_month
  FROM `${project}.${dataset}.future_price`
  WHERE created_at >= start_at AND created_at < end_at
    AND STARTS_WITH(ingest_run_id, 'legacy-') AND NOT STARTS_WITH(ingest_run_id, 'legacy-fill114-')
  GROUP BY created_at
),
pairs AS (
  SELECT
    l.created_at, l.last_trading_day, l.type, l.target_price, l.iv, d,
    b.created_at IS NOT NULL AS has_built,
    b.price = l.price AS price_ok, b.price_time = l.price_time AS price_time_ok, b.volume = l.volume AS volume_ok,
    b.price = l.price AND b.price_time = l.price_time AND b.volume = l.volume AS all_ok,
    b.iv AS built_iv
  FROM legacy AS l
  CROSS JOIN UNNEST([0, 1, 2, 3]) AS d
  LEFT JOIN built AS b
    ON b.tbl = 'option' AND b.created_at = TIMESTAMP_SUB(l.created_at, INTERVAL d MINUTE)
    AND b.last_trading_day = l.last_trading_day AND b.type = l.type AND b.target_price = l.target_price
),
per_row AS (
  SELECT
    created_at, last_trading_day, type, target_price,
    LOGICAL_OR(d = 0 AND has_built) AS has_built,
    LOGICAL_OR(d = 0 AND IFNULL(price_ok, FALSE)) AS price_ok,
    LOGICAL_OR(d = 0 AND IFNULL(price_time_ok, FALSE)) AS price_time_ok,
    LOGICAL_OR(d = 0 AND IFNULL(volume_ok, FALSE)) AS volume_ok,
    LOGICAL_OR(d = 0 AND IFNULL(all_ok, FALSE)) AS all_ok,
    LOGICAL_OR(IFNULL(all_ok, FALSE)) AS all_ok_lag,
    -- 同じ約定（最小の d）の IV
    ARRAY_AGG(IF(IFNULL(all_ok, FALSE), STRUCT(iv AS legacy_iv, built_iv), NULL) IGNORE NULLS ORDER BY d LIMIT 1)[SAFE_OFFSET(0)] AS same
  FROM pairs
  GROUP BY created_at, last_trading_day, type, target_price
),
option_stats AS (
  SELECT
    COUNT(*) AS option_rows,
    COUNTIF(has_built) AS built_rows,
    COUNTIF(price_ok) AS price_ok, COUNTIF(price_time_ok) AS price_time_ok, COUNTIF(volume_ok) AS volume_ok,
    COUNTIF(all_ok) AS all_ok, COUNTIF(all_ok_lag) AS all_ok_lag,
    COUNTIF(same.legacy_iv > 0 AND same.built_iv IS NOT NULL) AS iv_rows,
    APPROX_QUANTILES(IF(same.legacy_iv > 0, ABS(same.built_iv - same.legacy_iv), NULL), 100)[SAFE_OFFSET(50)] AS iv_abs_p50,
    APPROX_QUANTILES(IF(same.legacy_iv > 0, ABS(same.built_iv - same.legacy_iv), NULL), 100)[SAFE_OFFSET(90)] AS iv_abs_p90,
    APPROX_QUANTILES(IF(same.legacy_iv > 0, ABS(same.built_iv - same.legacy_iv), NULL), 100)[SAFE_OFFSET(99)] AS iv_abs_p99,
    COUNTIF(same.legacy_iv > 0 AND same.built_iv IS NULL) AS iv_null_rows
  FROM per_row
),
built_only AS (
  SELECT COUNT(*) AS built_only_rows
  FROM built AS b
  WHERE b.tbl = 'option' AND b.created_at IN (SELECT created_at FROM legacy_atm)
    AND NOT EXISTS (
      SELECT 1 FROM legacy AS l
      WHERE l.created_at = b.created_at AND l.last_trading_day = b.last_trading_day AND l.type = b.type
        AND l.target_price = b.target_price
    )
),
built_atm AS (
  SELECT created_at, last_trading_day, MIN(target_price) AS atm_k
  FROM built WHERE tbl = 'option' AND is_atm
  GROUP BY created_at, last_trading_day
),
built_months AS (
  SELECT DISTINCT created_at, last_trading_day FROM built WHERE tbl = 'option'
),
atm_stats AS (
  SELECT COUNT(*) AS atm_pairs, COUNTIF(l.atm_k = b.atm_k) AS atm_ok
  FROM legacy_atm AS l JOIN built_atm AS b USING (created_at, last_trading_day)
),
month_stats AS (
  SELECT COUNT(*) AS month_pairs, COUNTIF(b.created_at IS NOT NULL) AS month_ok
  FROM legacy_atm AS l
  LEFT JOIN built_months AS b USING (created_at, last_trading_day)
  WHERE l.created_at IN (SELECT created_at FROM built_months)
),
future_stats AS (
  SELECT
    COUNT(DISTINCT l.created_at) AS future_rows,
    COUNT(DISTINCT IF(d = 0 AND b.price = l.price AND b.contract_month = l.contract_month, l.created_at, NULL)) AS future_ok,
    COUNT(DISTINCT IF(b.price = l.price AND b.contract_month = l.contract_month, l.created_at, NULL)) AS future_ok_lag
  FROM legacy_future AS l
  CROSS JOIN UNNEST([0, 1, 2, 3]) AS d
  LEFT JOIN built AS b ON b.tbl = 'future' AND b.created_at = TIMESTAMP_SUB(l.created_at, INTERVAL d MINUTE)
)
SELECT start_at, end_at, o.*, bo.*, a.*, m.*, f.*
FROM option_stats AS o, built_only AS bo, atm_stats AS a, month_stats AS m, future_stats AS f;
