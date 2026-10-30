-- 30 日固定の ATM IV の約定 IV からの値（#158）の合い方。JPX の気配がある期間（runbook の手順は 2020-03・2021-11・2022-08・2024-02・
-- 2024-07・2024-08 の数日）に、#114 と同じ作り方の行（-- functions:begin 〜 -- functions:end と -- result:begin 〜 -- result:end の source
-- 以外は sql/tick_build/accuracy.sql と同じ）を 5 分ごと（JST 9:00〜15:00）に一時テーブルに作り、fill.sql と同じ計算（-- trade:begin 〜
-- -- trade:end。tests/test_cm_trade.py が一致を見る）を当てて、atm_cm_iv の同じ時刻の気配からの行（basis が NULL）と比べる。読むだけ
-- （書くのはスクリプトの一時テーブルだけ）。結果は 1 行（差 = 約定からの値 − 気配からの値）:
--   rounds                  作った回（5 分ごとの、#114 と同じ作り方の行のある回）
--   quote_rounds            そのうち気配からの iv30 がある回。both_rounds はさらに約定からの iv30 もある回
--   iv_abs_p50・iv_abs_p90・iv_abs_p99   both_rounds の iv30 の差の絶対値（ポイント）。iv_bias_p50 は符号つきの中央値
--   f_abs_p50・f_abs_p90    both_rounds の m1_forward の差の絶対値（円）
--   close_days・close_abs_p50・close_abs_max   日ごとの引け（JST 14:30〜15:00 の両方ある最後の回）の日数と iv30 の差の絶対値
-- 名前は runbook の render が、窓は bq のクエリパラメータが入れる（分の境目）。DataCube の値・日ごとの値は Issue・docs に貼らない
-- （件数と分位点だけ）。
DECLARE start_at TIMESTAMP DEFAULT @start_at;
DECLARE end_at TIMESTAMP DEFAULT @end_at;
-- sql/tick_build/accuracy.sql と同じく TRUE（候補の分すべてで作る）
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
-- 約定 IV を使う時間（分）。fill.sql と同じ値
DECLARE trade_window_minutes INT64 DEFAULT 120;

IF start_at IS NULL OR end_at IS NULL OR start_at >= end_at
  OR TIMESTAMP_TRUNC(start_at, MINUTE) != start_at OR TIMESTAMP_TRUNC(end_at, MINUTE) != end_at THEN
  SELECT ERROR(FORMAT('cm_trade accuracy: invalid range [%t, %t)', start_at, end_at));
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
  -- スナップショットの候補（created_at = 分の終わり）: 窓の 5 分ごと（分が 5 の倍数）の JST 9:00〜15:00
  SELECT c FROM UNNEST(GENERATE_TIMESTAMP_ARRAY(start_at, TIMESTAMP_SUB(end_at, INTERVAL 1 MINUTE), INTERVAL 1 MINUTE)) AS c
  WHERE MOD(EXTRACT(MINUTE FROM c), 5) = 0 AND TIME(c, 'Asia/Tokyo') BETWEEN '09:00:00' AND '15:00:00'
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

-- fill.sql と同じ計算（source だけが違う。入力は作った行）
CREATE TEMP TABLE trade_cm AS
WITH
quote AS (
  SELECT created_at, last_trading_day, type, target_price, is_atm, price_time, iv
  FROM built
  WHERE tbl = 'option'
),
fut AS (
  SELECT created_at, price
  FROM built
  WHERE tbl = 'future'
),
holiday AS (
  SELECT calendar_date
  FROM `${project}.${dataset}.trading_calendar_latest`
  WHERE calendar_date >= DATE_SUB(DATE(start_at, 'Asia/Tokyo'), INTERVAL 1 DAY)
    AND calendar_date < DATE_ADD(DATE(end_at, 'Asia/Tokyo'), INTERVAL 120 DAY)
),
-- trade:begin
-- 行使価格ごとに call（type = 1）・put（type = 2）の約定 IV（iv > 0 で、約定の時刻 price_time がスナップショットの時刻（JST）の
-- trade_window_minutes 分前以降のもの）。atm = ATM の印（#114 の is_atm。先物 + offset に最も近い行使価格）
tr_strike AS (
  SELECT
    created_at,
    last_trading_day,
    target_price AS k,
    LOGICAL_OR(is_atm) AS atm,
    MAX(IF(type = 1 AND iv > 0 AND price_time >= recent, iv, NULL)) AS call_iv,
    MAX(IF(type = 2 AND iv > 0 AND price_time >= recent, iv, NULL)) AS put_iv
  FROM (SELECT *, DATETIME_SUB(DATETIME(created_at, 'Asia/Tokyo'), INTERVAL trade_window_minutes MINUTE) AS recent FROM quote)
  GROUP BY created_at, last_trading_day, target_price
),
-- 限月ごとの ATM の行使価格 k0: is_atm の行使価格（2 つあれば小さい方）。無ければ NULL（その限月は IV を出さない）
tr_atm AS (
  SELECT created_at, last_trading_day, MIN(IF(atm, k, NULL)) AS k0
  FROM tr_strike
  GROUP BY created_at, last_trading_day
),
-- ATM に近い順の番号（同じ距離は行使価格の小さい方から）
tr_near AS (
  SELECT s.*, a.k0, ROW_NUMBER() OVER (PARTITION BY created_at, last_trading_day ORDER BY ABS(k - a.k0), k) AS rn
  FROM tr_strike AS s JOIN tr_atm AS a USING (created_at, last_trading_day)
  WHERE a.k0 IS NOT NULL
),
-- ATM に近い 9 本の約定 IV と F。F はそのスナップショットの先物の値（直近の四半期限月の最後の約定）で、1 限月・2 限月とも同じ。
-- 先物の行が無ければ出さない。列は cm の cm_iv と同じ（ここから後は cm の本文と同じ計算）
cm_iv AS (
  SELECT n.created_at, n.last_trading_day, CAST(f.price AS FLOAT64) AS fwd, n.k, n.call_iv, n.put_iv
  FROM tr_near AS n JOIN fut AS f USING (created_at)
  WHERE n.rn <= 9
),
-- SQ = 最終取引日の次の営業日（土日・休業日を飛ばす）の 09:00 JST（cm の cm_sq と同じ）
cm_sq AS (
  SELECT m.last_trading_day, TIMESTAMP(DATETIME(MIN(d), TIME(9, 0, 0)), 'Asia/Tokyo') AS sq_at
  FROM (SELECT DISTINCT last_trading_day FROM quote) AS m,
    UNNEST(GENERATE_DATE_ARRAY(DATE_ADD(m.last_trading_day, INTERVAL 1 DAY), DATE_ADD(m.last_trading_day, INTERVAL 15 DAY))) AS d
  WHERE EXTRACT(DAYOFWEEK FROM d) NOT IN (1, 7) AND d NOT IN (SELECT calendar_date FROM holiday)
  GROUP BY m.last_trading_day
),
-- 行使価格ごとの IV: call と put の約定 IV の平均（片方だけならその値）。ここから cm の終わりまでは cm の本文と同じ
cm_smile AS (
  SELECT
    created_at,
    last_trading_day,
    fwd,
    k,
    IF(call_iv IS NOT NULL AND put_iv IS NOT NULL, (call_iv + put_iv) / 2, COALESCE(call_iv, put_iv)) AS s
  FROM cm_iv
),
-- F をはさむ 2 本（F 以下で最大・F より上で最小の、IV のある行使価格）
cm_bracket AS (
  SELECT
    created_at,
    last_trading_day,
    ANY_VALUE(fwd) AS fwd,
    ARRAY_AGG(IF(k <= fwd, STRUCT(k, s), NULL) IGNORE NULLS ORDER BY k DESC LIMIT 1)[SAFE_OFFSET(0)] AS lo,
    ARRAY_AGG(IF(k > fwd, STRUCT(k, s), NULL) IGNORE NULLS ORDER BY k LIMIT 1)[SAFE_OFFSET(0)] AS hi
  FROM cm_smile
  WHERE s IS NOT NULL
  GROUP BY created_at, last_trading_day
),
-- 限月ごとの補間 ATM IV: 2 本の IV を log(K / F) で線形に補間（片側が無ければ NULL）
cm_month AS (
  SELECT
    created_at,
    last_trading_day,
    fwd,
    IF(lo IS NULL OR hi IS NULL, NULL, lo.s + (hi.s - lo.s) * LN(fwd / lo.k) / LN(hi.k / lo.k)) AS iv
  FROM cm_bracket
),
-- SQ 前の限月を最終取引日の順に 1 限月・2 限月とする（ATM の印・F・IV が無い限月も数える。数えないと 2 限月が繰り上がる）。
-- days = SQ までの暦日の日数
cm_rank AS (
  SELECT
    created_at,
    last_trading_day,
    TIMESTAMP_DIFF(sq_at, created_at, SECOND) / 86400 AS days,
    fwd,
    iv,
    ROW_NUMBER() OVER (PARTITION BY created_at ORDER BY last_trading_day) AS rk
  FROM (SELECT DISTINCT created_at, last_trading_day FROM quote)
    JOIN cm_sq USING (last_trading_day) LEFT JOIN cm_month USING (created_at, last_trading_day)
  WHERE sq_at > created_at
),
cm_pair AS (
  SELECT
    created_at,
    MAX(IF(rk = 1, last_trading_day, NULL)) AS m1_last_trading_day,
    MAX(IF(rk = 1, days, NULL)) AS m1_days,
    MAX(IF(rk = 1, fwd, NULL)) AS m1_forward,
    MAX(IF(rk = 1, iv, NULL)) AS m1_iv,
    MAX(IF(rk = 2, last_trading_day, NULL)) AS m2_last_trading_day,
    MAX(IF(rk = 2, days, NULL)) AS m2_days,
    MAX(IF(rk = 2, fwd, NULL)) AS m2_forward,
    MAX(IF(rk = 2, iv, NULL)) AS m2_iv
  FROM cm_rank
  WHERE rk <= 2
  GROUP BY created_at
),
-- 30 日の IV: 1 限月の SQ まで 30 日以上なら 1 限月の IV（method = 'm1'。SQ の直後）、3〜30 日なら総分散 σ²T を T について
-- 線形に補間（method = 'interp'）、3 日を切ったら 2 限月の IV（method = 'm2'。SQ の直前）。境目で値がつながる
-- （30 日ちょうどで補間の重みが 1 限月だけ、3 日では 2 限月がほぼすべて。SQ の前後は同じ限月の IV）。どれにも当たらなければ NULL
cm_var AS (
  SELECT
    *,
    ((m2_days - 30) * m1_iv * m1_iv * m1_days + (30 - m1_days) * m2_iv * m2_iv * m2_days) / (m2_days - m1_days) AS var30
  FROM cm_pair
),
cm AS (
  SELECT
    created_at,
    CASE
      WHEN m1_days >= 30 THEN m1_iv
      WHEN m1_days >= 3 AND var30 > 0 THEN SQRT(var30 / 30)
      WHEN m1_days < 3 THEN m2_iv
    END AS iv30,
    CASE
      WHEN m1_days >= 30 AND m1_iv IS NOT NULL THEN 'm1'
      WHEN m1_days >= 3 AND m1_days < 30 AND var30 > 0 THEN 'interp'
      WHEN m1_days < 3 AND m2_iv IS NOT NULL THEN 'm2'
    END AS method,
    m1_last_trading_day,
    m1_days,
    m1_forward,
    m1_iv,
    m2_last_trading_day,
    m2_days,
    m2_forward,
    m2_iv
  FROM cm_var
)
-- trade:end
SELECT * FROM cm;

WITH
stored AS (
  SELECT created_at, iv30, m1_forward
  FROM `${project}.${dataset}.atm_cm_iv`
  WHERE created_at >= start_at AND created_at < end_at AND basis IS NULL
),
pairs AS (
  SELECT t.created_at, t.iv30 AS trade_iv30, s.iv30 AS quote_iv30, t.m1_forward AS trade_f, s.m1_forward AS quote_f
  FROM trade_cm AS t JOIN stored AS s USING (created_at)
  WHERE s.iv30 IS NOT NULL
),
matched AS (
  SELECT *, trade_iv30 - quote_iv30 AS diff FROM pairs WHERE trade_iv30 IS NOT NULL
),
closes AS (
  SELECT DATE(created_at, 'Asia/Tokyo') AS d, ARRAY_AGG(diff ORDER BY created_at DESC LIMIT 1)[OFFSET(0)] AS diff
  FROM matched
  WHERE TIME(created_at, 'Asia/Tokyo') >= '14:30:00'
  GROUP BY d
)
SELECT
  start_at, end_at,
  (SELECT COUNT(*) FROM trade_cm) AS rounds,
  (SELECT COUNT(*) FROM pairs) AS quote_rounds,
  m.*,
  c.*
FROM (
  SELECT
    COUNT(*) AS both_rounds,
    ROUND(APPROX_QUANTILES(ABS(diff), 100)[SAFE_OFFSET(50)], 3) AS iv_abs_p50,
    ROUND(APPROX_QUANTILES(ABS(diff), 100)[SAFE_OFFSET(90)], 3) AS iv_abs_p90,
    ROUND(APPROX_QUANTILES(ABS(diff), 100)[SAFE_OFFSET(99)], 3) AS iv_abs_p99,
    ROUND(APPROX_QUANTILES(diff, 100)[SAFE_OFFSET(50)], 3) AS iv_bias_p50,
    ROUND(APPROX_QUANTILES(ABS(trade_f - quote_f), 100)[SAFE_OFFSET(50)], 1) AS f_abs_p50,
    ROUND(APPROX_QUANTILES(ABS(trade_f - quote_f), 100)[SAFE_OFFSET(90)], 1) AS f_abs_p90
  FROM matched
) AS m, (
  SELECT
    COUNT(*) AS close_days,
    ROUND(APPROX_QUANTILES(ABS(diff), 2)[SAFE_OFFSET(1)], 3) AS close_abs_p50,
    ROUND(MAX(ABS(diff)), 3) AS close_abs_max
  FROM closes
) AS c;
