-- 30 日固定の ATM IV の約定 IV からの値（#158）。#114 で DataCube の歩み値から作った行（ingest_run_id が 'legacy-fill114-' で始まる行。
-- 気配が無いので、定期クエリ・cm_backfill.sql の気配からの計算では値が出ない）のスナップショットごとに、約定 IV と先物から 30 日固定の
-- 値を作り、印 basis = 'trade' を付けて atm_cm_iv に足す。手で流す（docs/runbook.md の「約定 IV からの 30 日固定（#158）」）。
-- 東京で、created_at が [@start_at, @end_at) の範囲について流す。@mode = 'dryrun' は数えるだけ、'apply' は書く。
--
-- 作り方（-- trade:begin 〜 -- trade:end。accuracy.sql と一字一句同じ。tests/test_cm_trade.py が検査）:
--   約定 IV: iv > 0 で、約定の時刻 price_time がスナップショットの時刻（JST）の trade_window_minutes（120 分）前以降の約定
--   ATM k0: 限月の is_atm の行使価格（2 つなら小さい方）
--   F: そのスナップショットの #114 の先物の値（直近の四半期限月の最後の約定）。1 限月・2 限月とも同じ値（m1_forward・m2_forward）
--   行使価格ごとの IV: ATM に近い 9 本の、call・put の約定 IV の平均（片方だけならその値）
--   F の位置の補間・SQ・30 日の補間・method: sql/cm/atm_cm.sql の cm の本文と同じ（cm_sq と cm_smile 〜 cm。tests/test_cm_trade.py）
--   スナップショットごとに 1 行（iv30 が NULL の行も入れる。cm と同じく 1 回 1 行）
--
-- 書き込み（apply。1 つのトランザクション）: 範囲の basis = 'trade' の行と、結果と同じ時刻の気配の行（basis が NULL）のうち iv30 が NULL
-- の行（cm_backfill.sql が #114 の行から作る値の無い行）を消し、結果のうち範囲の atm_cm_iv に残った行と同じ時刻でないものを足す
-- （気配から作った値は消さない・上書きしない。conflicts に数える）。同じ範囲を流し直しても表は同じ（冪等。2 回目は消す数 = 足す数）。
-- 戻すのは rollback.sql（印で消す）。
-- 止める（ERROR）: 範囲が NULL・逆向き・分の境目でない・#114 の範囲（range_start 〜 range_end。sql/tick_build/build.sql と同じ）の外、
-- mode が不正。
-- 結果（最後の 1 行）: snapshots（範囲の #114 のスナップショット = 結果の行）、with_iv30、interp・m1・m2（method ごと）、
-- to_delete・to_insert・conflicts（書く前の見込み）、deleted・inserted（apply で消した・足した行。dryrun は 0）。
-- 名前（${project} 等）は runbook の render が、範囲・モードは bq のクエリパラメータが入れる。
-- -- source:begin 〜 -- source:end / -- result:begin 〜 -- result:end は tests/test_cm_trade_bq.py が固定値の入力に差し替える印。
DECLARE start_at TIMESTAMP DEFAULT @start_at;
DECLARE end_at TIMESTAMP DEFAULT @end_at;
DECLARE mode STRING DEFAULT @mode;
DECLARE range_start TIMESTAMP DEFAULT TIMESTAMP '2018-09-30 15:00:00+00';
DECLARE range_end TIMESTAMP DEFAULT TIMESTAMP '2024-08-06 03:25:00+00';
-- 約定 IV を使う時間（分）。accuracy.sql と同じ値
DECLARE trade_window_minutes INT64 DEFAULT 120;
DECLARE to_delete INT64 DEFAULT 0;
DECLARE conflicts INT64 DEFAULT 0;
DECLARE deleted INT64 DEFAULT 0;
DECLARE inserted INT64 DEFAULT 0;

IF start_at IS NULL OR end_at IS NULL OR start_at >= end_at
  OR TIMESTAMP_TRUNC(start_at, MINUTE) != start_at OR TIMESTAMP_TRUNC(end_at, MINUTE) != end_at THEN
  SELECT ERROR(FORMAT('cm_trade: invalid range [%t, %t)', start_at, end_at));
END IF;

IF start_at < range_start OR end_at > range_end THEN
  SELECT ERROR(FORMAT('cm_trade: outside the legacy range (#114): [%t, %t)', start_at, end_at));
END IF;

IF IFNULL(mode, '') NOT IN ('dryrun', 'apply') THEN
  SELECT ERROR(FORMAT('cm_trade: mode must be dryrun or apply: %s', IFNULL(mode, 'NULL')));
END IF;

CREATE TEMP TABLE result AS
-- result:begin
WITH
-- source:begin
-- #114 の行の最新版（気配が無い。約定の値段・時刻・IV と ATM の印）
quote AS (
  SELECT created_at, last_trading_day, type, target_price, is_atm, price_time, iv
  FROM `${project}.${dataset}.option_price_latest`(start_at, end_at)
  WHERE STARTS_WITH(ingest_run_id, 'legacy-fill114-')
),
-- #114 の先物の行（スナップショットに 1 行）
fut AS (
  SELECT created_at, price
  FROM `${project}.${dataset}.future_price_latest`(start_at, end_at)
  WHERE STARTS_WITH(ingest_run_id, 'legacy-fill114-')
),
-- SQ の日を決める休業日（sql/cm/atm_cm.sql と同じ。範囲の日付の前日から。is_trading_day に関係なく全行。#149）
holiday AS (
  SELECT calendar_date
  FROM `${project}.${dataset}.trading_calendar_latest`
  WHERE calendar_date >= DATE_SUB(DATE(start_at, 'Asia/Tokyo'), INTERVAL 1 DAY)
    AND calendar_date < DATE_ADD(DATE(end_at, 'Asia/Tokyo'), INTERVAL 120 DAY)
),
-- source:end
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
SELECT *, 'trade' AS basis FROM cm
-- result:end
;

-- 範囲の atm_cm_iv（書く前）
CREATE TEMP TABLE stored AS
SELECT created_at, basis, iv30
FROM `${project}.${dataset}.atm_cm_iv`
WHERE created_at >= start_at AND created_at < end_at;

-- 消す行: 前に流した印の行と、結果と同じ時刻の値の無い気配の行。足さない行: 残る気配の行（値のある行）と同じ時刻
SET to_delete = (
  SELECT COUNT(*) FROM stored
  WHERE basis = 'trade' OR (basis IS NULL AND iv30 IS NULL AND created_at IN (SELECT created_at FROM result))
);
SET conflicts = (
  SELECT COUNT(*) FROM result
  WHERE created_at IN (SELECT created_at FROM stored WHERE basis IS NULL AND iv30 IS NOT NULL)
);

IF mode = 'apply' THEN
  BEGIN TRANSACTION;

  DELETE FROM `${project}.${dataset}.atm_cm_iv`
  WHERE created_at >= start_at AND created_at < end_at
    AND (basis = 'trade' OR (basis IS NULL AND iv30 IS NULL AND created_at IN (SELECT created_at FROM result)));
  SET deleted = @@row_count;

  INSERT INTO `${project}.${dataset}.atm_cm_iv`
    (created_at, iv30, method, m1_last_trading_day, m1_days, m1_forward, m1_iv, m2_last_trading_day, m2_days, m2_forward, m2_iv,
     basis)
  SELECT * FROM result
  WHERE created_at NOT IN (
    SELECT created_at FROM `${project}.${dataset}.atm_cm_iv`
    WHERE created_at >= start_at AND created_at < end_at
  );
  SET inserted = @@row_count;

  COMMIT TRANSACTION;
END IF;

SELECT
  mode, start_at, end_at,
  COUNT(*) AS snapshots,
  COUNTIF(iv30 IS NOT NULL) AS with_iv30,
  COUNTIF(method = 'interp') AS interp,
  COUNTIF(method = 'm1') AS m1,
  COUNTIF(method = 'm2') AS m2,
  to_delete,
  COUNT(*) - conflicts AS to_insert,
  conflicts,
  deleted,
  inserted
FROM result;
