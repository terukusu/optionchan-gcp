-- 30 日固定の ATM IV（#100）。BigQuery Data Transfer Service のスケジュールされたクエリ atm-cm（asia-northeast1、実行 SA sa-sq-dts、
-- 毎時 :45 JST。#117）の本文。プロジェクトとデータセットは Terraform の templatefile が入れる。
-- 窓は 2 つ（#117）:
--   増分の回（@run_time が JST の 7 時台以外）: [表の最後の行（atm_cm_iv の MAX(created_at)）− 2 時間, @run_time)。始まりは
--     @run_time − 3 日より前にしない（ビューアーの補いの最大と同じ）。窓を表から決めるので、失敗した回の区間は次の回で埋まる。
--     重ねの 2 時間は取り込みの遅れ（ingest_runs.finished_at − snapshot_at。10 月の実測で最大 約 48 分）の分。終わりに余裕は付けない
--     （option_price_latest は書き終わった実行の行だけを返す。遅れて終わった分は次の回の重ねで拾う）
--   作り直しの回（@run_time が JST の 7 時台。1 日 1 回）: @run_time の JST の日付 D に対し [D-1 07:00, D 07:00) JST
--     （sql/ohlc/atm_hourly.sql と同じ。重ねより遅れた取り込み・再処理の安全網。手動の再実行もこの回の run_time で）。
-- 増分の回は、窓の中（3 日の範囲）の snapshot で前の回の後（@run_time − 65 分以降）に終わった取り込み（ingest_runs）が無ければ
-- 何もしない（週末・休場日・早朝は option_price も atm_cm_iv も読まない。ingest_runs だけの最低課金 10 MiB）。
-- 最新版（option_price_latest）の全行使価格の気配から、取り込み 1 回（created_at）ごとに 1 行を atm_cm_iv に書く:
--   限月ごとのフォワード F（put-call パリティ）、F の位置で補間した ATM IV、SQ までの日数、それらから 30 日の IV（iv30）。
--   計算（-- cm:begin 〜 -- cm:end）は sql/cm/cm_backfill.sql・apps/viewer の sql/atm_cm.sql と一字一句同じ（tests/test_cm.py が検査）。
--   式・定数（スプレッドが中値の 30% 以下の気配、近い 5 本で F、3 本以上、近い 9 本で補間、30 日、SQ の 3 日前から 2 限月だけ）は
--   README の「30 日固定の ATM IV」。
-- 窓の中を置き換える（再実行しても同じ結果。3 日を超える停止・前日より前の最新版の変化は cm_backfill.sql で作り直す）。
-- 窓に結果の行が 1 件も無ければ何もしない（休場日・週末と、データの無い過去の日の手動実行で既存の行を消さないため）。
-- スキャン量（dry run）: option_price は UTC の日パーティションを丸ごと読むので、増分の回 約 112 MiB（1 パーティション）、
-- 作り直しの回 約 226 MiB（2 パーティション）。1 日の見積もりは docs/runbook.md の「30 日固定の ATM IV（#100）」。
-- -- source:begin 〜 -- source:end / -- result:begin 〜 -- result:end は tests/test_cm_bq.py が固定値の入力に差し替える印。
-- 7 時台かどうかは @run_time（DTS の予定時刻）の JST の時で決める（実行の遅れ・分に左右されない）
DECLARE rebuild BOOL DEFAULT EXTRACT(HOUR FROM DATETIME(@run_time, 'Asia/Tokyo')) = 7;
DECLARE win_end TIMESTAMP DEFAULT IF(rebuild, TIMESTAMP(DATETIME(DATE(@run_time, 'Asia/Tokyo'), TIME '07:00:00'), 'Asia/Tokyo'), @run_time);
DECLARE win_start TIMESTAMP DEFAULT TIMESTAMP_SUB(win_end, INTERVAL IF(rebuild, 1, 3) DAY);

IF win_end > CURRENT_TIMESTAMP() THEN
  SELECT ERROR(FORMAT('atm_cm: win_end %t is in the future (run_time %t)', win_end, @run_time));
END IF;
-- 増分の回: 新しい取り込みが無ければ終わる。あれば窓の始まりを表の最後の行 − 2 時間まで縮める
-- （窓より前の snapshot の再処理は、ここでは計算し直さない。翌朝の 7 時台の回か cm_backfill.sql で作り直す）
-- （表の MAX は win_end より前の行だけ。過去の run_time の手動実行で、その後の行があっても窓は空にならない）
IF NOT rebuild THEN
  IF NOT EXISTS (
    SELECT 1
    FROM `${project}.${dataset}.ingest_runs`
    WHERE snapshot_at >= win_start AND snapshot_at < win_end AND finished_at >= TIMESTAMP_SUB(@run_time, INTERVAL 65 MINUTE)
  ) THEN
    RETURN;
  END IF;
  SET win_start = GREATEST(IFNULL(TIMESTAMP_SUB((
    SELECT MAX(created_at)
    FROM `${project}.${dataset}.atm_cm_iv`
    WHERE created_at >= win_start AND created_at < win_end
  ), INTERVAL 2 HOUR), win_start), win_start);
END IF;

CREATE TEMP TABLE result AS
-- result:begin
WITH
-- source:begin
quote AS (
  SELECT created_at, last_trading_day, type, target_price, is_atm, bid, ask, bid_iv, ask_iv
  FROM `${project}.${dataset}.option_price_latest`(win_start, win_end)
),
-- SQ の日を決める休業日（窓の日付から 120 日先まで。2 限月の SQ は約 2 か月先）
holiday AS (
  SELECT calendar_date
  FROM `${project}.${dataset}.trading_calendar_latest`
  WHERE calendar_date >= DATE(win_start, 'Asia/Tokyo') AND calendar_date < DATE_ADD(DATE(win_end, 'Asia/Tokyo'), INTERVAL 120 DAY)
    AND NOT is_trading_day
),
-- source:end
-- cm:begin
-- 限月ごとの ATM の行使価格（is_atm。2 行あれば小さい方）。近い行使価格を選ぶ起点
cm_atm AS (
  SELECT created_at, last_trading_day, MIN(target_price) AS k0
  FROM quote
  WHERE is_atm
  GROUP BY created_at, last_trading_day
),
-- 使える気配: bid・ask が両方 0 より大きく、スプレッド（ask − bid）が中値の 30% 以下（薄い時間帯の極端に低い bid・高い ask を除く）
cm_quote AS (
  SELECT *, bid > 0 AND ask >= bid AND (ask - bid) * 2 <= 0.3 * (ask + bid) AS ok
  FROM quote
),
-- 行使価格ごとに call（type = 1）・put（type = 2）の気配の中値と mid IV（使える気配で、bid_iv・ask_iv が両方 0 より大きいときだけ）
cm_strike AS (
  SELECT
    created_at,
    last_trading_day,
    target_price AS k,
    ANY_VALUE(k0) AS k0,
    MAX(IF(type = 1 AND ok, (bid + ask) / 2, NULL)) AS call_mid,
    MAX(IF(type = 2 AND ok, (bid + ask) / 2, NULL)) AS put_mid,
    MAX(IF(type = 1 AND ok AND bid_iv > 0 AND ask_iv > 0, (bid_iv + ask_iv) / 2, NULL)) AS call_iv,
    MAX(IF(type = 2 AND ok AND bid_iv > 0 AND ask_iv > 0, (bid_iv + ask_iv) / 2, NULL)) AS put_iv
  FROM cm_quote JOIN cm_atm USING (created_at, last_trading_day)
  GROUP BY created_at, last_trading_day, target_price
),
-- ATM に近い順の番号（同じ距離は行使価格の小さい方から）
cm_near AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY created_at, last_trading_day ORDER BY ABS(k - k0), k) AS rn
  FROM cm_strike
),
-- フォワード F: ATM に近い 5 本のうち call・put の両方に気配のある行使価格の C − P + K（put-call パリティ。金利は無視）の中央値。
-- 3 本に満たなければ出さない
cm_parity AS (
  SELECT created_at, last_trading_day, ARRAY_AGG(call_mid - put_mid + k ORDER BY call_mid - put_mid + k) AS xs
  FROM cm_near
  WHERE rn <= 5 AND call_mid IS NOT NULL AND put_mid IS NOT NULL
  GROUP BY created_at, last_trading_day
  HAVING COUNT(*) >= 3
),
cm_fwd AS (
  SELECT created_at, last_trading_day, (xs[OFFSET(DIV(ARRAY_LENGTH(xs) - 1, 2))] + xs[OFFSET(DIV(ARRAY_LENGTH(xs), 2))]) / 2 AS fwd
  FROM cm_parity
),
-- 行使価格ごとの IV: call と put の mid IV の平均（片方だけならその値）。ATM に近い 9 本まで
cm_smile AS (
  SELECT
    created_at,
    last_trading_day,
    fwd,
    k,
    IF(call_iv IS NOT NULL AND put_iv IS NOT NULL, (call_iv + put_iv) / 2, COALESCE(call_iv, put_iv)) AS s
  FROM cm_near JOIN cm_fwd USING (created_at, last_trading_day)
  WHERE rn <= 9
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
-- SQ = 最終取引日の次の営業日（土日・休業日を飛ばす）の 09:00 JST
cm_sq AS (
  SELECT m.last_trading_day, TIMESTAMP(DATETIME(MIN(d), TIME(9, 0, 0)), 'Asia/Tokyo') AS sq_at
  FROM (SELECT DISTINCT last_trading_day FROM quote) AS m,
    UNNEST(GENERATE_DATE_ARRAY(DATE_ADD(m.last_trading_day, INTERVAL 1 DAY), DATE_ADD(m.last_trading_day, INTERVAL 15 DAY))) AS d
  WHERE EXTRACT(DAYOFWEEK FROM d) NOT IN (1, 7) AND d NOT IN (SELECT calendar_date FROM holiday)
  GROUP BY m.last_trading_day
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
-- cm:end
SELECT * FROM cm
-- result:end
;

IF EXISTS (SELECT 1 FROM result) THEN
  BEGIN TRANSACTION;
  DELETE FROM `${project}.${dataset}.atm_cm_iv` WHERE created_at >= win_start AND created_at < win_end;
  INSERT INTO `${project}.${dataset}.atm_cm_iv` SELECT * FROM result;
  COMMIT TRANSACTION;
END IF;
