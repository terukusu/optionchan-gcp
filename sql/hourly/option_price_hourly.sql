-- 銘柄ごとの 1 時間足（#161）。BigQuery Data Transfer Service のスケジュールされたクエリ option-hourly（asia-northeast1、
-- 実行 SA sa-sq-dts、毎時 :45 JST）の本文。プロジェクトとデータセットは Terraform の templatefile が入れる。
-- 最新版（option_price_latest）の行から、(started_at, last_trading_day, target_price, type) ごとに 1 行を option_price_hourly に書く。
-- 窓は 2 つ（sql/cm/atm_cm.sql と同じ考え方）:
--   増分の回（@run_time が JST の 7 時台以外）: [表の最後の足（MAX(started_at)）− 2 時間, @run_time)。始まりは @run_time − 3 日を
--     正時に切り捨てた時刻より前にしない。足の始まりは正時なので、窓の始まりも正時（足が窓をまたがない）
--   作り直しの回（@run_time が JST の 7 時台。1 日 1 回）: @run_time の JST の日付 D に対し [D-1 07:00, D 07:00) JST
--     （重ねより遅れた取り込み・再処理の安全網。手動の再実行もこの回の run_time で）
-- 増分の回は、窓の中（3 日の範囲）の snapshot で前の回の後（@run_time − 65 分以降）に終わった取り込み（ingest_runs）が無ければ
-- 何もしない（週末・休場日・早朝は option_price も表も読まない）。
-- 足の切り方は 2 つ（docs/data.md の「1 時間足の計算（銘柄ごと）」）:
--   気配・数の列（mid_*・bid_*・ask_*・volume・positions・snapshots・last_at）は created_at を UTC の正時に切り捨てた足
--   約定の列（trade_*・iv_close）は price_time（JST の約定の分）を JST の正時に切り捨てた足。同じ立会（JST の 07:00・16:00 で切る）の
--   行の約定だけを数え、窓の始まりより前の足の約定は出さない（増分の窓の始まりでも、その前の足を壊さない）
-- 計算（-- bars:begin 〜 -- bars:end）は sql/hourly/hourly_backfill.sql と一字一句同じ（tests/test_hourly.py が検査）。
-- 窓の中を置き換える（再実行しても同じ結果。3 日を超える停止・前日より前の最新版の変化は hourly_backfill.sql で作り直す）。
-- 窓に結果の行が 1 件も無ければ何もしない（休場日・週末と、データの無い過去の日の手動実行で既存の行を消さないため）。
-- 表のパーティションは last_trading_day の月で require_partition_filter なので、表の読み書き（MAX・DELETE）は限月の範囲
-- [ltd_lo, ltd_hi)（窓の始まりの JST の日付 〜 窓の終わりの 1 年後）で絞る。結果にその範囲の外の限月の行があれば ERROR
-- （置き換えの DELETE から漏れて重複するのを防ぐ）。
-- スキャン量（dry run）: option_price は UTC の日パーティションを丸ごと読むので、元の行（13 列）が増分の回 約 121 MiB（1 パーティション）、
-- 作り直しの回 約 245 MiB（2 パーティション）。DELETE は生きている限月のパーティションの全列（2 限月で最大 約 150 MiB）。
-- 1 日の見積もりは docs/runbook.md の「銘柄ごとの 1 時間足（#161）」。
-- -- source:begin 〜 -- source:end / -- result:begin 〜 -- result:end は tests/test_hourly_bq.py が固定値の入力に差し替える印。
-- 7 時台かどうかは @run_time（DTS の予定時刻）の JST の時で決める（実行の遅れ・分に左右されない）
DECLARE rebuild BOOL DEFAULT EXTRACT(HOUR FROM DATETIME(@run_time, 'Asia/Tokyo')) = 7;
DECLARE win_end TIMESTAMP DEFAULT IF(rebuild, TIMESTAMP(DATETIME(DATE(@run_time, 'Asia/Tokyo'), TIME '07:00:00'), 'Asia/Tokyo'), @run_time);
DECLARE win_start TIMESTAMP DEFAULT IF(rebuild, TIMESTAMP_SUB(win_end, INTERVAL 1 DAY), TIMESTAMP_TRUNC(TIMESTAMP_SUB(win_end, INTERVAL 3 DAY), HOUR));
-- 表を読み書きする限月の範囲（パーティションの絞り込み）。最初の win_start から決める（増分で窓を縮めても変えない）
DECLARE ltd_lo DATE DEFAULT DATE(win_start, 'Asia/Tokyo');
DECLARE ltd_hi DATE DEFAULT DATE_ADD(DATE(win_end, 'Asia/Tokyo'), INTERVAL 1 YEAR);

IF win_end > CURRENT_TIMESTAMP() THEN
  SELECT ERROR(FORMAT('option_price_hourly: win_end %t is in the future (run_time %t)', win_end, @run_time));
END IF;
-- 増分の回: 新しい取り込みが無ければ終わる。あれば窓の始まりを表の最後の足 − 2 時間まで縮める
-- （窓より前の snapshot の再処理は、ここでは計算し直さない。翌朝の 7 時台の回か hourly_backfill.sql で作り直す）
-- （表の MAX は win_end より前の足だけ。過去の run_time の手動実行で、その後の足があっても窓は空にならない）
IF NOT rebuild THEN
  IF NOT EXISTS (
    SELECT 1
    FROM `${project}.${dataset}.ingest_runs`
    WHERE snapshot_at >= win_start AND snapshot_at < win_end AND finished_at >= TIMESTAMP_SUB(@run_time, INTERVAL 65 MINUTE)
  ) THEN
    RETURN;
  END IF;
  SET win_start = GREATEST(IFNULL(TIMESTAMP_SUB((
    SELECT MAX(started_at)
    FROM `${project}.${dataset}.option_price_hourly`
    WHERE last_trading_day >= ltd_lo AND last_trading_day < ltd_hi AND started_at >= win_start AND started_at < win_end
  ), INTERVAL 2 HOUR), win_start), win_start);
END IF;

CREATE TEMP TABLE result AS
-- result:begin
WITH
-- source:begin
src AS (
  SELECT created_at, last_trading_day, target_price, type, bid, ask, bid_iv, ask_iv, price, price_time, iv, volume, positions
  FROM `${project}.${dataset}.option_price_latest`(win_start, win_end)
  WHERE type IS NOT NULL
),
-- source:end
-- bars:begin
-- s = created_at の JST − 7 時間（07:00 JST 起点の時刻）。sd = 取引の日（日中はその日、夜間は夕方の日。0〜7 時は前の日）、
-- st = その中の時刻（日中・夜間とも日付をまたがない）。ps = price_time の JST − 7 時間
hourly_rows AS (
  SELECT *, DATE(s) AS sd, TIME(s) AS st, DATETIME_SUB(price_time, INTERVAL 7 HOUR) AS ps
  FROM (SELECT *, DATETIME_SUB(DATETIME(created_at, 'Asia/Tokyo'), INTERVAL 7 HOUR) AS s FROM src)
),
-- 中値に使う気配: bid・ask が両方 0 より大きく bid ≤ ask で、連続売買の窓の中（[始まり, 終わり)。引けの板寄せの前の 5 分は入れない）。
-- 窓は取引の日 sd の時代で切り替える（st は JST − 7 時間: 01:45 = 8:45、02:00 = 9:00、08:10 = 15:10、08:40 = 15:40、
-- 09:30 = 16:30、10:00 = 17:00、22:25 = 翌 5:25、22:55 = 翌 5:55）
hourly_quote AS (
  SELECT
    *,
    bid > 0 AND ask > 0 AND bid <= ask AND CASE
      -- 2021-09-21 より前: 日中 [9:00, 15:10)・夜間 [16:30, 翌 5:25)
      WHEN sd < DATE '2021-09-21' THEN
        (st >= TIME '02:00:00' AND st < TIME '08:10:00') OR (st >= TIME '09:30:00' AND st < TIME '22:25:00')
      -- 2021-09-21 から（J-GATE 3.0）: 日中 [8:45, 15:10)・夜間 [16:30, 翌 5:55)
      WHEN sd < DATE '2024-11-05' THEN
        (st >= TIME '01:45:00' AND st < TIME '08:10:00') OR (st >= TIME '09:30:00' AND st < TIME '22:55:00')
      -- 2024-11-05 から（立会時間の延長）: 日中 [8:45, 15:40)・夜間 [17:00, 翌 5:55)
      ELSE
        (st >= TIME '01:45:00' AND st < TIME '08:40:00') OR (st >= TIME '10:00:00' AND st < TIME '22:55:00')
    END AS ok
  FROM hourly_rows
),
-- 気配・数の列の足（created_at の UTC の正時）。同じ created_at の行の順は値で決める（結果を一意にする）
hourly_quote_bars AS (
  SELECT
    TIMESTAMP_TRUNC(created_at, HOUR) AS started_at,
    last_trading_day,
    target_price,
    type,
    ARRAY_AGG(IF(ok, (bid + ask) / 2, NULL) IGNORE NULLS ORDER BY created_at, bid, ask LIMIT 1)[SAFE_OFFSET(0)] AS mid_open,
    MAX(IF(ok, (bid + ask) / 2, NULL)) AS mid_high,
    MIN(IF(ok, (bid + ask) / 2, NULL)) AS mid_low,
    ARRAY_AGG(IF(ok, STRUCT((bid + ask) / 2 AS mid, bid, ask, bid_iv, ask_iv), NULL) IGNORE NULLS
      ORDER BY created_at DESC, bid DESC, ask DESC, bid_iv DESC, ask_iv DESC LIMIT 1)[SAFE_OFFSET(0)] AS quote_close,
    -- 足の最後の行の値（NULL もありうる。同じ時刻は大きい方、NULL は後）
    ARRAY_AGG(STRUCT(volume) ORDER BY created_at DESC, volume DESC LIMIT 1)[OFFSET(0)].volume AS volume,
    ARRAY_AGG(positions IGNORE NULLS ORDER BY created_at DESC, positions DESC LIMIT 1)[SAFE_OFFSET(0)] AS positions,
    COUNT(DISTINCT created_at) AS snapshots,
    MAX(created_at) AS last_at
  FROM hourly_quote
  GROUP BY started_at, last_trading_day, target_price, type
),
-- 約定: price・price_time があり、price_time の立会（JST の 07:00・16:00 で切る = ps の日付と 09:00 の前後）が created_at の立会と
-- 同じ行だけ（週明けの朝の金曜夜間の約定・日中の朝の夜間の約定・夕方の日中の約定の持ち越しは数えない）。足は price_time の JST の正時
hourly_trade AS (
  SELECT
    created_at,
    last_trading_day,
    target_price,
    type,
    price,
    price_time,
    iv,
    TIMESTAMP_TRUNC(TIMESTAMP(price_time, 'Asia/Tokyo'), HOUR) AS started_at
  FROM hourly_rows
  WHERE price IS NOT NULL AND price_time IS NOT NULL
    AND DATE(ps) = sd AND (TIME(ps) < TIME '09:00:00') = (st < TIME '09:00:00')
),
-- 約定の足。窓の始まりより前の足は出さない（その足の行は窓の前にもあり、ここでは揃わない）。
-- 始値 = price_time が最も早い分の、最初のスナップショットの行。終値 = price_time が最も遅い分の、その分が終わった後の最初の
-- スナップショットの行（無ければその分の最後のスナップショット）。iv_close はその行の iv（持ち越しの行の iv には左右されない）
hourly_trade_bars AS (
  SELECT
    started_at,
    last_trading_day,
    target_price,
    type,
    ARRAY_AGG(price ORDER BY price_time, created_at, price LIMIT 1)[OFFSET(0)] AS trade_open,
    MAX(price) AS trade_high,
    MIN(price) AS trade_low,
    ARRAY_AGG(STRUCT(price, iv)
      ORDER BY price_time DESC,
        IF(created_at >= TIMESTAMP_ADD(TIMESTAMP(price_time, 'Asia/Tokyo'), INTERVAL 1 MINUTE), 0, 1),
        IF(created_at >= TIMESTAMP_ADD(TIMESTAMP(price_time, 'Asia/Tokyo'), INTERVAL 1 MINUTE), UNIX_MICROS(created_at), -UNIX_MICROS(created_at)),
        price DESC, iv DESC
      LIMIT 1)[OFFSET(0)] AS trade_last
  FROM hourly_trade
  WHERE started_at >= win_start
  GROUP BY started_at, last_trading_day, target_price, type
),
hourly_bars AS (
  SELECT
    started_at,
    last_trading_day,
    target_price,
    type,
    q.mid_open,
    q.mid_high,
    q.mid_low,
    q.quote_close.mid AS mid_close,
    q.quote_close.bid AS bid_close,
    q.quote_close.ask AS ask_close,
    q.quote_close.bid_iv AS bid_iv_close,
    q.quote_close.ask_iv AS ask_iv_close,
    t.trade_open,
    t.trade_high,
    t.trade_low,
    t.trade_last.price AS trade_close,
    t.trade_last.iv AS iv_close,
    q.volume,
    q.positions,
    IFNULL(q.snapshots, 0) AS snapshots,
    q.last_at
  FROM hourly_quote_bars AS q
  FULL OUTER JOIN hourly_trade_bars AS t USING (started_at, last_trading_day, target_price, type)
)
-- bars:end
SELECT * FROM hourly_bars
-- result:end
;

-- 限月の範囲の外の行（置き換えの DELETE の範囲から漏れる）は書かない
IF EXISTS (
  SELECT 1 FROM result
  WHERE last_trading_day < DATE(started_at, 'Asia/Tokyo') OR last_trading_day >= DATE_ADD(DATE(started_at, 'Asia/Tokyo'), INTERVAL 1 YEAR)
) THEN
  SELECT ERROR(FORMAT('option_price_hourly: last_trading_day out of range in [%t, %t)', win_start, win_end));
END IF;
IF EXISTS (SELECT 1 FROM result) THEN
  BEGIN TRANSACTION;
  DELETE FROM `${project}.${dataset}.option_price_hourly`
  WHERE last_trading_day >= ltd_lo AND last_trading_day < ltd_hi AND started_at >= win_start AND started_at < win_end;
  INSERT INTO `${project}.${dataset}.option_price_hourly`
    (started_at, last_trading_day, target_price, type, mid_open, mid_high, mid_low, mid_close, bid_close, ask_close, bid_iv_close,
     ask_iv_close, trade_open, trade_high, trade_low, trade_close, iv_close, volume, positions, snapshots, last_at)
  SELECT * FROM result;
  COMMIT TRANSACTION;
END IF;
