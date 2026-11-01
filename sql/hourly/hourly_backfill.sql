-- 銘柄ごとの 1 時間足（#161）の過去分の作り直し。手で流す（docs/runbook.md の「銘柄ごとの 1 時間足（#161）」の「過去分」）。
-- 範囲 [@start_at, @end_at) の option_price_hourly を、最新版から計算し直した行で置き換える。範囲の端はどちらも JST の 07:00
-- （取引日の区切り。日中・夜間の途中で切ると、price_time の足が範囲の境目で欠ける）で、未来は不可（SQL が検査する）。
-- 計算（-- bars:begin 〜 -- bars:end）は定期クエリ sql/hourly/option_price_hourly.sql と一字一句同じ（tests/test_hourly.py が検査）。
-- 変数の名前も定期クエリと同じ win_start・win_end（計算の本文を共通にするため）。限月の範囲 [ltd_lo, ltd_hi) も同じ決め方。
-- 1 か月（月の 1 日 07:00 JST 〜 翌月 1 日 07:00 JST）で元の行を 約 0.4〜2.3 GiB 読む（全期間で約 47 GiB）。1 日のクエリ量の上限
-- （#95）に合わせて範囲を区切って流す。範囲に結果の行が 1 件も無ければ何もしない。最後に要約を 1 行返す
-- （bars = 足の数、with_mid = mid_close のある足、with_trade = trade_close のある足）。プロジェクトとデータセットは render で入れる（runbook）。
DECLARE win_start TIMESTAMP DEFAULT @start_at;
DECLARE win_end TIMESTAMP DEFAULT @end_at;
DECLARE ltd_lo DATE DEFAULT DATE(win_start, 'Asia/Tokyo');
DECLARE ltd_hi DATE DEFAULT DATE_ADD(DATE(win_end, 'Asia/Tokyo'), INTERVAL 1 YEAR);

IF win_start IS NULL OR win_end IS NULL OR win_start >= win_end THEN
  SELECT ERROR(FORMAT('hourly_backfill: bad range [%t, %t)', win_start, win_end));
END IF;
IF win_end > CURRENT_TIMESTAMP() OR TIME(DATETIME(win_start, 'Asia/Tokyo')) != TIME '07:00:00'
    OR TIME(DATETIME(win_end, 'Asia/Tokyo')) != TIME '07:00:00' THEN
  SELECT ERROR(FORMAT('hourly_backfill: range must start and end at 07:00 JST and not be in the future [%t, %t)', win_start, win_end));
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
  SELECT ERROR(FORMAT('hourly_backfill: last_trading_day out of range in [%t, %t)', win_start, win_end));
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
SELECT COUNT(*) AS bars, COUNTIF(mid_close IS NOT NULL) AS with_mid, COUNTIF(trade_close IS NOT NULL) AS with_trade FROM result;
