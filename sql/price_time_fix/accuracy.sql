-- 過去データの補正（#4）の推定の一致率。東京の新系の最新版（正しい price_time を持つ）を旧系の補完で旧系の保存値に戻し、fix.sql と
-- 同じ推定（-- functions:begin 〜 -- functions:end と -- result:begin 〜 -- result:end の source 以外は fix.sql と同じ。
-- tests/test_price_time_fix.py が一致を見る）で戻した値と、元の正しい値を比べる。読み取りだけ。
-- 範囲: [@ctx_start, @end_at) を読み、[@start_at, @end_at) の行を数える（@ctx_start 〜 @start_at は約定の始まりを見つけるための文脈。
-- 新系の稼働の初め（2026-10-13）の行は前の表示が無いので series_start / gap になりやすい。1 日以上あける）。
-- 表 × 確かさ × 正しいずれの日数 × 直すずれの日数ごとに、行数・補正後に正しい行数（ok）・補正しない場合に正しい行数（ok_without_fix）を返す。
-- 一意に決まらない行（determined 以外）は元のままなので、ok = ok_without_fix。AC は determined の ok / n と全体の ok / n
-- 旧系の 2 限月の行は 2 限月ページの更新時刻で補完していたので、option は page_updated_at（無ければ created_at）で戻す。
-- 名前は runbook の render が、範囲は bq のクエリパラメータが入れる。
DECLARE ctx_start TIMESTAMP DEFAULT @ctx_start;
DECLARE start_at TIMESTAMP DEFAULT @start_at;
DECLARE end_at TIMESTAMP DEFAULT @end_at;
DECLARE fix_version STRING DEFAULT 'legacy-fix4-1';
DECLARE closed_trading ARRAY<DATE> DEFAULT (
  SELECT IFNULL(ARRAY_AGG(calendar_date), []) FROM `${project}.${dataset}.trading_calendar_latest` WHERE NOT is_trading_day
);
DECLARE closed_cash ARRAY<DATE> DEFAULT (
  SELECT IFNULL(ARRAY_AGG(calendar_date), []) FROM `${project}.${dataset}.trading_calendar_latest`
);

IF ctx_start IS NULL OR start_at IS NULL OR end_at IS NULL OR NOT (ctx_start <= start_at AND start_at < end_at) THEN
  SELECT ERROR(FORMAT('price_time_fix accuracy: invalid range ctx %t [%t, %t)', ctx_start, start_at, end_at));
END IF;

-- functions:begin
-- 取引日（土日と休業日以外）
CREATE TEMP FUNCTION is_open(d DATE, closed ARRAY<DATE>) AS (
  EXTRACT(DAYOFWEEK FROM d) BETWEEN 2 AND 6 AND d NOT IN UNNEST(closed)
);
-- x（JST）が立会中か。cash = TRUE は spot（東証の営業日の日中だけ）
CREATE TEMP FUNCTION in_session(x DATETIME, closed ARRAY<DATE>, cash BOOL) AS (
  CASE
    WHEN cash THEN is_open(DATE(x), closed)
    WHEN TIME(x) BETWEEN '08:45:00' AND '15:45:00' OR TIME(x) >= '16:30:00' THEN is_open(DATE(x), closed)
    WHEN TIME(x) <= '06:00:00' THEN is_open(DATE_SUB(DATE(x), INTERVAL 1 DAY), closed)
    ELSE FALSE
  END
);
-- 保存値 b と同じ時刻で、upper 以下・立会中の最も新しい日時（b + 1 日から 31 日前まで）。無ければ NULL
CREATE TEMP FUNCTION latest_valid(b DATETIME, upper DATETIME, closed ARRAY<DATE>, cash BOOL) AS ((
  SELECT x
  FROM UNNEST(GENERATE_ARRAY(-1, 31)) AS k, UNNEST([DATETIME_SUB(b, INTERVAL k DAY)]) AS x
  WHERE x <= upper AND in_session(x, closed, cash)
  ORDER BY k
  LIMIT 1
));
-- a より前で、同じ時刻・立会中の最も新しい日時（次の候補）
CREATE TEMP FUNCTION previous_valid(a DATETIME, closed ARRAY<DATE>, cash BOOL) AS ((
  SELECT x
  FROM UNNEST(GENERATE_ARRAY(1, 31)) AS k, UNNEST([DATETIME_SUB(a, INTERVAL k DAY)]) AS x
  WHERE in_session(x, closed, cash)
  ORDER BY k
  LIMIT 1
));
-- functions:end

CREATE TEMP TABLE est AS
-- result:begin
WITH
-- source:begin
src AS (
  -- 新系の最新版の正しい price_time を、旧系の補完（ページの更新時刻の日付 + 時刻、未来なら 1 日前）で旧系の保存値に戻したもの
  SELECT
    'spot_price' AS tbl, '' AS series, CAST(NULL AS DATE) AS last_trading_day, CAST(NULL AS INT64) AS type,
    CAST(NULL AS INT64) AS target_price, CAST(NULL AS DATE) AS contract_month,
    created_at,
    IF(DATETIME(DATE(DATETIME(created_at, 'Asia/Tokyo')), TIME(price_time)) > DATETIME(created_at, 'Asia/Tokyo'),
      DATETIME_SUB(DATETIME(DATE(DATETIME(created_at, 'Asia/Tokyo')), TIME(price_time)), INTERVAL 1 DAY), DATETIME(DATE(DATETIME(created_at, 'Asia/Tokyo')), TIME(price_time))) AS price_time,
    CAST(price AS FLOAT64) AS price, CAST(NULL AS INT64) AS volume, CAST(NULL AS STRING) AS price_time_raw,
    CAST(NULL AS STRING) AS parser_version
  FROM `${project}.${dataset}.spot_price_latest`(ctx_start, end_at)
  UNION ALL
  SELECT
    'future_price', FORMAT('%t', contract_month), NULL, NULL, NULL, contract_month,
    created_at,
    IF(DATETIME(DATE(DATETIME(created_at, 'Asia/Tokyo')), TIME(price_time)) > DATETIME(created_at, 'Asia/Tokyo'),
      DATETIME_SUB(DATETIME(DATE(DATETIME(created_at, 'Asia/Tokyo')), TIME(price_time)), INTERVAL 1 DAY), DATETIME(DATE(DATETIME(created_at, 'Asia/Tokyo')), TIME(price_time))),
    CAST(price AS FLOAT64), NULL, NULL, NULL
  FROM `${project}.${dataset}.future_price_latest`(ctx_start, end_at)
  UNION ALL
  SELECT
    'option_price', FORMAT('%t|%t|%t', last_trading_day, type, target_price), last_trading_day, type, target_price, NULL,
    created_at,
    IF(DATETIME(DATE(DATETIME(COALESCE(page_updated_at, created_at), 'Asia/Tokyo')), TIME(price_time)) > DATETIME(COALESCE(page_updated_at, created_at), 'Asia/Tokyo'),
      DATETIME_SUB(DATETIME(DATE(DATETIME(COALESCE(page_updated_at, created_at), 'Asia/Tokyo')), TIME(price_time)), INTERVAL 1 DAY), DATETIME(DATE(DATETIME(COALESCE(page_updated_at, created_at), 'Asia/Tokyo')), TIME(price_time))),
    CAST(price AS FLOAT64), volume, NULL, NULL
  FROM `${project}.${dataset}.option_price_latest`(ctx_start, end_at)
),
-- source:end
orig AS (
  -- 元の保存値（補正済みの行は price_time_raw から戻す）
  SELECT
    * EXCEPT (price_time_raw, parser_version),
    IFNULL(parser_version = fix_version, FALSE) AS marked,
    IF(IFNULL(parser_version = fix_version, FALSE), SAFE.PARSE_DATETIME('%F %T', price_time_raw), price_time) AS b
  FROM src
),
keys AS (
  SELECT
    tbl, series, created_at, b, price, volume,
    ANY_VALUE(last_trading_day) AS last_trading_day, ANY_VALUE(type) AS type, ANY_VALUE(target_price) AS target_price,
    ANY_VALUE(contract_month) AS contract_month,
    COUNT(*) AS n,
    COUNTIF(NOT marked) AS n_unmarked,
    ARRAY_AGG(IF(marked, price_time, NULL) IGNORE NULLS) AS marked_values
  FROM orig
  GROUP BY tbl, series, created_at, b, price, volume
),
ordered AS (
  SELECT
    *,
    LAG(STRUCT(created_at, b, price, volume)) OVER (
      PARTITION BY tbl, series ORDER BY created_at, b, price, volume
    ) AS prev
  FROM keys
),
starts AS (
  SELECT
    *,
    prev IS NULL
      OR prev.b IS NULL
      OR TIME(prev.b) != TIME(b)
      OR prev.price IS DISTINCT FROM price
      OR IFNULL(volume, 0) > IFNULL(prev.volume, 0) AS is_start
  FROM ordered
),
runs AS (
  SELECT
    *,
    COUNTIF(is_start) OVER (
      PARTITION BY tbl, series ORDER BY created_at, b, price, volume
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS run_no
  FROM starts
),
start_est AS (
  SELECT
    tbl, series, run_no, b,
    prev IS NULL AS series_start,
    DATETIME(prev.created_at, 'Asia/Tokyo') AS prev_c,
    DATETIME(created_at, 'Asia/Tokyo') AS c,
    IF(tbl = 'spot_price', closed_cash, closed_trading) AS closed,
    tbl = 'spot_price' AS cash
  FROM runs
  WHERE is_start AND b IS NOT NULL
),
start_off AS (
  SELECT
    *,
    -- 立会の外（休業日など。終了後 30 分は立会の内とみなす）のスナップショットで、表示が取得の直前の約定: 推定の根拠が無いので元のまま
    NOT (in_session(c, closed_trading, FALSE) OR in_session(DATETIME_SUB(c, INTERVAL 30 MINUTE), closed_trading, FALSE))
      AND b BETWEEN DATETIME_SUB(c, INTERVAL 30 MINUTE) AND DATETIME_ADD(c, INTERVAL 10 MINUTE) AS off_session
  FROM start_est
),
start_a AS (
  SELECT
    *,
    IF(off_session, b, latest_valid(b, DATETIME_ADD(c, INTERVAL 10 MINUTE), closed, cash)) AS a
  FROM start_off
),
start_basis AS (
  SELECT
    tbl, series, run_no, a,
    CASE
      WHEN off_session THEN 'off_session'
      WHEN a IS NULL THEN 'no_session'
      WHEN series_start THEN 'series_start'
      WHEN previous_valid(a, closed, cash) >= DATETIME_SUB(prev_c, INTERVAL 10 MINUTE) THEN 'gap'
      ELSE 'determined'
    END AS basis
  FROM start_a
)
SELECT
  r.tbl, r.last_trading_day, r.type, r.target_price, r.contract_month, r.created_at, r.b, r.price, r.volume,
  -- 一意に決まる行（determined）だけを直す。それ以外は約定の始まり以外の行も含めて元のまま（始まりの日付に揃えない）
  IF(s.basis = 'determined', s.a, r.b) AS new_price_time,
  s.basis,
  DATE_DIFF(DATE(r.b), DATE(IF(s.basis = 'determined', s.a, r.b)), DAY) AS shift_days,
  -- 参考: 最も新しい候補で直したときのずれ（一意に決まらない行の内訳に使う）
  DATE_DIFF(DATE(r.b), DATE(IFNULL(s.a, r.b)), DAY) AS candidate_shift_days,
  r.n,
  r.n_unmarked,
  (SELECT COUNT(*) FROM UNNEST(r.marked_values) AS v WHERE v != IF(s.basis = 'determined', s.a, r.b)) AS n_mismatch
FROM runs AS r
JOIN start_basis AS s USING (tbl, series, run_no)
WHERE r.created_at >= start_at AND r.b IS NOT NULL
-- result:end
;

-- 正しい値（推定の行と、表・系列・スナップショット・時刻・値段・取引高で対応づける）
SELECT
  e.tbl, e.basis, DATE_DIFF(DATE(e.b), DATE(t.a), DAY) AS true_shift_days, e.shift_days,
  SUM(e.n) AS n,
  SUM(IF(e.new_price_time = t.a, e.n, 0)) AS ok,
  SUM(IF(e.b = t.a, e.n, 0)) AS ok_without_fix
FROM est AS e
JOIN (
  SELECT DISTINCT 'spot_price' AS tbl, '' AS series, created_at, TIME(price_time) AS t, CAST(price AS FLOAT64) AS price,
    CAST(NULL AS INT64) AS volume, price_time AS a
  FROM `${project}.${dataset}.spot_price_latest`(start_at, end_at)
  WHERE price_time IS NOT NULL
  UNION ALL
  SELECT DISTINCT 'future_price', FORMAT('%t', contract_month), created_at, TIME(price_time), CAST(price AS FLOAT64), NULL, price_time
  FROM `${project}.${dataset}.future_price_latest`(start_at, end_at)
  WHERE price_time IS NOT NULL
  UNION ALL
  SELECT DISTINCT 'option_price', FORMAT('%t|%t|%t', last_trading_day, type, target_price), created_at, TIME(price_time),
    CAST(price AS FLOAT64), volume, price_time
  FROM `${project}.${dataset}.option_price_latest`(start_at, end_at)
  WHERE price_time IS NOT NULL
) AS t
ON t.tbl = e.tbl AND t.created_at = e.created_at AND t.t = TIME(e.b)
  AND t.series = CASE e.tbl
    WHEN 'option_price' THEN FORMAT('%t|%t|%t', e.last_trading_day, e.type, e.target_price)
    WHEN 'future_price' THEN FORMAT('%t', e.contract_month)
    ELSE '' END
  AND IFNULL(t.price, -1) = IFNULL(e.price, -1) AND IFNULL(t.volume, -1) = IFNULL(e.volume, -1)
GROUP BY e.tbl, e.basis, true_shift_days, e.shift_days
ORDER BY e.tbl, e.basis, true_shift_days, e.shift_days;
