-- 過去データの補正（#4、target.md 6 章 3b・D6）: 移行した旧系の行（ingest_run_id が 'legacy-' で始まる行、#12）の price_time の日付を推定で直す。
-- 東京で、created_at が [@start_at, @end_at) の範囲について流す。@mode = 'dryrun' は数えるだけ、'apply' は UPDATE する。
--
-- ずれの仕組み（旧系の functions/jpx_loader.py、2026-10-12 の #2 の修正まで）: ページの約定日時 MM/DD HH:MM の MM/DD を捨て、
-- ページの更新時刻 C（JST）の日付に HH:MM を付け、C より未来なら 1 日戻していた。保存値 B は「C 以前で最も新しい、時刻が HH:MM の日時」。
-- 正しい値 A は B と同じ時刻で、日付が B 以前（A = B - k 日、k >= 0。表示が C より数分先の約定だけ k = -1）。@fixed_at（#2 の修正後の
-- 最初のスナップショット）以降の行は正しいので触らない。
--
-- 推定（DATE だけを直す。時刻は変えない）:
--   1. 系列（option: 限月・種別・行使価格、future: 限月、spot: 1 つ）ごとに created_at 順に並べ、表示（時刻・値段）が変わった行、
--      表示が無い行（price_time が NULL。少なくとも 2024 年末までのページは取引日が変わると表示を消していた）の次の行、取引高が増えた行
--      （同じ時刻・値段の別の約定）を「約定の始まり」とする。同じ約定の間の行は始まりの行と同じ A を持つ
--   2. 始まりの行の A は、B - k 日（k = -1..31）のうち C + 10 分以下で立会中（取引日のセッション）の最も新しいもの。約定は表示が変わる
--      直前に起きているので、これが最も確からしい。立会は日中 08:45〜15:45・夜間 16:30〜翌 06:00（2018 年以降の各時期の和集合）。
--      日中・夜間の前半は当日、夜間の後半（00:00〜06:00）は前日が取引日であること。spot（日経平均）は東証の営業日（休業日はすべて除く）
--   3. 推定の確かさ（basis）: determined = 前の行（前回の表示）より後に別の候補が無い（日付が一意に決まる）。gap = 前の行との間が
--      空いていて、もう 1 日前の候補もありうる（取り込みの停止・行使価格がページから外れていた間など）。series_start = 系列の最初
--      （文脈の 100 日の中で）。no_session = 立会中の候補が無い。off_session = 立会の外のスナップショット（2024-01-03・2025-01-03・04
--      など休業日の取得）で表示が取得の直前の約定
--   4. 直すのは determined の約定の行だけ。gap・series_start・no_session・off_session の約定の行は、始まり以外の行も含めて元のまま
--      （印も付けない。件数は内訳の rows_kept に basis ごとに出る）
-- 取引日の判定は ${dataset}.trading_calendar_latest（#63。過去年は #67 で埋める）を使う。@require_calendar = TRUE なら、文脈の
-- 31 日前（@start_at の 131 日前）からカレンダーが無いとき、またはその間の年に年始の休業日（1/1〜1/3 の平日）の行が無いとき
-- （年が欠けた版）に止める。
--
-- 書き込み（apply）: determined の未補正の行（parser_version が NULL）だけを UPDATE し、price_time_raw に元の値（'%F %T'）、parser_version に
-- 補正の版 fix_version を入れる。元の値は price_time_raw から戻せる（rollback.sql）。推定は元の値（補正済みの行は price_time_raw）から
-- 毎回同じに計算するので、同じ範囲を流し直しても結果は同じで、2 回目は 0 行になる（冪等）。範囲の前 100 日を文脈として読む。
-- 名前（${project} 等）は runbook の render が、範囲・モードは bq のクエリパラメータが入れる。
-- -- source:begin 〜 -- source:end / -- result:begin 〜 -- result:end / -- functions:begin 〜 -- functions:end は
-- tests/test_price_time_fix_bq.py が固定値の入力に差し替える印。
DECLARE start_at TIMESTAMP DEFAULT @start_at;
DECLARE end_at TIMESTAMP DEFAULT @end_at;
DECLARE fixed_at TIMESTAMP DEFAULT @fixed_at;
DECLARE mode STRING DEFAULT @mode;
DECLARE require_calendar BOOL DEFAULT @require_calendar;
DECLARE fix_version STRING DEFAULT 'legacy-fix4-1';
DECLARE ctx_start TIMESTAMP DEFAULT TIMESTAMP_SUB(start_at, INTERVAL 100 DAY);
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
  FROM UNNEST(GENERATE_ARRAY(
    EXTRACT(YEAR FROM DATE(TIMESTAMP_SUB(ctx_start, INTERVAL 31 DAY), 'Asia/Tokyo')),
    EXTRACT(YEAR FROM DATE(end_at, 'Asia/Tokyo')))) AS y
  -- 版の行は平日だけなので、1/1〜1/3（年始の休業日。3 日のどれかは平日）のどれかがあればその年はある
  WHERE y NOT IN (
    SELECT EXTRACT(YEAR FROM calendar_date) FROM `${project}.${dataset}.trading_calendar_latest`
    WHERE EXTRACT(MONTH FROM calendar_date) = 1 AND EXTRACT(DAY FROM calendar_date) <= 3
  )
);
DECLARE other_versions INT64 DEFAULT (
  SELECT COUNT(*) FROM (
    SELECT parser_version FROM `${project}.${dataset}.spot_price`
    WHERE created_at >= ctx_start AND created_at < end_at
    UNION ALL
    SELECT parser_version FROM `${project}.${dataset}.future_price`
    WHERE created_at >= ctx_start AND created_at < end_at
    UNION ALL
    SELECT parser_version FROM `${project}.${dataset}.option_price`
    WHERE created_at >= ctx_start AND created_at < end_at
  )
  WHERE parser_version LIKE 'legacy-fix4-%' AND parser_version != fix_version
);
DECLARE updated_spot INT64 DEFAULT 0;
DECLARE updated_future INT64 DEFAULT 0;
DECLARE updated_option INT64 DEFAULT 0;

IF start_at IS NULL OR end_at IS NULL OR fixed_at IS NULL OR start_at >= end_at THEN
  SELECT ERROR(FORMAT('price_time_fix: invalid range [%t, %t) fixed_at %t', start_at, end_at, fixed_at));
END IF;

IF end_at > fixed_at THEN
  SELECT ERROR(FORMAT('price_time_fix: end_at %t is after fixed_at %t (rows after the fix are correct)', end_at, fixed_at));
END IF;

IF IFNULL(mode, '') NOT IN ('dryrun', 'apply') THEN
  SELECT ERROR(FORMAT('price_time_fix: mode must be dryrun or apply: %s', IFNULL(mode, 'NULL')));
END IF;

IF IFNULL(require_calendar, TRUE) AND (calendar_from IS NULL OR calendar_from > DATE(TIMESTAMP_SUB(ctx_start, INTERVAL 31 DAY), 'Asia/Tokyo')) THEN
  SELECT ERROR(FORMAT('price_time_fix: trading calendar starts at %t, needs %t (#67)',
    calendar_from, DATE(TIMESTAMP_SUB(ctx_start, INTERVAL 31 DAY), 'Asia/Tokyo')));
END IF;

IF IFNULL(require_calendar, TRUE) AND ARRAY_LENGTH(IFNULL(missing_new_year, [])) > 0 THEN
  SELECT ERROR(FORMAT('price_time_fix: trading calendar has no new-year rows (1/1-1/3) for years %t (#67)', missing_new_year));
END IF;

-- 別の版の補正が残っている（版を上げるときは rollback.sql で戻してから流す）
IF other_versions > 0 THEN
  SELECT ERROR(FORMAT('price_time_fix: %d rows corrected by another version (not %s)', other_versions, fix_version));
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

-- 1. 推定（範囲の行。表示が同じ重複行は 1 行にまとめ、行数を n に持つ）
CREATE TEMP TABLE est AS
-- result:begin
WITH
-- source:begin
src AS (
  SELECT
    'spot_price' AS tbl, '' AS series, CAST(NULL AS DATE) AS last_trading_day, CAST(NULL AS INT64) AS type,
    CAST(NULL AS INT64) AS target_price, CAST(NULL AS DATE) AS contract_month,
    created_at, price_time, CAST(price AS FLOAT64) AS price, CAST(NULL AS INT64) AS volume, price_time_raw, parser_version
  FROM `${project}.${dataset}.spot_price`
  WHERE created_at >= ctx_start AND created_at < end_at
    AND created_at < fixed_at AND STARTS_WITH(ingest_run_id, 'legacy-')
  UNION ALL
  SELECT
    'future_price', FORMAT('%t', contract_month), NULL, NULL, NULL, contract_month,
    created_at, price_time, CAST(price AS FLOAT64), NULL, price_time_raw, parser_version
  FROM `${project}.${dataset}.future_price`
  WHERE created_at >= ctx_start AND created_at < end_at
    AND created_at < fixed_at AND STARTS_WITH(ingest_run_id, 'legacy-')
  UNION ALL
  SELECT
    'option_price', FORMAT('%t|%t|%t', last_trading_day, type, target_price), last_trading_day, type, target_price, NULL,
    created_at, price_time, CAST(price AS FLOAT64), volume, price_time_raw, parser_version
  FROM `${project}.${dataset}.option_price`
  WHERE created_at >= ctx_start AND created_at < end_at
    AND created_at < fixed_at AND STARTS_WITH(ingest_run_id, 'legacy-')
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

-- 2. 書き込み（日付が一意に決まる（determined）未補正の旧系の行だけ。一意に決まらない行・新系の行・#2 の修正後の行・
--    price_time が NULL の行には触れない（印も付けない））
IF mode = 'apply' THEN
  UPDATE `${project}.${dataset}.spot_price` AS t
  SET price_time = e.new_price_time, price_time_raw = FORMAT_DATETIME('%F %T', t.price_time), parser_version = fix_version
  FROM est AS e
  WHERE t.created_at >= start_at AND t.created_at < end_at
    AND t.created_at < fixed_at AND STARTS_WITH(t.ingest_run_id, 'legacy-')
    AND t.parser_version IS NULL AND t.price_time IS NOT NULL
    AND e.basis = 'determined'
    AND e.tbl = 'spot_price' AND e.created_at = t.created_at AND e.b = t.price_time
    AND IFNULL(e.price, -1) = IFNULL(CAST(t.price AS FLOAT64), -1);
  SET updated_spot = @@row_count;

  UPDATE `${project}.${dataset}.future_price` AS t
  SET price_time = e.new_price_time, price_time_raw = FORMAT_DATETIME('%F %T', t.price_time), parser_version = fix_version
  FROM est AS e
  WHERE t.created_at >= start_at AND t.created_at < end_at
    AND t.created_at < fixed_at AND STARTS_WITH(t.ingest_run_id, 'legacy-')
    AND t.parser_version IS NULL AND t.price_time IS NOT NULL
    AND e.basis = 'determined'
    AND e.tbl = 'future_price' AND e.created_at = t.created_at AND e.b = t.price_time
    AND IFNULL(e.contract_month, DATE '1970-01-01') = IFNULL(t.contract_month, DATE '1970-01-01')
    AND IFNULL(e.price, -1) = IFNULL(CAST(t.price AS FLOAT64), -1);
  SET updated_future = @@row_count;

  UPDATE `${project}.${dataset}.option_price` AS t
  SET price_time = e.new_price_time, price_time_raw = FORMAT_DATETIME('%F %T', t.price_time), parser_version = fix_version
  FROM est AS e
  WHERE t.created_at >= start_at AND t.created_at < end_at
    AND t.created_at < fixed_at AND STARTS_WITH(t.ingest_run_id, 'legacy-')
    AND t.parser_version IS NULL AND t.price_time IS NOT NULL
    AND e.basis = 'determined'
    AND e.tbl = 'option_price' AND e.created_at = t.created_at AND e.b = t.price_time
    AND e.last_trading_day = t.last_trading_day AND IFNULL(e.type, -1) = IFNULL(t.type, -1)
    AND e.target_price = t.target_price
    AND IFNULL(e.price, -1) = IFNULL(CAST(t.price AS FLOAT64), -1)
    AND IFNULL(e.volume, -1) = IFNULL(t.volume, -1);
  SET updated_option = @@row_count;
END IF;

-- 3. 内訳（表 × 確かさ × 最も新しい候補のずれ）。rows_pending は書く（書いた）行数で、apply では updated_* の合計と同じになる。
--    rows_kept は一意に決まらず元のままにする行数
SELECT
  mode, start_at, end_at, tbl, basis, candidate_shift_days,
  SUM(n) AS rows_total,
  SUM(IF(basis = 'determined', n_unmarked, 0)) AS rows_pending,
  SUM(IF(basis = 'determined' AND shift_days != 0, n_unmarked, 0)) AS rows_pending_changed,
  SUM(IF(basis = 'determined', 0, n)) AS rows_kept,
  SUM(n_mismatch) AS rows_mismatch,
  updated_spot, updated_future, updated_option
FROM est
GROUP BY tbl, basis, candidate_shift_days
ORDER BY tbl, basis, candidate_shift_days;
