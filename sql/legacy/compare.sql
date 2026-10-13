-- 新旧の突き合わせ（#40、#11 の AC 5〜7）。BigQuery Data Transfer Service のスケジュールされたクエリ legacy-compare
-- （asia-northeast1、実行 SA sa-sq-dts、毎日 07:30 JST）の本文。プロジェクト・データセット・ERROR の切り替えは Terraform の templatefile が入れる。
-- 旧系の前日分は legacy-stage（US、07:02）→ legacy-copy（データセットコピー、07:10）で ${copy_dataset} に来ている。
-- 新系は最新版のテーブル関数（*_latest、#34）で読む。窓は [D-1 07:00, D 07:00) JST（D は @run_time の JST の日付）。
--
-- 比べ方（行単位。created_at = 1 限月ページの更新時刻 T。キーは spot・future は T、option は T と (last_trading_day, type, target_price)）:
-- JPX のページはスポット・先物・option の各行が 1 分の中でばらばらに書き換わり、旧系は更新時刻の表示が変わった直後に取得するので、
-- 旧系の T の行は新系の T・その直前・直後のどれかの版になる（2026-10-13 の実データで確認。#38、#40 のレビュー）。
-- 旧系の各行を、新系の T の同じキーの行 → 直前・直後のスナップショットの同じ行の順に比べる:
--   match        全列が新系の T と一致
--   version      全列が新系の直前か直後と一致（版の差。行の有無も含む: 新系の T にだけある行で、直前か直後に無いもの）
--   *_year       年の補完の修正（先物の限月が 旧 = 新 + 1 年、option の清算日が ±1 年）で、他の列が一致
--   quote_only   option で約定の列（price・price_time・diff・diff_rate・volume・positions・quotation・quotation_date と、
--                約定価格から求める iv・delta・gamma・theta・vega）が T・直前・直後のどれかと一致し、気配と表示の列
--                （bid・bid_volume・bid_iv・ask・ask_volume・ask_iv・is_atm）だけが違う（1 分の中の版の差。どちらの系も取っていない版）
--   new_stale    spot・future で、新系の T が直前と同じ中身（新系がその分の更新を取れなかった）で、旧系だけ新しい版
--   page2        option で新系の 2 限月ページの更新時刻が 1 限月と違うスナップショットの 2 限月の行（#11 AC 5）
--   unexplained  それ以外（約定の列がどの候補とも合わない行など）
-- スナップショット: 旧系にあって新系に無い T は legacy_only（新系の取りこぼし、説明できない差分）、新系だけの T は保存条件の差
-- （説明済み）。旧系の重複・欠けは件数だけ数えて比べない。
-- 説明できない差分 = legacy_only + 各表の unexplained。legacy_compare_daily に記録する（同じ日は置き換え）。
-- ERROR にするのは fail_on_unexplained = true のときだけ（infra/prod/sq.tf。分類を数日の記録で確かめてから有効にする。runbook）。
-- コピーの行数が stage の件数と違うとき（コピーの欠け）は記録してから常に ERROR。
-- -- source:begin 〜 -- source:end / -- result:begin 〜 -- result:end は tests/test_sql_bq.py が固定値の入力に差し替える印。
DECLARE win_end TIMESTAMP DEFAULT TIMESTAMP(DATETIME(DATE(@run_time, 'Asia/Tokyo'), TIME '07:00:00'), 'Asia/Tokyo');
DECLARE win_start TIMESTAMP DEFAULT TIMESTAMP_SUB(win_end, INTERVAL 1 DAY);
DECLARE fail_on_unexplained BOOL DEFAULT ${fail_on_unexplained};

IF win_end > CURRENT_TIMESTAMP() THEN
  SELECT ERROR(FORMAT('legacy_compare: win_end %t is in the future (run_time %t)', win_end, @run_time));
END IF;

-- コピーが今回の窓のものでなければ（legacy-stage / legacy-copy の失敗・遅れ）記録せずに止める
IF NOT EXISTS (
  SELECT 1 FROM `${project}.${copy_dataset}.staged_window` WHERE window_start = win_start AND window_end = win_end
) THEN
  SELECT ERROR(FORMAT(
    'legacy_compare: copy is not for the window %t (staged: %s)',
    win_start,
    (SELECT TO_JSON_STRING(ARRAY_AGG(STRUCT(window_start, staged_at))) FROM `${project}.${copy_dataset}.staged_window`)
  ));
END IF;

CREATE TEMP TABLE result AS
-- result:begin
WITH
-- source:begin
staged AS (
  SELECT spot_rows, future_rows, option_rows
  FROM `${project}.${copy_dataset}.staged_window`
  WHERE window_start = win_start AND window_end = win_end
),
legacy_spot AS (
  SELECT price, price_time, diff, diff_rate, hv, created_at
  FROM `${project}.${copy_dataset}.spot_price`
  WHERE created_at >= win_start AND created_at < win_end
),
legacy_future AS (
  SELECT price, price_time, diff, diff_rate, hv, contract_month, created_at
  FROM `${project}.${copy_dataset}.future_price`
  WHERE created_at >= win_start AND created_at < win_end
),
legacy_option AS (
  SELECT
    type, target_price, is_atm, price, price_time, diff, diff_rate, iv,
    bid, bid_volume, bid_iv, ask, ask_volume, ask_iv, volume, positions,
    quotation, quotation_date, delta, gamma, theta, vega, last_trading_day, created_at
  FROM `${project}.${copy_dataset}.option_price`
  WHERE created_at >= win_start AND created_at < win_end
),
-- 直前・直後のスナップショットを連休明けや窓の終わりでも取れるように、スナップショット・スポット・先物は 7 日前から 2 時間後まで読む（小さい表）
new_runs AS (
  SELECT snapshot_at, page2_updated_at
  FROM `${project}.${dataset}.latest_runs`(TIMESTAMP_SUB(win_start, INTERVAL 7 DAY), TIMESTAMP_ADD(win_end, INTERVAL 2 HOUR))
),
new_spot AS (
  SELECT price, price_time, diff, diff_rate, hv, created_at
  FROM `${project}.${dataset}.spot_price_latest`(TIMESTAMP_SUB(win_start, INTERVAL 7 DAY), TIMESTAMP_ADD(win_end, INTERVAL 2 HOUR))
),
new_future AS (
  SELECT price, price_time, diff, diff_rate, hv, contract_month, created_at
  FROM `${project}.${dataset}.future_price_latest`(TIMESTAMP_SUB(win_start, INTERVAL 7 DAY), TIMESTAMP_ADD(win_end, INTERVAL 2 HOUR))
),
-- option は窓の 6 時間前から 2 時間後まで（窓の境 07:00 JST = 22:00 UTC の前後。読むパーティション（UTC の日）は増えない）。
-- 直前・直後のスナップショットがこの範囲の外なら、その候補は使わない（option_from / option_to）
new_option AS (
  SELECT
    type, target_price, is_atm, price, price_time, diff, diff_rate, iv,
    bid, bid_volume, bid_iv, ask, ask_volume, ask_iv, volume, positions,
    quotation, quotation_date, delta, gamma, theta, vega, last_trading_day, created_at
  FROM `${project}.${dataset}.option_price_latest`(TIMESTAMP_SUB(win_start, INTERVAL 6 HOUR), TIMESTAMP_ADD(win_end, INTERVAL 2 HOUR))
),
-- source:end
option_range AS (
  SELECT TIMESTAMP_SUB(win_start, INTERVAL 6 HOUR) AS option_from, TIMESTAMP_ADD(win_end, INTERVAL 2 HOUR) AS option_to
),
legacy_counts AS (
  SELECT created_at, SUM(n_spot) AS n_spot, SUM(n_future) AS n_future, SUM(n_option) AS n_option
  FROM (
    SELECT created_at, 1 AS n_spot, 0 AS n_future, 0 AS n_option FROM legacy_spot
    UNION ALL
    SELECT created_at, 0, 1, 0 FROM legacy_future
    UNION ALL
    SELECT created_at, 0, 0, COUNT(*) FROM legacy_option GROUP BY created_at
  )
  GROUP BY created_at
),
new_seq AS (
  SELECT
    r.snapshot_at,
    COALESCE(r.page2_updated_at != r.snapshot_at, FALSE) AS page2_differs,
    f.price_time AS future_time,
    LAG(r.snapshot_at) OVER w AS prev_at,
    LEAD(r.snapshot_at) OVER w AS next_at,
    LAG(f.price_time) OVER w AS prev_future_time
  FROM new_runs AS r
  LEFT JOIN new_future AS f ON f.created_at = r.snapshot_at
  WINDOW w AS (ORDER BY r.snapshot_at)
),
new_in AS (
  SELECT * FROM new_seq WHERE snapshot_at >= win_start AND snapshot_at < win_end
),
snap AS (
  SELECT
    l.created_at,
    CASE
      WHEN n.snapshot_at IS NULL THEN 'legacy_only'
      WHEN l.n_spot > 1 OR l.n_future > 1 THEN 'legacy_dup'
      WHEN l.n_spot = 0 OR l.n_future = 0 OR l.n_option = 0 THEN 'legacy_partial'
      ELSE 'compared'
    END AS status
  FROM legacy_counts AS l
  LEFT JOIN new_in AS n ON n.snapshot_at = l.created_at
),
-- 比べるスナップショット T と、新系の直前・直後（option は読んだ範囲の中だけ）
pairs AS (
  SELECT
    s.created_at AS t,
    n.prev_at,
    n.next_at,
    IF(n.prev_at >= o.option_from, n.prev_at, NULL) AS option_prev_at,
    IF(n.next_at < o.option_to, n.next_at, NULL) AS option_next_at,
    n.page2_differs
  FROM snap AS s
  JOIN new_in AS n ON n.snapshot_at = s.created_at
  CROSS JOIN option_range AS o
  WHERE s.status = 'compared'
),
spot_v AS (
  SELECT created_at, TO_JSON_STRING(STRUCT(price, price_time, diff, diff_rate, hv)) AS v FROM new_spot
),
spot_cls AS (
  SELECT
    p.t,
    CASE
      WHEN l.v = n.v THEN 'match'
      WHEN l.v = pv.v OR l.v = nx.v THEN 'version'
      WHEN n.v = pv.v THEN 'new_stale'
      ELSE 'unexplained'
    END AS cls
  FROM pairs AS p
  JOIN (
    SELECT created_at, TO_JSON_STRING(STRUCT(price, price_time, diff, diff_rate, hv)) AS v FROM legacy_spot
  ) AS l ON l.created_at = p.t
  LEFT JOIN spot_v AS n ON n.created_at = p.t
  LEFT JOIN spot_v AS pv ON pv.created_at = p.prev_at
  LEFT JOIN spot_v AS nx ON nx.created_at = p.next_at
),
future_v AS (
  SELECT created_at, contract_month AS cm, TO_JSON_STRING(STRUCT(price, price_time, diff, diff_rate, hv)) AS v
  FROM new_future
),
future_cls AS (
  SELECT
    p.t,
    CASE
      WHEN l.v = n.v AND l.cm = n.cm THEN 'match'
      WHEN l.v = n.v AND l.cm = DATE_ADD(n.cm, INTERVAL 1 YEAR) THEN 'contract_year'
      WHEN (l.v = pv.v AND l.cm IN (pv.cm, DATE_ADD(pv.cm, INTERVAL 1 YEAR)))
        OR (l.v = nx.v AND l.cm IN (nx.cm, DATE_ADD(nx.cm, INTERVAL 1 YEAR))) THEN 'version'
      WHEN n.v = pv.v AND n.cm = pv.cm THEN 'new_stale'
      ELSE 'unexplained'
    END AS cls
  FROM pairs AS p
  JOIN (
    SELECT created_at, contract_month AS cm, TO_JSON_STRING(STRUCT(price, price_time, diff, diff_rate, hv)) AS v
    FROM legacy_future
  ) AS l ON l.created_at = p.t
  LEFT JOIN future_v AS n ON n.created_at = p.t
  LEFT JOIN future_v AS pv ON pv.created_at = p.prev_at
  LEFT JOIN future_v AS nx ON nx.created_at = p.next_at
),
-- option の値: 約定の列 tv（iv・ギリシャ指標は約定価格から求める値で、約定の列と一緒に変わる。2026-10-13 の実データで確認）、
-- 気配と表示の列 qv（is_atm はスポットに合わせて動く表示）、清算日 qd（年の補完を見るので別）
legacy_opt AS (
  SELECT
    created_at, last_trading_day, type, target_price, quotation_date AS qd,
    TO_JSON_STRING(STRUCT(
      price, price_time, diff, diff_rate, iv, volume, positions, quotation, delta, gamma, theta, vega)) AS tv,
    TO_JSON_STRING(STRUCT(is_atm, bid, bid_volume, bid_iv, ask, ask_volume, ask_iv)) AS qv
  FROM legacy_option
),
new_opt AS (
  SELECT
    created_at, last_trading_day, type, target_price, quotation_date AS qd,
    TO_JSON_STRING(STRUCT(
      price, price_time, diff, diff_rate, iv, volume, positions, quotation, delta, gamma, theta, vega)) AS tv,
    TO_JSON_STRING(STRUCT(is_atm, bid, bid_volume, bid_iv, ask, ask_volume, ask_iv)) AS qv
  FROM new_option
),
page1_month AS (
  SELECT created_at, MIN(last_trading_day) AS month FROM new_opt GROUP BY created_at
),
opt_join AS (
  SELECT
    COALESCE(l.created_at, n.created_at) AS t,
    COALESCE(l.last_trading_day, n.last_trading_day) AS last_trading_day,
    COALESCE(l.type, n.type) AS type,
    COALESCE(l.target_price, n.target_price) AS target_price,
    l.created_at IS NOT NULL AS in_legacy,
    n.created_at IS NOT NULL AS in_new,
    l.tv AS ltv, l.qv AS lqv, l.qd AS lqd,
    n.tv AS ntv, n.qv AS nqv, n.qd AS nqd
  FROM (SELECT o.* FROM legacy_opt AS o JOIN pairs AS p ON p.t = o.created_at) AS l
  FULL JOIN (SELECT o.* FROM new_opt AS o JOIN pairs AS p ON p.t = o.created_at) AS n
    ON n.created_at = l.created_at AND n.last_trading_day = l.last_trading_day AND n.type = l.type
    AND n.target_price = l.target_price
),
opt_cand AS (
  SELECT
    j.*,
    p.page2_differs,
    m.month AS page1_month,
    p.option_prev_at IS NOT NULL AS has_prev,
    p.option_next_at IS NOT NULL AS has_next,
    pv.created_at IS NOT NULL AS in_prev, pv.tv AS ptv, pv.qv AS pqv, pv.qd AS pqd,
    nx.created_at IS NOT NULL AS in_next, nx.tv AS xtv, nx.qv AS xqv, nx.qd AS xqd
  FROM opt_join AS j
  JOIN pairs AS p ON p.t = j.t
  LEFT JOIN page1_month AS m ON m.created_at = j.t
  LEFT JOIN new_opt AS pv
    ON pv.created_at = p.option_prev_at AND pv.last_trading_day = j.last_trading_day AND pv.type = j.type
    AND pv.target_price = j.target_price
  LEFT JOIN new_opt AS nx
    ON nx.created_at = p.option_next_at AND nx.last_trading_day = j.last_trading_day AND nx.type = j.type
    AND nx.target_price = j.target_price
),
opt_cls AS (
  SELECT
    t, last_trading_day, type, target_price,
    CASE
      WHEN in_legacy AND in_new AND ltv = ntv AND lqv = nqv AND lqd IS NOT DISTINCT FROM nqd THEN 'match'
      -- 版の差: 直前か直後と全列一致。新系の T にだけある行は、直前か直後に無ければ（行使価格の入れ替わり）
      WHEN in_legacy AND (
          (in_prev AND ltv = ptv AND lqv = pqv AND lqd IS NOT DISTINCT FROM pqd)
          OR (in_next AND ltv = xtv AND lqv = xqv AND lqd IS NOT DISTINCT FROM xqd)) THEN 'version'
      WHEN NOT in_legacy AND ((has_prev AND NOT in_prev) OR (has_next AND NOT in_next)) THEN 'version'
      WHEN in_legacy AND in_new AND ltv = ntv AND lqv = nqv
        AND lqd IN (DATE_ADD(nqd, INTERVAL 1 YEAR), DATE_SUB(nqd, INTERVAL 1 YEAR)) THEN 'quotation_year'
      -- 気配と表示の列だけの差: 約定の列（清算日は年の補完を許す）が T・直前・直後のどれかと一致
      WHEN in_legacy AND (
          (in_new AND ltv = ntv AND (lqd IS NOT DISTINCT FROM nqd OR lqd IN (DATE_ADD(nqd, INTERVAL 1 YEAR), DATE_SUB(nqd, INTERVAL 1 YEAR))))
          OR (in_prev AND ltv = ptv AND (lqd IS NOT DISTINCT FROM pqd OR lqd IN (DATE_ADD(pqd, INTERVAL 1 YEAR), DATE_SUB(pqd, INTERVAL 1 YEAR))))
          OR (in_next AND ltv = xtv AND (lqd IS NOT DISTINCT FROM xqd OR lqd IN (DATE_ADD(xqd, INTERVAL 1 YEAR), DATE_SUB(xqd, INTERVAL 1 YEAR))))
        ) THEN 'quote_only'
      WHEN page2_differs AND last_trading_day > page1_month THEN 'page2'
      ELSE 'unexplained'
    END AS cls
  FROM opt_cand
),
-- #11 AC 6: 旧系の保存条件（先物の約定時刻の時:分が直前から変わった）を満たす新系のスナップショットと、そのうち旧系に無いもの
save_condition AS (
  SELECT n.snapshot_at, l.created_at IS NOT NULL AS in_legacy
  FROM new_in AS n
  LEFT JOIN legacy_counts AS l ON l.created_at = n.snapshot_at
  WHERE FORMAT_DATETIME('%H:%M', n.future_time) IS DISTINCT FROM FORMAT_DATETIME('%H:%M', n.prev_future_time)
),
samples AS (
  SELECT 'legacy_only' AS kind, created_at, CAST(NULL AS STRING) AS detail FROM snap WHERE status = 'legacy_only'
  UNION ALL
  SELECT 'spot', t, NULL FROM spot_cls WHERE cls = 'unexplained'
  UNION ALL
  SELECT 'future', t, NULL FROM future_cls WHERE cls = 'unexplained'
  UNION ALL
  SELECT 'option', t, FORMAT('%d %d %t', type, target_price, last_trading_day) FROM opt_cls WHERE cls = 'unexplained'
),
counts AS (
  SELECT
    (
      SELECT LOGICAL_AND(
        s.spot_rows = (SELECT COUNT(*) FROM legacy_spot)
        AND s.future_rows = (SELECT COUNT(*) FROM legacy_future)
        AND s.option_rows = (SELECT COUNT(*) FROM legacy_option))
      FROM staged AS s
    ) AS copy_complete,
    (SELECT COUNT(*) FROM legacy_counts) AS legacy_snapshots,
    (SELECT COUNT(*) FROM new_in) AS new_snapshots,
    (SELECT COUNTIF(status != 'legacy_only') FROM snap) AS both_snapshots,
    (SELECT COUNT(*) FROM new_in WHERE snapshot_at NOT IN (SELECT created_at FROM legacy_counts)) AS new_only_snapshots,
    (SELECT COUNTIF(status = 'legacy_only') FROM snap) AS legacy_only_snapshots,
    (SELECT COUNTIF(status = 'legacy_dup') FROM snap) AS legacy_dup_snapshots,
    (SELECT COUNTIF(status = 'legacy_partial') FROM snap) AS legacy_partial_snapshots,
    (SELECT COUNT(*) FROM spot_cls) AS spot_compared,
    (SELECT COUNTIF(cls = 'version') FROM spot_cls) AS spot_version,
    (SELECT COUNTIF(cls = 'new_stale') FROM spot_cls) AS spot_new_stale,
    (SELECT COUNTIF(cls = 'unexplained') FROM spot_cls) AS spot_unexplained,
    (SELECT COUNT(*) FROM future_cls) AS future_compared,
    (SELECT COUNTIF(cls = 'version') FROM future_cls) AS future_version,
    (SELECT COUNTIF(cls = 'contract_year') FROM future_cls) AS future_contract_year,
    (SELECT COUNTIF(cls = 'new_stale') FROM future_cls) AS future_new_stale,
    (SELECT COUNTIF(cls = 'unexplained') FROM future_cls) AS future_unexplained,
    (SELECT COUNT(*) FROM opt_cls) AS option_compared,
    (SELECT COUNTIF(cls = 'version') FROM opt_cls) AS option_version,
    (SELECT COUNTIF(cls = 'quote_only') FROM opt_cls) AS option_quote_only,
    (SELECT COUNTIF(cls = 'quotation_year') FROM opt_cls) AS option_quotation_year,
    (SELECT COUNTIF(cls = 'page2') FROM opt_cls) AS option_page2,
    (SELECT COUNTIF(cls = 'unexplained') FROM opt_cls) AS option_unexplained,
    (
      SELECT IFNULL(ARRAY_AGG(STRUCT(kind, created_at, detail) ORDER BY created_at, kind, detail LIMIT 20), [])
      FROM samples
    ) AS unexplained_samples,
    (SELECT COUNT(*) FROM save_condition) AS legacy_condition_candidates,
    (SELECT COUNTIF(NOT in_legacy) FROM save_condition) AS legacy_condition_missed
)
SELECT
  DATE(win_start, 'Asia/Tokyo') AS compare_date,
  win_start AS window_start,
  win_end AS window_end,
  IFNULL(copy_complete, FALSE) AS copy_complete,
  legacy_snapshots,
  new_snapshots,
  both_snapshots,
  new_only_snapshots,
  legacy_only_snapshots,
  legacy_dup_snapshots,
  legacy_partial_snapshots,
  spot_compared,
  spot_version,
  spot_new_stale,
  spot_unexplained,
  future_compared,
  future_version,
  future_contract_year,
  future_new_stale,
  future_unexplained,
  option_compared,
  option_version,
  option_quote_only,
  option_quotation_year,
  option_page2,
  option_unexplained,
  legacy_only_snapshots + spot_unexplained + future_unexplained + option_unexplained AS unexplained,
  unexplained_samples,
  legacy_condition_candidates,
  legacy_condition_missed,
  fail_on_unexplained,
  CURRENT_TIMESTAMP() AS computed_at
FROM counts
-- result:end
;

-- 同じ日の行を置き換える（再実行しても 1 行。1 文なので途中で失敗しても半端にならない）
MERGE `${project}.${dataset}.legacy_compare_daily` AS t
USING result AS s
ON FALSE
WHEN NOT MATCHED BY SOURCE AND t.compare_date = DATE(win_start, 'Asia/Tokyo') THEN DELETE
WHEN NOT MATCHED THEN INSERT ROW;

-- コピーの欠け（stage の件数とコピーの行数が違う）は、比べた結果が当てにならないので常に失敗させる
IF NOT (SELECT copy_complete FROM result) THEN
  SELECT ERROR(FORMAT('legacy_compare: copy incomplete on %t (legacy-copy の失敗・途中のコピー)', DATE(win_start, 'Asia/Tokyo')));
END IF;

-- 説明できない差分で失敗させるのは fail_on_unexplained = true のときだけ（分類を確かめるまでは記録だけ）
IF fail_on_unexplained AND (SELECT unexplained FROM result) > 0 THEN
  SELECT ERROR((
    SELECT FORMAT(
      'legacy_compare: unexplained %d on %t %s',
      r.unexplained,
      r.compare_date,
      TO_JSON_STRING(STRUCT(
        r.legacy_only_snapshots, r.spot_unexplained, r.future_unexplained, r.option_unexplained,
        ARRAY(SELECT AS STRUCT * FROM UNNEST(r.unexplained_samples) LIMIT 5) AS samples))
    )
    FROM result AS r
  ));
END IF;
