-- 過去データの移行（#12、target.md 6 章 3a）のコピーの検証: 旧系 3 表の created_at が [@start_at, @end_at) の行を、
-- 日（JST 07:00 区切り。quality_daily.window_date・legacy_compare_daily.compare_date と同じ）× 表ごとに
--   n       行数
--   fp_xor  BIT_XOR(FARM_FINGERPRINT(TO_JSON_STRING(STRUCT(<旧スキーマの列>))))
--   fp_sum  フィンガープリントの合計（XOR だけだと同じ行 2 つが打ち消し合い、重複の増減が見えないため）
-- にする。US の旧 optionchan（source_dataset = optionchan、--location=US）と、東京の一時データセット
-- （source_dataset = optionchan_import）で同じ SQL を流し、CSV を diff する（別リージョンは JOIN できない）。
-- 列は schema/<表>.json の順に明示する（tests/test_legacy_import.py が検査）。手順は docs/runbook.md「過去データの移行（#12）」。
WITH fps AS (
  SELECT
    'spot_price' AS tbl,
    created_at,
    FARM_FINGERPRINT(TO_JSON_STRING(STRUCT(price, price_time, diff, diff_rate, hv, created_at))) AS fp
  FROM `${project}.${source_dataset}.spot_price`
  WHERE created_at >= @start_at AND created_at < @end_at
  UNION ALL
  SELECT
    'future_price',
    created_at,
    FARM_FINGERPRINT(TO_JSON_STRING(STRUCT(price, price_time, diff, diff_rate, hv, contract_month, created_at)))
  FROM `${project}.${source_dataset}.future_price`
  WHERE created_at >= @start_at AND created_at < @end_at
  UNION ALL
  SELECT
    'option_price',
    created_at,
    FARM_FINGERPRINT(TO_JSON_STRING(STRUCT(
      type, target_price, is_atm, price, price_time, diff, diff_rate, iv,
      bid, bid_volume, bid_iv, ask, ask_volume, ask_iv, volume, positions,
      quotation, quotation_date, delta, gamma, theta, vega, last_trading_day, created_at)))
  FROM `${project}.${source_dataset}.option_price`
  WHERE created_at >= @start_at AND created_at < @end_at
)
SELECT
  DATE(TIMESTAMP_SUB(created_at, INTERVAL 7 HOUR), 'Asia/Tokyo') AS trade_date,
  tbl,
  COUNT(*) AS n,
  BIT_XOR(fp) AS fp_xor,
  SUM(CAST(fp AS NUMERIC)) AS fp_sum
FROM fps
GROUP BY trade_date, tbl
ORDER BY trade_date, tbl
