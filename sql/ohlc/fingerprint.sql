-- 1 時間足（#49）の移行の検証。${source_dataset} の atm_iv / atm_option_price / atm_target_price のうち started_at が
-- [@start_at, @end_at) の行を、表ごとに次の 1 行にする:
--   rows_ / keys / dup_keys / conflict_keys   行数 / キーの数 / 2 行以上あるキー / 中身の違う行が 2 種類以上あるキー
--   fp_all_xor / fp_all_sum       全行の FARM_FINGERPRINT(TO_JSON_STRING(STRUCT(<7 列>))) の XOR / 合計（XOR だけだと同じ行 2 つが打ち消し合うため）
--   fp_clean_xor / fp_clean_sum   中身の違う重複のキーを除き、同じ中身の重複を 1 行にしたもの（= sql/ohlc/import.sql が移す行）
-- 同じ SQL を US の旧 optionchan（--location=US）、東京の一時データセット（#12 の optionchan_import）、移行後の optionchan_tokyo で流し、
--   US と一時データセット: すべての列が一致（コピーの検証）
--   US と optionchan_tokyo: fp_clean_* が一致、optionchan_tokyo の rows_ = keys = US の keys − conflict_keys（移行の検証）
-- を見る。範囲は [2018-01-01, C)（C は import.sql と同じ新系の最初の窓の始まり）。手順は docs/runbook.md「1 時間足（#49）」。
WITH
rows_ AS (
  SELECT 'atm_iv' AS tbl, last_trading_day, time_frame, started_at,
    FARM_FINGERPRINT(TO_JSON_STRING(STRUCT(open, high, low, close, last_trading_day, time_frame, started_at))) AS fp
  FROM `${project}.${source_dataset}.atm_iv`
  WHERE started_at >= @start_at AND started_at < @end_at
  UNION ALL
  SELECT 'atm_option_price', last_trading_day, time_frame, started_at,
    FARM_FINGERPRINT(TO_JSON_STRING(STRUCT(open, high, low, close, last_trading_day, time_frame, started_at)))
  FROM `${project}.${source_dataset}.atm_option_price`
  WHERE started_at >= @start_at AND started_at < @end_at
  UNION ALL
  SELECT 'atm_target_price', last_trading_day, time_frame, started_at,
    FARM_FINGERPRINT(TO_JSON_STRING(STRUCT(open, high, low, close, last_trading_day, time_frame, started_at)))
  FROM `${project}.${source_dataset}.atm_target_price`
  WHERE started_at >= @start_at AND started_at < @end_at
),
keys AS (
  SELECT
    tbl, last_trading_day, time_frame, started_at,
    COUNT(*) AS n,
    COUNT(DISTINCT fp) AS versions,
    ANY_VALUE(fp) AS fp,
    BIT_XOR(fp) AS xor_all,
    SUM(CAST(fp AS NUMERIC)) AS sum_all
  FROM rows_
  GROUP BY tbl, last_trading_day, time_frame, started_at
)
SELECT
  tbl,
  COALESCE(SUM(n), 0) AS rows_,
  COUNT(keys.tbl) AS keys,
  COUNTIF(n > 1) AS dup_keys,
  COUNTIF(versions > 1) AS conflict_keys,
  BIT_XOR(xor_all) AS fp_all_xor,
  SUM(sum_all) AS fp_all_sum,
  BIT_XOR(IF(versions = 1, fp, NULL)) AS fp_clean_xor,
  SUM(IF(versions = 1, CAST(fp AS NUMERIC), NULL)) AS fp_clean_sum
FROM UNNEST(['atm_iv', 'atm_option_price', 'atm_target_price']) AS tbl
LEFT JOIN keys USING (tbl)
GROUP BY tbl
ORDER BY tbl
