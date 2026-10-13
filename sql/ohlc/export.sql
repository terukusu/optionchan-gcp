-- 1 時間足（#49）の新旧の突き合わせの入力: ${source_dataset} の atm_iv / atm_option_price / atm_target_price のうち started_at が
-- [@start_at, @end_at) の行を、1 つの JSON 文字列（列 bars）にする。同じ中身の重複は 1 要素にまとめて件数 n を付ける
-- （中身の違う重複はキーが同じ別の要素になる）。US の旧系（source_dataset = optionchan、--location=US）で流し、
-- 結果を sql/ohlc/compare.sql（東京）のクエリパラメータ legacy_bars に渡す（別リージョンは JOIN できない）。
-- FLOAT64 は TO_JSON_STRING で往復しても同じ値になる（2026-10-13 に旧 atm_iv で確認）。
-- 1 日分（1 表 約 40 行、全体 約 20KB）ずつ流す（コマンドの引数 1 つの上限 128KB を超えないように）。手順は docs/runbook.md「1 時間足（#49）」。
SELECT
  COUNT(*) AS rows_,
  TO_JSON_STRING(ARRAY_AGG(STRUCT(tbl, last_trading_day, time_frame, started_at, open, high, low, close, n)
    ORDER BY tbl, started_at, last_trading_day, open, high, low, close)) AS bars
FROM (
  SELECT tbl, last_trading_day, time_frame, started_at, open, high, low, close, COUNT(*) AS n
  FROM (
    SELECT 'atm_iv' AS tbl, open, high, low, close, last_trading_day, time_frame, started_at
    FROM `${project}.${source_dataset}.atm_iv`
    WHERE started_at >= @start_at AND started_at < @end_at
    UNION ALL
    SELECT 'atm_option_price', open, high, low, close, last_trading_day, time_frame, started_at
    FROM `${project}.${source_dataset}.atm_option_price`
    WHERE started_at >= @start_at AND started_at < @end_at
    UNION ALL
    SELECT 'atm_target_price', open, high, low, close, last_trading_day, time_frame, started_at
    FROM `${project}.${source_dataset}.atm_target_price`
    WHERE started_at >= @start_at AND started_at < @end_at
  )
  GROUP BY tbl, last_trading_day, time_frame, started_at, open, high, low, close
)
