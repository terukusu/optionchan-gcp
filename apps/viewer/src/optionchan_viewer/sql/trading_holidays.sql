-- /atm_iv_bars の日足（1d。#68）: 取引カレンダーの最新版（#63）の [@start_date, @end_date) の休業日（土日以外で取引の無い日）。
-- アプリは取引日 = 土日でも休業日でもない日として、夜間の足を次の取引日の日足に入れる（docs/architecture/target.md 3.2 の契約。
-- 祝日取引の実施日は取引日。掲載範囲の外の平日は取引日として扱う）。数十行の小さい表
SELECT calendar_date
FROM `${project}.${dataset}.trading_calendar_latest`
WHERE calendar_date >= @start_date AND calendar_date < @end_date AND NOT is_trading_day
ORDER BY calendar_date
