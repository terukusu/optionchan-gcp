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
--   version      全列が新系の直前か直後と一致（版の差。option は旧系の行が新系の T にもあるときだけ。行の有無も含む:
--                新系の T にだけある行で、直前か直後に無いもの）
--   *_year       年の補完の修正（先物の限月が 旧 = 新 + 1 年、option の清算日が ±1 年）で、他の列が一致
--   quote_only   option で約定の列（price・price_time・diff・diff_rate・volume・positions・quotation・quotation_date と、
--                約定価格から求める iv・delta・gamma・theta・vega）が T・直前・直後のどれかと一致し、気配と表示の列
--                （bid・bid_volume・bid_iv・ask・ask_volume・ask_iv・is_atm）だけが違う（1 分の中の版の差。どちらの系も取っていない版。
--                新系の T にも行があるときだけ）
--   new_stale    spot・future で、新系の T が直前と同じ中身（新系がその分の更新を取れなかった）で、旧系だけ新しい版
--                （1 日に表ごと 3 行までなら説明済み、超えたらこの形の行を全部 unexplained。新系が系統的に古い版を返す不具合を
--                捕まえる。上限 3 は #61 で 2026-10-13 の spot 2 行・future 2 行に余裕を持たせて決めた値。その 4 行は #89 で
--                どれも new_behind（1 分で追いつく形）に移り、#89 の後にここへ残るのは 2 分以上止まる・1 分を超えて遅れる形だけ。
--                2026-10-13〜15 の再計算では 10-14 18:52〜19:27 JST の凍結の future 14 行だけ（上限を超えて unexplained））。
--                または新系の T が約定時刻だけ直前から進み価格の列（price・diff・diff_rate）が直前のまま（書き換え途中の版）で、
--                旧系の行が同じ約定時刻・同じ基準価格（price − diff）・同じ hv（future は限月も）の別の価格のもの（#61）。
--                旧系の行の版を新系も T の後 5 分の中で取っていれば裏付けあり。裏付けが無いもの（新系の価格の持ち越しの誤りと
--                区別できない）は 1 日に表ごと 3 行までなら説明済み、超えたら全部 unexplained（系統的な持ち越しの不具合は
--                1 日に多数出るので捕まえる。2026-10-13 は future 1 行（13:44 JST）。上限 3 はそれに余裕を持たせた値）
--   new_behind   spot・future で新系の 1 分遅れ（#89）: 新系の T の約定時刻が旧系より前で、新系の直後が T と違う版に進み、
--                その約定時刻が旧系と同じか後（新系が直後で追いついた）。遅れは 1 分ちょうど（旧系 ≤ 新系の T + 1 分）で、新系の T は直前より古くない（新系の直前 ≤ 新系の T。逆行は当たらない）。
--                基準価格・hv が新系の T と同じ（future は限月も）。new_stale の判定より前に見る。長い凍結の最後の行（直後で
--                現在まで飛ぶ）と古い版に戻る形は遅れが 1 分を超えるので当たらない。新系が止まり続ける形（直後も T と同じ版。2026-10-14 18:52〜19:27 JST の凍結）は
--                当たらず、new_stale の上限で unexplained になる。1 日に表ごと 10 行までなら説明済み、超えたら全部 unexplained
--                （2026-10-15 は future 5 行・spot 3 行。上限 10 はそれに余裕を持たせた値。毎分遅れる不具合は 1 日数百行になる）。
--                option で新系の 1 分遅れ（#131）: 新系の T が直前と同じ（約定の列と清算日）で、新系の直後が T と違う版に進み、
--                出来高が 新系の T < 旧系 ≤ 直後、約定時刻が 新系の T < 旧系 ≤ 直後（新系の T に約定が無ければ最小とみなす）。
--                遅れは 1 分（旧系の約定時刻が T の 1 分前以後）。建玉・清算値が新系の T と同じ、清算日は新系の T と同じか ±1 年、
--                基準価格（price − diff）が新系の直後と同じ。旧系・新系の T・直前・直後のどれにも行があるときだけ（2026-10-19 の
--                4 行・10-20 の 2 行の形）。1 日に 10 行までなら説明済み、超えたら option の new_behind を全部 unexplained。
--                option は約定が疎で新系の T の約定時刻から遅れを測れないので、2 分以上の凍結の最後の分（直後で追いつく）は
--                当たりうる（凍結の途中の分は直後も T と同じで当たらず unexplained に残るので、凍結そのものは検出される）
--   page2        option で新系の 2 限月ページの更新時刻が 1 限月と違うスナップショットの 2 限月の行（#11 AC 5。新系の T にも行があるときだけ）
--   legacy_older 旧系が新系の T より古い版・途中の版を読んだ（#61）。上のどれにも当たらない行のうち、新系の T が旧系の行から
--                進んでいて（spot・future は約定時刻が後、option は出来高が多く約定時刻が同じか後）、次のどちらか:
--                (a) 旧系の行が新系の T の 10 分前〜T の前のスナップショットの同じ行と全列一致（option は約定の列の一致。
--                    清算日は年の補完を許す。気配と表示の列は問わない = quote_only と同じ）。旧系が古い版を読んだ
--                (b) 旧系の行が新系の連続する 2 スナップショット S1 < S2（S1 は T の 10 分前以降、S2 ≤ T）の版の間:
--                    option は出来高が S1 < 旧 < S2（約定が S1 の後・S2 の前にあった）、約定時刻が S1 ≤ 旧 ≤ S2、
--                    基準価格（price − diff）・清算値・清算日が S1 か S2 と同じ、建玉が S1 と S2 の間。
--                    spot・future は出来高が無いので約定時刻が S1 ≤ 旧 < S2（分の精度で間にある。S1 と同じ分は、その分の中の
--                    別の約定を旧系が読んだ形。#89）、基準価格（price − diff）と hv が S1 か S2 と同じ（future は限月も）。
--                    1 分の中で途中に出た版を旧系だけが読んだ
--                どちらも新系の T の値の裏付けが要る: 旧系の次のスナップショットの同じ行が新系の T と同じ値（option は約定の列）、
--                または (a) と S2 < T の (b) に限って新系の直後のスナップショットの同じ行が同じ値。新系の直後は新系自身の読み取りで
--                独立の裏付けではない（同じ誤りが直後まで続けば満たす）ので、S2 = T の (b) には使わない。旧系の次に同じ行があれば、
--                それが新系の T と矛盾しない（option は出来高・約定時刻が新系の T 以上、spot・future は約定時刻が新系の T 以後）こと。
--                10 分は 2026-10-13 の実データで見た最も古い版（2 分前）に余裕を持たせた上限。
--                これらの条件で、新系の T の解析の誤りのうち、T だけの誤り（価格だけ違う、出来高を多く読むなど）と、旧系の次の
--                スナップショットと矛盾する誤りは unexplained に残る。新系の同じ誤りが直後まで続き、旧系の次がその行を持たない
--                （または矛盾しない）形の (a) と S2 < T の (b) は説明済みになりうる（tests/test_sql_bq.py）
--   legacy_older_unconfirmed  legacy_older の裏付けだけが無いもの（#89）。旧系の次のスナップショットの同じ行が新系の T より
--                先へ進んでいる（spot・future は約定時刻が後、option は出来高が多い）= 新系の T が旧系の T と旧系の次の間にある。
--                spot・future は約定時刻が旧系と同じ分の別の価格も含める（新系の T が旧系 ≤ 新系、(b) は S1 ≤ 旧 ≤ S2。分の精度では
--                どちらが先か決まらない）。新系がたまたま 1 回だけ値を読み誤った形と区別できないので、1 日に表ごと 3 行までなら
--                説明済み、超えたら全部 unexplained（2026-10-14 は future 2 行・option 1 行、10-15 は future 1 行）
--   unexplained  それ以外（約定の列がどの候補とも合わない行など）
-- スナップショット: 旧系にあって新系に無い T は legacy_only（新系の取りこぼし、説明できない差分）、新系だけの T は保存条件の差
-- （説明済み）。旧系の重複・欠けは件数だけ数えて比べない。
-- 説明できない差分 = legacy_only + 各表の unexplained。legacy_compare_daily に記録する（同じ日は置き換え）。
-- ERROR にするのは fail_on_unexplained = true で、窓の日付が fail_from 以降のときだけ（infra/prod/sq.tf。fail_from より前の日を
-- 再実行しても ERROR にしない。結果の fail_on_unexplained 列はこの 2 つを合わせた値。runbook）。
-- コピーの行数が stage の件数と違うとき（コピーの欠け）は記録してから常に ERROR。
-- -- source:begin 〜 -- source:end / -- result:begin 〜 -- result:end は tests/test_sql_bq.py が固定値の入力に差し替える印。
DECLARE win_end TIMESTAMP DEFAULT TIMESTAMP(DATETIME(DATE(@run_time, 'Asia/Tokyo'), TIME '07:00:00'), 'Asia/Tokyo');
DECLARE win_start TIMESTAMP DEFAULT TIMESTAMP_SUB(win_end, INTERVAL 1 DAY);
DECLARE fail_on_unexplained BOOL DEFAULT ${fail_on_unexplained};
DECLARE fail_from DATE DEFAULT DATE '${fail_from}';

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
-- 新系は過去データの移行（#12）で入れた旧系の行（ingest_run_id が legacy- で始まる）を除く（旧系どうしを比べないように）。
-- 直前・直後のスナップショットを連休明けや窓の終わりでも取れるように、スナップショット・スポット・先物は 7 日前から 2 時間後まで読む（小さい表）
new_runs AS (
  SELECT snapshot_at, page2_updated_at
  FROM `${project}.${dataset}.latest_runs`(TIMESTAMP_SUB(win_start, INTERVAL 7 DAY), TIMESTAMP_ADD(win_end, INTERVAL 2 HOUR))
  WHERE NOT STARTS_WITH(ingest_run_id, 'legacy-')
),
new_spot AS (
  SELECT price, price_time, diff, diff_rate, hv, created_at
  FROM `${project}.${dataset}.spot_price_latest`(TIMESTAMP_SUB(win_start, INTERVAL 7 DAY), TIMESTAMP_ADD(win_end, INTERVAL 2 HOUR))
  WHERE NOT STARTS_WITH(ingest_run_id, 'legacy-')
),
new_future AS (
  SELECT price, price_time, diff, diff_rate, hv, contract_month, created_at
  FROM `${project}.${dataset}.future_price_latest`(TIMESTAMP_SUB(win_start, INTERVAL 7 DAY), TIMESTAMP_ADD(win_end, INTERVAL 2 HOUR))
  WHERE NOT STARTS_WITH(ingest_run_id, 'legacy-')
),
-- option は窓の 6 時間前から 2 時間後まで（窓の境 07:00 JST = 22:00 UTC の前後。読むパーティション（UTC の日）は増えない）。
-- 直前・直後のスナップショットがこの範囲の外なら、その候補は使わない（option_from / option_to）
new_option AS (
  SELECT
    type, target_price, is_atm, price, price_time, diff, diff_rate, iv,
    bid, bid_volume, bid_iv, ask, ask_volume, ask_iv, volume, positions,
    quotation, quotation_date, delta, gamma, theta, vega, last_trading_day, created_at
  FROM `${project}.${dataset}.option_price_latest`(TIMESTAMP_SUB(win_start, INTERVAL 6 HOUR), TIMESTAMP_ADD(win_end, INTERVAL 2 HOUR))
  WHERE NOT STARTS_WITH(ingest_run_id, 'legacy-')
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
    n.page2_differs,
    ln.legacy_next_at
  FROM snap AS s
  JOIN new_in AS n ON n.snapshot_at = s.created_at
  -- 旧系の次のスナップショット（新系の T の値の裏付けに使う。spot・future が重複したスナップショットは使わない）
  LEFT JOIN (
    SELECT created_at, LEAD(created_at) OVER (ORDER BY created_at) AS legacy_next_at FROM snap WHERE status != 'legacy_dup'
  ) AS ln ON ln.created_at = s.created_at
  CROSS JOIN option_range AS o
  WHERE s.status = 'compared'
),
-- 古い版・途中の版（legacy_older）の候補: T ごとに、新系の T の 10 分前〜T のスナップショット S と、その直前 S1（10 分の中のときだけ）。
-- 古い版 (a) は S < T。途中の版 (b) は S2 = S ≤ T（S2 = T を許すのは、legacy_older が新系の T の値の裏付けを必ず求めるため）
recent AS (
  SELECT
    p.t,
    s.snapshot_at AS s_at,
    IF(s.prev_at >= TIMESTAMP_SUB(p.t, INTERVAL 10 MINUTE), s.prev_at, NULL) AS s1_at
  FROM pairs AS p
  JOIN new_seq AS s ON s.snapshot_at >= TIMESTAMP_SUB(p.t, INTERVAL 10 MINUTE) AND s.snapshot_at <= p.t
),
-- 新系の T の後 5 分のスナップショット（new_stale の裏付け: 旧系の版を新系も後で取ったか）
later AS (
  SELECT p.t, s.snapshot_at AS s_at
  FROM pairs AS p
  JOIN new_seq AS s ON s.snapshot_at > p.t AND s.snapshot_at <= TIMESTAMP_ADD(p.t, INTERVAL 5 MINUTE)
),
-- spot・future の値: v は全列、pt は約定時刻、base は基準価格（price − diff。1 日の中で変わらない）
spot_v AS (
  SELECT
    created_at, price_time AS pt, ROUND(price - diff, 2) AS base, hv,
    TO_JSON_STRING(STRUCT(price, diff, diff_rate)) AS pr,
    TO_JSON_STRING(STRUCT(price, price_time, diff, diff_rate, hv)) AS v
  FROM new_spot
),
legacy_spot_v AS (
  SELECT
    created_at, price_time AS pt, ROUND(price - diff, 2) AS base, hv,
    TO_JSON_STRING(STRUCT(price, price_time, diff, diff_rate, hv)) AS v
  FROM legacy_spot
),
-- 旧系の T の版を、新系も T の後 5 分の中で取った（new_stale の裏付け）
spot_seen AS (
  SELECT DISTINCT f.t
  FROM later AS f
  JOIN legacy_spot_v AS l ON l.created_at = f.t
  JOIN spot_v AS s ON s.created_at = f.s_at AND s.v = l.v
),
spot_cls0 AS (
  SELECT
    p.t, l.pt AS lpt, l.base AS lbase, l.hv AS lhv, l.v AS lv, n.pt AS npt,
    -- legacy_older の条件: 新系の T の値が新系の直後（ok_nx）か旧系の次のスナップショット（ok_ln）と同じ。旧系の次があれば
    -- 新系の T と矛盾しない（約定時刻が新系の T 以後）こと（ln_ok）
    IFNULL(n.v = nx.v, FALSE) AS ok_nx,
    IFNULL(n.v = ln.v, FALSE) AS ok_ln,
    ln.created_at IS NULL OR IFNULL(ln.pt >= n.pt, FALSE) AS ln_ok,
    -- legacy_older_unconfirmed の条件: 旧系の次の約定時刻が新系の T より後（#89）
    IFNULL(ln.pt > n.pt, FALSE) AS ln_ahead,
    CASE
      WHEN l.v = n.v THEN 'match'
      WHEN l.v = pv.v OR l.v = nx.v THEN 'version'
      -- 新系の 1 分遅れ（#89）: 新系の T が旧系より前の約定時刻で、新系の直後が T と違う版に進み旧系に追いついた。
      -- 遅れは 1 分ちょうどに限る（凍結の最後の行・古い版に戻る形を説明しない）。新系が止まり続ける形（直後も T と同じ）は
      -- 当たらず、下の new_stale_same（上限 3）に残る。1 日の上限は *_cls で見る
      WHEN n.pt < l.pt AND l.pt <= DATETIME_ADD(n.pt, INTERVAL 1 MINUTE) AND n.pt >= pv.pt AND l.pt <= nx.pt AND nx.v != n.v
        AND l.base = n.base AND l.hv = n.hv THEN 'new_behind'
      -- 新系の版の遅れ（新系の T が直前と同じ中身）。旧系の版を新系が後で取ったかは問わない（#61 で 2026-10-13 の
      -- spot・future 各 2 行を見て決めた。その 4 行は #89 で new_behind に移り、ここに残るのは 2 分以上止まる・1 分を超えて
      -- 遅れる形）。1 日の上限は *_cls で見る
      WHEN n.v = pv.v THEN 'new_stale_same'
      -- 新系の T は約定時刻だけ進み価格の列が直前のまま（書き換え途中の版）。旧系は同じ約定時刻の価格まで書き換わった版で、
      -- 新系も後でその版を取った
      WHEN n.pt > pv.pt AND n.pr = pv.pr AND l.pt = n.pt AND l.base = n.base AND l.hv = n.hv
        THEN IF(sn.t IS NOT NULL, 'new_stale', 'new_stale_unconfirmed')
      ELSE 'unexplained'
    END AS cls
  FROM pairs AS p
  JOIN legacy_spot_v AS l ON l.created_at = p.t
  LEFT JOIN spot_v AS n ON n.created_at = p.t
  LEFT JOIN spot_v AS pv ON pv.created_at = p.prev_at
  LEFT JOIN spot_v AS nx ON nx.created_at = p.next_at
  LEFT JOIN legacy_spot_v AS ln ON ln.created_at = p.legacy_next_at
  LEFT JOIN spot_seen AS sn ON sn.t = p.t
),
-- (a) 10 分の中の前のスナップショットと全列一致、(b) 連続する 2 スナップショットの約定時刻の間。
-- confirmed（legacy_older）: 新系の T の約定時刻が旧系より後、(b) は S1 ≤ 旧 < S2、新系の T の値の裏付けがある。
-- それ以外で、旧系の次の約定時刻が新系の T より後（旧系 ≤ 新系 < 旧系の次）、(b) は S1 ≤ 旧 ≤ S2 のものは
-- legacy_older_unconfirmed（#89。同じ分の別の価格を含む）。1 日の上限（n_unconfirmed）は spot_cls で見る。
-- 1 回の走査で両方の旗を出す（分類の CTE の参照を増やさない）
spot_older AS (
  SELECT t, LOGICAL_OR(confirmed) AS confirmed, COUNTIF(NOT LOGICAL_OR(confirmed)) OVER () AS n_unconfirmed
  FROM (
    SELECT
      c.t,
      -- 新系の直後による裏付けは S < T のときだけ（S2 = T は旧系の次による裏付けが要る。新系の誤りが直後まで続く形を説明しない）
      IFNULL(c.npt > c.lpt AND (c.ok_ln OR (c.ok_nx AND s.created_at < c.t))
        AND ((s.created_at < c.t AND c.lv = s.v)
          OR (c.lpt >= s1.pt AND c.lpt < s.pt AND c.lbase IN (s1.base, s.base) AND c.lhv IN (s1.hv, s.hv))), FALSE) AS confirmed,
      IFNULL(c.npt >= c.lpt AND c.ln_ahead
        AND ((s.created_at < c.t AND c.lv = s.v)
          OR (c.lpt >= s1.pt AND c.lpt <= s.pt AND c.lbase IN (s1.base, s.base) AND c.lhv IN (s1.hv, s.hv))), FALSE) AS unconfirmed
    FROM spot_cls0 AS c
    JOIN recent AS w ON w.t = c.t
    JOIN spot_v AS s ON s.created_at = w.s_at
    LEFT JOIN spot_v AS s1 ON s1.created_at = w.s1_at
    WHERE c.cls = 'unexplained' AND c.ln_ok
  )
  WHERE confirmed OR unconfirmed
  GROUP BY t
),
-- 新系の版の遅れ（new_stale_same）と裏付けの無い書き換え途中の版（new_stale_unconfirmed）は、それぞれ 1 日に表ごと 3 行までを
-- new_stale、超えたらその種類を全部 unexplained にする（新系の誤りで同じ形が系統的に出ると 1 日に多数になるので捕まえる）。
-- 新系の 1 分遅れ（new_behind、#89）は 10 行まで
spot_unconfirmed AS (
  SELECT
    COUNTIF(cls = 'new_stale_same') AS n_same, COUNTIF(cls = 'new_stale_unconfirmed') AS n, COUNTIF(cls = 'new_behind') AS n_behind
  FROM spot_cls0
),
spot_cls AS (
  SELECT
    c.t,
    CASE
      WHEN c.cls = 'unexplained' AND o.confirmed THEN 'legacy_older'
      WHEN c.cls = 'unexplained' AND o.t IS NOT NULL THEN IF(o.n_unconfirmed > 3, 'unexplained', 'legacy_older_unconfirmed')
      WHEN c.cls = 'new_behind' THEN IF(u.n_behind > 10, 'unexplained', 'new_behind')
      WHEN c.cls = 'new_stale_same' THEN IF(u.n_same > 3, 'unexplained', 'new_stale')
      WHEN c.cls = 'new_stale_unconfirmed' THEN IF(u.n > 3, 'unexplained', 'new_stale')
      ELSE c.cls
    END AS cls
  FROM spot_cls0 AS c
  CROSS JOIN spot_unconfirmed AS u
  LEFT JOIN spot_older AS o ON o.t = c.t
),
future_v AS (
  SELECT
    created_at, contract_month AS cm, price_time AS pt, ROUND(price - diff, 2) AS base, hv,
    TO_JSON_STRING(STRUCT(price, diff, diff_rate)) AS pr,
    TO_JSON_STRING(STRUCT(price, price_time, diff, diff_rate, hv)) AS v
  FROM new_future
),
legacy_future_v AS (
  SELECT
    created_at, contract_month AS cm, price_time AS pt, ROUND(price - diff, 2) AS base, hv,
    TO_JSON_STRING(STRUCT(price, price_time, diff, diff_rate, hv)) AS v
  FROM legacy_future
),
future_seen AS (
  SELECT DISTINCT f.t
  FROM later AS f
  JOIN legacy_future_v AS l ON l.created_at = f.t
  JOIN future_v AS s ON s.created_at = f.s_at AND s.v = l.v AND l.cm IN (s.cm, DATE_ADD(s.cm, INTERVAL 1 YEAR))
),
future_cls0 AS (
  SELECT
    p.t, l.cm AS lcm, l.pt AS lpt, l.base AS lbase, l.hv AS lhv, l.v AS lv, n.pt AS npt,
    IFNULL(n.v = nx.v AND n.cm = nx.cm, FALSE) AS ok_nx,
    IFNULL(n.v = ln.v AND ln.cm IN (n.cm, DATE_ADD(n.cm, INTERVAL 1 YEAR)), FALSE) AS ok_ln,
    ln.created_at IS NULL OR IFNULL(ln.pt >= n.pt, FALSE) AS ln_ok,
    IFNULL(ln.pt > n.pt, FALSE) AS ln_ahead,
    CASE
      WHEN l.v = n.v AND l.cm = n.cm THEN 'match'
      WHEN l.v = n.v AND l.cm = DATE_ADD(n.cm, INTERVAL 1 YEAR) THEN 'contract_year'
      WHEN (l.v = pv.v AND l.cm IN (pv.cm, DATE_ADD(pv.cm, INTERVAL 1 YEAR)))
        OR (l.v = nx.v AND l.cm IN (nx.cm, DATE_ADD(nx.cm, INTERVAL 1 YEAR))) THEN 'version'
      WHEN n.pt < l.pt AND l.pt <= DATETIME_ADD(n.pt, INTERVAL 1 MINUTE) AND n.pt >= pv.pt AND l.pt <= nx.pt AND nx.v != n.v AND n.cm = nx.cm
        AND l.base = n.base AND l.hv = n.hv
        AND l.cm IN (n.cm, DATE_ADD(n.cm, INTERVAL 1 YEAR)) THEN 'new_behind'
      WHEN n.v = pv.v AND n.cm = pv.cm THEN 'new_stale_same'
      WHEN n.pt > pv.pt AND n.pr = pv.pr AND n.cm = pv.cm AND l.pt = n.pt AND l.base = n.base AND l.hv = n.hv
        AND l.cm IN (n.cm, DATE_ADD(n.cm, INTERVAL 1 YEAR)) THEN IF(sn.t IS NOT NULL, 'new_stale', 'new_stale_unconfirmed')
      ELSE 'unexplained'
    END AS cls
  FROM pairs AS p
  JOIN legacy_future_v AS l ON l.created_at = p.t
  LEFT JOIN future_v AS n ON n.created_at = p.t
  LEFT JOIN future_v AS pv ON pv.created_at = p.prev_at
  LEFT JOIN future_v AS nx ON nx.created_at = p.next_at
  LEFT JOIN legacy_future_v AS ln ON ln.created_at = p.legacy_next_at
  LEFT JOIN future_seen AS sn ON sn.t = p.t
),
-- spot と同じ。限月は年の補完を許す
future_older AS (
  SELECT t, LOGICAL_OR(confirmed) AS confirmed, COUNTIF(NOT LOGICAL_OR(confirmed)) OVER () AS n_unconfirmed
  FROM (
    SELECT
      c.t,
      IFNULL(c.npt > c.lpt AND (c.ok_ln OR (c.ok_nx AND s.created_at < c.t))
        AND ((s.created_at < c.t AND c.lv = s.v AND c.lcm IN (s.cm, DATE_ADD(s.cm, INTERVAL 1 YEAR)))
          OR (c.lpt >= s1.pt AND c.lpt < s.pt AND c.lbase IN (s1.base, s.base) AND c.lhv IN (s1.hv, s.hv)
            AND c.lcm IN (s.cm, DATE_ADD(s.cm, INTERVAL 1 YEAR)))), FALSE) AS confirmed,
      IFNULL(c.npt >= c.lpt AND c.ln_ahead
        AND ((s.created_at < c.t AND c.lv = s.v AND c.lcm IN (s.cm, DATE_ADD(s.cm, INTERVAL 1 YEAR)))
          OR (c.lpt >= s1.pt AND c.lpt <= s.pt AND c.lbase IN (s1.base, s.base) AND c.lhv IN (s1.hv, s.hv)
            AND c.lcm IN (s.cm, DATE_ADD(s.cm, INTERVAL 1 YEAR)))), FALSE) AS unconfirmed
    FROM future_cls0 AS c
    JOIN recent AS w ON w.t = c.t
    JOIN future_v AS s ON s.created_at = w.s_at
    LEFT JOIN future_v AS s1 ON s1.created_at = w.s1_at
    WHERE c.cls = 'unexplained' AND c.ln_ok
  )
  WHERE confirmed OR unconfirmed
  GROUP BY t
),
-- spot と同じ（new_stale は 3 行まで、new_behind は 10 行まで）
future_unconfirmed AS (
  SELECT
    COUNTIF(cls = 'new_stale_same') AS n_same, COUNTIF(cls = 'new_stale_unconfirmed') AS n, COUNTIF(cls = 'new_behind') AS n_behind
  FROM future_cls0
),
future_cls AS (
  SELECT
    c.t,
    CASE
      WHEN c.cls = 'unexplained' AND o.confirmed THEN 'legacy_older'
      WHEN c.cls = 'unexplained' AND o.t IS NOT NULL THEN IF(o.n_unconfirmed > 3, 'unexplained', 'legacy_older_unconfirmed')
      WHEN c.cls = 'new_behind' THEN IF(u.n_behind > 10, 'unexplained', 'new_behind')
      WHEN c.cls = 'new_stale_same' THEN IF(u.n_same > 3, 'unexplained', 'new_stale')
      WHEN c.cls = 'new_stale_unconfirmed' THEN IF(u.n > 3, 'unexplained', 'new_stale')
      ELSE c.cls
    END AS cls
  FROM future_cls0 AS c
  CROSS JOIN future_unconfirmed AS u
  LEFT JOIN future_older AS o ON o.t = c.t
),
-- option の値: 約定の列 tv（iv・ギリシャ指標は約定価格から求める値で、約定の列と一緒に変わる。2026-10-13 の実データで確認）、
-- 気配と表示の列 qv（is_atm はスポットに合わせて動く表示）、清算日 qd（年の補完を見るので別）。
-- legacy_older の判定に使う約定の列: pt（約定時刻）、vol（出来高）、base（基準価格 price − diff）、pos（建玉）、quo（清算値）
legacy_opt AS (
  SELECT
    created_at, last_trading_day, type, target_price, quotation_date AS qd,
    price_time AS pt, volume AS vol, price - diff AS base, positions AS pos, quotation AS quo,
    TO_JSON_STRING(STRUCT(
      price, price_time, diff, diff_rate, iv, volume, positions, quotation, delta, gamma, theta, vega)) AS tv,
    TO_JSON_STRING(STRUCT(is_atm, bid, bid_volume, bid_iv, ask, ask_volume, ask_iv)) AS qv
  FROM legacy_option
),
new_opt AS (
  SELECT
    created_at, last_trading_day, type, target_price, quotation_date AS qd,
    price_time AS pt, volume AS vol, price - diff AS base, positions AS pos, quotation AS quo,
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
    l.pt AS lpt, l.vol AS lvol, l.base AS lbase, l.pos AS lpos, l.quo AS lquo,
    n.tv AS ntv, n.qv AS nqv, n.qd AS nqd,
    n.pt AS npt, n.vol AS nvol, n.pos AS npos, n.quo AS nquo
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
    nx.created_at IS NOT NULL AS in_next, nx.tv AS xtv, nx.qv AS xqv, nx.qd AS xqd,
    nx.vol AS xvol, nx.pt AS xpt, nx.base AS xbase,
    -- legacy_older の条件: 新系の T の約定の列が新系の直後（ok_nx）か旧系の次のスナップショット（ok_ln）の同じ行と一致。
    -- 旧系の次に同じ行があれば新系の T と矛盾しない（出来高・約定時刻が新系の T 以上）こと（ln_ok）
    IFNULL(j.ntv = nx.tv, FALSE) AS ok_nx,
    IFNULL(j.ntv = ln.tv, FALSE) AS ok_ln,
    ln.created_at IS NULL OR (IFNULL(ln.vol, 0) >= IFNULL(j.nvol, 0)
      AND IFNULL(ln.pt, DATETIME '1970-01-01') >= IFNULL(j.npt, DATETIME '1970-01-01')) AS ln_ok,
    -- legacy_older_unconfirmed の条件: 旧系の次の同じ行の出来高が新系の T より多い（#89）
    ln.created_at IS NOT NULL AND IFNULL(ln.vol, 0) > IFNULL(j.nvol, 0) AS ln_ahead
  FROM opt_join AS j
  JOIN pairs AS p ON p.t = j.t
  LEFT JOIN page1_month AS m ON m.created_at = j.t
  LEFT JOIN new_opt AS pv
    ON pv.created_at = p.option_prev_at AND pv.last_trading_day = j.last_trading_day AND pv.type = j.type
    AND pv.target_price = j.target_price
  LEFT JOIN new_opt AS nx
    ON nx.created_at = p.option_next_at AND nx.last_trading_day = j.last_trading_day AND nx.type = j.type
    AND nx.target_price = j.target_price
  LEFT JOIN legacy_opt AS ln
    ON ln.created_at = p.legacy_next_at AND ln.last_trading_day = j.last_trading_day AND ln.type = j.type
    AND ln.target_price = j.target_price
),
opt_cls0 AS (
  SELECT
    t, last_trading_day, type, target_price, in_legacy, in_new, ltv, lqd, lpt, lvol, lbase, lpos, lquo, npt, nvol, ok_nx, ok_ln, ln_ok,
    ln_ahead,
    CASE
      WHEN in_legacy AND in_new AND ltv = ntv AND lqv = nqv AND lqd IS NOT DISTINCT FROM nqd THEN 'match'
      -- 版の差: 直前か直後と全列一致（旧系の行は新系の T にもあること。T の行の欠けは説明しない）。
      -- 新系の T にだけある行は、直前か直後に無ければ（行使価格の入れ替わり）
      WHEN in_legacy AND in_new AND (
          (in_prev AND ltv = ptv AND lqv = pqv AND lqd IS NOT DISTINCT FROM pqd)
          OR (in_next AND ltv = xtv AND lqv = xqv AND lqd IS NOT DISTINCT FROM xqd)) THEN 'version'
      WHEN NOT in_legacy AND ((has_prev AND NOT in_prev) OR (has_next AND NOT in_next)) THEN 'version'
      WHEN in_legacy AND in_new AND ltv = ntv AND lqv = nqv
        AND lqd IN (DATE_ADD(nqd, INTERVAL 1 YEAR), DATE_SUB(nqd, INTERVAL 1 YEAR)) THEN 'quotation_year'
      -- 気配と表示の列だけの差: 約定の列（清算日は年の補完を許す）が T・直前・直後のどれかと一致（新系の T にも行があること）
      WHEN in_legacy AND in_new AND (
          (in_new AND ltv = ntv AND (lqd IS NOT DISTINCT FROM nqd OR lqd IN (DATE_ADD(nqd, INTERVAL 1 YEAR), DATE_SUB(nqd, INTERVAL 1 YEAR))))
          OR (in_prev AND ltv = ptv AND (lqd IS NOT DISTINCT FROM pqd OR lqd IN (DATE_ADD(pqd, INTERVAL 1 YEAR), DATE_SUB(pqd, INTERVAL 1 YEAR))))
          OR (in_next AND ltv = xtv AND (lqd IS NOT DISTINCT FROM xqd OR lqd IN (DATE_ADD(xqd, INTERVAL 1 YEAR), DATE_SUB(xqd, INTERVAL 1 YEAR))))
        ) THEN 'quote_only'
      WHEN in_new AND page2_differs AND last_trading_day > page1_month THEN 'page2'
      -- 新系の 1 分遅れ（#131）: 新系の T が直前と同じ版のままで、直後が T と違う版に進み旧系に追いついた（出来高・約定時刻が
      -- 新系の T < 旧系 ≤ 直後）。遅れは 1 分に限る。新系が止まり続ける形（直後も T と同じ）は当たらず unexplained に残る。
      -- 基準価格は直後と比べる（新系の T は約定が無いことがある。2026-10-20 10:46 JST）。1 日の上限は opt_cls で見る
      WHEN in_legacy AND in_new AND in_prev AND in_next
        AND ntv = ptv AND nqd IS NOT DISTINCT FROM pqd AND xtv != ntv
        AND IFNULL(nvol, 0) < IFNULL(lvol, 0) AND IFNULL(lvol, 0) <= IFNULL(xvol, 0)
        AND IFNULL(npt, DATETIME '1970-01-01') < lpt AND lpt <= xpt
        AND lpt >= DATETIME_SUB(DATETIME(t, 'Asia/Tokyo'), INTERVAL 1 MINUTE)
        AND lpos IS NOT DISTINCT FROM npos AND lquo IS NOT DISTINCT FROM nquo
        AND (lqd IS NOT DISTINCT FROM nqd OR lqd IN (DATE_ADD(nqd, INTERVAL 1 YEAR), DATE_SUB(nqd, INTERVAL 1 YEAR)))
        AND lbase = xbase THEN 'new_behind'
      ELSE 'unexplained'
    END AS cls
  FROM opt_cand
),
-- 古い版・途中の版。新系の T は旧系の行より出来高が多く、約定時刻が同じか後（約定が無い行は出来高 0・約定時刻は最小とみなす）。
-- (a) 10 分の中の前のスナップショット S と約定の列が一致、(b) 連続する S1 < S2 の間（出来高は狭い不等号。同じ出来高なら同じ版のはず）。
-- 新系の T の約定の列の裏付けがあれば confirmed（legacy_older）。裏付けが無くても旧系の次の出来高が新系の T より多い
-- （旧系 < 新系 < 旧系の次）ものは legacy_older_unconfirmed（#89。1 日の上限 n_unconfirmed は opt_cls で見る）
opt_older AS (
  SELECT
    t, last_trading_day, type, target_price, LOGICAL_OR(confirmed) AS confirmed,
    COUNTIF(NOT LOGICAL_OR(confirmed)) OVER () AS n_unconfirmed
  FROM (
    SELECT c.t, c.last_trading_day, c.type, c.target_price, c.ok_ln OR (c.ok_nx AND s.created_at < c.t) AS confirmed
    FROM opt_cls0 AS c
    JOIN recent AS w ON w.t = c.t
    JOIN new_opt AS s
      ON s.created_at = w.s_at AND s.last_trading_day = c.last_trading_day AND s.type = c.type AND s.target_price = c.target_price
    LEFT JOIN new_opt AS s1
      ON s1.created_at = w.s1_at AND s1.last_trading_day = c.last_trading_day AND s1.type = c.type
      AND s1.target_price = c.target_price
    WHERE c.cls = 'unexplained' AND c.in_legacy AND c.in_new AND c.ln_ok
      AND (c.ok_ln OR (c.ok_nx AND s.created_at < c.t) OR c.ln_ahead)
      AND IFNULL(c.nvol, 0) > IFNULL(c.lvol, 0)
      AND IFNULL(c.npt, DATETIME '1970-01-01') >= IFNULL(c.lpt, DATETIME '1970-01-01')
      AND (
        (s.created_at < c.t AND c.ltv = s.tv
          AND (c.lqd IS NOT DISTINCT FROM s.qd OR c.lqd IN (DATE_ADD(s.qd, INTERVAL 1 YEAR), DATE_SUB(s.qd, INTERVAL 1 YEAR))))
        OR (
          s1.created_at IS NOT NULL AND c.lpt IS NOT NULL
          AND IFNULL(s1.vol, 0) < c.lvol AND c.lvol < s.vol
          AND IFNULL(s1.pt, DATETIME '1970-01-01') <= c.lpt AND c.lpt <= s.pt
          AND (c.lbase IS NOT DISTINCT FROM s1.base OR c.lbase IS NOT DISTINCT FROM s.base)
          AND (c.lpos BETWEEN LEAST(s1.pos, s.pos) AND GREATEST(s1.pos, s.pos)
            OR (c.lpos IS NULL AND (s1.pos IS NULL OR s.pos IS NULL)))
          AND (c.lquo IS NOT DISTINCT FROM s1.quo OR c.lquo IS NOT DISTINCT FROM s.quo)
          AND (c.lqd IS NOT DISTINCT FROM s1.qd OR c.lqd IS NOT DISTINCT FROM s.qd
            OR c.lqd IN (DATE_ADD(s1.qd, INTERVAL 1 YEAR), DATE_SUB(s1.qd, INTERVAL 1 YEAR),
                         DATE_ADD(s.qd, INTERVAL 1 YEAR), DATE_SUB(s.qd, INTERVAL 1 YEAR)))))
  )
  GROUP BY t, last_trading_day, type, target_price
),
opt_cls AS (
  SELECT
    c.t, c.last_trading_day, c.type, c.target_price,
    CASE
      WHEN c.cls = 'unexplained' AND o.confirmed THEN 'legacy_older'
      WHEN c.cls = 'unexplained' AND o.t IS NOT NULL THEN IF(o.n_unconfirmed > 3, 'unexplained', 'legacy_older_unconfirmed')
      -- 新系の 1 分遅れ（#131）は 1 日に 10 行まで（spot・future と同じ）。opt_cls0 を CROSS JOIN でもう一度読むと展開が増えて
      -- 「too complex」の恐れがあるので、分析関数で数える
      WHEN c.cls = 'new_behind' THEN IF(COUNTIF(c.cls = 'new_behind') OVER () > 10, 'unexplained', 'new_behind')
      ELSE c.cls
    END AS cls
  FROM opt_cls0 AS c
  LEFT JOIN opt_older AS o
    ON o.t = c.t AND o.last_trading_day = c.last_trading_day AND o.type = c.type AND o.target_price = c.target_price
),
-- #11 AC 6: 旧系の保存条件（先物の約定時刻の時:分が直前から変わった）を満たす新系のスナップショットと、そのうち旧系に無いもの
save_condition AS (
  SELECT n.snapshot_at, l.created_at IS NOT NULL AS in_legacy
  FROM new_in AS n
  LEFT JOIN legacy_counts AS l ON l.created_at = n.snapshot_at
  WHERE FORMAT_DATETIME('%H:%M', n.future_time) IS DISTINCT FROM FORMAT_DATETIME('%H:%M', n.prev_future_time)
),
-- 行の分類を 1 つにまとめて 1 回で数える（分類の CTE を何度も参照すると BigQuery がそのたびに展開し、クエリの計画が
-- 「too complex」で失敗するため）
cls_all AS (
  SELECT 'spot' AS kind, t, CAST(NULL AS STRING) AS detail, cls FROM spot_cls
  UNION ALL
  SELECT 'future', t, NULL, cls FROM future_cls
  UNION ALL
  SELECT 'option', t, FORMAT('%d %d %t', type, target_price, last_trading_day), cls FROM opt_cls
),
cls_counts AS (
  SELECT
    COUNTIF(kind = 'spot') AS spot_compared,
    COUNTIF(kind = 'spot' AND cls = 'version') AS spot_version,
    COUNTIF(kind = 'spot' AND cls = 'new_stale') AS spot_new_stale,
    COUNTIF(kind = 'spot' AND cls = 'unexplained') AS spot_unexplained,
    COUNTIF(kind = 'future') AS future_compared,
    COUNTIF(kind = 'future' AND cls = 'version') AS future_version,
    COUNTIF(kind = 'future' AND cls = 'contract_year') AS future_contract_year,
    COUNTIF(kind = 'future' AND cls = 'new_stale') AS future_new_stale,
    COUNTIF(kind = 'future' AND cls = 'unexplained') AS future_unexplained,
    COUNTIF(kind = 'option') AS option_compared,
    COUNTIF(kind = 'option' AND cls = 'version') AS option_version,
    COUNTIF(kind = 'option' AND cls = 'quote_only') AS option_quote_only,
    COUNTIF(kind = 'option' AND cls = 'quotation_year') AS option_quotation_year,
    COUNTIF(kind = 'option' AND cls = 'page2') AS option_page2,
    COUNTIF(kind = 'option' AND cls = 'unexplained') AS option_unexplained,
    COUNTIF(kind = 'spot' AND cls = 'legacy_older') AS spot_legacy_older,
    COUNTIF(kind = 'future' AND cls = 'legacy_older') AS future_legacy_older,
    COUNTIF(kind = 'option' AND cls = 'legacy_older') AS option_legacy_older,
    COUNTIF(kind = 'spot' AND cls = 'new_behind') AS spot_new_behind,
    COUNTIF(kind = 'future' AND cls = 'new_behind') AS future_new_behind,
    COUNTIF(kind = 'option' AND cls = 'new_behind') AS option_new_behind,
    COUNTIF(kind = 'spot' AND cls = 'legacy_older_unconfirmed') AS spot_legacy_older_unconfirmed,
    COUNTIF(kind = 'future' AND cls = 'legacy_older_unconfirmed') AS future_legacy_older_unconfirmed,
    COUNTIF(kind = 'option' AND cls = 'legacy_older_unconfirmed') AS option_legacy_older_unconfirmed,
    ARRAY_AGG(IF(cls = 'unexplained', STRUCT(kind, t AS created_at, detail), NULL) IGNORE NULLS
      ORDER BY t, kind, detail LIMIT 20) AS row_samples
  FROM cls_all
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
    c.* EXCEPT (row_samples),
    -- 説明できない差分の例: 旧系のみのスナップショットと各表の説明できない行の先頭 20 件
    ARRAY(
      SELECT AS STRUCT * FROM (
        SELECT AS STRUCT 'legacy_only' AS kind, created_at, CAST(NULL AS STRING) AS detail FROM snap WHERE status = 'legacy_only'
        UNION ALL
        SELECT AS STRUCT x.kind, x.created_at, x.detail FROM UNNEST(c.row_samples) AS x
      )
      ORDER BY created_at, kind, detail LIMIT 20
    ) AS unexplained_samples,
    (SELECT COUNT(*) FROM save_condition) AS legacy_condition_candidates,
    (SELECT COUNTIF(NOT in_legacy) FROM save_condition) AS legacy_condition_missed
  FROM cls_counts AS c
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
  fail_on_unexplained AND DATE(win_start, 'Asia/Tokyo') >= fail_from AS fail_on_unexplained,
  CURRENT_TIMESTAMP() AS computed_at,
  -- #61 で足した列（表の列は後ろにしか足せず、MERGE の INSERT ROW は位置で入るので最後に置く）
  spot_legacy_older,
  future_legacy_older,
  option_legacy_older,
  -- #89 で足した列
  spot_new_behind,
  future_new_behind,
  spot_legacy_older_unconfirmed,
  future_legacy_older_unconfirmed,
  option_legacy_older_unconfirmed,
  -- #131 で足した列
  option_new_behind
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

-- 説明できない差分で失敗させるのは fail_on_unexplained = true で窓の日付が fail_from 以降のときだけ（結果の fail_on_unexplained 列）
IF (SELECT fail_on_unexplained AND unexplained > 0 FROM result) THEN
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
