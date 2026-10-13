"""現行（旧系）の HTTP 関数 2 つの SQL の写し（新旧のロジックの同値テスト test_sql_bq.py 用）。

写し元はデプロイ済みのソース（2026-10-13 に Cloud Functions API で取得し、git の master と同じことを確認済み）:
- smile_data: functions/optionchan_dao.py find_option_price_by_created_at
- atm_iv_data: functions_py/atm_iv/main.py query_atm_iv

本文は一字一句そのまま。変えたのは次だけ:
- テーブル名（{table} 等）を、テストが固定値で作る CTE の名前（option_price / atm_iv）にする
- atm_iv の CURRENT_TIMESTAMP() / CURRENT_DATE('Asia/Tokyo') を、テストの「現在時刻」の定数にする（結果を固定するため）
後処理（pandas）は legacy_* 関数が同じ意味で行う（列の削除、price_time の UNIX 秒化、ATM の行使価格、CSV の行）。
"""

from zoneinfo import ZoneInfo

TZ_JST = ZoneInfo("Asia/Tokyo")


def smile_query(created_at, table="option_price"):
    created_at_str = created_at.isoformat()

    query = (f'''
        WITH t AS(
            SELECT max(last_trading_day) l_max,  min(last_trading_day)  l_min
            FROM {table} WHERE created_at = "{created_at_str}"
        )
        SELECT target_price,
            MAX(CASE WHEN last_trading_day = (SELECT l_min FROM t) AND type = 1 THEN iv END) AS o1_call_iv,
            MAX(CASE WHEN last_trading_day = (SELECT l_min FROM t) AND type = 1 THEN price_time END) AS o1_call_price_time,
            MAX(CASE WHEN last_trading_day = (SELECT l_min FROM t) AND type = 2 THEN iv END) AS o1_put_iv,
            MAX(CASE WHEN last_trading_day = (SELECT l_min FROM t) AND type = 2 THEN price_time END) AS o1_put_price_time,
            MAX(CASE WHEN last_trading_day = (SELECT l_min FROM t) AND type = 2 THEN is_atm END) AS o1_put_is_atm,
            MAX(CASE WHEN last_trading_day = (SELECT l_max FROM t) AND type = 1 THEN iv END) AS o2_call_iv,
            MAX(CASE WHEN last_trading_day = (SELECT l_max FROM t) AND type = 1 THEN price_time END) AS o2_call_price_time,
            MAX(CASE WHEN last_trading_day = (SELECT l_max FROM t) AND type = 2 THEN iv END) AS o2_put_iv,
            MAX(CASE WHEN last_trading_day = (SELECT l_max FROM t) AND type = 2 THEN price_time END) AS o2_put_price_time,
            MAX(CASE WHEN last_trading_day = (SELECT l_max FROM t) AND type = 2 THEN is_atm END) AS o2_put_is_atm
        FROM {table} WHERE created_at = "{created_at_str}" GROUP BY target_price ORDER BY target_price'''
    )
    return query


def atm_iv_query(num_days, n_th_contract_month, now, table_iv="atm_iv", table_option_price="option_price"):
    query = (f'''
        WITH t1 AS (
            SELECT
                (ARRAY_AGG(DISTINCT(last_trading_day) IGNORE NULLS ORDER BY last_trading_day ASC))[OFFSET({n_th_contract_month})] AS last_trading_day
            FROM
                {table_iv}
            WHERE
                started_at > TIMESTAMP_TRUNC(TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL -10 DAY), DAY)
                AND last_trading_day >=  CURRENT_DATE('Asia/Tokyo')
        ), t2 AS (
            SELECT
                open,high,low,close, started_at
            FROM
                {table_iv}
            WHERE
                last_trading_day=(SELECT t1.last_trading_day FROM t1)
                AND started_at >= TIMESTAMP_TRUNC(TIMESTAMP_ADD(CURRENT_TIMESTAMP() , INTERVAL -{num_days} DAY), DAY)
        ), t3 AS (
            -- まだ時間足集計が保存されていない分(=UTCでの当日分)の時間足を集計する
            SELECT  
                (array_agg(iv IGNORE NULLS ORDER BY created_at ASC))[OFFSET(0)] open,
                MAX(iv) high,
                MIN(iv) low,
                (array_agg(iv IGNORE NULLS ORDER BY created_at DESC))[OFFSET(0)] close,
                TIMESTAMP_SECONDS(CAST(TRUNC(UNIX_SECONDS(created_at)/3600) AS INT64) * 3600) AS started_at
            FROM
                {table_option_price}
            WHERE
                created_at >= TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY)
                AND is_atm=True AND type=2 AND last_trading_day=(SELECT t1.last_trading_day FROM t1)
            GROUP BY started_at
        )
        SELECT
            open, high, low, close, started_at
        FROM
            t2

        UNION ALL

        SELECT
            open, high, low, close, started_at
        FROM
            t3
        ORDER BY started_at
    ''')
    now_literal = f"TIMESTAMP '{now.isoformat()}'"
    return (query.replace("CURRENT_TIMESTAMP()", now_literal)
            .replace("CURRENT_DATE('Asia/Tokyo')", f"DATE({now_literal}, 'Asia/Tokyo')"))


# ---- 後処理（functions/main.py smile_data、functions_py/atm_iv/main.py atm_iv_data と同じ意味） ----

def legacy_smile_rows(rows):
    """(1 限月 put の ATM の行使価格, CSV の行) 。ATM は o1_put_is_atm == True の最初の行（無ければ IndexError = 現行は 500）"""
    atm = [r["target_price"] for r in rows if r["o1_put_is_atm"] is True][0]
    out = []
    for r in rows:
        r = dict(r)
        r.pop("o1_put_is_atm")
        r.pop("o2_put_is_atm")
        for c in ("o1_call_price_time", "o2_call_price_time", "o1_put_price_time", "o2_put_price_time"):
            # 取引時刻は naive な JST → タイムゾーンを付けて UNIX 秒
            r[c] = None if r[c] is None else int(r[c].replace(tzinfo=TZ_JST).timestamp())
        out.append(r)
    return atm, out
