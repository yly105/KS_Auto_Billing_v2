import json

import pandas as pd
from sqlalchemy import text
from tabulate import tabulate

from modules import Engine
from services.woda_query import get_woda_order_list

pd.set_option('display.width', 1000)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.unicode.ambiguous_as_wide', True)
pd.set_option('display.unicode.east_asian_width', True)

if __name__ == '__main__':
    trade_status = 0
    record_list = get_woda_order_list(trade_status=trade_status)
    need_sku = {}
    for rd in record_list:
        for t in rd.get('trades', []):
            for od in t.get("orders", []):
                short_title = od.get("shortTitle").split()[0]
                short_sku_name = od.get('shortSkuName')
                if need_sku.get(short_title):
                    if need_sku.get(short_title).get(short_sku_name):
                        need_sku[short_title][short_sku_name] += 1
                    else:
                        need_sku[short_title][short_sku_name] = 1
                else:
                    need_sku[short_title] = {}
                    need_sku[short_title][short_sku_name] = 1

    print("需求列表：")
    print(json.dumps(need_sku, indent=4, ensure_ascii=False))
    print("=-" * 30)

    for product_name, sku_info in need_sku.items():
        for sku_name in sku_info.keys():
            sql = f'''
                select
                    r.refund_id,
                    r.refund_logistics_id,
                    r.recycle_status
                from
                    refunds r
                left join orders o on
                    r.order_id = o.order_id
                WHERE
                    o.sku_name = '{sku_name}'
                    AND
                    r.recycle_status = "可重发"
            '''
            with Engine.connect() as con:
                rs = con.execute(text(sql))
                if rs.rowcount:
                    df = pd.DataFrame(rs)
                    print(tabulate(df, headers='keys', tablefmt='psql', showindex=False))
                    print(
                        f"***【{product_name}】-【{sku_name}】 有存货！ =》 需求：{need_sku[product_name][sku_name]}个，存货{rs.rowcount}个\n")
    else:
        print("未查到任何数据")
