import datetime
import json
import os

import pandas as pd
from sqlalchemy import text
from tabulate import tabulate

from modules import Engine, db_session
from modules.refund import Refund
from services.woda_query import get_woda_order_list

pd.set_option('display.width', 1000)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.colheader_justify', 'center')
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

    msg = ""
    opt_refund_id_list = []
    for product_name, sku_info in need_sku.items():
        for sku_name in sku_info.keys():
            sql = f'''
                select
                    r.refund_id,
                    r.refund_logistics_id,
                    r.refund_end_time,
                    r.recycle_status,
                    r.record_update_time
                from
                    refunds r
                left join orders o on
                    r.order_id = o.order_id
                WHERE
                    o.sku_name = '{sku_name}'
                    AND
                    (r.recycle_status = "可重发" or r.recycle_status = "重发中")
            '''
            with Engine.connect() as con:
                rs = con.execute(text(sql))
                if rs.rowcount:
                    df = pd.DataFrame(rs)
                    q = min(need_sku[product_name][sku_name], rs.rowcount)
                    msg += f"\n\n***【{product_name}】-【{sku_name}】 有存货！ =》 需求：{need_sku[product_name][sku_name]}个，存货{rs.rowcount}个，可重发 {q} 个\n"
                    msg += tabulate(df, headers='keys', tablefmt='psql', showindex=False)
                    for index, row in df.iterrows():
                        if index == q:
                            break
                        opt_refund_id_list.append(row['refund_id'])

    if msg:
        formatted_time = datetime.datetime.now().strftime('%Y-%m-%d_%H:%M:%S')
        msg = (f"退货重发匹配记录时间：{formatted_time}"
               + msg
               + f"\n{'=-' * 30}\n可重发商品退款单编号：{opt_refund_id_list}，共计 {len(opt_refund_id_list)} 个")
        print(msg)
        print('=-' * 30)
        ctn = input("是否将回收状态改为'重发中'，输入 y 保存：")
        if ctn == "y" or ctn == "Y":
            for rid in opt_refund_id_list:
                rfd = db_session.query(Refund).filter(Refund.refund_id == rid).first()
                rfd.recycle_status = "重发中"
                rfd.recycle_time = datetime.datetime.now()
                db_session.commit()
            print('已完成数据修改～')
            file_path = os.path.dirname(os.path.abspath(__file__)) + f"/logs/重发-{formatted_time}.txt"
            with open(file_path, "w", encoding="utf8") as f:
                f.write(msg)
                print(f"文件保存成功!{file_path}")
        else:
            print("取消文件保存！")
    else:
        print("未查到任何数据!")
