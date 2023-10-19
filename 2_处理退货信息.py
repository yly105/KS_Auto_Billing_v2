from datetime import datetime

import pandas as pd
from sqlalchemy import text
from modules import Engine, db_session
from modules.refund import Refund

pd.set_option('display.width', 1000)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.colheader_justify', 'center')
pd.set_option('display.unicode.ambiguous_as_wide', True)
pd.set_option('display.unicode.east_asian_width', True)

if __name__ == '__main__':
    sql = None
    print("退货处理".center(20, "="))
    print("1、根据退货物流号查询")
    print("2、根据SKU查询")
    print("3、根据回收状态查询")
    opt = input("请选择查询方式：")
    while True:
        if opt == "1":
            print("-" * 20)
            lid = input("请输入退款物流单号：")
            if not lid:
                continue
            sql = f'''
                select
                    r.refund_status ,
                    r.recycle_status,
                    r.recycle_time ,
--                     SUBSTRING_INDEX(o.product_name , ' ', 1) as product_name ,
                    o.sku_name,
                    r.refund_logistics_id ,
                    o.order_id ,
                    r.refund_id,
                    r.refund_reason,
                    r.refund_desc,                    o.delivery_time ,
                    r.refund_create_time ,
                    r.refund_end_time
                from
                    refunds r
                left join orders o on
                    r.order_id = o.order_id
                where
                    r.refund_logistics_id like '%{lid}%'
            '''
        if opt == "2":
            print("*" * 20)
            sku_key = input("请输入SKU关键字：")
            if not sku_key:
                continue
            sql = f"""
                select
                    r.refund_status ,
                    r.recycle_status,
                    r.recycle_time ,
--                     SUBSTRING_INDEX(o.product_name , ' ', 1) as product_name ,
                    o.sku_name,
                    r.refund_logistics_id ,
                    o.order_id ,
                    r.refund_id,
                    r.refund_reason,
                    r.refund_desc,                    o.delivery_time ,
                    r.refund_create_time ,
                    r.refund_end_time
                from
                    refunds r
                left join orders o on
                    r.order_id = o.order_id
                where
                    r.refund_logistics_id is not null
                    and
                    o.sku_name like '{sku_key}'
            """
        if opt == "3":
            print("*" * 20)
            print('0：商品已损坏')
            print('1：商品可重发')
            print('2：商品重新发出中')
            print('3：商品已重新发出')
            print('4：未处理订单')
            rs = input("请输入要查询的状态(支持多个)：")
            if not rs:
                continue
            sub_sql = []
            for c in rs:
                if c == "0":
                    sub_sql += ["r.recycle_status = '已损坏'"]
                elif c == "1":
                    sub_sql += ["r.recycle_status = '可重发'"]
                elif c == "2":
                    sub_sql += ["r.recycle_status = '重发中'"]
                elif c == "3":
                    sub_sql += ["r.recycle_status = '已重发'"]
                else:
                    sub_sql += ["r.recycle_status is null and r.refund_logistics_id is not null"]
            sub_sql_str = " or ".join(sub_sql)
            sql = f"""
                select
                    r.refund_status ,
                    r.recycle_status,
                    r.recycle_time ,
--                     SUBSTRING_INDEX(o.product_name , ' ', 1) as product_name ,
                    o.sku_name,
                    r.refund_logistics_id ,
                    o.order_id ,
                    r.refund_id,
                    r.refund_reason,
                    r.refund_desc,                    o.delivery_time ,
                    r.refund_create_time ,
                    r.refund_end_time
                    from
                        refunds r
                    left join orders o on
                        r.order_id = o.order_id
                    where
                        {sub_sql_str}
            """

        if sql:
            refund_id = None
            recycle_status = None
            with Engine.connect() as con:
                rs = con.execute(text(sql))
                df = pd.DataFrame(rs)
                print(df)
            if len(df) == 0:
                print('未查到数据，请重新输入...')
                continue
            elif len(df) > 1:
                idx = input("选择要处理的数据序号：")
                if not idx:
                    continue
                print(df.iloc[int(idx)].to_string())
                refund_id = df.iloc[int(idx)]['refund_id']
                recycle_status = df.iloc[int(idx)]['recycle_status']
            else:
                refund_id = df.iloc[0]['refund_id']
                recycle_status = df.iloc[0]['recycle_status']
            print("*" * 20)
            if recycle_status:
                ctn = input("该退货已处理，输入 y 继续：")
                if ctn != "y" and ctn != "Y":
                    continue
            print(f"将要处理的退款单号为：{refund_id}，设置状态为：")
            print('0：商品已损坏')
            print('1：商品可重发')
            print('2：商品重新发出中')
            print('3：商品已重新发出')
            idx = input("请选择您的操作：")
            if idx not in ["1", "2", "3"]:
                continue
            rfd = db_session.query(Refund).filter(Refund.refund_id == refund_id).first()
            rfd.recycle_status = ["已损坏", "可重发", "重发中", "已重发"][int(idx)]
            rfd.recycle_time = datetime.now()
            db_session.commit()
            print("数据库更新成功！")
