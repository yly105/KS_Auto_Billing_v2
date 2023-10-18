import pandas as pd
from sqlalchemy import text
from tabulate import tabulate

from modules import Engine, db_session
from modules.order import Order
from modules.refund import Refund

if __name__ == '__main__':
    refund_id = None
    sql = None
    print("退货处理".center(20, "="))
    print("1、根据退货物流号查询")
    print("2、根据SKU查询")
    opt = input("请选择查询方式：")
    while True:
        if opt == "1":
            print("-" * 20)
            lid = input("请输入退款物流单号：")
            sql = f'''
                select
                    o.order_id ,
                    o.delivery_time ,
                    o.refund_status ,
                    o.product_id ,
                    SUBSTRING_INDEX(o.product_name , ' ', 1) as product_name ,
                    o.sku_name ,
                    r.refund_id ,
                    r.refund_logistics_id ,
                    r.refund_status ,
                    r.recycle_status
                from
                    refunds r
                left join orders o on
                    r.order_id = o.order_id
                where
                    r.refund_logistics_id like '%{lid}%'
            '''
        if opt == "2":
            print("*" * 20)
            lid = input("请输入SKU关键字(多个空格隔开)）：")
            keys = lid.split(" ")
            sub_sql = f"(o.product_name like '%{keys[0]}%' or o.sku_name like '%{keys[0]}%')"
            for sub_key in keys[1:]:
                sub_sql += f" and o.sku_name like '%{sub_key}%'"
            sql = f"""
                select
                    o.order_id ,
                    o.delivery_time ,
                    o.order_status ,
                    o.product_id ,
                    SUBSTRING_INDEX(o.product_name , ' ', 1) as product_name ,
                    o.sku_name,
                    r.refund_id,
                    r.refund_logistics_id ,
                    r.refund_status ,
                    r.recycle_status
                from
                    refunds r
                left join orders o on
                    r.order_id = o.order_id
                where
                    r.refund_logistics_id is not null
                    and
                    {sub_sql}
            """
        if sql:
            with Engine.connect() as con:
                rs = con.execute(text(sql))
                df = pd.DataFrame(rs)

                print(tabulate(df, tablefmt='psql', headers='keys'))
            if len(df) == 0:
                print('未查到数据，请重新输入...')
                break
            elif len(df) > 1:
                idx = input("选择要处理的数据序号：")
                print(df.iloc[int(idx)].to_string())
                refund_id = df.iloc[int(idx)]['refund_id']
            else:
                refund_id = df.iloc[0]['refund_id']
            print("*" * 20)
            print(f"将要处理的退款单号为：{refund_id}，设置状态为：")
            print('0：商品已损坏')
            print('1：商品可重发')
            print('2：商品重新发出中')
            print('3：商品已重新发出')
            idx = input("请选择您的操作：")
            rfd = db_session.query(Refund).filter(Refund.refund_id == refund_id).first()
            rfd.recycle_status = ["已损坏", "可重发", "重发中", "已重发"][int(idx)]
            db_session.commit()
            print("数据库更新成功！")
