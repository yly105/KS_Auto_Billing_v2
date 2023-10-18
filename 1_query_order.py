from sqlalchemy import text
from tabulate import tabulate
import pandas as pd

from modules import Engine

start_time = '2023-10-17 00:00:00'
end_time = '2023-10-18 23:59:59'

sql = f"""
SELECT
    '总计' AS 日期,
    ROUND(SUM(amount), 2) AS 订单金额,
    ROUND(SUM(real_amount), 2) AS 实收金额,
    ROUND(SUM(real_amount)-SUM(amount), 2) AS 退款金额,
    ROUND((SUM(amount)-SUM(real_amount))/SUM(amount), 2) AS 退款占比,
    ROUND(SUM(cost) + SUM(extra_cost) + SUM(packing_fee) + SUM(insurance_fee) + SUM(service_fee) + SUM(logistics_fee), 2) AS 总共成本,
    ROUND(SUM(real_amount) - (SUM(cost) + SUM(extra_cost) + SUM(packing_fee) + SUM(insurance_fee) + SUM(service_fee) + SUM(logistics_fee)), 2) AS 总利润,
    ROUND((SUM(real_amount) - (SUM(cost) + SUM(extra_cost) + SUM(packing_fee) + SUM(insurance_fee) + SUM(service_fee) + SUM(logistics_fee))) / SUM(real_amount), 2) AS 利润比,
    ROUND(SUM(cost) + SUM(extra_cost) + SUM(packing_fee), 2) AS 给晖哥
FROM
    orders
WHERE
    create_time BETWEEN '{start_time}' AND '{end_time}' AND pay_time IS NOT NULL 
UNION ALL
SELECT
    DATE(create_time) AS 日期,
    ROUND(SUM(amount), 2) AS 订单金额,
    ROUND(SUM(real_amount), 2) AS 实收金额,
    ROUND(SUM(real_amount)-SUM(amount), 2) AS 退款金额,
    ROUND((SUM(amount)-SUM(real_amount))/SUM(amount), 2) AS 退款占比,
    ROUND(SUM(cost) + SUM(extra_cost) + SUM(packing_fee) + SUM(insurance_fee) + SUM(service_fee) + SUM(logistics_fee), 2) AS 总共成本,
    ROUND(SUM(real_amount) - (SUM(cost) + SUM(extra_cost) + SUM(packing_fee) + SUM(insurance_fee) + SUM(service_fee) + SUM(logistics_fee)), 2) AS 总利润,
    ROUND((SUM(real_amount) - (SUM(cost) + SUM(extra_cost) + SUM(packing_fee) + SUM(insurance_fee) + SUM(service_fee) + SUM(logistics_fee))) / SUM(real_amount), 2) AS 利润比,
    ROUND(SUM(cost) + SUM(extra_cost) + SUM(packing_fee), 2) AS 给晖哥
FROM
    orders
WHERE
    create_time BETWEEN '{start_time}' AND '{end_time}' AND pay_time IS NOT NULL 
GROUP BY
    DATE(create_time)
"""

with Engine.connect() as con:
    rs = con.execute(text(sql))
    df = pd.DataFrame(rs,
                      columns=['日期', '订单金额', '实收金额', "退款金额", "退款占比", '总共成本', '总利润', '利润比',
                               '给晖哥'])

    print(tabulate(df, headers='keys', tablefmt='psql', showindex=False))
