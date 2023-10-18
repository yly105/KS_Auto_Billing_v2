from modules import db_session
from modules.order import Order
from sqlalchemy import func, union_all, literal_column, and_, not_
import pandas as pd
from tabulate import tabulate

start_time = '2023-09-26 00:00:00'
end_time = '2023-10-15 23:59:59'
print(f"开始计算 {start_time} 到 {end_time} 的订单数据...")

daily_query = db_session.query(
    func.date(Order.create_time).label('日期'),
    func.round(func.sum(Order.amount), 2).label('订单金额'),
    func.round(func.sum(Order.real_amount), 2).label('实收金额'),
    func.round(func.sum(Order.cost) + func.sum(Order.extra_cost) + func.sum(Order.packing_fee), 2).label('给晖哥'),
    func.round(func.sum(Order.cost) + func.sum(Order.extra_cost) + func.sum(Order.packing_fee) + func.sum(
        Order.insurance_fee) + func.sum(Order.service_fee) + func.sum(Order.logistics_fee), 2).label('总共成本'),
    func.round(func.sum(Order.real_amount) - (
            func.sum(Order.cost) + func.sum(Order.extra_cost) + func.sum(Order.packing_fee) + func.sum(
        Order.insurance_fee) + func.sum(Order.service_fee) + func.sum(Order.logistics_fee)), 2).label('总利润'),
    func.round((func.sum(Order.real_amount) - (
            func.sum(Order.cost) + func.sum(Order.extra_cost) + func.sum(Order.packing_fee) + func.sum(
        Order.insurance_fee) + func.sum(Order.service_fee) + func.sum(Order.logistics_fee))) / func.sum(
        Order.real_amount), 2).label('利润比')
).filter(
    and_(Order.create_time.between(start_time, end_time), Order.pay_time != None)
).group_by(
    func.date(Order.create_time)
)

total_query = db_session.query(
    literal_column("'总计'").label('日期'),
    func.round(func.sum(Order.amount), 2).label('订单金额'),
    func.round(func.sum(Order.real_amount), 2).label('实收金额'),
    func.round(func.sum(Order.cost) + func.sum(Order.extra_cost) + func.sum(Order.packing_fee), 2).label('给晖哥'),
    func.round(func.sum(Order.cost) + func.sum(Order.extra_cost) + func.sum(Order.packing_fee) + func.sum(
        Order.insurance_fee) + func.sum(Order.service_fee) + func.sum(Order.logistics_fee), 2).label('总共成本'),
    func.round(func.sum(Order.real_amount) - (
            func.sum(Order.cost) + func.sum(Order.extra_cost) + func.sum(Order.packing_fee) + func.sum(
        Order.insurance_fee) + func.sum(Order.service_fee) + func.sum(Order.logistics_fee)), 2).label('总利润'),
    func.round((func.sum(Order.real_amount) - (
            func.sum(Order.cost) + func.sum(Order.extra_cost) + func.sum(Order.packing_fee) + func.sum(
        Order.insurance_fee) + func.sum(Order.service_fee) + func.sum(Order.logistics_fee))) / func.sum(
        Order.real_amount), 2).label('利润比')
).filter(
    and_(Order.create_time.between(start_time, end_time), Order.pay_time != None)
)

compound_query = union_all(total_query, daily_query)

results = db_session.execute(compound_query).fetchall()

# Convert results to DataFrame
pd.set_option('display.unicode.ambiguous_as_wide', True)
pd.set_option('display.unicode.east_asian_width', True)
df = pd.DataFrame(results, columns=[u'日期', u'订单金额', u'实收金额', u'给晖哥', u'总共成本', u'总利润', u'利润比'])

print(tabulate(df, headers='keys', tablefmt='psql', showindex=False))
