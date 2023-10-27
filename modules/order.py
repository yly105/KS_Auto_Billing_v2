import io
import re

from sqlalchemy import Column, Integer, String, DateTime, Float, func, and_
import msoffcrypto
from configs.fee_conf import *
from modules import Base, db_session
import pandas
from tqdm import tqdm

from modules.product import Product
from utils.data_format import str_to_datetime, floor_float


class Order(Base):
    __tablename__ = 'orders'

    order_id = Column(String, primary_key=True)
    create_time = Column(DateTime)
    pay_time = Column(DateTime)
    order_status = Column(DateTime)
    amount = Column(Float)
    num = Column(Integer)
    buyer_msg = Column(String)
    seller_msg = Column(String)
    refund_status = Column(String)
    product_name = Column(String)
    product_id = Column(String)
    sku_name = Column(String)
    sku_code = Column(String)
    price = Column(Float)
    user_name = Column(String)
    user_phone = Column(String)
    receiver_province = Column(String)
    receiver_city = Column(String)
    receiver_district = Column(String)
    receiver_street = Column(String)
    delivery_time = Column(DateTime)
    logistics_company = Column(String)
    logistics_id = Column(String)
    delivery_status = Column(String)
    real_amount = Column(Float)
    cost = Column(Float)
    extra_cost = Column(Float)
    packing_fee = Column(Float)
    logistics_fee = Column(Float)
    insurance_fee = Column(Float)
    service_fee = Column(Float)
    record_update_time = Column(DateTime, server_default=func.now(), onupdate=func.now())

    __KEY__ = {
        'order_id': "订单号",
        'create_time': "订单创建时间",
        'pay_time': "订单支付时间",
        'order_status': "订单状态",
        'amount': "实付款",
        'num': "成交数量",
        'buyer_msg': "买家留言",
        'seller_msg': "订单备注",
        'refund_status': "退货退款",
        'product_name': "商品名称",
        'product_id': "商品ID",
        'sku_name': "商品规格",
        'sku_code': "SKU编码",
        'price': "商品单价",
        'user_name': "收货人姓名",
        'user_phone': "收货人电话",
        'receiver_province': "收货地址-省",
        'receiver_city': "收货地址-市",
        'receiver_district': "收货地址-区",
        'receiver_street': "收货地址-街道",
        'delivery_time': "发货时间",
        'logistics_company': "快递公司",
        'logistics_id': "快递单号",
        'delivery_status': "物流信息"
    }

    def set_data_by_file_row(self, **keyword):
        for k, v in keyword.items():
            if keyword[k] == '':
                keyword[k] = None
            if pandas.isna(v):
                keyword[k] = None
            elif k == self.__KEY__["order_id"]:
                keyword[k] = str(v)
            elif k == self.__KEY__["product_id"]:
                keyword[k] = str(v)
            elif k == self.__KEY__["logistics_id"]:
                keyword[k] = str(v).split(".")[0] if v else None
            elif k == self.__KEY__["create_time"]:
                keyword[k] = str_to_datetime(v)
            elif k == self.__KEY__["pay_time"]:
                keyword[k] = str_to_datetime(v)
            elif k == self.__KEY__["delivery_time"]:
                keyword[k] = str_to_datetime(v)
            elif k == self.__KEY__["amount"]:
                keyword[k] = floor_float(v)
            elif k == self.__KEY__["price"]:
                keyword[k] = floor_float(v)

        msg = ''
        for k, v in self.__KEY__.items():
            if self.__getattribute__(k) != keyword.get(v):
                msg += f"{v}: {self.__getattribute__(k)} -> {keyword.get(v)}；"
                self.__setattr__(k, keyword.get(v))
        return msg

    @staticmethod
    def insert_row_by_file(**keyword):
        need_commit = False
        order_id = keyword.get(Order.__KEY__["order_id"])
        od = db_session.query(Order).filter(Order.order_id == order_id).first()
        if od:
            msg = od.set_data_by_file_row(**keyword)
            if msg:
                need_commit = True
                print(f"修改订单数据：{order_id} => {msg}")
        else:
            od = Order()
            od.set_data_by_file_row(**keyword)
            od.packing_fee = 0.0
            od.logistics_fee = 0.0
            if od.pay_time:
                od.real_amount = od.amount
                sku_list = Product.query_sku_by_product_id(od.product_id)
                if sku_list:
                    for sku in sku_list:
                        sku_sort_name = sku["sku_name"]
                        if (sku_sort_name.startswith("...")
                                or re.match("^" + sku_sort_name.replace("*", ".+") + "$", od.sku_name.split(",")[0])):
                            od.cost = sku.get("cost") * od.num
                            od.extra_cost = (sku.get("extra_cost") if sku.get(
                                "extra_cost") else 0) * od.num
                            od.insurance_fee = insurance_fee if floor_float(od.price) >= 9 else 0
                            od.service_fee = floor_float(od.amount * service_fee_rate)
                            break
                    else:
                        raise Exception(
                            f"【错误】未知的商品规格：{od.product_id} - {od.sku_name.split(',')[0]}！！！")
                else:
                    raise Exception(f"【错误]】未知的商品ID：{od.product_id}!!!")
            else:
                od.real_amount = 0.0
                od.cost = 0
                od.extra_cost = 0
                od.insurance_fee = 0
                od.service_fee = 0

            need_commit = True
            db_session.add(od)
            print(f"新增订单数据：{order_id}！")

        # 如果运单号不重复则更新发货费用
        if need_commit:
            if od.logistics_id and not db_session.query(Order).filter(
                    and_(Order.logistics_id == od.logistics_id, Order.order_id != od.order_id)).first():
                od.packing_fee = packing_fee
                od.logistics_fee = logistics_fee

            db_session.commit()

    @staticmethod
    def insert_all_by_file(file_path, passwd=None):
        if passwd:
            temp = io.BytesIO()
            with open(file_path, 'rb') as f:
                excel = msoffcrypto.OfficeFile(f)
                excel.load_key(passwd)
                excel.decrypt(temp)
            df = pandas.read_excel(temp)
        else:
            df = pandas.read_excel(file_path)
        data = df.to_dict("records")
        print(f'{file_path}文件读取成功！共计{len(data)} 条数据...')
        for record in tqdm(data):
            Order.insert_row_by_file(**record)


if __name__ == '__main__':
    r = db_session.query(Order).filter(Order.logistics_id == None).first()
    print(r)
