import numpy
from sqlalchemy import Column, Integer, String, DateTime, Float, func, and_

from modules import Base, db_session
import pandas
from tqdm import tqdm

from modules.order import Order
from utils.data_format import str_to_datetime, floor_float


class Refund(Base):
    __tablename__ = 'refunds'

    order_id = Column(String)
    order_status = Column(String)
    order_create_time = Column(DateTime)
    product_id = Column(String)
    product_name = Column(String)
    buyer_id = Column(String)
    buyer_nickname = Column(String)
    refund_id = Column(String, primary_key=True)
    refund_status = Column(String)
    refund_create_time = Column(DateTime)
    refund_type = Column(String)
    refund_reason = Column(String)
    refund_desc = Column(String)
    amount = Column(Float)
    refund_amount = Column(Float)
    refund_logistics_id = Column(String)
    refund_end_time = Column(String)
    is_atom = Column(Integer)
    record_update_time = Column(DateTime, server_default=func.now(), onupdate=func.now())
    refund_result = Column(String)
    recycle_status = Column(String)

    __KEY__ = {
        'order_id': "订单编号",
        'order_status': "订单状态",
        'order_create_time': "订单创建时间",
        'product_id': "商品ID",
        'product_name': "商品名称",
        'buyer_id': "买家ID",
        'buyer_nickname': "买家昵称",
        'refund_id': "售后单号",
        'refund_status': "售后状态",
        'refund_create_time': "售后申请时间",
        'refund_type': "售后类型",
        'refund_reason': "售后原因",
        'refund_desc': "申请描述",
        'amount': "商家实收金额",
        'refund_amount': "退款金额",
        'refund_logistics_id': "退货物流单号",
        'refund_end_time': "售后完结时间",
        'is_atom': "是否ATOM处理"
    }

    def set_data_by_file_row(self, **keyword):
        for k, v in keyword.items():
            if pandas.isna(v):
                keyword[k] = None
            elif k == self.__KEY__["order_id"]:
                keyword[k] = str(v)
            elif k == self.__KEY__["refund_id"]:
                keyword[k] = str(v)
            elif k == self.__KEY__["buyer_id"]:
                keyword[k] = str(v)
            elif k == self.__KEY__["product_id"]:
                keyword[k] = str(v)
            elif k == self.__KEY__["refund_logistics_id"]:
                keyword[k] = str(v).split(".")[0] if v else None
            elif k == self.__KEY__["order_create_time"]:
                keyword[k] = str_to_datetime(v)
            elif k == self.__KEY__["refund_create_time"]:
                keyword[k] = str_to_datetime(v)
            elif k == self.__KEY__["refund_end_time"]:
                keyword[k] = str_to_datetime(v)
            elif k == self.__KEY__["amount"]:
                keyword[k] = floor_float(v)
            elif k == self.__KEY__["refund_amount"]:
                keyword[k] = floor_float(v)
            elif k == self.__KEY__["is_atom"]:
                keyword[k] = 1 if v == "是" else 0

        msg = ''
        for k, v in self.__KEY__.items():
            if self.__getattribute__(k) != keyword.get(v):
                msg += f"{v}: {self.__getattribute__(k)} -> {keyword.get(v)}；"
                self.__setattr__(k, keyword.get(v))
        return msg

    @staticmethod
    def insert_row_by_file(**keyword):
        need_commit = False
        order_id = keyword.get(Refund.__KEY__["order_id"])
        refund_id = keyword.get(Refund.__KEY__["refund_id"])
        refund_end_time = keyword.get(Refund.__KEY__["refund_end_time"])
        od = db_session.query(Order).filter(Order.order_id == order_id).first()
        if not od:
            print(f"未找到对应的商品订单【{order_id}】!!!")
            return {order_id: "未找到对应的商品订单"}
        if pandas.notna(refund_end_time) and od.record_update_time < str_to_datetime(refund_end_time):
            print(f"商品订单【{order_id}】的更新时间小于售后完成时间!!!")
            return {order_id: "订单更新时间小于售后完成时间"}

        rfd = db_session.query(Refund).order_by(Refund.refund_create_time).filter(Refund.refund_id == refund_id).first()
        if rfd:
            msg = rfd.set_data_by_file_row(**keyword)
            if msg:
                need_commit = True
                print(f"修改退款订单【{refund_id}】！{msg}")
        else:
            rfd = Refund()
            rfd.set_data_by_file_row(**keyword)
            need_commit = True
            db_session.add(rfd)
            print(f"新增退款订单【{refund_id}】！")

        if need_commit:
            if rfd.refund_status == "售后成功":
                if not od.logistics_id :
                    od.real_amount = 0.0
                    od.cost = 0.0
                    od.extra_cost = 0.0
                    od.packing_fee = 0.0
                    od.logistics_fee = 0.0
                    od.insurance_fee = 0.0
                    od.service_fee = 0.0
                    rfd.refund_result = "未发货退款"
                else:
                    if rfd.refund_logistics_id:
                        od.real_amount = 0.0
                        od.service_fee = 0.0
                        rfd.refund_result = "退货退款"
                    else:
                        if od.delivery_status == "签收":
                            od.real_amount = 0.0
                            od.service_fee = 0.0
                            rfd.refund_result = "收货仅退款"
                        else:
                            od.real_amount = 0.0
                            od.service_fee = 0.0
                            od.cost = 0.0
                            od.extra_cost = 0.0
                            rfd.refund_result = "拦截退回"

            db_session.commit()

    @staticmethod
    def insert_all_by_file(file_path):
        df = pandas.read_excel(file_path)
        data = df.to_dict("records")
        print(f'{file_path}文件读取成功！共计{len(data)} 条数据...')
        failed_record_list = []
        for record in tqdm(data):
            failed_order_id = Refund.insert_row_by_file(**record)
            if failed_order_id:
                failed_record_list.append(failed_order_id)
        if failed_record_list:
            print(f"处理失败数据：{failed_record_list}")
