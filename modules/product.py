from pprint import pprint

from sqlalchemy import Column, Integer, String, DateTime, Float
from modules import Base, db_session


class Product(Base):
    __tablename__ = 'products'

    id = Column(Integer, primary_key=True)
    product_id = Column(String)
    product_title = Column(String)
    sku_img = Column(String)
    sku_name = Column(String)
    is_hqb = Column(Integer)
    cost = Column(Float)
    extra_cost = Column(Float)
    remark = Column(String)

    @staticmethod
    def query_sku_by_product_id(product_id):
        products = db_session.query(Product).filter(Product.product_id == product_id).all()
        sku_list = []
        for pd in products:
            sku_list.append({
                "sku_name": pd.sku_name,
                "sku_img": pd.sku_img,
                "is_hqb": pd.is_hqb,
                "cost": pd.cost,
                "extra_cost": pd.extra_cost,
                "remark": pd.remark
            })
        return sku_list

    @staticmethod
    def query_all_to_dict():
        result = {}
        products = db_session.query(Product).all()
        for pd in products:
            if not result.get(pd.product_id):
                result[pd.product_id] = {
                    "product_title_list": [],
                    "sku_list": []
                }
            if pd.product_title not in result[pd.product_id]["product_title_list"]:
                result[pd.product_id]["product_title_list"].append(pd.product_title)

            result[pd.product_id]["sku_list"].append({
                "sku_name": pd.sku_name,
                "sku_img": pd.sku_img,
                "is_hqb": pd.is_hqb,
                "cost": pd.cost,
                "extra_cost": pd.extra_cost,
                "remark": pd.remark
            })
        return result


if __name__ == '__main__':
    # result = Product.query_all_to_dict()
    result = Product.query_by_product_id("20239375172546")
    pprint(result)
