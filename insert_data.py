from modules import db_session
from modules.order import Order
from modules.refund import Refund

if __name__ == '__main__':
    # Order.insert_all_by_file("./order-2.xlsx")
    Refund.insert_all_by_file("./refund.xlsx")