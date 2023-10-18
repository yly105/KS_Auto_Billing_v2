from modules.order import Order
from modules.refund import Refund

if __name__ == '__main__':
    Order.insert_all_by_file("./files/order.xlsx")
    Refund.insert_all_by_file("./files/refund.xlsx")
