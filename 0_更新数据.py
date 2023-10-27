from modules.order import Order
from modules.refund import Refund

if __name__ == '__main__':
    Order.insert_all_by_file("./files/1.xlsx", "5R8J9m")
    Refund.insert_all_by_file("./files/2.xlsx")
