import json
import os
import time

import requests
from selenium import webdriver
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from utils.data_format import str_to_timestamp, timestamp_to_str


def _format_datetime(t):
    return timestamp_to_str(str_to_timestamp(t), -8, "%Y-%m-%dT%H:%M:%S.000Z")


def _query_shop_id(session):
    shop_url = "https://ksxds.woda.com/homepage/queryMultiShopSummary"
    rsp = session.get(shop_url)
    shop_id = rsp.json()["shopList"][0]["shopId"]
    return shop_id


def _query_orders_api(session, shop_id, trade_status=0, startTime=None, endTime=None):
    url = "https://ksxds.woda.com/wodaTrade/multiShopWodaTradeList"
    body = {
        "pageIndex": 1,
        "pageSize": 200,
        "wodaShopIds": [shop_id],
        "startTime": _format_datetime(startTime) if startTime else None,
        "endTime": _format_datetime(endTime) if endTime else None,
        "timeType": 0,
        "wodaTradeStatus": 1 if trade_status == 0 else 11,
        "printStatus": 0 if trade_status == 0 else 2,
        "shortcutSearch": 0,
        "shortcutMemo": 0,
        "sellerFlag": 0,
        "wodaFlag": 0,
        "tradeTags": [],
        "bTypeTags": [],
        "cBizTags": [],
        "tradeType": -1,
        "exactPopSkuShortTitles": [],
        "queryKeyType": "automatic",
        "factorySearchIds": [],
        "businessSearchIds": [],
        "sortType": "15",
        "skuIds": [],
        "exactPopItemShortTitles": [],
        "multiShopWodaShopId": shop_id,
        "multiShopOffSet": 0,
        "multiShopOffSetType": 0
    }
    have_next = True
    all_order_list = []
    while have_next:
        rsp = session.put(url, json=body)
        if rsp.json():
            all_order_list += rsp.json().get("rows", [])
            have_next = rsp.json().get('haveNext')
            if have_next:
                body["multiShopOffSet"] = body["pageIndex"] * body["pageSize"]
                body["pageIndex"] += 1
        else:
            raise Exception("我打接口请求异常：{}".format(rsp.text))
    return all_order_list


def flush_cookie():
    pass


def get_woda_order_list(start_time=None, end_time=None, trade_status=0):
    cookie_config_path = os.path.dirname(os.path.abspath(__file__)) + "/../configs/woda_cookie.txt"
    with open(cookie_config_path, "r") as f:
        cookie_str = f.read()

    req_headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36 Edg/117.0.2045.40",
        "Referer": "https://ksxds.woda.com/",
        "Origin": "https://ksxds.woda.com/",
        "accept": "application/json, text/plain, */*",
        "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6,ja;q=0.5",
        "cache-control": "no-cache",
        "content-type": "application/json;charset=UTF-8",
        "pragma": "no-cache",
        "Cookie": cookie_str,
        "sec-ch-ua": "\"Microsoft Edge\";v=\"117\", \"Not;A=Brand\";v=\"8\", \"Chromium\";v=\"117\"",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "\"macOS\"",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin"
    }
    session = requests.session()
    session.headers.update(req_headers)

    try:
        shop_id = _query_shop_id(session)
    except:
        browser = webdriver.Edge()

        ks_url = "https://s.kwaixiaodian.com/"
        browser.get(ks_url)
        WebDriverWait(browser, 180).until(EC.url_contains(ks_url))
        ks_cookie_path = os.path.dirname(os.path.abspath(__file__)) + "/../configs/ks_cookie.json"
        with open(ks_cookie_path, "w") as f:
            json.dump(browser.get_cookies(), f)

        woda_url = "https://ksxds.woda.com/"
        browser.get(woda_url)
        WebDriverWait(browser, 180).until(EC.url_contains(woda_url))
        time.sleep(3)

        cookie_str = ""
        for cookie in browser.get_cookies():
            cookie_str += "{}={};".format(cookie["name"], cookie["value"])

        with open(cookie_config_path, "w") as f:
            f.write(cookie_str)

        req_headers["Cookie"] = cookie_str
        session.headers.clear()
        session.headers.update(req_headers)
        shop_id = _query_shop_id(session)

        browser.close()
        browser.quit()

    all_order_list = _query_orders_api(session, shop_id, trade_status, start_time, end_time)

    print("共计：{} 条数据".format(len(all_order_list)))

    return all_order_list


if __name__ == '__main__':
    trade_status = 0
    result = get_woda_order_list(trade_status=trade_status)
    print(json.dumps(result, indent=4, ensure_ascii=False))
