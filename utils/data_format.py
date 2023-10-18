import math
import datetime
import pandas as pd


def floor_float(v, n=2):
    if isinstance(v, str):
        v = float(v.replace("¥", ""))
    return round(float(v), 2)


def timestamp_to_str(timestamp, extra_hours=0, format_str='%Y-%m-%d %H:%M:%S'):
    timestamp = timestamp / 1000
    dt_object = datetime.datetime.fromtimestamp(timestamp)
    dt_object += datetime.timedelta(hours=extra_hours)
    formatted_date = dt_object.strftime(format_str)
    return formatted_date


def str_to_timestamp(date_string, format_str='%Y-%m-%d %H:%M:%S'):
    dt_object = datetime.datetime.strptime(date_string, format_str)
    timestamp = dt_object.timestamp()
    timestamp_ms = int(timestamp * 1000)
    return timestamp_ms


def str_to_datetime(date_string, format_str='%Y-%m-%d %H:%M:%S'):
    dt_object = datetime.datetime.strptime(date_string, format_str)
    return dt_object
