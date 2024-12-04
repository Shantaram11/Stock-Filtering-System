import statistics

import pyTSL
import pandas as pd
from datetime import datetime, timedelta
import numpy as np
from chinese_calendar import is_workday
import matplotlib.pyplot as plt


def n_trade_day_before(date, n):
    if isinstance(date, str):
        date = datetime.strptime(date, '%Y%m%d').date()
    diff_days = 0
    while diff_days < n:
        date = (date + timedelta(days=-1))
        if is_workday(date):
            diff_days += 1
    return date.strftime('%Y%m%d')

def get_date(code, n):
    today = datetime.today()
    begin_date = n_trade_day_before(today, n)
    begin_date = datetime.strptime(begin_date, '%Y%m%d').date()
    r = c.query(stock=code, cycle="日线", begin_time=begin_date, end_time=today, fields="date, price, vol")
    data = pd.DataFrame(r.value())
    print(data)
    return np.array(data["date"]), np.array(data["price"]), np.array(data["vol"])

def get_lines(code, n):
    date_set, price_set, vol_set = get_date(code, n)
    avg_price_4 = list()
    avg_price_8 = list()
    avg_price_17 = list()
    for i in range(21, len(price_set)):
        avg_price_4.append(float(price_set[i-4:i].mean()))
    for i in range(21, len(price_set)):
        avg_price_8.append(float(price_set[i-8:i].mean()))
    for i in range(21, len(price_set)):
        avg_price_17.append(float(price_set[i-17:i].mean()))
    avg_vol_7 = list()
    avg_vol_13 = list()
    avg_vol_20 = list()
    for i in range(21, len(vol_set)):
        avg_vol_7.append(float(vol_set[i-7:i].mean()))
    for i in range(21, len(vol_set)):
        avg_vol_13.append(float(vol_set[i-13:i].mean()))
    for i in range(21, len(vol_set)):
        avg_vol_20.append(float(vol_set[i-20:i].mean()))
    return date_set[21:], price_set[21:], vol_set[21:], avg_price_4, avg_price_8, avg_price_17, avg_vol_7, avg_vol_13, avg_vol_20

def get_adjust_value(code, n):
    date_set, price_set, vol_set, avg_price_4, avg_price_8, avg_price_17, avg_vol_7, avg_vol_13, avg_vol_20 = get_lines(code, n)
    result_set = list()
    for day in range(len(price_set)):
        result = 1
        if avg_price_4[day] >= avg_price_8[day]:
            result = result * 1.2
        else:
            result = result * 0.8

        if avg_price_4[day] >= avg_price_17[day] :
            result = result*1.4
        else:
            result = result*0.7

        if avg_vol_7[day] >= avg_vol_13[day]:
            result = result*1.2
        else:
            result = result*0.8

        if avg_vol_7[day] >= avg_vol_20[day]:
            result = result*1.4
        else:
            result = result*0.7

        if price_set[day] >= avg_price_4[day]:
            result = result*1.1
        else:
            result = result*0.9

        if price_set[day] >= avg_price_8[day]:
            result = result*1.2
        else:
            result = result*0.8

        if price_set[day] >= avg_price_17[day]:
            result = result*1.4
        else:
            result = result*0.6

        if vol_set[day] >= avg_vol_7[day]:
            result = result*1.1
        else:
            result = result*0.9

        if vol_set[day] >= avg_vol_13[day]:
            result = result*1.2
        else:
            result = result*0.8

        if vol_set[day] >= avg_vol_20[day]:
            result = result*1.4
        else:
            result = result*0.6

        result_set.append(result)
    return date_set, result_set

def run():
    date_set_sh, result_set_sh = get_adjust_value("883421", 180)
    date_set_sz, result_set_sz = get_adjust_value("SZ399001", 180)

    plt.subplot(211)
    plt.plot(result_set_sh,date_set_sh)
    plt.title("SH000001")
    plt.subplot(212)
    plt.plot(result_set_sz,date_set_sz)
    plt.title("SZ399001")
    plt.show()
    result_set_sh = np.array(result_set_sh)
    result_set_sz = np.array(result_set_sz)
    print(statistics.mean(result_set_sh), statistics.stdev(result_set_sh), statistics.median(result_set_sh))
    print(statistics.mean(result_set_sz), statistics.stdev(result_set_sz), statistics.median(result_set_sz))










if __name__ == "__main__":
    c = pyTSL.Client("yinyafei", "yinyafei@2023", "219.143.214.209", 888)
    run()


