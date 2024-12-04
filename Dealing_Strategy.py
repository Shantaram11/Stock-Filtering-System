
import pyTSL
import pandas as pd
from datetime import datetime, timedelta
import os

from chinese_calendar import is_workday

import event as ev
import N_CallBack


def n_trade_day_before(date, n):
    date = datetime.strptime(date, '%Y%m%d').date()
    diff_days = 0
    while diff_days < n:
        date = (date + timedelta(days=-1))
        if is_workday(date):
            diff_days += 1
    return date.strftime('%Y%m%d')

def getHistoryData(code):
    r = c.query(stock=code, cycle="5分钟线", begin_time=int(today), end_time=int(tomorrow), fields=["code", "date", "price"])
    df = pd.DataFrame(r.value())
    return df


def deal_with_C_Pool():
    c_pool = pd.read_csv("res_b\\c_pool_new_" + today + ".csv")
    c_pool_larger = list()
    c_pool_smaller = list()
    datetime_formal_35 = datetime.today().strftime('%Y-%m-%d')
    datetime_formal_35 += " 14:35:00"
    time_35min = pd.to_datetime(datetime_formal_35)
    datetime_formal_40 = datetime.today().strftime('%Y-%m-%d')
    datetime_formal_40 += " 14:40:00"
    time_40min = pd.to_datetime(datetime_formal_40)

    for i in range(len(c_pool)):
        code = c_pool.iloc[i, 0]
        recommend_price = c_pool.iloc[i, 1]
        history_date = getHistoryData(code)
        price_35min = float(history_date[history_date["date"] == time_35min]["price"])
        price_40min = float(history_date[history_date["date"] == time_40min]["price"])
        price_open = float(history_date["price"].iloc[0])
        if price_35min/price_open >= 1.022 and price_40min/price_open >= 1.022:
            c_pool_larger.append([code, recommend_price, price_40min/price_open])
        else:
            c_pool_smaller.append([code, recommend_price, price_40min/price_open])
    return c_pool_larger, c_pool_smaller

def check_c_pool_buy(code, increasing):
    r = c.query(stock=code, cycle="1分钟线", begin_time=int(today), end_time=int(tomorrow), fields=["price", "sectional_yclose"])
    df = pd.DataFrame(r.value())
    current_price = float(df["price"].iloc[-1])
    yesterday_close_price = float(df["sectional_yclose"].iloc[-1])
    boundary_price = yesterday_close_price*(1+increasing+0.007)
    if current_price > boundary_price:
        return 0, 0, False
    return current_price, yesterday_close_price, True


def c_pool_buy():
    c_pool_larger, c_pool_smaller = deal_with_C_Pool()
    if os.path.exists("Holding_List.csv"):
        existed_df = pd.read_csv("Holding_List.csv")
    else:
        existed_df = pd.DataFrame(columns=["code", "date", "price", "increasing", "volume", "holding_days", "yesterday_close_price", "Pool", "checking_status"])
    result = list()
    for stock in c_pool_larger:
        code = stock[0]
        recommend_price = stock[1]
        increasing = stock[2]
        current_price, yesterday_close_price, flag = check_c_pool_buy(code, increasing)
        volume, rank = get_c_pool_buy_Volume(code, current_price, increasing)
        if not flag:
            continue
        else:
            volume = 100*int(volume*2/300)
            c_pool_buy_stock(code, current_price, volume)
            result.append([code, today, current_price, increasing, volume, 0, yesterday_close_price, "C", 0])
    for stock in c_pool_smaller:
        code = stock[0]
        recommend_price = stock[1]
        increasing = stock[2]
        current_price, yesterday_close_price, flag = check_c_pool_buy(code, increasing)
        volume, rank = get_c_pool_buy_Volume(code, current_price, increasing)
        if not flag:
            continue
        else:
            c_pool_buy_stock(code, current_price, volume)
            result.append([code, today, current_price, increasing, volume, 0, yesterday_close_price, "C", 0])
    result_df = pd.DataFrame(result, columns=["code", "date", "price", "increasing", "volume", "holding_days", "yesterday_close_price", "Pool", "checking_status"])
    result_df = pd.concat([existed_df, result_df], ignore_index=True)
    result_df.to_csv("Holding_List.csv", index=False)

def getTurnOver(code):
    test = '''
        function get_value(endt);
        begin
        	endt:=inttodate(endt);
        	return StockHsl4(endt);	
        end;
        '''
    r = c.call('get_value', int(today), code=test, stock=code)
    return r.value()
def get_c_pool_buy_Volume(code, price, increasing):
    if price == 0:
        return 0,0
    turn_over = getTurnOver(code) / 100
    r = c.query(stock=code, cycle="日线", begin_time=int(today), end_time=int(today), fields=["amount"])
    amount = r.value()[0]["amount"]
    transaction_amount = amount / turn_over
    rank = 0
    base_asset = 200000
    if transaction_amount >= 7000000000:
        base_asset *= 2.5
        rank += 50
    elif 5000000000 <= transaction_amount < 7000000000:
        base_asset *= 2.4
        rank += 40
    elif 3000000000 <= transaction_amount < 5000000000:
        base_asset *= 2.0
        rank += 30
    elif 1800000000 <= transaction_amount < 3000000000:
        base_asset *= 1.8
        rank += 20
    elif 1000000000 <= transaction_amount < 1800000000:
        base_asset *= 0.6
        rank += 10
    else:
        base_asset *= 0.1

    if increasing >= 1.05:
        base_asset *= 2
        base_asset += 200000
        rank += 4
    elif 3 <= increasing < 5:
        base_asset *= 1.6
        base_asset += 200000
        rank += 3
    elif 2.2 <= increasing < 3:
        base_asset *= 1.2
        base_asset += 200000
        rank += 2
    elif 0 <= increasing < 2.2:
        base_asset *= 0.5
        rank += 1
    else:
        return 0, 0
    volume = int((base_asset/price)/100)*100
    return volume, rank



def update_holding_days():
    df = pd.read_csv("Holding_List.csv")
    for i in range(len(df)):
        df.at[i, "holding_days"] += 1
        df.at[i, "checking_status"] = 0
    df.to_csv("Holding_List.csv", index=False)

def sell_situdation_1():
    holding_list = pd.read_csv("Holding_List.csv")
    drop_lines = list()
    for i in range(len(holding_list)):
        code = str(holding_list.iloc[i]["code"])
        base_price = float(holding_list.iloc[i]["price"])
        holding_days = int(holding_list.iloc[i]["holding_days"])
        volume = int(holding_list.iloc[i]["volume"])
        r = c.query(stock=code, cycle="日线", begin_time=int(today), end_time=int(today), fields=["price", "high"])
        df = pd.DataFrame(r.value())
        current_price = float(df["price"].iloc[-1])
        high_price = float(df["high"].iloc[-1])
        if holding_days == 10:
            sell_stock(code, current_price, volume)
            drop_lines.append(i)
            continue

        if holding_days != 1:
            continue

        if current_price / base_price < 1.01:
            holding_list.at[i, "checking_status"] = 1
            continue
        else:
            if current_price <= base_price + 0.65*(high_price - base_price):
                sell_stock(code, current_price, volume)
                drop_lines.append(i)
            else:
                holding_list.at[i, "checking_status"] = 2
    holding_list.drop(drop_lines, axis=0, inplace=True)
    holding_list.to_csv("Holding_List.csv", index=False)

def sell_situdation_2():
    holding_list = pd.read_csv("Holding_List.csv")
    drop_lines = list()
    for i in range(len(holding_list)):
        code = str(holding_list.iloc[i]["code"])
        holding_days = int(holding_list.iloc[i]["holding_days"])
        yesterday_close_price = float(holding_list.iloc[i]["yesterday_close_price"])
        if holding_days <= 1:
            continue
        volume = int(holding_list.iloc[i]["volume"])
        r = c.query(stock=code, cycle="1分钟线", begin_time=int(today), end_time=int(tomorrow), fields=["price"])
        df = pd.DataFrame(r.value())
        current_price = float(df["price"].iloc[-1])
        print(current_price, 0.97*yesterday_close_price)
        if current_price <= 0.97*yesterday_close_price:
            sell_stock(code, current_price, volume)
            drop_lines.append(i)
    holding_list.drop(drop_lines, axis=0, inplace=True)
    holding_list.to_csv("Holding_List.csv", index=False)

def sell_situation_3():
    holding_list = pd.read_csv("Holding_List.csv")
    drop_lines = list()
    for i in range(len(holding_list)):
        if holding_list.iloc[i]["checking_status"] == 0:
            continue
        code = holding_list.iloc[i]["code"]
        base_price = holding_list.iloc[i]["price"]
        volume = holding_list.iloc[i]["volume"]
        if holding_list.iloc[i]["checking_status"] == 1:
            r = c.query(stock=code, cycle="1分钟线", begin_time=int(today), end_time=int(tomorrow), fields=["price"])
            df = pd.DataFrame(r.value())
            current_price = float(df.iloc[-1]["price"])
            if current_price >= base_price + 0.05:
                sell_stock(code, current_price, volume)
                drop_lines.append(i)
        elif holding_list.iloc[i]["checking_status"] == 2:
            r = c.query(stock=code, cycle="1分钟线", begin_time=int(today), end_time=int(tomorrow), fields=["price", "sectional_high"])
            df = pd.DataFrame(r.value())
            current_price = float(df.iloc[-1]["price"])
            high_40min_ago = float(df.iloc[-40]["sectional_high"])
            high_now = float(df.iloc[0]["sectional_high"])
            if high_now == high_40min_ago:
                sell_volume = 100*int(volume/200)
                sell_stock(code, current_price, sell_volume)
                if sell_volume == volume:
                    drop_lines.append(i)
                else:
                    holding_list.at[i, "checking_status"] = 0
                    holding_list.at[i, "volume"] = volume - sell_volume
    holding_list.drop(drop_lines, axis=0, inplace=True)
    holding_list.to_csv("Holding_List.csv", index=False)

def supply_buy():
    holding_list = pd.read_csv("Holding_List.csv")
    length = len(holding_list)
    add_list = list()
    for i in range(length):
        code = str(holding_list.iloc[i]["code"])
        holding_days = int(holding_list.iloc[i]["holding_days"])
        increasing = float(holding_list.iloc[i]["increasing"])
        if holding_days != 1 or increasing < 1.022:
            continue
        volume = int(holding_list.iloc[i]["volume"])
        r = c.query(stock=code, cycle="1分钟线", begin_time=int(today), end_time=int(tomorrow), fields=["price", "sectional_yclose"])
        df = pd.DataFrame(r.value())
        if float(df["price"].iloc[-1]) >= float(df["price"].iloc[-10]):
            continue
        current_price = float(df["price"].iloc[-1])
        yesterday_close_price = float(df["sectional_yclose"].iloc[0])
        volume = volume/2
        volume = max(100, 100*int(volume/100))
        c_pool_buy_stock(code, current_price, volume)
        add_list.append([code, today, current_price, -100, volume, 0, yesterday_close_price, "C"])
    add_list_df = pd.DataFrame(add_list, columns=["code", "date", "price", "increasing", "volume", "holding_days", "yesterday_close_price", "Pool"])
    holding_list = pd.concat([holding_list, add_list_df], ignore_index=True)
    holding_list.to_csv("Holding_List.csv", index=False)



def generate_c_pool():
    N_CallBack.check_folders()
    N_CallBack.compute_deal_b_pool(today)
    N_CallBack.compute_deal_a_pool(today)

def generate_limitup():
    N_CallBack.compute_limitup(today)


##实际购买股票的方法
def c_pool_buy_stock(code, price, volume):
    return True

##实际出售股票的方法
def sell_stock(code, price, volume):
    return True


def run_14_40():
    #生成C Pool
    generate_c_pool()
    #对C Pool进行分类，核验并购买
    c_pool_buy()

def run_14_55():
    #对持股大于2天的股票判断是否清仓
    sell_situdation_2()
    #对昨天刚买的股票判断是否补仓
    supply_buy()

def run_15_00():
    #更新持股天数
    update_holding_days()
    #生成qualified_ts_code文件供明日组成A Pool（需30-40分钟）
    generate_limitup()

def run_10_30():
    #对昨天刚购买的股票判断是否清仓
    sell_situdation_1()
    #对于昨天买的且涨幅不超过1%的股票，一直检测最新价格并判断是否卖出
    while(1):
        sell_situation_3()

if __name__ == "__main__":
    c = pyTSL.Client("yinyafei", "yinyafei@2023", "219.143.214.209", 888)
    today = datetime.today().strftime('%Y%m%d')
    tomorrow = ev.new_day(today, 1)
    yesterday = n_trade_day_before(today, 1)



