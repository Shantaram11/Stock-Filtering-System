import sys
import tkinter

import pyTSL
import pandas as pd
from datetime import datetime
import event as ev
import os
import datetime
from chinese_calendar import is_workday
import tkinter as tk
from tkinter import messagebox

def n_trade_day_before(date, n):
    date = datetime.datetime.strptime(date, '%Y%m%d').date()
    diff_days = 0
    while diff_days < n:
        date = (date + datetime.timedelta(days=-1))
        if is_workday(date):
            diff_days += 1
    return date.strftime('%Y%m%d')

def check_folders():
    if not os.path.exists("res_a"):
        os.mkdir("res_a")
        print("Folder \'res_a\' has been created")
    if not os.path.exists("res_b"):
        os.mkdir("res_b")
        print("Folder \'res_b\' has been created")
    if not os.path.exists("qualified_ts_code"):
        os.mkdir("qualified_ts_code")
        print("Folder \'qualified_ts_code\' has been created")



def check_file(file):
    # file1 = "res_a\\a_pool_new_" + yesterday + ".csv"
    # file2 = "qualified_ts_code\\qualified_ts_codes_" + yesterday + ".csv"
    # file3 = "res_b\\back_to_a_new_" + yesterday + ".csv"
    # file4 = "res_a\\b_pool_new_" + yesterday + ".csv"
    # file5 = "res_b\\b_pool_new_" + yesterday + ".csv"
    if not os.path.exists(file):
        return False
    else:
        return True
    
def merge_csv_files(files, filename):
    data = pd.read_csv(files[0])
    if len(files) == 1:
        data.to_csv(filename, index=False)
    else:
        for i in range(1, len(files)):
            data_prime = pd.read_csv(files[i])
            data = pd.merge(data, data_prime, how="outer")
        data.to_csv(filename, index=False)
    
def compute_deal_a_pool(day):
    yesterday = n_trade_day_before(day, 1)
    file1 = "res_a\\a_pool_new_" + yesterday + ".csv"
    file2 = "qualified_ts_code\\qualified_ts_codes_" + yesterday + ".csv"
    file3 = "res_b\\back_to_a_new_" + yesterday + ".csv"
    files = list()
    files.append(file2)
    if check_file(file1):
        files.append(file1)
    else:
        print(file1 + " missed")
    if check_file(file3):
        files.append(file3)
    else:
        print(file3 + " missed")
    if len(files) == 3:
        print("No file missed for A Pool")
    merge_csv_files(files, "a_pool.csv")
    a_pool = pd.read_csv("a_pool.csv")
    b_pool = []
    indexes_to_drop = []
    day = int(day)
    # print("Today is " + str(day))
    for index in range(0, len(a_pool)):
        # print("------------------------------------")
        # print("Process Index " + str(index))
        stock_code = a_pool.iloc[index].ts_code
        p_price = a_pool.iloc[index].p
        # print("The Stock Code Is " + str(stock_code))
        # print(stock_code)
        # print(day)
        today_price = ev.get_day_close(c, stock_code, day)
        # print("Today's Price Is " + str(today_price))
        if (ev.limitup(c, stock_code, day)):
            a_pool.at[index, "n"] = 0
            a_pool.at[index, "p"] = today_price
            # print("Limit Up! Go Next!")
            continue
        if (today_price > p_price):
            a_pool.at[index, "n"] = 0
            a_pool.at[index, "p"] = today_price
            # print("New High Pirce! Go Next!")
            continue
        # 出现2 8 10 且尾盘上涨不超过1%
        if (ev.event2(c, stock_code, day) and ev.event8(c, stock_code, day) and ev.event10(c, stock_code, day)):
            # 尾盘上涨不超过1%
            r = c.query(stock=stock_code, cycle="日线", begin_time=day, end_time=day)
            df = pd.DataFrame(r.value())
            if (df.iloc[0].close/df.iloc[0].yclose < 1.01):
                # print("Event 2 9 10 Skip!")
                continue
        # 未出现9
        if (ev.event9(c, stock_code, day) == False):
            # print("No Event 9 Skip!")
            continue
        # print("N + 1")
        a_pool.at[index, "n"] += 1
        if (a_pool.at[index, "n"] >= 2):
            # print("N >= 2: Move to B Pool")
            # a_pool.drop(index,inplace=True)
            indexes_to_drop.append(index)
            b_pool.append(stock_code)
    a_pool = a_pool.drop(indexes_to_drop)
    a_pool.to_csv("res_a/a_pool_new_" + str(day) + ".csv", index=False, encoding='utf-8')
    print("file res_a/a_pool_new_" + str(day) + ".csv has been created")
    b_pool = pd.DataFrame(b_pool, columns=['ts_code'])
    b_pool.to_csv("res_a/b_pool_new_" + str(day) + ".csv", index=False, encoding='utf-8')
    print("file res_a/b_pool_new_" + str(day) + ".csv has been created")


def compute_limitup(today):
    today = int(today)
    code_list = pd.read_csv('code_list.csv', usecols=['ts_code'], encoding='utf-8')
    i = 0
    empty = list()
    empty_count = 0
    qualified_ts_codes = []
    for ts_code in code_list["ts_code"]:
        # ui.progressBar.setValue(int(100*i/len(code_list)))
        print("processing " + str(i) + "/" + str(len(code_list)))
        i += 1
        r = c.query(stock=ts_code, cycle="日线", begin_time=19890604, end_time=today)
        test = '''
        function get_value(begt);
        begin
            return StockTotalValue(inttodate(begt));
        end;
        '''
        cap = c.call('get_value', today, code=test, stock=ts_code, time=today)
        # 市值大于140亿 跳过
        try:
            if (int(cap.value()) > 1400000):
                continue
            df = pd.DataFrame(r.value())
            # print(df)
            if (len(df) < 180):
                continue
        except:
            empty.append(ts_code)
            empty_count = empty_count + 1
            print(empty_count)
            continue



        last_180 = df.tail(180).reset_index(drop=True)
        is_highest_price = last_180.loc[179, 'price'] == df['price'][-120:].max()
        if (is_highest_price):
            ratio = (df['price'].iloc[-1] - df['price'].iloc[-9])/df["price"].iloc[-9]
            if (ratio > 0.5):
                qualified_ts_codes.append({"ts_code": ts_code, "p": float(df['price'].iloc[-1]), "n": 0})
    qualified_df = pd.DataFrame(qualified_ts_codes, columns=["ts_code", "p", "n"])
    qualified_df.to_csv("qualified_ts_code/qualified_ts_codes_" + str(today) + ".csv", index=False, encoding='utf-8')
    print("file qualified_ts_code/qualified_ts_codes_" + str(today) + ".csv has been created")
    print(empty)


def compute_deal_b_pool(day):
    yesterday = n_trade_day_before(day, 1)
    file4 = "res_a\\b_pool_new_" + yesterday + ".csv"
    file5 = "res_b\\b_pool_new_" + yesterday + ".csv"
    if not check_file(file5):
        merge_csv_files([file4], "b_pool.csv")
        f = "res_b\\merged_b_pool_" + str(today) + ".csv"
        merge_csv_files([file4], f)
        print("file 5 missed")
    else:
        merge_csv_files([file4, file5], "b_pool.csv")
        f = "res_b\\merged_b_pool_" + str(today) + ".csv"
        merge_csv_files([file4, file5], f)
        print("no file missed for B pool")
    b_pool = pd.read_csv("b_pool.csv")
    day = int(day)
    back_to_a = []
    indexes_to_drop = []
    c_pool = []
    for index in range(0, len(b_pool)):
        # print("------------------------------------")
        # print("Process Index " + str(index))
        stock_code = b_pool.iloc[index].ts_code
        # 涨停 回到A池
        if (ev.limitup(c, stock_code, day)):
            back_to_a.append(stock_code)
            indexes_to_drop.append(index)
            # print("Limit Up! Back to A!")
            continue
        e2 = ev.event2(c, stock_code, day)
        e3 = ev.event3(c, stock_code, day)
        e5 = ev.event5(c, stock_code, day)
        e6 = ev.event6(c, stock_code, day)
        e7 = ev.event7(c, stock_code, day)
        e8 = ev.event8(c, stock_code, day)
        e10 = ev.event10(c, stock_code, day)
        e12 = ev.event12(c, stock_code, day)
        # print("Event 2:", e2)
        # print("Event 3:", e3)
        # print("Event 5:", e5)
        # print("Event 6:", e6)
        # print("Event 7:", e7)
        # print("Event 8:", e8)
        # print("Event 10:", e10)
        # print("Event 12:", e12)
        buy_price = ev.get_range_avg_price(c, stock_code, day, "14:40", "14:40")
        # print(buy_price)
        if (e2 and e8 and e10 and (not e3) and (not e5) and (not e6) and (not e12)):
            c_pool.append({"ts_code": stock_code, "buy_price": buy_price})
            indexes_to_drop.append(index)
            continue
        if (e2 and e7 and e8 and (not e3) and (not e10) and (not e12)):
            c_pool.append({"ts_code": stock_code, "buy_price": buy_price})
            indexes_to_drop.append(index)
            continue

    b_pool = b_pool.drop(indexes_to_drop)
    b_pool.to_csv("res_b/b_pool_new_" + str(day) + ".csv", index=False, encoding='utf-8')
    print("file res_b/b_pool_new_" + str(day) + ".csv has been created")
    back_to_a = pd.DataFrame(back_to_a, columns=['ts_code'])
    back_to_a.to_csv("res_b/back_to_a_new_" + str(day) + ".csv", index=False, encoding='utf-8')
    print("file res_b/back_to_a_new_" + str(day) + ".csv has been created")
    c_pool = pd.DataFrame(c_pool)
    c_pool.to_csv("res_b/c_pool_new_" + str(day) + ".csv", index=False, encoding='utf-8')
    print("res_b/c_pool_new_" + str(day) + ".csv has been created")


def throw_out_message_box(title, content):
    return messagebox.askyesno(title, content)

def run():
    if not is_workday(datetime.datetime.today().date()):
        throw_out_message_box("Error!", "Today is not trading day")
        return 0
    else:
        check_folders()
    file2 = "qualified_ts_code\\qualified_ts_codes_" + yesterday + ".csv"
    if check_file(file2):
        file1 = "res_a\\a_pool_new_" + today + ".csv"
        file2 = "qualified_ts_code\\qualified_ts_codes_" + today + ".csv"
        file3 = "res_b\\back_to_a_new_" + today + ".csv"
        file4 = "res_a\\b_pool_new_" + today + ".csv"
        file5 = "res_b\\b_pool_new_" + today + ".csv"
        if check_file(file1) and check_file(file2) and check_file(file3) and check_file(file4) and check_file(file5):
            print("All files for today have been created before")
            return 0
        compute_deal_b_pool(today)
        throw_out_message_box("Reminder!", "File " + "res_b/c_pool_new_" + str(today) + ".csv" + " has benn created.\nAfter click Yes, the procedure will keep running for about 30 min\nwhich will create files for tomorrow")
        compute_deal_a_pool(today)
        compute_limitup(today)
    else:
        reply = throw_out_message_box("Reminder", "some pre-requisite files are missed\nclick Yes to create all of them\nwhich will cost aprox. 30min")
        if reply == messagebox.NO:
            return 0
        compute_limitup(last_2_day)
        compute_deal_a_pool(yesterday)
        compute_deal_b_pool(today)
        throw_out_message_box("Reminder!", "File " + "res_b/c_pool_new_" + str(today) + ".csv" + " has benn created.\nAfter click Yes, the procedure will keep running for about 1 h\nwhich will create files for tomorrow")
        compute_limitup(yesterday)
        compute_deal_a_pool(today)
        compute_limitup(today)
        throw_out_message_box("Reminder!", "finished! click Yes to stop")

def check_missed_files():
    file1 = "res_a\\a_pool_new_" + yesterday + ".csv"
    file2 = "qualified_ts_code\\qualified_ts_codes_" + yesterday + ".csv"
    file3 = "res_b\\back_to_a_new_" + yesterday + ".csv"
    file4 = "res_a\\b_pool_new_" + yesterday + ".csv"
    file5 = "res_b\\b_pool_new_" + yesterday + ".csv"
    missed_files = list()
    if not check_file(file1):
        missed_files.append(str(file1 + "\n"))
    if not check_file(file2):
        missed_files.append(str(file2 + "\n"))
    if not check_file(file3):
        missed_files.append(str(file3 + "\n"))
    if not check_file(file4):
        missed_files.append(str(file4 + "\n"))
    if not check_file(file5):
        missed_files.append(str(file5 + "\n"))
    if len(missed_files) == 0:
        print("All files are prepared for today")
    else:
        print("Files listed below are missed:")
        print(missed_files)



if __name__ == "__main__":
    c = pyTSL.Client("yinyafei", "yinyafei@2023", "219.143.214.209", 888)
    root = tkinter.Tk()
    root.withdraw()
    root.attributes("-topmost", True)
    # today = datetime.datetime.today().strftime('%Y%m%d')
    today = "20241125"
    yesterday = n_trade_day_before(today, 1)
    last_2_day = n_trade_day_before(today, 2)
    mode = int(input("type\n1 for check files' existence\n2 for generating C pool for today"))
    if mode == 1:
        check_missed_files()
    elif mode == 2:
        run()
    else: print("Error")



