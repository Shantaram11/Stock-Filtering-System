import pyTSL
import pandas as pd
from datetime import datetime
import event as ev

c = pyTSL.Client("yinyafei", "yinyafei@2023", "219.143.214.209", 888)
b_pool = pd.read_csv("b_pool.csv")
day = int(input("输入当天的日期 例20240315: "))

back_to_a = []
indexes_to_drop = []
c_pool = []
for index in range(0,len(b_pool)):
    print("------------------------------------")
    print("Process Index "+str(index))
    stock_code = b_pool.iloc[index].ts_code
    # 涨停 回到A池
    if(ev.limitup(c,stock_code,day)):
        back_to_a.append(stock_code)
        indexes_to_drop.append(index)
        print("Limit Up! Back to A!")
        continue
    e2 = ev.event2(c,stock_code,day)
    e3 = ev.event3(c,stock_code,day)
    e5 = ev.event5(c,stock_code,day)
    e6 = ev.event6(c,stock_code,day)
    e7 = ev.event7(c,stock_code,day)
    e8 = ev.event8(c,stock_code,day)
    e10 = ev.event10(c,stock_code,day)
    e12 = ev.event12(c,stock_code,day)
    print("Event 2:", e2)
    print("Event 3:", e3)
    print("Event 5:", e5)
    print("Event 6:", e6)
    print("Event 7:", e7)
    print("Event 8:", e8)
    print("Event 10:", e10)
    print("Event 12:", e12)
    buy_price = ev.get_range_avg_price(c, stock_code, day, "14:40", "14:40")
    # print(buy_price)
    if(e2 and e8 and e10 and(not e3)and (not e5) and (not e6) and(not e12)):
        c_pool.append({"ts_code":stock_code,"buy_price":buy_price})
        indexes_to_drop.append(index)
        continue
    else:
        print("reminder 1")
    if(e2 and e7 and e8 and (not e3) and(not e10) and (not e12)):
        c_pool.append({"ts_code":stock_code,"buy_price":buy_price})
        indexes_to_drop.append(index)
        continue
    else:
        print("reminder 2")

b_pool = b_pool.drop(indexes_to_drop)
b_pool.to_csv("res_b/b_pool_new_"+str(day)+".csv",index=False, encoding='utf-8')
back_to_a = pd.DataFrame(back_to_a, columns=['ts_code'])
back_to_a.to_csv("res_b/back_to_a_new_"+str(day)+".csv",index=False, encoding='utf-8')
c_pool = pd.DataFrame(c_pool)
c_pool.to_csv("res_b/c_pool_new_"+str(day)+".csv",index=False, encoding='utf-8')
