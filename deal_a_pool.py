import pyTSL
import pandas as pd
from datetime import datetime
import event as ev

c = pyTSL.Client("yinyafei", "yinyafei@2023", "219.143.214.209", 888)
a_pool = pd.read_csv("a_pool.csv")
day = int(input("输入当天的日期 例20240315: "))
#day = int(datetime.now().strftime("%Y%m%d"))
#day =int(20240424)
b_pool = []
indexes_to_drop = []
print("Today is "+str(day))
for index in range(0,len(a_pool)):
    print("------------------------------------")
    print("Process Index "+str(index))
    stock_code = a_pool.iloc[index].ts_code
    p_price = a_pool.iloc[index].p
 #   print(type(p_price))
    print("The Stock Code Is "+str(stock_code))
    today_price = ev.get_day_close(c,stock_code,day)
    print("Today's Price Is "+str(today_price))
    if(ev.limitup(c,stock_code,day)):
        a_pool.at[index,"n"] = 0
        a_pool.at[index,"p"] = today_price
        print("Limit Up! Go Next!")
        continue
    # 如果创新高
    if(today_price > p_price):
        a_pool.at[index,"n"] = 0
        a_pool.at[index,"p"] = today_price
        print("New High Pirce! Go Next!")
        continue
    # 出现2 8 10 且尾盘上涨不超过1%
    if(ev.event2(c,stock_code,day) and ev.event8(c,stock_code,day) and ev.event10(c,stock_code,day)):
        # 尾盘上涨不超过1%
        r = c.query(stock=stock_code,cycle="日线",begin_time=day,end_time=day)
        df = pd.DataFrame(r.value())
        if(df.iloc[0].close/df.iloc[0].yclose<1.01):
            print("Event 2 9 10 Skip!")
            continue
    # 未出现9
    if(ev.event9(c,stock_code,day) == False):
        print("No Event 9 Skip!")
        continue
    print("N + 1")
    a_pool.at[index,"n"] += 1
    if(a_pool.at[index,"n"]>=2):
        print("N >= 2: Move to B Pool")
       # a_pool.drop(index,inplace=True)
        indexes_to_drop.append(index)
        b_pool.append(stock_code)
a_pool = a_pool.drop(indexes_to_drop)
a_pool.to_csv("res_a/a_pool_new_"+str(day)+".csv",index=False, encoding='utf-8')
b_pool = pd.DataFrame(b_pool, columns=['ts_code'])
b_pool.to_csv("res_a/b_pool_new_"+str(day)+".csv",index=False, encoding='utf-8')