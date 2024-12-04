import pyTSL
import pandas as pd
from datetime import datetime

c = pyTSL.Client("yinyafei", "yinyafei@2023", "219.143.214.209", 888)

#today = int(datetime.now().strftime("%Y%m%d"))
today = 20240704
code_list = pd.read_csv('code_list.csv', usecols=['ts_code'], encoding='utf-8')
i = 0 
qualified_ts_codes = []
co0unt = 0

for ts_code in code_list["ts_code"]:
    print("processing "+str(i)+"/"+str(len(code_list)))
    i+=1
    r = c.query(stock=ts_code,cycle="日线",begin_time=19890604,end_time=today)
    test = '''
    function get_value(begt);
    begin
	    return StockTotalValue(inttodate(begt));
    end;
    '''
    cap = c.call('get_value',today,code=test,stock=ts_code,time=today)
    # 市值大于140亿 跳过
    if (int(cap.value()) > 1400000):
        continue
    df = pd.DataFrame(r.value())
    # print(df)
    if (len(df) < 180):
        continue
    
    last_180 = df.tail(180).reset_index(drop=True)
    is_highest_price = last_180.loc[179, 'price'] == df['price'][-180:].max()
    if(is_highest_price):
        ratio = (df['price'].iloc[-1]-df['price'].iloc[-9])/df["price"].iloc[-9]
        if(ratio > 0.5):
            qualified_ts_codes.append({"ts_code":ts_code,"p": float(df['price'].iloc[-1]),"n":0})
qualified_df = pd.DataFrame(qualified_ts_codes)
qualified_df.to_csv("qualified_ts_code/qualified_ts_codes_"+str(today)+".csv", index=False, encoding='utf-8')


                                                