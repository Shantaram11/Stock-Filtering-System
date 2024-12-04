import pyTSL
from datetime import datetime, timedelta
import pandas as pd


def new_day(date, n):
    date_obj = datetime.strptime(str(date), '%Y%m%d')
    new_day = date_obj + timedelta(days=n)
    new_day = int(new_day.strftime('%Y%m%d'))
    return new_day


def next_trade_day(c, stock_code, date):
    r = c.query(stock=stock_code, cycle="日线",
                begin_time=date, end_time=20240128)
    df = df = pd.DataFrame(r.value())
    date_value = pd.to_datetime(df['date'].iloc[1])
    date_value_formatted = date_value.strftime('%Y%m%d')
    return int(date_value_formatted)


def before_trade_day(c, date):
    while (1):
        date = new_day(date, -1)
        r = c.query(stock="SZ000002", cycle="日线",
                    begin_time=date, end_time=date)
        df = df = pd.DataFrame(r.value())
        if (df.empty == False):
            return date


def get_day_close(c, stock_code, date):
    print(1)
    r = c.query(stock=stock_code, cycle="日线", begin_time=date, end_time=date)
    print(2)
    df = pd.DataFrame(r.value())
    print(df)
    # print(df)
    # print(df.iloc[0].close)
    return df.iloc[0].close
    # print(df)
    return pd.DataFrame(c.query(stock=stock_code, cycle="日线", begin_time=date, end_time=date).value()).iloc[0].close


def get_range_avg_price(c, stock_code, date, start_time, end_time):
    r = c.query(stock=stock_code, cycle="1分钟线",
                begin_time=date, end_time=new_day(date, 1))
    df = pd.DataFrame(r.value())
    mask = (df['date'].dt.time >= pd.to_datetime(start_time).time()) & (
        df['date'].dt.time <= pd.to_datetime(end_time).time())
    filtered_df = df.loc[mask]
    average_price = filtered_df['price'].mean()
    return average_price


def do_stock_type(code):
    number, suffix = code.split('.')
    return f"{suffix}{number}"


def event1(c, stock_code, day):
    r = c.query(stock=stock_code, cycle="1分钟线",
                begin_time=day, end_time=new_day(day, 1))
    df = pd.DataFrame(r.value())

    df['date'] = pd.to_datetime(df['date'])
    start_time_1 = datetime.strptime('09:30:00', '%H:%M:%S').time()
    end_time_1 = datetime.strptime('10:10:00', '%H:%M:%S').time()
    start_time_2 = datetime.strptime('14:10:00', '%H:%M:%S').time()
    end_time_2 = datetime.strptime('14:40:00', '%H:%M:%S').time()

    df_time_1 = df[df['date'].dt.time.between(start_time_1, end_time_1)]
    df_time_2 = df[df['date'].dt.time.between(start_time_2, end_time_2)]

    avg_price_1 = df_time_1['price'].mean()
    avg_price_2 = df_time_2['price'].mean()

    if avg_price_1 >= avg_price_2:
        return True
    else:
        return False


def event2(c, stock_code, day):
    r = c.query(stock=stock_code, cycle="1分钟线",
                begin_time=day, end_time=new_day(day, 1))
    df = pd.DataFrame(r.value())

    df['date'] = pd.to_datetime(df['date'])
    start_time_1 = datetime.strptime('09:30:00', '%H:%M:%S').time()
    end_time_1 = datetime.strptime('10:10:00', '%H:%M:%S').time()
    start_time_2 = datetime.strptime('14:10:00', '%H:%M:%S').time()
    end_time_2 = datetime.strptime('14:40:00', '%H:%M:%S').time()

    df_time_1 = df[df['date'].dt.time.between(start_time_1, end_time_1)]
    df_time_2 = df[df['date'].dt.time.between(start_time_2, end_time_2)]

    avg_price_1 = df_time_1['price'].mean()
    avg_price_2 = df_time_2['price'].mean()

    if avg_price_1 >= avg_price_2:
        return False
    else:
        return True


def event3(c, stock_code, day):
    r = c.query(stock=stock_code, cycle="1分钟线",
                begin_time=day, end_time=new_day(day, 1))
    df = pd.DataFrame(r.value())
    start_time = '09:30'
    end_time = '10:30'

    highest_price_row = df.loc[df['high'].idxmax()]
    highest_price_time = highest_price_row['date'].time()

    start_time = pd.to_datetime(start_time).time()
    end_time = pd.to_datetime(end_time).time()
    return start_time <= highest_price_time <= end_time


def event4(c, stock_code, day):
    r = c.query(stock=stock_code, cycle="1分钟线", begin_time=day, end_time=day)
    df = pd.DataFrame(r.value())
    today_high = df["clode"].max()

    last_avg_price = get_range_avg_price(c, stock_code, day, "14:30", "14:40")

    r2 = c.query(stock=stock_code, cycle="日线", begin_time=day, end_time=day)
    df2 = pd.DataFrame(r.value())

    beofre_close_price = df2.iloc[0].yclose

    if ((today_high-beofre_close_price) > 2.5*(last_avg_price-beofre_close_price)):
        return True
    else:
        return False


def event5(c, stock_code, day):

    r = c.query(stock=stock_code, cycle="日线", begin_time=day, end_time=day)
    df = pd.DataFrame(r.value())
    beofre_close_price = df.iloc[0].yclose
    # print(beofre_close_price)
    price_2_40 = get_range_avg_price(c, stock_code, day, "14:40", "14:40")

    if (beofre_close_price*1.004 > price_2_40):
        return True
    else:
        return False


def event6(c, stock_code, day):
    open_price = get_range_avg_price(c, stock_code, day, "9:32", "9:40")
    close_price = get_range_avg_price(c, stock_code, day, "14:30", "14:40")
    eval = open_price/close_price
    if (0.99 < eval and eval < 1.01):
        return True
    else:
        return False


def event7(c, stock_code, day):
    r = c.query(stock=stock_code, cycle="1分钟线", begin_time=day, end_time=new_day(day, 1))
    df = pd.DataFrame(r.value())
   
    day_str = pd.to_datetime(str(day), format='%Y%m%d').strftime('%Y-%m-%d')

    df['date'] = pd.to_datetime(df['date'])

    begin_time_str = f'{day_str} 00:00:00'
    end_time_str = f'{day_str} 10:50:00'

    begin_time = pd.Timestamp(begin_time_str)
    end_time = pd.Timestamp(end_time_str)

    df_filtered = df[(df['date'] >= begin_time) & (df['date'] <= end_time)]

    max_price = df_filtered['high'].max()
    min_price = df_filtered['low'].min()

    drop = (max_price - min_price) / max_price * 100
    if (drop > 5):
        return True
    else:
        return False


def event8(c, stock_code, day):
    r = c.query(stock=stock_code, cycle="日线", begin_time=day, end_time=day)
    df = pd.DataFrame(r.value())
    if (df.iloc[0].open < df.iloc[0].close):
        return True
    else:
        return False


def event9(c, stock_code, day):
    r = c.query(stock=stock_code, cycle="日线", begin_time=day, end_time=day)
    df = pd.DataFrame(r.value())
    if (df.iloc[0].open < df.iloc[0].close):
        return False
    else:
        return True


def event10(c, stock_code, day):
    r = c.query(stock=stock_code, cycle="1分钟线",
                begin_time=day, end_time=new_day(day, 1))
    df = pd.DataFrame(r.value())
    price_2_40 = df.iloc[219].open
    price_9_30 = df.iloc[0].open
    if (price_2_40 > price_9_30):
        return True
    else:
        return False


def event12(c, stock_code, day):
    return False


def limitup(c, stock_code, day, time="14:30"):
    r = c.query(stock=stock_code, cycle="1分钟线",
                begin_time=day, end_time=new_day(day, 1))
    df = pd.DataFrame(r.value())
    df['date'] = pd.to_datetime(df['date'])
    df_240 = df[df['date'].dt.time == pd.to_datetime('14:40').time()]
    df_240 = df_240.iloc[0]
    if (df_240.close/df_240.sectional_yclose > 1.096):
        return True
    else:
        return False
