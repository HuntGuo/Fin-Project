import pandas as pd
from interval import Interval

#_Within Trading time
df = pd.read_csv("C:/Users/www/Desktop/BackTest/M6/601989/601989 20190731.csv", index_col=0)  #7/10/2019 9:00:43 AM
df = df.rename(columns={'times': 'Dates', 'type': 'Type', 'value': 'Price', 'size': 'Size'})
del(df['condcode'])

df['Dates'] = pd.to_datetime(df['Dates'])   # convert str object to datetime
df = df[df.Dates.dt.strftime('%H:%M:%S').between('09:29:59', '15:00:01')]   # filter

df['Day'] = df['Dates'].dt.date     #   extract date
#df['Time'] = df['Dates'].dt.time    #   extract time
print(df)

# Calculate CumuIndicator
CumuIndicator = 0
bid = 0
ask = 0
mid = (bid+ask)/2
day_1 = df.at[0, 'Day']
day_2 = df.at[1, 'Day']

#   Calculate Indicator:
for Row in range(0, len(df)):
    day_2 = df.at[Row, 'Day']
    df.at[Row, 'Bid Price'] = bid
    df.at[Row, 'Ask Price'] = ask
    df.at[Row, 'Mid Price'] = mid
    if day_2 == day_1:
        #   Set value for ask and bid price
        if df.at[Row, 'Type'] == 'BID':
            if df.at[Row, 'Price'] != 0:
                bid = df.at[Row, 'Price']
            else:
                bid = ask
            df.at[Row, 'CumuIndicator'] = CumuIndicator
            day_1 = day_2
            mid = (bid + ask) / 2

        elif df.at[Row, 'Type'] == 'ASK':
            if df.at[Row, 'Price'] != 0:
                ask = df.at[Row, 'Price']
            else:
                ask = bid
            df.at[Row, 'CumuIndicator'] = CumuIndicator
            day_1 = day_2
            mid = (bid + ask) / 2

        #   Calculate CumuIndicator if it is TRADE Type
        else:   #   Calculate The Cumulative indicator
            if df.at[Row, 'Price'] in Interval(bid, mid):
                CumuIndicator = CumuIndicator - df.at[Row, 'Price'] * df.at[Row, 'Size']
                df.at[Row, 'CumuIndicator'] = CumuIndicator
                day_1 = day_2

            elif df.at[Row, 'Price'] in Interval(mid, ask):
                CumuIndicator = CumuIndicator + df.at[Row, 'Price'] * df.at[Row, 'Size']
                df.at[Row, 'CumuIndicator'] = CumuIndicator
                day_1 = day_2

            else:
                df.at[Row, 'CumuIndicator'] = CumuIndicator
                day_1 = day_2
                continue

    else:   #如果日期不相等，则直接将CumuIndicator归零
        CumuIndicator = 0
        df.at[Row, 'CumuIndicator'] = CumuIndicator
        day_1 = day_2

df = df[df['Type'].isin(['TRADE'])]

print(df)

#_Fill in seconds
del(df['Type'], df['Size'], df['Mid Price'])
df['CumuIndicator'] = df['CumuIndicator'].fillna(method='pad')  # 用前一个数据代替
df['Dates'] = pd.to_datetime(df['Dates'])

df_bysecond = df.groupby(['Dates'], as_index=False).last()  # Column: Dates is the index column

daylist = list(set(df_bysecond['Day']))
daylist.sort()

df_integrated = pd.DataFrame()
for eachday in daylist:
    df_selected = df_bysecond[df_bysecond['Day'].isin([eachday])]
    # 上午盘
    df_morning = df_selected[df_selected.Dates.dt.strftime('%H:%M:%S').between('09:29:59', '11:30:01')]
    df_morning = df_morning.set_index('Dates')
    df_morning = df_morning.resample('s').ffill()
    df_integrated = df_integrated.append(df_morning)

    # 下午盘
    df_afternoon = df_selected[df_selected.Dates.dt.strftime('%H:%M:%S').between('13:00:00', '15:00:00')]
    df_afternoon = df_afternoon.set_index('Dates')
    df_afternoon = df_afternoon.resample('s').ffill()
    df_integrated = df_integrated.append(df_afternoon)
df_integrated.reset_index(drop=False, inplace=True)

df_integrated['Time'] = df_integrated['Dates'].dt.time  # extract time
print(df_integrated)

#地址手动修改一下：
df_integrated.to_csv("C:/Users/www/Desktop/BackTest/M6/601989/CumuIndicator by second 601989.csv")

