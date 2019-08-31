import pandas as pd
from interval import Interval
#   计算CumuIndicator: 时期不同时，CumuIndicator立刻清零，每次出现新的Bid, Ask时，Bid(Ask) price 立刻更换

df = pd.read_csv("C:/Users/www/Desktop/BackTest/M6/Within Trading time.csv")    #   修改地址
del(df['Unnamed: 0'])
#print(df.isnull().any())   #   无缺失值

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
df.to_csv("C:/Users/www/Desktop/BackTest/M6/CumuIndicator.csv") #修改地址