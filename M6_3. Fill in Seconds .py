import pandas as pd
from datetime import datetime
import numpy as np
#整合到秒级数据，读入的列都取最后一个值

df_0 = pd.read_csv('C:/Users/www/Desktop/BackTest/M6/CumuIndicator.csv',
                   usecols=['Dates', 'Day', 'Price', 'Ask Price', 'Bid Price', 'CumuIndicator'])
df_0['CumuIndicator'] = df_0['CumuIndicator'].fillna(method='pad') #用前一个数据代题
df_0['Dates'] = pd.to_datetime(df_0['Dates'])

df_bysecond = df_0.groupby(['Dates'], as_index=False).last() #Column: Dates is the index column

daylist = list(set(df_bysecond['Day']))
daylist.sort()

df_integrated = pd.DataFrame()
for eachday in daylist:
    df_selected = df_bysecond[df_bysecond['Day'].isin([eachday])]
    #上午盘
    df_morning = df_selected[df_selected.Dates.dt.strftime('%H:%M:%S').between('09:29:59', '11:30:01')]
    df_morning = df_morning.set_index('Dates')
    df_morning = df_morning.resample('s').ffill()
    df_integrated = df_integrated.append(df_morning)

    #下午盘
    df_afternoon = df_selected[df_selected.Dates.dt.strftime('%H:%M:%S').between('13:00:00', '15:00:00')]
    df_afternoon = df_afternoon.set_index('Dates')
    df_afternoon = df_afternoon.resample('s').ffill()
    df_integrated = df_integrated.append(df_afternoon)
df_integrated.reset_index(drop=False, inplace=True)

df_integrated['Time'] = df_integrated['Dates'].dt.time    #   extract time
print(df_integrated)
df_integrated.to_csv("C:/Users/www/Desktop/BackTest/M6/CumuIndicator by second.csv")


'''
#   填充数据
delta = datetime.timedelta(seconds=1)
print((df_bysecond.loc[7,'Dates'] - df_bysecond.loc[6,'Dates']).seconds)    #3
print('6 is', df_bysecond.at[6, 'Dates'])                                   #6 is 2019-02-01 09:30:11
print('7 is', df_bysecond.at[7, 'Dates'])                                   #7 is 2019-02-01 09:30:14
print('After adding', df_bysecond.at[6, 'Dates'] + delta)                   #After adding 2019-02-01 09:30:12
'''