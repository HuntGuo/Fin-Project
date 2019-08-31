import pandas as pd
#筛选交易时间段的数据

df = pd.read_csv("C:/Users/www/Desktop/BackTest/M6/600519 20190731.csv", index_col=0)  #7/10/2019 9:00:43 AM
df = df.rename(columns={'times': 'Dates', 'type': 'Type', 'value': 'Price', 'size': 'Size'})
del(df['condcode'])

df['Dates'] = pd.to_datetime(df['Dates'])   # convert str object to datetime
df = df[df.Dates.dt.strftime('%H:%M:%S').between('09:29:59', '15:00:01')]   # filter

df['Day'] = df['Dates'].dt.date     #   extract date
#df['Time'] = df['Dates'].dt.time    #   extract time

print(df)

df.to_csv("C:/Users/www/Desktop/BackTest/M6/Within Trading time.csv")

#df = df[df['Dates'].dt.hour.isin(np.arange(9, 15))] #  提取特定小时内