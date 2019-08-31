import pandas as pd
import numpy as np
import math

#   Start with reading csv file:
df_0 = pd.read_csv('C:/Users/www/Desktop/BackTest/M6/CumuIndicator by second.csv')

#   Define EMA function
def EMA(values, window):
    weights = np.exp(np.linspace(-1., 0., window))
    weights /= weights.sum()
    a = np.convolve(values, weights, mode='full')[:len(values)]
    a[:window] = a[window]
    return a

##  Create a DF containing all Close_day
daylist = []
for each_day in df_0['Dates']:
    each_day = each_day.split()
    daylist.append(each_day[0])
daylist = list(set(daylist))
daylist = sorted(daylist)
ratelist = [0]*len(daylist)
series1 = pd.Series(daylist)
series2 = pd.Series(ratelist)
series1.rename("Date", inplace=True)
series2.rename("Rate", inplace=True)
AllDate = pd.DataFrame([series1, series2])
AllDate = AllDate.T
initial_data = AllDate #    all objects (str)

def sharpe(N, K):
    #   Create an empty DataFrame to record return rate
    ReturnRate = pd.DataFrame(
        columns=['Open_DateTime', 'Close_DateTime', 'Open Price', 'Close Price',
                 'Return Rate', 'Date'])

    Transaction_order = 0

    #   To create the Return Rate
    for day in daylist:
        df = df_0[df_0['Day'] == day]   #   筛选出相应的天
        #df.reset_index(drop=True, inplace=True)     #   重新设置index
        df.index = np.arange(1, len(df) + 1)

        # Create the columns of MA, STD, and UpperBand
        df['EMA'] = EMA(df['CumuIndicator'], N)
        df['STD'] = df['CumuIndicator'].rolling(N).std()
        df['UpperBand'] = df.apply(lambda x: x['EMA'] + K * x['STD'], axis=1)

        # Index for opening and closing long position
        OpenLongPosition = df[df['CumuIndicator'] > df['UpperBand']].index.tolist()
        CloseLongPosition = df[df['CumuIndicator'] <= df['EMA']].index.tolist()
        OpenLongPosition = np.array(OpenLongPosition)
        CloseLongPosition = np.array(CloseLongPosition)


        # 必须在14:56:58前平仓，因此开仓和平仓点也必须在这个时间点之前
        try:
            LastClosePoint = df['Time'].tolist().index('14:56:58')   #最后平仓点Index
        except ValueError:
            print('Skip Day: ', day)
            continue
        OpenLongPosition = OpenLongPosition[OpenLongPosition < LastClosePoint]
        CloseLongPosition = CloseLongPosition[CloseLongPosition < LastClosePoint]

        buypoint = 0
        closepoint = 0
        for RowIndex in OpenLongPosition:
            #   Only open Long Position
            if RowIndex > closepoint: # Open Position
                if len(CloseLongPosition[np.where(CloseLongPosition > RowIndex + 1)]):
                    Transaction_order += 1  # Order plus 1
                    buypoint = RowIndex + 1
                    closepoint = CloseLongPosition[np.where(CloseLongPosition > buypoint)][0] + 1

                    P_open = df.at[buypoint, 'Price']  # 买入价/开仓价
                    P_close = df.at[closepoint, 'Price'] #  卖出价/平仓价

                    ReturnRate.at[Transaction_order, 'Open_DateTime'] = df.at[buypoint, 'Dates']  # 开仓日期
                    ReturnRate.at[Transaction_order, 'Close_DateTime'] = df.at[closepoint, 'Dates']  # 平仓日期
                    ReturnRate.at[Transaction_order, 'Open Price'] = P_open
                    ReturnRate.at[Transaction_order, 'Close Price'] = P_close

                    spread_open = df.at[buypoint, 'Ask Price'] - df.at[buypoint, 'Bid Price'] # 开仓spread
                    spread_close = df.at[closepoint, 'Ask Price'] - df.at[closepoint, 'Bid Price'] #    平仓spread

                    ReturnRate.at[Transaction_order, 'Spread Open'] = spread_open
                    ReturnRate.at[Transaction_order, 'Spread Close'] = spread_close
                    ReturnRate.at[Transaction_order, 'Return Rate'] = (P_close - P_open -0.001*P_close -
                                                                       (spread_close + spread_open))/P_open
                    ReturnRate.at[Transaction_order, 'Date'] = day

                else:
                    #本段用来处理大于buypoint的平仓信号点list为空的情况-用最后平仓点来计算
                    Transaction_order += 1
                    buypoint = RowIndex + 1
                    closepoint = LastClosePoint + 1

                    P_open = df.at[buypoint, 'Price']  # 买入价/开仓价
                    P_close = df.at[closepoint, 'Price'] #  卖出价/平仓价

                    ReturnRate.at[Transaction_order, 'Open_DateTime'] = df.at[buypoint, 'Dates']  # 开仓日期
                    ReturnRate.at[Transaction_order, 'Close_DateTime'] = df.at[closepoint, 'Dates']  # 平仓日期
                    ReturnRate.at[Transaction_order, 'Open Price'] = P_open
                    ReturnRate.at[Transaction_order, 'Close Price'] = P_close

                    spread_open = df.at[buypoint, 'Ask Price'] - df.at[buypoint, 'Bid Price'] # 开仓spread
                    spread_close = df.at[closepoint, 'Ask Price'] - df.at[closepoint, 'Bid Price'] #    平仓spread

                    ReturnRate.at[Transaction_order, 'Spread Open'] = spread_open
                    ReturnRate.at[Transaction_order, 'Spread Close'] = spread_close
                    ReturnRate.at[Transaction_order, 'Return Rate'] = (P_close - P_open -0.001*P_close -
                                                                       (spread_close + spread_open))/P_open
                    ReturnRate.at[Transaction_order, 'Date'] = day

            else:   #RowIndex < closepoint, cannot open another position
                continue

    #   Group by per day:
    ReturnRate['Return Rate'] = pd.to_numeric(ReturnRate['Return Rate'])
    ReturnDay = ReturnRate.groupby(['Date'], as_index=False)['Return Rate'].sum()  # append
    append_data = ReturnDay

    #   Reset index
    initial_data.rename(index=str, columns={0: "Date", 1: "Rate"}, inplace=True)
    append_data.rename(index=str, columns={0: "Date", 1: "Rate"}, inplace=True)

    #   Merge
    merged_data = initial_data.merge(append_data, how='left', on='Date')
    merged_data['Return Rate'] = merged_data['Return Rate'].fillna(0)
    merged_data['ABS'] = merged_data['Return Rate'].abs()
    del (merged_data['Rate'])

    #   Sort
    merged_data = merged_data.sort_values(by='Date', ascending=True)

    STD = merged_data.loc[:, 'Return Rate'].std()
    Mean = merged_data.loc[:, 'Return Rate'].mean()
    SharpeRatio = math.sqrt(252) * Mean / STD

    #ReturnRate.to_csv('C:/Users/www/Desktop/BackTest/M6/ReturnRate 1050&2.5.csv')    #参数
    #merged_data.to_csv('C:/Users/www/Desktop/BackTest/M6/Daily Return 1050&2.5.csv') #参数

    ReturnDF = {'Sharpe': [SharpeRatio], 'EMA': [N], 'STD': [K], '# of Trade': [Transaction_order]}
    ReturnDF = pd.DataFrame(ReturnDF)
    print(ReturnDF)
    return(ReturnDF)

SharpeRecord = pd.DataFrame(columns=['Sharpe', 'EMA', 'STD', '# of Trade'])
for N in range(900, 2050, 50):    #upperband is not inclusive
    for K in np.arange(2.5, 4.5, 0.5):
        dfx = sharpe(N, K)
        SharpeRecord = SharpeRecord.append(dfx, ignore_index=True)
SharpeRecord.to_csv("C:/Users/www/Desktop/BackTest/M6/Sharpe Record.csv")

#print(sharpe(1050, 2.5)) #参数
#即使发生我上述我所说的情况，那么平仓点也是在最后一刻！
