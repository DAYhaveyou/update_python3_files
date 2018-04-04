# coding=utf-8
import pandas as pd
import MySQLdb as mdb
import numpy as np
import time

'''part 0'''
# The comment
'''
(!*_*!) show you are pig!
(!-_-!) show not good!
(-_-) show normal!
(^_^) show good!
(\^_^/) show perfect!
'''


def connect_database(message):
    host_id, user_name, password, db_name = message
    conn = mdb.connect(host=host_id, user=user_name, passwd=password, db=db_name)
    return conn


def get_values_mysql(tablename, message):
    h, u, p, d = message
    # conn = mdb.connect(host='localhost', port=3306, user='root', passwd='7ondr', db='testpy')
    conn = mdb.connect(host=h, port=3306, user=u, passwd=p, db=d)
    cur = conn.cursor()
    sql = 'SELECT * FROM ' + tablename
    cur.execute(sql)
    df = pd.read_sql(sql, conn)
    df.rename(columns={'A': 'date', 'B': 'time', 'C1': 'price', 'D': 'position', 'E': 'fnormpos', 'F': 'fnetreturn',
                       'G': 'normpos', 'H': 'netreturn'}, inplace=True)
    conn.close()
    return df


# (-_^)
# get the 16 index from csv
def get_index14(tablename, message):
    # t1 = time.time()
    df = get_values_mysql(tablename, message)
    # t2 = time.time() - t1
    # print t2
    monBaseTen = 100
    yearBaseTen = 10000
    df = df.dropna(how="any")

    print(len(df['netreturn']))
    df['netreturn'] = df['netreturn'] - df['netreturn'][0]  # 减去第一个

    numtrade = df.shape[0]  # count of all
    # 1 get the unique day
    datenum = df['date'].apply(lambda d: int(d))  # select the only day
    numday = len(datenum.unique())

    # 2 get the unique month
    mon_df = datenum / 100
    mon_df = mon_df.apply(lambda d: int(d))
    num_mons = len(mon_df.unique())

    hands = df["normpos"].max()  # get the max normpos
    if 0 == hands:
        print("None of trade record")
        return -2
    profits = df["netreturn"] / hands  #
    drawdown = profits - profits.cummax()
    drawdown_range = np.array(profits < profits.cummax())

    close = np.array((df["price"]))
    drawdown_close = np.zeros(close.shape)
    drawdown_init_close = np.zeros(close.shape)
    min_close = close[0]
    init_close = min_close
    for ii in np.arange(1, drawdown_range.shape[0]):
        if drawdown_range[ii] and close[ii] < min_close:
            min_close = close[ii]

        elif not (drawdown_range[ii]):
            min_close = close[ii]
            init_close = min_close

        drawdown_close[ii] = min_close
        drawdown_init_close[ii] = init_close
    drawdown_d_close = -drawdown / drawdown_close
    drawdown_d_init_close = -drawdown / drawdown_init_close

    # trade
    pos = df["normpos"] - df["normpos"].shift(1)
    pos[0] = df["normpos"][0]
    tradetimes = np.sum(np.abs(pos.values))
    tradetimes = int(tradetimes / hands / 2)

    tradeprofit = profits.diff()
    tradeprofit.iloc[0] = profits.iloc[0]
    # winrate = float((tradeprofit > 0).sum()) / numtrade
    # pf = tradeprofit[tradeprofit > 0].sum() / -tradeprofit[tradeprofit < 0].sum()

    # day
    dailyprofitcum = profits.groupby(datenum).last()
    dailyprofit = dailyprofitcum.diff()
    dailyprofit.iloc[0] = dailyprofitcum.iloc[0]

    dailywinrate = float((dailyprofit > 0).sum()) / numday
    dailysharpe = dailyprofit.mean() / dailyprofit.std() * (242 ** 0.5)

    isnewhigh = dailyprofit > (dailyprofit.cummax() - 0.001)

    maxwindays = 0
    maxlossdays = 0
    nowdays = 0
    for x in dailyprofit.values:
        if x > 0:
            if nowdays > 0:
                nowdays += 1
            else:
                nowdays = 1
        if x < 0:
            if nowdays < 0:
                nowdays -= 1
            else:
                nowdays = -1
        maxwindays = max(maxwindays, nowdays)
        maxlossdays = min(maxlossdays, nowdays)
    daystonewhigh = 0
    nowdays = 0
    for x in isnewhigh.values:
        if x:
            nowdays = 0
        else:
            nowdays += 1
        daystonewhigh = max(daystonewhigh, nowdays + 1)

    # month
    monIdx = (datenum / monBaseTen).astype(int)

    # year
    yearIdx = (datenum / yearBaseTen).astype(int)

    netProfit = profits.iloc[-1]  # net profit
    maxDrawDown = drawdown.min()  # max drawdown
    tradeTimes = tradetimes
    numDay = numday  # days
    dailyWr = dailywinrate * 100  # day winrate
    dailyShr = dailysharpe  # annualized daily sharpe
    dailyStd = dailyprofit.std()
    PpT = netProfit / tradetimes
    dailyMaxProfit = dailyprofit.max()
    dailyMinProfit = dailyprofit.min()
    maxWinDays = int(maxwindays)
    maxLossDays = int(-maxlossdays)
    daysToNewHigh = int(daystonewhigh)

    '''
    ind1 = 'NP = {}'.format('%.1f' % (netProfit))
    ind2 = 'MDD = {}'.format('%.1f' % (maxDrawDown))
    ind3 = 'PpT = {}'.format('%.1f' % (PpT))
    ind4 = 'TT = {}'.format('%.0f' % (tradeTimes))
    ind5 = 'ShR = {}'.format('%.1f' % (dailyShr))
    ind6 = 'Days = {}'.format('%.0f' % (numDay))
    ind7 = 'D_WR = {}'.format('%.1f' % (dailyWr))
    ind8 = 'D_STD = {}'.format('%.1f' % (dailyStd))
    ind9 = 'D_MaxNP = {}'.format('%.1f' % (dailyMaxProfit))
    ind10 = 'D_MinNP = {}'.format('%.1f' % (dailyMinProfit))
    ind11 = 'maxW_Days = {}'.format('%.0f' % (maxWinDays))
    ind12 = 'maxL_Days = {}'.format('%.0f' % (maxLossDays))
    ind13 = 'mDD_mC = {}'.format('%.2f' % (drawdown_d_close.max()))
    ind14 = 'mDD_iC = {}'.format('%.2f' % (drawdown_d_init_close.max()))
    '''
    sql = "insert into %s(NP, MDD, PpT, TT, ShR, Days, " \
          "D_WR, D_STD, D_MaxNP, D_MinNP, maxW_Days, maxL_Days, mDD_mC, mDD_iC)" \
          "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    ind1 = netProfit
    ind2 = maxDrawDown
    ind3 = PpT
    ind4 = tradeTimes
    ind5 = dailyShr
    ind6 = numDay
    ind7 = dailyWr
    ind8 = dailyStd
    ind9 = dailyMaxProfit
    ind10 = dailyMinProfit
    ind11 = maxWinDays
    ind12 = maxLossDays

    ind13 = drawdown_d_close.max()
    ind14 = drawdown_d_init_close.max()

    # print t2
    # t3 = (time.time() - t2 - t1)
    # print t3
    return ind1, ind2, ind3, ind4, ind5, ind6, ind7, ind8, ind9, ind10, ind11, ind12, ind13, ind14


# (^_-)
# get 16 index from csv
def get_index16(tablename, message, stop_loss=20, point_value=5):

    # get the values
    conn = connect_database(message)
    cur = conn.cursor()
    sql = 'SELECT * FROM ' + tablename
    cur.execute(sql)
    df = pd.read_sql(sql, conn)
    df.rename(columns={'A': 'date', 'B': 'time', 'C1': 'price', 'D': 'position', 'E': 'fnormpos', 'F': 'fnetreturn',
                       'G': 'normpos', 'H': 'netreturn'}, inplace=True)

    # 2
    monBaseTen = 100
    yearBaseTen = 10000

    df = df.dropna(how="any")
    conn.close()
    df['netreturn'] = df['netreturn'] - df['netreturn'][0]

    numtrade = df.shape[0]
    datenum = df['date'].apply(lambda d: int(d))
    numday = len(datenum.unique())
    mon_df = datenum / 100
    mon_df = mon_df.apply(lambda d: int(d))
    num_mons = len(mon_df.unique())

    hands = df["normpos"].max()
    if 0 == hands:
        print("None of trade record")
        return -2
    profits = df["netreturn"] / hands
    drawdown = profits - profits.cummax()
    drawdown_range = np.array(profits < profits.cummax())
    close = np.array((df["price"]))
    drawdown_close = np.zeros(close.shape)
    drawdown_init_close = np.zeros(close.shape)
    min_close = close[0]
    init_close = min_close
    for ii in np.arange(1, drawdown_range.shape[0]):
        if drawdown_range[ii] and close[ii] < min_close:
            min_close = close[ii]

        elif not (drawdown_range[ii]):
            min_close = close[ii]
            init_close = min_close

        drawdown_close[ii] = min_close
        drawdown_init_close[ii] = init_close
    drawdown_d_close = -drawdown / (drawdown_close * point_value)  # 2
    drawdown_d_init_close = -drawdown / (drawdown_init_close * point_value)  # 3
    leverage = stop_loss / drawdown_d_init_close.max()  # 1
    # trade
    pos = df["normpos"] - df["normpos"].shift(1)
    pos[0] = df["normpos"][0]
    tradetimes = np.sum(np.abs(pos.values))
    tradetimes = int(tradetimes / hands / 2)

    tradeprofit = profits.diff()
    tradeprofit.iloc[0] = profits.iloc[0]
    # winrate = float((tradeprofit > 0).sum()) / numtrade
    # pf = tradeprofit[tradeprofit > 0].sum() / -tradeprofit[tradeprofit < 0].sum()

    # day
    dailyprofitcum = profits.groupby(datenum).last()
    dailyprofit = dailyprofitcum.diff()
    dailyprofit.iloc[0] = dailyprofitcum.iloc[0]

    dailywinrate = float((dailyprofit > 0).sum()) / numday
    dailysharpe = dailyprofit.mean() / dailyprofit.std() * (242 ** 0.5)

    isnewhigh = dailyprofit > (dailyprofit.cummax() - 0.001)

    maxwindays = 0
    maxlossdays = 0
    nowdays = 0
    for x in dailyprofit.values:
        if x > 0:
            if nowdays > 0:
                nowdays += 1
            else:
                nowdays = 1
        if x < 0:
            if nowdays < 0:
                nowdays -= 1
            else:
                nowdays = -1
        maxwindays = max(maxwindays, nowdays)
        maxlossdays = min(maxlossdays, nowdays)
    daystonewhigh = 0
    nowdays = 0
    for x in isnewhigh.values:
        if x:
            nowdays = 0
        else:
            nowdays += 1
        daystonewhigh = max(daystonewhigh, nowdays + 1)

    # month
    monIdx = (datenum / monBaseTen).astype(int)
    monIdxDiff = monIdx.diff()
    monIdxDiff.loc[0] = 1
    monIdxDiff.loc[df.index[-1]] = 1
    xind = monIdxDiff[monIdxDiff > 0].index
    # year
    yearIdx = (datenum / yearBaseTen).astype(int)

    netProfit = profits.iloc[-1]  # net profit
    maxDrawDown = drawdown.min()  # max drawdown
    tradeTimes = tradetimes
    numDay = numday  # days
    dailyWr = dailywinrate * 100  # day winrate
    dailyShr = dailysharpe  # annualized daily sharpe
    dailyStd = dailyprofit.std()
    PpT = netProfit / tradetimes
    dailyMaxProfit = dailyprofit.max()
    dailyMinProfit = dailyprofit.min()
    maxWinDays = int(maxwindays)
    maxLossDays = int(-maxlossdays)
    daysToNewHigh = int(daystonewhigh)

    ind1 = 'NP = {}'.format('%.1f' % (netProfit))
    ind2 = 'MDD = {}'.format('%.1f' % (maxDrawDown))
    ind3 = 'PpT = {}'.format('%.1f' % (PpT))
    ind4 = 'TT = {}'.format('%.0f' % (tradeTimes))
    ind5 = 'ShR = {}'.format('%.1f' % (dailyShr))
    ind6 = 'Days = {}'.format('%.0f' % (numDay))
    ind7 = 'D_WR = {}'.format('%.1f' % (dailyWr))
    ind8 = 'D_STD = {}'.format('%.1f' % (dailyStd))
    ind9 = 'D_MaxNP = {}'.format('%.1f' % (dailyMaxProfit))
    ind10 = 'D_MinNP = {}'.format('%.1f' % (dailyMinProfit))
    ind11 = 'maxW_Days = {}'.format('%.0f' % (maxWinDays))
    ind12 = 'maxL_Days = {}'.format('%.0f' % (maxLossDays))

    ind13 = 'mDD_mC = {}'.format('%.2f' % (drawdown_d_close.max()))  # 4
    ind14 = 'mDD_iC = {}'.format('%.2f' % (drawdown_d_init_close.max()))  # 5
    ind15 = 'LVRG = {}'.format('%.2f' % (leverage))  # 6
    ind16 = 'LVRG_Mon_NP = {}'.format('%.2f' % (leverage * netProfit / num_mons))  # 7

    ind1 = netProfit
    ind2 = maxDrawDown
    ind3 = PpT
    ind4 = tradeTimes
    ind5 = dailyShr
    ind6 = numDay
    ind7 = dailyWr
    ind8 = dailyStd
    ind9 = dailyMaxProfit
    ind10 = dailyMinProfit
    ind11 = maxWinDays
    ind12 = maxLossDays

    ind13 = drawdown_d_close.max()
    ind14 = drawdown_d_init_close.max()
    ind15 = leverage  # 6
    ind16 = leverage * netProfit / num_mons

    sql2 = "update " + 'table' + " set NP=%s, MDD=%s, PpT=%s, TT=%s, ShR=%s, Days=%s, D_WR=%s, D_STD=%s, D_MaxNP=%s," \
                                 " D_MinNP=%s, maxW_Days=%s, maxL_Days=%s, mDD_mC=%s, mDD_iC=%s, LVRG=%s, LVRG_Mon_NP=%s"\
                                 " where name='%s'"

    return ind1, ind2, ind3, ind4, ind5, ind6, ind7, ind8, ind9, ind10, ind11, ind12, ind13, ind14, ind15, ind16