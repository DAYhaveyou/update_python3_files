import yaml
import MySQLdb as mdb
import numpy as np
import sys
import re
import random
import subprocess
import os
import pandas as pd
import time


def get_one_csv_indicators(dir_name):

    if dir_name[-1] == "/":
        dir_name = dir_name[:-1]

    indicator_array = []
    indicator_cols = ['File', 'NP', 'MDD', 'Ppt', 'TT', 'ShR', 'Days', 'D_WR', 'D_STD', 'D_MaxNP',
                      'D_MinNP', 'maxW_Days', 'maxL_Days', 'mDD_mC', 'mDD_iC', 'LVRG', 'LVRG_Mon_NP']

    file_list = os.listdir(dir_name)

    num = 0
    number = 0
    t2 = 0
    t1 = time.time()
    for file in file_list:
        if file[-7:] == "bkt.csv":
            file_name = dir_name + '/' + file
            indicator_list = plot_wfo_leverage_result_to_indicator_csv(file_name)
            if len(indicator_list) == len(indicator_cols):
                indicator_array.append(indicator_list)
                number += 1
            else:
                num += 1
                print("NUM %d missed!" % num)

            t2 = time.time()
            print("The number %d cost %.3f s\n" % (number, t2 - t1))
    print("The missed number %d" % num)
    csv_file_name = 'one_indicators_test12.csv'
    t2 = time.time()
    if len(indicator_array) > 0:
        ind_pd_new = pd.DataFrame(indicator_array, columns=indicator_cols)
        ind_pd_new.to_csv('%s/%s' % (dir_name, csv_file_name), index=False)
        t3 = time.time()
        print("The create csv time: %.3f" % (t3-t2))


def plot_wfo_leverage_indicators(fileName, stop_loss=20, point_value=5):
    fileSize = os.path.getsize(fileName)
    monBaseTen = 100
    yearBaseTen = 10000
    indicator_list = []
    if fileSize > 0:
        df = pd.read_csv(fileName, header=None, names=['date', 'time', 'price',
                                                       'position', 'fnormpos', 'fnetreturn', 'normpos', 'netreturn'])
        df = df.dropna(how="any")

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
        drawdown_d_init_close = -drawdown / (drawdown_init_close * point_value)
        leverage = stop_loss / drawdown_d_init_close.max()
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

        netProfit = profits.iloc[-1]  # net profit
        maxDrawDown = drawdown.min()  # max drawdown
        tradeTimes = tradetimes
        numDay = numday  # days
        dailyShr = dailysharpe  # annualized daily sharpe
        PpT = netProfit / tradetimes



        ####
        indicator_list.append(fileName.split('/')[-1])
        indicator_list.append(netProfit)
        indicator_list.append(maxDrawDown)
        indicator_list.append(PpT)
        indicator_list.append(drawdown_d_init_close.max())
        indicator_list.append(leverage)
        indicator_list.append((leverage * netProfit / num_mons))
        indicator_list.append(tradeTimes)
        indicator_list.append(dailyShr)
        indicator_list.append(numDay)
        ####

        return indicator_list
    else:
        print("The %s Backtest result is empty!" % (fileName))
        return indicator_list


def plot_wfo_leverage_result_to_indicator_csv(fileName, stop_loss=20, point_value=5):
    fileSize = os.path.getsize(fileName)
    monBaseTen = 100
    yearBaseTen = 10000
    indicator_list = []
    if fileSize > 0:
        df = pd.read_csv(fileName, header=None, names=['date', 'time', 'price',
                                                       'position', 'fnormpos', 'fnetreturn', 'normpos', 'netreturn'])
        df = df.dropna(how="any")

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
        drawdown_d_close = -drawdown / (drawdown_close * point_value)
        drawdown_d_init_close = -drawdown / (drawdown_init_close * point_value)
        leverage = stop_loss / drawdown_d_init_close.max()
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

        ####

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


        indicator_list.append(fileName.split('/')[-1])  # file name
        # indicator_list.append(netProfit)
        # indicator_list.append(maxDrawDown)
        # indicator_list.append(PpT)
        # indicator_list.append(drawdown_d_init_close.max())
        # indicator_list.append(leverage)
        # indicator_list.append((leverage * netProfit / num_mons))
        # indicator_list.append(tradeTimes)
        # indicator_list.append(dailyShr)
        # indicator_list.append(numDay)

        indicator_list.append(ind1)
        indicator_list.append(ind2)
        indicator_list.append(ind3)
        indicator_list.append(ind4)
        indicator_list.append(ind5)
        indicator_list.append(ind6)
        indicator_list.append(ind7)
        indicator_list.append(ind8)
        indicator_list.append(ind9)
        indicator_list.append(ind10)
        indicator_list.append(ind11)
        indicator_list.append(ind12)
        indicator_list.append(ind13)
        indicator_list.append(ind14)
        indicator_list.append(ind15)
        indicator_list.append(ind16)

        return indicator_list
    else:
        print("The %s Backtest result is empty!" % (fileName))
        return indicator_list


path = '/public/home/liziqiang/Desktop/j_zs_WH_WOBV_DX_QG_GZ_20180102-81542_grid.yaml'  # /one_indicators_test12.csv'
get_one_csv_indicators(path)
t6 = time.time()
t7 = time.time() + 8
print("The create csv time: %.3f" % (t7-t6))
