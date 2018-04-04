# coding=utf-8
import os
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.offsetbox import AnchoredOffsetbox, TextArea, HPacker
import yaml
import numpy as np
import MySQLdb as mdb
import sys
import time

# get the information of csv from database
def connect_database_return_df_basic_csv(message, table):
    host_id, user_name, password, db_name = message
    conn = mdb.connect(host=host_id, user=user_name, passwd=password, db=db_name)
    cursor = conn.cursor()
    sql = 'SELECT A, B, C1, D, E, F, G, H FROM ' + table
    cursor.execute(sql)
    df = pd.read_sql(sql, conn)
    conn.close()
    return df


def connect_database(message):
    host_id, user_name, password, db_name = message
    conn = mdb.connect(host=host_id, user=user_name, passwd=password, db=db_name)
    return conn


# just combination the all
def get_portfolio_bkt_csv(csv_dir):
    max_ctime = 0.0
    file_list = os.listdir(csv_dir)

    for file in file_list:
        ctime = os.path.getctime("%s/%s" % (csv_dir, file))
        if ctime > max_ctime and file[-4:] == "csv":
            max_ctime = ctime  # get the max time of a file

    is_new_pot = True
    pot_file_name = "portfolio.csv"
    pot_dir_name = "%s/%s" % (csv_dir, pot_file_name)
    # make sure that the portfolio is the best!
    if pot_file_name in file_list:
        is_new_pot = False
        ctime = os.path.getctime("%s/%s" % (csv_dir, pot_file_name))
        if max_ctime > ctime:
            is_new_pot = True

    if is_new_pot:
        PnL_df = pd.DataFrame()
        csv_file_num = 0
        for file in file_list:
            if file[-4:] == ".csv" and len(file.split('.')) == 3:
                file_name = "%s/%s" % (csv_dir, file)
                print(file_name)
                file_size = os.path.getsize(file_name)
                if file_size > 0:
                    eps = 0.000001
                    pnl_csv_df = pd.read_csv(file_name, header=None, names=["date", "time", "price",
                                                                            "position", "fnormpos", "fnetreturn",
                                                                            "normpos", "netreturn"])
                    pnl_df = pd.DataFrame()
                    pnl_df["datetimenum"] = pnl_csv_df["date"] * 1000000 + pnl_csv_df["time"]
                    pnl_df["price"] = pnl_csv_df["price"]
                    pnl_df["delta_pnl"] = pnl_csv_df["netreturn"] - pnl_csv_df["netreturn"].shift(1)
                    pnl_df.set_value(pnl_df.index[0], "delta_pnl", 0)

                    PnL_df = PnL_df.append(pnl_df, ignore_index=True)
                    csv_file_num += 1

        Grouped_Pnl_df = PnL_df.groupby(PnL_df["datetimenum"], sort=True)

        Pot_Pnl_df = pd.DataFrame()
        Pot_Pnl_df["datetimenum"] = Grouped_Pnl_df["datetimenum"].first()
        Pot_Pnl_df["price"] = Grouped_Pnl_df["price"].first()
        Pot_Pnl_df["delta_pnl"] = Grouped_Pnl_df["delta_pnl"].sum()
        Pot_Pnl_df["pnl"] = Pot_Pnl_df["delta_pnl"].cumsum()
        Pot_Pnl_df["delta_pnl"] = Pot_Pnl_df["delta_pnl"] / csv_file_num
        Pot_Pnl_df["pnl"] = Pot_Pnl_df["pnl"] / csv_file_num

        Pot_Pnl_df.to_csv(pot_dir_name, index=False)

    return pot_dir_name


# Be ready for the all result
# it is version of the csv file
def get_portfolio_bkt_csv_select(csv_dir, csv_array):
    max_ctime = 0.0
    file_list = os.listdir(csv_dir)
    No = 0

    for file in file_list:
        if file[-13:] == "portfolio.csv":
            No += 1

    is_new_pot = True
    head_inf = "The_%s_combination" % No
    pot_file_name = head_inf+"_portfolio.csv"

    pot_dir_name = "%s/%s" % (csv_dir, pot_file_name)
    # make sure that the portfolio is the best!
    if is_new_pot:
        PnL_df = pd.DataFrame()
        csv_file_num = 0
        for file in csv_array:
            if file[-4:] == ".csv" and len(file.split('.')) == 3:
                file_name = "%s/%s" % (csv_dir, file)
                print(file_name)
                file_size = os.path.getsize(file_name)
                if file_size > 0:
                    eps = 0.000001
                    pnl_csv_df = pd.read_csv(file_name, header=None, names=["date", "time", "price",
                                                                            "position", "fnormpos", "fnetreturn",
                                                                            "normpos", "netreturn"])
                    pnl_df = pd.DataFrame()
                    pnl_df["datetimenum"] = pnl_csv_df["date"] * 1000000 + pnl_csv_df["time"]
                    pnl_df["price"] = pnl_csv_df["price"]
                    pnl_df["delta_pnl"] = pnl_csv_df["netreturn"] - pnl_csv_df["netreturn"].shift(1)
                    pnl_df.set_value(pnl_df.index[0], "delta_pnl", 0)

                    PnL_df = PnL_df.append(pnl_df, ignore_index=True)
                    csv_file_num += 1

        Grouped_Pnl_df = PnL_df.groupby(PnL_df["datetimenum"], sort=True)

        Pot_Pnl_df = pd.DataFrame()
        Pot_Pnl_df["datetimenum"] = Grouped_Pnl_df["datetimenum"].first()
        Pot_Pnl_df["price"] = Grouped_Pnl_df["price"].first()
        Pot_Pnl_df["delta_pnl"] = Grouped_Pnl_df["delta_pnl"].sum()
        Pot_Pnl_df["pnl"] = Pot_Pnl_df["delta_pnl"].cumsum()
        Pot_Pnl_df["delta_pnl"] = Pot_Pnl_df["delta_pnl"] / csv_file_num
        Pot_Pnl_df["pnl"] = Pot_Pnl_df["pnl"] / csv_file_num

        Pot_Pnl_df.to_csv(pot_dir_name, index=False)

    return pot_dir_name


# Be ready for the all result
# it is version of the database file
def get_portfolio_bkt_csv_select_database(csv_dir, csv_array, message, stand_name):
    max_ctime = 0.0
    file_list = os.listdir(csv_dir)
    No = 0

    if csv_dir[-1] == '/':
        csv_dir = csv_dir[:-1]

    for file in file_list:
        if file[-13:] == "portfolio.csv":
            No += 1

    is_new_pot = True
    pot_file_name = stand_name + "_portfolio.csv"

    pot_dir_name = "%s/%s" % (csv_dir, pot_file_name)
    # make sure that the portfolio is the best!

    '''
    # storage the selected file in this txt file
    txt_name = head_inf + '_selected_table.txt'
    txt_name = csv_dir + '/' + txt_name
    f1 = open(txt_name, 'w')
    for i in csv_array:
        f1.write(i+'\n')
    f1.close()
    '''

    if is_new_pot:
        PnL_df = pd.DataFrame()
        csv_file_num = 0
        number = 1
        t1 = time.time()
        for file in csv_array:
            print("the %d selected: " % number, file)
            number += 1
            pnl_csv_df = connect_database_return_df_basic_csv(message, file)
            eps = 0.000001
            pnl_csv_df.rename(
                columns={'A': 'date', 'B': 'time', 'C1': 'price', 'D': 'position', 'E': 'fnormpos', 'F': 'fnetreturn',
                         'G': 'normpos', 'H': 'netreturn'}, inplace=True)
            '''
            pnl_csv_df = pd.read_csv(file_name, header=None, names=["date", "time", "price",
                                                                    "position", "fnormpos", "fnetreturn",
                                                                    "normpos", "netreturn"])
            '''
            pnl_df = pd.DataFrame()
            pnl_df["datetimenum"] = pnl_csv_df["date"] * 1000000 + pnl_csv_df["time"]
            pnl_df["price"] = pnl_csv_df["price"]
            pnl_df["delta_pnl"] = pnl_csv_df["netreturn"] - pnl_csv_df["netreturn"].shift(1)
            pnl_df.set_value(pnl_df.index[0], "delta_pnl", 0)

            PnL_df = PnL_df.append(pnl_df, ignore_index=True)
            csv_file_num += 1
            t2 = time.time()
            print("the cost time: ", t2 - t1, "\n")

        Grouped_Pnl_df = PnL_df.groupby(PnL_df["datetimenum"], sort=True)

        Pot_Pnl_df = pd.DataFrame()
        Pot_Pnl_df["datetimenum"] = Grouped_Pnl_df["datetimenum"].first()
        Pot_Pnl_df["price"] = Grouped_Pnl_df["price"].first()
        Pot_Pnl_df["delta_pnl"] = Grouped_Pnl_df["delta_pnl"].sum()
        Pot_Pnl_df["pnl"] = Pot_Pnl_df["delta_pnl"].cumsum()
        Pot_Pnl_df["delta_pnl"] = Pot_Pnl_df["delta_pnl"] / csv_file_num
        Pot_Pnl_df["pnl"] = Pot_Pnl_df["pnl"] / csv_file_num

        Pot_Pnl_df.to_csv(pot_dir_name, index=False)

    return pot_dir_name


# draw the picture of masive of it
def plot_portfolio_result(pot_file_name):
    pot_file_name_main = pot_file_name[:-4]
    cfg_file_name = "%s%s" % (pot_file_name_main, "_config.yaml")
    pot_ratio = 1
    point_value = 1
    if (os.path.exists(cfg_file_name)):
        cfg_stream = open(cfg_file_name, 'r')
        cfg = yaml.load(cfg_stream)
        cfg_stream.close()
        if "Ratio" in cfg.keys():
            pot_ratio = cfg["Ratio"]
        if "PointValue" in cfg.keys():
            point_value = cfg["PointValue"]

    Pot_Pnl_df = pd.read_csv(pot_file_name)
    datenum = Pot_Pnl_df["datetimenum"].apply(lambda d: int(d / 1000000))
    profits = Pot_Pnl_df["pnl"] * pot_ratio
    contract_value = Pot_Pnl_df["price"] * point_value * pot_ratio
    profit_peak = profits.cummax()
    drawdown = profits - profit_peak
    profit_peak_idx = profits[profits == profit_peak].index
    contract_peak = Pot_Pnl_df["price"] * point_value * pot_ratio
    ## Get the initial contract_value for drawdown range.
    for ipk in np.arange(1, len(profit_peak_idx)):
        ipk_st = profit_peak_idx[ipk - 1]
        ipk_ed = profit_peak_idx[ipk]
        contract_peak[ipk_st:ipk_ed] = contract_value[ipk_st]
    ipk_st = profit_peak_idx[-1]
    contract_peak[ipk_st:] = contract_value[ipk_st]
    drawdown_ratio = -drawdown / contract_peak
    max_dd_ratio = drawdown_ratio.max()
    max_dd_ratio_idx = drawdown_ratio[drawdown_ratio == max_dd_ratio].index

    # day
    dailyprofitcum = profits.groupby(datenum).last()
    dailyprofit = dailyprofitcum.diff()
    dailyprofit.iloc[0] = dailyprofitcum.iloc[0]
    dailywinrate = float((dailyprofit > 0).sum()) / len(datenum.unique())
    dailysharpe = dailyprofit.mean() / dailyprofit.std() * (242 ** 0.5)
    # month
    mon_idx = (datenum / 100).astype(int)
    mon_idx_diff = mon_idx.diff()
    mon_idx_diff.loc[0] = 1
    mon_idx_diff.loc[Pot_Pnl_df.index[-1]] = 1
    x_mon_idx = mon_idx_diff[mon_idx_diff > 0].index
    ###
    net_profit = profits.tail(1)
    max_drawdown = drawdown.min()  # max drawdown
    sharpe_ratio = dailysharpe
    mdd_idx = drawdown[drawdown == max_drawdown].index
    max_profit = profits.values[0:mdd_idx.max()].max()

    ind1 = 'NP = {}'.format('%.1f' % (net_profit))
    ind2 = 'MDD = {}'.format('%.1f' % (max_drawdown))
    ind3 = 'ShR = {}'.format('%.1f' % (sharpe_ratio))
    ind4 = 'Ratio = {}'.format('%.1f' % (pot_ratio))
    ind5 = 'MDD_R = {}'.format('%.4f' % (max_dd_ratio))

    ind_1 = float(net_profit)
    ind_2 = float(max_drawdown)
    ind_3 = float(sharpe_ratio)
    ind_4 = float(pot_ratio)
    ind_5 = float(max_dd_ratio)

    ##
    fsize = 20
    fig = plt.figure(figsize=(44, 20))
    plt.subplots_adjust(left=0.05, right=0.95, top=0.95, bottom=0.05)
    plt.title(pot_file_name_main, size=(fsize + 5))
    ## contract_value and contract_peak
    plt.plot(Pot_Pnl_df.index, contract_value, color='black')
    # plt.plot(Pot_Pnl_df.index, contract_peak, color = 'blue')
    ## drawdown and profit
    plt.fill_between(Pot_Pnl_df.index, 0, drawdown, facecolor='red', color='red')
    plt.fill_between(Pot_Pnl_df.index, 0, profits, facecolor='green', color='green')

    nowsum = 0
    xT = []
    yT = []
    for x in x_mon_idx.values:
        ptd = profits.values[x]
        y = ptd - nowsum

        if y > 0:
            bbox_props = dict(boxstyle='round', ec='none', fc='#F4F4F4', alpha=0.7)
            plt.text(x, (4 * y), float(format(y, '.2f')), size=fsize, bbox=bbox_props, color='#000000')
        else:
            bbox_props = dict(boxstyle='round', ec='none', fc='#F4F4F4', alpha=0.7)
            plt.text(x, (4 * y), float(format(y, '.2f')), size=fsize, bbox=bbox_props, color='#000000')

        xT.append(x)
        yT.append(str(datenum.values[x]))
        nowsum = ptd

    max_value = contract_value.max()
    for x in max_dd_ratio_idx:
        bbox_props = dict(boxstyle='round', ec='none', fc='r', alpha=0.9)
        plt.text(x, (2.0 * max_value / 3.0), "MDD_R", size=fsize, bbox=bbox_props, color='#000000')

    plt.xticks(xT, yT)
    plt.tick_params(labelsize=fsize)

    bbox_props = dict(boxstyle='round', ec='g', fc='none')
    box1 = TextArea(ind1, textprops=dict(size=fsize, bbox=bbox_props))
    bbox_props = dict(boxstyle='round', ec='r', fc='none')
    box2 = TextArea(ind2, textprops=dict(size=fsize, bbox=bbox_props))
    bbox_props = dict(boxstyle='round', ec='b', fc='none')
    box3 = TextArea(ind3, textprops=dict(size=fsize, bbox=bbox_props))
    bbox_props = dict(boxstyle='round', ec='c', fc='none')
    box4 = TextArea(ind4, textprops=dict(size=fsize, bbox=bbox_props))
    bbox_props = dict(boxstyle='round', ec='c', fc='none')
    box5 = TextArea(ind5, textprops=dict(size=fsize, bbox=bbox_props))

    box = HPacker(children=[box4, box1, box2, box3, box5], pad=0, sep=fsize - 5)
    # box = HPacker(children=[box4, box1, box2, box3], pad=0, sep=fsize-5)

    ax = plt.gca()
    anchored_box = AnchoredOffsetbox(loc=2, child=box, pad=0.2, frameon=False)
    ax.add_artist(anchored_box)

    ax.grid(True)
    ax.autoscale_view()
    fig.autofmt_xdate()

    plt.savefig("%s%s" % (pot_file_name, ".png"))
    plt.close()
    pic_name = "%s%s" % (pot_file_name, ".png")
    return ind_1, ind_2, ind_3, ind_4, ind_5, pic_name


def show(dir_name):

    file_name = get_portfolio_bkt_csv(dir_name)
    plot_portfolio_result(file_name)


def test_unit0(dir_name, table, message):
    conn = connect_database(message)
    cursor = conn.cursor()
    sql = "select name1 from " + table
    cursor.execute(sql)

    raw_value = cursor.fetchall()[:10]
    conn.close()

    real_values = []

    for i in raw_value:
        real_values.append(i[0])

    print(len(raw_value))
    print(raw_value)
    call_it(dir_name, real_values, 'you')
    # get_portfolio_bkt_csv_select_database(dir_name, real_values)
    print('success!')


def call_it(dir_name, table_array, stand_name):
    message = ["192.168.1.2", "root", "rootKa$QZ", "LI_DataBase_ver3"]
    name = get_portfolio_bkt_csv_select_database(dir_name, table_array, message, stand_name)
    values = plot_portfolio_result(name)
    # print("success!")
    print(values[:-1])
    return name, values


dir_name = "/home/liziqiang/PycharmProjects/untitled/test2"
table = 'OCBJ5Y_rb_zs_TWDQGAD_20180323_102633_grid_name'
message = ["10.25.10.249", "huacheng", "huacheng123", "LI_DataBase_test5"]
# show('/home/liziqiang/PycharmProjects/untitled/test2')

'''
vals = ['SJ7ZNE.WOBV_DX_QG_GZ_ADC_DK_KBar_20m-_20150105-20180102_9th_bkt.csv',
        'SJ7ZNE.WOBV_DX_QG_GZ_ADC_DK_KBar_22m-_20150105-20180102_11th_bkt.csv',
        'SJ7ZNE.WOBV_DX_QG_GZ_ADC_DK_KBar_22m-_20150105-20180102_12th_bkt.csv',
        'SJ7ZNE.WOBV_DX_QG_GZ_ADC_DK_KBar_24m-_20150105-20180102_17th_bkt.csv',
        'SJ7ZNE.WOBV_DX_QG_GZ_ADC_DK_KBar_27m-_20150105-20180102_22th_bkt.csv']
table = 'OCBJ5Y_rb_zs_TWDQGAD_20180323_102633_grid_name'
message = ["10.25.10.249", "huacheng", "huacheng123", "LI_DataBase_test5"]
# name = get_portfolio_bkt_csv_select(dir_name, vals)
# print name
# test_unit0(dir_name, table, message)
'''
'''
if __name__ == '__main__':
    dir_name = sys.argv[1]
    table_array = sys.argv[2]
    call_it(dir_name, table_array)
'''
# test_unit0(dir_name, table, message)
# plot_portfolio_result('/home/liziqiang/PycharmProjects/untitled/test2/The_3_combination_portfolio.csv')