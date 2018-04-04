# coding=utf-8
import pandas as pd
import yaml
import MySQLdb as mdb


def get_data_from_csv_to_selected_csv(dir_name):
    pass


def get_csv(file_name):

    # df = pd.read_csv(file_name, header=None, names=['File', 'NP', 'MDD', 'PpT', 'MDD_iC', 'LVRG',
    #                                                 'LVRG_Mon_NP', 'TT', 'ShR', 'NumD'])
    df = pd.read_csv(file_name)
    # t = df.sort_values(by='NP')

    # print t['NP']
    # print t['NP']
    t = df.sort_values(by='LVRG', ascending=False).head(10)
    print(type(df))
    # print df['NP'][1]
    print(t['NP'])
    print(t['LVRG'])


def select_csv(file_name):

    storage_csv = []

    data_csv = pd.read_csv(file_name)
    LVRG_data = data_csv.sort_values(by='LVRG', ascending=False)[0:100]
    LVRG_Mon_NP_data = data_csv.sort_values(by='LVRG_Mon_NP', ascending=False)[0:100]
    NP_data = data_csv.sort_values(by='NP', ascending=False)[0:100]
    ShR_data = data_csv.sort_values(by='ShR', ascending=False)[0:100]
    ### merge

    temp_data = [LVRG_data['File'], LVRG_Mon_NP_data['File'], NP_data['File'], ShR_data['File']]
    # for i in range(1, len(temp_data[0])):
    #    storage_csv.append(temp_data[0][i])

    for i in temp_data[0]:
        storage_csv.append(i)
    # 合并的操作

    for i in range(1, len(temp_data)):
        for j in temp_data[i]:
            if j not in storage_csv:
                storage_csv.append(j)

    sort_data = pd.merge(LVRG_Mon_NP_data, NP_data, on='File')
    sort_data = pd.merge(sort_data, ShR_data, on='File')
    # sort_data = pd.merge(sort_data, LVRG_data, on='File')
    sort_name = sort_data['File']
    # print ShR_data[:10]
    # print len(sort_data)
    print('The number of selected csv: ', len(storage_csv))
    # print storage_csv[:10]
    return storage_csv


def select_csv_from_setting_yaml(yaml_file, file_name):
    f = open(yaml_file)
    raw_value0 = yaml.load(f)

    # flag = 0
    flag = raw_value0[1]['flag_site']
    # raw_value = {'first': 1, 'second': 2}
    raw_value = raw_value0[0]
    raw_value = dict(raw_value)
    keys = []
    values = []
    num = 0

    for value in raw_value.items():
        keys.append(value[0])
        values.append(value[1])
        num += 1
    print(keys)
    print(values)
    print(num, flag)

    storage_values = []
    storage_values2 = []
    temp_data = []
    data_csv = pd.read_csv(file_name)

    for i in range(num):
        val = data_csv.sort_values(by=keys[i], ascending=False)[0:values[i]]
        temp_data.append(val['File'])

    for i in temp_data[0]:
        storage_values.append(i)
    # combination
    if flag == 1:
        for i in range(1, len(temp_data)):
            for j in temp_data[i]:
                if j not in storage_values:
                    storage_values.append(j)
    # just n
    elif flag == 0:
        for i in range(1, len(temp_data)):
            for j in temp_data[i]:
                if j in storage_values:
                    storage_values2.append(j)
            storage_values = storage_values2
            storage_values2 = []
            if len(storage_values) == 0:
                return 0
    print(len(storage_values))
    return storage_values


def select_csv_from_setting_yaml_return_database(yaml_file, file_name):
    f = open(yaml_file)
    raw_value0 = yaml.load(f)

    # flag = 0
    flag = raw_value0[1]['flag_site']
    # raw_value = {'first': 1, 'second': 2}
    raw_value = raw_value0[0]
    raw_value = dict(raw_value)
    keys = []
    values = []
    num = 0

    for value in raw_value.iteritems():
        keys.append(value[0])
        values.append(value[1])
        num += 1
    print(keys)
    print(values)
    print(num, flag)

    storage_values = []
    storage_values2 = []
    temp_data = []
    data_csv = pd.read_csv(file_name)

    for i in range(num):
        val = data_csv.sort_values(by=keys[i], ascending=False)[0:values[i]]
        temp_data.append(val)

    for i in temp_data[0]:
        storage_values.append(i)
    # combination
    if flag == 1:
        for i in range(1, len(temp_data)):
            for j in temp_data[i]:
                if j not in storage_values:
                    storage_values.append(j)
    # just n
    elif flag == 0:
        for i in range(1, len(temp_data)):
            for j in temp_data[i]:
                if j in storage_values:
                    storage_values2.append(j)
            storage_values = storage_values2
            storage_values2 = []
            if len(storage_values) == 0:
                return 0
    print(len(storage_values))
    return storage_values


def select_csv_new(setting, file_name):
    f = open(setting, 'r')

    raw_yaml = yaml.load(f)

    storage_csv = []

    data_csv = pd.read_csv(file_name)


def work_setting_select(file_name):
    dict = [{'LVRG': 100, 'LVRG_Mon_NP': 100, 'NP': 100, 'ShR': 100}, {'flag_site': 1}]
    f = open(file_name, 'w')

    yaml.dump(dict, f)
    f.close()


def test_unit6():
    val1 = []
    val2 = []
    val3 = []
    storage_values = []
    storage_values2 = []
    for i in range(10):
        val1.append(str(i))
        val2.append(str(i**2))
        val3.append(str(i ** 3))
    temp_data = [val1, val2, val3]

    for i in temp_data[0]:
        storage_values.append(i)

    for i in range(1, len(temp_data)):
        for j in temp_data[i]:
            if j in storage_values:
                storage_values2.append(j)
        storage_values = storage_values2
        storage_values2 = []
        if len(storage_values) == 0:
            print("failed!")
            return

    print(storage_values)


def tset_csv(file_name, name_array):
    data_csv = pd.read_csv(file_name)

    print(data_csv['File'][0])
    print(len(data_csv['File']))

    all_array = []
    indicator_cols = ['File', 'NP', 'MDD', 'Ppt', 'TT', 'ShR', 'Days', 'D_WR', 'D_STD', 'D_MaxNP',
                      'D_MinNP', 'maxW_Days', 'maxL_Days', 'mDD_mC', 'mDD_iC', 'LVRG', 'LVRG_Mon_NP']

    for i in range(len(name_array)):
        temp = []
        for j in range(len(data_csv['File'])):
            if name_array[i] == data_csv['File'][j]:
                temp.append(data_csv['NP'])



path = '/opt/wfo_data_csv/youxian_canshu/rb_zs_TB_WOBV_DX_QG_GZ_ADC_DK_20180323-102633_grid.yaml/0indicator_statistics.csv'

# get_csv('newfile/0indicator_statistics.csv')

# work_setting_select('test_make_setting.yaml')
# select_csv_from_setting_yaml('test_make_setting.yaml', path)

# test_unit6()
# tset_csv('one_indicators_test.csv')
