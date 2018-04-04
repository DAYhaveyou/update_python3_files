# coding=utf-8
import MySQLdb as mdb
import os
import getpass
import select_csv_and_combination as scc
import datetime
import yaml_get as yt
import json
import pwd


def connect_database(message):
    host_id, user_name, password, db_name = message
    conn = mdb.connect(host=host_id, user=user_name, passwd=password, db=db_name)
    return conn


def create_companation_indicator(table, message):
    conn = connect_database(message)

    sql = "create table %s(combination_id int not null AUTO_INCREMENT, " \
          "combination_name VARCHAR(100) not null," \
          "address_json text not null," \
          "address_portfolio_csv text not null," \
          "address_png text not null," \
          "operator VARCHAR(100) not null," \
          "time_real VARCHAR(100) not null," \
          "PRIMARY KEY (combination_id))" % table

    cursor = conn.cursor()
    cursor.execute(sql)
    conn.commit()
    conn.close()


def make_json_value(table, table_array, message, indicator='ShR'):
    name_table = table[0]
    yaml_table = table[1]
    indicator = 'ShR'
    storate_val = []
    ratio = (1.0 / len(table_array))
    for i in range(len(table_array)):
        table_name = table_array[i]
        sql1 = "select " + indicator + ", " + "Config_KBarPeriod_KBarTimeMin, optimum_period, strategy from " \
               + name_table + ", " + yaml_table + " where name1='" + table_name + "' and " \
               + name_table + ".fcsv_id=" + yaml_table + ".csv1_id"
        conn = connect_database(message)
        cursor = conn.cursor()
        cursor.execute(sql1)
        val1 = cursor.fetchone()
        conn.close()
        val2 = []
        val2.append(table_array[i])
        for j in range(len(val1)):
            if j == 2:
                val2.append(yt.deal_data5_of_unknown(val1[j]))
                continue
            val2.append(val1[j])
        val2.append(ratio)
        storate_val.append(val2)
    print(storate_val)
    return storate_val


# used for json make AC
def make_json(array, indicators, json_name, indicator='ShR'):

    f = open(json_name, 'w')

    comment = "The comment is about Values. " \
              "Each column represents: name, indicator value, KBar time, Period, Strategy, Ratio. " \
              "Each row represents: The value of each property of the csv file you choose."
    dict_1 = {}
    dict_2 = {}
    dict_2['NP'] = indicators[0]
    dict_2['MDD'] = indicators[1]
    dict_2['ShR'] = indicators[2]
    dict_2['Ratio'] = indicators[3]
    dict_2['MDD_R'] = indicators[4]
    dict_1['Values'] = array
    dict_1['Indicator'] = dict_2
    dict_1['Comment'] = comment
    dict_1['Index'] = indicator
    dict_1['Current_time'] = '%s' % datetime.datetime.now()
    json.dump(dict_1, f, indent=6)

    f.close()
    print("Success!")


def make_portfolio_png_dir(dir_name, specie_name, indicator, table_array, message):
    if dir_name[-1] == "/":
        dir_name = dir_name[:-1]

    now = datetime.datetime.now()
    today = "%s" % datetime.date.today()
    today = today.replace('-', '')
    hour = "%02d" % now.hour

    minute = "%02d" % now.minute
    second = "%02d" % now.second
    second = "%s" % second

    L1 = "%s" % len(table_array)

    # storage into database;
    operator = pwd.getpwuid(os.getuid())[0]
    combination_name = indicator + '_' + specie_name + '_' + L1 + '_' + today + '_' + hour + minute + second + "_" \
                       + operator
    json_file_name = dir_name + '/' + indicator + '_' + specie_name + '_' + L1 + '_' + today + '_' + \
                     hour + minute + second + "_" + operator +".json"
    now_time = ("%s" % datetime.date.today()) + " " + hour + ":" + minute + ":" + second
    stand_name = combination_name
    # get the
    values = scc.call_it(dir_name, table_array, stand_name)
    portfolio_address = values[0]
    png_address = values[1][-1]

    # get the table need1
    tables = ['NP', 'LvRG', 'LvRG_Mon_NP', 'ShR']
    flag = 0
    for i in range(len(tables)):
        if indicator == tables[i]:
            flag = i
            break

    # storage information to database
    # message = ["10.25.10.249", "huacheng", "huacheng123", "LI_DataBase_ver2"]
    conn = connect_database(message)
    corsur = conn.cursor()
    sql = "insert into %s(combination_name, address_json, address_portfolio_csv, address_png, operator, time_real) " \
          "VALUES('%s', '%s', '%s', '%s', '%s', '%s')" % (tables[flag], combination_name, json_file_name,
                                                          portfolio_address, png_address, operator, now_time)
    corsur.execute(sql)
    conn.commit()
    conn.close()
    return json_file_name, values[1][:-1]


# used for delete the message storage in the database and file system of selected combination!
def remove_combination(table, message, combination_name):

    conn0 = connect_database(message)
    cursor0 = conn0.cursor()
    sql0 = "select combination_name from %s" % table
    cursor0.execute(sql0)
    conn0.commit()
    val0 = cursor0.fetchall()
    conn0.close()
    is_falg = 0
    for i in val0:
        if i[0] == combination_name:
            is_flag = 1
            break

    if is_falg == 0:
        return "Nothing in here!"

    conn = connect_database(message)
    cursor = conn.cursor()
    sql = "select address_json, address_portfolio_csv, address_png from %s WHERE combination_name='%s'" % \
          (table, combination_name)
    cursor.execute(sql)
    val = cursor.fetchone()
    cursor.close()
    conn.close()
    address_json = val[0]
    address_portfolio_csv = val[1]
    address_png = val[2]

    address_name = ['address_json', 'address_portfolio_csv', 'address_png']

    flags = [1, 1, 1]
    flag_delete = 0
    length = len(address_json)
    length_1 = len(address_name)
    flag = 0
    for i in range(length):
        if address_json[length-1-i] == "/":
            flag = i
            break
    dir_name = address_json[:length-1-flag]

    # remove the file in file system
    for i in range(3):
        if os.path.exists(val[i]):
            flag_delete += 1

    if flag_delete == 3:
        flag_delete = 0
        for i in range(length_1):
            try:
                os.remove(val[i])
                print("success remove %s!" % val[i])
                flag_delete += 1
            except:
                flags[i] = 0

        if flag_delete == 3:
            sql_1 = "delete from %s where combination_name='%s'" % (table, combination_name)
            try:
                do_sql(message, sql_1)
            except:
                print("Failed!--delete the value!")
        else:
            for i in range(length_1):
                if flags[i] == 1:
                    sql = "update %s set %s='None' where combination_name='%s'" % (table, address_name[i],
                                                                                 combination_name)
                    try:
                        do_sql(message, sql)
                        print("Delete %s success!" % val[i])
                    except:
                        print("Can't delete %s from the table:%s  and it's combination name is %s.\n Please delete "
                              "by your self!" % (val[i], table, combination_name))
    else:
        sql_1 = "delete from %s where combination_name='%s'" % (table, combination_name)
        try:
            do_sql(message, sql_1)
        except:
            print("Failed!--delete the value!")
        print("You delete nothing!")


def do_sql(message, sql):
    conn = connect_database(message)
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        conn.commit()
    except:
        print('!')
        conn.rollback()
    cursor.close()
    conn.close()


# Now we need give the message to them
# Them show should give me the table_array, dir_name, specie, indicator
def union_test(table_array, message, dir_name, specie, indicator):
    table = table_array[:2]
    table_array1 = table_array[2:]
    val1 = make_json_value(table, table_array1, message)
    val2 = make_portfolio_png_dir(dir_name, specie, indicator, table_array1, message)
    indicators = val2[1]
    # print(indicators)
    make_json(val1, indicators, val2[0], indicator)

table_array = ['test_name_new', 'yaml_table_test', 'sj7znekbar_20m__20150105_20180102_9th',
               'sj7znekbar_22m__20150105_20180102_11th',
               'sj7znekbar_22m__20150105_20180102_12th', 'sj7znekbar_24m__20150105_20180102_17th',
               'sj7znekbar_27m__20150105_20180102_22th']
message = ["localhost", "root", "7ondr", "testpy"]
message1 = ["192.168.1.2", "root", "rootKa$QZ", "LI_DataBase_ver3"]
name = 'NP_Jz_5_20180404_142618_liziqiang'

# remove_combination('NP', message1, name)
# union_test(table_array, message, 'H:/PYFILE/update_items_py3', 'Jz', 'NP')
print("hell0!")

path = '/public/home/liziqiang/Desktop/temp_file'

table_s = ['EBTL20_j_zs_WWDQG_20180102_81542_grid_name ', 'EBTL20_j_zs_WWDQG_20180102_81542_grid_yaml',
           'EBTL20KBar_27m__20170103_20180102_661th', 'EBTL20KBar_27m__20170103_20180102_662th',\
          'EBTL20KBar_27m__20170103_20180102_669th', 'EBTL20KBar_27m__20170103_20180102_670th',\
          'EBTL20KBar_27m__20170103_20180102_671th']

union_test(table_s, message1, path, 'Jz', 'NP')

# create_companation_indicator('NP', message1)
