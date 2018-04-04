# coding=utf-8
import time
import MySQLdb as mdb
import numpy as np
import sys
import os
import pwd
import get_index
import yaml
import yaml_get as yl
import yaml_db as test_db
import for_all_species_manage as fm
import select_indicator as sir

'''
test time:2018/3/27
the first index 16 success!
the next is change the table that need select 

'''

'''
test time:2018/3/27 16:47 add the select part!
the first index 16 success!
the next is change the table that need select 
'''

'''part 0'''
# The comment
'''
(!*_*!) show you are pig!
(!-_-!) show not good!
(-_-) show normal!
(^_^) show good!
(\^_^/) show perfect!
'''

'''part 1'''


# just for test
def storage_value(table_name):
    if 2 != len(sys.argv):
        exit(-1)

    dir_name = sys.argv[1]
    file_names = os.listdir(dir_name)
    for file in file_names:
        if file[-4:] == '.csv':
            s = file
            val1, val2, val3, val4, val5, val6, val7, val8 \
                = np.loadtxt("%s" % s, delimiter=',', usecols=(0, 1, 2, 3, 4, 5, 6, 7), unpack=True)
            # values = [val1, val2, val3, val4, val5, val6, val7, val8]
    for i in range(len(val1[:10])):
        values = [val1[i], val2[i], val3[i], val4[i], val5[i], val6[i], val7[i], val8[i]]
        insert_values(values, table_name)


'''
def insert_values(values, table_name):
    conn = mdb.connect(host='127.0.0.1', user='root', passwd='7ondr', db='test')
    sql1="insert into "+table_name+"(a, b, c1, d, e, f, g, h) values("+str(values[0])+","+str(values[1])+","+str(values[2])+","+str(values[3])+","+str(values[4])+","\
    +str(values[5]) + ","+str(values[6])+","+str(values[7])+")"
    cursor = conn.cursor()
    cursor.execute(sql1)
    conn.commit()
    conn.close()
'''


def connect_database(message):
    host_id, user_name, password, db_name = message
    conn = mdb.connect(host=host_id, user=user_name, passwd=password, db=db_name)
    return conn


'''part 2'''


# a way to deal name (-_-)
def deal_name(name):
    le = len(name)
    file_name = name
    str1 = ''
    site = 0
    for i in range(le):
        if name[i] == '.':
            str1 = file_name[:i]
            site = i + 1
            break

    # le1 = len(file_name)
    for i in range(site, le):
        if name[i] == "K":
            if name[i:i + 4] == 'KBar':
                str1 += file_name[i:-4]
    return str1.replace('-', '_')


# (-_-)
def storage_name(name, table_name, message, c_id=0):
    sql = "insert into %s(name1, fcsv_id) VALUES('%s', %d) " % (table_name, name, c_id)
    print(sql)
    conn = connect_database(message)
    # conn = mdb.connect(host='127.0.0.1', user='root', passwd='7ondr', db='test')
    cursor = conn.cursor()
    cursor.execute(sql)
    conn.commit()
    conn.close()


# use for name
# get the stop loss and point value
def get_name_stoploss_pointvalue(table, message):
    sql = "select name1, stop_loss, point_value from %s" % table
    conn = connect_database(message)
    # conn = mdb.connect(host='127.0.0.1', user='root', passwd='7ondr', db='test')
    cursor = conn.cursor()
    cursor.execute(sql)
    val = []
    values = cursor.fetchall()
    # print values
    for i in range(len(values)):
        val.append(values[i])
    # val1 = list(values)
    # print val
    conn.close()
    return val


def get_name(table, message):
    sql = "select name1 from %s" % table
    conn = connect_database(message)
    # conn = mdb.connect(host='127.0.0.1', user='root', passwd='7ondr', db='test')
    cursor = conn.cursor()
    cursor.execute(sql)
    val = []
    values = cursor.fetchall()
    # print values
    for i in range(len(values)):
        val.append(values[i][0])
    # val1 = list(values)
    # print val
    conn.close()
    return val


def csv_to_database(message, dir_name):
    '''
    if 2 != len(sys.argv):
        exit(-1)
    '''
    t = time.time()
    # dir_name = 'H:\PYFILE\Tf\j9000'
    file_names = os.listdir(dir_name)
    t2 = 0
    for file in file_names:
        if file[-4:] == '.csv':
            s = dir_name + '/' + file
            name = file[:-4]
            name1 = deal_name(name)
            print(name1)
            create_table(name1, message)
            storage(name1, s, message)
            storage_name(name1, 'names', message)
            t1 = time.time() - t
            print(t2, t1)
            t2 += 1


# test success!
def get_raw_data(table_all, dir_name, table, table2, message):
    is_yaml = 0
    file_name = ""
    file_names = os.listdir(dir_name)  # get the file names in dir_name
    # just ready for a file have one yaml file, if need more Please find all
    for file_s in file_names:
        if file_s[-5:] == '.yaml':
            file_name = file_s
            is_yaml = 1
            break
    if is_yaml == 0:
        print("Can't find the yaml file!\n")
        return
    file_name = dir_name + '/' + file_name
    print("Please wait to get the yaml data! name:\t %s" % file_name)
    g_t1 = time.time()
    f_value = open(file_name)
    raw_values = yaml.load(f_value)
    g_t2 = time.time()

    '''
        在这里需要加一个进行全局管理的函数将names 和 yaml 存入其中
        同时 这两者的名字处理需要设定标准
    '''

    fm.storage_values_specie(table_all, table, table2, 'Likeyo', message, file_name)

    print("Load yaml over! Cost time: %.3f" % (g_t2 - g_t1))
    raw_values = raw_values
    g_all_length = len(raw_values)
    # print g_all_length
    print("Now read the data to move to database.")
    for i in range(2, g_all_length):
        value_temp = yl.deal_val_strategy(raw_values[i])
        temp_id = test_db.insert_yaml(table2, message, value_temp)
        print("The insert value's csv_file name: %s " % value_temp[10])  # 0 1 2 3 4 5 6 7 8 9 10
        if value_temp[10] in file_names:
            s = dir_name + '/' + value_temp[10]
            name = value_temp[10][:-4]
            name1 = deal_name(name)
            print(name1)
            create_table(name1, message)
            storage(name1, s, message)
            storage_name(name1, table, message, temp_id)
    print("Now make the single sort name table full!\n")
    insert_index_names_16(table, message)


# change at 2018/3/28 16:29
# add at 2018/3/27
def get_raw_data_selected_good(table_all, dir_name, message, file_name_vals):
    is_yaml = 0
    file_name = ""
    file_names = os.listdir(dir_name)  # get the file names in dir_name
    # just ready for a file have one yaml file, if need more Please find all
    for file_s in file_names:
        if file_s[-5:] == '.yaml':
            file_name = file_s
            is_yaml = 1
            break
    if is_yaml == 0:
        return
    print(file_name)
    print('success!')

    '''
    here need a name to make sure the single sort!
    '''
    file_temp_name = file_name
    # make the table standard table = specie_table_name, table2 = specie_table_yaml
    file_temp_name = get_name_yaml_table_name(file_temp_name)
    specie_table_name = file_temp_name + '_name'
    specie_table_yaml = file_temp_name + '_yaml'
    table = specie_table_name
    table2 = specie_table_yaml
    file_name = dir_name + '/' + file_name

    print("Create yaml_table: ")
    test_db.create_yaml_table(table2, message)  # 1
    print("Create yaml_table success!")
    print("Create name_table:")
    create_names_table(table, table2, message)  # 2
    print("Create name_table success!")

    print("Please wait to get the yaml data! name:\t %s" % file_name)
    g_t1 = time.time()
    f_value = open(file_name)
    raw_values = yaml.load(f_value)
    g_t2 = time.time()
    print("Load yaml over! Cost time: %.3f" % (g_t2 - g_t1))


    fm.storage_values_specie_new_test(table_all, table, table2, 'Likeyo', message, file_name, raw_values[:2])
    raw_values = raw_values
    g_all_length = len(raw_values)
    # print g_all_length
    print("\nNow read the data to move to database.\n")
    flag = 0
    # in here create csv table and storage it, make name's can do it and yaml tables have it's information
    for i in range(2, g_all_length):
        value_temp = yl.deal_val_strategy(raw_values[i])

        if value_temp[10] in file_name_vals:
            print("The flag: ", flag)
            temp_id = test_db.insert_yaml(table2, message, value_temp)
            print("The insert value's csv_file name: %s " % value_temp[10])  # 0 1 2 3 4 5 6 7 8 9 10
            if value_temp[10] in file_names:
                s = dir_name + '/' + value_temp[10]
                name = value_temp[10][:-4]
                name1 = deal_name(name)
                print(name1)
                create_table(name1, message)
                storage(name1, s, message)
                storage_name(name1, table, message, temp_id)
                print
                "\n\n"
    insert_index_names_16(table, message)


def insert_index_names(table, message):
    names = get_name(table, message)
    '''
    sql = "insert into %s(NP, MDD, PpT, TT, ShR, Days, " \
          "D_WR, D_STD, D_MaxNP, D_MinNP, maxW_Days, maxL_Days, mDD_mC, mDD_iC)" \
          "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    '''
    print(names)
    num = 0

    for name in names:
        t1 = time.time()
        val = get_index.get_index14(name, message)  # get the index of table name
        # val = []
        t3 = time.time()
        print('single 1 time:\t', (t3 - t1))
        sql = "insert into " + table + "(NP, MDD, PpT, TT, ShR, Days, " \
                                       "D_WR, D_STD, D_MaxNP, D_MinNP, maxW_Days, maxL_Days, mDD_mC, mDD_iC)" \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)" % (
                                           val[0], val[0], val[0], val[0], val[0], \
                                           val[0], val[0], val[0], val[0], val[0], \
                                           val[0], val[0], val[0], val[0])
        sql1 = "update " + table + " set NP=%s, MDD=%s, PpT=%s, TT=%s, ShR=%s, Days=%s, D_WR=%s, D_STD=%s, D_MaxNP=%s," \
                                   " D_MinNP=%s, maxW_Days=%s, maxL_Days=%s, mDD_mC=%s, mDD_iC=%s where name='%s'" % (
                                       val[0], val[1], val[2], val[3], val[4], \
                                       val[5], val[6], val[7], val[8], val[9], \
                                       val[10], val[11], val[12], val[13], name)

        conn = connect_database(message)
        cursor = conn.cursor()
        cursor.execute(sql1)
        conn.commit()
        cursor.close()
        conn.close()
        t2 = time.time()
        print("num: %d\t" % num, (t2 - t1))

        print(sql1)
        num += 1


# new 2018/3/24
# change
# right  :2018/3/24 20:51
def insert_index_names_16(table, message):
    name_two_value = get_name_stoploss_pointvalue(table, message)
    # names = get_name(table, message)
    '''
    sql = "insert into %s(NP, MDD, PpT, TT, ShR, Days, " \
          "D_WR, D_STD, D_MaxNP, D_MinNP, maxW_Days, maxL_Days, mDD_mC, mDD_iC)" \
          "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    '''

    num = 0

    for val_3 in name_two_value:
        t1 = time.time()

        # get the index of table name it need a change!
        val = get_index.get_index16(val_3[0], message, val_3[1], val_3[2])
        # val = []
        t3 = time.time()
        operator = pwd.getpwuid(os.getuid())[0]
        print('single 1 time:\t', (t3 - t1))
        sql = "insert into " + table + "(NP, MDD, PpT, TT, ShR, Days, " \
                                       "D_WR, D_STD, D_MaxNP, D_MinNP, maxW_Days, maxL_Days, mDD_mC, mDD_iC)" \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)" % (
                                           val[0], val[0], val[0], val[0], val[0], \
                                           val[0], val[0], val[0], val[0], val[0], \
                                           val[0], val[0], val[0], val[0])

        sql2 = "update " + table + " set NP=%s, MDD=%s, PpT=%s, TT=%s, ShR=%s, Days=%s, D_WR=%s, D_STD=%s, " \
                                   "D_MaxNP=%s, D_MinNP=%s, maxW_Days=%s, maxL_Days=%s, mDD_mC=%s, mDD_iC=%s, " \
                                   "LVRG=%s, LVRG_Mon_NP=%s, origin_operator='%s', select_operator='%s' where name1='%s'" % \
                                                                                (val[0], val[1], val[2], val[3], val[4],
                                                                                 val[5], val[6], val[7], val[8], val[9],
                                                                                 val[10], val[11], val[12], val[13],
                                                                                 val[14], val[15], operator,
                                                                                 operator, val_3[0])

        sql1 = "update " + table + " set NP=%s, MDD=%s, PpT=%s, TT=%s, ShR=%s, Days=%s, D_WR=%s, D_STD=%s, D_MaxNP=%s," \
                                   " D_MinNP=%s, maxW_Days=%s, maxL_Days=%s, mDD_mC=%s, mDD_iC=%s where name1='%s'" % (
                                       val[0], val[1], val[2], val[3], val[4], \
                                       val[5], val[6], val[7], val[8], val[9], \
                                       val[10], val[11], val[12], val[13], val_3[0])

        conn = connect_database(message)
        cursor = conn.cursor()
        cursor.execute(sql2)
        conn.commit()
        cursor.close()
        conn.close()
        t2 = time.time()
        print("num: %d\t" % num, (t2 - t1))

        print(sql2)
        num += 1


# a big csv to database;
# single one
# (-_-) 10s for 27level, two 1:np.load 3 two 2: pd.read_sql 7
def storage(table_name, file_name, message):
    val1, val2, val3, val4, val5, val6, val7, val8 \
        = np.loadtxt(file_name, delimiter=',', usecols=(0, 1, 2, 3, 4, 5, 6, 7), unpack=True)
    t1 = "insert into %s (a, b, c1, d, e, f, g, h)" % table_name
    print(t1)
    length = len(val1)
    print(length)
    number = 45000
    n = length / number
    n = int(n)

    '''
    if n > 0:
        return
    '''
    if n > 0:
        num = 0
        for j in range(n):
            params = []
            num += number
            conn = connect_database(message)
            # conn = mdb.connect(host='127.0.0.1', user='root', passwd='7ondr', db='test')
            for i in range(j * number, (j + 1) * number):
                values = [val1[i], val2[i], val3[i], val4[i], val5[i], val6[i], val7[i], val8[i]]
                params.append(values)
            sql = t1 + " values(%s, %s, %s, %s, %s, %s, %s, %s)"
            cursor = conn.cursor()
            cursor.executemany(sql, params)
            conn.commit()
            cursor.close()
            conn.close()

        # l2 = len(val1[n * number:])

        params = []
        conn = connect_database(message)
        # conn = mdb.connect(host='127.0.0.1', user='root', passwd='7ondr', db='test')
        for i in range(n * number, len(val1)):
            values = [val1[i], val2[i], val3[i], val4[i], val5[i], val6[i], val7[i], val8[i]]
            params.append(values)
        sql = t1 + " values(%s, %s, %s, %s, %s, %s, %s, %s)"
        cursor = conn.cursor()
        cursor.executemany(sql, params)
        conn.commit()
        cursor.close()
        conn.close()
    else:
        params = []
        conn = connect_database(message)
        # conn = mdb.connect(host='127.0.0.1', user='root', passwd='7ondr', db='test')
        for i in range(len(val1)):
            values = [val1[i], val2[i], val3[i], val4[i], val5[i], val6[i], val7[i], val8[i]]
            params.append(values)
        sql = t1 + " values(%s, %s, %s, %s, %s, %s, %s, %s)"
        cursor = conn.cursor()
        cursor.executemany(sql, params)
        conn.commit()
        cursor.close()
        conn.close()


'''part 2'''


# just for test the storage part
# (!*_*!)
def read_buck(params, table):
    try:
        print(len(params))
        t1 = "insert into %s (a, b, c1, d, e, f, g, h)" % table
        sql = t1 + " values(%s, %s, %s, %s, %s, %s, %s, %s)"
        print(sql)
        conn = mdb.connect(host='127.0.0.1', user='root', passwd='7ondr', db='test')
        conn.cursor().executemany(sql, params)
        conn.cursor().close()
        conn.close()
    except Exception as e:
        print(e)


def test_read(table_name, file_name):
    val1, val2, val3, val4, val5, val6, val7, val8 \
        = np.loadtxt(file_name, delimiter=',', usecols=(0, 1, 2, 3, 4, 5, 6, 7), unpack=True)
    length = len(val1)
    print(length)
    number = 50000
    n = length / number
    print(n)
    if n > 0:
        num = 0
        for j in range(n):
            params = []
            num += number
            for i in range(len(val1[j * number:(j + 1) * number])):
                values = [str(val1[i]), str(val2[i]), str(val3[i]), str(val4[i]), str(val5[i]), str(val6[i]),
                          str(val7[i]), str(val8[i])]
                params.append(values)
            read_buck(params, table_name)

        l2 = len(val1[n * number:])
        # print l2
        # print num
        params = []
        for i in range(len(val1[n * number:])):
            values = [val1[i], val2[i], val3[i], val4[i], val5[i], val6[i], val7[i], val8[i]]
            params.append(values)
        read_buck(params, table_name)

    else:
        params = []
        for i in range(len(val1)):
            values = [val1[i], val2[i], val3[i], val4[i], val5[i], val6[i], val7[i], val8[i]]
            params.append(values)
        read_buck(params, table_name)


'''part 3'''


def create_names_table(name, table_f, message):
    conn = connect_database(message)
    # conn = mdb.connect(host='127.0.0.1', user='root', passwd='7ondr', db='test')
    sql = "create table %s(name_id int not null AUTO_INCREMENT," \
          "name1 varchar(200) not null," \
          "NP FLOAT DEFAULT 0.0," \
          "MDD  FLOAT DEFAULT 0.0," \
          "mDD_mC FLOAT DEFAULT 0.0," \
          "mDD_iC FLOAT DEFAULT 0.0," \
          "LVRG FLOAT DEFAULT 0.0," \
          "LVRG_Mon_NP FLOAT DEFAULT 0.0," \
          "PpT  FLOAT DEFAULT 0.0," \
          "TT FLOAT DEFAULT 0.0," \
          "ShR  FLOAT DEFAULT 0.0," \
          "Days FLOAT DEFAULT 0.0," \
          "D_WR FLOAT DEFAULT 0.0," \
          "D_STD FLOAT DEFAULT 0.0," \
          "D_MaxNP FLOAT DEFAULT 0.0," \
          "D_MinNP FLOAT DEFAULT 0.0," \
          "maxW_Days FLOAT DEFAULT 0.0," \
          "maxL_Days FLOAT DEFAULT 0.0," \
          "stop_loss FLOAT DEFAULT 20," \
          "point_value FLOAT DEFAULT 5," \
          "change_select_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP() ON UPDATE CURRENT_TIMESTAMP," \
          "origin_operator VARCHAR(36) DEFAULT 'Likeyo'," \
          "select_operator VARCHAR(36) DEFAULT 'NO_ONE'," \
          "fcsv_id int DEFAULT 0," \
          "PRIMARY KEY (name_id)," \
          "constraint FK_fcsv_id foreign key(fcsv_id) references %s(csv1_id))" % (name, table_f)

    cursor = conn.cursor()
    cursor.execute(sql)
    conn.commit()
    conn.close()


# just create the basic csv table
def create_table(name, message):
    conn = connect_database(message)
    # conn = mdb.connect(host='127.0.0.1', user='root', passwd='7ondr', db='test')
    sql0 = "if not exists(select * from sys.Tables where name='" + name + "') "
    sql = "CREATE TABLE " + name + "(csv_id int NOT NULL AUTO_INCREMENT,A  int NOT NULL," \
                                   "B  int NOT NULL," \
                                   "C1  int NOT NULL," \
                                   "D  int NOT NULL," \
                                   "E  int NOT NULL," \
                                   "F  int NOT NULL," \
                                   "G  int NOT NULL," \
                                   "H  int NOT NULL," \
                                   "PRIMARY KEY ( csv_id )" \
                                   ")"
    su = sql0 + sql
    # print su
    sql2 = "CREATE TABLE IF NOT EXISTS " + name + \
           "(csv_id int NOT NULL AUTO_INCREMENT," \
           "A  int NOT NULL," \
           "B  int NOT NULL," \
           "C1  float NOT NULL," \
           "D  int NOT NULL," \
           "E  int NOT NULL," \
           "F  float NOT NULL," \
           "G  int NOT NULL," \
           "H  int NOT NULL," \
           "PRIMARY KEY ( csv_id )) "

    sql3 = "CREATE TABLE " + "test-112" + \
           "(csv_id int NOT NULL AUTO_INCREMENT," \
           "A  int NOT NULL," \
           "B  int NOT NULL," \
           "C1  float NOT NULL," \
           "D  int NOT NULL," \
           "E  int NOT NULL," \
           "F  float NOT NULL," \
           "G  int NOT NULL," \
           "H  int NOT NULL," \
           "PRIMARY KEY ( csv_id ))"

    cursor = conn.cursor()
    cursor.execute(sql2)
    conn.close()


def create_index_table():
    pass


'''part 4'''


# using
def insert_index_names1(table, message):
    names = get_name(table, message)
    '''
    sql = "insert into %s(NP, MDD, PpT, TT, ShR, Days, " \
          "D_WR, D_STD, D_MaxNP, D_MinNP, maxW_Days, maxL_Days, mDD_mC, mDD_iC)" \
          "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    '''
    print(names)
    for name in names:
        val = test6.plot_result1(name)  # get the index of table name
        val = []
        sql = "insert into %s(NP, MDD, PpT, TT, ShR, Days, " \
              "D_WR, D_STD, D_MaxNP, D_MinNP, maxW_Days, maxL_Days, mDD_mC, mDD_iC)" \
              "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)" % (
              name, val[0], val[0], val[0], val[0], val[0], \
              val[0], val[0], val[0], val[0], val[0], \
              val[0], val[0], val[0], val[0])
        print(sql)


# just test
def insert_values(values, table_name):
    conn = mdb.connect(host='127.0.0.1', user='root', passwd='7ondr', db='test')
    sql1 = "insert into " + table_name + "(a, b, c1, d, e, f, g, h) values(" + str(values[0]) + "," + str(
        values[1]) + "," + str(values[2]) + "," + str(values[3]) + "," + str(values[4]) + "," \
           + str(values[5]) + "," + str(values[6]) + "," + str(values[7]) + ")"
    cursor = conn.cursor()
    cursor.execute(sql1)
    conn.commit()
    conn.close()


'''part 5'''


# just for test get the value from mysql  (!-_-!)
def get_values(table_name, message):
    conn = connect_database(message)
    # conn = mdb.connect(host='127.0.0.1', user='root', passwd='7ondr', db='test')
    sql2 = 'select a, b, c1, d, e, f, g, h from ' + table_name
    cursor = conn.cursor()
    cursor.execute(sql2)
    values = cursor.fetchall()
    val1 = []
    val2 = []
    val3 = []
    val4 = []
    val5 = []
    val6 = []
    val7 = []
    val8 = []

    # print values
    for row in values:
        val1.append(row[0])
        val2.append(row[1])
        val3.append(row[2])
        val4.append(row[3])
        val5.append(row[4])
        val6.append(row[5])
        val7.append(row[6])
        val8.append(row[7])
    conn.close()
    # print val1


'''part 6'''


# delete the pack tables in a name table
def delete_csv_table(table, message):
    names = get_name(table, message)
    le = len(names)
    print(le)
    if le > 0:
        print(names)
        for i in range(le):
            database = connect_database(message)
            delete_table(names[i], database)
        database = connect_database(message)
        delete_value('names', database)
    else:
        print("Empty!")


def delete_value(table, database):
    try:
        # conn = mdb.connect(host='127.0.0.1', user='root', passwd='7ondr', db='test')
        conn = connect_database(database)
        sql = 'TRUNCATE TABLE ' + table
        print(sql)
        cursor = conn.cursor()
        cursor.execute(sql)
        conn.commit()
        conn.close()
    except Exception as e:
        print("TRUNCATE TABLE: %s\t" % table, e)


def delete_table(table_name, database):
    try:
        # conn = mdb.connect(host='127.0.0.1', user='root', passwd='7ondr', db='test')
        conn = database
        sql = 'drop table ' + table_name
        cursor = conn.cursor()
        cursor.execute(sql)
        conn.commit()
        conn.close()
    except Exception as e:
        print("drop table: %s\t" % table_name, e)


def test_connect_database(message):
    conn = connect_database(message)
    sql = 'show tables'
    cursor = conn.cursor()
    cursor.execute(sql)
    val = cursor.fetchall()
    conn.close()
    print(val)


def make_one_sort_function(table1, table2, message, dir_name):
    test_db.create_yaml_table(table2, message)
    create_names_table(table1, table2, message)
    get_raw_data(dir_name, table1, table2, message)


def delete_names_csv(table, message):
    print("Do you want to delete all the basic csv table in your database!(Y/N)")

    answer = raw_input("Please input your choose:(Y/N)")
    answer = answer.upper()
    if answer != 'Y':
        print("Your information is not deleted in this step!")
        return

    delete_csv_table(table, message)
    # delete_value(table, message)
    print("Delete success!")


# add 2018/3/28 16:28 china
# for the name
def get_name_yaml_table_name(str):
    flag = 7
    index = 7
    str = str[:-5]
    str1 = str[:8]
    # str1 += str[7]
    for i in str[8:]:
        flag += 1
        if i.islower():
            str1 += i
            index += 1
            continue
        if i == '_':
            if str1[index].isupper() and str[flag + 1].isupper():
                continue
            else:
                str1 += i
                index += 1
                continue
        if i.isupper():
            if str[flag - 1] == '_':
                str1 += i
                index += 1
                continue

        else:
            str1 += i
            index += 1
    str1 = str1.replace('-', "_")
    str1 = str1.replace('.', "_")

    return str1


def select_csv_to_database(table0, file_name, message):
    ls = len(file_name)
    site = 0
    for i in range(ls):
        if file_name[ls - i - 1] == '/':
            site = i
            break
    if site == 0:
        print("The file_name is wrong!")
        return
    dir_name = file_name[: ls - site - 1]
    print(dir_name)
    vals = sir.select_csv_from_setting_yaml('test_make_setting.yaml', file_name)
    print(len(vals), "\n")
    get_raw_data_selected_good(table0, dir_name, message, vals)


def select_csv_to_database_sys(table0, file_name, message, setting_yaml):
    ls = len(file_name)
    site = 0
    for i in range(ls):
        if file_name[ls - i - 1] == '/':
            site = i
            break
    if site == 0:
        print("The file_name is wrong!")
        return
    dir_name = file_name[: ls - site - 1]
    print(dir_name)
    vals = sir.select_csv_from_setting_yaml(setting_yaml, file_name)
    print(len(vals), "\n")
    get_raw_data_selected_good(table0, dir_name, message, vals)


def change_database(message, sql):
    conn = connect_database(message)

    cursor = conn.cursor()
    cursor.execute(sql)
    conn.close()


dir_name = '/home/liziqiang/PycharmProjects/untitled/j9000'
dir_name1 = '/home/liziqiang/Desktop/SHOWyou/j_zs_WH_WOBV_DX_QG_GZ_20180102-81542_grid.yaml'
test_dir1 = '/public/home/liziqiang/Desktop/j_zs_WH_WOBV_DX_QG_GZ_20180102-81542_grid.yaml'
message = ['127.0.0.1', 'root', '7ondr', 'testpy']
message3 = ["10.25.10.249", "huacheng", "huacheng123", "LI_DataBase"]
message1 = ["10.25.10.249", "huacheng", "huacheng123", "LI_DataBase_ver1"]
message2 = ["10.25.10.249", "huacheng", "huacheng123", "LI_DataBase_ver2"]  # Li_test1
message4 = ["10.25.10.249", "huacheng", "huacheng123", "Li_test1"]
message5 = ["192.168.1.2", "root", "rootKa$QZ", "LI_DataBase_ver2"]
message_new = ["192.168.1.2", "root", "rootKa$QZ", "LI_DataBase_ver1"]
message_new1 = ["192.168.1.2", "root", "rootKa$QZ", "LI_DataBase_ver3"]

table0 = 'all_species_table_test1'
table2 = 'test_yaml_table'
table1 = 'test_names'

# fm.create_all_names(table0, message_new1)
# test_connect_database(message1)
# create_table('csv_table', message1)
# test_db.create_yaml_table(table2, message5)  # 1
# create_names_table(table1, table2, message5)  # 2
# storage('csv_table', 'test.csv', message1)
# csv_to_database(message1, dir_name1)

# insert_index_names(table1, message2)  # success!
# delete_csv_table('names', message3)
# get_raw_data(dir_name1, table1, table2, message2)  # 3
# get_raw_data(table0, test_dir1, table1, table2, message5)
# delete_names_csv(table1, message4)
# make_one_sort_function(table1, table2, message4, test_dir1)

# path = '/opt/wfo_data_csv/youxian_canshu/rb_zs_TB_WOBV_DX_QG_GZ_ADC_DK_20180323-102633_grid.yaml/0indicator_statistics.csv'
# path = '/public/home/liziqiang/Desktop/test1forall/rb_zs_TB_WOBV_DX_QG_GZ_ADC_DK_20180323-102633_grid.yaml/'
path = '/public/home/liziqiang/Desktop/j_zs_WH_WOBV_DX_QG_GZ_20180102-81542_grid.yaml/one_indicators_test12.csv'
vals = ['SJ7ZNE.WOBV_DX_QG_GZ_ADC_DK_KBar_20m-_20150105-20180102_9th_bkt.csv',
        'SJ7ZNE.WOBV_DX_QG_GZ_ADC_DK_KBar_22m-_20150105-20180102_11th_bkt.csv']
# get_raw_data_selected_good(table0, test_dir1, table1, table2, message4, vals)
# select_csv_to_database(table0, path, message_new1)

# sql = 'update all_species_table_test1 set specie_table_yaml="OCBJ5Y_rb_zs_TWDQGAD_20180323_102633_grid_yaml",' \
#      ' specie_table_name="OCBJ5Y_rb_zs_TWDQGAD_20180323_102633_grid_name" where specie_id=1'

# change_database(message5, sql)
# insert_index_names_16("EBTL20_j_zs_WWDQG_20180102_81542_grid_name", message_new1)


# used for the terminal
def do_sys():
    pass
