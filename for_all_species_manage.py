# coding=utf-8
import MySQLdb as mdb
import yaml_get as yl
import os
import time
import yaml


def connect_database_test2(message):
    host_id, user_name, password, db_name = message
    conn = mdb.connect(host=host_id, user=user_name, passwd=password, db=db_name)
    return conn


# 头部信息参数, 显示配置头部信息
def test_head_information(val0, val1, val2):
    Data_DB = val1['Data']['DataBase']['DB']
    Data_PassWd = val1['Data']['DataBase']['PassWd']
    Data_Server = val1['Data']['DataBase']['Server']
    Data_User = val1['Data']['DataBase']['User']
    Data_Table = val1['Data']['DataBase']['Table']
    Data_Symbol_CommissionRatio = val1['Data']['Symbol']['CommissionRatio']
    Data_Symbol_Market = val1['Data']['Symbol']['Market']
    Data_Symbol_MktDataType = val1['Data']['Symbol']['MktDataType']
    Data_Symbol_Trade = val1['Data']['Symbol']['Trade']

    Setting_BktVfy = val2['Setting']['BktVfy']
    Setting_Objective_Fitness = val2['Setting']['Objective']['Fitness']
    Setting_Objective_ObjInSam = val2['Setting']['Objective']['ObjInSam']
    Setting_Objective_ObjOutSam = val2['Setting']['Objective']['ObjOutSam']
    Setting_Objective_ObjVldSam = val2['Setting']['Objective']['ObjVldSam']
    Setting_Objective_ScoreFormula = val2['Setting']['Objective']['ScoreFormula']
    Setting_OptCont = val2['Setting']['OptCont']
    Setting_OptEngine_SPSO_Epsilon = val2['Setting']['OptEngine']['SPSO']['Epsilon']
    Setting_OptEngine_SPSO_EvalMax = val2['Setting']['OptEngine']['SPSO']['EvalMax']
    Setting_OptEngine_SPSO_RunMax = val2['Setting']['OptEngine']['SPSO']['RunMax']
    Setting_OptResultSize = val2['Setting']['OptResultSize']
    Setting_SamRange_InSamStart = val2['Setting']['SamRange']['InSamStart']
    Setting_SamRange_OutSamRangeMonth = val2['Setting']['SamRange']['OutSamRangeMonth']
    Setting_SamRange_OutSamStart = val2['Setting']['SamRange']['OutSamStart']
    Setting_TradeLots = val2['Setting']['TradeLots']

    return val0[0], val0[3], Data_Table[0], val0[1], val0[4], val0[2], Data_DB, Data_PassWd, Data_Server, Data_Table, Data_User, \
           Data_Symbol_CommissionRatio, Data_Symbol_Market,\
           Data_Symbol_MktDataType, Data_Symbol_Trade, Setting_BktVfy, Setting_Objective_Fitness, \
           Setting_Objective_ObjInSam, Setting_Objective_ObjOutSam, Setting_Objective_ObjVldSam, \
           Setting_Objective_ScoreFormula, Setting_OptCont, Setting_OptEngine_SPSO_Epsilon, \
           Setting_OptEngine_SPSO_EvalMax, Setting_OptEngine_SPSO_RunMax, Setting_OptResultSize,\
           Setting_SamRange_InSamStart, Setting_SamRange_OutSamRangeMonth, Setting_SamRange_OutSamStart, \
           Setting_TradeLots


def do_head_new_yaml(val00, yaml_name, val1):
    # 1
    val0 = val00[1:]
    dict_data = {}
    dict_data['Data'] = ""
    dict_data_database_d = {}
    dict_data_database_d["DataBase"] = ""
    dict_data_database = {}
    dict_data_database['DB'] = val0[0]  # 1
    dict_data_database['PassWd'] = val0[1]
    dict_data_database['Server'] = val0[2]
    dict_data_database['Table'] = yl.deal_data5_of_unknown(val0[3])
    dict_data_database['User'] = val0[4]
    dict_data_database_d["DataBase"] = dict_data_database

    dict_data_symbol_dict = {}
    dict_data_symbol_dict["CommissionRatio"] = val0[5]
    dict_data_symbol_dict["Market"] = val0[6]
    dict_data_symbol_dict["MktDataType"] = val0[7]
    dict_data_symbol_dict["Trade"] = val0[8]
    dict_data_database_d["Symbol"] = dict_data_symbol_dict

    dict_data['Data'] = dict_data_database_d

    # 2
    dict_setting = {}
    dict_setting['Setting'] = ""

    dict_setting_val = {}
    dict_setting_val['BktVfy'] = val0[9]
    dict_setting_val['Objective'] = {}
    dict_setting_val['Objective']['Fitness'] = yl.deal_data5_of_unknown(val0[10])
    dict_setting_val['Objective']['ObjInSam'] = yl.deal_data5_of_unknown(val0[11])
    dict_setting_val['Objective']['ObjOutSam'] = yl.deal_data5_of_unknown(val0[12])
    dict_setting_val['Objective']['ObjVldSam'] = yl.deal_data5_of_unknown(val0[13])
    dict_setting_val['Objective']['ScoreFormula'] = val0[14]
    dict_setting_val['OptCont'] = val0[15]
    dict_setting_val['OptEngine'] = {}
    dict_setting_val['OptEngine']["SPSO"] = {}
    dict_setting_val['OptEngine']["SPSO"]['Epsilon'] = val0[16]
    dict_setting_val['OptEngine']["SPSO"]['EvalMax'] = val0[17]
    dict_setting_val['OptEngine']["SPSO"]['RunMax'] = val0[18]
    dict_setting_val['OptResultSize'] = int(val0[19])
    dict_setting_val['SamRange'] = {}
    dict_setting_val['SamRange']['InSamStart'] = int(val0[20])
    dict_setting_val['SamRange']['OutSamRangeMonth'] = int(val0[21])
    dict_setting_val['SamRange']['OutSamStart'] = int(val0[22])
    dict_setting_val['TradeLots'] = int(val0[23])
    dict_setting['Setting'] = dict_setting_val

    vals = []
    vals.append(dict_data)
    vals.append(dict_setting)

    # 3
    print(len(val1))
    if len(val1) != 0:
        for i in range(len(val1)):
            dict_3_values = [{}, {}, {}, {}, {}]
            print(i, val1[i][0])
            dict_3_values[0]['Strategy'] = val1[i][0]

            dict_3_values[1]['Config'] = {}
            dict_3_values[1]['Config']['WarmUp'] = val1[i][1]
            dict_3_values[1]['Config']['KBarPeriod'] = {}
            dict_3_values[1]['Config']['KBarPeriod']['KBarTimeMin'] = int(val1[i][2])
            dict_3_values[1]['Config']['Parameters'] = yl.deal_data5_of_unknown(val1[i][3])

            dict_3_values[2]['Optimum'] = yl.deal_data5_of_unknown(val1[i][4])

            dict_3_values[3]['backtest'] = yl.deal_data5_of_unknown(val1[i][5])

            dict_3_values[4]['bkt.csv'] = [val1[i][6]]
            vals.append(dict_3_values)

    f1 = open(yaml_name, 'w')

    yaml.dump(vals, f1)

    f1.close()

    print(vals)


# The first five pieces of information are self-contained
def create_all_names(table, message):
    # 加入了 yaml 和 name table 的名字以标注
    sql = "create table %s (specie_id int not null AUTO_INCREMENT," \
          "specie_yaml_name VARCHAR(100) not null, " \
          "specie_name VARCHAR(100) not null," \
          "specie_table_name VARCHAR(100) not null," \
          "specie_table_yaml VARCHAR(100) not null," \
          "import_time TIMESTAMP  DEFAULT CURRENT_TIMESTAMP() ," \
          "import_operator VARCHAR(36) not null DEFAULT 'your name', " \
          "DB VARCHAR(100) NOT NULL," \
          "PassWd VARCHAR(50) NOT NULL," \
          "Server VARCHAR(60) NOT NULL," \
          "Tablename VARCHAR(60) NOT NULL," \
          "DB_User VARCHAR(60) NOT NULL," \
          "Symbol_CR VARCHAR(10) NOT NULL," \
          "Symbol_M VARCHAR(30) NOT NULL," \
          "Symbol_MDT VARCHAR(30) NOT NULL," \
          "Symbol_T VARCHAR(30) NOT NULL," \
          "Setting_BV VARCHAR(10) NOT NULL," \
          "Setting_OBJ_Fitness Text NOT NULL," \
          "Setting_OBJ_insam VARCHAR(100) NOT NULL," \
          "Setting_OBJ_outsam VARCHAR(100) NOT NULL," \
          "Setting_OBJ_vlsam VARCHAR(100) NOT NULL," \
          "Setting_OBJ_SF VARCHAR(60) NOT NULL," \
          "Setting_OptCont VARCHAR(10) NOT NULL," \
          "Setting_OE_SPSO_E FLOAT NOT NULL," \
          "Setting_OE_SPSO_EM FLOAT NOT NULL," \
          "Setting_OE_SPSO_RM FLOAT NOT NULL," \
          "Setting_ORS int NOT NULL," \
          "Setting_SamRange_inSS int NOT NULL," \
          "Setting_SamRange_outSRangeMonth int NOT NULL," \
          "Setting_SamRange_outSS int NOT NULL," \
          "Setting_TradeLots int NOT NULL," \
          "PRIMARY KEY (specie_id))" % table

    conn = connect_database_test2(message)

    cursor = conn.cursor()
    cursor.execute(sql)
    conn.commit()
    conn.close()


def deal_dir_filename(name):
    file_name = ''

    for i in name:
        if i != '/' and i != '\\':
            file_name += i
        else:
            file_name = ''
    return file_name


# 进行存储一个品种
def storage_values_specie(table_all, table_yaml, table_child, operator, message, file_name):
    f = open(file_name, 'r')
    value = yaml.load(f)
    f.close()
    file_name = deal_dir_filename(file_name)
    val_add = [table_all, table_yaml, operator, file_name, table_child]
    get_value = test_head_information(val_add, value[0], value[1])  # get it correctly!
    print(len(get_value))
    '''
    for i in range(8):
        print get_value[i]
    '''

    sql = 'insert into %s (specie_yaml_name, specie_name, specie_table_yaml, specie_table_name, import_operator, DB,' \
          'PassWd, Server, Tablename, DB_User, Symbol_CR, Symbol_M, Symbol_MDT,' \
          'Symbol_T, Setting_BV, Setting_OBJ_Fitness, Setting_OBJ_insam, Setting_OBJ_outsam,' \
          'Setting_OBJ_vlsam, Setting_OBJ_SF, Setting_OptCont, Setting_OE_SPSO_E,' \
          'Setting_OE_SPSO_EM, Setting_OE_SPSO_RM, Setting_ORS, Setting_SamRange_inSS,' \
          'Setting_SamRange_outSRangeMonth, Setting_SamRange_outSS, Setting_TradeLots) ' \
          'VALUES("%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s",' \
          ' "%s","%s","%s", "%s", %s, %s, %s, %s, %s, %s, %s, %s)' % get_value
    print(sql)

    conn = connect_database_test2(message)
    cursor = conn.cursor()
    cursor.execute(sql)
    conn.commit()
    conn.close()


# change time 2018/3/28  17:11
def storage_values_specie_new_test(table_all, table_child, table_yaml, operator, message, file_name, value):
    file_name = deal_dir_filename(file_name)
    val_add = [table_all, table_child, operator, file_name, table_yaml]
    get_value = test_head_information(val_add, value[0], value[1])  # get it correctly!
    print(len(get_value))
    '''
    for i in range(8):
        print get_value[i]
    '''

    sql = 'insert into %s (specie_yaml_name, specie_name, specie_table_name, specie_table_yaml, import_operator, DB,' \
          'PassWd, Server, Tablename, DB_User, Symbol_CR, Symbol_M, Symbol_MDT,' \
          'Symbol_T, Setting_BV, Setting_OBJ_Fitness, Setting_OBJ_insam, Setting_OBJ_outsam,' \
          'Setting_OBJ_vlsam, Setting_OBJ_SF, Setting_OptCont, Setting_OE_SPSO_E,' \
          'Setting_OE_SPSO_EM, Setting_OE_SPSO_RM, Setting_ORS, Setting_SamRange_inSS,' \
          'Setting_SamRange_outSRangeMonth, Setting_SamRange_outSS, Setting_TradeLots) ' \
          'VALUES("%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s",' \
          ' "%s","%s","%s", "%s", %s, %s, %s, %s, %s, %s, %s, %s)' % get_value
    print(sql)

    conn = connect_database_test2(message)
    cursor = conn.cursor()
    cursor.execute(sql)
    conn.commit()
    conn.close()


def get_all_specie_yaml(table, message, id):
    sql = "select specie_yaml_name, specie_table_yaml, DB, PassWd, Server, Tablename, DB_User, Symbol_CR, Symbol_M, " \
          "Symbol_MDT, Symbol_T, Setting_BV, Setting_OBJ_Fitness, Setting_OBJ_insam, Setting_OBJ_outsam," \
          "Setting_OBJ_vlsam, Setting_OBJ_SF, Setting_OptCont, Setting_OE_SPSO_E," \
          "Setting_OE_SPSO_EM, Setting_OE_SPSO_RM, Setting_ORS, Setting_SamRange_inSS," \
          "Setting_SamRange_outSRangeMonth, Setting_SamRange_outSS, Setting_TradeLots from %s " \
          "WHERE specie_id=%s" % (table, id)

    conn = connect_database_test2(message)
    cursor = conn.cursor()
    cursor.execute(sql)
    raw_value = cursor.fetchall()
    conn.close()
    print(len(raw_value[0]))
    return raw_value[0]


def get_one_specie_yaml(table, message):
    sql = "select strategy, Config_WarmUp, Config_KBarPeriod_KBarTimeMin, Config_Parameters," \
          " optimum_values, back_test_value, csv_name from %s" % table

    conn = connect_database_test2(message)
    cursor = conn.cursor()
    cursor.execute(sql)
    raw_value = cursor.fetchall()
    conn.close()
    print(len(raw_value))
    return raw_value


'''
该函数运行成功， 
table： 为种类总表的内容
message： 为数据库信息， 密码等
id： 为总表里所选中的种类id
'''


# 还应该加入一个 路径名 在此未加载
def make_one_specie_yaml(table, message, specie_id):
    value_head = get_all_specie_yaml(table, message, specie_id)
    val1 = get_one_specie_yaml(value_head[1], message)
    # print(val1)
    do_head_new_yaml(value_head[1:], value_head[0], val1)
    # do_head_new_yaml(value_head[1:], 'test_less2.yaml', val1)

    # print("Success!")


table1 = 'all_species_table_test1'
message1 = ['127.0.0.1', 'root', '7ondr', 'testpy']

head_val = [{'Data': {'Symbol': {'MktDataType': 'KBar', 'CommissionRatio': 'n', 'Market': 'RB', 'Trade': 'RB'}, 'DataBase': {'PassWd': 'huacheng123', 'Table': ['rb_zs'], 'DB': 'huacheng', 'User': 'huacheng', 'Server': '10.25.10.249'}}}, {'Setting': {'SamRange': {'InSamStart': 20140101, 'OutSamStart': 20170101, 'OutSamRangeMonth': 3}, 'OptResultSize': 5, 'TradeLots': 10, 'Objective': {'ScoreFormula': 'LVRG_Mon_NP', 'ObjVldSam': [['NP/-MDD', 0.5]], 'Fitness': [['LVRG_Mon_NP', 0.2, 0], ['NP', 0]], 'ObjOutSam': [['NP', 100000]], 'ObjInSam': [['DD/Close', 1.2]]}, 'OptEngine': {'SPSO': {'Epsilon': 0.001, 'RunMax': 2, 'EvalMax': 100}}, 'OptCont': 'n', 'BktVfy': 'n'}}]
# create_all_names(table1, message1)
# storage_values_specie(table1, 'yaml_table', 'test_name', 'show_me', message1, 'test_less.yaml')  # success
# make_one_specie_yaml(table1, message1, 1)  # good!
# deal_dir_filename('i9000/test/do.yaml')