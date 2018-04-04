import MySQLdb as mdb


def connect_database(message):
    host_id, user_name, password, db_name = message
    conn = mdb.connect(host=host_id, user=user_name, passwd=password, db=db_name)
    return conn


def create_yaml_table(table, message):
    name = ''
    slq = "create table "+table+"(optimum_id int not null AUTO_INCREMENT," \
                               "csv_name varchar(200) not null," \
                               "year_period varchar(100) not null," \
                               "optimum_num int not null)"
    sql1 = "create table "+table+"(csv1_id int not null AUTO_INCREMENT," \
                                 "strategy varchar(100) not null," \
                                 "Config_WarmUp float not null," \
                                 "Config_KBarPeriod_KBarTimeMin int not null," \
                                 "Config_Parameters text not null," \
                                 "optimum_num int not null," \
                                 "optimum_period varchar(100) not null," \
                                 "optimum_values text not null," \
                                 "back_test_num int not null," \
                                 "back_test_period varchar(100) not null," \
                                 "back_test_value text not null," \
                                 "csv_name varchar(200) not null," \
                                 "PRIMARY KEY (csv1_id))"

    sql = "create table %s(name_id int not null AUTO_INCREMENT," \
          "name varchar(200) not null," \
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
          "PRIMARY KEY (name_id)," \
          ")" % name

    conn = connect_database(message)
    cursor = conn.cursor()
    cursor.execute(sql1)
    conn.commit()
    conn.close()


# no update
def create_names_tables(table, table1, message):
    # name = ''
    sql = "create table %s(name_id int not null AUTO_INCREMENT," \
          "name varchar(200) not null," \
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
          "origin_insert_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP()," \
          "change_select_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP()," \
          "origin_operator VARCHAR(36) DEFAULT 'Likeyo'," \
          "select_operator VARCHAR(36) DEFAULT 'NO_ONE'," \
          "fcsv_id int," \
          "PRIMARY KEY (name_id)," \
          "constraint FK_fcsv_id foreign key(fcsv_id) references %s(csv1_id))" % (table, table1)

    # print sql
    conn = connect_database(message)
    cursor = conn.cursor()
    cursor.execute(sql)
    conn.commit()
    conn.close()


# in here, we need a single and one by one
def insert_yaml(table, message, value):
    sql = "insert into %s (strategy, Config_WarmUp, Config_KBarPeriod_KBarTimeMin," \
          "Config_Parameters, optimum_num, optimum_period, optimum_values, back_test_num," \
          "back_test_period, back_test_value, csv_name)" % table
    sql1 = 'insert into %s (strategy, Config_WarmUp, Config_KBarPeriod_KBarTimeMin,' \
           'Config_Parameters, optimum_num, optimum_period, optimum_values, back_test_num,' \
           'back_test_period, back_test_value, csv_name)' % table

    t1 = ' values("%s", %s, %s, "%s", %s, "%s", "%s", %s, "%s", "%s", "%s")'\
         % (value[0], value[1], value[2], value[3], value[4],
            value[5], value[6], value[7], value[8], value[9], value[10])
    t2 = "values('"+str(value[0])+"', "+str(value[1])+", "+str(value[2])+", '"+str(value[3])+"', "+str(value[4])+", '"\
         +str(value[5])+"', '"+str(value[6])+"', "+str(value[7])+", '"+str(value[8])+"', '"+str(value[9])+"', '"+str(value[10])+"')"
    sql += t2
    sql2 = "select last_insert_id()"
    sql1 += t1
    conn = connect_database(message)
    cursor = conn.cursor()
    cursor.execute(sql1)
    conn.commit()
    cursor.execute(sql2)
    conn.commit()

    val = cursor.fetchone()
    # print val
    cursor.close()
    conn.close()

    a = "(csv1_id int not null AUTO_INCREMENT," \
                                 "strategy varchar(100) not null," \
                                 "Config_WarmUp float not null," \
                                 "Config_KBarPeriod_KBarTimeMin int not null," \
                                 "Config_Parameters text not null," \
                                 "optimum_num int not null," \
                                 "optimum_period varchar(100) not null," \
                                 "optimum_values text not null," \
                                 "back_test_num int not null," \
                                 "back_test_period varchar(100) not null," \
                                 "back_test_value text not null," \
                                 "csv_name varchar(200) not null," \
                                 "PRIMARY KEY (csv1_id))"
    return val[0]


def get_value_yaml(table, message):
    sql = "select back_test_value from %s where csv1_id = 1" % table
    sql2 = "select optimum_values from %s where csv1_id = 1" % table
    conn = connect_database(message)
    cursor = conn.cursor()
    cursor.execute(sql2)
    val = cursor.fetchone()
    conn.close()

    return val[0]

message2 = ["10.25.10.249", "huacheng", "huacheng123", "LI_DataBase_ver2"]
table = 'yaml_test_table_ver2'

value = ['test1rwer', 12.1, 12, 'test12', 6, 'test13', 'test14', 9, 'test15', 'test16', 'test11']

# create_yaml_table(table, message2)
# create_names_tables('test_name', 'yaml_table', message)

# insert_yaml(table, message, value)