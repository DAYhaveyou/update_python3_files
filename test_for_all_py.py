import pandas as pd
import os
import pwd
import json

def test_compare(file1, file2):

    df = pd.read_csv(file1, header=None, names=['File', 'NP', 'MDD', 'Ppt', 'TT', 'ShR', 'Days', 'D_WR', 'D_STD', 'D_MaxNP',
                      'D_MinNP', 'maxW_Days', 'maxL_Days', 'mDD_mC', 'mDD_iC', 'LVRG', 'LVRG_Mon_NP'])
    df1  = pd.read_csv(file2, header=None, names=['File', 'NP', 'MDD', 'Ppt', 'TT', 'ShR', 'Days', 'D_WR', 'D_STD', 'D_MaxNP',
                      'D_MinNP', 'maxW_Days', 'maxL_Days', 'mDD_mC', 'mDD_iC', 'LVRG', 'LVRG_Mon_NP'])

    different = 0
    j = 10
    print(len(df['File']))
    for i in range(len(df['File'])):
        if df['File'][i] != df1['File'][i]:
            different += 1
    NAME = ['File', 'NP', 'MDD', 'Ppt', 'TT', 'ShR', 'Days', 'D_WR', 'D_STD', 'D_MaxNP',
            'D_MinNP', 'maxW_Days', 'maxL_Days', 'mDD_mC', 'mDD_iC', 'LVRG', 'LVRG_Mon_NP']

    for name1 in NAME[3:4]:
        for i in range(len(df['File'])):
            if df[name1][i] != df1[name1][i]:
                different += 1
    '''
    for i in range(len(df['NP'])):
        if df['NP'][i] != df1['NP'][i]:
            different += 1
    
    for i in range(len(df['File'])):
        if df['File'][i] != df1['File'][i]:
            different += 1
            
    for i in range(len(df['File'])):
        if df['File'][i] != df1['File'][i]:
            different += 1
    '''

    print(different)

def make_json_test(value):
    f = open("test.json", 'w')
    json.dump(value, f, indent=6)
    f.close()

# test_compare('one_indicators_test.csv', 'one_indicators_test1.csv')

# print(pwd.getpwuid(os.getuid())[0])

dict = {'test': 12, "test2": 21, 'arr':[1, 3, 4 , 5 , 6, 'show me code']}
# make_json_test(dict)
