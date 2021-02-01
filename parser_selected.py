#%%

import pandas as pd
from os import walk, makedirs
from os.path import join, exists, basename
import xml.etree.ElementTree as elemTree
import os, operator
from os.path import getsize
from tqdm.notebook import tqdm as tn

#%%

def get_data(filename):
    tree = elemTree.parse(filename)
    root = tree.getroot().find('BestExRptgRTS27DataRpt')
    basic_tree = root.find('Header')
    body_tree = root.find('Body')

    columns = ['VnNm', 'VnCd', 'CtryCompAuth',
               'MktSgmt', 'MktSgmtNm', 'DtTrdDay',
               'FinInstr', 'FinInstrNm', 'CFICd', 'Ccy',
               'LkhdExec.TtlValExecTx']

    all_data = pd.DataFrame(columns=columns)

    # 1. Parse basic data
    basic_data_columns = columns[:6]
    basic_data = {}
    for tag in basic_data_columns:
        basic_data[tag] = [basic_tree.find(tag).text]

    # 2. Parse FinInstrument data
    for fininst_tree in body_tree.iter('FinInstrument'):
        LkhdExec = fininst_tree.find('LkhdExec')
        if LkhdExec is None:
            continue

        fin_basic_data_columns = columns[6:10]
        fin_basic_data = {}

        for tag in fin_basic_data_columns:
            fin_basic_data[tag] = [fininst_tree.find(tag).text]

        # 2. Parse LkhdExec columns, and add to all_dataset
        TtlValExecTx = LkhdExec.find('TtlValExecTx').text
        all_data = all_data.append(pd.DataFrame.from_dict(
            {**fin_basic_data, **basic_data, 'LkhdExec.TtlValExecTx': [TtlValExecTx]}
        ), ignore_index=True)


    return all_data

def write_data(filepath, filename, all_data, header=False, mode='a'):
    if not exists(filepath):
        makedirs(filepath)

    full_filename = join(filepath, filename)
    all_data.to_csv(
        full_filename,
        index=False,
        sep=",",
        mode=mode,
        header=header
    )

def make_new(SAVE_PATH, save_name, columns):
    if not exists(SAVE_PATH):
        makedirs(SAVE_PATH)

    full_filename = join(SAVE_PATH, save_name)

    all_data = pd.DataFrame(columns=columns)
    all_data.to_csv(full_filename, index=False)

    return save_name

#%%

PATH = "/home/genne/Downloads/2020/files"  # need to modify this
files_list = []
for path, dirs, files in walk(PATH):
    files_list.extend([(join(path, file),
                        getsize(join(path, file))) for file in files])
files_list = sorted(files_list, key=operator.itemgetter(1), reverse=True)
print(len(files_list))
columns = ['VnNm', 'VnCd', 'CtryCompAuth',
           'MktSgmt', 'MktSgmtNm', 'DtTrdDay',
           'FinInstr', 'FinInstrNm', 'CFICd', 'Ccy',
           'LkhdExec.TtlValExecTx']

SAVE_PATH = "all"
SAVE_PATH_SEP = "sep2020"

save_name = 'bond_transactions.csv'
for fname, fsize in tn(files_list):
    all_data = get_data(fname)
    write_data(SAVE_PATH,
               save_name,
               all_data)
    write_data(SAVE_PATH_SEP,
               basename(fname).split(".")[0]+".csv",
               all_data,
               mode='w', header=True)