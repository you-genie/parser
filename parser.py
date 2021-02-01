import pandas as pd
from os import walk, makedirs
from os.path import join, exists
from tqdm import tqdm_notebook
import xml.etree.ElementTree as elemTree
import os, operator
from os.path import getsize


def get_data(filename):
    tree = elemTree.parse(filename)
    root = tree.getroot()
    basic_data = root.find('BestExRptgRTS27DataRpt').find('Header')

    VnNm = basic_data.find('VnNm').text
    VnCd = basic_data.find('VnCd').text
    CtryCompAuth = basic_data.find('CtryCompAuth').text
    MktSgmt = basic_data.find('MktSgmt').text
    MktSgmtNm = basic_data.find('MktSgmtNm').text
    DtTrdDay = basic_data.find('DtTrdDay').text

    basic_data = {
        'VnNm': [VnNm],
        'VnCd': [VnCd],
        "CtryCompAuth": [CtryCompAuth],
        'MktSgmt': [MktSgmt],
        'MktSgmtNm': [MktSgmtNm],
        'DtTrdDay': [DtTrdDay]
    }
    columns = "VnNm VnCd CtryCompAuth MktSgmt MktSgmtNm DtTrdDay " \
              "FinInstr FinInstrNm CFICd Ccy " \
              "Data SzRn Pric ExecTime TxSize TrdSystm TrdMode TrdPlt " \
              "SmplAvgExecPric TtlValExec " \
              "SmplAvgTxPric VolWghTxPric HgstTxPric LwstTxPric " \
              "NumRcvdOrQt NumExecTx TtlValExecTx NmOfCxlQrQt " \
              "NmOfAmndQrQt MdnTxSize MdnOrQtSize " \
              "Data2 BstBdPric BstOfrPric BbSz OfrSz".split(" ")

    all_data = pd.DataFrame(columns=columns)

    for fininst in root.find('BestExRptgRTS27DataRpt').find('Body').iter('FinInstrument'):
        fin_basic = {
            "FinInstr": [fininst.find('FinInstr').text],
            "FinInstrNm": [fininst.find('FinInstrNm').text],
            "CFICd": [fininst.find('CFICd').text],
            "Ccy": [fininst.find('Ccy').text]
        }
        DaylyPric = fininst.find('DaylyPric')
        if DaylyPric is not None:
            for data in DaylyPric.iter():
                if data.tag != 'DaylyPric':
                    fin_basic[data.tag] = data.text

        LkhdExec = fininst.find('LkhdExec') # NumRcvdOrQt etc...
        if LkhdExec is not None:
            for data in LkhdExec.iter():
                if data.tag != "LkhdExec":
                    fin_basic[data.tag] = data.text

        IntrDayPric = fininst.find('IntrDayPric') # SzRn etc...
        if IntrDayPric is not None:
            for Time in IntrDayPric.iter('Time'):
                time_data = {}
                time_data['SzRn'] = Time.find('Data').attrib['SzRn']
                time_data['Data'] = Time.find('Data').text
                FrstTrd = Time.find('FrstTrd')

                if FrstTrd is not None:
                    frsttrd_data = {}
                    for data in FrstTrd.iter():
                        if data.tag != "FrstTrd":
                            frsttrd_data[data.tag] = data.text

                    all_data = all_data.append(pd.DataFrame.from_dict(
                        {**frsttrd_data, **time_data, **fin_basic, **basic_data}),
                        ignore_index=True)

                AllTrds = Time.find('AllTrds')

                if AllTrds is not None:
                    alltrds_data = {
                        'SmplAvgExecPric': AllTrds.find('SmplAvgExecPric').text,
                        'TtlValExec': AllTrds.find('TtlValExec').text
                    }
                    all_data = all_data.append(pd.DataFrame.from_dict(
                        {**alltrds_data, **time_data, **fin_basic, **basic_data}
                    ), ignore_index=True)

        LkhdExecInfoTm = fininst.find('LkhdExecInfoTm')
        if LkhdExecInfoTm is not None:
            for Time in LkhdExecInfoTm.iter('Time'):
                time_data = {}
                for data in Time.iter():
                    if data.tag == 'Data':
                        time_data['Data2'] = data.text
                    elif data.tag != 'Time':
                        time_data[data.tag] = data.text

                all_data = all_data.append(pd.DataFrame.from_dict(
                        {**time_data, **fin_basic, **basic_data}),
                        ignore_index=True)

        # if LkhdExecInfoTm.find('Time') is None and IntrDayPric is None:
        if LkhdExecInfoTm.find('Time') is None:

            all_data = all_data.append(pd.DataFrame.from_dict(
                {**fin_basic, **basic_data}), ignore_index=True)

    return all_data


def write_data(filepath, filename, all_data, resume=False):
    full_filename = join(filepath, filename)
    if not resume:
        if not exists(filepath):
            makedirs(filepath)

        full_filename = make_new(full_filename)

    all_data.to_csv(
        full_filename,
        index=False,
        sep=",",
        mode='a',
        header="False"
    )


def make_new_filename(save_num):
    SAVE_PATH = "/home/genne/Downloads/transactions_new"
    save_name = f"bond_transactions_{save_num}.csv"
    return join(SAVE_PATH, save_name)


def make_new(full_filename, save_num=-1):
    if full_filename == "":
        assert save_num > 0
        full_filename = make_new_filename(save_num)

    columns = "VnNm VnCd CtryCompAuth MktSgmt MktSgmtNm DtTrdDay " \
          "FinInstr FinInstrNm CFICd Ccy " \
          "Data SzRn Pric ExecTime TxSize TrdSystm TrdMode TrdPlt " \
          "SmplAvgExecPric TtlValExec " \
          "SmplAvgTxPric VolWghTxPric HgstTxPric LwstTxPric " \
          "NumRcvdOrQt NumExecTx TtlValExecTx NmOfCxlQrQt " \
          "NmOfAmndQrQt MdnTxSize MdnOrQtSize " \
          "Data2 BstBdPric BstOfrPric BbSz OfrSz".split(" ")
    all_data = pd.DataFrame(columns=columns)

    all_data.to_csv(full_filename, index=False)

    return save_name


## GET FILELIST
PATH = "/home/genne/Downloads/output"
files_list = []
for path, dirs, files in walk(PATH):
    files_list.extend([(os.path.join(path, file),
                        getsize(os.path.join(path, file))) for file in files])
files_list = sorted(files_list, key=operator.itemgetter(1), reverse=True)
print(files_list)

## PROCESS DATA
SAVE_PATH = "/home/genne/Downloads/transactions_new"
save_name = make_new("bond_transactions.csv")
for fname, _ in tqdm_notebook(files_list):
    all_data = get_data(fname)
    write_data(SAVE_PATH, save_name, all_data)


