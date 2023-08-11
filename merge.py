import pandas as pd
import numpy as np
import os
from transform import Transform_Financial, edit_folder_path


def merge_value(x, y):
    '''
    x: value bên financial
    y: value bên IR
    '''
    if x == "NAN": return y
    if y == "NAN": return x
    if x == 0.0 and y == 0.0: return 0.0
    if x * y == 0.0: return x
    return x

def merge_series(x, y):
    return x.combine(y, merge_value)


class Merge_Financial:
    def __init__(self) -> None:
        tf = Transform_Financial()
        self.list_balance_feature = tf.list_balance_keys[:8] + ["Long-term financial investments"] + tf.list_balance_keys[8:]

        self.list_income_feature = tf.list_income_keys
        self.list_income_feature.remove("Volume")

    def pre_merge_setup(self, financial_type):
        '''
        financial_type: Balance hoặc Income
        '''
        if financial_type == "Balance":
            self.list_feature = self.list_balance_feature
        elif financial_type == "Income":
            self.list_feature = self.list_income_feature
        else: raise

    def merge_1(self, data_fi: pd.DataFrame, data_ir: pd.DataFrame):
        if data_fi is not None:
            data_fi.rename(columns={"Unnamed: 0": "Feature"}, inplace=True)
            data_fi = data_fi.set_index("Feature").loc[self.list_feature]
            data_fi.columns = pd.to_datetime(data_fi.columns, format="%d/%m/%Y").strftime("%m/%Y")

        if data_ir is not None:
            data_ir = data_ir.set_index("Feature").loc[self.list_feature]
            data_ir.columns = pd.to_datetime(data_ir.columns, format="%d/%m/%Y").strftime("%m/%Y")

        if data_fi is None:
            data_fi = data_ir.replace(data_ir.values, "NAN")

        if data_ir is None:
            data_ir = data_fi.replace(data_fi.values, "NAN")

        for col in data_ir.columns.difference(data_fi.columns):
            data_fi[col] = "NAN"

        for col in data_fi.columns.difference(data_ir.columns):
            data_ir[col] = "NAN"

        if len(data_fi.columns.unique()) != len(data_fi.columns) or len(data_ir.columns.unique()) != len(data_ir.columns):
            return "Có 2 cột giống nhau ở cùng 1 file", False

        df_rs = data_fi.combine(data_ir, merge_series)

        return df_rs[sorted(df_rs.columns, key=lambda x:x[-4:])], True

    def merge_all(self,
                    folder_F1_financial,
                    folder_F1_balance,
                    folder_F1_income,
                    folder_result_BA,
                    folder_result_IC,
                    folder_save_error,
                    print_status=False,
                    list_com=[],
                    type_merge=""):
        folder_F1_financial = edit_folder_path(folder_F1_financial)
        folder_F1_balance = edit_folder_path(folder_F1_balance)
        folder_F1_income = edit_folder_path(folder_F1_income)
        folder_result_BA = edit_folder_path(folder_result_BA)
        folder_result_IC = edit_folder_path(folder_result_IC)
        folder_save_error = edit_folder_path(folder_save_error)
        if len(list_com) == 0:
            list_path_fi = os.listdir(folder_F1_financial)
            list_path_ba = os.listdir(folder_F1_balance)
            list_path_ic = os.listdir(folder_F1_income)
            if type_merge != "Balance" and type_merge != "Income" and type_merge != "":
                raise Exception("Sai type_merge")
        else:
            list_path_fi_temp = os.listdir(folder_F1_financial)
            if type_merge == "Balance":
                list_path_ic = []
                list_path_ba_temp = os.listdir(folder_F1_balance)
                list_path_ba = [str(com)+".csv" for com in list_com]
                list_path_fi = [str(com)+"_balance.csv" for com in list_com]
                for i in range(len(list_com)):
                    com = list_com[i]
                    count = 0
                    if str(com)+".csv" not in list_path_ba_temp:
                        list_path_ba.remove(str(com)+".csv")
                        count += 1

                    if str(com)+"_balance.csv" not in list_path_fi_temp:
                        list_path_fi.remove(str(com)+"_balance.csv")
                        count += 1

                    if count == 2:
                        raise Exception(f"{com} (balance) không tồn tại ở cả 2 nguồn")
            elif type_merge == "Income":
                list_path_ba = []
                list_path_ic_temp = os.listdir(folder_F1_income)
                list_path_ic = [str(com)+".csv" for com in list_com]
                list_path_fi = [str(com)+"_income.csv" for com in list_com]
                for i in range(len(list_com)):
                    com = list_com[i]
                    count = 0
                    if str(com)+".csv" not in list_path_ic_temp:
                        list_path_ic.remove(str(com)+".csv")
                        count += 1

                    if str(com)+"_income.csv" not in list_path_fi_temp:
                        list_path_fi.remove(str(com)+"_income.csv")
                        count += 1

                    if count == 2:
                        raise Exception(f"{com} (income) không tồn tại ở cả 2 nguồn")
            else:
                raise Exception("type_merge không hợp lệ")

        list_error = []

        if type_merge != "Income":
            # Balance
            self.pre_merge_setup("Balance")
            for ii in range(len(list_path_fi)):
                path = list_path_fi[ii]

                if path.endswith("_balance.csv"):
                    if print_status: print(ii, path)
                    df_fi = pd.read_csv(folder_F1_financial + path)
                    temp_path = path.split("_balance.csv")[0] + ".csv"
                    if temp_path in list_path_ba:
                        df_ir = pd.read_csv(folder_F1_balance + temp_path)
                    else:
                        df_ir = None

                    try:
                        df_rs, check = self.merge_1(df_fi, df_ir)
                        if check:
                            df_rs.to_csv(folder_result_BA + temp_path)
                        else:
                            list_error.append(f"{path}: {df_rs}")
                    except Exception as exception:
                        list_error.append(f"{path}: {exception}")

            for ii in range(len(list_path_ba)):
                path = list_path_ba[ii]
                temp_path = path.split(".csv")[0] + "_balance.csv"
                if temp_path not in list_path_fi:
                    if print_status: print(ii, path, "balance")
                    df_ir = pd.read_csv(folder_F1_balance + path)

                    try:
                        df_rs, check = self.merge_1(None, df_ir)
                        if check:
                            df_rs.to_csv(folder_result_BA + path)
                        else:
                            list_error.append(f"IR_balance_{path}: {df_rs}")
                    except Exception as exception:
                        list_error.append(f"IR_balance_{path}: {exception}")

        if type_merge != "Balance":
            # Income
            self.pre_merge_setup("Income")
            for ii in range(len(list_path_fi)):
                path = list_path_fi[ii]

                if path.endswith("_income.csv"):
                    if print_status: print(ii, path)
                    df_fi = pd.read_csv(folder_F1_financial + path)
                    temp_path = path.split("_income.csv")[0] + ".csv"
                    if temp_path in list_path_ic:
                        df_ir = pd.read_csv(folder_F1_income + temp_path)
                    else:
                        df_ir = None

                    try:
                        df_rs, check = self.merge_1(df_fi, df_ir)
                        if check:
                            df_rs.to_csv(folder_result_IC + temp_path)
                        else:
                            list_error.append(f"{path}: {df_rs}")
                    except Exception as exception:
                        list_error.append(f"{path}: {exception}")

            for ii in range(len(list_path_ic)):
                path = list_path_ic[ii]
                temp_path = path.split(".csv")[0] + "_income.csv"
                if temp_path not in list_path_fi:
                    if print_status: print(ii, path, "income")
                    df_ir = pd.read_csv(folder_F1_income + path)

                    try:
                        df_rs, check = self.merge_1(None, df_ir)
                        if check:
                            df_rs.to_csv(folder_result_IC + path)
                        else:
                            list_error.append(f"IR_income_{path}: {df_rs}")
                    except Exception as exception:
                        list_error.append(f"IR_income_{path}: {exception}")

        if print_status: print("Số file lỗi:", len(list_error))
        pd.DataFrame(list_error).to_csv(folder_save_error + "Financial_merge_error.csv", index=False)