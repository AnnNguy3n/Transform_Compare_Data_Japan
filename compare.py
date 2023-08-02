import pandas as pd
import numpy as np
import os
from transform import Transform_Financial, edit_folder_path


def compare_value_financial(x, y):
    if x == "NAN": return "0N" if y == 0.0 else "1N"

    if y == "NAN": return "0N" if x == 0.0 else "1N"

    if x == 0.0 and y == 0.0: return "N"

    if x * y == 0.0: return "M"

    a = 2*abs(x-y) / (abs(x) + abs(y))
    if a <= 0.01: return "1.00"

    return str(round(1.0 - a, 2))

def compare_series_financial(x, y):
    return x.combine(y, compare_value_financial)

def compare_value(x, y):
    if x == "NAN" and y == "NAN": return "N"
    if x == "NAN" or y == "NAN": return "2"
    if x == y: return "1"

    try:
        if float(x) == float(y): return "1"
    except: pass

    return "0"

def compare_series(x, y):
    return x.combine(y, compare_value)

def compare_series_with_NAN(x):
    return x.combine("NAN", compare_value)


class Compare_Financial:
    def __init__(self) -> None:
        tf = Transform_Financial()
        self.list_balance_feature = tf.list_balance_keys[:8] + ["Long-term financial investments"] + tf.list_balance_keys[8:]

        self.list_income_feature = tf.list_income_keys
        self.list_income_feature.remove("Volume")
        self.list_count = ["0N", "1N", "N", "M"]

    def pre_compare_setup(self, financial_type):
        '''
        financial_type: Balance hoặc Income
        '''
        if financial_type == "Balance":
            self.list_feature = self.list_balance_feature
        elif financial_type == "Income":
            self.list_feature = self.list_income_feature
        else: raise

    def compare_1(self, data_fi: pd.DataFrame, data_ir: pd.DataFrame):
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

        # data_fi_cols = data_fi.columns
        # for col in data_fi_cols:
        #     if col.startswith("29/02"):
        #         temp = "28/02" + col[5:]
        #         check = True
        #         for c in data_fi_cols:
        #             if c == temp:
        #                 check = False
        #                 break

        #         if check: data_fi.rename(columns={col:temp}, inplace=True)

        # data_ir_cols = data_ir.columns
        # for col in data_ir_cols:
        #     if col.startswith("29/02"):
        #         temp = "28/02" + col[5:]
        #         check = True
        #         for c in data_ir_cols:
        #             if c == temp:
        #                 check = False
        #                 break

        #         if check: data_ir.rename(columns={col:temp}, inplace=True)

        for col in data_ir.columns.difference(data_fi.columns):
            data_fi[col] = "NAN"

        for col in data_fi.columns.difference(data_ir.columns):
            data_ir[col] = "NAN"
        
        if len(data_fi.columns.unique()) != len(data_fi.columns) or len(data_ir.columns.unique()) != len(data_ir.columns):
            raise

        df_rs = data_fi.combine(data_ir, compare_series_financial)

        all_values = df_rs.values.flatten().astype(object)
        temp = {}
        for c in self.list_count:
            temp[c] = np.count_nonzero(all_values == c)

        temp_values = pd.to_numeric(all_values[~np.isin(all_values, self.list_count)], errors="raise")
        temp["1"] = np.count_nonzero(temp_values == 1)
        temp["-1"] = np.count_nonzero(temp_values == -1)
        temp["0.9x"] = np.count_nonzero((temp_values >= 0.9) & (temp_values <= 0.99))
        temp["0.8x"] = np.count_nonzero((temp_values >= 0.8) & (temp_values < 0.9))
        temp["0.7x"] = np.count_nonzero((temp_values >= 0.7) & (temp_values < 0.8))
        temp["0.6x"] = np.count_nonzero((temp_values >= 0.6) & (temp_values < 0.7))
        temp["0.5x"] = np.count_nonzero((temp_values >= 0.5) & (temp_values < 0.6))
        temp["<0.5"] = np.count_nonzero((temp_values < 0.5) & (temp_values != -1))
        temp["total"] = len(all_values)

        return df_rs, temp

    def compare_all(self,
                    folder_F1_financial,
                    folder_F1_balance,
                    folder_F1_income,
                    folder_result_BA,
                    folder_result_IC,
                    folder_save_error,
                    print_status=False):
        folder_F1_financial = edit_folder_path(folder_F1_financial)
        folder_F1_balance = edit_folder_path(folder_F1_balance)
        folder_F1_income = edit_folder_path(folder_F1_income)
        folder_result_BA = edit_folder_path(folder_result_BA)
        folder_result_IC = edit_folder_path(folder_result_IC)
        folder_save_error = edit_folder_path(folder_save_error)

        list_path_fi = os.listdir(folder_F1_financial)
        list_path_ba = os.listdir(folder_F1_balance)
        list_path_ic = os.listdir(folder_F1_income)
        list_error = []

        df_count = pd.DataFrame({self.list_count[i]: [] for i in range(len(self.list_count))})
        df_count["1"] = []
        df_count["-1"] = []
        df_count["0.9x"] = []
        df_count["0.8x"] = []
        df_count["0.7x"] = []
        df_count["0.6x"] = []
        df_count["0.5x"] = []
        df_count["<0.5"] = []
        df_count["total"] = []
        df_count_ic = df_count.copy()

        # Balance
        self.pre_compare_setup("Balance")
        for ii in range(len(list_path_fi)):
            path = list_path_fi[ii]
            if path.endswith("_balance.csv"):
                num = path.split("_balance.csv")[0]
                df_fi = pd.read_csv(folder_F1_financial + path)
                temp_path = path.split("_balance.csv")[0] + ".csv"
                if temp_path in list_path_ba:
                    df_ir = pd.read_csv(folder_F1_balance + temp_path)
                else:
                    df_ir = None

                try:
                    df_rs, temp = self.compare_1(df_fi, df_ir)
                    df_rs.to_csv(folder_result_BA + temp_path)
                    df_count.loc[num] = temp
                except:
                    list_error.append(path)

                if print_status: print(ii, path)

        for ii in range(len(list_path_ba)):
            path = list_path_ba[ii]
            temp_path = path.split(".csv")[0] + "_balance.csv"
            if temp_path not in list_path_fi:
                num = path.split(".csv")[0]
                df_ir = pd.read_csv(folder_F1_balance + path)
                try:
                    df_rs, temp = self.compare_1(None, df_ir)
                    df_rs.to_csv(folder_result_BA + path)
                    df_count.loc[num] = temp
                except:
                    list_error.append("IR_balance_"+path)

                if print_status: print(ii, path, "balance")

        # Income
        self.pre_compare_setup("Income")
        for ii in range(len(list_path_fi)):
            path = list_path_fi[ii]
            if path.endswith("_income.csv"):
                num = path.split("_income.csv")[0]
                df_fi = pd.read_csv(folder_F1_financial + path)
                temp_path = path.split("_income.csv")[0] + ".csv"
                if temp_path in list_path_ic:
                    df_ir = pd.read_csv(folder_F1_income + temp_path)
                else:
                    df_ir = None

                try:
                    df_rs, temp = self.compare_1(df_fi, df_ir)
                    df_rs.to_csv(folder_result_IC + temp_path)
                    df_count_ic.loc[num] = temp
                except:
                    list_error.append(path)

                if print_status: print(ii, path)

        for ii in range(len(list_path_ic)):
            path = list_path_ic[ii]
            temp_path = path.split(".csv")[0] + "_income.csv"
            if temp_path not in list_path_fi:
                num = path.split(".csv")[0]
                df_ir = pd.read_csv(folder_F1_income + path)
                try:
                    df_rs, temp = self.compare_1(None, df_ir)
                    df_rs.to_csv(folder_result_IC + path)
                    df_count_ic.loc[num] = temp

                except:
                    list_error.append("IR_income_"+path)

                if print_status: print(ii, path, "income")

        if print_status: print("Số file lỗi:", len(list_error))
        pd.DataFrame(list_error).to_csv(folder_save_error + "Financial_compare_error.csv", index=False)
        df_count.to_csv(folder_result_BA + "Count_values.csv")
        df_count_ic.to_csv(folder_result_IC + "Count_values.csv")


class Compare_Price:
    def __init__(self) -> None: pass

    def compare_1(self, df_1: pd.DataFrame, df_2: pd.DataFrame):
        df_1 = df_1.set_index("Time").fillna("NAN")
        df_2 = df_2.set_index("Time").fillna("NAN")
        df_1["PriceClosed"] = df_1["PriceClosed"].apply(lambda x: round(float(x), 0) if x != "NAN" else "NAN")
        df_2["PriceClosed"] = df_2["PriceClosed"].apply(lambda x: round(float(x), 0) if x != "NAN" else "NAN")
        df_1["VolumnTrade"] = df_1["VolumnTrade"].apply(lambda x: round(float(x), 0) if x != "NAN" else "NAN")
        df_2["VolumnTrade"] = df_2["VolumnTrade"].apply(lambda x: round(float(x), 0) if x != "NAN" else "NAN")

        if len(df_1.index.unique()) != len(df_1) or len(df_2.index.unique()) != len(df_2):
            return None, False

        if len(df_1) == 0 and len(df_2) == 0:
            return None, True

        list_index_chung = df_1.index.intersection(df_2.index)
        chung = df_1.loc[list_index_chung].combine(df_2.loc[list_index_chung], compare_series)
        rieng1 = df_1.loc[df_1.index.difference(df_2.index)].apply(compare_series_with_NAN)
        rieng2 = df_2.loc[df_2.index.difference(df_1.index)].apply(compare_series_with_NAN)

        df_rs = pd.concat([chung, rieng1, rieng2])
        df_rs["__T"] = pd.to_datetime(df_rs.index, format="%d/%m/%Y")
        df_rs.sort_values("__T", inplace=True, ascending=False)
        df_rs.pop("__T")

        return df_rs, True

    def compare_all(self,
                folder_src1: str,
                folder_src2: str,
                folder_result: str,
                print_status=False):
        folder_src1 = edit_folder_path(folder_src1)
        folder_src2 = edit_folder_path(folder_src2)
        folder_result = edit_folder_path(folder_result)
        df_count = pd.DataFrame({"0": [], "1": [], "2": [], "N": [], "total": []})

        list_path_1 = os.listdir(folder_src1)
        list_path_2 = os.listdir(folder_src2)
        list_path_total = np.unique(list_path_1+list_path_2)
        df_0 = pd.DataFrame({"Time": [], "PriceClosed": [], "VolumnTrade": []})
        count = [0, 0, 0]
        for ii in range(len(list_path_total)):
            path = list_path_total[ii]
            if path in list_path_1:
                df_1 = pd.read_csv(folder_src1 + path)
            else:
                df_1 = df_0

            if path in list_path_2:
                df_2 = pd.read_csv(folder_src2 + path)
            else:
                df_2 = df_0

            df_rs, check = self.compare_1(df_1, df_2)
            if not check:
                count[2] += 1
                df_count.loc[path.split(".csv")[0]] = {"0": "Err", "1": "Err", "2": "Err", "N": "Err", "total": "Err"}
            else:
                if df_rs is None:
                    count[1] += 1
                    df_count.loc[path.split(".csv")[0]] = {"0": 0, "1": 0, "2": 0, "N": 0, "total": 0}
                else:
                    df_rs.to_csv(folder_result + path)
                    count[0] += 1
                    all_values = df_rs.values.flatten()
                    unique, count_values = np.unique(all_values, return_counts=True)
                    temp = {"0": 0, "1": 0, "2": 0, "N": 0, "total": 0}
                    temp["total"] = len(all_values)
                    for i in range(len(unique)):
                        temp[str(unique[i])] = count_values[i]

                    df_count.loc[path.split(".csv")[0]] = temp

            if print_status: print(ii, path)

        df_count.to_csv(folder_result + "Count_values.csv")
        if print_status: print("Đã compare xong", count)


class Compare_Dividend:
    def __init__(self) -> None: pass

    def compare_1(self, df_1: pd.DataFrame, df_2: pd.DataFrame):
        df_1 = df_1.set_index("Time")
        df_2 = df_2.set_index("Time")
        df_1["Money"] = df_1["Money"].apply(lambda x: round(float(x), 4) if x != "NAN" else "NAN")
        df_2["Money"] = df_2["Money"].apply(lambda x: round(float(x), 4) if x != "NAN" else "NAN")

        if len(df_1.index.unique()) != len(df_1) or len(df_2.index.unique()) != len(df_2):
            return None, False

        if len(df_1) == 0 and len(df_2) == 0:
            return None, True

        list_index_chung = df_1.index.intersection(df_2.index)
        chung = df_1.loc[list_index_chung].combine(df_2.loc[list_index_chung], compare_series)
        rieng1 = df_1.loc[df_1.index.difference(df_2.index)].apply(compare_series_with_NAN)
        rieng2 = df_2.loc[df_2.index.difference(df_1.index)].apply(compare_series_with_NAN)

        df_rs = pd.concat([chung, rieng1, rieng2])
        df_rs["__T"] = pd.to_datetime(df_rs.index, format="%d/%m/%Y")
        df_rs.sort_values("__T", inplace=True, ascending=False)
        df_rs.pop("__T")

        return df_rs, True

    def compare_all(self,
                folder_src1: str,
                folder_src2: str,
                folder_result: str,
                print_status=False):
        folder_src1 = edit_folder_path(folder_src1)
        folder_src2 = edit_folder_path(folder_src2)
        folder_result = edit_folder_path(folder_result)
        df_count = pd.DataFrame({"0": [], "1": [], "2": [], "N": [], "total": []})

        list_path_1 = os.listdir(folder_src1)
        list_path_2 = os.listdir(folder_src2)
        list_path_total = np.unique(list_path_1+list_path_2)
        df_0 = pd.DataFrame({"Time": [], "Stock": [], "Money": []})
        count = [0, 0, 0]
        for ii in range(len(list_path_total)):
            path = list_path_total[ii]
            if path in list_path_1:
                df_1 = pd.read_csv(folder_src1 + path)
            else:
                df_1 = df_0

            if path in list_path_2:
                df_2 = pd.read_csv(folder_src2 + path)
            else:
                df_2 = df_0

            df_rs, check = self.compare_1(df_1, df_2)
            if not check:
                count[2] += 1
                df_count.loc[path.split(".csv")[0]] = {"0": "Err", "1": "Err", "2": "Err", "N": "Err", "total": "Err"}
            else:
                if df_rs is None:
                    count[1] += 1
                    df_count.loc[path.split(".csv")[0]] = {"0": 0, "1": 0, "2": 0, "N": 0, "total": 0}
                else:
                    df_rs.to_csv(folder_result + path)
                    count[0] += 1
                    all_values = df_rs.values.flatten()
                    unique, count_values = np.unique(all_values, return_counts=True)
                    temp = {"0": 0, "1": 0, "2": 0, "N": 0, "total": 0}
                    temp["total"] = len(all_values)
                    for i in range(len(unique)):
                        temp[str(unique[i])] = count_values[i]

                    df_count.loc[path.split(".csv")[0]] = temp

            if print_status: print(ii, path)

        df_count.to_csv(folder_result + "Count_values.csv")
        if print_status: print("Đã compare xong", count)
