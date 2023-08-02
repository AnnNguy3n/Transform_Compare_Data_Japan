import pandas as pd
import os


def edit_folder_path(path: str):
    assert(os.path.exists(path))
    if not path.endswith("/") and not path.endswith("\\"): return path + "/"
    return path


class Transform_Financial:
    def __init__(self) -> None:
        self.dict_balance_name = {'CURRENT ASSETS': ['TOTALCURRENTASSETS'], 'Cash and cash equivalents': ['CASHANDCASHEQUIVALENTS'], 'Short Term Financial Investments': ['SHORTTERMINVESTMENTS'], 'Short term receivables': ['TRADEANDOTHERRECEIVABLES,CURRENT'], 'Total Inventories': ['INVENTORIES'], 'Total Other Current Assets': ['OTHERCURRENTASSETS'], 'TOTAL NON-CURRENT ASSETS': ['TOTALNON-CURRENTASSETS'], 'Fixed assets': ['NETPROPERTY,PLANTANDEQUIPMENT'], 'Total Other non-current assets': ['NETINTANGIBLEASSETS'], 'TOTAL ASSETS': ['TOTALASSETS'], 'LIABILITIES': ['TOTALLIABILITIES'], 'Current liabilities': ['TOTALCURRENTLIABILITIES'], 'Non-current liabilities': ['TOTALNON-CURRENTLIABILITIES'], "TOTAL OWNER'S EQUITY": ['TOTALEQUITY'], "Owner's equity": ['TOTALEQUITYATTRIBUTABLETOPARENTSTOCKHOLDERS', 'EQUITYATTRIBUTABLETOPARENTSTOCKHOLDERS'], 'Other sources and funds_header': ['RESERVES/ACCUMULATEDCOMPREHENSIVEINCOME/LOSSES'], 'Non-controlling interests': ['NON-CONTROLLING/MINORITYINTERESTSINEQUITY'], "TOTAL LIABILITIES AND OWNER'S EQUITY": ['TOTALLIABILITIES', 'TOTALEQUITY']}
        self.list_balance_keys = list(self.dict_balance_name.keys())
        self.list_balance_name = ['CASHANDCASHEQUIVALENTS', 'EQUITYATTRIBUTABLETOPARENTSTOCKHOLDERS', 'INVENTORIES', 'NETINTANGIBLEASSETS', 'NETPROPERTY,PLANTANDEQUIPMENT', 'NON-CONTROLLING/MINORITYINTERESTSINEQUITY', 'OTHERCURRENTASSETS', 'RESERVES/ACCUMULATEDCOMPREHENSIVEINCOME/LOSSES', 'SHORTTERMINVESTMENTS', 'TOTALASSETS', 'TOTALCURRENTASSETS', 'TOTALCURRENTLIABILITIES', 'TOTALEQUITY', 'TOTALEQUITYATTRIBUTABLETOPARENTSTOCKHOLDERS', 'TOTALLIABILITIES', 'TOTALNON-CURRENTASSETS', 'TOTALNON-CURRENTLIABILITIES', 'TRADEANDOTHERRECEIVABLES,CURRENT']
        self.b_month = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

        self.dict_income_name = {'Net sales': 'TOTALREVENUE', 'Cost of sales': 'COSTOFREVENUE', 'Gross profit': 'GROSSPROFIT', 'Selling, general and administrative expenses': 'OPERATINGINCOME/EXPENSES', 'Net operating profit': 'TOTALOPERATINGPROFIT/LOSS', 'Other profit': 'NON-OPERATINGINCOME/EXPENSE,TOTAL', 'Total accounting profit before tax': 'PRETAXINCOME', 'Income taxes': 'PROVISIONFORINCOMETAX', 'Profit after tax': 'NETINCOMEAFTEREXTRAORDINARYITEMSANDDISCONTINUEDOPERATIONS', 'Net profit after tax of the parent': 'NETINCOMEAFTERNON-CONTROLLING/MINORITYINTERESTS', 'Benefits of minority shareholders': 'NON-CONTROLLING/MINORITYINTERESTS', 'Volume': 'BASICWASO'}
        self.list_income_keys = list(self.dict_income_name.keys())
        self.list_income_name = list(self.dict_income_name.values())
        self.list_mul = [1, -1, 1, -1, 1, 1, 1, -1, 1, 1, -1, 1]

    def transform_balance_1(self, data: pd.DataFrame):
        data = data.fillna(0.0).drop_duplicates(ignore_index=True)
        feature_col = data.columns[0]
        time = data.loc[len(data)-1, feature_col].split("Fiscal year ends in ")[1].split(" |")[0]
        if len(time) > 3 and time[0:3] in self.b_month:
            time = pd.to_datetime(time + ", 2000", format="%b %d, %Y").strftime("%d/%m")
        else:
            return "Không tìm được Time_BCTC", False

        data[feature_col] = data[feature_col].str.replace(" ", "").str.upper()
        data.set_index(feature_col, inplace=True)

        dict_morning = {}
        for name in self.list_balance_name:
            if name in data.index:
                if len(data.loc[name].shape) == 2:
                    return "Có một feature xuất hiện ở 2 hàng trở lên, dữ liệu khác nhau", False

                dict_morning[name] = data.loc[name]

        list_row = []
        dict_morning_keys = list(dict_morning.keys())
        data_columns = data.columns
        for name in self.list_balance_keys:
            temp_col = None
            for col in self.dict_balance_name[name]:
                if col in dict_morning_keys:
                    if temp_col is None:
                        temp_col = dict_morning[col].copy()
                    else:
                        if name == "Owner's equity":
                            if (temp_col != dict_morning[col]).any():
                                return "2 hàng dữ liệu khác nhau tương ứng Owner's equity", False
                        else:
                            temp_col += dict_morning[col]

            if temp_col is None:
                temp_col = pd.Series({col:0.0 for col in data_columns})

            list_row.append(temp_col)

        new_data = pd.DataFrame(list_row)
        new_data.index = self.list_balance_keys
        temp_row\
            = new_data.loc["TOTAL NON-CURRENT ASSETS"]\
            - new_data.loc["Fixed assets"]\
            - new_data.loc["Total Other non-current assets"]
        temp_df = pd.DataFrame([temp_row])
        temp_df.index = ["Long-term financial investments"]

        df_rs = pd.concat([new_data.iloc[0:8], temp_df, new_data.iloc[8:]])
        df_rs.rename(columns={data_columns[i]: time+"/"+data_columns[i] for i in range(len(data_columns))}, inplace=True)
        return df_rs, True

    def transform_income_1(self, data: pd.DataFrame):
        data = data.fillna(0.0).drop_duplicates(ignore_index=True)
        if "TTM" in data.columns:
            data.pop("TTM")

        feature_col = data.columns[0]
        time = data.loc[len(data)-1, feature_col].split("Fiscal year ends in ")[1].split(" |")[0]
        if len(time) > 3 and time[0:3] in self.b_month:
            time = pd.to_datetime(time + ", 2000", format="%b %d, %Y").strftime("%d/%m")
        else:
            return "Không tìm được Time_BCTC", False

        data[feature_col] = data[feature_col].str.replace(" ", "").str.upper()
        data.set_index(feature_col, inplace=True)
        data_columns = data.columns
        data.loc["-1"] = {col:0.0 for col in data_columns}

        list_row = []
        for ii in range(len(self.list_income_name)):
            name = self.list_income_name[ii]
            if name in data.index:
                if len(data.loc[name].shape) == 2:
                    return "Có một feature xuất hiện ở 2 hàng trở lên, dữ liệu khác nhau", False

                list_row.append(self.list_mul[ii] * data.loc[name])
            else:
                list_row.append(data.loc["-1"])

        new_data = pd.DataFrame(list_row)
        new_data.index = self.list_income_keys
        new_data.rename(columns={data_columns[i]: time+"/"+data_columns[i] for i in range(len(data_columns))}, inplace=True)
        return new_data, True

    def transform_all(self,
                      folder_data: str,
                      folder_F1: str,
                      print_status=False):
        folder_data = edit_folder_path(folder_data)
        folder_F1 = edit_folder_path(folder_F1)

        list_path_data = os.listdir(folder_data)
        count = [0, 0]
        df_error = pd.DataFrame({"File": [], "Error": []})
        df_income_error = pd.DataFrame({"File": [], "Error": []})

        for ii in range(len(list_path_data)):
            path = list_path_data[ii]
            if path.endswith("_balance.csv"):
                data = pd.read_csv(folder_data + path)
                df_rs, check = self.transform_balance_1(data)
                if not check:
                    df_error.loc[len(df_error)] = {"File": path, "Error": df_rs}
                    count[1] += 1
                else:
                    df_rs.to_csv(folder_F1 + path)
                    count[0] += 1

                if print_status: print(ii, path)

            elif path.endswith("_income.csv"):
                data = pd.read_csv(folder_data + path)
                df_rs, check = self.transform_income_1(data)
                if not check:
                    df_income_error.loc[len(df_income_error)] = {"File": path, "Error": df_rs}
                    count[1] += 1
                else:
                    df_rs.to_csv(folder_F1 + path)
                    count[0] += 1

                if print_status: print(ii, path)

        if print_status: print("Xong,", count[0], "file đã trasform,", count[1], "file chưa transform")
        df_error.to_csv(folder_F1 + "Balance_errors.csv", index=False)
        df_income_error.to_csv(folder_F1 + "Income_errors.csv", index=False)


class Transform_Price:
    def __init__(self) -> None:
        self.list_col_YahooJP = ["日付", "終値", "出来高"]
        self.list_col_Minkabu = ["日時", "調整後終値", "出来高(株)"]
        self.format_ymd_YahooJP = "%Y年%m月%d"
        self.format_ymd_Minkabu = "%Y/%m/%d"

        self.list_renamed_col = ["Time", "PriceClosed", "VolumnTrade"]

    def pre_transform_setup(self, src):
        '''
        src: YahooJP hoặc Minkabu
        '''

        if src == "YahooJP":
            self.list_col = self.list_col_YahooJP
            self.format = self.format_ymd_YahooJP
            self.cut_index = -1
        elif src == "Minkabu":
            self.list_col = self.list_col_Minkabu
            self.format = self.format_ymd_Minkabu
            self.cut_index = 30
        else:
            raise Exception("Nguồn không hợp lệ")

    def transform_1(self, data: pd.DataFrame):
        df = data[self.list_col].rename(columns={self.list_col[i]: self.list_renamed_col[i] for i in range(len(self.list_col))})
        df["Time"] = df["Time"].str[:self.cut_index]
        df["Time"] = pd.to_datetime(df["Time"], format=self.format).dt.strftime("%d/%m/%Y")
        df["PriceClosed"] = pd.to_numeric(df["PriceClosed"], errors="coerce")
        df["VolumnTrade"] = pd.to_numeric(df["VolumnTrade"], errors="coerce")
        return df[df["PriceClosed"].notna()].drop_duplicates()

    def transform_all(self,
                      src,
                      folder_lst: str,
                      folder_delst: str,
                      folder_F1: str,
                      print_status=False):
        '''
        src: YahooJP hoặc Minkabu
        '''

        self.pre_transform_setup(src)

        folder_lst = edit_folder_path(folder_lst)
        folder_delst = edit_folder_path(folder_delst)
        folder_F1 = edit_folder_path(folder_F1)

        list_path_listing = os.listdir(folder_lst)
        list_path_delisted = os.listdir(folder_delst)

        count = 0
        for ii in range(len(list_path_listing)):
            path = list_path_listing[ii]
            df_1 = self.transform_1(pd.read_csv(folder_lst + path))
            if path in list_path_delisted:
                df_2 = self.transform_1(pd.read_csv(folder_delst + path))
                df_1 = pd.concat([df_1, df_2]).drop_duplicates()
                df_1["__T"] = pd.to_datetime(df_1["Time"], format="%d/%m/%Y")
                df_1.sort_values("__T", ascending=False, inplace=True)
                df_1.pop("__T")

            df_1.to_csv(folder_F1 + path, index=False)
            count += 1
            if print_status: print(ii, path)

        for ii in range(len(list_path_delisted)):
            path = list_path_delisted[ii]
            if path not in list_path_listing:
                df_1 = self.transform_1(pd.read_csv(folder_delst + path))
                df_1.to_csv(folder_F1 + path, index=False)
                count += 1
                if print_status: print(ii, path, "Delisted")

        if print_status: print("Tổng số file:", count)


class Transform_Dividend:
    def __init__(self) -> None: pass

    def transform_Mor1_F0(self, data: pd.DataFrame):
        data["Date"] = pd.to_datetime(data["Date"], format="%m/%d/%Y").dt.strftime("%d/%m/%Y")
        df_rs = pd.DataFrame({"Time": [], "Stock": [], "Money": []})
        df_rs.set_index("Time", inplace=True)

        if len(data) == 0:
            return df_rs

        def transform(i):
            if data.loc[i, "Data Type"] == "Dividends":
                df_rs.loc[data.loc[i, "Date"], "Money"] = data.loc[i, "Value"]
            elif data.loc[i, "Data Type"] == "Splits":
                df_rs.loc[data.loc[i, "Date"], "Stock"] = data.loc[i, "Value"].replace(":", "/")
            else:
                raise

        pd.Series(data.index).apply(transform)

        df_rs.fillna("NAN", inplace=True)
        df_rs["Money"] = df_rs["Money"].apply(lambda x: float(str(x).replace(",", "")) if x != "NAN" else "NAN")
        return df_rs

    def transform_NB_Mor_Full(self, data: pd.DataFrame):
        data = data.drop(columns=["Symbol"]).dropna(how="all").reset_index(drop=True)
        date_col = "Ex-Dividend Date"
        split_col = "Data Split"
        data[date_col] = pd.to_datetime(data[date_col], format="%b %d, %Y").dt.strftime("%d/%m/%Y")
        data[split_col] = pd.to_datetime(data[split_col], format="%b %d, %Y").dt.strftime("%d/%m/%Y")
        df_rs = pd.DataFrame({"Time": [], "Stock": [], "Money": []})
        df_rs.set_index("Time", inplace=True)

        if len(data) == 0:
            return df_rs

        for i in range(len(data)):
            if pd.notna(data.loc[i, date_col]):
                date = data.loc[i, date_col]
                if date in df_rs.index:
                    df_rs.loc[date, "Money"] += float(str(data.loc[i, "Amount"]).replace(",", ""))
                else:
                    df_rs.loc[date, "Money"] = float(str(data.loc[i, "Amount"]).replace(",", ""))
            else:
                date = data.loc[i, split_col]
                df_rs.loc[date, "Stock"] = data.loc[i, "Ratio"].replace(":", "/")

        df_rs["__T"] = pd.to_datetime(df_rs.index, format="%d/%m/%Y")
        df_rs.sort_values("__T", inplace=True, ascending=False)
        df_rs.pop("__T")

        return df_rs.fillna("NAN")

    def transform_all(self, src, folder_data, folder_result, print_status=False):
        '''
        src: Mor1 hoặc NB_Mor_Full
        '''
        folder_data = edit_folder_path(folder_data)
        folder_result = edit_folder_path(folder_result)

        count = 0
        if src == "Mor1" or src == "NB_Mor_Full":
            if src == "Mor1":
                transform_func = self.transform_Mor1_F0
            else:
                transform_func = self.transform_NB_Mor_Full

            list_path = os.listdir(folder_data)
            for ii in range(len(list_path)):
                path = list_path[ii]
                data = pd.read_csv(folder_data + path)
                df_rs = transform_func(data)
                if len(df_rs) > 0:
                    count += 1

                df_rs.to_csv(folder_result + path)

                if print_status: print(ii, path)

        if print_status: print("Số file không rỗng:", count)
