import pandas as pd
import numpy as np
import os
folder_price_ = "MergedPrice/"
folder_dividend_ = "MergedDividend/"
list_price_ = os.listdir(folder_price_)
list_dividend_ = os.listdir(folder_dividend_)


list_infor = []
for ii in range(len(list_price_)):
    path = list_price_[ii]
    if ii % 100 == 0:
        print(ii, path)

    data = pd.read_csv(folder_price_+path)
    data["Time"] = pd.to_datetime(data["Time"], format="%d/%m/%Y").dt.strftime("%Y-%m-%d")
    data.set_index("Time", inplace=True)
    data.sort_index(ascending=False, inplace=True)

    if path in list_dividend_:
        dividend = pd.read_csv(folder_dividend_+path)
    else:
        dividend = pd.DataFrame({"Time":[], "Stock":[], "Money":[]})

    dividend["Time"] = pd.to_datetime(dividend["Time"], format="%d/%m/%Y").dt.strftime("%Y-%m-%d")
    dividend.set_index("Time", inplace=True)
    dividend.sort_index(ascending=False, inplace=True)

    list_year = data.index.str[:4].unique()
    for year in list_year:
        df_buy = data[data.index.str.startswith(year+"-04-")]
        if len(df_buy) > 0:
            time_buy = df_buy.index[-1]
            buy = df_buy.loc[time_buy, "PriceClosed"]
        else:
            time_buy = "NAN"
            buy = -1.0

        df_sell = data[data.index.str.startswith(str(int(year)+1)+"-03-")]
        if len(df_sell) > 0:
            time_sell = df_sell.index[-1]
            sell = df_sell.loc[time_sell, "PriceClosed"]
        else:
            time_sell = "NAN"
            sell = -1

        if time_buy != "NAN" and time_sell != "NAN":
            df_divi = dividend[(dividend.index > time_buy) & (dividend.index <= time_sell)]
            for stock in df_divi["Stock"]:
                if stock != "NAN":
                    a, b = stock.split("/")
                    buy = buy / float(a) * float(b)

        if buy != -1 and sell != -1:
            profit = float(sell) / float(buy)
        else:
            profit = -1

        list_infor.append({
            "Time": int(year),
            "Symbol": path.split(".csv")[0],
            "Profit": profit,
            "Buy": buy,
            "Sell": sell,
            "Start": time_buy,
            "End": time_sell
        })

result = pd.DataFrame(list_infor)