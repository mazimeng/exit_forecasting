import pandas as pd
import os
import datetime
import commons


def make_return(symbol, t_minus_n_days=[], t_plus_n_days=[], t_day_lag=5):
    csv_path = commons.data_dir(
        "strategy", "market", "adjusted_bars", symbol + ".csv")

    if not os.path.isfile(csv_path):
        print(csv_path, "does not exist")
        return []

    bars = pd.read_csv(csv_path, dtype={"trade_date": str})
    bars["trade_date_object"] = bars["trade_date"].apply(
        func=lambda x: datetime.datetime.strptime(x, "%Y%m%d"))
    bars = bars.sort_values(by=["trade_date_object"], ignore_index=True)

    days_asc = bars["trade_date_object"]
    first_day = days_asc.iloc[0]
    latest_day = days_asc.iloc[-1]
    # if len(bars) < max(365, t_minus_n_days[-1]) + t_plus_n_days[-1]:
    listed_days = (latest_day - first_day).days
    if listed_days < 365 * 2:
        return []

    unlock_day = first_day + datetime.timedelta(365)
    unlock_trading_day_index = None

    for rowi, row in bars.iterrows():
        if row["trade_date_object"] >= unlock_day:
            unlock_trading_day_index = rowi
            break

    T_day_index = unlock_trading_day_index - t_day_lag
    T_day_bar = bars.iloc[T_day_index]
    unlock_trading_day_bar = bars.iloc[unlock_trading_day_index]

    t_minus_n_days_bars = [bars.iloc[T_day_index - x]
                           for x in t_minus_n_days]
    t_minus_n_dates = [x["trade_date"] for x in t_minus_n_days_bars]
    t_minus_n_days_returns = [T_day_bar["close"] /
                              x["close"] - 1 for x in t_minus_n_days_bars]
    t_plus_n_days_bars = [bars.iloc[T_day_index + x]
                          for x in t_plus_n_days]
    t_plus_n_dates = [x["trade_date"] for x in t_plus_n_days_bars]
    t_plus_n_days_returns = [x["close"] /
                             T_day_bar["close"] - 1 for x in t_plus_n_days_bars]

    return [symbol,
            commons.format_date(first_day),
            commons.format_date(latest_day),
            unlock_trading_day_bar["trade_date"],
            unlock_trading_day_bar["close"],
            T_day_bar["trade_date"],
            T_day_bar["close"]] + \
        t_minus_n_days_returns + t_minus_n_dates + \
        t_plus_n_days_returns + t_plus_n_dates


def add_alpha(returns_df, t_plus_n_days):
    index_df = pd.read_csv(commons.data_dir("strategy", "market", "000300.SH.csv"),
                           dtype={"trade_date": str})
    index_close_map = dict([(row["trade_date"], row["close"])
                           for rowi, row in index_df.iterrows()])

    for rowi, row in returns_df.iterrows():
        t_day = row["T_day"]
        index_t_close = index_close_map[t_day]

        dates = [row[f"T+{x}_date"] for x in t_plus_n_days]
        index_closes = [index_close_map[x] for x in dates]
        returns_df.loc[rowi, [
            f"index_close_T+{x}" for x in t_plus_n_days]] = index_closes

        index_returns = [x / index_t_close - 1 for x in index_closes]
        returns_df.loc[rowi,  [
            f"index_return_T+{x}" for x in t_plus_n_days]] = index_returns

    R = returns_df.loc[:, [f"return_T+{x}" for x in t_plus_n_days]]
    B = returns_df.loc[:, [
        f"index_return_T+{x}" for x in t_plus_n_days]].values
    alpha_df = pd.DataFrame(
        (R-B).values, columns=[f"alpha_T+{x}" for x in t_plus_n_days])
    return pd.concat([returns_df, alpha_df], axis=1)


def make_returns():
    df = pd.read_excel(os.path.join("data", "新上市公司样本列表.xlsx"))

    t_plus_n_days = [20 * (x + 1) for x in range(12)]
    t_plus_n_days = [x for x in range(20, 240 + 1)]
    t_minus_n_days = [20]
    rows = []
    for rowi, row in df.iterrows():
        list_date = row["上市日期"]
        symbol = row["证券代码"]
        row = make_return(symbol, t_minus_n_days=t_minus_n_days,
                          t_plus_n_days=t_plus_n_days)
        if len(row) > 0:
            print(rowi, len(df), symbol, len(row))
            rows.append(row)

    result_df = pd.DataFrame(rows, columns=["symbol",
                                            "list_day",
                                            "latest_day",
                                            "unlock_day",
                                            "unlock_day_close",
                                            "T_day",
                                            "T_day_close"] +
                             [f"return_T-{x}" for x in t_minus_n_days] +
                             [f"T-{x}_date" for x in t_minus_n_days] +
                             [f"return_T+{x}" for x in t_plus_n_days] +
                             [f"T+{x}_date" for x in t_plus_n_days])

    result_df = add_alpha(result_df, t_plus_n_days)
    result_df.to_csv(os.path.join("data", "returns.csv"), index=False)


def main():
    make_returns()


main()
