import pandas as pd
import commons
import os


def make_factors_to_returns():
    returns_df = pd.read_csv(os.path.join("data", "returns.csv"),
                             dtype={"T_day": str})

    returns_df["T_day"] = returns_df["T_day"].str.replace("-", "")

    db = commons.connect_database()

    factors_to_analyze_df = pd.read_csv(
        os.path.join("data", "factors_to_analyze.csv"))
    groups_by_table = factors_to_analyze_df.groupby(by="table_name")
    for table_name, group in groups_by_table:
        factor_simple_names = sorted(list(group["column_name"]))
        factor_full_names = list(
            [f"{table_name}.{x}" for x in factor_simple_names])
        returns_df.loc[:, factor_full_names] = None
        print(table_name, factor_simple_names)
        for returni, return_row in returns_df.iterrows():
            symbol = return_row["symbol"]
            T_day = return_row["T_day"]

            factor_columns = ",".join([f"`{x}`" for x in factor_simple_names])
            sql = f"select {factor_columns} from {table_name} where sec_code = '{symbol}' and trade_date = '{T_day}';"
            symbol_factors_df = pd.read_sql(sql, con=db)
            if len(symbol_factors_df) == 0:
                continue
            factor_values = symbol_factors_df.loc[0, factor_simple_names]
            returns_df.loc[returni, factor_full_names] = list(factor_values)

    returns_df.to_csv(os.path.join(
        "data", "factors_to_returns.csv"), index=False)


make_factors_to_returns()
