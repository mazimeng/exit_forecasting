import uuid
import pandas as pd
from sqlalchemy import create_engine
import os
import datetime

import zipfile
from pathlib import Path
import hashlib
import commons
import sqlalchemy

def unzip(f, output_dir, encoding="utf-8", verbose=1):
    with zipfile.ZipFile(f) as z:
        for fileinfo in z.infolist():
            n = fileinfo.filename.encode('cp437').decode(encoding)
            full = os.path.join(output_dir, n)
            if verbose > 0:
                print(output_dir, n, fileinfo.is_dir())

            if fileinfo.is_dir():
                parent_dir = full
            else:
                parent_dir, fname = os.path.split(full)
            if not os.path.exists(parent_dir):
                os.makedirs(parent_dir, exist_ok=True)

            if not fileinfo.is_dir():
                with open(full, 'wb') as w:
                    w.write(z.read(fileinfo.filename))


def add_pk(db, table_name):
    sql = f"select * from information_schema.table_constraints where table_name = '{table_name}' and table_schema = 'strategy' and constraint_name = 'PRIMARY';"
    pk_df = pd.read_sql(sql, con=db)
    if len(pk_df) > 0:
        print(table_name, "pk ok")
        return

    # fix trade_date and sec_code type
    print(table_name, "fixing  trade_date and sec_code type")
    sql_fix_column_type = f"ALTER TABLE {table_name} MODIFY trade_date varchar(64), MODIFY sec_code varchar(64);"
    with db.connect() as conn:
        conn.execute(sqlalchemy.text(sql_fix_column_type))

    print(table_name, "adding pk")
    sql_add_pk = f"ALTER TABLE {table_name} ADD CONSTRAINT pk_{table_name} PRIMARY KEY (sec_code, trade_date);"
    with db.connect() as conn:
        conn.execute(sqlalchemy.text(sql_add_pk))


def fix_pk():
    db = commons.connect_database()
    table_names = ["financial_quality_capital_structure_factor",
                   "financial_quality_earnings_quality_factor",
                   "financial_quality_improvement_factor",
                   "financial_quality_operational_capability_factor",
                   "financial_quality_per_shares_factor",
                   "financial_quality_profitability_factor",
                   "financial_quality_solvency_factor",
                   "growth_factor",
                   "intangible_factor",
                   "investment_factor",
                   "leverage_factor",
                   "liquidity_factor",
                   "momentum_factor",
                   "shareholders_factor",
                   "size_factor",
                   "technical_indicators_momentum_reversal",
                   "technical_indicators_overbought_oversold",
                   "technical_indicators_trending",
                   "technical_indicators_volatility",
                   "technical_indicators_volume",
                   "value_factor",
                   "volatility_factor"]
    for table_name in table_names:
        add_pk(db, table_name)


def load_factors():
    data_dir = os.path.join("D:\\", "data", "strategy", "factors")
    engine = create_engine('mysql+pymysql://root:0@localhost:3306/strategy')

    sync_df = pd.read_sql("select * from synchronizations", con=engine)
    sync_map = dict([(row["file_path"], row["state"])
                    for rowi, row in sync_df.iterrows()])

    # df = pd.read_sql("show index from financial_quality_capital_structure_factor", con=engine)
    # print(df)
    # return

    whitelist = {"financial_quality_capital_structure_factor.csv"}
    whitelist = {}
    for root, dirs, files in os.walk(data_dir):
        for name in files:
            file_path = os.path.join(root, name)
            if not name.endswith(".csv"):
                continue
            if len(whitelist) > 0 and name not in whitelist:
                continue

            if file_path in sync_map and sync_map[file_path] == "ok":
                print(file_path, "skipped")
                continue
            print(file_path, "loading")

            directory, file_name = os.path.split(file_path)
            table_name = file_name.split(".")[0]

            dtype = {"trade_date": str}
            df = pd.read_csv(file_path, dtype=dtype)
            df["trade_date"] = df["trade_date"].str.replace("-", "")

            # df["id"] = df.apply(func=lambda x: int(hashlib.sha1(
            #     (x["trade_date"] + x["sec_code"]).encode("utf-8")).hexdigest(), 16), axis=1)

            with engine.connect() as conn:
                with conn.begin() as tx:
                    df.to_sql(table_name,
                              con=conn,
                              if_exists="append",
                              method="multi",
                              chunksize=1000,
                              index=False)
                    sync = pd.DataFrame([[str(file_path), "ok", datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")]],
                                        columns=["file_path", "state", "updated_at"])
                    sync.to_sql("synchronizations", con=conn,
                                if_exists="append", index=False)
            add_pk(engine, table_name)


def main():
    load_factors()


if __name__ == '__main__':
    main()
