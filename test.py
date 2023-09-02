import ts
import os

pro = ts.get_pro()
df = pro.index_daily(ts_code='000300.SH')
df = df.sort_values(by=["trade_date"], ignore_index=True)
df.to_csv(os.path.join("data", "000300.SH.csv"), index=False)
print(df)