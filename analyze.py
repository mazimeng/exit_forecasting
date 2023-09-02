import math
import pandas as pd
import commons
import os

factors_to_analyze_df = pd.read_csv(
    os.path.join("data", "factors_to_analyze.csv"))
factors_to_returns_df = pd.read_csv(
    os.path.join("data", "factors_to_returns.csv"))

factor_full_names = factors_to_analyze_df["table_name"].str.cat(
    factors_to_analyze_df["column_name"], sep=".")

num_groups = 5

windows = [20 * (x + 1) for x in range(12)]
rows = []
for window in windows:
    for factor_full_name in factor_full_names:
        return_col = f"return_T+{window}"
        alpha_col = f"alpha_T+{window}"
        y_columns = [return_col, alpha_col]
        factors_df = pd.DataFrame(
            factors_to_returns_df.loc[~factors_to_returns_df[factor_full_name].isna()])
        num_rows = len(factors_df)

        if num_rows == 0:
            print(factor_full_name, window, "is emtpy")
            continue

        rows_per_group = int(math.ceil(float(num_rows) / num_groups))
        factors_df = factors_df.sort_values(
            by=[factor_full_name], ignore_index=True, ascending=True)
        factors_df["group"] = factors_df.index // rows_per_group
        groups = factors_df.groupby(by=["group"])
        means = groups[y_columns].mean()
        medians = groups[y_columns].median()
        result = pd.DataFrame()

        result[[f"{col}.mean" for col in y_columns]] = means[y_columns]
        result[[f"{col}.median" for col in y_columns]] = medians[y_columns]
        means.to_csv(os.path.join("data", "intermediate",
                     f"{factor_full_name}.{window}.mean.csv"))
        medians.to_csv(os.path.join(
            "data", "intermediate", f"{factor_full_name}.{window}.median.csv"))
        result.to_csv(os.path.join("data", "intermediate",
                                   f"{factor_full_name}.{window}.combined.csv"))

        top_mean_return = means.iloc[0][return_col]
        bottom_mean_return = means.iloc[-1][return_col]
        top_minus_bottom_mean_return = top_mean_return - bottom_mean_return

        top_median_return = medians.iloc[0][return_col]
        bottom_median_return = medians.iloc[-1][return_col]
        top_minus_bottom_median_return = top_median_return - bottom_median_return

        top_mean_alpha = means.iloc[0][alpha_col]
        bottom_mean_alpha = means.iloc[-1][alpha_col]
        top_minus_bottom_mean_alpha = top_mean_alpha - bottom_mean_alpha

        top_median_alpha = medians.iloc[0][alpha_col]
        bottom_median_alpha = medians.iloc[-1][alpha_col]
        top_minus_bottom_median_alpha = top_median_alpha - bottom_median_alpha

        row = [factor_full_name, window,
               top_mean_return, bottom_mean_return, top_minus_bottom_mean_return,
               top_median_return, bottom_median_return, top_minus_bottom_median_return,
               top_mean_alpha, bottom_mean_alpha, top_minus_bottom_mean_alpha,
               top_median_alpha, bottom_median_alpha, top_minus_bottom_median_alpha,
               num_rows, rows_per_group]
        rows.append(row)

columns = ["因子名", "窗口", "Top组平均回报", "Bottom组平均回报", "TOP-BOTTOM回报", "Top组中位数回报", "Bottom组中位数回报", "TOP-BOTTOM中位数回报",
           "Top组平均超额回报", "Bottom组平均超额回报", "TOP-BOTTOM超额回报", "Top组中位数超额回报", "Bottom组中位数超额回报", "TOP-BOTTOM中位数超额回报",
           "num_rows", "rows_per_group"]
analysis = pd.DataFrame(rows, columns=columns)

with pd.ExcelWriter(os.path.join("data", "analysis.xlsx")) as writer:
    groups = analysis.groupby(by="窗口")
    for groupi, group in groups:
        group.to_excel(writer, sheet_name=f"T+{groupi}", index=False)

# analysis.to_excel(os.path.join("data", "analysis.xlsx"), index=False)
