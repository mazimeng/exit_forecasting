# 代码说明
- returns.py，读本地的股票日线和000300.SH日线，生成return和alpha
- factors_to_returns.py，把因子库的因子跟returns.py生成的结果拼在一起，形成T日的因子和T+[0...240]日的收益
- analyze.py，把factors_to_returns.py的结果按照因子排序后分5组，生成T+[20, 40, ...240]日每组平均收益
