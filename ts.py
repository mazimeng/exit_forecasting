import tushare as ts

def get_pro():
    ts.set_token("59f0f08cf0ab715513991f5be974338ae276237bae77490f78e7620a")
    pro = ts.pro_api()
    return pro