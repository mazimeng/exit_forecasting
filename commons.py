import os
from sqlalchemy import create_engine

DATA_DIR = os.path.join("D:\\", "data")

def data_dir(*sub_dirs):
    return os.path.join(DATA_DIR, *sub_dirs)

class FactorFinder:
    def __init__(self) -> None:
        self.engine = None
    
    def start():
        engine = create_engine('mysql+pymysql://root:0@localhost:3306/strategy')

def connect_database():
    return create_engine('mysql+pymysql://root:0@localhost:3306/strategy')

def format_date(date_object):
    return date_object.strftime("%Y%m%d")