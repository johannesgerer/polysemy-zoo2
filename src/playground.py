import pandas as pd
import pyodbc
from datetime import datetime
from sqlalchemy.engine import URL, create_engine

import os
os.environ['msoPy']

# os.system("odbcinst -j")
# os.system("odbcinst -q -s")
# os.system("odbcinst -q -d")

cs = f"DRIVER={os.environ['msoPy']};server=localhost;Authentication=SqlPassword;UID=sa;PWD=asd@@@ASD123;Encrypt=No;database=db1;"

if 0:
    c = pyodbc.connect()

engine = create_engine(URL.create("mssql+pyodbc", query={"odbc_connect": cs}))

a = pd.DataFrame({'stock':['aapl'],
                  'val1':['\u4234'],
                  'date':[datetime(2022,3,23)],
                  'val2':[2.232]})

b = pd.read_sql("select * from marketdata", engine)

if 0:
    a.to_sql('marketdata',engine, if_exists='append', index=False)
