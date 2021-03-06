import requests
import pandas as pd
import pyodbc
import sqlalchemy
pd.options.mode.chained_assignment = None
from datetime import datetime,timedelta

tanggal1 = datetime.now() 
tanggalstart = tanggal1.strftime('%Y-%m-%d')
tanggal2 = datetime.now() + timedelta(days=1)
tanggalstop = tanggal2.strftime('%Y-%m-%d')

params = {'tanggalstart': tanggalstart, 'tanggalstop': tanggalstop}
url = "http://XXXXX/api/DataBridge/GetDataBridge"

response = requests.get(url, params=params)
data1 = response.json()
df1 = pd.DataFrame.from_dict(data1)

## Filtering Column
df2=df1[['TransactionNumber','CompletionDate','TransactionStatus','VehicleName','GrossWeight','TareWeight','NetWeight','VehicleDescription','MaterialName','MaterialDescription','CDE_Fleet_Name','CDE_Pit_Name','CDE_Cluster_Name','CDE_Dumping_Name']]

## Select last transaction
conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=XXXXX;DATABASE=API_Timbang;UID=ihub;PWD=xxxx')
Sql_insert_query='''SELECT convert(datetime, COALESCE(MAX(CompletionDate),GETDATE()-2),126) FROM Data_Timbang '''

mycursor = conn.cursor()
mycursor.execute(Sql_insert_query)

result = mycursor.fetchone()

#Filtering data based on last transaction
result = result[0].strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]

## Filter DF
df_insert = df2[df2['CompletionDate'] > result ]

## Insert to SQL DB 
#conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=XXXXXX;DATABASE=API_Timbang;UID=ihub;PWD=xxxx')
#Sql_insert_query='''INSERT INTO Data_Timbang (TransactionStatus) VALUES ('Complete')'''
#cursor = conn.cursor()
#cursor.execute(Sql_insert_query)

constring = "mssql+pyodbc://ihub:ihub@XXXXXX/API_Timbang?driver=ODBC+Driver+17+for+SQL+Server"
engine = sqlalchemy.create_engine(constring, echo=False)

df_insert.to_sql('Data_Timbang', con=engine, if_exists="append", index=False)

#close sql connection
conn.commit()
mycursor.close() 
conn.close() 

## Check inserted Data
conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=XXXXX;DATABASE=API_Timbang;UID=ihub;PWD=xxxx')
query = 'SELECT * FROM Data_Timbang'
df = pd.read_sql(query, conn)
conn.close() 
df
