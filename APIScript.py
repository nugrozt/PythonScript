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
url = "http://192.168.190.21:3810/api/DataBridge/GetDataBridge"

response = requests.get(url, params=params)
data1 = response.json()

## Filtering Column
df2=df1[['TransactionNumber','CompletionDate','TransactionStatus','VehicleName','GrossWeight','TareWeight','NetWeight','VehicleDescription','MaterialName','MaterialDescription','CDE_Fleet_Name','CDE_Pit_Name','CDE_Cluster_Name','CDE_Dumping_Name']]

## Select last transaction
conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=KPCSGT-DB01.KPC.CO.ID;DATABASE=API_Timbang;UID=ihub;PWD=ihub')
Sql_insert_query='''SELECT convert(datetime, COALESCE(MAX(CompletionDate),GETDATE()-2),126) FROM Data_Timbang '''

mycursor = conn.cursor()
mycursor.execute(Sql_insert_query)

result = mycursor.fetchone()
result = result[0].strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]

mycursor.close() 
conn.close() 

## Filter DF
df_insert = df2[df2['CompletionDate'] > result ]
#df_insert

## Insert to SQL DB 
conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=KPCSGT-DB01.KPC.CO.ID;DATABASE=API_Timbang;UID=ihub;PWD=ihub')
#Sql_insert_query='''INSERT INTO Data_Timbang (TransactionStatus) VALUES ('Complete')'''
#cursor = conn.cursor()

#cursor.execute(Sql_insert_query)

constring = "mssql+pyodbc://ihub:ihub@KPCSGT-DB01.KPC.CO.ID/API_Timbang?driver=ODBC+Driver+17+for+SQL+Server"
engine = sqlalchemy.create_engine(constring, echo=False)

df_insert.to_sql('Data_Timbang', con=engine, if_exists="append", index=False)

conn.commit()

## Check inserted Data
query = 'SELECT * FROM Data_Timbang'
df = pd.read_sql(query, conn)
conn.close() 
df
