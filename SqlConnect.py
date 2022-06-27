import pyodbc
import pandas as pd

conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=KPCSGT-DB01.KPC.CO.ID;DATABASE=IHUB;UID=ihub;PWD=ihub')
cursor = conn.cursor()
#cursor.execute('SELECT * FROM wincc_table')

query = 'SELECT * FROM wincc_table'
df = pd.read_sql(query, conn)
#print(df.head(5))

df
#for row in cursor:
#    print('row = %r' % (row,))
