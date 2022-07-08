
# Importing python libraries


import os
import glob
import pandas as pd
import ntpath

import urllib.parse
#Connecting AZURE SQL Server
params = urllib.parse.quote_plus(
    'Driver=%s;' % '{ODBC Driver 17 for SQL Server}' +
    'Server=%s,1433;' % 'code-dev.database.windows.net' +
    'Database=%s;' % 'code_dev' +
    'Uid=%s;' % 'code_dev'  +
    'Pwd={%s};' % 'Shiva9999!' +
    'Encrypt=no;' +
    'TrustServerCertificate=no;'
    )
    
from sqlalchemy.engine import create_engine
conn_str = 'mssql+pyodbc:///?odbc_connect=' + params
engine = create_engine(conn_str)
        
connection = engine.connect()
connection



#Reading CardBase CSV File
df_card=pd.read_csv('C:/Users/Siva Doddapaneni/Downloads/mecca-coding-task/mecca-coding-task/data/CardBase.csv')

df_card.info()

print("Number of records in CardBase File:-"+str(len(df_card)))  

#Loading Card_Base data to sql server


df_card.to_sql(
    'Card_Base',
    engine, 
    if_exists='replace', 
    chunksize=1000, 
    index=False)
   
   
print("Card_Base is Loaded to Azure")
 


# In[10]:

#Reading CustomerBase CSV File

df_cust=pd.read_csv('C:/Users/Siva Doddapaneni/Downloads/mecca-coding-task/mecca-coding-task/data/CustomerBase.csv')
df_cust.info()
print("Number of records in CustomerBase File:-"+str(len(df_cust)))    

#Loading Customer_Base data to sql server

df_cust.to_sql(
    'Customer_Base',
    engine, 
    if_exists='replace', 
    chunksize=1000, 
    index=False)
   
   
print("Customer_Base is Loaded to Azure")

#Reading TransactionBase CSV File

df_transaction=pd.read_csv('C:/Users/Siva Doddapaneni/Downloads/mecca-coding-task/mecca-coding-task/data/TransactionBase.csv')
df_transaction.info()
print("Number of records in TransactionBase File:-"+str(len(df_transaction))) 

#Loading transaction_Base data to sql server

df_transaction.to_sql(
     'Transaction_Base',
     engine, 
     if_exists='replace', 
     chunksize=1000, 
     index=False)
    
    
print("Transaction_Base is loadedto Azure")






