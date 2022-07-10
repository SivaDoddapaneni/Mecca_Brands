import pandas as pd
import urllib.parse
import configparser

config = configparser.ConfigParser()

config.read(r'C:/Users/Siva Doddapaneni/Downloads/mecca-coding-task/mecca-coding-task/config/code_config.ini')

inputpath=config.get('paths','input')

server=config.get('server','Server')

database=config.get('database','Database')

uid=config.get('credentials','userid')
password=config.get('credentials','password')

#Connecting AZURE SQL Server
params = urllib.parse.quote_plus(
    'Driver=%s;' % '{ODBC Driver 17 for SQL Server}' +
    'Server=%s,1433;' % server+
    'Database=%s;' % database +
    'Uid=%s;' % uid  +
    'Pwd={%s};' % password +
    'Encrypt=no;' +
    'TrustServerCertificate=no;'
    )
    
from sqlalchemy.engine import create_engine
conn_str = 'mssql+pyodbc:///?odbc_connect=' + params
engine = create_engine(conn_str)
        
connection = engine.connect()
connection


# Validating Customer Base Data


df_c=pd.read_sql("select * from Customer_Base",engine)

df_c.head(20)

cust_count=pd.read_sql("select count(*) from Customer_Base",engine)

print("Customer Records Count: -"+str(cust_count))

df_len_cust=pd.read_sql("""select distinct len(Cust_ID) len_cust_id,len(age) len_age ,len(customer_segment) len_customer_segment,
                           customer_segment,len(Customer_Vintage_Group) len_Customer_Vintage_Group
                           from Customer_Base;""",engine)

df_len_cust.head()

# Validating Card Base Data

df_card=pd.read_sql("select * from Card_Base",engine)
df_card.head(20)
card_count=pd.read_sql("select count(*) from Card_Base",engine)
print("Card Records Count: -"+str(card_count))

df_len_card=pd.read_sql("""select distinct len(Card_Number) as Len_Card_Number ,LEN(Card_Family) as len_Card_Family
                         ,Card_Family,len(Cust_ID) as len_cust_id  from Card_Base;""",engine)

df_len_card.head()




# Validating Transaction Base Data

df_trans=pd.read_sql("select * from TransactionBase_1",engine)

df_trans.head(20)

transaction_count=pd.read_sql("select count(*) from transaction_Base",engine)

print("Trnsaction Records Count: -"+str(transaction_count))


df_len_transaction=pd.read_sql(""" select distinct LEN(Transaction_ID) len_Transaction_id
                                   ,LEN(Credit_Card_ID) len_credit_card,LEN(Transaction_Segment) len_transaction_segment
                                   from Transaction_Base;""",engine)
df_len_transaction.head()

