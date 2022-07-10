
# Importing python libraries
import pandas as pd
import urllib.parse
import configparser

config = configparser.ConfigParser()

config.read(r'C:/Users/Siva Doddapaneni/Downloads/mecca-coding-task/mecca-coding-task/config/code_config.ini')

inputpath=config.get('paths','input')
outputpath=config.get('paths','output')

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

#Assignment Results.

#Credit Card Transactions per Month
df_1=pd.read_sql(""" select right(rtrim(Transaction_date),6) as  transaction_month,format(sum(Transaction_Value),'C') Transaction_value_month
                     from  Transaction_Base group by right(rtrim(Transaction_date),6) """,engine)

df_1.to_json(outputpath+'CC_Transaction_By_Month.json')

print("Data Written to CC_Transaction_By_Month.json file") 

df_1.head(20)

#transaction amount by Customer_Segment

df_2=pd.read_sql("""  select a.Customer_Segment,format(sum(c.Transaction_Value),'C' )as Transaction_Value  from Customer_Base a inner join  Card_Base b 
					  on a.cust_id=b.Cust_ID
					  inner join Transaction_Base c on b.card_number=c.Credit_Card_ID
					  group by a.Customer_Segment;""",engine)
					  
					  
df_2.to_json(outputpath+'transaction_amount_by_Customer_Segment.json')

print("Data Written to transaction_amount_by_Customer_Segment.json file") 

df_2.head(100)


#Average age of customers by Customer_Segment

df_3=pd.read_sql("""  select customer_segment,avg(age) as customer_average_age from  Customer_Base
                     group by customer_segment;""",engine)
					  
					  
df_3.to_json(outputpath+'Avg_age_cust_by_cust_segment.json')

print("Data Written to Avg_age_cust_by_cust_segment.json file") 

df_3.head(50)


#Minimum, Max and Average credit card transaction value by Card_Family

df_4=pd.read_sql("""  select a.Card_Family,format(min(Transaction_Value),'C') as min_Transaction_Value,format(max(Transaction_Value),'C') as max_Transaction_Value,
                      format(avg(Transaction_Value),'c') as avg_Transaction_Value from Card_Base a inner join Transaction_Base b
                      on a.card_number=b.Credit_Card_ID group by a.Card_Family;""",engine)
					  
					  
df_4.to_json(outputpath+'min_max_avg_cc_by_card_family.json')

print("Data Written to min_max_avg_cc_by_card_family.json file")

df_4.head(50)


#Top 5 customers who have made the most credit card transaction spend (I Assumed this as Top 5 customers who made highest number of transaction)

df_5=pd.read_sql(""" select top 5 a.Cust_ID,count(c.transaction_value) as transaction_value from card_base a inner join customer_base b on a.Cust_ID=b.Cust_ID
                     inner join 
                     Transaction_Base c  on c.Credit_Card_ID=a.Card_Number 
                     GROUP by a.cust_id
                     order by transaction_value desc;""",engine)
					  
					  
df_5.to_json(outputpath+'top5_customers_by_CC_Transaction.json')

print("Data Written to top5_customers_by_CC_Transaction.json file")

df_5.head(10)

#Top 5 customers who have made the most credit card transaction spend (If this is in terms of total value of transactions)

df_6=pd.read_sql(""" select top 5 a.Cust_ID,sum(c.transaction_value) as transaction_value from card_base a inner join customer_base b on a.Cust_ID=b.Cust_ID
                     inner join 
                     Transaction_Base c  on c.Credit_Card_ID=a.Card_Number 
                     GROUP by a.cust_id
                     order by transaction_value desc;""",engine)
					  
					  
df_6.to_json('C:/Users/Siva Doddapaneni/Downloads/mecca-coding-task/mecca-coding-task/output/top5_customers_by_CC_Transaction_value.json')

print("Data Written to top5_customers_by_CC_Transaction_value.json file")

df_6.head(10)
					  