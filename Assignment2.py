#importing the required libraries
import requests
import pandas as pd
import psycopg2
import json

#using request to get the data from json file
URL1 = "https://jsonplaceholder.typicode.com/todos"
response = requests.get(url = URL1)
word = response.json()
table1 = pd.DataFrame(word)

#converting the dataframe to csv
table1.to_csv('table1.csv')

# for second table
URL2 = "https://jsonplaceholder.typicode.com/users"

response = requests.get(url = URL2)

# flattning the file
table2 = pd.json_normalize(response.json())

#renaming the columns of the table
table2.rename(columns = {'address.street':'street','address.suite':'suite','address.city':'city',
                        'address.zipcode':'zipcode','address.geo.lat':'latitude',
                        'address.geo.lng':'longitude','company.name':'compname','company.catchPhrase':'company_catchphrase',
                        'company.bs':'company_bs'}, inplace=True)

table2.info()

#for the third file
URL3 = "https://jsonplaceholder.typicode.com/posts"

response = requests.get(url = URL3)
table3 = pd.json_normalize(response.json())

table3.info()

# for the fourth file
URL4 = "https://jsonplaceholder.typicode.com/comments"

response = requests.get(url = URL4)
table4 = pd.json_normalize(response.json())

table4.info()

table2.to_csv('table2.csv')
table3.to_csv('table3.csv')
table4.to_csv('table4.csv')

#to send the file pgadmin4
from sqlalchemy import create_engine, text
#import psycopg2

engine = create_engine('postgresql://postgres:star&dust@localhost:5432/postgres')
con = engine.connect()
'''
#can also do this by using psycopg2
con = psycopg2.connect(
   database="postgres", user='postgres', password='star&dust', host='localhost', port= '5432')

con.autocommit = True
'''
# getting the table names (should be empty)
print(engine.table_names())

#sending the csv file to pgadmin4 (table1)
table1.to_sql('Todos', engine)
print(engine.table_names())

#sending the dataframe to pgadmin4 (table2)
table2.to_sql('Users', engine)
print(engine.table_names())

#sending the dataframe to pgadmin4 (table3)
table3.to_sql('Post', engine)
print(engine.table_names())

#sending the dataframe to pgadmin4 (table4)
table4.to_sql('Comments', engine)
print(engine.table_names())

con.close()