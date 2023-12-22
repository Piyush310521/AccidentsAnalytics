import requests # pip install requests
from consumer_details import CONSUMER_KEY , CONSUMER_SECRET, USERNAME, PASSWORD
import pandas as pd # pip install pandas 
import json
import duckdb
import matplotlib.pyplot as plt




DOMAIN ='https://pegasus44-dev-ed.develop.my.salesforce.com'

#generate Access token
def generate_token():
 DOMAIN ='https://pegasus44-dev-ed.develop.my.salesforce.com'
 payload = {
    'grant_type':'password',
    'client_id':CONSUMER_KEY ,
    'client_secret':CONSUMER_SECRET,
    'username': USERNAME,
    'password': PASSWORD
 }

 oauth_endpoint ='/services/oauth2/token'
 response = requests.post(DOMAIN + oauth_endpoint, data=payload)
 return response.json()

access_token = generate_token()['access_token']

print(access_token)

headers = {
 'Authorization': 'Bearer ' + access_token
}


# Contact data
response_data =requests.get(DOMAIN +'/services/apexrest/contactData',headers=headers)
print(response_data.json())

json_file_path = r'C:\Users\HP\Desktop\SF TO PYTHON\main.pyy\Contacts.json'
with open(json_file_path,'r') as file:
    data = json.load(file)
lst = []
for dict in data:
    df={
        'FirstName': dict.get('FirstName',0),
        'LastName': dict.get('LastName',0),
        'Email': dict.get('Email',0),
        'MobilePhone': dict.get('MobilePhone',0),
        'Phone': dict.get('Phone',0),
        'LeadSource':dict.get('LeadSource',0)
 }
    lst.append(df)
   
con = duckdb.connect(database=':memory:', read_only=False)
con.execute('CREATE TABLE salesforce_data (FirstName STRING, LastName STRING, Email STRING, MobilePhone STRING, Phone STRING, LeadSource STRING)')

# Insert data into DuckDB table
for row in lst:
       con.execute('INSERT INTO salesforce_data VALUES (?, ?, ?, ?, ?, ?)', (row['FirstName'], row['LastName'], row['Email'], row['MobilePhone'], row['Phone'], row['LeadSource']))
    
result = con.execute('SELECT * FROM salesforce_data')
duckdb.sql = result.fetchall()

# Display the retrieved data
for row in duckdb.sql:
     print(row)
duckdb.sql = pd.read_sql('SELECT * FROM salesforce_data', con)
print(duckdb.sql)
# Perform basic analysis with Pandas
print("Summary Statistics:")
print(duckdb.sql.describe())

# Group data by LeadSource and count occurrences
lead_source_counts = duckdb.sql['LeadSource'].value_counts()
print("\nLead Source Counts:")
print(lead_source_counts)

plt.figure(figsize=(10, 6))
duckdb.sql['LeadSource'].value_counts().plot(kind='bar')
plt.title('Lead Source Distribution')
plt.xlabel('Lead Source')
plt.ylabel('Count')
plt.show()

