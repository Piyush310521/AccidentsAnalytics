
import pandas as pd # pip install pandas 
import duckdb
import matplotlib.pyplot as plt
xlsx_path = 'C:/Users/HP/Desktop/SF TO PYTHON/Road Accidents.xlsx'
df = pd.read_excel(xlsx_path)
json_data = df.to_json(orient='records')
print(json_data)
with open('output.json', 'w') as json_file:
    json_file.write(json_data)
with open('duck.db', 'w') as duckdb_file:
    duckdb_file.write(json_data)

con = duckdb.connect(database=':duck.db:', read_only=False)

# Create a DuckDB table and insert data
con.create_table('road_accidents', df)

# Perform SQL queries on the DuckDB table
result = con.execute('SELECT * FROM road_accidents WHERE Severity = 3').fetchdf()

# Display the result
print("\nFiltered Data:")
print(result.head())

# Data Visualization
plt.figure(figsize=(10, 6))
result['Day_of_Week'].value_counts().sort_index().plot(kind='bar')
plt.title('Distribution of Road Accidents by Day of Week')
plt.xlabel('Day of Week')
plt.ylabel('Count')
plt.show()

   







