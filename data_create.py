import sqlite3
import random
import string
import pandas as pd

data = pd.read_csv('Sales_Records.csv')
# Connect to SQLite database (creates it if it doesn't exist)
conn = sqlite3.connect('sales_records.db')
cursor = conn.cursor()

# Create a sample table
cursor.execute('''
CREATE TABLE IF NOT EXISTS sales (
    Country VARCHAR(255) NOT NULL,
	Price FLOAT NOT NULL,
    Cost FLOAT NOT NULL,
    Revenue FLOAT NOT NULL,
    TotalCost FLOAT NOT NULL
);
''')

# Insert data from CSV into the database
data.to_sql('sales', conn, if_exists='append', index=False)

# Commit and close the connection
conn.commit()
conn.close()