import pandas as pd
import sqlite3
# Load the CSV file into a pandas DataFrame
file_path = r"C:\Users\hp\Desktop\assignment\dataset.csv"  # Update with your actual path
df = pd.read_csv(file_path)
# Connect to SQLite database (this will create the database file if not exists)
conn = sqlite3.connect('sales_data.db')
cursor = conn.cursor()
cursor.execute('DROP TABLE IF EXISTS products')
cursor.execute('DROP TABLE IF EXISTS customers')
cursor.execute('DROP TABLE IF EXISTS orders')
cursor.execute('DROP TABLE IF EXISTS sales')
# Create the Products Table (stores product information)
cursor.execute('''
CREATE TABLE IF NOT EXISTS products (
    product_code TEXT PRIMARY KEY,
    product_line TEXT,
    msrp REAL
)
''')
# Create the Customers Table (stores customer information)
cursor.execute('''
CREATE TABLE IF NOT EXISTS customers (
    customer_name TEXT,
    phone TEXT,
    address_line1 TEXT,
    address_line2 TEXT,
    city TEXT,
    state TEXT,
    postal_code TEXT,
    country TEXT,
    territory TEXT,
    contact_first_name TEXT,
    contact_last_name TEXT
)
''')
cursor.execute('DROP TABLE IF EXISTS orders')
cursor.execute('''
CREATE TABLE orders (
    order_number INTEGER PRIMARY KEY,
    order_date TEXT,
    status TEXT,
    quarter INTEGER,
    month INTEGER,
    year INTEGER
)
''')
# Create the Orders Table (stores order details)
cursor.execute('DROP TABLE IF EXISTS orders')
cursor.execute('''
CREATE TABLE orders (
    order_number INTEGER PRIMARY KEY,
    order_date TEXT,
    status TEXT,
    quarter INTEGER,
    month INTEGER,
    year INTEGER
)
''')
cursor.execute('DROP TABLE IF EXISTS sales')
cursor.execute('''
CREATE TABLE sales (
    order_line_number INTEGER,
    quantity_ordered INTEGER,
    price_each REAL,
    sales REAL,
    order_number INTEGER,
    deal_size TEXT,
    FOREIGN KEY (order_number) REFERENCES orders(order_number)
)
''')
# Create the Sales Table (stores sales information like deal size)
cursor.execute('''
CREATE TABLE IF NOT EXISTS sales (
    order_line_number INTEGER,
    quantity_ordered INTEGER,
    price_each REAL,
    sales REAL,
    order_number INTEGER,
    deal_size TEXT,
    FOREIGN KEY (order_number) REFERENCES orders(order_number)
)
''')
print("Tables created successfully!")
# Commit the changes to create the tables
conn.commit()
# Insert data into the products table
for index, row in df.iterrows():
    cursor.execute('''
    INSERT OR REPLACE INTO products (product_code, product_line, msrp)
    VALUES (?, ?, ?)
    ''', (row['PRODUCTCODE'], row['PRODUCTLINE'], row['MSRP']))
# Insert data into the customers table
for index, row in df.iterrows():
    cursor.execute('''
    INSERT OR REPLACE INTO customers (customer_name, phone, address_line1, address_line2, city, state, postal_code, country, territory, contact_first_name, contact_last_name)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (row['CUSTOMERNAME'], row['PHONE'], row['ADDRESSLINE1'], row['ADDRESSLINE2'], row['CITY'], row['STATE'], row['POSTALCODE'], row['COUNTRY'], row['TERRITORY'], row['CONTACTFIRSTNAME'], row['CONTACTLASTNAME']))
# Insert data into the orders table
for index, row in df.iterrows():
    cursor.execute('''
    INSERT OR REPLACE INTO orders (order_number, order_date, status, quarter, month, year)
    VALUES (?, ?, ?, ?, ?, ?)
    ''', (row['ORDERNUMBER'], row['ORDERDATE'], row['STATUS'], row['QTR_ID'], row['MONTH_ID'], row['YEAR_ID']))
# Insert data into the sales table
for index, row in df.iterrows():
    # Calculate the sales value (QUANTITYORDERED * PRICEEACH)
    sales_value = row['QUANTITYORDERED'] * row['PRICEEACH']
    cursor.execute('''
    INSERT INTO sales (order_line_number, quantity_ordered, price_each, sales, order_number, deal_size)
    VALUES (?, ?, ?, ?, ?, ?)
    ''', (row['ORDERLINENUMBER'], row['QUANTITYORDERED'], row['PRICEEACH'], sales_value, row['ORDERNUMBER'], row['DEALSIZE']))
print("Data Inserted into the Tables successfully!")
# Commit the changes and close the connection
conn.commit()
conn.close()

#Step 2: Remove Duplicates
df.drop_duplicates(inplace=True)

# Step 3: Handle Missing Values (replace with defaults)
df.fillna({
    'STATE': 'N/A',
    'POSTALCODE': 0,
    'TERRITORY': 'N/A'
}, inplace=True)
# Step 4: Convert ORDERDATE to datetime and extract date parts
df['ORDERDATE'] = pd.to_datetime(df['ORDERDATE'], errors='coerce')
df['ORDER_YEAR'] = df['ORDERDATE'].dt.year
df['ORDER_MONTH'] = df['ORDERDATE'].dt.month
df['ORDER_DAY'] = df['ORDERDATE'].dt.day

# Step 5: Calculate total sales if not already present
df['CALC_SALES'] = df['QUANTITYORDERED'] * df['PRICEEACH']

# Step 6: Create a Deal Size category
def deal_size_category(x):
    if x < 3000:
        return 'Small'
    elif x < 6000:
        return 'Medium'
    else:
        return 'Large'

df['DEAL_SIZE_CAT'] = df['CALC_SALES'].apply(deal_size_category)

# Step 7: Clean text columns
df['CUSTOMERNAME'] = df['CUSTOMERNAME'].str.strip().str.title()
df['CITY'] = df['CITY'].str.strip().str.title()
df['COUNTRY'] = df['COUNTRY'].str.upper()

# Step 8: Add a Profit column (example: assume 25% profit margin)
df['PROFIT'] = df['CALC_SALES'] * 0.25

# Step 9: Display transformed data
print(df[['ORDERNUMBER', 'CUSTOMERNAME', 'CALC_SALES', 'DEAL_SIZE_CAT', 'PROFIT']].head())

# Step 10: Save the transformed DataFrame to a new CSV file
df.to_csv("C:/Users/hp/Desktop/assignment/transformed_sales.csv", index=False)
print("✅ Transformations complete! File saved as 'transformed_sales.csv'")







