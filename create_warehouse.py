import pandas as pd 
import sqlite3

# Connect to the new warehouse database (this will create it if it doesn't exist)
conn_warehouse = sqlite3.connect('warehouse.db')
cursor_warehouse = conn_warehouse.cursor()

# Connect to the original sales database (sales_data.db)
conn_sales = sqlite3.connect('sales_data.db')
cursor_sales = conn_sales.cursor()

print("✅ Connected to warehouse and sales data.")
# STEP 1: Create Dimension Tables in warehouse.db

# Create dim_date table in warehouse
cursor_warehouse.execute('''
CREATE TABLE IF NOT EXISTS dim_date (
    date_key INTEGER PRIMARY KEY,
    order_date TEXT,
    order_year INTEGER,
    order_month INTEGER,
    order_day INTEGER
)
''')

# Create dim_customer table in warehouse
cursor_warehouse.execute('''
CREATE TABLE IF NOT EXISTS dim_customer (
    customer_id INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_name TEXT,
    city TEXT,
    state TEXT,
    country TEXT
)
''')

# Create dim_product table in warehouse
cursor_warehouse.execute('''
CREATE TABLE IF NOT EXISTS dim_product (
    product_code TEXT PRIMARY KEY,
    product_line TEXT
)
''')

# Create fact_sales table in warehouse
cursor_warehouse.execute('''
CREATE TABLE IF NOT EXISTS fact_sales (
    sales_id INTEGER PRIMARY KEY AUTOINCREMENT,
    order_number INTEGER,
    quantity_ordered INTEGER,
    price_each REAL,
    calc_sales REAL,
    profit REAL,
    deal_size_cat TEXT,
    product_code TEXT,
    customer_id INTEGER,
    date_key INTEGER,
    FOREIGN KEY (product_code) REFERENCES dim_product(product_code),
    FOREIGN KEY (customer_id) REFERENCES dim_customer(customer_id),
    FOREIGN KEY (date_key) REFERENCES dim_date(date_key)
)
''')

# STEP 2: Load Data from sales_data.db and insert into warehouse.db

# Load data from dim_date in sales_data.db
dim_date_data = pd.read_sql_query("SELECT * FROM dim_date", conn_sales)

# Insert data into dim_date in warehouse.db
dim_date_data.to_sql('dim_date', conn_warehouse, if_exists='replace', index=False)


# Load data from dim_customer in sales_data.db
dim_customer_data = pd.read_sql_query("SELECT * FROM dim_customer", conn_sales)

# Insert data into dim_customer in warehouse.db
dim_customer_data.to_sql('dim_customer', conn_warehouse, if_exists='append', index=False)

# Load data from dim_product in sales_data.db
dim_product_data = pd.read_sql_query("SELECT * FROM dim_product", conn_sales)

# Insert data into dim_product in warehouse.db
dim_product_data.to_sql('dim_product', conn_warehouse, if_exists='append', index=False)

# Load data from fact_sales in sales_data.db
fact_sales_data = pd.read_sql_query("SELECT * FROM fact_sales", conn_sales)

# Insert data into fact_sales in warehouse.db
fact_sales_data.to_sql('fact_sales', conn_warehouse, if_exists='append', index=False)

# Commit and close the connections
conn_warehouse.commit()
conn_sales.close()



print("✅ Data successfully moved to warehouse.db!")



print("✅ Dimension and Fact tables created in warehouse.db.")




