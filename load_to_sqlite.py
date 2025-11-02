import pandas as pd
import sqlite3

# === STEP 1: LOAD THE TRANSFORMED CSV ===
# Use raw string so Windows paths work correctly
file_path = r"C:\Users\hp\Desktop\assignment\transformed_sales.csv"
df = pd.read_csv(file_path)

# === STEP 2: CONNECT TO (OR CREATE) THE DATABASE ===
conn = sqlite3.connect('sales_data.db')
cursor = conn.cursor()

# Clean up old tables if they exist (for re-runs)
cursor.execute("DROP TABLE IF EXISTS dim_date")
cursor.execute("DROP TABLE IF EXISTS dim_customer")
cursor.execute("DROP TABLE IF EXISTS dim_product")
cursor.execute("DROP TABLE IF EXISTS fact_sales")

# === STEP 3: CREATE DIMENSION TABLES ===

# 3a. DATE DIMENSION
cursor.execute('''
CREATE TABLE dim_date (
    date_key INTEGER PRIMARY KEY,
    order_date TEXT,
    order_year INTEGER,
    order_month INTEGER,
    order_day INTEGER
)
''')

# 3b. CUSTOMER DIMENSION
cursor.execute('''
CREATE TABLE dim_customer (
    customer_id INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_name TEXT,
    city TEXT,
    state TEXT,
    country TEXT
)
''')

# 3c. PRODUCT DIMENSION
cursor.execute('''
CREATE TABLE dim_product (
    product_code TEXT PRIMARY KEY,
    product_line TEXT
)
''')

# === STEP 4: CREATE FACT TABLE ===
cursor.execute('''
CREATE TABLE fact_sales (
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

conn.commit()
print("✅ Tables created successfully!")

# === STEP 5: LOAD DIMENSION TABLES ===

# 5a. DIM_DATE
dim_date = df[['ORDERDATE', 'ORDER_YEAR', 'ORDER_MONTH', 'ORDER_DAY']].drop_duplicates().copy()
dim_date['date_key'] = pd.to_datetime(dim_date['ORDERDATE']).dt.strftime("%Y%m%d").astype(int)
dim_date.rename(columns={
    'ORDERDATE': 'order_date',
    'ORDER_YEAR': 'order_year',
    'ORDER_MONTH': 'order_month',
    'ORDER_DAY': 'order_day'
}, inplace=True)
dim_date = dim_date[['date_key', 'order_date', 'order_year', 'order_month', 'order_day']]
dim_date.to_sql('dim_date', conn, if_exists='append', index=False)

# 5b. DIM_CUSTOMER
dim_customer = df[['CUSTOMERNAME', 'CITY', 'STATE', 'COUNTRY']].drop_duplicates().copy()
dim_customer.rename(columns={
    'CUSTOMERNAME': 'customer_name',
    'CITY': 'city',
    'STATE': 'state',
    'COUNTRY': 'country'
}, inplace=True)
dim_customer.to_sql('dim_customer', conn, if_exists='append', index=False)

# 5c. DIM_PRODUCT
if 'PRODUCTCODE' in df.columns and 'PRODUCTLINE' in df.columns:
    dim_product = df[['PRODUCTCODE', 'PRODUCTLINE']].drop_duplicates().copy()
    dim_product.rename(columns={
        'PRODUCTCODE': 'product_code',
        'PRODUCTLINE': 'product_line'
    }, inplace=True)
    dim_product.to_sql('dim_product', conn, if_exists='append', index=False)

print("✅ Dimension tables loaded successfully!")

# === STEP 6: LOAD FACT TABLE ===

# Build a clean date_key column in the main DataFrame
df['date_key'] = pd.to_datetime(df['ORDERDATE']).dt.strftime("%Y%m%d").astype(int)

# Select the needed columns for the fact table
fact_sales = df[['ORDERNUMBER', 'QUANTITYORDERED', 'PRICEEACH', 'CALC_SALES', 
                 'PROFIT', 'DEAL_SIZE_CAT', 'PRODUCTCODE', 'CUSTOMERNAME', 'date_key']].copy()

# Get customer_id from dim_customer
dim_cust = pd.read_sql('SELECT rowid as customer_id, customer_name FROM dim_customer', conn)
fact_sales = fact_sales.merge(dim_cust, left_on='CUSTOMERNAME', right_on='customer_name', how='left')
fact_sales.drop(columns=['CUSTOMERNAME', 'customer_name'], inplace=True)

# Rename columns to match table
fact_sales.rename(columns={
    'ORDERNUMBER': 'order_number',
    'QUANTITYORDERED': 'quantity_ordered',
    'PRICEEACH': 'price_each',
    'CALC_SALES': 'calc_sales',
    'PRODUCTCODE': 'product_code'
}, inplace=True)

# Save the fact table
fact_sales.to_sql('fact_sales', conn, if_exists='append', index=False)

print("✅ Fact table loaded successfully!")

# === STEP 7: VERIFY DATA COUNTS ===
for table in ['dim_date', 'dim_customer', 'dim_product', 'fact_sales']:
    count = pd.read_sql(f"SELECT COUNT(*) as cnt FROM {table}", conn)
    print(f"{table}: {count['cnt'][0]} rows")

# === STEP 8: SAMPLE QUERIES ===
print("\n📊 Sample Checks:")
sample = pd.read_sql('''
SELECT d.order_year, SUM(f.calc_sales) as total_sales
FROM fact_sales f
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY d.order_year
ORDER BY d.order_year
''', conn)
print(sample)

conn.close()
print("\n✅ Step 4 completed — data loaded into SQLite using Dimensional Modeling!")
