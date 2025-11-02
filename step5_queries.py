import sqlite3
import pandas as pd

# Connect to your database
conn = sqlite3.connect('sales_data.db')

# === QUERY 1: Total Sales by Year ===
print("\n📅 Total Sales by Year:")
query1 = '''
SELECT 
    d.order_year AS Year, 
    ROUND(SUM(f.calc_sales), 2) AS Total_Sales
FROM fact_sales f
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY d.order_year
ORDER BY d.order_year;
'''
print(pd.read_sql(query1, conn))


# === QUERY 2: Top 5 Customers by Profit ===
print("\n👑 Top 5 Customers by Profit:")
query2 = '''
SELECT 
    c.customer_name AS Customer, 
    ROUND(SUM(f.profit), 2) AS Total_Profit
FROM fact_sales f
JOIN dim_customer c ON f.customer_id = c.customer_id
GROUP BY c.customer_name
ORDER BY Total_Profit DESC
LIMIT 5;
'''
print(pd.read_sql(query2, conn))


# === QUERY 3: Monthly Sales Trend (by Year & Month) ===
print("\n📈 Monthly Sales Trend:")
query3 = '''
SELECT 
    d.order_year AS Year,
    d.order_month AS Month,
    ROUND(SUM(f.calc_sales), 2) AS Monthly_Sales
FROM fact_sales f
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY d.order_year, d.order_month
ORDER BY d.order_year, d.order_month;
'''
print(pd.read_sql(query3, conn))


# === QUERY 4: Product Line Performance ===
print("\n📦 Product Line Performance:")
query4 = '''
SELECT 
    p.product_line AS Product_Line,
    ROUND(SUM(f.calc_sales), 2) AS Total_Sales,
    ROUND(SUM(f.profit), 2) AS Total_Profit
FROM fact_sales f
JOIN dim_product p ON f.product_code = p.product_code
GROUP BY p.product_line
ORDER BY Total_Sales DESC;
'''
print(pd.read_sql(query4, conn))


# === QUERY 5: Deal Size Distribution ===
print("\n💰 Deal Size Distribution:")
query5 = '''
SELECT 
    f.deal_size_cat AS Deal_Size,
    COUNT(*) AS Order_Count,
    ROUND(SUM(f.calc_sales), 2) AS Total_Sales
FROM fact_sales f
GROUP BY f.deal_size_cat
ORDER BY Total_Sales DESC;
'''
print(pd.read_sql(query5, conn))

conn.close()
print("\n✅ Queries executed successfully!")
