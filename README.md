# Mini ETL Project - Sales Data
This project demonstrates how to build a simple ETL (Extract, Transform, Load) pipeline using Python and SQLite.
The goal is to extract sales data from a CSV file, perform various data cleaning and transformation operations, and load the transformed data into a SQLite database for further analysis.

This project covers the first two steps of a modern data pipeline:

Extraction & Loading – Load sales data from a CSV file into a SQLite database.

Transformation – Clean, format, and enrich the data for analytics and reporting.

🧠 Key Learning Objectives

By completing this project, you’ll learn how to:

Read and manipulate CSV data using pandas

Apply multiple data transformation techniques (calculations, formatting, cleaning)

Work with SQLite databases using Python’s sqlite3 library

Use regular expressions (re) to clean and standardize data

Write transformed data back into a relational database

⚙️ Technologies Used
Component	Description
Language	Python
Libraries	pandas, sqlite3, re
Database	SQLite
Dataset	sales_data_sample.csv
IDE (Recommended)	Visual Studio Code
📂 Project Structure
Sales-Data-ETL/
│
├── sales_data_sample.csv          # Raw sales data file
├── load_data.py                   # Script to load data from CSV into database.db
├── transform_data.py              # Script to clean and transform sales data
├── database.db                    # SQLite database (created by script)
├── README.md                      # Project documentation
└── requirements.txt               # Python dependencies (optional)

🚀 Steps Performed
Step 1: Data Extraction & Loading

Loaded sales_data_sample.csv into a pandas DataFrame.

Created a SQLite database (database.db) using sqlite3.

Inserted all raw records from the CSV into a new table called sales_data.

Step 2: Data Transformation

Applied several key transformations using pandas:

✅ Calculated total sales (QUANTITYORDERED × PRICEEACH)

✅ Converted order dates into a standard datetime format

✅ Created a new profit column (assuming a 20% margin)

✅ Categorized deal sizes into Small, Medium, and Large

✅ Standardized phone numbers using regular expressions

✅ Handled missing or invalid data by filling or removing null values

Step 3: Store Transformed Data

Created a new table sales_data_transformed in database.db

Inserted all cleaned and enriched records into this new table.

🧩 Example Transformation: Standardizing Phone Numbers
import re

def standardize_phone_number(phone_number):
    phone_number = re.sub(r'\D', '', phone_number)  # Remove non-numeric characters
    if len(phone_number) == 10:
        return f"({phone_number[:3]}) {phone_number[3:6]}-{phone_number[6:]}"
    else:
        return None  # Invalid format


This ensures all phone numbers follow the format:
(123) 456-7890

🛠️ How to Run the Project
1. Clone the Repository
git clone https://github.com/<your-username>/Sales-Data-ETL.git
cd Sales-Data-ETL

2. Install Dependencies

If you have a requirements.txt file, install dependencies with:

pip install -r requirements.txt


(You mainly need pandas.)

pip install pandas

3. Run the Python Scripts

Load data into SQLite:

python load_data.py


Apply transformations:

python transform_data.py

4. Verify the Results

Open your SQLite database (database.db) in DB Browser for SQLite or run:

import sqlite3
import pandas as pd

conn = sqlite3.connect('database.db')
print(pd.read_sql_query("SELECT * FROM sales_data_transformed LIMIT 5;", conn))
conn.close()

📊 Sample Output
ORDERNUMBER	QUANTITYORDERED	PRICEEACH	TOTAL_SALES	DEAL_SIZE	STANDARDIZED_PHONE
10107	30	95.70	2871.00	Large	(123) 456-7890
10121	34	81.35	2765.90	Large	(456) 789-0123
10134	41	94.74	3884.34	Large	(789) 012-3456
🧱 Future Enhancements

Build a data warehouse (warehouse.db) with dimensional modeling

Automate ETL jobs using Apache Airflow

Add data validation and logging

Visualize the data using Power BI / Tableau

✨ Why This Project Matters

This project is a perfect starting point for anyone learning data engineering or ETL pipelines.
It shows how to move data from raw CSVs into a structured, queryable database while cleaning and enriching it using industry-standard Python tools.
