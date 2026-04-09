import duckdb
import pandas as pd
import numpy as np

print("Reading and cleaning raw csv/excel data...")

# store in data frame df 
try : 
    df = pd.read_excel('data/raw/salesdaily.csv.xls')
except :
    df = pd.read_csv('data/raw/salesdaily.csv.xls')

# dayfirst - false means mm/dd/yyyy 
df['datum'] = pd.to_datetime(df['datum'], dayfirst=False)


con = duckdb.connect('data/pharmalyze.db')

con.register('df_raw', df)

print("Building the Normalized Star Schema...")

# Create Geography Dimension
con.execute("""
    CREATE TABLE dim_geography AS 
    SELECT * FROM (VALUES 
        (1, 'Mumbai', 'West'), (2, 'Delhi', 'North'), 
        (3, 'Bangalore', 'South'), (4, 'Kolkata', 'East'),
        (5, 'Pune', 'West'), (6, 'Chennai', 'South')
    ) AS t(geo_id, city, region);
""")

# Create the Fact Table by "Unpivoting" the drug columns
# This converts columns like M01AB, N02BA into a single 'drug_category' column
# so we will have drug_category - M01AB and units_sold as the numerical value inside!!
# this is called normalization
con.execute("""
    CREATE OR REPLACE TABLE fact_sales AS
    SELECT (row_number() OVER ()) AS sale_id, datum AS sale_date, 
           floor(random() * 6 + 1)::INT AS geo_id, floor(random() * 100 + 1)::INT AS rep_id,
           'M01AB' AS drug_category, M01AB AS units_sold FROM df_raw
    UNION ALL
    SELECT (row_number() OVER ()) + 1000000, datum, floor(random() * 6 + 1)::INT, floor(random() * 100 + 1)::INT,
           'M01AE' AS drug_category, M01AE AS units_sold FROM df_raw
    UNION ALL
    SELECT (row_number() OVER ()) + 2000000, datum, floor(random() * 6 + 1)::INT, floor(random() * 100 + 1)::INT,
           'N02BA' AS drug_category, N02BA AS units_sold FROM df_raw
    UNION ALL
    SELECT (row_number() OVER ()) + 3000000, datum, floor(random() * 6 + 1)::INT, floor(random() * 100 + 1)::INT,
           'N02BE' AS drug_category, N02BE AS units_sold FROM df_raw;
""")

# Scale the data to 5M rows by cross-joining with a range
# why - real world client of ZS like Pfizer and Novartis have millions of transactions
# prove your code wont break at sale 
print("Scaling to 5M+ records for performance optimization...")
con.execute("""
    CREATE TABLE fact_sales_scaled AS 
    SELECT 
        (row_number() OVER ()) AS sale_id,
        sale_date + (range * INTERVAL '1 day') AS sale_date, -- shifts dates to simulate more time
        geo_id,
        rep_id,
        drug_category,
        units_sold * (random() + 0.5) AS units_sold
    FROM fact_sales, range(10) -- Adjust this multiplier to hit your 5M goal
""")

print(f"✅ Success! Total rows in fact_sales_scaled: {con.execute('SELECT count(*) FROM fact_sales_scaled').fetchone()[0]}")
con.close()


# Unlike a Pandas DataFrame
# which stays in your RAM and crashes your computer if it's too big), 
# DuckDB stores the data on your Hard Drive but processes it in 
# small chunks. This is how you handle 5M rows on a laptop with 
# limited memory.