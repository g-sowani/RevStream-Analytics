import duckdb

con = duckdb.connect('data/pharmalyze.db')

def run_validation():
    print("🧪 Starting Data Validation Suite...")
    
    # Check 1: Null values in critical columns
    nulls = con.execute("SELECT count(*) FROM fact_sales_scaled WHERE units_sold IS NULL OR sale_date IS NULL").fetchone()[0]
    print(f"- Null Check: {'✅ Passed' if nulls == 0 else '❌ Failed (' + str(nulls) + ' nulls found)'}")

    # Check 2: Referential Integrity (Do all geo_ids exist in dim_geography?)
    orphans = con.execute("""
        SELECT count(*) FROM fact_sales_scaled f 
        LEFT JOIN dim_geography g ON f.geo_id = g.geo_id 
        WHERE g.geo_id IS NULL
    """).fetchone()[0]
    print(f"- Referential Integrity: {'✅ Passed' if orphans == 0 else '❌ Failed (' + str(orphans) + ' orphan rows)'}")

    # Check 3: Logical Volume (Are sales always positive?)
    negatives = con.execute("SELECT count(*) FROM fact_sales_scaled WHERE units_sold < 0").fetchone()[0]
    print(f"- Negative Sales Check: {'✅ Passed' if negatives == 0 else '❌ Failed'}")

run_validation()
con.close()