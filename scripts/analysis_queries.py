import duckdb

# connect to db 
con = duckdb.connect('data/pharmalyze.db')

print("Running Business Intelligence Queries...\n")

# Query 1: Regional Sales Distribution
# Shows which regions are moving the most volume
query_1 = """
SELECT 
    g.region, 
    f.drug_category, 
    round(sum(f.units_sold), 2) as total_units
FROM fact_sales_scaled f
JOIN dim_geography g ON f.geo_id = g.geo_id
GROUP BY 1, 2
ORDER BY 1, 3 DESC;
"""

# Query 2: Identifying "Top Performers" (Window Function)
# ZS loves this: Find the top sales rep per region
query_2 = """
WITH RepPerformance AS (
    SELECT 
        g.region,
        f.rep_id,
        sum(f.units_sold) as units
    FROM fact_sales_scaled f
    JOIN dim_geography g ON f.geo_id = g.geo_id
    GROUP BY 1, 2
)
SELECT region, rep_id, units
FROM (
    SELECT *, 
    RANK() OVER (PARTITION BY region ORDER BY units DESC) as rank
    FROM RepPerformance
)
WHERE rank = 1;
"""

print("--- Top Sales Rep per Region ---")
print(con.execute(query_2).df())

con.close()