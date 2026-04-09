import sys
import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import os
import subprocess
import time

# --- 1. SETUP & DATABASE GENERATION ---
db_path = 'data/pharmalyze.db'

# Ensure the data directory exists
if not os.path.exists('data'):
    os.makedirs('data')

# If the DB doesn't exist (common on first cloud deploy), run the ETL script
if not os.path.exists(db_path):
    st.info("🚀 First-time setup: Generating analytical warehouse. Please wait...")
    # Run the ingestion script and WAIT for it to complete
    result = subprocess.run(["python", "scripts/ingest_data.py"], capture_output=True, text=True)
    
    if result.returncode != 0:
        st.error(f"ETL Error: {result.stderr}")
        st.stop()
    else:
        st.success("✅ Warehouse generated successfully!")
        time.sleep(2) # Buffer for file system indexing

# Page Config
st.set_page_config(page_title="RevStream Analytics", layout="wide", page_icon="💊")

# --- 2. OPTIMIZED DATABASE CONNECTION ---
@st.cache_resource
def get_connection():
    # Connect without read_only for the first initialization to ensure access
    return duckdb.connect(db_path)

con = get_connection()

# --- 3. CACHED DATA FETCHING ---
@st.cache_data
def get_filter_options():
    regions = con.execute("SELECT DISTINCT region FROM dim_geography").df()['region'].tolist()
    drugs = con.execute("SELECT DISTINCT drug_category FROM fact_sales_scaled").df()['drug_category'].tolist()
    return sorted(regions), sorted(drugs)

@st.cache_data
def fetch_dashboard_data(where_stmt):
    # Metrics
    metrics = con.execute(f"""
        SELECT sum(units_sold), avg(units_sold) 
        FROM fact_sales_scaled f 
        JOIN dim_geography g ON f.geo_id = g.geo_id {where_stmt}
    """).fetchone()
    
    # Category Bar Chart
    cat_df = con.execute(f"""
        SELECT drug_category, sum(units_sold) as total
        FROM fact_sales_scaled f
        JOIN dim_geography g ON f.geo_id = g.geo_id
        {where_stmt}
        GROUP BY 1 ORDER BY total DESC
    """).df()
    
    # Time Series Line Chart
    trend_df = con.execute(f"""
        SELECT date_trunc('month', sale_date) as month, sum(units_sold) as total
        FROM fact_sales_scaled f
        JOIN dim_geography g ON f.geo_id = g.geo_id
        {where_stmt}
        GROUP BY 1 ORDER BY month
    """).df()
    
    # Sales Rep Leaderboard
    rep_df = con.execute(f"""
        SELECT f.rep_id, sum(f.units_sold) as total_units
        FROM fact_sales_scaled f
        JOIN dim_geography g ON f.geo_id = g.geo_id
        {where_stmt}
        GROUP BY 1 ORDER BY total_units DESC LIMIT 5
    """).df()
    
    return metrics, cat_df, trend_df, rep_df

# --- 4. SIDEBAR & FILTERS ---
regions, drugs = get_filter_options()

st.sidebar.header("Control Panel")
selected_region = st.sidebar.selectbox("Select Region", ["All"] + regions, index=0)
selected_drug = st.sidebar.selectbox("Select Drug Category", ["All"] + drugs, index=0)

# Build dynamic WHERE clause
query_parts = []
if selected_region != "All":
    query_parts.append(f"g.region = '{selected_region}'")
if selected_drug != "All":
    query_parts.append(f"f.drug_category = '{selected_drug}'")

where_clause = "WHERE " + " AND ".join(query_parts) if query_parts else ""

# Execute data fetch
metrics, cat_data, trend_data, rep_data = fetch_dashboard_data(where_clause)

# --- 5. UI RENDERING ---
st.title("Pharmalyze: Revenue Operations Engine")
st.markdown(f"**Current Scope:** Region: `{selected_region}` | Category: `{selected_drug}`")
st.markdown("---")

# Row 1: Key Metrics
c1, c2, c3 = st.columns(3)
c1.metric("Total Units (Projected)", f"{metrics[0] or 0:,.0f}")
c2.metric("Avg Daily Volume", f"{metrics[1] or 0:,.2f}")
c3.metric("Project Status", "Cloud Optimized")

# Row 2: Charts
col_left, col_right = st.columns(2)

with col_left:
    st.subheader("Regional Category Performance")
    fig_bar = px.bar(cat_data, x='drug_category', y='total', color='total', template="plotly_dark")
    st.plotly_chart(fig_bar, use_container_width=True)

with col_right:
    st.subheader("Monthly Sales Trend")
    fig_line = px.line(trend_data, x='month', y='total', markers=True, template="plotly_dark")
    st.plotly_chart(fig_line, use_container_width=True)

# Row 3: Leaderboard
st.markdown("---")
st.subheader("Top Performing Sales Representatives")
st.dataframe(rep_data, use_container_width=True, hide_index=True)