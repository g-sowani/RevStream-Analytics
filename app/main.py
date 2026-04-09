import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px

st.set_page_config(page_title="RevStream Analytics", layout="wide", page_icon="💊")

# 1. Optimized Database Connection (Prevents reset on click)
@st.cache_resource
def get_connection():
    return duckdb.connect('data/pharmalyze.db', read_only=True)

con = get_connection()

# 2. Get Filter Options once (Cached for speed)
@st.cache_data
def get_filter_options():
    regions = con.execute("SELECT DISTINCT region FROM dim_geography").df()['region'].tolist()
    drugs = con.execute("SELECT DISTINCT drug_category FROM fact_sales_scaled").df()['drug_category'].tolist()
    return sorted(regions), sorted(drugs)

regions, drugs = get_filter_options()

# --- SIDEBAR ---
st.sidebar.header("🕹️ Control Panel")

# Adding 'All' to the options
selected_region = st.sidebar.selectbox("Select Region", ["All"] + regions, index=0)
selected_drug = st.sidebar.selectbox("Select Drug Category", ["All"] + drugs, index=0)

# --- DYNAMIC QUERY BUILDING ---
# We build the WHERE clause based on current selection
query_parts = []
if selected_region != "All":
    query_parts.append(f"g.region = '{selected_region}'")
if selected_drug != "All":
    query_parts.append(f"f.drug_category = '{selected_drug}'")

where_clause = "WHERE " + " AND ".join(query_parts) if query_parts else ""

# --- DATA FETCHING (The Engine) ---
@st.cache_data
def fetch_dashboard_data(where_stmt):
    # We use a single connection to fetch different slices
    metrics = con.execute(f"""
        SELECT sum(units_sold), avg(units_sold) 
        FROM fact_sales_scaled f 
        JOIN dim_geography g ON f.geo_id = g.geo_id {where_stmt}
    """).fetchone()
    
    cat_df = con.execute(f"""
        SELECT drug_category, sum(units_sold) as total
        FROM fact_sales_scaled f
        JOIN dim_geography g ON f.geo_id = g.geo_id
        {where_stmt}
        GROUP BY 1 ORDER BY total DESC
    """).df()
    
    trend_df = con.execute(f"""
        SELECT date_trunc('month', sale_date) as month, sum(units_sold) as total
        FROM fact_sales_scaled f
        JOIN dim_geography g ON f.geo_id = g.geo_id
        {where_stmt}
        GROUP BY 1 ORDER BY month
    """).df()
    
    rep_df = con.execute(f"""
        SELECT f.rep_id, sum(f.units_sold) as total_units
        FROM fact_sales_scaled f
        JOIN dim_geography g ON f.geo_id = g.geo_id
        {where_stmt}
        GROUP BY 1 ORDER BY total_units DESC LIMIT 5
    """).df()
    
    return metrics, cat_df, trend_df, rep_df

# Execute the fetch
metrics, cat_data, trend_data, rep_data = fetch_dashboard_data(where_clause)

# --- UI RENDERING ---
st.title("💊 Pharmalyze: Revenue Operations Engine")
st.markdown(f"**Showing Data for:** Region: `{selected_region}` | Category: `{selected_drug}`")
st.markdown("---")

# Metrics Row
c1, c2, c3 = st.columns(3)
c1.metric("Total Units", f"{metrics[0] or 0:,.0f}")
c2.metric("Avg Daily Volume", f"{metrics[1] or 0:,.2f}")
c3.metric("Project Status", "Cloud Ready")

# Visuals Row
col_left, col_right = st.columns(2)

with col_left:
    st.subheader("Regional Performance")
    fig_bar = px.bar(cat_data, x='drug_category', y='total', color='total', template="plotly_dark")
    st.plotly_chart(fig_bar, use_container_width=True)

with col_right:
    st.subheader("Monthly Sales Trend")
    fig_line = px.line(trend_data, x='month', y='total', markers=True, template="plotly_dark")
    st.plotly_chart(fig_line, use_container_width=True)

# Leaderboard
st.markdown("---")
st.subheader("🏆 Top Performing Sales Representatives")
st.dataframe(rep_data, use_container_width=True)