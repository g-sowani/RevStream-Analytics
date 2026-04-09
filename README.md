# 💊 Pharmalyze: Enterprise Revenue Operations Engine

### 📊 Project Overview

Pharmalyze is an end-to-end data engineering and business intelligence solution designed for the pharmaceutical industry. It transforms fragmented, flat-file sales data into a scalable **Star Schema** architecture, enabling high-performance regional analysis and sales representative tracking.

---

## 💼 Business Case

Pharmaceutical firms often struggle with "Data Silos" where sales data is trapped in wide-format Excel sheets, making it impossible to perform granular regional or category-level analysis.

**The Solution:** This project implements a centralized **Data Warehouse** logic that:

- **Normalizes** drug categories for scalable reporting.
- **Simulates Enterprise Volume** (84k+ records scaled from real-world data).
- **Automates KPI Tracking** for Sales Representatives and Regional Managers.

---

## 🏗️ Technical Architecture

The system follows a modern OLAP (Online Analytical Processing) design:

1. **Ingestion Layer:** Python/Pandas handles initial cleanup of heterogeneous Excel/CSV sources.
2. **Storage & Compute:** **DuckDB** serves as the analytical engine, utilizing columnar storage for sub-second query execution on multi-million row datasets.
3. **Data Modeling:** A **Star Schema** design separates quantitative 'Facts' from descriptive 'Dimensions'.
4. **Visualization:** A **Streamlit** dashboard provides real-time interactivity powered by SQL Window Functions and CTEs.

---

## 🗄️ Data Model (Star Schema)

To ensure high performance, the data is structured as follows:

- **Fact Table (`fact_sales_scaled`):** Contains transaction-level data (Date, Units Sold, Foreign Keys).
- **Dimension Tables:** \* `dim_geography`: Mapping IDs to Cities and Regions (North, West, etc.).
  - `dim_reps`: Sales personnel details and performance tiers.
  - `dim_products`: Drug classifications (ATC codes) and pricing.

---

## 🚀 How to Run

1. **Clone the repo:** `git clone https://github.com/YOUR_USERNAME/RevStream-Analytics.git`
2. **Install dependencies:** `pip install -r requirements.txt`
3. **Initialize Database:** `python scripts/ingest_data.py`
4. **Launch Dashboard:** `streamlit run app/main.py`
