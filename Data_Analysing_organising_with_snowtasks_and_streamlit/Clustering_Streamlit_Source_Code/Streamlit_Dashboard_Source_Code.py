# -----------------------------
# COVID ETL + Dashboard in Streamlit
# -----------------------------

# Import packages
import streamlit as st
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col
import pandas as pd
import time
import random

# Optional: advanced visuals
try:
    import plotly.express as px
    PLOTLY_AVAILABLE = True
except ImportError:
    PLOTLY_AVAILABLE = False

# -----------------------------
# Snowflake session
# -----------------------------
session = get_active_session()

# -----------------------------
# Functions
# -----------------------------
def load_main_table():
    """Load main table into Pandas"""
    df_main = session.table("COVID_ANALYSIS").to_pandas()
    df_main.columns = [c.strip().replace(" ", "_").replace("/", "_").replace("%", "pct").lower() for c in df_main.columns]
    return df_main

def simulate_new_data():
    """Insert a random row into staging table"""
    country = random.choice(["India", "USA", "Brazil", "Italy", "France"])
    confirmed = random.randint(50000, 150000)
    death = random.randint(1000, 5000)
    recovered = random.randint(40000, confirmed - 1000)
    active = confirmed - death - recovered
    new_case = random.randint(100, 5000)
    new_deaths = random.randint(1, 500)
    new_recovered = random.randint(50, 4000)
    deaths_per_100_cases = round(death / confirmed * 100, 2)
    recovered_per_100_cases = round(recovered / confirmed * 100, 2)
    deaths_per_100_recovered = round(death / recovered * 100, 2)
    confirmed_last_week = confirmed - random.randint(1000, 5000)
    week_change = confirmed - confirmed_last_week
    week_pct_increase = int(week_change / confirmed_last_week * 100)
    who_region = random.choice(["SEARO", "EURO", "AFRO", "AMRO", "WPRO"])

    session.sql(f"""
        INSERT INTO covid_analysis_stage (
            "Country", "Confirmed", "Death", "Recovered", "Active",
            "New case", "New deaths", "New recovered",
            "Deaths / 100 Cases", "Recovered / 100 Cases", "Deaths / 100 Recovered",
            "Confirmed last week", "1 week change", "1 week % increase", "WHO Region"
        ) VALUES (
            '{country}', {confirmed}, {death}, {recovered}, {active},
            {new_case}, {new_deaths}, {new_recovered},
            {deaths_per_100_cases}, {recovered_per_100_cases}, {deaths_per_100_recovered},
            {confirmed_last_week}, {week_change}, {week_pct_increase}, '{who_region}'
        )
    """).collect()
    st.success(f"New row for {country} added to staging!")

def run_task():
    """Trigger Snowflake task to move stream rows to main table"""
    session.sql("EXECUTE TASK load_covid_task").collect()
    st.success("Task executed successfully!")

# -----------------------------
# Dashboard UI
# -----------------------------
st.title("COVID19 Dashboard with ETL Simulation")

# Buttons for simulating data and running task
col1, col2 = st.columns(2)
with col1:
    if st.button("Simulate New Daily Data"):
        simulate_new_data()
with col2:
    if st.button("Run Task Now"):
        run_task()

# Load main table
df = load_main_table()

# Sidebar filter for WHO Region
region_filter = st.sidebar.selectbox(
    "Select WHO Region", ["All"] + sorted(df["who_region"].unique().tolist())
)

# Filtered DataFrame
if region_filter != "All":
    start = time.time()
    df_filtered = df[df["who_region"] == region_filter]
    end = time.time()
    st.write(f"⏱ Query runtime for WHO Region '{region_filter}': {end - start:.3f} seconds")
else:
    df_filtered = df.copy()

# Show filtered table
st.subheader(f"Filtered Data (Region: {region_filter})")
st.dataframe(df_filtered)

# Metrics row
st.metric("Total Confirmed", int(df_filtered["confirmed"].sum()))
st.metric("Total Deaths", int(df_filtered["death"].sum()))
st.metric("Total Recovered", int(df_filtered["recovered"].sum()))

# Charts
st.subheader("Confirmed vs Death vs Recovered by Country")
if PLOTLY_AVAILABLE:
    fig = px.bar(
        df_filtered,
        x="country",
        y=["confirmed", "death", "recovered"],
        barmode="group",
        title="Cases by Country"
    )
    st.plotly_chart(fig, use_container_width=True)
else:
    st.bar_chart(df_filtered.set_index("country")[["confirmed", "death", "recovered"]])

# 1 Week % Increase chart
if "1_week_pct_increase" in df_filtered.columns:
    st.subheader("1 Week % Increase by Country")
    if PLOTLY_AVAILABLE:
        fig2 = px.bar(
            df_filtered,
            x="country",
            y="1_week_pct_increase",
            title="1 Week % Increase"
        )
        st.plotly_chart(fig2, use_container_width=True)
    else:
        st.bar_chart(df_filtered.set_index("country")[["1_week_pct_increase"]])

# Clustering Information
st.subheader("Clustering Information")
clustering_info = session.sql("SELECT SYSTEM$CLUSTERING_INFORMATION('COVID_ANALYSIS')").collect()
if clustering_info:
    clustering_dict = dict(clustering_info[0].as_dict())
    st.json(clustering_dict)
else:
    st.write("No clustering information available")
