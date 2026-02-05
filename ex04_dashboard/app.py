import streamlit as st
import pandas as pd
from sqlalchemy import create_engine

st.set_page_config(page_title="Yellow taxis NYC dashboard", layout="wide")

engine = create_engine("postgresql+psycopg2://bigdata:bigdata123@localhost:5432/bigdata_dwh")

df = pd.read_sql("SELECT COUNT(*) AS trips FROM fact_trip", engine)

st.title("NYC taxis - Datamart dashboard")

# ########### Total trips ###########
# st.metric("Total trips", f"{int(df.trips[0]):,}")

# ########### Top 10 pickup zones ###########
st.subheader("Top 10 pickup zones")

df_pickups = pd.read_sql("""
SELECT l.borough, l.zone, COUNT(*) AS trips
FROM fact_trip f
JOIN dim_location l ON f.pickup_location_id = l.location_id
GROUP BY l.borough, l.zone
ORDER BY trips DESC
LIMIT 10;
""", engine)

# Label = label unique borough + zone
df_pickups["zone_label"] = df_pickups["borough"] + " - " + df_pickups["zone"]

st.dataframe(df_pickups[["borough", "zone", "trips"]])
st.bar_chart(df_pickups.set_index("zone_label")["trips"])
