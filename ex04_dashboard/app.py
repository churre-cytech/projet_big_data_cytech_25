import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import matplotlib.pyplot as plt

st.set_page_config(page_title="Yellow taxis - NYC dashboard", layout="wide")
engine = create_engine("postgresql+psycopg2://bigdata:bigdata123@localhost:5432/bigdata_dwh")

# ###################### Streamlit dashboard struct ######################
# [Title + description]
# [KPI KPI KPI KPI KPI]
# [Graph A] [Graph B]
# [Graph C] [Graph D]
# [Top pickup zones table + bar]


# ###################### [Title + description] ######################
st.title("NYC taxis - Datamart dashboard")
st.caption("Source : Postgres (fact_trip) - Processed yellow taxis NYC data - May 2025 (yellow_tripdata_2025-05.parquet)")
st.divider()


# ###################### [KPI KPI KPI KPI KPI] ######################
query_kpis = """
SELECT
    COUNT(*) AS trips,
    SUM(total_amount) AS revenue,
    AVG(total_amount) AS avg_fare,
    AVG(trip_distance) AS avg_distance,
    AVG(CASE WHEN total_amount > 0 THEN tip_amount / total_amount END) AS tip_rate
FROM fact_trip;
"""
df_kpi = pd.read_sql(query_kpis, engine).iloc[0]

with st.container():
    st.subheader("KPIs")
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Total trips", f"{int(df_kpi.trips):,}")
    c2.metric("Revenue", f"{df_kpi.revenue:,.2f}$")
    c3.metric("Average fare", f"{df_kpi.avg_fare:,.2f}$")
    c4.metric("Average distance", f"{df_kpi.avg_distance:,.2f} miles")
    c5.metric("Tip rate", f"{df_kpi.tip_rate*100:.1f}%")
    st.divider()


# ###################### 2x2 graphs ######################
# [Graph A] [Graph B]
# [Graph C] [Graph D]


# ######### [Graph A] [Graph B] #########
# ######### [Graph A = Trips by day of week] #########
query_trips_day = """
SELECT d.day_of_week, COUNT(*) AS trips
FROM fact_trip f
JOIN dim_date d ON f.pickup_date_id = d.date_id
GROUP BY d.day_of_week
ORDER BY d.day_of_week;
"""
df_trips_day = pd.read_sql(query_trips_day, engine)

# ######### [Graph B = Revenue by hour] #########
query_revenue_hour = """
SELECT EXTRACT(HOUR FROM pickup_datetime) AS hour,
        SUM(total_amount) AS revenue
FROM fact_trip
GROUP BY hour
ORDER BY hour;
"""
df_revenue_hour = pd.read_sql(query_revenue_hour, engine)

with st.container():
    st.subheader("Volume and revenue")
    left1, right1 = st.columns(2)
    with left1:
        # Remind : dayofweek() Spark function -> 1 = Sunday
        labels_trips_day = {
            1: "7-Sunday",
            2: "1-Monday",
            3: "2-Tuesday",
            4: "3-Wednesday",
            5: "4-Thursday",
            6: "5-Friday",
            7: "6-Saturday",
        }
        df_trips_day["day"] = df_trips_day["day_of_week"].map(labels_trips_day)

        # Trying to re-order day of weeek... -> simpliest way putting "number-dayofweek"
        # day_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        # df_trips_day["day"] = pd.Categorical(df_trips_day["day"], categories=day_order, ordered=True)
        # df_trips_day = df_trips_day.sort_values("day")

        st.subheader("Trips by day of week")
        st.write("...")

        st.bar_chart(df_trips_day.set_index("day")["trips"])
    with right1:
        st.subheader("Revenue by hour")
        st.write("...")
        df_revenue_hour["hour"] = df_revenue_hour["hour"].astype(int)
        st.line_chart(df_revenue_hour.set_index("hour")["revenue"])


# ######### [Graph C] [Graph D] #########
# ######### [Graph C = Average fare by payment type] #########
query_avg_fare_payment_type = """
SELECT p.payment_type_name, AVG(f.total_amount) AS avg_fare
FROM fact_trip f
JOIN dim_payment p ON f.payment_type_id = p.payment_type_id
GROUP BY p.payment_type_name
ORDER BY avg_fare DESC;
"""
df_avg_fare_payment_type = pd.read_sql(query_avg_fare_payment_type, engine)

# ######### [Graph D = Average tip rate by vendor] #########
query_tip_rate_vendor = """
SELECT v.vendor_name,
    AVG(CASE WHEN f.total_amount > 0 THEN f.tip_amount / f.total_amount END) AS tip_rate
FROM fact_trip f
JOIN dim_vendor v ON f.vendor_id = v.vendor_id
GROUP BY v.vendor_name
ORDER BY tip_rate DESC;
"""
df_tip_rate_vendor = pd.read_sql(query_tip_rate_vendor, engine)

with st.container():
    st.subheader("Payment and tips")
    left2, right2 = st.columns(2)
    with left2:
        st.subheader("Average fare by payment type")
        st.write("...")
        st.bar_chart(df_avg_fare_payment_type.set_index("payment_type_name")["avg_fare"])
    with right2:
        st.subheader("Average tip rate by vendor")
        st.write("...")
        df_tip_rate_vendor["tip_rate_pct"] = df_tip_rate_vendor["tip_rate"] * 100
        st.bar_chart(df_tip_rate_vendor.set_index("vendor_name")["tip_rate_pct"])

        # With matplotlib 
        # fig, ax = plt.subplots()
        # ax.barh(df_tip_rate_vendor["vendor_name"], df_tip_rate_vendor["tip_rate"])
        # ax.set_label("Tip rate (%)")
        # ax.set_title("Average tip rate by vendor")
        # st.pyplot(fig)

st.divider()


# ###################### [Top 10 pickup zones table + bar] ######################
query_top_pickups = """
SELECT l.borough, l.zone, COUNT(*) AS trips
FROM fact_trip f
JOIN dim_location l ON f.pickup_location_id = l.location_id
GROUP BY l.borough, l.zone
ORDER BY trips DESC
LIMIT 10;
"""
df_query_top_pickups = pd.read_sql(query_top_pickups, engine)

df_query_top_pickups["borough"] = df_query_top_pickups["borough"].astype(str)
df_query_top_pickups["zone"] = df_query_top_pickups["zone"].astype(str)
df_query_top_pickups["zone_label"] = df_query_top_pickups["borough"] + " - " + df_query_top_pickups["zone"]

with st.container():
    st.subheader("Top 10 pickup zones")
    left, right = st.columns(2)
    with left:
        st.write("Table ...")
        st.table(df_query_top_pickups[["borough", "zone", "trips"]])
    with right:
        st.write("Bar ...")
        st.bar_chart(df_query_top_pickups.set_index("zone_label")["trips"])
