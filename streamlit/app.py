import streamlit as st
import plotly.express as px
import pandas as pd
import redis
import json
from pymongo import MongoClient

st.set_page_config(
    page_title="Real-Time Payments Intelligence",
    layout="wide"
)

# --------------------------------------------------
# DATABASE CONNECTIONS
# --------------------------------------------------
redis_client = redis.Redis(host="redis-cache", port=6379, decode_responses=True)

mongo = MongoClient("mongodb://mongo:27017")
db = mongo["bda_project"]

# --------------------------------------------------
# LOAD REDIS KPI DATA
# --------------------------------------------------
def load_kpi(key):
    try:
        data = redis_client.get(key)
        if not data:
            return None
        return pd.DataFrame(json.loads(data))
    except:
        return None

daily_df = load_kpi("kpi:daily")
merchant_df = load_kpi("kpi:merchant")
customer_df = load_kpi("kpi:customer")
payment_df = load_kpi("kpi:payment")

# --------------------------------------------------
# LIVE KPI FROM MONGO
# --------------------------------------------------
def get_live_transactions_last_minute():
    try:
        return db.transactions_fact.count_documents({})
    except:
        return 0


# --------------------------------------------------
# HEADER
# --------------------------------------------------
st.markdown(
    "<h1 style='text-align:center; color:#38bdf8;'>Real-Time Payments Intelligence Dashboard</h1>",
    unsafe_allow_html=True
)

st.markdown(
    "<p style='text-align:center;'><span style='color:#22c55e;font-weight:900;'>● LIVE</span> Spark · Airflow · MongoDB · Hadoop · Redis</p>",
    unsafe_allow_html=True
)

# --------------------------------------------------
# KPI CARDS
# --------------------------------------------------
st.markdown("## 📊 Platform Overview")

c1, c2, c3, c4 = st.columns(4)

if daily_df is not None and len(daily_df) > 0:
    total_tx = int(daily_df["transactions"].sum())
    total_revenue = daily_df["total_volume"].sum()
    avg_value = int(daily_df["avg_value"].mean())
else:
    total_tx = 0
    total_revenue = 0
    avg_value = 0

live_tx = get_live_transactions_last_minute()

def kpi_card(title, value):
    st.markdown(f"""
    <div style="
        background:#0f172a;
        padding:18px;
        border-radius:14px;
        text-align:center;
        box-shadow:0 10px 30px rgba(0,0,0,.3)">
        <div style="color:#9ca3af;font-size:14px;">{title}</div>
        <div style="color:white;font-size:36px;font-weight:900">{value}</div>
    </div>
    """, unsafe_allow_html=True)

with c1:
    kpi_card("Total Transactions", f"{total_tx:,}")

with c2:
    kpi_card("Total Revenue (PKR)", f"{total_revenue/1e9:.2f} B")

with c3:
    kpi_card("Avg Transaction Value", avg_value)

with c4:
    kpi_card("Live Transactions (Last 3 hrss)", live_tx)

# --------------------------------------------------
# PERFORMANCE ANALYTICS SECTION
# --------------------------------------------------
st.markdown("## 📈 Performance Analytics")

# -------- DAILY TRENDS --------
if daily_df is not None and len(daily_df) > 0:
    g1, g2 = st.columns(2)

    with g1:
        fig = px.line(
            daily_df,
            x="txn_date",
            y="transactions",
            title="Daily Transaction Volume",
            markers=True
        )
        st.plotly_chart(fig, use_container_width=True)

    with g2:
        fig = px.line(
            daily_df,
            x="txn_date",
            y="total_volume",
            title="Daily Revenue Trend",
            markers=True
        )
        st.plotly_chart(fig, use_container_width=True)
else:
    st.info("Waiting for KPI Job to populate Redis...")

# -------- MERCHANT + CUSTOMER --------
g3, g4 = st.columns(2)

if merchant_df is not None and len(merchant_df) > 0:
    with g3:
        fig = px.bar(
            merchant_df,
            x="business_category",
            y="total_revenue",
            title="Revenue by Merchant Category"
        )
        st.plotly_chart(fig, use_container_width=True)

if customer_df is not None and len(customer_df) > 0:
    with g4:
        fig = px.bar(
            customer_df,
            x="customer_segment",
            y="total_spend",
            title="Spending by Customer Segment"
        )
        st.plotly_chart(fig, use_container_width=True)

# -------- PAYMENT METHOD --------
if payment_df is not None and len(payment_df) > 0:
    fig = px.pie(
        payment_df,
        names="method_type",
        values="total_volume",
        title="Payment Method Share"
    )
    st.plotly_chart(fig, use_container_width=True)

# --------------------------------------------------
# FOOTER
# --------------------------------------------------
st.markdown(
    "<p style='text-align:center; color:#9ca3af;'>Auto-refresh every 5 seconds · Powered by Redis Cache + Mongo Live Data</p>",
    unsafe_allow_html=True
)

st_autorefresh = st.empty()
st_autorefresh.experimental_rerun = False
#st_autorefresh  # placeholder for UI stability
#st.write(redis.get("kpi:debug"))
