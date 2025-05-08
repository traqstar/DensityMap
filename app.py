import os
import pandas as pd
import plotly.express as px
import streamlit as st
from databricks import sql

# Load ZIP coordinate lookup from uscities Excel file
zip_lookup_raw = pd.read_excel("uscities.xlsx")

# Melt ZIP columns into long format
zip_columns = zip_lookup_raw.columns[7:]  # All columns from 8th onward
zip_lookup = zip_lookup_raw.melt(
    id_vars=["lat", "lng"],
    value_vars=zip_columns,
    var_name="zip_source_col",
    value_name="zipcode"
).dropna(subset=["zipcode"])

zip_lookup["zipcode"] = zip_lookup["zipcode"].astype(str).str.zfill(5)

# Databricks SQL connection config
connection = sql.connect(
    server_hostname = "adb-2583620669710215.15.azuredatabricks.net",
    http_path = "/sql/1.0/warehouses/148435e03690fbe3",
    access_token = os.getenv("DATABRICKS_TOKEN")
)

# SQL query
query = """
SELECT DISTINCT
    receipt_id,
    completed_at, 
    store,
    customer_zipcode,
    total_collected_post_discount_post_tax_post_fees
FROM analytics_dev.flowhub_silver.transactions
WHERE completed_at >= CURRENT_DATE() - INTERVAL 7 DAYS
  AND transaction_type != 'void'
"""

cursor = connection.cursor()
cursor.execute(query)
rows = cursor.fetchall()
columns = [desc[0] for desc in cursor.description]
cursor.close()
connection.close()

df = pd.DataFrame(rows, columns=columns)
df['completed_at'] = pd.to_datetime(df['completed_at'])
df['customer_zipcode'] = df['customer_zipcode'].astype(str).str.zfill(5)

# Join ZIPs to coordinates
df = df.merge(zip_lookup, left_on="customer_zipcode", right_on="zipcode", how="left")

# Leave lat/lng blank if ZIP code is blank or missing
df['lat'] = df.apply(lambda row: row['lat'] if pd.notna(row['customer_zipcode']) and row['customer_zipcode'].strip() != "" else None, axis=1)
df['lng'] = df.apply(lambda row: row['lng'] if pd.notna(row['customer_zipcode']) and row['customer_zipcode'].strip() != "" else None, axis=1)

# Streamlit UI
st.title("Customer Purchase Density Map (Last 7 Days)")
selected_store = st.selectbox("Choose a Store", sorted(df['store'].dropna().unique()))
color_scale = st.selectbox("Choose a color scale", ["Jet", "Viridis", "Plasma", "Cividis", "Hot"])

filtered_df = df[df['store'] == selected_store].dropna(subset=["lat", "lng"])

# Plot using OpenStreetMap style and user-selected color scale
fig = px.density_mapbox(
    filtered_df,
    lat="lat",
    lon="lng",
    z="total_collected_post_discount_post_tax_post_fees",
    radius=20,
    center=dict(lat=filtered_df["lat"].mean(), lon=filtered_df["lng"].mean()),
    zoom=6,
    mapbox_style="open-street-map",
    color_continuous_scale=color_scale.lower(),  # Plotly expects lowercase
    title=f"Customer Purchase Density for {selected_store}"
)
st.plotly_chart(fig)
